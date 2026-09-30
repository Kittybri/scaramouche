"""Small durable replay, action audit, quota and ephemeral event store."""

from __future__ import annotations
import json
import time
from contextlib import asynccontextmanager
import aiosqlite
from .protocol import Rejected


class Store:
    def __init__(self, path, audit_retention_seconds=30 * 86400):
        self.path = str(path)
        self.audit_retention = max(60, min(30 * 86400, float(audit_retention_seconds)))

    @asynccontextmanager
    async def db(self):
        async with aiosqlite.connect(self.path, timeout=15) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("PRAGMA busy_timeout=15000")
            yield db

    async def init(self):
        async with self.db() as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.executescript("""
            CREATE TABLE IF NOT EXISTS home_receipts(key TEXT PRIMARY KEY,expires REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS home_state(key TEXT PRIMARY KEY,payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS home_audit(
                request_id TEXT PRIMARY KEY,ts REAL,bot TEXT,user_id INTEGER,device TEXT,action TEXT,trigger TEXT,result TEXT);
            CREATE INDEX IF NOT EXISTS home_audit_device ON home_audit(device,ts);
            CREATE TABLE IF NOT EXISTS home_events(
                id INTEGER PRIMARY KEY,ts REAL,user_id INTEGER,bot TEXT,kind TEXT,zone TEXT,consumed INTEGER DEFAULT 0);
            CREATE INDEX IF NOT EXISTS home_events_owner ON home_events(user_id,bot,consumed,ts);
            """)
            await db.commit()

    async def claim(self, key, ttl=120):
        async with self.db() as db:
            await db.execute("BEGIN IMMEDIATE")
            await db.execute(
                "DELETE FROM home_receipts WHERE expires<?", (time.time(),)
            )
            cursor = await db.execute(
                "INSERT OR IGNORE INTO home_receipts VALUES(?,?)",
                (key, time.time() + ttl),
            )
            await db.commit()
            return bool(cursor.rowcount)

    async def get(self, key, default=None):
        async with self.db() as db:
            row = await (
                await db.execute("SELECT payload FROM home_state WHERE key=?", (key,))
            ).fetchone()
        return json.loads(row[0]) if row else default

    async def rate(self, identity, limit=10):
        prefix = "rate:" + identity + ":" + str(int(time.time())) + ":"
        async with self.db() as db:
            await db.execute("BEGIN IMMEDIATE")
            row = await (
                await db.execute(
                    "SELECT COUNT(*) FROM home_receipts WHERE key LIKE ?",
                    (prefix + "%",),
                )
            ).fetchone()
            if row[0] >= limit:
                return False
            await db.execute(
                "INSERT INTO home_receipts VALUES(?,?)",
                (prefix + str(row[0]), time.time() + 2),
            )
            await db.commit()
            return True

    async def put(self, key, value):
        async with self.db() as db:
            await db.execute(
                "INSERT INTO home_state VALUES(?,?) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload",
                (key, json.dumps(value)),
            )
            await db.commit()

    async def consume_proposal(self, request_id, user_id, guild_id, bot):
        """Atomically bind and consume one exact confirmation proposal."""
        key = "proposal:" + request_id
        async with self.db() as db:
            await db.execute("BEGIN IMMEDIATE")
            row = await (
                await db.execute("SELECT payload FROM home_state WHERE key=?", (key,))
            ).fetchone()
            if not row:
                raise Rejected("unknown_confirmation")
            try:
                proposal = json.loads(row[0])
            except (TypeError, json.JSONDecodeError) as exc:
                raise Rejected("unknown_confirmation") from exc
            if (
                proposal.get("user_id") != user_id
                or proposal.get("guild_id") != guild_id
                or proposal.get("bot") != bot
            ):
                raise Rejected("unknown_confirmation")
            cursor = await db.execute("DELETE FROM home_state WHERE key=?", (key,))
            if cursor.rowcount != 1:
                raise Rejected("unknown_confirmation")
            await db.commit()
            return proposal

    async def reserve(self, c, d, now=None):
        now = time.time() if now is None else now
        cooldown = max(60, float(d.get("cooldown_seconds", 300)))
        limit = max(1, min(6, int(d.get("max_changes_hour", 6))))
        async with self.db() as db:
            await db.execute("BEGIN IMMEDIATE")
            rows = await (
                await db.execute(
                    "SELECT ts FROM home_audit WHERE device=? AND ts>? "
                    "AND result NOT IN ('permission','expired') ORDER BY ts DESC",
                    (c["device_id"], now - 3600),
                )
            ).fetchall()
            if (
                c["action"] not in {"stop", "pause"}
                and rows
                and (now - rows[0][0] < cooldown or len(rows) >= limit)
            ):
                raise Rejected("cooldown")
            if c["action"] == "print_note":
                count = await (
                    await db.execute(
                        "SELECT COUNT(*) FROM home_audit WHERE device=? AND action='print_note' AND ts>=?",
                        (c["device_id"], int(now // 86400) * 86400),
                    )
                ).fetchone()
                if count[0] >= max(0, min(3, int(d.get("max_jobs_day", 3)))):
                    raise Rejected("daily_print_quota")
            try:
                await db.execute(
                    "INSERT INTO home_audit VALUES(?,?,?,?,?,?,?,?)",
                    (
                        c["request_id"],
                        now,
                        c["bot"],
                        c["user_id"],
                        c["device_id"],
                        c["action"],
                        c["trigger"],
                        "reserved",
                    ),
                )
            except aiosqlite.IntegrityError:
                raise Rejected("replayed_request")
            await db.commit()

    async def result(self, rid, result):
        allowed = {
            "completed",
            "submitted",
            "failed",
            "offline",
            "timeout",
            "cancelled",
            "reserved",
            "permission",
            "expired",
            "provider_error",
        }
        async with self.db() as db:
            await db.execute(
                "UPDATE home_audit SET result=? WHERE request_id=?",
                (result if result in allowed else "failed", rid),
            )
            await db.commit()

    async def rejection(self, c, category, now=None):
        """Record a bounded rejected physical request without message/provider detail."""
        if category not in {"permission", "expired", "offline", "provider_error"}:
            category = "provider_error"
        async with self.db() as db:
            await db.execute(
                "INSERT OR IGNORE INTO home_audit VALUES(?,?,?,?,?,?,?,?)",
                (
                    c["request_id"], time.time() if now is None else now, c["bot"],
                    c["user_id"], c["device_id"], c["action"], c["trigger"],
                    category,
                ),
            )
            await db.commit()

    async def audit(self):
        async with self.db() as db:
            rows = await (
                await db.execute("SELECT * FROM home_audit ORDER BY ts DESC LIMIT 20")
            ).fetchall()
        return [dict(row) for row in rows]

    async def device_status(self, device, policy):
        now = time.time()
        async with self.db() as db:
            rows = await (
                await db.execute(
                    "SELECT ts,action,result FROM home_audit WHERE device=? ORDER BY ts DESC LIMIT 20",
                    (device,),
                )
            ).fetchall()
        cooldown = max(60, float(policy.get("cooldown_seconds", 300)))
        wait = max(0, cooldown - (now - rows[0]["ts"])) if rows else 0
        hourly = [r for r in rows if r["ts"] > now - 3600]
        if len(hourly) >= max(1, min(6, int(policy.get("max_changes_hour", 6)))):
            wait = max(wait, 3600 - (now - hourly[-1]["ts"]))
        return {
            "cooldown_remaining_seconds": int(wait + 0.999),
            "last_result": rows[0]["result"] if rows else "not_tested",
            "last_action_at": rows[0]["ts"] if rows else None,
            "availability": "unverified",
        }  # Online relay is not proof of device reachability.

    async def clean(self):
        async with self.db() as db:
            await db.execute(
                "DELETE FROM home_audit WHERE ts<?",
                (time.time() - self.audit_retention,),
            )
            await db.execute(
                "DELETE FROM home_events WHERE ts<?", (time.time() - 86400,)
            )
            await db.execute(
                "DELETE FROM home_receipts WHERE expires<?", (time.time(),)
            )
            await db.execute(
                "DELETE FROM home_state WHERE key LIKE 'location_last:%' AND json_extract(payload,'$.ts')<?",
                (time.time() - 86400,),
            )
            await db.execute(
                "DELETE FROM home_state WHERE key LIKE 'proposal:%' AND json_extract(payload,'$.expires_at')<?",
                (time.time(),),
            )
            await db.commit()
