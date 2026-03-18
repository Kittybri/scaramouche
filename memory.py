"""
memory.py — Persistent memory for Scaramouche bot (SQLite)
Stores users, conversation history, modes, active channels, and DM cooldowns.
"""

import aiosqlite
import time

DB_PATH = "scaramouche.db"


class Memory:
    async def init(self):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.executescript("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id      INTEGER PRIMARY KEY,
                    username     TEXT,
                    display_name TEXT,
                    romance_mode INTEGER DEFAULT 0,
                    nsfw_mode    INTEGER DEFAULT 0,
                    proactive    INTEGER DEFAULT 1,
                    allow_dms    INTEGER DEFAULT 1,
                    last_seen    REAL    DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS messages (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id     INTEGER,
                    channel_id  INTEGER,
                    role        TEXT,
                    content     TEXT,
                    ts          REAL
                );
                CREATE TABLE IF NOT EXISTS channels (
                    channel_id  INTEGER PRIMARY KEY,
                    guild_id    INTEGER,
                    last_active REAL
                );
                CREATE TABLE IF NOT EXISTS proactive_cooldown (
                    channel_id  INTEGER PRIMARY KEY,
                    last_sent   REAL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS dm_cooldown (
                    user_id   INTEGER PRIMARY KEY,
                    last_sent REAL DEFAULT 0
                );
            """)
            # Safe migration: add new columns if they don't exist yet
            try:
                await db.execute("ALTER TABLE users ADD COLUMN allow_dms INTEGER DEFAULT 1")
            except Exception:
                pass
            await db.commit()

    # ── Users ────────────────────────────────────────────────────────────────
    async def upsert_user(self, user_id: int, username: str, display_name: str):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO users (user_id, username, display_name, last_seen)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username     = excluded.username,
                    display_name = excluded.display_name,
                    last_seen    = excluded.last_seen
            """, (user_id, username, display_name, time.time()))
            await db.commit()

    async def get_user(self, user_id: int) -> dict | None:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT user_id, username, display_name, romance_mode, nsfw_mode, proactive, allow_dms "
                "FROM users WHERE user_id=?", (user_id,)
            ) as cur:
                row = await cur.fetchone()
                if not row:
                    return None
                return {
                    "user_id":      row[0],
                    "username":     row[1],
                    "display_name": row[2],
                    "romance_mode": bool(row[3]),
                    "nsfw_mode":    bool(row[4]),
                    "proactive":    bool(row[5]),
                    "allow_dms":    bool(row[6]),
                }

    async def set_mode(self, user_id: int, field: str, value: bool):
        allowed = {"nsfw_mode", "romance_mode", "proactive", "allow_dms"}
        if field not in allowed:
            raise ValueError(f"Unknown field: {field}")
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                f"UPDATE users SET {field}=? WHERE user_id=?",
                (int(value), user_id)
            )
            await db.commit()

    # ── Conversation history ──────────────────────────────────────────────────
    async def get_history(self, user_id: int, channel_id: int, limit: int = 22) -> list[dict]:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("""
                SELECT role, content FROM messages
                WHERE user_id=? AND channel_id=?
                ORDER BY ts DESC LIMIT ?
            """, (user_id, channel_id, limit)) as cur:
                rows = await cur.fetchall()
        return [{"role": r[0], "content": r[1]} for r in reversed(rows)]

    async def add_message(self, user_id: int, channel_id: int, role: str, content: str):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO messages (user_id, channel_id, role, content, ts) "
                "VALUES (?, ?, ?, ?, ?)",
                (user_id, channel_id, role, content, time.time())
            )
            await db.commit()

    async def reset_user(self, user_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM messages WHERE user_id=?", (user_id,))
            await db.commit()

    # ── Channels ──────────────────────────────────────────────────────────────
    async def track_channel(self, channel_id: int, guild_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO channels (channel_id, guild_id, last_active)
                VALUES (?, ?, ?)
                ON CONFLICT(channel_id) DO UPDATE SET last_active=excluded.last_active
            """, (channel_id, guild_id, time.time()))
            await db.commit()

    async def get_active_channels(self) -> list[tuple]:
        cutoff = time.time() - 86400 * 3   # Active in last 3 days
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT channel_id, guild_id FROM channels WHERE last_active > ?", (cutoff,)
            ) as cur:
                return await cur.fetchall()

    # ── Proactive cooldown ────────────────────────────────────────────────────
    async def can_proactive(self, channel_id: int, cooldown: int = 3600) -> bool:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT last_sent FROM proactive_cooldown WHERE channel_id=?", (channel_id,)
            ) as cur:
                row = await cur.fetchone()
                if not row:
                    return True
                return (time.time() - row[0]) >= cooldown

    async def set_proactive_sent(self, channel_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO proactive_cooldown (channel_id, last_sent)
                VALUES (?, ?)
                ON CONFLICT(channel_id) DO UPDATE SET last_sent=excluded.last_sent
            """, (channel_id, time.time()))
            await db.commit()

    # ── Romance helpers ───────────────────────────────────────────────────────
    async def get_romance_users(self) -> list[int]:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT user_id FROM users WHERE romance_mode=1 AND proactive=1"
            ) as cur:
                return [r[0] for r in await cur.fetchall()]

    async def get_user_last_channel(self, user_id: int) -> int | None:
        """Find the most recent channel this user spoke in."""
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT channel_id FROM messages WHERE user_id=? ORDER BY ts DESC LIMIT 1",
                (user_id,)
            ) as cur:
                row = await cur.fetchone()
                return row[0] if row else None

    # ── Voluntary DM cooldown ─────────────────────────────────────────────────
    async def can_dm_user(self, user_id: int, cooldown: int = 7200) -> bool:
        """Return True if enough time has passed since last voluntary DM to this user."""
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT last_sent FROM dm_cooldown WHERE user_id=?", (user_id,)
            ) as cur:
                row = await cur.fetchone()
                if not row:
                    return True
                return (time.time() - row[0]) >= cooldown

    async def set_dm_sent(self, user_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO dm_cooldown (user_id, last_sent)
                VALUES (?, ?)
                ON CONFLICT(user_id) DO UPDATE SET last_sent=excluded.last_sent
            """, (user_id, time.time()))
            await db.commit()

    async def get_dm_eligible_users(self) -> list[dict]:
        """
        Return users who:
          - have allow_dms = 1
          - have spoken before (exist in messages table)
          - are within the last 7 days of activity
        """
        cutoff = time.time() - 86400 * 7
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("""
                SELECT u.user_id, u.display_name, u.romance_mode, u.nsfw_mode
                FROM users u
                WHERE u.allow_dms = 1
                  AND u.last_seen > ?
                  AND EXISTS (
                      SELECT 1 FROM messages m WHERE m.user_id = u.user_id
                  )
            """, (cutoff,)) as cur:
                rows = await cur.fetchall()
        return [
            {
                "user_id":      r[0],
                "display_name": r[1],
                "romance_mode": bool(r[2]),
                "nsfw_mode":    bool(r[3]),
            }
            for r in rows
        ]
