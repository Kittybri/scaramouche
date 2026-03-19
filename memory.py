"""
memory.py — Persistent memory for Scaramouche bot (SQLite)
Stores users, conversation history, modes, moods, affection,
grudges, rivals, reminders, message counts, and cooldowns.
"""

import aiosqlite
import time

DB_PATH = "scaramouche.db"


class Memory:
    async def init(self):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.executescript("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id        INTEGER PRIMARY KEY,
                    username       TEXT,
                    display_name   TEXT,
                    romance_mode   INTEGER DEFAULT 0,
                    nsfw_mode      INTEGER DEFAULT 0,
                    proactive      INTEGER DEFAULT 1,
                    allow_dms      INTEGER DEFAULT 1,
                    mood           INTEGER DEFAULT 0,
                    affection      INTEGER DEFAULT 0,
                    rival_id       INTEGER DEFAULT NULL,
                    grudge_nick    TEXT    DEFAULT NULL,
                    message_count  INTEGER DEFAULT 0,
                    milestone_last INTEGER DEFAULT 0,
                    last_seen      REAL    DEFAULT 0
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
                CREATE TABLE IF NOT EXISTS reminders (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id     INTEGER,
                    channel_id  INTEGER,
                    reminder    TEXT,
                    due_ts      REAL,
                    done        INTEGER DEFAULT 0
                );
            """)
            # Safe migrations
            for col, default in [
                ("allow_dms",      "INTEGER DEFAULT 1"),
                ("mood",           "INTEGER DEFAULT 0"),
                ("affection",      "INTEGER DEFAULT 0"),
                ("rival_id",       "INTEGER DEFAULT NULL"),
                ("grudge_nick",    "TEXT DEFAULT NULL"),
                ("message_count",  "INTEGER DEFAULT 0"),
                ("milestone_last", "INTEGER DEFAULT 0"),
            ]:
                try:
                    await db.execute(f"ALTER TABLE users ADD COLUMN {col} {default}")
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
                "SELECT user_id, username, display_name, romance_mode, nsfw_mode, "
                "proactive, allow_dms, mood, affection, rival_id, grudge_nick, "
                "message_count, milestone_last "
                "FROM users WHERE user_id=?", (user_id,)
            ) as cur:
                row = await cur.fetchone()
                if not row:
                    return None
                return {
                    "user_id":        row[0],
                    "username":       row[1],
                    "display_name":   row[2],
                    "romance_mode":   bool(row[3]),
                    "nsfw_mode":      bool(row[4]),
                    "proactive":      bool(row[5]),
                    "allow_dms":      bool(row[6]),
                    "mood":           row[7] or 0,
                    "affection":      row[8] or 0,
                    "rival_id":       row[9],
                    "grudge_nick":    row[10],
                    "message_count":  row[11] or 0,
                    "milestone_last": row[12] or 0,
                }

    async def set_mode(self, user_id: int, field: str, value: bool):
        allowed = {"nsfw_mode", "romance_mode", "proactive", "allow_dms"}
        if field not in allowed:
            raise ValueError(f"Unknown field: {field}")
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(f"UPDATE users SET {field}=? WHERE user_id=?", (int(value), user_id))
            await db.commit()

    # ── Mood ─────────────────────────────────────────────────────────────────
    async def update_mood(self, user_id: int, delta: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE users SET mood = MAX(-10, MIN(10, mood + ?)) WHERE user_id=?",
                (delta, user_id)
            )
            await db.commit()

    async def get_mood(self, user_id: int) -> int:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT mood FROM users WHERE user_id=?", (user_id,)) as cur:
                row = await cur.fetchone()
                return row[0] if row else 0

    # ── Affection ─────────────────────────────────────────────────────────────
    async def update_affection(self, user_id: int, delta: int):
        """Affection: 0–100. Unlocks hidden softer behavior at high levels."""
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE users SET affection = MAX(0, MIN(100, affection + ?)) WHERE user_id=?",
                (delta, user_id)
            )
            await db.commit()

    async def get_affection(self, user_id: int) -> int:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT affection FROM users WHERE user_id=?", (user_id,)) as cur:
                row = await cur.fetchone()
                return row[0] if row else 0

    # ── Grudge ────────────────────────────────────────────────────────────────
    async def set_grudge_nick(self, user_id: int, nick: str | None):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE users SET grudge_nick=? WHERE user_id=?", (nick, user_id))
            await db.commit()

    # ── Rivalry ───────────────────────────────────────────────────────────────
    async def set_rival(self, user_id: int, rival_id: int | None):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE users SET rival_id=? WHERE user_id=?", (rival_id, user_id))
            await db.commit()

    # ── Message count & milestones ────────────────────────────────────────────
    async def increment_message_count(self, user_id: int) -> tuple[int, bool]:
        """Increment message count. Returns (new_count, milestone_hit)."""
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE users SET message_count = message_count + 1 WHERE user_id=?", (user_id,)
            )
            await db.commit()
            async with db.execute(
                "SELECT message_count, milestone_last FROM users WHERE user_id=?", (user_id,)
            ) as cur:
                row = await cur.fetchone()
                if not row:
                    return 1, False
                count, last = row[0], row[1]

        milestones = [50, 100, 250, 500, 1000]
        for m in milestones:
            if count >= m and last < m:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "UPDATE users SET milestone_last=? WHERE user_id=?", (m, user_id)
                    )
                    await db.commit()
                return count, True
        return count, False

    async def get_top_users(self, limit: int = 5) -> list[dict]:
        """Get users sorted by message count — for 'favorites' tracking."""
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT user_id, display_name, message_count FROM users "
                "ORDER BY message_count DESC LIMIT ?", (limit,)
            ) as cur:
                rows = await cur.fetchall()
        return [{"user_id": r[0], "display_name": r[1], "message_count": r[2]} for r in rows]

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

    async def get_random_old_message(self, user_id: int) -> str | None:
        cutoff = time.time() - 86400 * 2
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("""
                SELECT content FROM messages
                WHERE user_id=? AND role='user' AND ts < ?
                ORDER BY RANDOM() LIMIT 1
            """, (user_id, cutoff)) as cur:
                row = await cur.fetchone()
                return row[0] if row else None

    async def get_recent_messages(self, user_id: int, limit: int = 10) -> list[str]:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("""
                SELECT content FROM messages WHERE user_id=? AND role='user'
                ORDER BY ts DESC LIMIT ?
            """, (user_id, limit)) as cur:
                rows = await cur.fetchall()
        return [r[0] for r in rows]

    async def add_message(self, user_id: int, channel_id: int, role: str, content: str):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO messages (user_id, channel_id, role, content, ts) VALUES (?,?,?,?,?)",
                (user_id, channel_id, role, content, time.time())
            )
            await db.commit()

    async def reset_user(self, user_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM messages WHERE user_id=?", (user_id,))
            await db.execute(
                "UPDATE users SET mood=0, affection=0, rival_id=NULL, grudge_nick=NULL, "
                "message_count=0, milestone_last=0 WHERE user_id=?", (user_id,)
            )
            await db.commit()

    # ── Reminders ─────────────────────────────────────────────────────────────
    async def add_reminder(self, user_id: int, channel_id: int, reminder: str, due_ts: float):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO reminders (user_id, channel_id, reminder, due_ts) VALUES (?,?,?,?)",
                (user_id, channel_id, reminder, due_ts)
            )
            await db.commit()

    async def get_due_reminders(self) -> list[dict]:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("""
                SELECT id, user_id, channel_id, reminder FROM reminders
                WHERE due_ts <= ? AND done = 0
            """, (time.time(),)) as cur:
                rows = await cur.fetchall()
            if rows:
                ids = [r[0] for r in rows]
                await db.execute(
                    f"UPDATE reminders SET done=1 WHERE id IN ({','.join('?'*len(ids))})", ids
                )
                await db.commit()
        return [{"id": r[0], "user_id": r[1], "channel_id": r[2], "reminder": r[3]} for r in rows]

    # ── Channels ──────────────────────────────────────────────────────────────
    async def track_channel(self, channel_id: int, guild_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO channels (channel_id, guild_id, last_active) VALUES (?,?,?)
                ON CONFLICT(channel_id) DO UPDATE SET last_active=excluded.last_active
            """, (channel_id, guild_id, time.time()))
            await db.commit()

    async def get_active_channels(self) -> list[tuple]:
        cutoff = time.time() - 86400 * 3
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
                INSERT INTO proactive_cooldown (channel_id, last_sent) VALUES (?,?)
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
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT channel_id FROM messages WHERE user_id=? ORDER BY ts DESC LIMIT 1",
                (user_id,)
            ) as cur:
                row = await cur.fetchone()
                return row[0] if row else None

    # ── DM cooldown ───────────────────────────────────────────────────────────
    async def can_dm_user(self, user_id: int, cooldown: int = 7200) -> bool:
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
                INSERT INTO dm_cooldown (user_id, last_sent) VALUES (?,?)
                ON CONFLICT(user_id) DO UPDATE SET last_sent=excluded.last_sent
            """, (user_id, time.time()))
            await db.commit()

    async def get_dm_eligible_users(self) -> list[dict]:
        cutoff = time.time() - 86400 * 7
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("""
                SELECT u.user_id, u.display_name, u.romance_mode, u.nsfw_mode
                FROM users u
                WHERE u.allow_dms = 1 AND u.last_seen > ?
                  AND EXISTS (SELECT 1 FROM messages m WHERE m.user_id = u.user_id)
            """, (cutoff,)) as cur:
                rows = await cur.fetchall()
        return [
            {"user_id": r[0], "display_name": r[1], "romance_mode": bool(r[2]), "nsfw_mode": bool(r[3])}
            for r in rows
        ]
