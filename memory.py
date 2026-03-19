"""
memory.py — Scaramouche Bot v5
Full persistence: mood, affection, trust, grudges, rivalry, milestones,
inside jokes, absence tracking, anniversaries, message counts, reminders.
"""

import aiosqlite
import time
import json

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
                    trust          INTEGER DEFAULT 0,
                    rival_id       INTEGER DEFAULT NULL,
                    grudge_nick    TEXT    DEFAULT NULL,
                    message_count  INTEGER DEFAULT 0,
                    milestone_last INTEGER DEFAULT 0,
                    first_seen     REAL    DEFAULT 0,
                    last_seen      REAL    DEFAULT 0,
                    last_active    REAL    DEFAULT 0,
                    greeted_today  INTEGER DEFAULT 0,
                    anniversary_last INTEGER DEFAULT 0
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
                CREATE TABLE IF NOT EXISTS inside_jokes (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id     INTEGER,
                    joke        TEXT,
                    ts          REAL
                );
                CREATE TABLE IF NOT EXISTS lore_cooldown (
                    channel_id  INTEGER PRIMARY KEY,
                    last_sent   REAL DEFAULT 0
                );
            """)
            migrations = [
                ("allow_dms",        "INTEGER DEFAULT 1"),
                ("mood",             "INTEGER DEFAULT 0"),
                ("affection",        "INTEGER DEFAULT 0"),
                ("trust",            "INTEGER DEFAULT 0"),
                ("rival_id",         "INTEGER DEFAULT NULL"),
                ("grudge_nick",      "TEXT DEFAULT NULL"),
                ("message_count",    "INTEGER DEFAULT 0"),
                ("milestone_last",   "INTEGER DEFAULT 0"),
                ("first_seen",       "REAL DEFAULT 0"),
                ("last_active",      "REAL DEFAULT 0"),
                ("greeted_today",    "INTEGER DEFAULT 0"),
                ("anniversary_last", "INTEGER DEFAULT 0"),
            ]
            for col, default in migrations:
                try:
                    await db.execute(f"ALTER TABLE users ADD COLUMN {col} {default}")
                except Exception:
                    pass
            await db.commit()

    # ── Users ────────────────────────────────────────────────────────────────
    async def upsert_user(self, user_id: int, username: str, display_name: str):
        now = time.time()
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT first_seen FROM users WHERE user_id=?", (user_id,)) as cur:
                row = await cur.fetchone()
            if row:
                await db.execute("""
                    UPDATE users SET username=?, display_name=?, last_seen=?, last_active=?
                    WHERE user_id=?
                """, (username, display_name, now, now, user_id))
            else:
                await db.execute("""
                    INSERT INTO users (user_id, username, display_name, last_seen, last_active, first_seen)
                    VALUES (?,?,?,?,?,?)
                """, (user_id, username, display_name, now, now, now))
            await db.commit()

    async def get_user(self, user_id: int) -> dict | None:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("""
                SELECT user_id, username, display_name, romance_mode, nsfw_mode,
                       proactive, allow_dms, mood, affection, trust, rival_id,
                       grudge_nick, message_count, milestone_last, first_seen,
                       last_seen, last_active, greeted_today, anniversary_last
                FROM users WHERE user_id=?
            """, (user_id,)) as cur:
                row = await cur.fetchone()
                if not row:
                    return None
                return {
                    "user_id":          row[0],  "username":       row[1],
                    "display_name":     row[2],  "romance_mode":   bool(row[3]),
                    "nsfw_mode":        bool(row[4]), "proactive":  bool(row[5]),
                    "allow_dms":        bool(row[6]), "mood":       row[7] or 0,
                    "affection":        row[8] or 0,  "trust":      row[9] or 0,
                    "rival_id":         row[10], "grudge_nick":    row[11],
                    "message_count":    row[12] or 0,
                    "milestone_last":   row[13] or 0,
                    "first_seen":       row[14] or 0,
                    "last_seen":        row[15] or 0,
                    "last_active":      row[16] or 0,
                    "greeted_today":    bool(row[17]),
                    "anniversary_last": row[18] or 0,
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
                "UPDATE users SET mood=MAX(-10,MIN(10,mood+?)) WHERE user_id=?",
                (delta, user_id))
            await db.commit()

    async def get_mood(self, user_id: int) -> int:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT mood FROM users WHERE user_id=?", (user_id,)) as cur:
                row = await cur.fetchone()
                return row[0] if row else 0

    # ── Affection ─────────────────────────────────────────────────────────────
    async def update_affection(self, user_id: int, delta: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE users SET affection=MAX(0,MIN(100,affection+?)) WHERE user_id=?",
                (delta, user_id))
            await db.commit()

    # ── Trust ─────────────────────────────────────────────────────────────────
    async def update_trust(self, user_id: int, delta: int):
        """Trust: 0-100. Hard to earn, easy to lose. Unlocks rare personal lore reveals."""
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE users SET trust=MAX(0,MIN(100,trust+?)) WHERE user_id=?",
                (delta, user_id))
            await db.commit()

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
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE users SET message_count=message_count+1 WHERE user_id=?", (user_id,))
            await db.commit()
            async with db.execute(
                "SELECT message_count, milestone_last FROM users WHERE user_id=?", (user_id,)
            ) as cur:
                row = await cur.fetchone()
                if not row:
                    return 1, False
                count, last = row
        for m in [50, 100, 250, 500, 1000]:
            if count >= m and last < m:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "UPDATE users SET milestone_last=? WHERE user_id=?", (m, user_id))
                    await db.commit()
                return count, True
        return count, False

    async def get_top_users(self, limit: int = 5) -> list[dict]:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT user_id, display_name, message_count FROM users ORDER BY message_count DESC LIMIT ?",
                (limit,)
            ) as cur:
                rows = await cur.fetchall()
        return [{"user_id": r[0], "display_name": r[1], "message_count": r[2]} for r in rows]

    # ── Greetings ─────────────────────────────────────────────────────────────
    async def should_greet(self, user_id: int, is_morning: bool) -> bool:
        """Return True if user hasn't been greeted today and was active yesterday."""
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT greeted_today, last_active FROM users WHERE user_id=?", (user_id,)
            ) as cur:
                row = await cur.fetchone()
                if not row:
                    return False
                greeted, last_active = row
                if greeted:
                    return False
                # Was active in the last 48 hours
                return (time.time() - (last_active or 0)) < 172800

    async def mark_greeted(self, user_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE users SET greeted_today=1 WHERE user_id=?", (user_id,))
            await db.commit()

    async def reset_daily_greetings(self):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE users SET greeted_today=0")
            await db.commit()

    # ── Anniversary ───────────────────────────────────────────────────────────
    async def check_anniversary(self, user_id: int) -> bool:
        """Return True if today is roughly the anniversary of first contact."""
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT first_seen, anniversary_last, message_count FROM users WHERE user_id=?",
                (user_id,)
            ) as cur:
                row = await cur.fetchone()
                if not row:
                    return False
                first, ann_last, count = row
                if count < 10 or first == 0:
                    return False
                now  = time.time()
                days = (now - first) / 86400
                if days < 30:
                    return False
                # Check if we're within 2 days of the anniversary and haven't sent this year
                year_secs  = 365.25 * 86400
                years_past = int(days / 365.25)
                if years_past == 0:
                    return False
                anniversary_ts = first + years_past * year_secs
                near = abs(now - anniversary_ts) < 172800  # within 2 days
                sent_this_year = (now - (ann_last or 0)) < year_secs * 0.9
                return near and not sent_this_year

    async def mark_anniversary(self, user_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE users SET anniversary_last=? WHERE user_id=?", (time.time(), user_id))
            await db.commit()

    # ── Absence detection ─────────────────────────────────────────────────────
    async def get_absent_romance_users(self, days: float = 3.0) -> list[dict]:
        """Return romance users who haven't been seen in `days` days."""
        cutoff = time.time() - days * 86400
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("""
                SELECT user_id, display_name, last_active,
                       (julianday('now') - julianday(datetime(last_active,'unixepoch'))) as days_gone
                FROM users
                WHERE romance_mode=1 AND allow_dms=1
                  AND last_active > 0 AND last_active < ?
                  AND message_count > 5
            """, (cutoff,)) as cur:
                rows = await cur.fetchall()
        return [
            {"user_id": r[0], "display_name": r[1],
             "last_active": r[2], "days_gone": round(r[3] or 0, 1)}
            for r in rows
        ]

    # ── Inside jokes ─────────────────────────────────────────────────────────
    async def add_inside_joke(self, user_id: int, joke: str):
        async with aiosqlite.connect(DB_PATH) as db:
            # Keep max 10 jokes per user
            async with db.execute(
                "SELECT COUNT(*) FROM inside_jokes WHERE user_id=?", (user_id,)
            ) as cur:
                count = (await cur.fetchone())[0]
            if count >= 10:
                await db.execute("""
                    DELETE FROM inside_jokes WHERE id=(
                        SELECT id FROM inside_jokes WHERE user_id=? ORDER BY ts ASC LIMIT 1
                    )
                """, (user_id,))
            await db.execute(
                "INSERT INTO inside_jokes (user_id, joke, ts) VALUES (?,?,?)",
                (user_id, joke[:200], time.time()))
            await db.commit()

    async def get_random_inside_joke(self, user_id: int) -> str | None:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT joke FROM inside_jokes WHERE user_id=? ORDER BY RANDOM() LIMIT 1",
                (user_id,)
            ) as cur:
                row = await cur.fetchone()
                return row[0] if row else None

    async def get_joke_count(self, user_id: int) -> int:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM inside_jokes WHERE user_id=?", (user_id,)
            ) as cur:
                return (await cur.fetchone())[0]

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
                WHERE user_id=? AND role='user' AND ts<?
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
                "INSERT INTO messages (user_id,channel_id,role,content,ts) VALUES (?,?,?,?,?)",
                (user_id, channel_id, role, content, time.time()))
            await db.commit()

    async def reset_user(self, user_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM messages WHERE user_id=?", (user_id,))
            await db.execute("DELETE FROM inside_jokes WHERE user_id=?", (user_id,))
            await db.execute("""
                UPDATE users SET mood=0, affection=0, trust=0, rival_id=NULL,
                grudge_nick=NULL, message_count=0, milestone_last=0
                WHERE user_id=?
            """, (user_id,))
            await db.commit()

    # ── Reminders ─────────────────────────────────────────────────────────────
    async def add_reminder(self, user_id: int, channel_id: int, reminder: str, due_ts: float):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO reminders (user_id,channel_id,reminder,due_ts) VALUES (?,?,?,?)",
                (user_id, channel_id, reminder, due_ts))
            await db.commit()

    async def get_due_reminders(self) -> list[dict]:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("""
                SELECT id,user_id,channel_id,reminder FROM reminders
                WHERE due_ts<=? AND done=0
            """, (time.time(),)) as cur:
                rows = await cur.fetchall()
            if rows:
                ids = [r[0] for r in rows]
                await db.execute(
                    f"UPDATE reminders SET done=1 WHERE id IN ({','.join('?'*len(ids))})", ids)
                await db.commit()
        return [{"id": r[0], "user_id": r[1], "channel_id": r[2], "reminder": r[3]} for r in rows]

    # ── Channels ──────────────────────────────────────────────────────────────
    async def track_channel(self, channel_id: int, guild_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO channels (channel_id,guild_id,last_active) VALUES (?,?,?)
                ON CONFLICT(channel_id) DO UPDATE SET last_active=excluded.last_active
            """, (channel_id, guild_id, time.time()))
            await db.commit()

    async def get_active_channels(self) -> list[tuple]:
        cutoff = time.time() - 86400 * 3
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT channel_id,guild_id FROM channels WHERE last_active>?", (cutoff,)
            ) as cur:
                return await cur.fetchall()

    # ── Proactive cooldown ────────────────────────────────────────────────────
    async def can_proactive(self, channel_id: int, cooldown: int = 3600) -> bool:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT last_sent FROM proactive_cooldown WHERE channel_id=?", (channel_id,)
            ) as cur:
                row = await cur.fetchone()
                return not row or (time.time() - row[0]) >= cooldown

    async def set_proactive_sent(self, channel_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO proactive_cooldown (channel_id,last_sent) VALUES (?,?)
                ON CONFLICT(channel_id) DO UPDATE SET last_sent=excluded.last_sent
            """, (channel_id, time.time()))
            await db.commit()

    # ── Lore cooldown ─────────────────────────────────────────────────────────
    async def can_lore_drop(self, channel_id: int, cooldown: int = 21600) -> bool:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT last_sent FROM lore_cooldown WHERE channel_id=?", (channel_id,)
            ) as cur:
                row = await cur.fetchone()
                return not row or (time.time() - row[0]) >= cooldown

    async def set_lore_sent(self, channel_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO lore_cooldown (channel_id,last_sent) VALUES (?,?)
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
                return not row or (time.time() - row[0]) >= cooldown

    async def set_dm_sent(self, user_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO dm_cooldown (user_id,last_sent) VALUES (?,?)
                ON CONFLICT(user_id) DO UPDATE SET last_sent=excluded.last_sent
            """, (user_id, time.time()))
            await db.commit()

    async def get_dm_eligible_users(self) -> list[dict]:
        cutoff = time.time() - 86400 * 7
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("""
                SELECT u.user_id, u.display_name, u.romance_mode, u.nsfw_mode
                FROM users u
                WHERE u.allow_dms=1 AND u.last_seen>?
                  AND EXISTS (SELECT 1 FROM messages m WHERE m.user_id=u.user_id)
            """, (cutoff,)) as cur:
                rows = await cur.fetchall()
        return [
            {"user_id": r[0], "display_name": r[1],
             "romance_mode": bool(r[2]), "nsfw_mode": bool(r[3])}
            for r in rows
        ]

    # ── Stats ─────────────────────────────────────────────────────────────────
    async def get_stats(self, user_id: int) -> dict:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("""
                SELECT message_count, mood, affection, trust, first_seen,
                       grudge_nick, romance_mode, nsfw_mode
                FROM users WHERE user_id=?
            """, (user_id,)) as cur:
                row = await cur.fetchone()
                if not row:
                    return {}
            async with db.execute(
                "SELECT COUNT(*) FROM inside_jokes WHERE user_id=?", (user_id,)
            ) as cur:
                joke_count = (await cur.fetchone())[0]
        return {
            "message_count": row[0] or 0, "mood": row[1] or 0,
            "affection": row[2] or 0,     "trust": row[3] or 0,
            "first_seen": row[4] or 0,    "grudge_nick": row[5],
            "romance_mode": bool(row[6]), "nsfw_mode": bool(row[7]),
            "joke_count": joke_count,
        }
