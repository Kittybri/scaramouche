"""Restore the shared story archive, not a second persistent-world scheduler."""
from __future__ import annotations
import hashlib
import time
import aiosqlite
import discord
from restored_lifecycle import WORK


async def migrate(db,_name):
    # Match Wanderer's current shared schema; never replace populated tables.
    await db.execute('''CREATE TABLE IF NOT EXISTS hidden_achievements(
        scope TEXT,achievement_key TEXT,note TEXT DEFAULT NULL,unlocked_ts REAL DEFAULT 0,
        PRIMARY KEY(scope,achievement_key))''')
    await db.execute('''CREATE TABLE IF NOT EXISTS shared_world_entities(
        entity_key TEXT PRIMARY KEY,entity_type TEXT,name TEXT,summary TEXT DEFAULT NULL,
        status TEXT DEFAULT NULL,channel_id INTEGER DEFAULT 0,owner_user_id INTEGER DEFAULT 0,
        updated_by TEXT DEFAULT NULL,ts REAL DEFAULT 0,updated_ts REAL DEFAULT 0)''')
    await db.execute('''CREATE TABLE IF NOT EXISTS shared_world_cases(
        case_key TEXT PRIMARY KEY,channel_id INTEGER DEFAULT 0,case_type TEXT,title TEXT,
        status TEXT DEFAULT 'open',summary TEXT DEFAULT NULL,enemy TEXT DEFAULT NULL,
        updated_by TEXT DEFAULT NULL,opened_ts REAL DEFAULT 0,updated_ts REAL DEFAULT 0)''')


class WorldArchive:
    def __init__(self,bot,mem,guard):
        self.bot,self.mem,self.guard=bot,mem,guard

    async def read(self,uid,channel,kind):
        await self.guard(uid)
        async with aiosqlite.connect(self.mem.shared_db_path) as db:
            if kind=='achievements':
                rows=await (await db.execute('SELECT achievement_key,note FROM hidden_achievements WHERE scope=? ORDER BY unlocked_ts DESC LIMIT 12',
                                             (f'user:{uid}',))).fetchall()
                text='Achievement gallery:\n'+('\n'.join(f'- {a}: {b or ""}' for a,b in rows) or 'Nothing unlocked yet. Try being more interesting.')
            elif kind=='cases':
                rows=await (await db.execute('SELECT title,status,summary FROM shared_world_cases WHERE channel_id=? ORDER BY updated_ts DESC LIMIT 8',(channel,))).fetchall()
                # Current duo stories are already durable. Read that backend
                # too, rather than resurrecting a duplicate case writer.
                stories=await (await db.execute('SELECT topic,status,outcome FROM duo_story_log WHERE channel_id=? ORDER BY updated_ts DESC LIMIT 8',(channel,))).fetchall()
                rows=list(dict.fromkeys([*stories,*rows]))[:8]
                text='Shared cases:\n'+('\n'.join(f'- {a} [{b}] — {(c or "")[:120]}' for a,b,c in rows) or 'None open enough to matter.')
            else:
                rows=await (await db.execute('SELECT entity_type,name,summary FROM shared_world_entities WHERE channel_id=? ORDER BY updated_ts DESC LIMIT 8',(channel,))).fetchall()
                text='Shared world:\n'+('\n'.join(f'- {a}: {b} — {(c or "")[:120]}' for a,b,c in rows) or 'Nothing persistent yet.')
        await self.guard(uid)
        return text[:1900]

    async def add(self,uid,channel,kind,payload):
        await self.guard(uid,payload)
        if kind not in {'enemy','ally','gift','place','prop','faction','figure','case'}:
            raise ValueError('Use enemy, ally, gift, place, prop, faction, figure, or case.')
        name,_,summary=payload.partition('|')
        name,summary=name.strip()[:80],summary.strip()[:700]
        if not name: raise ValueError('Give the entry a name.')
        # The old global type/name key allowed one guild/user to overwrite another.
        key=f'{channel}:{uid}:{kind}:'+hashlib.sha256(name.casefold().encode()).hexdigest()[:24]
        async with aiosqlite.connect(self.mem.shared_db_path,timeout=15) as db:
            # ATTACH lets this write atomically observe the local deletion ledger,
            # rather than trusting a check performed before acquiring the write lock.
            await db.execute('ATTACH DATABASE ? AS local_privacy',(self.mem.db_path,))
            await db.execute('BEGIN IMMEDIATE')
            pending=await (await db.execute("SELECT 1 FROM local_privacy.privacy_deletion_jobs WHERE user_id=? AND status IN ('PENDING','IN_PROGRESS','RETRYABLE')",(uid,))).fetchone()
            if pending: raise PermissionError('Your privacy reset is still finishing.')
            await db.execute('''INSERT INTO shared_world_entities VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(entity_key) DO UPDATE SET summary=excluded.summary,updated_ts=excluded.updated_ts''',
                (key,kind,name,summary,'remembered',channel,uid,'scaramouche',time.time(),time.time()))
            # This is the original remembered-artifact achievement, earned by
            # explicit user-owned artifact registration, not a guessed memory.
            if kind in {'gift','prop'}:
                await db.execute('INSERT OR IGNORE INTO hidden_achievements VALUES(?,?,?,?)',
                    (f'user:{uid}','remembered_artifact','A personal artifact was remembered.',time.time()))
            await db.commit()
        return f'Fine. I filed {name} under {kind}.'

    async def prefix_read(self,ctx,kind):
        try:
            text=await self.read(ctx.author.id,ctx.channel.id,kind)
            if kind=='achievements' and ctx.guild:
                await ctx.author.send(text,allowed_mentions=discord.AllowedMentions.none())
                await ctx.send('Your achievement gallery is in your DMs.')
            else: await ctx.send(text,allowed_mentions=discord.AllowedMentions.none())
        except discord.Forbidden: await ctx.send('Use /dashboard achievements for a private gallery.')
        except (ValueError,PermissionError) as exc: await ctx.send(str(exc))

    def install(self):
        for name in ('world','cases','achievements'):
            def make(kind):
                async def callback(ctx): await self.prefix_read(ctx,kind)
                return callback
            self.bot.command(name=name,aliases=['gallery'] if name=='achievements' else [])(make(name))
        @self.bot.command(name='worldadd')
        async def worldadd(ctx,entity_type: str='',*,payload: str=''):
            async with WORK.track(ctx.author.id):
                try: text=await self.add(ctx.author.id,ctx.channel.id,entity_type,payload)
                except (ValueError,PermissionError) as exc: text=str(exc)
                await ctx.send(text,allowed_mentions=discord.AllowedMentions.none())
        return self
