"""Additive, user/guild-scoped persistence for restored character features.

Every asynchronous campaign update is compare-and-swap against an opaque revision.
Deletion removes the row: a late generator can never INSERT it back. No credentials
or shared Connected Accounts tables are accessed here.
"""
from __future__ import annotations

import json
import uuid
from contextlib import asynccontextmanager
import aiosqlite


async def migrate(db, _bot_name):
    await db.execute('''CREATE TABLE IF NOT EXISTS restored_campaigns(
        user_id INTEGER NOT NULL,guild_id INTEGER NOT NULL,revision TEXT NOT NULL,
        state_json TEXT NOT NULL,PRIMARY KEY(user_id,guild_id))''')
    await db.execute('''CREATE TABLE IF NOT EXISTS restored_medals(
        user_id INTEGER NOT NULL,guild_id INTEGER NOT NULL,completions INTEGER NOT NULL,
        best_points INTEGER NOT NULL,PRIMARY KEY(user_id,guild_id))''')
    # Migrate only existing local progress, privately. Never import remote backups
    # or guess which guild owns a historical unscoped campaign.
    tables = {r[0] for r in await (await db.execute("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()}
    if 'rpg_state' in tables:
        rows = await (await db.execute('SELECT * FROM rpg_state')).fetchall()
        columns = [r[1] for r in await (await db.execute('PRAGMA table_info(rpg_state)')).fetchall()]
        for row in rows:
            state = dict(zip(columns,row))
            uid = state.pop('user_id')
            for name, default in [('bosses_beaten','[]'),('scenario_data','{}')]:
                state[name] = json.loads(state.get(name) or default)
            # Historical round zero means a new boss, eleven means the dice
            # already ran. Never replay a paid choice or award a second roll.
            if state.get('active') and state.get('world_type') and state.get('char_type'):
                state['current_round'] = max(1,int(state.get('current_round',0)))
                if state['current_round'] >= 11:
                    state['stage'] = 'boss'
                elif state['current_round'] == 10:
                    state['stage'] = 'dice'
            await db.execute('INSERT OR IGNORE INTO restored_campaigns VALUES(?,?,?,?)',
                             (uid,0,uuid.uuid4().hex,json.dumps(state)))
    if 'rpg_medals' in tables:
        await db.execute('''INSERT OR IGNORE INTO restored_medals
            SELECT user_id,0,completions,best_points FROM rpg_medals''')


class StaleCampaign(PermissionError):
    pass


class RestorationStore:
    def __init__(self, path):
        self.path = str(path)

    @asynccontextmanager
    async def transaction(self):
        async with aiosqlite.connect(self.path, timeout=15) as db:
            await db.execute('BEGIN IMMEDIATE')
            try:
                yield db
                await db.commit()
            except BaseException:
                await db.rollback()
                raise

    async def pending(self, db, uid):
        # The migration runner installs the privacy ledger before these tables.
        row = await (await db.execute("SELECT 1 FROM privacy_deletion_jobs WHERE user_id=? "
            "AND status IN ('PENDING','IN_PROGRESS','RETRYABLE') LIMIT 1",(uid,))).fetchone()
        if row:
            raise StaleCampaign('Your privacy reset is still finishing.')

    async def get(self, uid, guild):
        async with aiosqlite.connect(self.path) as db:
            row = await (await db.execute('SELECT revision,state_json FROM restored_campaigns WHERE user_id=? AND guild_id=?',
                                         (uid,guild))).fetchone()
            return (row[0],json.loads(row[1])) if row else None

    async def open(self, uid, guild):
        async with self.transaction() as db:
            await self.pending(db,uid)
            await db.execute('INSERT OR IGNORE INTO restored_campaigns VALUES(?,?,?,?)',
                             (uid,guild,uuid.uuid4().hex,json.dumps({'stage':'world'})))
        return await self.get(uid,guild)

    async def update(self, uid, guild, revision, state, *, completed=False):
        next_revision = uuid.uuid4().hex
        async with self.transaction() as db:
            await self.pending(db,uid)
            result = await db.execute('UPDATE restored_campaigns SET revision=?,state_json=? '
                'WHERE user_id=? AND guild_id=? AND revision=?',
                (next_revision,json.dumps(state),uid,guild,revision))
            if result.rowcount != 1:
                raise StaleCampaign('That quest panel is outdated. Use !rpg1 to continue.')
            if completed:
                await db.execute('''INSERT INTO restored_medals VALUES(?,?,1,?)
                    ON CONFLICT(user_id,guild_id) DO UPDATE SET
                    completions=completions+1,best_points=MAX(best_points,excluded.best_points)''',
                    (uid,guild,state['total_points']))
        return next_revision

    async def valid(self, uid, guild, revision):
        async with self.transaction() as db:
            await self.pending(db,uid)
            row = await (await db.execute('SELECT 1 FROM restored_campaigns WHERE user_id=? AND guild_id=? AND revision=?',
                                         (uid,guild,revision))).fetchone()
            if not row:
                raise StaleCampaign('That quest panel has expired or was deleted.')

    async def reset(self, uid, guild, revision):
        async with self.transaction() as db:
            await self.pending(db,uid)
            result = await db.execute('DELETE FROM restored_campaigns WHERE user_id=? AND guild_id=? AND revision=?',
                                     (uid,guild,revision))
            if result.rowcount != 1:
                raise StaleCampaign('That reset confirmation is outdated.')

    async def leaderboard(self, guild, uid):
        async with aiosqlite.connect(self.path) as db:
            rows = await (await db.execute('SELECT user_id,completions,best_points FROM restored_medals '
                'WHERE guild_id=? AND (?!=0 OR user_id=?) ORDER BY completions DESC,best_points DESC LIMIT 15',
                (guild,guild,uid))).fetchall()
        return rows

    async def forget(self, uid):
        async with self.transaction() as db:
            for table in ('restored_campaigns','restored_medals'):
                await db.execute(f'DELETE FROM {table} WHERE user_id=?',(uid,))
            tables = {r[0] for r in await (await db.execute("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()}
            for table in ('rpg_state','rpg_medals'):
                if table in tables:
                    await db.execute(f'DELETE FROM {table} WHERE user_id=?',(uid,))
