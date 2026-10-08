"""User birthday registration/delivery; distinct from January-3 character events.

Announcements are bound to the channel where their owner enabled them, not a
last-seen channel. Dates/years are only reported privately. Claims reserve before
delivery and are not retried after ambiguous network outcomes (no duplicate ping).
"""
from __future__ import annotations
import asyncio
from datetime import datetime, timezone
import logging
import re
import uuid
from zoneinfo import ZoneInfo
import aiosqlite
import discord
from discord.ext import tasks
from restored_lifecycle import WORK

log=logging.getLogger(__name__)


def parse_birthday(raw, now=None):
    text=re.sub(r'\s+',' ',raw.strip().replace(',',' '))
    match=re.fullmatch(r'(\d{1,4})[/-](\d{1,2})(?:[/-](\d{1,4}))?',text)
    year=None
    if match:
        a,b,c=match.groups()
        if c and len(a)==4: year,month,day=int(a),int(b),int(c)
        else: month,day,year=int(a),int(b),int(c) if c else None
    else:
        for fmt in ('%B %d %Y','%b %d %Y','%B %d','%b %d'):
            try:
                # Use leap-year anchor for yearless February 29.
                value=datetime.strptime(text if '%Y' in fmt else text+' 2000',fmt if '%Y' in fmt else fmt+' %Y')
                month,day,year=value.month,value.day,value.year if '%Y' in fmt else None
                break
            except ValueError: pass
        else: raise ValueError('Use !birthday MM-DD, YYYY-MM-DD, or March 14.')
    if year is not None and not 1900<=year<=(now or datetime.now(timezone.utc)).year:
        raise ValueError('That birth year is invalid.')
    datetime(year or 2000,month,day)
    return month,day,year


async def migrate(db, _name):
    await db.execute('''CREATE TABLE IF NOT EXISTS restored_birthdays(
        user_id INTEGER PRIMARY KEY,month INTEGER NOT NULL,day INTEGER NOT NULL,
        year INTEGER,guild_id INTEGER NOT NULL,channel_id INTEGER NOT NULL,
        enabled INTEGER NOT NULL DEFAULT 1,revision TEXT NOT NULL,last_sent_year INTEGER NOT NULL DEFAULT 0)''')
    columns={r[1] for r in await (await db.execute('PRAGMA table_info(users)')).fetchall()}
    if {'birth_month','birth_day','birth_year','birthday_last_sent_year'}<=columns:
        # Existing birthday values remain available privately. No guessed public
        # destination or unsolicited announcement from an unmapped legacy record.
        for uid,month,day,year,sent in await (await db.execute('SELECT user_id,birth_month,birth_day,birth_year,birthday_last_sent_year FROM users WHERE birth_month>0 AND birth_day>0')).fetchall():
            await db.execute('INSERT OR IGNORE INTO restored_birthdays VALUES(?,?,?,?,0,0,0,?,?)',
                             (uid,month,day,year,uuid.uuid4().hex,sent or 0))


class BirthdayController:
    def __init__(self,bot,mem,deletion_pending,secret_detector,setup,initialize=None):
        self.bot,self.mem=bot,mem
        self.pending,self.secret_detector,self.setup=deletion_pending,secret_detector,setup
        self.initialize=initialize

    async def get(self,uid):
        async with aiosqlite.connect(self.mem.db_path) as db:
            db.row_factory=aiosqlite.Row
            row=await (await db.execute('SELECT * FROM restored_birthdays WHERE user_id=?',(uid,))).fetchone()
            return dict(row) if row else None

    async def command(self,ctx,raw=''):
        uid=ctx.author.id
        if await self.pending(uid):
            await ctx.send('Your privacy reset is still finishing.'); return
        if self.secret_detector(raw):
            await ctx.send('Keep credentials out of birthday settings.'); return
        # No admin/mention override: only the data subject can register a date.
        if '<@' in raw:
            await ctx.send('Each person must set their own birthday.'); return
        await self.setup(ctx)
        prior=await self.get(uid)
        key=raw.strip().lower()
        if key in ('clear','remove','delete','forget'):
            await self.forget(uid)
            await ctx.send('Fine. I forgot your birthday.'); return
        if key in ('off','on'):
            if not prior:
                await ctx.send('Set your birthday first.'); return
            if key=='on' and not ctx.guild:
                await ctx.send('Use !birthday on in the server channel where you want the greeting.'); return
            async with aiosqlite.connect(self.mem.db_path) as db:
                await db.execute('UPDATE restored_birthdays SET enabled=?,guild_id=?,channel_id=?,revision=? WHERE user_id=? AND revision=?',
                    (int(key=='on'),getattr(ctx.guild,'id',0),ctx.channel.id,uuid.uuid4().hex,uid,prior['revision']))
                await db.commit()
            await ctx.send('Birthday announcements '+key+'.'); return
        if key:
            try: month,day,year=parse_birthday(raw)
            except ValueError:
                await ctx.send('Use !birthday MM-DD, YYYY-MM-DD, or March 14.'); return
            async with aiosqlite.connect(self.mem.db_path,timeout=15) as db:
                await db.execute('BEGIN IMMEDIATE')
                # Same local ledger transaction prevents a pending reset race.
                active=await (await db.execute("SELECT 1 FROM privacy_deletion_jobs WHERE user_id=? AND status IN ('PENDING','IN_PROGRESS','RETRYABLE')",(uid,))).fetchone()
                if active: return
                await db.execute('''INSERT INTO restored_birthdays VALUES(?,?,?,?,?,?,1,?,0)
                    ON CONFLICT(user_id) DO UPDATE SET month=excluded.month,day=excluded.day,
                    year=excluded.year,guild_id=excluded.guild_id,channel_id=excluded.channel_id,
                    enabled=excluded.enabled,revision=excluded.revision''',
                    (uid,month,day,year,getattr(ctx.guild,'id',0),ctx.channel.id,uuid.uuid4().hex))
                await db.commit()
            await ctx.send('Birthday remembered. I will greet you here, subject to your privacy and quiet-hour settings. '
                           'Use !birthday off to stop announcements, or !birthday clear to erase it.'); return
        if not prior:
            await ctx.send('Use !birthday MM-DD or March 14. A year is optional; include it only if you want age mentioned.'); return
        date=f"{prior['month']:02d}-{prior['day']:02d}"+(f"-{prior['year']}" if prior['year'] else '')
        try:
            await ctx.author.send(f"Your birthday: {date}. Announcements: {'on' if prior['enabled'] else 'off'}.",allowed_mentions=discord.AllowedMentions.none())
            if ctx.guild: await ctx.send('I sent your birthday settings privately.')
        except discord.Forbidden:
            await ctx.send('Open your DMs to view the date privately.')

    async def forget(self,uid):
        async with aiosqlite.connect(self.mem.db_path) as db:
            await db.execute('DELETE FROM restored_birthdays WHERE user_id=?',(uid,))
            columns={r[1] for r in await (await db.execute('PRAGMA table_info(users)')).fetchall()}
            if {'birth_month','birth_day','birth_year','birthday_last_sent_year'}<=columns:
                await db.execute('UPDATE users SET birth_month=0,birth_day=0,birth_year=0,birthday_last_sent_year=0 WHERE user_id=?',(uid,))
            await db.commit()

    async def due(self,now=None):
        async with aiosqlite.connect(self.mem.db_path) as db:
            db.row_factory=aiosqlite.Row
            rows=await (await db.execute('SELECT b.*,u.timezone_name,u.quiet_hours_start,u.quiet_hours_end,u.proactive,u.allow_dms '
                'FROM restored_birthdays b JOIN users u ON u.user_id=b.user_id WHERE b.enabled=1')).fetchall()
        result=[]
        for row in rows:
            b=dict(row)
            try: tz=ZoneInfo(b['timezone_name'] or 'America/Los_Angeles')
            except (ValueError,KeyError): tz=ZoneInfo('America/Los_Angeles')
            local=(now or datetime.now(timezone.utc)).astimezone(tz)
            start,end=b['quiet_hours_start'],b['quiet_hours_end']
            quiet=(start<=local.hour<end) if start<end else (local.hour>=start or local.hour<end) if start!=end else False
            if (local.month,local.day)!=(b['month'],b['day']) or not 9<=local.hour<=14 or quiet or not b['proactive']:
                continue
            if b['last_sent_year']>=local.year or (not b['guild_id'] and not b['allow_dms']): continue
            if await self.pending(b['user_id']) or await self.mem.is_muted(b['user_id']): continue
            b['send_year']=local.year
            result.append(b)
        return result

    async def tick(self,now=None):
        for b in await self.due(now):
            uid=b['user_id']
            channel=self.bot.get_channel(b['channel_id'])
            if channel is None: continue
            if b['guild_id']:
                if getattr(getattr(channel,'guild',None),'id',0)!=b['guild_id']: continue
                member=channel.guild.get_member(uid)
                if member is None: continue
                perms=channel.permissions_for(channel.guild.me)
                if not (perms.view_channel and perms.send_messages): continue
                name=member.display_name
            else:
                if getattr(getattr(channel,'recipient',None),'id',0)!=uid: continue
                name=channel.recipient.display_name
            async with aiosqlite.connect(self.mem.db_path) as db:
                cursor=await db.execute('UPDATE restored_birthdays SET last_sent_year=? WHERE user_id=? AND revision=? AND enabled=1 AND last_sent_year<?',
                    (b['send_year'],uid,b['revision'],b['send_year']))
                await db.commit()
                if cursor.rowcount!=1: continue
            # Personalize from this user's relationship only; never pull a
            # private callback/summary into a public birthday announcement.
            if await self.pending(uid): continue
            user=await self.mem.get_user(uid)
            line=('I remembered. Stay a while; the day is yours, apparently.'
                  if user.get('trust',0)>=70 else
                  'Do not look so pleased—I remembered because forgetting would be beneath me.')
            current=await self.get(uid)
            if not current or current['revision']!=b['revision'] or not current['enabled']: continue
            age=f" {b['send_year']-b['year']} years, then." if b['year'] else ''
            try:
                await channel.send(f"Happy birthday, {discord.utils.escape_markdown(name)}.{age} "
                    +line,
                    allowed_mentions=discord.AllowedMentions.none())
            except discord.Forbidden:
                # Definitive denial: safe to retry later after permissions change.
                async with aiosqlite.connect(self.mem.db_path) as db:
                    await db.execute('UPDATE restored_birthdays SET last_sent_year=0 WHERE user_id=? AND revision=? AND last_sent_year=?',
                                     (uid,b['revision'],b['send_year']))
                    await db.commit()
            except Exception as exc:
                log.warning('Birthday delivery uncertain (%s); not duplicated',type(exc).__name__)

    @tasks.loop(minutes=30)
    async def delivery(self):
        try: await self.tick()
        except Exception as exc: log.warning('Birthday checker unavailable (%s)',type(exc).__name__)

    async def ready(self):
        if self.initialize: await self.initialize()
        if not self.delivery.is_running(): self.delivery.start()

    def install(self):
        @self.bot.command(name='birthday',aliases=['dob','birthdate','setbirthday'])
        async def birthday(ctx,*,birthday: str=''):
            async with WORK.track(ctx.author.id):
                await self.command(ctx,birthday)
        self.bot.add_listener(self.ready,'on_ready')
        return self
