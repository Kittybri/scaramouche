"""Opt-in adapters for character pranks; no censorship, webhooks or new LLM calls."""

from __future__ import annotations

import asyncio
import logging
import random
import time

import discord
from discord.ext import commands

from server_chaos.errors import ChaosError
from server_chaos.service import harmless, public_channel, quiet
from interaction_policy import optional_allowed, gags_paused

log = logging.getLogger("scaramouche.trolling")


class TrollingConfig:
    TYPING_CHANCE = 0.003
    JUDGE_CHANCE = 0.018
    EDIT_CHANCE = 0.012
    EDIT_DELAY = 8
    CONSENT_SECONDS = 30 * 86400


class OwnerPrivilege:
    """Flavor only: never substitutes for consent, permissions or budget."""

    @staticmethod
    def greeting(is_owner):
        return (
            "Since it's you, I'll explain it once." if is_owner else "Read carefully."
        )


class TrollingEngine:
    FLAGS = {"typing", "judge", "edits", "parody"}
    TYPING_LINES = (
        "Whatever masterpiece you're composing, make it worth the wait.",
        "The typing indicator promises a grand entrance. We shall see.",
        "Take your time. Even I can exercise patience. Occasionally.",
    )
    EDIT_LINES = (
        "Theatrical amendment: naturally, I meant that with impeccable courtesy.",
        "Theatrical amendment: imagine a gracious smile. A very small one.",
        "Theatrical amendment: there. Practically diplomacy.",
    )

    def __init__(self, chaos):
        self.chaos, self.bot, self.store = chaos, chaos.bot, chaos.store
        self._lock = None
        self.tasks = set()
        self.edit_ids = set()
        self.last_line = {}
        self.closed = False

    @property
    def lock(self):
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def key(self, gid, uid):
        return f"chaos:trollprefs:{gid}:{uid}"

    async def prefs(self, gid, uid):
        record = await self.store.get(self.key(gid, uid)) or {}
        if record.get("expires", 0) <= time.time():
            return {}
        return record.get("flags", {})

    async def gate(self, channel, user, feature, *, personal=True):
        if (self.closed or not self.bot.user or not getattr(channel, "guild", None)
                or not optional_allowed("trolling") or gags_paused(channel.guild.id)):
            return False
        if user.bot or user.id == self.bot.user.id:
            return False
        gid = channel.guild.id
        if channel.guild.get_member(user.id) is None:
            return False
        cfg = self.chaos.cfg(gid)
        if (
            channel.id not in cfg.get("allowed_channels", [])
            or not cfg.get("trolling", {}).get(feature, False)
            or not await self.chaos.enabled(gid, "trolling")
            or not public_channel(channel, user)
        ):
            return False
        if personal and not (await self.prefs(gid, user.id)).get(feature):
            return False
        profile = await self.chaos.mem.get_user(user.id) or {}
        return (
            profile.get("proactive", True)
            and not quiet(profile)
            and not await self.chaos.mem.is_muted(user.id)
        )

    def eligible_text(self, message):
        text = message.content or ""
        return (
            not message.author.bot
            and not message.attachments
            and not message.embeds
            and not message.mentions
            and not text.lstrip().startswith(("!", "/", "?", "$"))
            and "?" not in text
            and harmless(text)
        )

    async def utility_active(self, channel):
        return bool(
            await self.chaos.mem.get_active_trivia(channel.id)
            or await self.chaos.mem.get_duo_session(channel.id)
        )

    def line(self, gid, feature, choices):
        # Bounded anti-repeat cache, not authority or recovery state.
        if len(self.last_line) > 500:
            self.last_line.clear()
        key = (gid, feature)
        line = random.choice(
            [x for x in choices if x != self.last_line.get(key)] or choices
        )
        self.last_line[key] = line
        return line

    async def typing(self, channel, user):
        try:
            async with self.lock:
                if (
                    not await self.gate(channel, user, "typing")
                    or random.random() >= TrollingConfig.TYPING_CHANCE
                    or await self.utility_active(channel)
                ):
                    return
                # A typing event reveals neither a draft nor keystrokes. Require
                # recent playful context, and suppress on any serious/command turn.
                history = [m async for m in channel.history(limit=5)]
                humans = [m for m in history if not m.author.bot]
                if not humans or not all(self.eligible_text(m) for m in humans):
                    return
                latest = humans[0]
                if time.time() - latest.created_at.timestamp() > 300:
                    return
                await self.chaos.reserve(
                    channel.guild.id, user.id, "troll_typing", days=3
                )
                await self.chaos.send(
                    channel, self.line(channel.guild.id, "typing", self.TYPING_LINES)
                )
        except (ChaosError, discord.HTTPException):
            pass
        except Exception as exc:
            log.warning("typing gag skipped: %s", type(exc).__name__)

    async def before_reply(self, message):
        """Called after normal routing/security/utility handling, not above it."""
        try:
            async with self.lock:
                if not self.eligible_text(message) or await self.utility_active(
                    message.channel
                ):
                    return False
                if await self.gate(
                    message.channel, message.author, "parody"
                ) and await self.chaos.enabled(message.guild.id, "parody"):
                    muzzle = (
                        await self.store.get(
                            f"chaos:muzzle:{message.guild.id}:{message.author.id}"
                        )
                        or {}
                    )
                    if (
                        muzzle.get("state") == "created"
                        and muzzle.get("channel") == message.channel.id
                        and muzzle.get("expires", 0) > time.time()
                    ):
                        await self.chaos.translate(message)
                        return True  # Labeled bot reply, original stays untouched.
                if (
                    await self.gate(message.channel, message.author, "judge")
                    and random.random() < TrollingConfig.JUDGE_CHANCE
                ):
                    await self.chaos.reserve(
                        message.guild.id, message.author.id, "troll_judge", days=1
                    )
                    await message.add_reaction(
                        self.line(message.guild.id, "judge", ("🥱", "🙄", "😒", "🤨"))
                    )
                    return True
        except (ChaosError, discord.HTTPException):
            pass
        except Exception as exc:
            log.warning("reply gag skipped: %s", type(exc).__name__)
        return False

    def spawn(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return task

    async def revision(self, gid, uid):
        async with self.store.connect() as db:
            rows = await (
                await db.execute(
                    "SELECT key,updated_at FROM persistent_world_events WHERE key IN (?,?) ORDER BY key",
                    (f"chaos:control:{gid}", self.key(gid, uid)),
                )
            ).fetchall()
        return tuple(tuple(row) for row in rows)

    async def after_reply(self, source, sent):
        try:
            async with self.lock:
                if (
                    not self.eligible_text(source)
                    or not harmless(sent.content)
                    or sent.author.id != self.bot.user.id
                    or len(sent.content) > 1700
                    or sent.id in self.edit_ids
                    or len(self.tasks) >= 20
                    or not await self.gate(source.channel, source.author, "edits")
                    or await self.utility_active(source.channel)
                    or random.random() >= TrollingConfig.EDIT_CHANCE
                ):
                    return
                await self.chaos.reserve(
                    source.guild.id, source.author.id, "troll_edit", days=2
                )
                self.edit_ids.add(sent.id)
                revision = await self.revision(source.guild.id, source.author.id)
                self.spawn(self.edit_later(source, sent, revision))
        except (ChaosError, discord.HTTPException):
            pass
        except Exception as exc:
            log.warning("edit gag skipped: %s", type(exc).__name__)

    async def edit_later(self, source, sent, revision=None):
        original = sent.content
        try:
            if revision is None:
                revision = await self.revision(source.guild.id, source.author.id)
            await asyncio.sleep(TrollingConfig.EDIT_DELAY)
            if revision != await self.revision(source.guild.id, source.author.id):
                return  # Opt-out/restore followed by re-enable must not revive old edits.
            if not await self.gate(
                source.channel, source.author, "edits"
            ) or await self.utility_active(source.channel):
                return
            # Fresh-read both messages: never edit another author's content or
            # overwrite a subsequent bot/manual edit; source may have changed.
            fresh_source = await source.channel.fetch_message(source.id)
            current = await source.channel.fetch_message(sent.id)
            if (
                fresh_source.content != source.content
                or not self.eligible_text(fresh_source)
                or current.author.id != self.bot.user.id
                or current.content != original
            ):
                return
            suffix = self.line(source.guild.id, "edit", self.EDIT_LINES)
            await current.edit(
                content=original + "\n\n[Visible character gag] " + suffix,
                allowed_mentions=discord.AllowedMentions.none(),
            )
        except discord.HTTPException:
            pass
        except Exception as exc:
            log.warning("delayed gag skipped: %s", type(exc).__name__)
        finally:
            self.edit_ids.discard(sent.id)

    async def preferences(self, ctx, flag="status", value=""):
        if flag == "help":
            await self.chaos.send(
                ctx,
                "Personal opt-in: !trollprefs typing|judge|edits|parody on/off; status; off. "
                "Owner performances: !phantomping @user, !kidnap @user (voluntary VC invitation), "
                "!muzzle @user [10–300 seconds], !unmuzzle @user, !parodyas @user (reply to their message), "
                "!slowtrap, !serverwipe. All require guild configuration. No censorship or webhook impersonation.",
            )
            return
        if not ctx.guild:
            raise ChaosError("Use this in the guild whose settings you want to change.")
        async with self.lock:
            await self.store.init()
            flags = await self.prefs(ctx.guild.id, ctx.author.id)
            if flag == "off":
                flags = {}
            elif flag in self.FLAGS and value in {"on", "off"}:
                flags[flag] = value == "on"
            elif flag != "status":
                raise ChaosError(
                    "!trollprefs typing|judge|edits|parody on/off; status; off"
                )
            if flag != "status":
                await self.store.put(
                    self.key(ctx.guild.id, ctx.author.id),
                    "chaos_trollprefs",
                    dict(
                        user_id=ctx.author.id,
                        guild_id=ctx.guild.id,
                        flags=flags,
                        expires=time.time() + TrollingConfig.CONSENT_SECONDS,
                        state="consent",
                    ),
                )
                if flag == "off" or (flag == "parody" and value == "off"):
                    await self.store.remove(
                        f"chaos:muzzle:{ctx.guild.id}:{ctx.author.id}"
                    )
            await self.chaos.send(
                ctx,
                OwnerPrivilege.greeting(ctx.author.id == self.chaos.owner_id)
                + f" Your opt-ins: {flags or 'all off'}. They expire after 30 days."
                + " Parody also requires !pranks parody on. !trollprefs off stops these gags.",
            )

    async def manual(self, ctx, feature, target=None, duration=300):
        if not self.chaos.owner_id or ctx.author.id != self.chaos.owner_id:
            raise ChaosError("Only the configured owner may start this performance.")
        if not ctx.guild:
            raise ChaosError("A configured guild is required.")
        async with self.lock:
            await self.store.init()
            if feature == "unmuzzle":
                if not target:
                    raise ChaosError("Mention a participant.")
                await self.store.remove(f"chaos:muzzle:{ctx.guild.id}:{target.id}")
                await self.chaos.send(
                    ctx, "The parody session has ended. Nothing was censored."
                )
                return
            if not await self.gate(ctx.channel, ctx.author, feature, personal=False):
                raise ChaosError(
                    "This performance is disabled, outside allowed hours, or not in an allowed public channel."
                )
            if feature in {"phantomping", "muzzle", "parodyas", "kidnap"} and (
                not target or target.bot
            ):
                raise ChaosError("Mention one human participant.")
            if feature == "phantomping":
                await self.chaos.dispatch(ctx, "pranks", "phantom", "")
            elif feature in {"muzzle", "parodyas"}:
                if (
                    not await self.gate(ctx.channel, target, "parody")
                    or not (await self.store.prefs(target.id))["chaos_parody"]
                    or not await self.chaos.enabled(ctx.guild.id, "parody")
                ):
                    raise ChaosError(
                        "The participant must opt into parody and the guild must enable it."
                    )
                if feature == "parodyas":
                    mid = getattr(ctx.message.reference, "message_id", None)
                    if not mid:
                        raise ChaosError(
                            "Reply to that participant's harmless public message."
                        )
                    source, _ = await self.chaos.source(ctx.channel, mid, target)
                    if source.author.id != target.id:
                        raise ChaosError("The source must belong to the participant.")
                    await self.chaos.translate(source)
                else:
                    if not 10 <= duration <= 300:
                        raise ChaosError("Use 10–300 seconds.")
                    # Activation is a consent/configuration receipt, not a gag.
                    # Actual parody consumes the shared budget in translate().
                    ok, _ = await self.chaos.mem.consume_shared_cooldown(
                        f"chaos:{ctx.guild.id}:{target.id}:troll_muzzle", 86400
                    )
                    if not ok:
                        raise ChaosError("Parody sessions are cooling down.")
                    await self.store.put(
                        f"chaos:muzzle:{ctx.guild.id}:{target.id}",
                        "chaos_trollsession",
                        dict(
                            id=f"muzzle:{ctx.guild.id}:{target.id}",
                            bot=self.chaos.name,
                            user_id=target.id,
                            participants=[target.id],
                            channel=ctx.channel.id,
                            guild_id=ctx.guild.id,
                            state="created",
                            expires=time.time() + duration,
                        ),
                    )
                    await self.chaos.send(
                        ctx,
                        f"Labeled parody session for user ID {target.id}, {duration}s."
                        " Original messages stay visible. !trollprefs off ends participation.",
                    )
            elif feature == "kidnap":
                await self.chaos.reserve(
                    ctx.guild.id, target.id, "troll_interrogate", days=1
                )
                await self.chaos.voice.features.games.command(ctx, "interrogate", "")
            elif feature == "slowtrap":
                await self.chaos.reserve(
                    ctx.guild.id, ctx.author.id, "troll_slowmode", cost=2, days=1
                )
                record = await self.chaos.restoration.apply(
                    ctx.guild,
                    "channel",
                    ctx.channel.id,
                    "slowmode_delay",
                    10,
                    ctx.author.id,
                    90,
                    feature="troll_slowmode",
                )
                await self.chaos.send(
                    ctx,
                    "A brief dramatic pause: at most 10-second slowmode for 90 seconds. "
                    + (
                        "Recovery recorded; Discord confirmation is pending."
                        if record and record["state"] != "applied"
                        else "Tracked restoration will preserve newer administrator edits."
                    ),
                )
            elif feature == "serverwipe":
                await self.chaos.reserve(
                    ctx.guild.id, ctx.author.id, "troll_wipe", days=90
                )
                await self.countdown(ctx)
            else:
                raise ChaosError("Unknown performance.")

    async def countdown(self, ctx):
        message = await ctx.send(
            "[Server game] PRETEND SERVER WIPE — nothing will be deleted. 5",
            allowed_mentions=discord.AllowedMentions.none(),
        )
        for number in (4, 3, 2, 1):
            await asyncio.sleep(1)
            if not await self.chaos.enabled(ctx.guild.id, "trolling"):
                await message.edit(
                    content="[Server game] Performance cancelled. Nothing was deleted."
                )
                return
            await message.edit(
                content=f"[Server game] PRETEND SERVER WIPE — no deletions. {number}"
            )
        await message.edit(
            content="[Server game] Curtain down. Nothing was deleted. Even my theatrics have limits."
        )

    async def shutdown(self):
        self.closed = True
        tasks = tuple(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.tasks.clear()
        self.edit_ids.clear()

    async def invoke(self, ctx, feature, target=None, duration=300):
        try:
            await self.manual(ctx, feature, target, duration)
        except ChaosError as exc:
            await self.chaos.send(ctx, str(exc))
        except (ValueError, discord.HTTPException):
            await self.chaos.send(
                ctx,
                "That performance could not run. Check consent, configuration and permissions; recovery receipts remain.",
            )

    def install(self):
        close = self.bot.close

        async def closing():
            await self.shutdown()
            await close()

        self.bot.close = closing

        async def report(ctx, call):
            try:
                await call
            except ChaosError as exc:
                await self.chaos.send(ctx, str(exc))
            except (ValueError, discord.HTTPException):
                await self.chaos.send(
                    ctx,
                    "That performance could not run. Check consent, configuration and permissions; recovery receipts remain.",
                )

        @self.bot.command(name="trollprefs")
        async def preferences(ctx, flag="status", value=""):
            await report(ctx, self.preferences(ctx, flag.lower(), value.lower()))

        def register(name):
            async def command(ctx, target: discord.Member = None, duration: int = 300):
                await self.invoke(ctx, name, target, duration)

            self.bot.command(name=name)(command)

        for name in (
            "phantomping",
            "kidnap",
            "muzzle",
            "unmuzzle",
            "parodyas",
            "slowtrap",
            "serverwipe",
        ):
            register(name)
