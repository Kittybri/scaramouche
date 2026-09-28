import asyncio
import ast
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import discord
import pytest
from discord.ext import commands

from test_server_chaos import setup, optin
from trolling_features import TrollingEngine, TrollingConfig


def run(coro):
    return asyncio.run(coro)


def test_constructor_does_not_create_event_loop_bound_objects(monkeypatch):
    def forbidden():
        raise AssertionError("Lock constructed outside async runtime")

    monkeypatch.setattr("trolling_features.asyncio.Lock", forbidden)
    engine = TrollingEngine(NS(bot=None, store=None))
    assert engine._lock is None


async def fixture(tmp_path, monkeypatch):
    chaos, ctx, source = await setup(tmp_path)
    chaos.mem.is_muted = AsyncMock(return_value=False)
    chaos.mem.get_active_trivia = AsyncMock(return_value=None)
    chaos.mem.get_duo_session = AsyncMock(return_value=None)
    source.mentions = []
    source.created_at = datetime.now(timezone.utc)
    source.add_reaction = AsyncMock()
    chaos.cfg(5)["features"]["trolling"] = True
    chaos.cfg(5)["trolling"] = dict.fromkeys(
        [
            "typing",
            "judge",
            "edits",
            "parody",
            "phantomping",
            "muzzle",
            "parodyas",
            "kidnap",
            "slowtrap",
            "serverwipe",
        ],
        True,
    )
    monkeypatch.setattr("trolling_features.quiet", lambda p: False)
    monkeypatch.setattr("trolling_features.random.random", lambda: 0)
    engine = TrollingEngine(chaos)
    for uid in (1, 2):
        await optin(chaos, uid)
        await chaos.store.put(
            engine.key(5, uid),
            "chaos_trollprefs",
            dict(
                user_id=uid,
                flags=dict.fromkeys(engine.FLAGS, True),
                expires=time.time() + 600,
            ),
        )

    async def history(**kwargs):
        yield source

    ctx.channel.history = history
    return engine, chaos, ctx, source


@pytest.mark.parametrize(
    "block",
    [
        "guild",
        "feature",
        "channel",
        "consent",
        "expiry",
        "quiet",
        "muted",
        "bot",
        "dm",
        "utility",
    ],
)
def test_typing_gates(tmp_path, monkeypatch, block):
    async def check():
        e, o, c, s = await fixture(tmp_path, monkeypatch)
        if block == "guild":
            o.cfg(5)["enabled"] = False
        if block == "feature":
            o.cfg(5)["trolling"]["typing"] = False
        if block == "channel":
            o.cfg(5)["allowed_channels"] = []
        if block == "consent":
            await o.store.remove(e.key(5, 2))
        if block == "expiry":
            await o.store.put(
                e.key(5, 2), "chaos_trollprefs", dict(expires=0, flags={"typing": True})
            )
        if block == "quiet":
            monkeypatch.setattr("trolling_features.quiet", lambda p: True)
        if block == "muted":
            o.mem.is_muted.return_value = True
        if block == "bot":
            s.author.bot = True
        if block == "dm":
            c.channel.guild = None
        if block == "utility":
            o.mem.get_active_trivia.return_value = {"question": "active"}
        await e.typing(c.channel, s.author)
        c.send.assert_not_awaited()

    run(check())


def test_typing_once_uses_shared_budget_no_ping(tmp_path, monkeypatch):
    async def check():
        e, o, c, s = await fixture(tmp_path, monkeypatch)
        await asyncio.gather(*(e.typing(c.channel, s.author) for _ in range(4)))
        c.send.assert_awaited_once()
        assert not c.send.call_args.kwargs["allowed_mentions"].everyone
        assert not c.send.call_args.kwargs["allowed_mentions"].users

    run(check())


@pytest.mark.parametrize(
    "text",
    [
        "!weather London",
        "Can you help?",
        "my password is secret",
        "I feel unsafe",
        "Scaramouche is dramatic.",
    ],
)
def test_reaction_routes_only_harmless_statements(tmp_path, monkeypatch, text):
    async def check():
        e, o, c, s = await fixture(tmp_path, monkeypatch)
        s.content = text
        reacted = await e.before_reply(s)
        assert reacted == (text == "Scaramouche is dramatic.")
        assert s.add_reaction.await_count == int(reacted)

    run(check())


def test_reaction_failure_falls_through(tmp_path, monkeypatch):
    async def check():
        e, o, c, s = await fixture(tmp_path, monkeypatch)
        s.add_reaction.side_effect = discord.Forbidden(
            NS(status=403, reason="no"), "no"
        )
        assert not await e.before_reply(s)

    run(check())


@pytest.mark.parametrize(
    "change", ["none", "optout", "edited", "source", "author", "disabled"]
)
def test_visible_edit_preserves_original_and_rechecks(tmp_path, monkeypatch, change):
    async def check():
        e, o, c, s = await fixture(tmp_path, monkeypatch)
        sent = NS(id=102, author=o.bot.user, content="This is a good game.")
        current = NS(id=102, author=o.bot.user, content=sent.content, edit=AsyncMock())
        fresh = NS(**vars(s))
        c.channel.fetch_message.side_effect = lambda mid: (
            fresh if mid == s.id else current
        )
        if change == "optout":
            await o.store.remove(e.key(5, 2))
        if change == "edited":
            current.content = "Newer text"
        if change == "source":
            fresh.content = "Different source"
        if change == "author":
            current.author = NS(id=22)
        if change == "disabled":
            o.cfg(5)["enabled"] = False
        monkeypatch.setattr(TrollingConfig, "EDIT_DELAY", 0)
        await e.edit_later(s, sent)
        assert current.edit.await_count == int(change == "none")
        if change == "none":
            text = current.edit.call_args.kwargs["content"]
            assert text.startswith(sent.content) and "Visible character gag" in text

    run(check())


def test_edit_shutdown_cancels_tasks(tmp_path, monkeypatch):
    async def check():
        e, o, c, s = await fixture(tmp_path, monkeypatch)
        sent = NS(id=102, author=o.bot.user, content="This is a good game.")
        await e.after_reply(s, sent)
        assert len(e.tasks) == 1
        await e.shutdown()
        assert e.closed and not e.tasks and not e.edit_ids
        c.channel.fetch_message.assert_not_awaited()

    run(check())


def test_restore_and_reenable_does_not_revive_pending_edit(tmp_path, monkeypatch):
    async def check():
        e, o, c, s = await fixture(tmp_path, monkeypatch)
        revision = await e.revision(5, 2)
        await o.dispatch(c, "chaos", "restore-all", "")
        await o.dispatch(c, "chaos", "enable", "")
        sent = NS(id=102, author=o.bot.user, content="This is a good game.")
        monkeypatch.setattr(TrollingConfig, "EDIT_DELAY", 0)
        await e.edit_later(s, sent, revision)
        c.channel.fetch_message.assert_not_awaited()

    run(check())


def test_muzzle_is_channel_scoped_labeled_and_can_be_cancelled(tmp_path, monkeypatch):
    async def check():
        e, o, c, s = await fixture(tmp_path, monkeypatch)
        await e.manual(c, "muzzle", s.author, 60)
        assert await e.before_reply(s)
        assert "PARODY" in s.reply.call_args.args[0]
        assert s.content == "Scaramouche is dramatic."
        c.author = s.author
        await e.preferences(c, "off")
        assert not await o.store.get("chaos:muzzle:5:2")
        assert not await e.before_reply(s)

    run(check())


@pytest.mark.parametrize("operation", ["restore", "forget", "expire"])
def test_muzzle_recovery_and_optout(tmp_path, monkeypatch, operation):
    async def check():
        e, o, c, s = await fixture(tmp_path, monkeypatch)
        await e.manual(c, "muzzle", s.author, 60)
        if operation == "restore":
            await o.dispatch(c, "chaos", "restore-all", "")
        if operation == "forget":
            await o.forget(s.author.id)
            assert not await e.prefs(5, s.author.id)
        if operation == "expire":
            await o.store.transition(
                "muzzle:5:2", {"created"}, {"expires": time.time() - 1}
            )
            await o.tick()
        assert (await o.store.get("chaos:muzzle:5:2"))["state"] == "cancelled"

    run(check())


def test_manual_requires_owner_and_consent(tmp_path, monkeypatch):
    async def check():
        e, o, c, s = await fixture(tmp_path, monkeypatch)
        c.author = s.author
        with pytest.raises(ValueError):
            await e.manual(c, "slowtrap")
        c.author = c.guild.get_member(1)
        await o.store.remove(e.key(5, 2))
        with pytest.raises(ValueError):
            await e.manual(c, "muzzle", s.author)
        o.owner_id = 0
        with pytest.raises(ValueError):
            await e.manual(c, "serverwipe")

    run(check())


def test_slowtrap_durable_restore_preserves_admin_change(tmp_path, monkeypatch):
    async def check():
        e, o, c, s = await fixture(tmp_path, monkeypatch)
        await e.manual(c, "slowtrap")
        assert c.channel.slowmode_delay == 10
        receipts = await o.store.recent("chaos_mutation", 10)
        assert receipts[0][1]["before"] == 0
        c.channel.slowmode_delay = 3
        await o.restoration.restore(5, force=True)
        assert c.channel.slowmode_delay == 3

    run(check())


def test_adapters_reuse_consent_and_existing_vc_game(tmp_path, monkeypatch):
    async def check():
        e, o, c, s = await fixture(tmp_path, monkeypatch)
        o.dispatch = AsyncMock()
        await e.manual(c, "phantomping", s.author)
        o.dispatch.assert_awaited_once_with(c, "pranks", "phantom", "")
        o.voice.features.games.command = AsyncMock()
        await e.manual(c, "kidnap", s.author)
        o.voice.features.games.command.assert_awaited_once_with(c, "interrogate", "")

    run(check())


def test_parodyas_source_target_must_match(tmp_path, monkeypatch):
    async def check():
        e, o, c, s = await fixture(tmp_path, monkeypatch)
        s.author = c.author
        with pytest.raises(ValueError):
            await e.manual(c, "parodyas", c.guild.get_member(2))
        s.reply.assert_not_awaited()

    run(check())


def test_countdown_always_labels_fiction(tmp_path, monkeypatch):
    async def check():
        e, o, c, s = await fixture(tmp_path, monkeypatch)
        msg = NS(edit=AsyncMock())
        c.send.return_value = msg
        monkeypatch.setattr("trolling_features.asyncio.sleep", AsyncMock())
        await e.manual(c, "serverwipe")
        assert "PRETEND" in c.send.call_args.args[0]
        assert all(
            "PRETEND" in call.kwargs["content"]
            or "Nothing was deleted" in call.kwargs["content"]
            for call in msg.edit.call_args_list
        )

    run(check())


def test_registration_preserves_impersonate_and_closes(tmp_path, monkeypatch):
    async def check():
        e, o, c, s = await fixture(tmp_path, monkeypatch)
        bot = commands.Bot(command_prefix="!", intents=discord.Intents.none())

        @bot.command(name="impersonate")
        async def original(ctx):
            pass

        e.bot = bot
        e.install()
        assert bot.get_command("impersonate") is original
        for name in (
            "trollprefs",
            "phantomping",
            "kidnap",
            "muzzle",
            "unmuzzle",
            "parodyas",
            "slowtrap",
            "serverwipe",
        ):
            assert bot.get_command(name)
        await bot.close()
        assert e.closed

    run(check())


def test_no_destructive_or_llm_implementation_and_no_duplicate_hooks():
    root = Path(__file__).resolve().parents[1]
    code = root.joinpath("trolling_features.py").read_text()
    for forbidden in (
        "create_webhook",
        "webhook.send",
        "message.delete",
        "move_to(",
        "create_voice_channel",
        "qai(",
        ".ban(",
        ".kick(",
    ):
        assert forbidden not in code
    tree = ast.parse(root.joinpath("bot.py").read_text())
    functions = [n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef)]
    assert sum(n.name == "on_typing" for n in functions) == 1
    assert not any(n.name == "_delayed_character_edit" for n in functions)
