"""Focused regressions for the staged Discord message pipeline."""

import asyncio
import ast
from functools import wraps
from pathlib import Path
import sqlite3
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

import discord
from discord.ext import commands
import pytest

from interaction_policy import Outcome, classify
from message_pipeline import (
    PreparedMessage, command_context_matches, error_category, log_operation_error,
)


class TypingContext:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False


def async_test(function):
    @wraps(function)
    def wrapped(*args, **kwargs):
        return asyncio.run(function(*args, **kwargs))

    return wrapped


def discord_error(kind):
    status = {discord.Forbidden: 403, discord.NotFound: 404}.get(kind, 500)
    response = NS(status=status, reason="test", headers={})
    return kind(response, "test failure")


def prepared(*, reference=None, content="hello", user=None):
    return PreparedMessage(
        user=user or {}, user_id=7, channel_id=20, guild_id=30,
        is_dm=False, is_owner=False, romance=False, content=content,
        previous_last_active=0.0, reference_message=reference,
    )


def fake_message(bot_module, *, content="hello", reference=None, guild=True):
    author = NS(
        id=7, bot=False, name="user", display_name="User", mention="<@7>",
    )
    channel = NS(
        id=20, typing=lambda: TypingContext(), fetch_message=AsyncMock(),
        send=AsyncMock(),
    )
    guild_obj = NS(id=30, get_member=lambda member_id: None) if guild else None
    message = NS(
        id=101, content=content, author=author, channel=channel,
        guild=guild_obj, mentions=[], attachments=[], embeds=[],
        reference=reference, reply=AsyncMock(), add_reaction=AsyncMock(),
        _state=bot_module.bot._connection,
    )
    return message


@pytest.fixture
def runtime(monkeypatch):
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    import bot

    monkeypatch.setattr(bot.bot._connection, "user", NS(id=999, bot=True), raising=False)
    return bot


@async_test
async def test_command_detection_uses_discord_parser_for_attempts_aliases_and_punctuation():
    parser = commands.Bot(command_prefix="!", intents=discord.Intents.none())
    parser._connection.user = NS(id=999)

    @parser.command(name="hello", aliases=["hi"])
    async def hello(ctx):
        return None

    async def parsed(text):
        message = NS(
            content=text, _state=parser._connection,
            author=NS(id=7, bot=False), channel=NS(id=20), id=101,
        )
        return await parser.get_context(message)

    assert command_context_matches(await parsed("!hello"))
    assert command_context_matches(await parsed("!hi"))
    assert command_context_matches(await parsed("!unknown"))
    assert not command_context_matches(await parsed("!"))
    assert not command_context_matches(await parsed("That was surprising!"))


@async_test
async def test_structured_owner_consumes_before_routed_pipeline(runtime, monkeypatch):
    message = fake_message(runtime, content="my answer")
    context = NS(command=None, prefix=None, invoked_with=None)
    memory = NS(
        upsert_user=AsyncMock(return_value=0.0),
        track_channel=AsyncMock(), get_user=AsyncMock(return_value={"proactive": True}),
        is_muted=AsyncMock(return_value=False),
    )
    answer = AsyncMock()
    routed = AsyncMock()
    monkeypatch.setattr(runtime.bot, "get_context", AsyncMock(return_value=context))
    monkeypatch.setattr(runtime, "mem", memory)
    monkeypatch.setattr(runtime, "_interaction_session", AsyncMock(
        return_value=("trivia", {"asker_id": 7}),
    ))
    monkeypatch.setattr(runtime, "answer_cmd", NS(callback=answer))
    monkeypatch.setattr(runtime, "_on_message_routed", routed)
    runtime._processed_msgs.discard(message.id)

    await runtime._dispatch_message(message)

    answer.assert_awaited_once_with(context, response="my answer")
    routed.assert_not_awaited()


@async_test
async def test_pending_privacy_deletion_blocks_new_memory(runtime, monkeypatch):
    message = fake_message(runtime, content="hello again")
    context = NS(command=None, prefix=None, invoked_with=None)
    memory = NS(upsert_user=AsyncMock())
    deletion = NS(is_pending=AsyncMock(return_value=True))
    monkeypatch.setattr(runtime.bot, "get_context", AsyncMock(return_value=context))
    monkeypatch.setattr(runtime, "mem", memory)
    monkeypatch.setattr(runtime, "PRIVACY_DELETION", deletion)
    runtime._processed_msgs.discard(message.id)

    await runtime._dispatch_message(message)

    deletion.is_pending.assert_awaited_once_with(message.author.id)
    memory.upsert_user.assert_not_awaited()
    message.reply.assert_awaited_once()
    assert "won't create new memory" in message.reply.await_args.args[0]


@async_test
async def test_partner_rich_command_output_does_not_trigger_banter(runtime, monkeypatch):
    message = NS(
        content="command result", embeds=[object()], attachments=[], components=[],
        stickers=[], channel=NS(id=20), guild=NS(id=30),
    )
    observe = AsyncMock()
    monkeypatch.setattr(runtime, "_observe_partner_message", observe)

    await runtime._handle_partner_message(message, target_info={
        "addressed_me": False, "duo_expected": False, "human_targets": [],
    })

    observe.assert_not_awaited()


@async_test
async def test_partner_human_targeted_reply_does_not_trigger_banter(runtime, monkeypatch):
    message = NS(
        content="answer for the user", embeds=[], attachments=[], components=[],
        stickers=[], channel=NS(id=20), guild=NS(id=30),
    )
    observe = AsyncMock()
    monkeypatch.setattr(runtime, "_observe_partner_message", observe)

    await runtime._handle_partner_message(message, target_info={
        "addressed_me": False, "duo_expected": False, "human_targets": ["Kittybri"],
    })

    observe.assert_not_awaited()


async def run_coordinator(runtime, monkeypatch, *, media=False, optional=False):
    message = fake_message(runtime)
    item = prepared()
    interaction = classify(
        message.content, user_id=7, channel_id=20, guild_id=30,
        user={"proactive": True}, direct=True,
    )
    stages = {
        "prepare": AsyncMock(return_value=item),
        "observe": AsyncMock(),
        "owned": AsyncMock(return_value=False),
        "media": AsyncMock(return_value=media),
        "special": AsyncMock(return_value=False),
        "tedtalk": AsyncMock(return_value=False),
        "optional": AsyncMock(return_value=optional),
        "context": AsyncMock(return_value="context"),
        "generate": AsyncMock(return_value="reply"),
        "post": AsyncMock(),
        "deliver": AsyncMock(),
    }
    monkeypatch.setattr(runtime, "_prepare_routed_message", stages["prepare"])
    monkeypatch.setattr(runtime, "_observe_prepared_message", stages["observe"])
    monkeypatch.setattr(runtime, "_handle_pre_response_ownership", stages["owned"])
    monkeypatch.setattr(runtime, "_handle_media", stages["media"])
    monkeypatch.setattr(runtime, "_handle_special_trigger", stages["special"])
    monkeypatch.setattr(runtime, "_handle_tedtalk_followup", stages["tedtalk"])
    monkeypatch.setattr(runtime, "_handle_optional_character_behavior", stages["optional"])
    monkeypatch.setattr(runtime, "_build_normal_response_context", stages["context"])
    monkeypatch.setattr(runtime, "_generate_normal_reply", stages["generate"])
    monkeypatch.setattr(runtime, "_apply_post_response_effects", stages["post"])
    monkeypatch.setattr(runtime, "_deliver_normal_reply", stages["deliver"])
    monkeypatch.setattr(runtime, "resp_prob", lambda *args, **kwargs: 1.0)
    monkeypatch.setattr(runtime.random, "random", lambda: 0.5)
    await runtime._on_message_routed(message, interaction)
    return stages, message, interaction, item


@async_test
async def test_media_owner_stops_optional_and_normal_response(runtime, monkeypatch):
    stages, _, _, _ = await run_coordinator(runtime, monkeypatch, media=True)
    stages["media"].assert_awaited_once()
    stages["optional"].assert_not_awaited()
    stages["generate"].assert_not_awaited()
    stages["deliver"].assert_not_awaited()
    stages["observe"].assert_awaited_once()


@async_test
async def test_optional_owner_stops_normal_response(runtime, monkeypatch):
    stages, _, _, _ = await run_coordinator(runtime, monkeypatch, optional=True)
    stages["optional"].assert_awaited_once()
    stages["generate"].assert_not_awaited()
    stages["deliver"].assert_not_awaited()


@async_test
async def test_normal_response_reaches_post_effects_and_delivery(runtime, monkeypatch):
    stages, message, interaction, item = await run_coordinator(runtime, monkeypatch)
    stages["generate"].assert_awaited_once_with(message, interaction, item, "context")
    stages["post"].assert_awaited_once_with(message, interaction, item)
    stages["deliver"].assert_awaited_once_with(message, interaction, item, "reply")


@async_test
async def test_post_effect_database_failure_does_not_block_delivery(runtime, monkeypatch):
    message = fake_message(runtime, content="short")
    item = prepared(content="short", user={"mood": -8})
    interaction = classify(
        message.content, user_id=7, channel_id=20, guild_id=30,
        user={"proactive": True}, direct=True,
    )
    memory = NS(set_grudge_nick=AsyncMock(side_effect=sqlite3.OperationalError("locked")))
    monkeypatch.setattr(runtime, "mem", memory)
    monkeypatch.setattr(runtime, "qai", AsyncMock(return_value="pest"))
    errors = Mock()
    monkeypatch.setattr(runtime, "log_operation_error", errors)

    await runtime._apply_post_response_effects(message, interaction, item)

    memory.set_grudge_nick.assert_awaited_once()
    assert errors.call_args.kwargs["subsystem"] == "persistence"


@async_test
async def test_post_effect_failure_still_reaches_main_delivery(runtime, monkeypatch):
    message = fake_message(runtime, content="short")
    item = prepared(content="short", user={"mood": -8})
    interaction = classify(
        message.content, user_id=7, channel_id=20, guild_id=30,
        user={"proactive": True}, direct=True,
    )
    monkeypatch.setattr(runtime, "_prepare_routed_message", AsyncMock(return_value=item))
    monkeypatch.setattr(runtime, "_observe_prepared_message", AsyncMock())
    monkeypatch.setattr(runtime, "_handle_pre_response_ownership", AsyncMock(return_value=False))
    monkeypatch.setattr(runtime, "_handle_media", AsyncMock(return_value=False))
    monkeypatch.setattr(runtime, "_handle_special_trigger", AsyncMock(return_value=False))
    monkeypatch.setattr(runtime, "_handle_tedtalk_followup", AsyncMock(return_value=False))
    monkeypatch.setattr(runtime, "_handle_optional_character_behavior", AsyncMock(return_value=False))
    monkeypatch.setattr(runtime, "_build_normal_response_context", AsyncMock(return_value=""))
    monkeypatch.setattr(runtime, "_generate_normal_reply", AsyncMock(return_value="reply"))
    delivery = AsyncMock()
    monkeypatch.setattr(runtime, "_deliver_normal_reply", delivery)
    monkeypatch.setattr(runtime, "resp_prob", lambda *args, **kwargs: 1.0)
    monkeypatch.setattr(runtime.random, "random", lambda: 0.5)
    monkeypatch.setattr(runtime, "qai", AsyncMock(return_value="pest"))
    monkeypatch.setattr(runtime, "mem", NS(
        set_grudge_nick=AsyncMock(side_effect=sqlite3.OperationalError("locked")),
    ))
    monkeypatch.setattr(runtime, "log_operation_error", Mock())

    await runtime._on_message_routed(message, interaction)

    delivery.assert_awaited_once_with(message, interaction, item, "reply")


@async_test
async def test_reference_resolution_uses_resolved_fetches_once_and_handles_stale(runtime, monkeypatch):
    interaction = classify("hello", user_id=7, channel_id=20)
    resolved = NS(id=51, author=NS(id=999), attachments=[])
    reference = NS(message_id=51, resolved=resolved)
    message = fake_message(runtime, reference=reference)
    assert await runtime._resolve_reference(message, interaction) is resolved
    message.channel.fetch_message.assert_not_awaited()

    reference.resolved = None
    message.channel.fetch_message.return_value = resolved
    assert await runtime._resolve_reference(message, interaction) is resolved
    message.channel.fetch_message.assert_awaited_once_with(51)

    for failure in (discord.NotFound, discord.Forbidden):
        message.channel.fetch_message.reset_mock()
        message.channel.fetch_message.side_effect = discord_error(failure)
        assert await runtime._resolve_reference(message, interaction) is None
        message.channel.fetch_message.assert_awaited_once_with(51)


@async_test
async def test_reference_cancellation_propagates(runtime):
    message = fake_message(runtime, reference=NS(message_id=51, resolved=None))
    message.channel.fetch_message.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await runtime._resolve_reference(message, classify("hello"))


@async_test
async def test_partner_and_human_reply_routing_share_prepared_reference(runtime, monkeypatch):
    monkeypatch.setattr(runtime.PC, "observe_message", Mock())
    monkeypatch.setattr(runtime, "PARTNER_BOT_ID", 222)
    monkeypatch.setattr(runtime, "mem", NS(
        get_channel_speaker_mode=AsyncMock(return_value="auto"),
    ))

    partner = NS(id=51, author=NS(id=222), attachments=[])
    partner_message = fake_message(
        runtime, reference=NS(message_id=51, resolved=partner),
    )
    partner_context = classify("hello", user_id=7, channel_id=20, guild_id=30)
    assert await runtime._prepare_routed_message(partner_message, partner_context) is None
    assert partner_context.outcome is Outcome.SUPPRESSED

    human = NS(id=52, author=NS(id=8), attachments=[])
    human_message = fake_message(
        runtime, reference=NS(message_id=52, resolved=human),
    )
    human_context = classify("hello", user_id=7, channel_id=20, guild_id=30)
    item = await runtime._prepare_routed_message(human_message, human_context)
    assert item.reference_message is human
    assert not human_context.direct


@async_test
async def test_reference_is_fetched_once_across_prepare_and_delivery(runtime, monkeypatch):
    monkeypatch.setattr(runtime.PC, "observe_message", Mock())
    monkeypatch.setattr(runtime, "PARTNER_BOT_ID", 0)
    memory = NS(add_message=AsyncMock())
    monkeypatch.setattr(runtime, "mem", memory)
    monkeypatch.setattr(runtime, "FISH_AUDIO_API_KEY", "")
    monkeypatch.setattr(runtime.random, "random", lambda: 1.0)
    monkeypatch.setattr(runtime, "maybe_react", AsyncMock())
    monkeypatch.setattr(runtime.troll, "after_reply", AsyncMock())
    reference = NS(message_id=51, resolved=None)
    referenced = NS(
        id=51, author=NS(id=999),
        attachments=[NS(filename="voice.mp3")],
    )
    message = fake_message(runtime, reference=reference)
    message.channel.fetch_message.return_value = referenced
    message.reply.return_value = NS(id=103, content="reply", author=NS(id=999))
    interaction = classify("hello", user_id=7, channel_id=20, guild_id=30)
    interaction.user = {"proactive": True}

    item = await runtime._prepare_routed_message(message, interaction)
    assert interaction.direct
    assert await runtime._deliver_normal_reply(message, interaction, item, "reply")

    message.channel.fetch_message.assert_awaited_once_with(51)
    memory.add_message.assert_awaited_once_with(7, 20, "assistant", "reply")


@async_test
async def test_text_delivery_controls_assistant_memory(runtime, monkeypatch):
    message = fake_message(runtime)
    message.reply.return_value = NS(id=103, content="reply", author=NS(id=999))
    interaction = classify("hello", user_id=7, channel_id=20, guild_id=30)
    item = prepared()
    memory = NS(add_message=AsyncMock(), consume_phrase=AsyncMock(return_value=False))
    monkeypatch.setattr(runtime, "mem", memory)
    monkeypatch.setattr(runtime, "FISH_AUDIO_API_KEY", "")
    monkeypatch.setattr(runtime.random, "random", lambda: 1.0)
    monkeypatch.setattr(runtime, "maybe_react", AsyncMock())
    monkeypatch.setattr(runtime.troll, "after_reply", AsyncMock())

    assert await runtime._deliver_normal_reply(message, interaction, item, "reply")
    memory.add_message.assert_awaited_once_with(7, 20, "assistant", "reply")

    memory.add_message.reset_mock()
    message.reply.side_effect = discord_error(discord.Forbidden)
    assert not await runtime._deliver_normal_reply(message, interaction, item, "reply")
    memory.add_message.assert_not_awaited()
    assert message.reply.await_count == 2


@async_test
async def test_dm_delivery_runs_home_roommate_only_after_success(runtime, monkeypatch):
    message = fake_message(runtime, guild=False)
    message.reply.return_value = NS(id=103, content="reply", author=NS(id=999))
    interaction = classify("hello", user_id=7, channel_id=20, direct=True)
    item = prepared()
    item.is_dm = True
    memory = NS(add_message=AsyncMock(), consume_phrase=AsyncMock(return_value=False))
    home = NS(client=NS(enabled=True), roommate=AsyncMock())
    monkeypatch.setattr(runtime, "mem", memory)
    monkeypatch.setattr(runtime, "HOME", home)
    monkeypatch.setattr(runtime, "_record_delivered_reply", AsyncMock())
    monkeypatch.setattr(runtime, "FISH_AUDIO_API_KEY", "")
    monkeypatch.setattr(runtime.random, "random", lambda: 1.0)
    monkeypatch.setattr(runtime, "maybe_react", AsyncMock())
    monkeypatch.setattr(runtime.troll, "after_reply", AsyncMock())

    assert await runtime._deliver_normal_reply(message, interaction, item, "reply")
    home.roommate.assert_awaited_once_with(7, "reply", item.user)

    home.roommate.reset_mock()
    message.reply.side_effect = discord_error(discord.Forbidden)
    assert not await runtime._deliver_normal_reply(message, interaction, item, "reply")
    home.roommate.assert_not_awaited()


@async_test
async def test_home_roommate_failure_is_logged_without_breaking_dm_delivery(
    runtime, monkeypatch,
):
    message = fake_message(runtime, guild=False)
    message.reply.return_value = NS(id=103, content="reply", author=NS(id=999))
    interaction = classify("hello", user_id=7, channel_id=20, direct=True)
    item = prepared()
    item.is_dm = True
    errors = Mock()
    monkeypatch.setattr(runtime, "mem", NS(
        add_message=AsyncMock(), consume_phrase=AsyncMock(return_value=False),
    ))
    monkeypatch.setattr(runtime, "HOME", NS(
        client=NS(enabled=True), roommate=AsyncMock(side_effect=OSError("relay down")),
    ))
    monkeypatch.setattr(runtime, "_record_delivered_reply", AsyncMock())
    monkeypatch.setattr(runtime, "log_operation_error", errors)
    monkeypatch.setattr(runtime, "FISH_AUDIO_API_KEY", "")
    monkeypatch.setattr(runtime.random, "random", lambda: 1.0)
    monkeypatch.setattr(runtime, "maybe_react", AsyncMock())
    monkeypatch.setattr(runtime.troll, "after_reply", AsyncMock())

    assert await runtime._deliver_normal_reply(message, interaction, item, "reply")
    assert errors.call_args.kwargs["subsystem"] == "home"
    assert errors.call_args.kwargs["operation"] == "home_roommate"


@async_test
async def test_http_delivery_failure_stops_without_retry_or_memory(runtime, monkeypatch):
    message = fake_message(runtime)
    message.reply.side_effect = discord_error(discord.HTTPException)
    interaction = classify("hello", user_id=7, channel_id=20, guild_id=30)
    memory = NS(add_message=AsyncMock(), consume_phrase=AsyncMock(return_value=False))
    monkeypatch.setattr(runtime, "mem", memory)
    monkeypatch.setattr(runtime, "FISH_AUDIO_API_KEY", "")
    monkeypatch.setattr(runtime.random, "random", lambda: 1.0)

    assert not await runtime._deliver_normal_reply(
        message, interaction, prepared(), "reply",
    )
    message.reply.assert_awaited_once()
    memory.add_message.assert_not_awaited()


@async_test
async def test_deleted_attachment_stops_media_owner_cleanly(runtime, monkeypatch):
    image = NS(
        size=10, content_type="image/png", filename="image.png",
        read=AsyncMock(side_effect=discord_error(discord.NotFound)),
    )
    message = fake_message(runtime)
    message.attachments = [image]
    interaction = classify(
        "hello", user_id=7, channel_id=20, guild_id=30, media=True,
    )
    memory = NS(add_message=AsyncMock())
    errors = Mock()
    monkeypatch.setattr(runtime, "mem", memory)
    monkeypatch.setattr(runtime, "log_operation_error", errors)

    assert await runtime._handle_media(message, interaction, prepared())
    image.read.assert_awaited_once_with(use_cached=True)
    message.reply.assert_not_awaited()
    memory.add_message.assert_not_awaited()
    assert errors.call_args.kwargs["operation"] == "image_download"


@async_test
async def test_voice_failure_falls_back_to_text_and_records_delivery(runtime, monkeypatch):
    message = fake_message(runtime, content="send me a voice")
    message.reply.return_value = NS(id=103, content="reply", author=NS(id=999))
    interaction = classify(message.content, user_id=7, channel_id=20, guild_id=30)
    item = prepared(content=message.content)
    memory = NS(add_message=AsyncMock(), consume_phrase=AsyncMock(return_value=False))
    monkeypatch.setattr(runtime, "mem", memory)
    monkeypatch.setattr(runtime, "FISH_AUDIO_API_KEY", "configured")
    monkeypatch.setattr(runtime.random, "random", lambda: 0.5)
    monkeypatch.setattr(runtime, "send_voice", AsyncMock(return_value=False))
    monkeypatch.setattr(runtime, "maybe_react", AsyncMock())
    monkeypatch.setattr(runtime.troll, "after_reply", AsyncMock())

    assert await runtime._deliver_normal_reply(message, interaction, item, "reply")
    runtime.send_voice.assert_awaited_once()
    message.reply.assert_awaited_once_with("reply")
    memory.add_message.assert_awaited_once_with(7, 20, "assistant", "reply")


@async_test
async def test_generation_cancellation_propagates(runtime, monkeypatch):
    message = fake_message(runtime)
    interaction = classify("hello", user_id=7, channel_id=20, guild_id=30)
    monkeypatch.setattr(runtime, "typing_delay", AsyncMock(
        side_effect=asyncio.CancelledError(),
    ))
    with pytest.raises(asyncio.CancelledError):
        await runtime._generate_normal_reply(message, interaction, prepared(), "")


@async_test
async def test_observation_database_errors_are_noncritical_and_categorized(runtime, monkeypatch):
    message = fake_message(runtime)
    item = prepared()
    interaction = classify(
        "hello", user_id=7, channel_id=20, guild_id=30,
        user={"proactive": True}, direct=True,
    )
    world = NS(observe=AsyncMock(side_effect=sqlite3.OperationalError("locked")))
    memory = NS(increment_message_count=AsyncMock(
        side_effect=sqlite3.IntegrityError("constraint"),
    ))
    self_perception = AsyncMock()
    errors = Mock()
    monkeypatch.setattr(runtime, "WORLD", world)
    monkeypatch.setattr(runtime, "mem", memory)
    monkeypatch.setattr(runtime, "_record_tattletale_if_eligible", AsyncMock())
    monkeypatch.setattr(runtime, "_record_self_perception", self_perception)
    monkeypatch.setattr(runtime, "log_operation_error", errors)

    await runtime._observe_prepared_message(message, interaction, item)

    world.observe.assert_awaited_once()
    self_perception.assert_awaited_once()
    assert [call.kwargs["operation"] for call in errors.call_args_list] == [
        "world_observe", "message_observation",
    ]
    assert error_category(sqlite3.OperationalError()) == "sqlite_operational"
    assert error_category(sqlite3.IntegrityError()) == "sqlite_integrity"


@async_test
async def test_reaction_forbidden_is_logged_once_without_retry(runtime, monkeypatch):
    message = fake_message(runtime)
    message.add_reaction.side_effect = discord_error(discord.Forbidden)
    monkeypatch.setattr(runtime.random, "random", lambda: 0.0)
    monkeypatch.setattr(runtime, "qai", AsyncMock(return_value=runtime.SCARA_EMOJIS[0]))
    errors = Mock()
    monkeypatch.setattr(runtime, "log_operation_error", errors)

    await runtime.maybe_react(message, interaction=classify("hello"))

    message.add_reaction.assert_awaited_once()
    assert errors.call_args.kwargs["operation"] == "reaction"
    assert isinstance(errors.call_args.kwargs["error"], discord.Forbidden)


@async_test
async def test_provider_generation_is_called_once(runtime, monkeypatch):
    message = fake_message(runtime)
    interaction = classify("hello", user_id=7, channel_id=20, guild_id=30)
    provider = AsyncMock(return_value="reply")
    monkeypatch.setattr(runtime, "get_response", provider)
    monkeypatch.setattr(runtime, "typing_delay", AsyncMock())

    assert await runtime._generate_normal_reply(message, interaction, prepared(), "ctx") == "reply"
    provider.assert_awaited_once()
    assert provider.await_args.kwargs["defer_delivery"] is True


@async_test
async def test_provider_outage_gets_character_fallback_but_programming_bug_propagates(
    runtime, monkeypatch,
):
    message = fake_message(runtime)
    monkeypatch.setattr(runtime, "typing_delay", AsyncMock())
    monkeypatch.setattr(runtime.random, "choice", lambda choices: "Hmph.")
    interaction = classify("hello", user_id=7, channel_id=20, guild_id=30)
    monkeypatch.setattr(runtime, "get_response", AsyncMock(
        side_effect=RuntimeError("Groq provider is not configured"),
    ))
    assert await runtime._generate_normal_reply(
        message, interaction, prepared(), "",
    ) == "Hmph."

    bug_context = classify("hello", user_id=7, channel_id=20, guild_id=30)
    monkeypatch.setattr(runtime, "get_response", AsyncMock(side_effect=KeyError("bug")))
    with pytest.raises(KeyError):
        await runtime._generate_normal_reply(message, bug_context, prepared(), "")


def test_dispatch_has_one_classifier_and_one_user_load():
    source = Path(__file__).resolve().parents[1].joinpath("bot.py").read_text()
    tree = ast.parse(source)
    dispatch = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_dispatch_message"
    )
    rendered = ast.get_source_segment(source, dispatch)
    assert rendered.count("classify_interaction(") == 1
    assert rendered.count("await mem.get_user(") == 1


def test_structured_error_log_excludes_private_content_and_logs_unexpected_loudly():
    logger = Mock()
    message = NS(
        id=1, content="password=do-not-log", author=NS(id=7),
        channel=NS(id=20), guild=NS(id=30),
    )
    assert log_operation_error(
        logger, subsystem="message_pipeline", operation="test",
        error=KeyError("bug"), message=message,
    ) == "unexpected"
    logger.exception.assert_called_once()
    extra = logger.exception.call_args.kwargs["extra"]
    assert extra["message_id"] == 1
    assert extra["user_id"] == 7
    assert "content" not in extra
    assert "do-not-log" not in repr(extra)
