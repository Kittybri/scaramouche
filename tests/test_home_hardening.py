"""Repair Batch 9B trust-boundary and deterministic UX regressions."""

import asyncio
import copy
import json
import time
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest
import aiohttp
from aiohttp.test_utils import TestServer

from home.bot_integration import HomeBot, natural_intent, render_diagnostic
from home.config import load_config, validate_agent_config
from home.hub import Hub
from home.client import HomeClient
from home.permissions import authorize
from home.protocol import Rejected, command, sign
from home.storage import Store
from home_agent.agent import Agent
from test_home_bridge import SECRET, NOW, cmd, device
from test_home_network import system


def run(coro):
    return asyncio.run(coro)


def aliases():
    return {
        "device_aliases": {
            "bedroom_lamp": {
                "friendly_name": "bedroom lamp",
                "aliases": ["bedroom lamp", "light"],
                "actions": ["power", "brightness", "color"],
                "colors": ["purple"],
            },
            "desk_lamp": {
                "friendly_name": "desk lamp",
                "aliases": ["desk lamp", "light"],
                "actions": ["power"],
            },
            "bedroom_speaker": {
                "friendly_name": "bedroom speaker",
                "aliases": ["bedroom speaker"],
                "actions": ["speak"],
            },
        }
    }


def test_strict_config_loader_and_cross_references(tmp_path):
    duplicate = tmp_path / "duplicate.json"
    duplicate.write_text('{"devices":{},"devices":{}}')
    with pytest.raises(Rejected, match="duplicate_configuration_key"):
        load_config(duplicate)

    config = {
        "mock": True,
        "enabled": False,
        "clients": {"scaramouche": SECRET},
        "agents": {"main": SECRET + "a"},
        "devices": {"lamp": device()},
    }
    Hub(config)
    with pytest.raises(Rejected, match="unknown_device_agent"):
        Hub(dict(config, agents={"other": SECRET + "a"}))
    with pytest.raises(Rejected, match="weak_credential"):
        Hub(dict(config, clients={"scaramouche": "short"}))


def test_mock_agent_still_validates_provider_shape():
    config = {
        "agent_id": "main",
        "secret": SECRET,
        "url": "ws://localhost",
        "mock": True,
        "enabled": False,
        "devices": {"lamp": device()},
    }
    validate_agent_config(config)
    broken = copy.deepcopy(config)
    broken["devices"]["lamp"]["tls_fingerprint"] = "not-hex"
    with pytest.raises(Rejected, match="invalid_tls_pin"):
        validate_agent_config(broken)
    broken = copy.deepcopy(config)
    broken["devices"]["lamp"]["agent"] = "other"
    with pytest.raises(Rejected, match="wrong_device_agent"):
        validate_agent_config(broken)


def test_command_is_immutable_and_unsafe_modes_fail_closed():
    valid = cmd("brightness", {"value": 40})
    original = copy.deepcopy(valid)
    assert authorize(valid, {"lamp": device()}, True, NOW) == original
    assert valid == original
    with pytest.raises(Rejected, match="brightness_out_of_range"):
        authorize(cmd("brightness", {"value": 90}), {"lamp": device()}, True, NOW)

    plug = device("kasa", "AUTONOMOUS_SAFE")
    plug["category"] = "NEVER_AUTOMATE"
    for trigger in ("proposal", "autonomous", "roommate", "alarm"):
        value = cmd(trigger=trigger)
        if trigger == "proposal":
            value["confirmed"] = True
        with pytest.raises(Rejected, match="unsafe_plug"):
            authorize(value, {"lamp": plug}, True, NOW)

    printer = device("printer", "MANUAL")
    print_command = cmd("print_note", {"template": "posture"})
    with pytest.raises(Rejected, match="confirmation_required"):
        authorize(print_command, {"lamp": printer}, True, NOW)


def test_confirmation_is_atomic_and_user_bot_guild_bound(tmp_path):
    async def check():
        store = Store(tmp_path / "confirm.db")
        await store.init()
        proposal = command("lamp", "power", {"on": True}, 1, 9, "scaramouche")
        await store.put("proposal:" + proposal["request_id"], proposal)
        for user, guild, bot in (
            (2, 9, "scaramouche"),
            (1, 8, "scaramouche"),
            (1, 9, "wanderer"),
        ):
            with pytest.raises(Rejected, match="unknown_confirmation"):
                await store.consume_proposal(proposal["request_id"], user, guild, bot)
        assert await store.consume_proposal(proposal["request_id"], 1, 9, "scaramouche") == proposal
        with pytest.raises(Rejected, match="unknown_confirmation"):
            await store.consume_proposal(proposal["request_id"], 1, 9, "scaramouche")

    run(check())


def test_natural_parser_is_bounded_unambiguous_and_non_llm():
    config = aliases()
    result = natural_intent("Turn my bedroom lamp on.", config)
    assert result["device"] == "bedroom_lamp"
    assert result["action"] == "power" and result["parameters"] == {"on": True}
    assert natural_intent("Set the bedroom lamp to purple.", config)["parameters"] == {"name": "purple"}
    assert natural_intent("Turn the light on.", config) == {"ambiguous": True}
    assert natural_intent("Say take a break through my bedroom speaker.", config)["action"] == "speak"
    for text in ("I bought a Chromecast.", "Hue is a weird word.", "That printer is annoying."):
        assert natural_intent(text, config) is None


def test_natural_request_only_creates_confirmation_proposal():
    async def check():
        home = HomeBot("scaramouche", NS(), NS(), dict(aliases(), enabled=True), AsyncMock())
        home.client.call = AsyncMock(
            return_value={"ok": True, "confirmation_required": "a" * 32}
        )
        reply = await home.natural_proposal(
            1, 9, "Turn my bedroom lamp on.", {"home_actions_enabled": True}
        )
        assert "Confirm this exact action" in reply
        sent = home.client.call.call_args.kwargs["command"]
        assert sent["trigger"] == "proposal" and sent["confirmed"] is False
        assert sent["guild_id"] == 9 and sent["device_id"] == "bedroom_lamp"
        home.client.call.reset_mock()
        assert "matches more than one" in await home.natural_proposal(
            1, 9, "Turn the light on.", {"home_actions_enabled": True}
        )
        home.client.call.assert_not_awaited()

    run(check())


def test_both_bot_identities_complete_real_mock_transport(tmp_path):
    async def check():
        async with system(tmp_path) as (hub, agent, scara, url, task):
            from home.client import HomeClient

            wanderer = HomeClient(
                "wanderer", dict(scara.config, secret=SECRET + "w")
            )
            for name, client in (("scaramouche", scara), ("wanderer", wanderer)):
                value = command("lamp", "power", {"on": True}, 1, 0, name)
                assert (await client.call("action", 1, command=value))["ok"]
                async with hub.store.db() as db:
                    await db.execute("UPDATE home_audit SET ts=ts-301")
                    await db.commit()
                async with agent.store.db() as db:
                    await db.execute("UPDATE home_audit SET ts=ts-301")
                    await db.commit()
            assert [call[0]["bots"] for call in agent.adapters["hue"].calls] == [
                ["scaramouche", "wanderer"], ["scaramouche", "wanderer"]
            ]
            assert {row["bot"] for row in await hub.store.audit()} == {
                "scaramouche", "wanderer"
            }

    run(check())


def test_diagnostics_are_compact_and_do_not_render_network_details():
    result = {
        "ok": True,
        "enabled": True,
        "relay_reachable": True,
        "agents": {"main": {"health": "healthy", "health_age_seconds": 1.2}},
        "devices": {"lamp": {"type": "hue", "mode": "MANUAL", "agent_online": True}},
        "host": "192.168.1.2",
        "secret": SECRET,
    }
    for operation in ("status", "agent", "devices", "permissions"):
        rendered = render_diagnostic(operation, result)
        assert "192.168" not in rendered and SECRET not in rendered


def test_hub_restart_preserves_replay_and_disable_without_stale_queue(tmp_path):
    async def check():
        config = {
            "mock": True,
            "enabled": True,
            "database": str(tmp_path / "restart.db"),
            "clients": {"scaramouche": SECRET},
            "agents": {"main": SECRET + "a"},
            "admins": [1],
            "devices": {"lamp": device()},
        }
        envelope = sign(SECRET, "rpc:scaramouche", {"op": "status", "user_id": 1})
        first = TestServer(Hub(config).app)
        await first.start_server()
        try:
            url = str(first.make_url("/")).rstrip("/")
            async with aiohttp.ClientSession() as session:
                async with session.post(url + "/rpc/scaramouche", json=envelope) as response:
                    assert response.status == 200
            client = HomeClient(
                "scaramouche", {"enabled": True, "mock": True, "url": url, "secret": SECRET}
            )
            assert (await client.call("disable", 1))["ok"]
        finally:
            await first.close()

        second_hub = Hub(config)
        second = TestServer(second_hub.app)
        await second.start_server()
        try:
            url = str(second.make_url("/")).rstrip("/")
            async with aiohttp.ClientSession() as session:
                async with session.post(url + "/rpc/scaramouche", json=envelope) as response:
                    assert response.status == 400
            client = HomeClient(
                "scaramouche", {"enabled": True, "mock": True, "url": url, "secret": SECRET}
            )
            status = await client.call("status", 1)
            assert status["ok"] and status["enabled"] is False
            value = command("lamp", "power", {"on": True}, 1, 0, "scaramouche")
            assert not (await client.call("action", 1, command=value))["ok"]
            assert not second_hub.pending
        finally:
            await second.close()

    run(check())


def test_maintenance_retries_after_one_cleanup_failure(monkeypatch):
    async def check():
        hub = Hub({
            "mock": True,
            "enabled": False,
            "clients": {"scaramouche": SECRET},
            "agents": {"main": SECRET + "a"},
            "devices": {"lamp": device()},
        })
        hub.store.clean = AsyncMock(side_effect=[RuntimeError("temporary"), None])
        sleeps = 0

        async def bounded_sleep(_):
            nonlocal sleeps
            sleeps += 1
            if sleeps >= 2:
                raise asyncio.CancelledError

        monkeypatch.setattr("home.hub.asyncio.sleep", bounded_sleep)
        with pytest.raises(asyncio.CancelledError):
            await hub.maintain()
        assert hub.store.clean.await_count == 2

    run(check())


def test_cast_pause_can_interrupt_inflight_bot_session(tmp_path):
    async def check():
        started = asyncio.Event()
        release = asyncio.Event()

        class Adapter:
            def __init__(self):
                self.actions = []

            async def execute(self, _device, action, _parameters, _media=None):
                self.actions.append(action)
                if action == "play":
                    started.set()
                    await release.wait()
                return "completed"

            async def close(self):
                release.set()

        adapter = Adapter()
        cast = device("cast")
        agent = Agent(
            {
                "agent_id": "main",
                "secret": SECRET,
                "url": "ws://localhost",
                "media_origin": "http://localhost",
                "mock": True,
                "enabled": True,
                "database": str(tmp_path / "cast-agent.db"),
                "devices": {"speaker": cast},
            },
            adapters={"cast": adapter},
        )
        await agent.store.init()
        play = command(
            "speaker", "play", {"asset": "tone", "volume": 0.2},
            1, 0, "scaramouche",
        )
        play_task = asyncio.create_task(
            agent.execute(sign(SECRET, "command:main", {"command": play, "media": None}))
        )
        await asyncio.wait_for(started.wait(), 1)
        pause = command("speaker", "pause", {}, 1, 0, "scaramouche")
        paused = await asyncio.wait_for(
            agent.execute(sign(SECRET, "command:main", {"command": pause, "media": None})),
            1,
        )
        assert paused["result"] == "completed" and adapter.actions == ["play", "pause"]
        release.set()
        assert (await play_task)["result"] == "completed"

    run(check())
