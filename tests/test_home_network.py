"""Loopback-only integration tests exercise real HTTP/WebSocket transports, no hardware."""

import asyncio
import base64
import time
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import aiohttp
from aiohttp.test_utils import TestServer
import pytest

from home.hub import Hub
from home.client import HomeClient
from home.protocol import canonical, sign, command, Rejected, verify
from home_agent.agent import Agent
from test_home_bridge import SECRET, device, location_config


def run(coro):
    return asyncio.run(coro)


async def eventually(predicate):
    for _ in range(200):
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("relay state did not converge")


@asynccontextmanager
async def system(tmp_path, *, mode="MANUAL", kind="hue", relay=True):
    d = device(kind, mode)
    config = {
        "mock": True,
        "enabled": True,
        "database": str(tmp_path / "hub.db"),
        **({"public_url": "http://localhost"} if kind == "cast" else {}),
        "clients": {"scaramouche": SECRET, "wanderer": SECRET + "w"},
        "agents": {"main": SECRET + "a"},
        "admins": [1],
        "devices": {"lamp": d},
        "owntracks": {
            "phone": dict(
                location_config(), user_id=1, username="phone", password=SECRET + "p"
            )
        },
    }
    hub = Hub(config)
    server = TestServer(hub.app)
    await server.start_server()
    url = str(server.make_url("/")).rstrip("/")
    client = HomeClient(
        "scaramouche", {"enabled": True, "mock": True, "url": url, "secret": SECRET}
    )
    agent = Agent(
        {
            "agent_id": "main",
            "secret": SECRET + "a",
            "url": url.replace("http:", "ws:"),
            "mock": True,
            "enabled": True,
            "media_origin": "http://localhost" if kind == "cast" else "https://home.example",
            "database": str(tmp_path / "agent.db"),
            "devices": {"lamp": d},
        }
    )
    await agent.store.init()
    task = asyncio.create_task(agent.connect_once()) if relay else None
    try:
        if task:
            await eventually(lambda: "main" in hub.agents)
        yield hub, agent, client, url, task
    finally:
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await server.close()


def test_outbound_roundtrip_replay_authentication_and_redaction(tmp_path):
    async def check():
        async with system(tmp_path) as (hub, agent, client, url, task):
            c = command("lamp", "brightness", {"value": 40}, 1, 0, "scaramouche")
            assert await client.call("action", 1, command=c) == {
                "ok": True,
                "result": "completed",
            }
            assert agent.adapters["hue"].calls[0][2] == {"value": 40}
            assert (await hub.store.audit())[0]["result"] == "completed"
            envelope = sign(SECRET, "rpc:scaramouche", {"op": "status", "user_id": 1})
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url + "/rpc/scaramouche", json=envelope
                ) as response:
                    body = await response.text()
                    assert (
                        response.status == 200
                        and SECRET not in body
                        and "application_key" not in body
                    )
                async with session.post(
                    url + "/rpc/scaramouche", json=envelope
                ) as response:
                    assert response.status == 400
                bad = sign(
                    SECRET + "wrong", "rpc:scaramouche", {"op": "status", "user_id": 1}
                )
                async with session.post(url + "/rpc/scaramouche", json=bad) as response:
                    assert (
                        response.status == 400 and SECRET not in await response.text()
                    )
            assert not (await client.call("status", 2))["ok"]
            assert not (await client.call("action", 1, command=c))["ok"]
            assert len(agent.adapters["hue"].calls) == 1

    run(check())


def test_duplicate_connection_reconnect_and_kill_switch(tmp_path):
    async def check():
        async with system(tmp_path) as (hub, agent, client, url, task):
            header = base64.b64encode(
                canonical(
                    sign(SECRET + "a", "agent-connect:main", {"agent_id": "main"})
                )
            ).decode()
            async with aiohttp.ClientSession() as session:
                with pytest.raises(aiohttp.WSServerHandshakeError):
                    await session.ws_connect(
                        url.replace("http:", "ws:") + "/agent/main",
                        headers={"X-Home-Auth": header},
                    )
            assert (await client.call("disable", 1))["ok"]
            c = command("lamp", "power", {"on": True}, 1, 0, "scaramouche")
            assert not (await client.call("action", 1, command=c))["ok"]
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await eventually(lambda: "main" not in hub.agents)
            assert (await client.call("enable", 1))["ok"]
            # A relay reconnect reconciles the enable it missed while offline.
            new = asyncio.create_task(agent.connect_once())
            try:
                await eventually(lambda: "main" in hub.agents)
                c = command("lamp", "power", {"on": True}, 1, 0, "scaramouche")
                assert (await client.call("action", 1, command=c))["ok"]
                assert len(agent.adapters["hue"].calls) == 1
            finally:
                new.cancel()
                await asyncio.gather(new, return_exceptions=True)

    run(check())


def test_confirmation_bound_to_user_bot_and_one_execution(tmp_path):
    async def check():
        async with system(tmp_path, mode="CONFIRM") as (hub, agent, client, url, task):
            c = command("lamp", "power", {"on": True}, 1, 0, "scaramouche")
            result = await client.call("action", 1, command=c)
            assert result["confirmation_required"] == c["request_id"]
            assert not agent.adapters["hue"].calls
            assert not (await client.call("confirm", 2, request_id=c["request_id"], guild_id=0))[
                "ok"
            ]
            assert not (await client.call(
                "confirm", 1, request_id=c["request_id"], guild_id=99
            ))["ok"]
            other = HomeClient("wanderer", dict(client.config, secret=SECRET + "w"))
            assert not (await other.call("confirm", 1, request_id=c["request_id"], guild_id=0))[
                "ok"
            ]
            assert (await client.call("confirm", 1, request_id=c["request_id"], guild_id=0))["ok"]
            assert not (await client.call("confirm", 1, request_id=c["request_id"], guild_id=0))[
                "ok"
            ]
            assert len(agent.adapters["hue"].calls) == 1

    run(check())


def test_owntracks_http_auth_optin_and_private_delete(tmp_path):
    async def check():
        async with system(tmp_path, relay=False) as (hub, agent, client, url, task):
            p = {"_type": "location", "lat": 10, "lon": 20, "tst": time.time()}
            async with aiohttp.ClientSession() as session:
                async with session.post(url + "/owntracks/phone", json=p) as response:
                    assert response.status == 400
                assert (await client.call("location_on", 1))["ok"]
                async with session.post(
                    url + "/owntracks/phone",
                    json=p,
                    auth=aiohttp.BasicAuth("phone", SECRET + "p"),
                ) as response:
                    assert response.status == 200 and await response.json() == []
                async with session.post(
                    url + "/owntracks/unknown",
                    json=p,
                    auth=aiohttp.BasicAuth("phone", SECRET + "p"),
                ) as response:
                    assert response.status == 400
            assert not (await client.call("events", 2))["ok"]
            received = await client.call("events", 1)
            assert received["events"][0]["zone"] == "HOME"
            assert set(received["events"][0]) == {"id", "ts", "kind", "zone"}
            assert (await client.call("location_delete", 1))["ok"]
            assert not await hub.store.get("location_optin:1")
            assert (await client.call("events", 1))["events"] == []

    run(check())


def test_offline_no_queue_and_timeout_audit(tmp_path):
    async def check():
        async with system(tmp_path, relay=False) as (hub, agent, client, url, task):
            c = command("lamp", "power", {"on": True}, 1, 0, "scaramouche")
            assert (await client.call("action", 1, command=c))[
                "error"
            ] == "relay_offline"
            assert (
                not hub.pending and (await hub.store.audit())[0]["result"] == "offline"
            )
            hub.devices["second"] = dict(device())
            ws = type(
                "WS",
                (),
                {"closed": False, "send_json": AsyncMock(), "close": AsyncMock()},
            )()
            hub.agents["main"] = ws
            c = command("second", "power", {"on": True}, 1, 0, "scaramouche")
            c["expires_at"] = time.time() + 0.1
            assert (await hub.dispatch(c))["error"] == "relay_timeout"
            assert (
                not hub.pending and (await hub.store.audit())[0]["result"] == "timeout"
            )

    run(check())


def test_temporary_audio_roundtrip_cleanup_and_failed_asset_audit(tmp_path):
    async def check():
        async with system(tmp_path, kind="cast") as (hub, agent, client, url, task):
            uploaded = await client.call(
                "audio",
                1,
                data=base64.b64encode(b"ID3" + b"0" * 30).decode(),
                mime="audio/mpeg",
            )
            key = uploaded["asset"]
            async with aiohttp.ClientSession() as session:
                async with session.get(url + "/audio/" + key) as response:
                    assert (
                        response.status == 200
                        and response.headers["Cache-Control"] == "no-store"
                    )
            c = command(
                "lamp", "play", {"asset": key, "volume": 0.2}, 1, 0, "scaramouche"
            )
            assert (await client.call("action", 1, command=c))["ok"]
            assert not hub.vault.items
            assert agent.adapters["cast"].calls[0][2]["volume"] == 0.2
            hub.devices["second"] = device("cast")
            c = command(
                "second",
                "play",
                {"asset": "missing", "volume": 0.2},
                1,
                0,
                "scaramouche",
            )
            assert not (await hub.dispatch(c))["ok"]
            assert (await hub.store.audit())[0]["result"] == "expired"

    run(check())


def test_physical_failure_cancel_and_stale_are_not_retried(tmp_path):
    async def check():
        async with system(tmp_path) as (hub, agent, client, url, task):
            provider = agent.adapters["hue"]
            entered = asyncio.Event()

            async def slow(*args):
                entered.set()
                await asyncio.Event().wait()

            provider.execute = slow
            c = command("lamp", "power", {"on": True}, 1, 0, "scaramouche")
            request = asyncio.create_task(client.call("action", 1, command=c))
            await asyncio.wait_for(entered.wait(), 2)
            assert (await client.call("disable", 1, device="lamp"))["ok"]
            await eventually(lambda: not agent.tasks)
            request.cancel()
            await asyncio.gather(request, return_exceptions=True)
            assert (await agent.store.audit())[0]["result"] == "cancelled"
            c["expires_at"] = time.time() - 1
            assert not (await client.call("action", 1, command=c))["ok"]

    run(check())


def test_result_is_correlated_to_request_agent_and_device_and_duplicate_is_harmless(tmp_path):
    async def check():
        async with system(tmp_path, relay=False) as (hub, agent, client, url, task):
            header = base64.b64encode(
                canonical(sign(SECRET + "a", "agent-connect:main", {"agent_id": "main"}))
            ).decode()
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    url.replace("http:", "ws:") + "/agent/main",
                    headers={"X-Home-Auth": header},
                ) as ws:
                    # Persistent global and per-device kill-switch reconciliation.
                    await ws.receive_json()
                    await ws.receive_json()
                    await ws.send_json({
                        "type": "hello", "devices": ["lamp"], "capabilities": ["hue"]
                    })
                    await eventually(lambda: "main" in hub.agents)
                    c = command("lamp", "power", {"on": True}, 1, 0, "scaramouche")
                    pending = asyncio.create_task(client.call("action", 1, command=c))
                    envelope = await ws.receive_json()
                    payload = verify(SECRET + "a", "command:main", envelope)
                    assert payload["command"] == c
                    wrong = {
                        "type": "result", "request_id": c["request_id"],
                        "device_id": "other", "result": "completed",
                    }
                    await ws.send_json(wrong)
                    await asyncio.sleep(0.05)
                    assert not pending.done()
                    correct = dict(wrong, device_id="lamp")
                    await ws.send_json(correct)
                    assert (await pending)["ok"]
                    await ws.send_json(correct)  # stale duplicate cannot execute/complete again
                    await ws.send_json({
                        "type": "health", "devices": ["lamp"], "capabilities": ["hue"]
                    })
                    await asyncio.sleep(0.05)
                    assert not ws.closed and len(await hub.store.audit()) == 1

    run(check())


def test_agent_must_advertise_exact_configured_device_set(tmp_path):
    async def check():
        async with system(tmp_path, relay=False) as (hub, agent, client, url, task):
            header = base64.b64encode(
                canonical(sign(SECRET + "a", "agent-connect:main", {"agent_id": "main"}))
            ).decode()
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    url.replace("http:", "ws:") + "/agent/main",
                    headers={"X-Home-Auth": header},
                ) as ws:
                    await ws.receive_json()
                    await ws.receive_json()
                    await ws.send_json({"type": "hello", "devices": [], "capabilities": []})
                    for _ in range(100):
                        if ws.closed:
                            break
                        message = await ws.receive(timeout=0.1)
                        if message.type in {aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED}:
                            break
                    await eventually(lambda: "main" not in hub.agents)

    run(check())
