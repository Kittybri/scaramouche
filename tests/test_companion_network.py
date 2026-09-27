"""Real loopback authenticated transport with a fake desktop, no GUI access."""

import asyncio
import time
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock
from aiohttp.test_utils import TestServer
import pytest
from home.hub import Hub
from home.client import HomeClient
from home.protocol import command, sign, Rejected
from home_agent.agent import Agent
from home_agent.computer.base import Sample
from test_companion import device, config, run
from test_home_network import eventually

SECRET = "synthetic-companion-test-secret-123456789"


def test_stop_interrupts_pending_action(tmp_path, monkeypatch):
    async def check():
        async with system(tmp_path, monkeypatch) as (hub, agent, client, task):
            await client.call("pc_on", 1)
            await eventually(lambda: agent.computer.enabled)
            playing, stopped = asyncio.Event(), asyncio.Event()

            async def long_action(*args):
                playing.set()
                await stopped.wait()

            async def stop():
                stopped.set()

            agent.computer.platform.action.side_effect = long_action
            agent.computer.platform.stop.side_effect = stop
            notify = command("pc", "notify", {"template": "test"}, 1, 0, "scaramouche")
            pending = asyncio.create_task(client.call("action", 1, command=notify))
            await asyncio.wait_for(playing.wait(), 3)
            result = await client.call(
                "action", 1, command=command("pc", "stop", {}, 1, 0, "scaramouche")
            )
            assert result["ok"] and (await pending)["ok"]

    run(check())


def test_confirmation_screen_roundtrip_and_revocation(tmp_path, monkeypatch):
    async def check():
        import base64

        async with system(tmp_path, monkeypatch) as (hub, agent, client, task):
            await client.call("pc_on", 1)
            await eventually(lambda: agent.computer.enabled)
            lock = command("pc", "lock", {}, 1, 0, "scaramouche")
            proposal = await client.call("action", 1, command=lock)
            assert proposal["confirmation_required"] == lock["request_id"]
            agent.computer.platform.action.assert_not_awaited()
            assert not (await client.call("confirm", 2, request_id=lock["request_id"]))[
                "ok"
            ]
            assert (await client.call("confirm", 1, request_id=lock["request_id"]))[
                "ok"
            ]
            # Separate device cooldown is intentional; age only test fixtures to exercise a second action.
            for store in (hub.store, agent.store):
                async with store.db() as db:
                    await db.execute("UPDATE home_audit SET ts=ts-61")
                    await db.commit()
            await client.call("pc_screen_on", 1)
            await eventually(lambda: agent.computer.consent["screen"])
            agent.computer.screenshot = Mock(
                return_value={
                    "image": base64.b64encode(b"\xff\xd8synthetic").decode(),
                    "app": "Editor",
                }
            )
            result = await client.call(
                "action", 1, command=command("pc", "screen", {}, 1, 0, "scaramouche")
            )
            assert result["ok"] and "screen" in result
            for store in (hub.store, agent.store):
                assert "synthetic" not in str(await store.audit())
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await eventually(lambda: "main" not in hub.agents)
            assert (await client.call("pc_forget", 1))["ok"]
            reconnect = asyncio.create_task(agent.connect_once())
            try:
                await eventually(lambda: "main" in hub.agents)
                for _ in range(100):
                    if not await agent.store.audit():
                        break
                    await asyncio.sleep(0.02)
                assert (
                    not agent.computer.enabled and not agent.computer.consent["screen"]
                )
                assert not await agent.store.audit()
            finally:
                reconnect.cancel()
                await asyncio.gather(reconnect, return_exceptions=True)

    run(check())


@asynccontextmanager
async def system(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPANION_AGENT_ENABLED", "true")
    hub = Hub(
        {
            "mock": True,
            "enabled": True,
            "database": str(tmp_path / "hub.db"),
            "devices": {"pc": device()},
            "clients": {"scaramouche": SECRET},
            "agents": {"main": SECRET},
        }
    )
    server = TestServer(hub.app)
    await server.start_server()
    url = str(server.make_url("/")).rstrip("/")
    client = HomeClient(
        "scaramouche", {"enabled": True, "mock": True, "url": url, "secret": SECRET}
    )
    agent = Agent(
        {
            "mock": True,
            "enabled": True,
            "agent_id": "main",
            "secret": SECRET,
            "url": url.replace("http:", "ws:"),
            "devices": {"pc": device()},
            "companion": config(),
            "database": str(tmp_path / "agent.db"),
        }
    )
    agent.computer.platform = Mock(
        sample=Mock(return_value=Sample("com.test.editor", 0, False, 1, "file.py")),
        action=AsyncMock(),
        stop=AsyncMock(),
    )
    await agent.store.init()
    task = asyncio.create_task(agent.connect_once())
    await eventually(lambda: "main" in hub.agents)
    try:
        yield hub, agent, client, task
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await server.close()


def test_consent_action_audit_replay_kill_and_disconnect(tmp_path, monkeypatch):
    async def check():
        async with system(tmp_path, monkeypatch) as (hub, agent, client, task):
            c = command("pc", "notify", {"template": "test"}, 1, 0, "scaramouche")
            assert not (await client.call("action", 1, command=c))["ok"]
            assert not (await client.call("pc_on", 2))["ok"]
            assert (await client.call("pc_on", 1))["ok"]
            await eventually(lambda: agent.computer.enabled)
            c = command("pc", "notify", {"template": "test"}, 1, 0, "scaramouche")
            assert (await client.call("action", 1, command=c))["ok"]
            agent.computer.platform.action.assert_awaited_once()
            assert (await hub.store.audit())[0]["action"] == "notify"
            assert not (await client.call("action", 1, command=c))["ok"]
            v = agent.computer.tick()
            await hub.companion.ingest("main", {"device": "pc", "presence": v})
            assert (await client.call("pc_status", 1))["presence"]["app"] == "Editor"
            assert (await client.call("pc_forget", 1))["ok"]
            await eventually(lambda: not agent.computer.enabled)
            assert not (await hub.store.audit())
            assert not (await client.call("pc_status", 1))["presence"]
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await eventually(lambda: "main" not in hub.agents)
            assert not hub.companion.current

    run(check())


def test_agent_expired_replay_and_kill_direct(tmp_path, monkeypatch):
    async def check():
        async with system(tmp_path, monkeypatch) as (hub, agent, client, task):
            await client.call("pc_on", 1)
            await eventually(lambda: agent.computer.enabled)
            c = command("pc", "notify", {"template": "test"}, 1, 0, "scaramouche")
            envelope = sign(SECRET, "command:main", {"command": c, "media": None})
            assert (await agent.execute(envelope))["result"] == "completed"
            with pytest.raises(Rejected, match="replayed"):
                await agent.execute(envelope)
            c["expires_at"] = time.time() - 1
            with pytest.raises(Rejected, match="expired"):
                await agent.execute(
                    sign(SECRET, "command:main", {"command": c, "media": None})
                )
            monkeypatch.setenv("COMPANION_AGENT_ENABLED", "false")
            c = command("pc", "test", {}, 1, 0, "scaramouche")
            with pytest.raises(Rejected):
                await agent.execute(
                    sign(SECRET, "command:main", {"command": c, "media": None})
                )

    run(check())
