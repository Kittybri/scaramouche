"""Security and policy regressions use isolated databases and no physical hardware."""

import asyncio
import copy
import json
import time
from datetime import datetime, timezone
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock
import pytest
from home.protocol import command, validate, sign, verify, Rejected, secure_url
from home.permissions import authorize, registry
from home.storage import Store
from home.location import ingest, events, delete_location
from home.media import AudioVault
from home.bot_integration import candidate, quiet, HomeBot
from home_agent.agent import Agent, backoff
from home_agent.devices import MockProvider, lan_host
from home_agent.ipp import packet, parse, attribute, Printer

SECRET = "unit-test-credential-not-production-000000"
NOW = datetime(2026, 1, 1, 12, tzinfo=timezone.utc).timestamp()


def run(c):
    return asyncio.run(c)


def device(kind="hue", mode="MANUAL"):
    value = {
        "type": kind,
        "enabled": True,
        "mode": mode,
        "actions": {
            "hue": [
                "power",
                "brightness",
                "scene",
                "color",
                "color_temperature",
                "test",
            ],
            "kasa": ["power", "test"],
            "cast": ["play", "pause", "stop", "volume", "test"],
            "printer": ["print_note", "test"],
        }[kind],
        "users": [1],
        "bots": ["scaramouche", "wanderer"],
        "guilds": [0],
        "hours": [0, 0],
        "agent": "main",
        "colors": {"purple": {"x": 0.2, "y": 0.2}},
        "scenes": {"calm": "00000000-0000-0000-0000-000000000001"},
    }
    if kind == "hue":
        value.update(
            host="192.168.1.2",
            resource_id="00000000-0000-0000-0000-000000000002",
            application_key="local-test-key",
            tls_fingerprint="00" * 32,
        )
    elif kind == "kasa":
        value.update(host="192.168.1.3")
    elif kind == "cast":
        value.update(
            host="192.168.1.4",
            uuid="00000000-0000-0000-0000-000000000003",
            asset_urls={},
        )
    elif kind == "printer":
        value.update(queue="safe_queue")
    return value


def cmd(action="power", p=None, trigger="manual", now=NOW):
    return command(
        "lamp",
        action,
        p if p is not None else {"on": True},
        1,
        0,
        "scaramouche",
        trigger,
        now,
    )


def test_authentication_tamper_expiry_domain():
    signed = sign(SECRET, "command:main", {"a": 1}, now=NOW)
    assert verify(SECRET, "command:main", signed, now=NOW) == {"a": 1}
    for secret, domain, stamp in [
        (SECRET + "x", "command:main", NOW),
        (SECRET, "result:main", NOW),
        (SECRET, "command:main", NOW + 31),
    ]:
        with pytest.raises(Rejected):
            verify(secret, domain, signed, now=stamp)
    signed["payload"]["a"] = 2
    with pytest.raises(Rejected):
        verify(SECRET, "command:main", signed, now=NOW)


@pytest.mark.parametrize(
    "changes",
    [
        {"action": "shell"},
        {"device_id": "http://evil"},
        {"parameters": {"on": True, "exec": "x"}},
        {"expires_at": NOW - 1},
        {"expires_at": NOW + 10000},
        {"bot": "imposter"},
        {"user_id": True},
        {"issued_at": float("nan")},
    ],
)
def test_protocol_fail_closed(changes):
    with pytest.raises(Rejected):
        validate(dict(cmd(), **changes), NOW)


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com",
        "https://name:password@example.com",
        "https://example.com?token=x",
        "file:///tmp/x",
    ],
)
def test_tls_only(url):
    with pytest.raises(Rejected):
        secure_url(url)


def test_modes_and_confirmation():
    for mode in ("DISABLED", "MANUAL", "CONFIRM"):
        with pytest.raises(Rejected):
            authorize(cmd(trigger="autonomous"), {"lamp": device(mode=mode)}, True, NOW)
    with pytest.raises(Rejected, match="confirmation"):
        authorize(cmd(), {"lamp": device(mode="CONFIRM")}, True, NOW)
    assert authorize(
        dict(cmd(), confirmed=True), {"lamp": device(mode="CONFIRM")}, True, NOW
    )
    assert authorize(
        cmd(trigger="autonomous"), {"lamp": device(mode="AUTONOMOUS_SAFE")}, True, NOW
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("users", [2]),
        ("bots", ["wanderer"]),
        ("guilds", [9]),
        ("actions", []),
        ("enabled", False),
        ("hours", [14, 15]),
    ],
)
def test_policy_scope(field, value):
    d = device()
    d[field] = value
    with pytest.raises(Rejected):
        authorize(cmd(), {"lamp": d}, True, NOW)


def test_limits_presets_no_strobe():
    d = device()
    with pytest.raises(Rejected, match="brightness_out_of_range"):
        authorize(cmd("brightness", {"value": 1000}), {"lamp": d}, True, NOW)
    valid = cmd("brightness", {"value": 40})
    before = copy.deepcopy(valid)
    assert authorize(valid, {"lamp": d}, True, NOW) == before
    with pytest.raises(Rejected):
        authorize(cmd("scene", {"name": "not_allowed"}), {"lamp": d}, True, NOW)
    with pytest.raises(Rejected):
        cmd("strobe", {})
    cast = device("cast")
    with pytest.raises(Rejected, match="volume_out_of_range"):
        authorize(cmd("volume", {"value": 100}), {"lamp": cast}, True, NOW)


@pytest.mark.parametrize("category", ["NEVER_AUTOMATE", None])
def test_unsafe_plug_cannot_automate(category):
    d = device("kasa", "AUTONOMOUS_SAFE")
    if category:
        d["category"] = category
    with pytest.raises(Rejected, match="unsafe_plug"):
        authorize(cmd(trigger="autonomous"), {"lamp": d}, True, NOW)
    assert authorize(cmd(), {"lamp": d}, True, NOW)


def test_safe_plug_and_global_disable():
    d = device("kasa", "AUTONOMOUS_SAFE")
    d["category"] = "DECORATIVE"
    assert authorize(cmd(trigger="autonomous"), {"lamp": d}, True, NOW)
    for kind in ("hue", "kasa", "cast", "printer"):
        action, p = {
            "hue": ("power", {"on": True}),
            "kasa": ("power", {"on": True}),
            "cast": ("stop", {}),
            "printer": ("print_note", {"template": "posture"}),
        }[kind]
        with pytest.raises(Rejected, match="automation_disabled"):
            authorize(cmd(action, p), {"lamp": device(kind)}, False, NOW)


def test_replay_quota_audit_and_retention(tmp_path):
    async def check():
        s = Store(tmp_path / "home.db")
        await s.init()
        await s.init()
        assert await s.claim("nonce") and not await s.claim("nonce")
        d = device("printer")
        d.update(cooldown_seconds=60, max_jobs_day=2)
        for n in range(2):
            c = cmd("print_note", {"template": "posture"}, now=NOW + 120 * n)
            await s.reserve(c, d, NOW + 120 * n)
            await s.result(c["request_id"], "submitted")
        with pytest.raises(Rejected, match="quota"):
            await s.reserve(
                cmd("print_note", {"template": "posture"}, now=NOW + 300), d, NOW + 300
            )
        rows = await s.audit()
        assert len(rows) == 2 and all(
            set(r)
            == {
                "request_id",
                "ts",
                "bot",
                "user_id",
                "device",
                "action",
                "trigger",
                "result",
            }
            for r in rows
        )
        await s.clean()
        assert not await s.audit()

    run(check())


def test_concurrent_cooldown(tmp_path):
    async def check():
        s = Store(tmp_path / "home.db")
        await s.init()
        results = await asyncio.gather(
            *(s.reserve(cmd(), device(), NOW) for _ in range(5)), return_exceptions=True
        )
        assert sum(r is None for r in results) == 1

    run(check())


def location_config():
    return {
        "enabled": True,
        "bots": ["scaramouche", "wanderer"],
        "zones": {"HOME": {"latitude": 10.0, "longitude": 20.0, "radius_meters": 100}},
    }


def test_location_privacy_debounce_optout_and_delete(tmp_path):
    async def check():
        s = Store(tmp_path / "home.db")
        await s.init()
        now = time.time()
        p = {"_type": "location", "lat": 10.0, "lon": 20.0, "tst": now}
        with pytest.raises(Rejected):
            await ingest(s, 1, location_config(), p, now)
        await s.put("location_optin:1", True)
        assert await ingest(s, 1, location_config(), p, now)
        assert not await ingest(s, 1, location_config(), dict(p, tst=now + 1), now + 1)
        assert not await events(s, 2, "scaramouche")
        received = await events(s, 1, "scaramouche")
        assert received[0]["zone"] == "HOME" and "lat" not in json.dumps(
            received
        ).replace("location", "")
        assert "longitude" not in json.dumps(await s.get("location_last:1"))
        assert await ingest(
            s,
            1,
            location_config(),
            {"_type": "transition", "desc": "HOME", "event": "leave", "tst": now + 400},
            now + 400,
        )
        assert (await events(s, 1, "wanderer"))[0]["kind"] == "location.left"
        await delete_location(s, 1)
        assert not await events(s, 1, "wanderer") and not await s.get(
            "location_optin:1"
        )
        assert await s.get("location_last:1") is None

    run(check())


def test_location_unknown_zone_stale(tmp_path):
    async def check():
        s = Store(tmp_path / "home.db")
        await s.init()
        await s.put("location_optin:1", True)
        for p in [
            {
                "_type": "transition",
                "desc": "PRIVATE_ADDRESS",
                "event": "enter",
                "tst": time.time(),
            },
            {"_type": "location", "lat": 10, "lon": 20, "tst": 1},
        ]:
            with pytest.raises(Rejected):
                await ingest(s, 1, location_config(), p)

    run(check())


def test_audio_capability_expiry_ownership_and_mime():
    import base64

    vault = AudioVault("https://example.test")
    key = vault.add(
        base64.b64encode(b"ID3" + b"0" * 50).decode(), "audio/mpeg", 1, "scaramouche"
    )
    assert vault.resolve(key, 1, "scaramouche")["url"].endswith(key)
    with pytest.raises(Rejected):
        vault.resolve(key, 2, "scaramouche")
    with pytest.raises(Rejected):
        vault.add(
            base64.b64encode(b"not audio").decode(), "audio/mpeg", 1, "scaramouche"
        )
    vault.items[key]["expires"] = 0
    with pytest.raises(Rejected):
        vault.fetch(key)
    assert not vault.items


def test_printer_rejects_private_content_and_page_options():
    for p in (
        {"template": "secret_conversation"},
        {"template": "posture", "pages": 100},
        {"template": "posture", "text": "password"},
    ):
        with pytest.raises(Rejected):
            cmd("print_note", p)
    data, rid = packet(2, "office", "posture")
    assert b"Sit up straight." in data and b"page-ranges" in data and b"copies" in data
    import struct

    response = (
        struct.pack(">BBHI", 2, 0, 0, rid)
        + b"\x02"
        + attribute(0x23, "job-state", struct.pack(">i", 9))
        + b"\x03"
    )
    assert parse(response, rid)["job-state"] == 9
    with pytest.raises(Rejected):
        parse(response, rid + 1)


def test_resolver_never_grants_permission():
    rules = [
        {
            "signal": "irritated",
            "device": "lamp",
            "action": "power",
            "parameters": {"on": False},
        }
    ]
    assert candidate("scaramouche", {"mood": -8, "proactive": True}, rules) is None
    p = candidate(
        "scaramouche",
        {"mood": -8, "proactive": True, "home_actions_enabled": True},
        rules,
    )
    with pytest.raises(Rejected):
        authorize(
            cmd(p["action"], p["parameters"], "autonomous"),
            {"lamp": device()},
            True,
            NOW,
        )
    assert (
        candidate(
            "scaramouche",
            {"mood": -8, "proactive": True, "home_actions_enabled": True},
            [{"signal": "RUN_SHELL"}],
        )
        is None
    )


def test_quiet_hours_and_disabled_client():
    from home.client import HomeClient

    assert quiet(
        {"quiet_hours_start": 23, "quiet_hours_end": 8}, datetime(2026, 1, 1, 1)
    )
    assert not quiet({"quiet_hours_start": 0, "quiet_hours_end": 0})
    assert (
        run(HomeClient("scaramouche", {}).call("status", 1))["error"]
        == "home_not_configured"
    )


def test_agent_mock_execute_replay_offline_limits(tmp_path):
    async def check():
        adapter = MockProvider()
        a = Agent(
            {
                "agent_id": "main",
                "secret": SECRET,
                "url": "https://example.test".replace("https", "wss"),
                "enabled": True,
                "mock": True,
                "devices": {"lamp": device()},
                "database": str(tmp_path / "agent.db"),
            },
            adapters={"hue": adapter},
        )
        await a.store.init()
        c = cmd(now=time.time())
        envelope = sign(SECRET, "command:main", {"command": c, "media": None})
        assert (await a.execute(envelope))["result"] == "completed"
        with pytest.raises(Rejected):
            await a.execute(envelope)
        assert len(adapter.calls) == 1
        await a.store.put("disabled", True)
        with pytest.raises(Rejected):
            await a.execute(
                sign(
                    SECRET,
                    "command:main",
                    {"command": cmd(now=time.time()), "media": None},
                )
            )

    run(check())
    assert 2 <= backoff(1) < 4 and 60 <= backoff(10) < 62


def test_local_host_restriction():
    assert lan_host("192.168.1.2")
    for host in ("8.8.8.8", "example.com", "0.0.0.0"):
        with pytest.raises(Rejected):
            lan_host(host)


def test_config_rejects_truthy_strings_placeholder_keys_and_nonfinite_limits():
    from home.hub import Hub
    from home.protocol import secret_ok

    assert not secret_ok("REPLACE_WITH_RANDOM_SCARAMOUCHE_SECRET")
    with pytest.raises(Rejected):
        Hub({"enabled": "false"})
    for update in (
        {"enabled": "false"},
        {"max_volume": float("nan")},
        {"users": [True]},
        {"hours": [25, 8]},
    ):
        with pytest.raises(Rejected):
            registry({"lamp": dict(device(), **update)})


def test_idle_retention_removes_private_zone_and_proposal(tmp_path):
    async def check():
        s = Store(tmp_path / "retention.db")
        await s.init()
        await s.put("location_last:1", {"zone": "HOME", "ts": 0})
        await s.put("proposal:old", {"expires_at": 0})
        await s.put("disabled", True)
        await s.clean()
        assert (
            await s.get("location_last:1") is None
            and await s.get("proposal:old") is None
        )
        assert await s.get("disabled") is True

    run(check())
