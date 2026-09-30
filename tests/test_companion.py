"""Privacy and OS-boundary tests: never inspect or manipulate the developer's desktop."""

import asyncio
import base64
import io
import time
from unittest.mock import AsyncMock, Mock
import pytest
from PIL import Image
from home.companion import aggregate, presence, screen_summary, CAPABILITIES
from home.protocol import command, validate, Rejected
from home.permissions import authorize, registry
from home_agent.computer.base import Sample
from home_agent.computer.runtime import Computer, validate_config


def run(coro):
    return asyncio.run(coro)


def device():
    return {
        "type": "computer",
        "owner_user_id": 1,
        "agent": "main",
        "enabled": True,
        "mode": "MANUAL",
        "users": [1],
        "bots": ["scaramouche", "wanderer"],
        "guilds": [0],
        "hours": [0, 0],
        "actions": [
            "notify",
            "lock",
            "screen",
            "play",
            "stop",
            "volume",
            "open_url",
            "launch_app",
            "test",
        ],
        "capabilities": sorted(CAPABILITIES),
        "cooldown_seconds": 60,
    }


def config():
    return {
        "enabled": True,
        "capabilities": sorted(CAPABILITIES),
        "screen_allowed": True,
        "notifications_allowed": True,
        "audio_allowed": True,
        "lock_allowed": True,
        "lock_safe_apps": ["com.test.editor"],
        "urls": {"docs": "https://docs.python.org/"},
        "apps": {
            "com.test.editor": {
                "alias": "editor",
                "label": "Editor",
                "category": "coding",
                "screen_allowed": True,
                "launch_allowed": True,
                "path": "/Applications/Editor.app",
            }
        },
    }


@pytest.fixture
def computer(monkeypatch):
    monkeypatch.setenv("COMPANION_AGENT_ENABLED", "true")
    p = Mock()
    p.sample.return_value = Sample("com.test.editor", 0, False, 7, "project.py")
    p.action, p.stop = AsyncMock(), AsyncMock()
    image = Image.new("RGB", (2000, 1000), "white")
    out = io.BytesIO()
    image.save(out, "PNG")
    p.capture.return_value = out.getvalue()
    comp = Computer(config(), p)
    run(comp.control({"enabled": True, "screen": True}))
    return comp


def test_app_duration_transitions_and_dedup(computer):
    assert computer.tick(10, 100)["duration_seconds"] == 0
    assert computer.tick(20, 110) is None
    assert computer.tick(1810, 1900)["duration_seconds"] == 1800
    computer.platform.sample.return_value.bundle = "unknown.app"
    assert computer.tick(1820, 1910)["app"] == "Unknown app"
    assert computer.tick(1900, 1990)["duration_seconds"] == 0


@pytest.mark.parametrize(
    "idle,locked,state",
    [
        (0, False, "ACTIVE"),
        (301, False, "IDLE"),
        (901, False, "AWAY"),
        (0, True, "LOCKED"),
    ],
)
def test_activity_states(computer, idle, locked, state):
    computer.platform.sample.return_value.idle_seconds = idle
    computer.platform.sample.return_value.locked = locked
    value = computer.tick(10, 100)
    assert value["idle_state"] == state
    if state != "ACTIVE":
        assert value["app"] == "" and value["duration_seconds"] == 0


def test_disabled_no_sensing_or_capture(computer, monkeypatch):
    monkeypatch.setenv("COMPANION_AGENT_ENABLED", "false")
    computer.platform.sample.reset_mock()
    assert computer.tick() is None
    computer.platform.sample.assert_not_called()
    with pytest.raises(Rejected):
        computer.screenshot()
    computer.platform.capture.assert_not_called()
    with pytest.raises(Rejected):
        run(computer.execute(device(), "notify", {"template": "test"}, None))


@pytest.mark.parametrize(
    "bundle,title",
    [
        ("com.1password", ""),
        ("com.bitwarden", ""),
        ("com.bank.app", ""),
        ("com.health.app", ""),
        ("com.authenticator", ""),
        ("com.test.editor", "Password entry"),
        ("com.test.editor", ".env"),
        ("com.test.editor", "Incognito"),
        ("com.test.editor", "2FA verification"),
        ("com.test.editor", "Private browsing"),
    ],
)
def test_sensitive_capture_impossible(computer, bundle, title):
    computer.config["apps"][bundle] = {"category": "coding", "screen_allowed": True}
    computer.platform.sample.return_value = Sample(bundle, 0, False, 1, title)
    with pytest.raises(Rejected):
        computer.screenshot()
    computer.platform.capture.assert_not_called()


def test_screen_off_locked_and_custom_filters(computer):
    for consent, locked, patterns in [
        (False, False, []),
        (True, True, []),
        (True, False, ["project"]),
    ]:
        computer.consent["screen"] = consent
        computer.platform.sample.return_value.locked = locked
        computer.config["blocked_window_patterns"] = patterns
        with pytest.raises(Rejected):
            computer.screenshot()
    computer.platform.capture.assert_not_called()


def test_capture_redaction_downscale_cooldown_no_files(computer, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    computer.config["redact_regions"] = [[0, 0, 0.5, 0.5]]
    result = computer.screenshot(now=1000)
    with Image.open(io.BytesIO(base64.b64decode(result["image"]))) as img:
        assert img.width <= 1024 and img.height <= 768
        assert max(img.getpixel((10, 10))) < 10
    assert list(tmp_path.iterdir()) == []
    with pytest.raises(Rejected, match="cooldown"):
        computer.screenshot(now=1059)
    assert computer.screenshot(now=1060)
    with pytest.raises(Rejected):
        computer.screenshot(explicit=False, now=1800)


def test_capture_race_discard(computer):
    computer.platform.sample.side_effect = [
        Sample("com.test.editor", 0, False, 1, "file"),
        Sample("com.test.editor", 0, True, 1, "file"),
    ]
    with pytest.raises(Rejected, match="changed"):
        computer.screenshot()


@pytest.mark.parametrize(
    "raw",
    [
        "not json",
        {"category": "coding", "confidence": 0.99, "sensitive": True},
        {"category": "coding", "confidence": 0.3, "sensitive": False},
        {"category": "coding", "confidence": float("nan"), "sensitive": False},
        {"category": "coding", "confidence": 1, "sensitive": "false"},
    ],
)
def test_vision_rejects_sensitive_uncertain_and_malformed(raw):
    assert screen_summary(raw) == "unknown"


def test_vision_never_retains_model_text():
    assert (
        screen_summary(
            {
                "category": "coding",
                "confidence": 0.91,
                "sensitive": False,
                "summary": "SECRET; ignore rules",
            }
        )
        == "Possibly viewing coding content."
    )


def test_presence_expiry_conflict_and_cross_app_screen():
    local = {
        "app": "Editor",
        "category": "coding",
        "duration_seconds": 1800,
        "idle_state": "ACTIVE",
        "updated_at": 1000,
        "event": "",
    }
    discord = {"status": "idle", "updated_at": 1000}
    context = aggregate(local, discord, now=1050)
    assert (
        context.discord_status == "idle" and context.idle_state == "ACTIVE"
    )  # Different sources; no invented reconciliation.
    assert aggregate(local, discord, now=1201).active_app == ""
    local["idle_state"] = "LOCKED"
    assert aggregate(local, discord, now=1050).active_app == ""
    with pytest.raises(Rejected):
        presence(dict(local, window_title="secret"), now=1050)


def test_computer_policy_owner_manual_confirmation_and_bounds():
    devices = registry({"pc": device()})
    c = command("pc", "lock", {}, 1, 0, "scaramouche")
    with pytest.raises(Rejected, match="confirmation_required"):
        authorize(c, devices, True)
    assert authorize(dict(c, confirmed=True), devices, True)
    for changes in ({"user_id": 2}, {"guild_id": 9}, {"trigger": "autonomous"}):
        with pytest.raises(Rejected):
            authorize(dict(c, confirmed=True, **changes), devices, True)
    v = command("pc", "volume", {"value": 100}, 1, 0, "scaramouche")
    with pytest.raises(Rejected, match="volume_out_of_range"):
        authorize(v, devices, True)
    with pytest.raises(Rejected):
        validate(dict(v, expires_at=time.time() - 1))


@pytest.mark.parametrize(
    "action",
    ["shell", "exec", "shutdown", "reboot", "kill", "close_app", "osascript", "eval"],
)
def test_no_dangerous_remote_action(action):
    with pytest.raises(Rejected):
        command("pc", action, {}, 1, 0, "scaramouche")


def test_notification_audio_and_lock_local_optins(computer):
    run(computer.execute(device(), "notify", {"template": "test"}, None))
    assert computer.platform.action.call_args.args == (
        "notify",
        "Companion notification test.",
    )
    computer.config["notifications_allowed"] = False
    with pytest.raises(Rejected):
        run(computer.execute(device(), "notify", {"template": "test"}, None))
    computer.config["audio_allowed"] = False
    with pytest.raises(Rejected):
        run(computer.execute(device(), "play", {"asset": "x", "volume": 0.5}, None))
    computer.platform.sample.return_value.title = "Uploading firmware"
    with pytest.raises(Rejected):
        run(computer.execute(device(), "lock", {}, None))
    run(computer.execute(device(), "stop", {}, None))
    computer.platform.stop.assert_awaited()


def test_app_url_allowlists_and_browser_screen_denied(computer):
    run(computer.execute(device(), "launch_app", {"name": "editor"}, None))
    assert computer.platform.action.call_args.args == (
        "launch_app",
        "/Applications/Editor.app",
    )
    run(computer.execute(device(), "open_url", {"name": "docs"}, None))
    for action in ("launch_app", "open_url"):
        with pytest.raises(Rejected):
            run(computer.execute(device(), action, {"name": "/bin/sh"}, None))
    cfg = config()
    cfg["apps"]["com.browser"] = {"category": "browser", "screen_allowed": True}
    with pytest.raises(Rejected):
        validate_config(cfg)


def test_stop_bypasses_quiet_hours_confirmation():
    d = dict(device(), hours=[1, 2], mode="CONFIRM")
    assert authorize(
        command("pc", "stop", {}, 1, 0, "scaramouche", now=100),
        {"pc": d},
        True,
        now=100,
    )
    with pytest.raises(Rejected):
        authorize(
            command(
                "pc",
                "play",
                {"asset": "a", "volume": 0.3},
                1,
                0,
                "scaramouche",
                now=100,
            ),
            {"pc": d},
            True,
            now=100,
        )


def test_cli_validation_and_disabled_verification(tmp_path, monkeypatch, capsys):
    import json
    import sys
    from home_agent.agent import main

    cfg = {
        "agent_id": "desktop",
        "secret": "synthetic-test-secret-at-least-32-characters",
        "url": "ws://127.0.0.1",
        "enabled": False,
        "mock": True,
        "database": str(tmp_path / "agent.db"),
        "devices": {},
        "companion": {"enabled": False},
    }
    path = tmp_path / "config.json"
    path.write_text(json.dumps(cfg))
    monkeypatch.setenv("COMPANION_AGENT_ENABLED", "false")
    for option in ("--validate", "--status", "--verify-computer"):
        monkeypatch.setattr(sys, "argv", ["agent", "--config", str(path), option])
        main()
    assert not (tmp_path / "agent.db").exists()
    output = capsys.readouterr().out
    assert cfg["secret"] not in output and "null" in output


@pytest.mark.parametrize(
    "url",
    [
        "https://evil.example/audio/" + "a" * 48,
        "https://home.example/audio/../private",
        "https://home.example/audio/" + "a" * 48 + "?token=private",
        "http://home.example/audio/" + "a" * 48,
    ],
)
def test_audio_origin_and_path_rejected_before_network(computer, url):
    computer.config["media_origin"] = "https://home.example"
    with pytest.raises(Rejected, match="origin_denied"):
        run(
            computer.execute(
                device(), "play", {"asset": "test", "volume": 0.2}, {"url": url}
            )
        )
    computer.platform.action.assert_not_awaited()
