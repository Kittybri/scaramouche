"""Vendor boundaries mocked; ensures scoped requests, cleanup, and failure handling."""

import asyncio
import sys
import time
import threading
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock
from uuid import UUID
import pytest

from home.protocol import Rejected
from home_agent.devices import Hue, Kasa, Cast, validate_devices
from home_agent.ipp import Printer


def run(coro):
    return asyncio.run(coro)


def test_hue_explicit_resource_verified_tls_and_payload(monkeypatch):
    response = NS(status=200, json=AsyncMock(return_value={"errors": []}))

    class Context:
        async def __aenter__(self):
            return response

        async def __aexit__(self, *args):
            pass

    request = Mock(return_value=Context())

    class Session:
        async def __aenter__(self):
            return NS(request=request)

        async def __aexit__(self, *args):
            pass

    monkeypatch.setattr(
        "home_agent.devices.aiohttp.ClientSession", lambda **kw: Session()
    )
    d = {
        "host": "192.168.1.2",
        "resource_id": "00000000-0000-0000-0000-000000000001",
        "application_key": "local-key",
        "tls_fingerprint": "ab" * 32,
    }
    assert run(Hue().execute(d, "brightness", {"value": 35})) == "completed"
    args = request.call_args
    assert args.args[0] == "PUT" and args.args[1].endswith(d["resource_id"])
    assert args.kwargs["json"] == {"dimming": {"brightness": 35}}
    assert args.kwargs["ssl"] is not False and not args.kwargs["allow_redirects"]
    response.json = AsyncMock(
        return_value={"errors": [{"description": "secret-vendor-error"}]}
    )
    with pytest.raises(Rejected, match="^hue_api_error$"):
        run(Hue().execute(d, "power", {"on": True}))
    d["_expires_at"] = 0
    with pytest.raises(Rejected, match="expired_command"):
        run(Hue().execute(d, "power", {"on": True}))


def test_kasa_outlet_offline_and_cleanup(monkeypatch):
    child = NS(device_id="only-this", turn_on=AsyncMock(), turn_off=AsyncMock())
    parent = NS(
        children=[child],
        update=AsyncMock(),
        disconnect=AsyncMock(),
        turn_on=AsyncMock(),
    )
    discover = AsyncMock(return_value=parent)
    monkeypatch.setitem(sys.modules, "kasa", NS(Discover=NS(discover_single=discover)))
    d = {"host": "192.168.1.3", "child_id": "only-this"}
    assert run(Kasa().execute(d, "power", {"on": True})) == "completed"
    assert (
        child.turn_on.await_count == 1
        and parent.turn_on.await_count == 0
        and parent.disconnect.await_count == 1
    )
    with pytest.raises(Rejected, match="outlet_not_configured"):
        run(Kasa().execute(dict(d, child_id="wrong"), "power", {"on": True}))
    assert parent.disconnect.await_count == 2
    discover.return_value = None
    with pytest.raises(Rejected, match="device_offline"):
        run(Kasa().execute(d, "test", {}))


def cast_fixture(monkeypatch):
    identity = UUID("00000000-0000-0000-0000-000000000001")
    controller = NS(
        play_media=Mock(),
        block_until_active=Mock(),
        stop=Mock(),
        pause=Mock(),
        status=NS(
            player_state="IDLE",
            content_id="https://home.example/audio/random",
            idle_reason="FINISHED",
        ),
        session_active_event=NS(is_set=Mock(return_value=True)),
    )
    cast = NS(
        uuid=identity,
        cast_info=NS(host="192.168.1.4"),
        wait=Mock(),
        media_controller=controller,
        set_volume=Mock(),
        disconnect=Mock(),
    )
    discover = Mock(return_value=([cast], "browser"))
    cleanup = Mock()
    monkeypatch.setitem(
        sys.modules,
        "pychromecast",
        NS(get_listed_chromecasts=discover, discovery=NS(stop_discovery=cleanup)),
    )
    d = {"host": "192.168.1.4", "uuid": str(identity)}
    media = {
        "url": "https://home.example/audio/random",
        "mime": "audio/mpeg",
        "expires": time.time() + 60,
    }
    return cast, discover, cleanup, d, media


def test_cast_selection_playback_cleanup_and_expiry(monkeypatch):
    cast, discover, cleanup, d, media = cast_fixture(monkeypatch)
    provider = Cast("https://home.example")
    assert (
        run(provider.execute(d, "play", {"asset": "random", "volume": 0.25}, media))
        == "completed"
    )
    assert discover.call_args.kwargs["known_hosts"] == [d["host"]]
    cast.set_volume.assert_called_once_with(0.25)
    cast.media_controller.stop.assert_called_once()
    cast.disconnect.assert_called_once()
    cleanup.assert_called_once_with("browser")
    with pytest.raises(Rejected, match="invalid_audio_origin"):
        run(
            provider.execute(
                d, "play", {"asset": "random", "volume": 0.25}, dict(media, expires=0)
            )
        )
    assert cast.media_controller.play_media.call_count == 1
    cast.cast_info.host = "192.168.1.99"
    with pytest.raises(Rejected, match="cast_offline"):
        run(provider.execute(d, "test", {}))
    assert not provider.connections


def test_cast_cancellation_and_activation_failure_cleanup(monkeypatch):
    cast, discover, cleanup, d, media = cast_fixture(monkeypatch)
    provider = Cast("https://home.example")
    event = threading.Event()
    event.set()
    with pytest.raises(Rejected, match="cancelled"):
        provider._run(d, "play", {"asset": "random", "volume": 0.25}, media, event)
    cast.media_controller.play_media.assert_not_called()
    cast.media_controller.block_until_active.side_effect = TimeoutError()
    with pytest.raises(TimeoutError):
        run(provider.execute(d, "play", {"asset": "random", "volume": 0.25}, media))
    cast.media_controller.stop.assert_called_once()
    assert not provider.connections and not provider.cancellations
    cast.media_controller.block_until_active.side_effect = None
    cast.media_controller.session_active_event.is_set.return_value = False
    with pytest.raises(Rejected, match="cast_activation_failed"):
        run(provider.execute(d, "play", {"asset": "random", "volume": 0.25}, media))
    assert cast.media_controller.stop.call_count == 2


def test_cast_pause_stop_only_control_bot_owned_session(monkeypatch):
    cast, discover, cleanup, d, media = cast_fixture(monkeypatch)
    provider = Cast("https://home.example")
    # An absent managed session is never discovered/adopted from the room.
    assert run(provider.execute(d, "stop", {})) == "completed"
    with pytest.raises(Rejected, match="no_managed_cast_session"):
        run(provider.execute(d, "pause", {}))
    discover.assert_not_called()
    cancelled = threading.Event()
    provider.managed[d["uuid"]] = (cast.media_controller, cancelled)
    assert run(provider.execute(d, "pause", {})) == "completed"
    cast.media_controller.pause.assert_called_once()
    assert run(provider.execute(d, "stop", {})) == "completed"
    assert cancelled.is_set()


def test_cast_provider_rejects_out_of_range_volume(monkeypatch):
    cast, discover, cleanup, d, media = cast_fixture(monkeypatch)
    provider = Cast("https://home.example")
    with pytest.raises(Rejected, match="volume_out_of_range"):
        run(provider.execute(d, "volume", {"value": 0.9}))
    with pytest.raises(Rejected, match="volume_out_of_range"):
        run(provider.execute(d, "play", {"asset": "random", "volume": 0.9}, media))


@pytest.mark.parametrize(
    "state,outcome", [(9, "completed"), (5, "submitted"), (7, "failed"), (8, "failed")]
)
def test_printer_completion_failure_and_no_repeat(state, outcome):
    provider = Printer()
    provider.request = AsyncMock(side_effect=[{"job-id": 10}, {"job-state": state}])
    if outcome == "failed":
        with pytest.raises(Rejected):
            run(
                provider.execute(
                    {"queue": "office"}, "print_note", {"template": "posture"}
                )
            )
    else:
        assert (
            run(
                provider.execute(
                    {"queue": "office"}, "print_note", {"template": "posture"}
                )
            )
            == outcome
        )
    assert provider.request.await_count == 2
    assert provider.request.call_args_list[0].args == ("office", 2, "posture")


def test_printer_offline_and_configuration_fail_closed():
    provider = Printer()
    provider.request = AsyncMock(side_effect=ConnectionError())
    with pytest.raises(ConnectionError):
        run(provider.execute({"queue": "office"}, "test", {}))
    for d in (
        {"type": "kasa", "host": "8.8.8.8"},
        {"type": "printer", "queue": "../../file"},
        {
            "type": "hue",
            "host": "192.168.1.2",
            "resource_id": "00000000-0000-0000-0000-000000000001",
        },
    ):
        with pytest.raises(Rejected):
            validate_devices({"devices": {"d": d}})
