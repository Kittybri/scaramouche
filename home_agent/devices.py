"""Explicit LAN adapters; optional hardware libraries import only when selected."""

from __future__ import annotations
import asyncio
import ipaddress
import ssl
import time
import threading
from pathlib import Path
from urllib.parse import urlsplit
import aiohttp
from home.protocol import Rejected, secure_url


def lan_host(host):
    try:
        address = ipaddress.ip_address(host)
        if not address.is_private or address.is_multicast or address.is_unspecified:
            raise ValueError
    except ValueError:
        raise Rejected("explicit_private_ip_required")
    return host


class MockProvider:
    def __init__(self):
        self.calls = []

    async def execute(self, device, action, parameters, media=None):
        self.calls.append((device, action, dict(parameters)))
        return "completed"

    async def close(self):
        pass


class Hue:
    async def execute(self, d, action, p, media=None):
        host = lan_host(d["host"])
        if not (d.get("ca_file") or d.get("tls_fingerprint")) or not d.get(
            "application_key"
        ):
            raise Rejected("hue_tls_configuration_required")
        context = (
            aiohttp.Fingerprint(bytes.fromhex(d["tls_fingerprint"]))
            if d.get("tls_fingerprint")
            else ssl.create_default_context(cafile=d["ca_file"])
        )
        resource = d["resource_id"]
        from uuid import UUID

        UUID(resource)
        payload = {}
        if action == "power":
            payload = {"on": {"on": p["on"]}}
        elif action == "brightness":
            payload = {"dimming": {"brightness": p["value"]}}
        elif action == "color":
            payload = {"color": {"xy": d["colors"][p["name"]]}}
        elif action == "color_temperature":
            payload = {"color_temperature": {"mirek": p["mirek"]}}
        elif action == "scene":
            resource = str(UUID(d["scenes"][p["name"]]))
            payload = {"recall": {"action": "active"}}
        elif action != "test":
            raise Rejected("unsupported_action")
        kind = "scene" if action == "scene" else "light"
        deadline(d)
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=8)
        ) as session:
            async with session.request(
                "GET" if action == "test" else "PUT",
                f"https://{host}/clip/v2/resource/{kind}/{resource}",
                json=payload if action != "test" else None,
                headers={"hue-application-key": d["application_key"]},
                ssl=context,
                allow_redirects=False,
            ) as response:
                if response.status != 200:
                    raise Rejected("device_error")
                result = await response.json()
                if result.get("errors"):
                    raise Rejected("device_error")
        return "completed"

    async def close(self):
        pass


class Kasa:
    async def execute(self, d, action, p, media=None):
        from kasa import Discover

        device = await Discover.discover_single(
            lan_host(d["host"]),
            username=d.get("username"),
            password=d.get("password"),
            discovery_timeout=5,
        )
        if device is None:
            raise Rejected("device_offline")
        try:
            await device.update()
            # Never control every outlet on a strip by mistake.
            if device.children:
                child = next(
                    (v for v in device.children if v.device_id == d.get("child_id")),
                    None,
                )
                if child is None:
                    raise Rejected("outlet_not_configured")
            else:
                child = device
            if action == "power":
                deadline(d)
                await (child.turn_on() if p["on"] else child.turn_off())
            elif action != "test":
                raise Rejected("unsupported_action")
            return "completed"
        finally:
            await device.disconnect()

    async def close(self):
        pass


from .ipp import Printer


class Cast:
    def __init__(self, media_origin):
        self.media_origin = media_origin
        self.connections = {}
        self.cancellations = set()

    async def execute(self, d, action, p, media=None):
        cancelled = threading.Event()
        self.cancellations.add(cancelled)
        try:
            return await asyncio.to_thread(self._run, d, action, p, media, cancelled)
        finally:
            cancelled.set()
            self.cancellations.discard(cancelled)

    def _run(self, d, action, p, media, cancelled):
        import pychromecast
        from uuid import UUID

        host = lan_host(d["host"])
        identity = UUID(d["uuid"])
        casts, browser = pychromecast.get_listed_chromecasts(
            uuids=[identity],
            known_hosts=[host],
            discovery_timeout=5,
            tries=1,
            retry_wait=1,
            timeout=5,
        )
        started = None
        try:
            cast = next(
                (c for c in casts if c.uuid == identity and c.cast_info.host == host),
                None,
            )
            if cast is None:
                raise Rejected("cast_offline")
            cast.wait(timeout=5)
            self.connections[d["uuid"]] = cast
            controller = cast.media_controller
            deadline(d)
            if cancelled.is_set():
                raise Rejected("cancelled")
            if action == "test":
                return "completed"
            if action == "volume":
                cast.set_volume(p["value"])
            elif action == "pause":
                controller.pause()
            elif action == "stop":
                controller.stop()
            elif action == "play":
                if media:
                    parsed = urlsplit(media["url"])
                    if (
                        (parsed.scheme + "://" + parsed.netloc) != self.media_origin
                        or not parsed.path.startswith("/audio/")
                        or media["expires"] <= time.time()
                    ):
                        raise Rejected("invalid_audio_origin")
                    url, mime = media["url"], media["mime"]
                else:
                    asset = d.get("asset_urls", {}).get(p["asset"])
                    if not asset:
                        raise Rejected("unauthorized_audio")
                    url = secure_url(asset["url"])
                    mime = asset["mime"]
                if mime not in {"audio/mpeg", "audio/wav", "audio/ogg"}:
                    raise Rejected("invalid_audio")
                cast.set_volume(p["volume"])
                started = controller
                controller.play_media(url, mime)
                controller.block_until_active(timeout=5)
                # PyChromecast's wait returns None even on timeout. An old active
                # session/IDLE status is not proof that our new audio has started.
                if not controller.session_active_event.is_set():
                    raise Rejected("cast_activation_failed")
                ready_by = time.monotonic() + 5
                while controller.status.content_id != url:
                    deadline(d)
                    if cancelled.is_set() or time.monotonic() >= ready_by:
                        raise Rejected("cast_activation_failed")
                    time.sleep(0.1)
                # Intentionally short; never indefinite room audio or high-volume alarms.
                end = time.monotonic() + min(
                    12, max(1, int(d.get("max_play_seconds", 12)))
                )
                while time.monotonic() < end and not cancelled.is_set():
                    if controller.status.player_state == "IDLE":
                        if controller.status.idle_reason == "ERROR":
                            raise Rejected("cast_playback_failed")
                        break
                    time.sleep(0.25)
            else:
                raise Rejected("unsupported_action")
            return "completed"
        finally:
            if started is not None:
                try:
                    started.stop()
                except Exception:
                    pass
            for cast in casts:
                try:
                    cast.disconnect(timeout=3)
                except Exception:
                    pass
            self.connections.pop(d["uuid"], None)
            pychromecast.discovery.stop_discovery(browser)

    async def close(self):
        for event in list(self.cancellations):
            event.set()
        for cast in list(self.connections.values()):
            try:
                await asyncio.to_thread(cast.media_controller.stop)
            except Exception:
                pass
            try:
                await asyncio.to_thread(cast.disconnect, timeout=3)
            except Exception:
                pass


def deadline(d):
    if time.time() >= d.get("_expires_at", float("inf")):
        raise Rejected("expired_command")


def validate_devices(config):
    if config.get("mock", False):
        return
    from uuid import UUID
    from home.protocol import ID

    for d in config.get("devices", {}).values():
        kind = d["type"]
        if kind not in {"printer", "computer"}:
            lan_host(d["host"])
        if kind == "hue":
            UUID(d["resource_id"])
            if not d.get("application_key"):
                raise Rejected("missing_hue_key")
            if d.get("tls_fingerprint"):
                if len(bytes.fromhex(d["tls_fingerprint"])) != 32:
                    raise Rejected("invalid_tls_pin")
            elif not Path(d.get("ca_file", "")).is_file():
                raise Rejected("missing_hue_tls_trust")
            for xy in d.get("colors", {}).values():
                if (
                    set(xy) != {"x", "y"}
                    or not all(0 <= float(v) <= 1 for v in xy.values())
                    or sum(float(v) for v in xy.values()) > 1
                ):
                    raise Rejected("invalid_color")
            for scene in d.get("scenes", {}).values():
                UUID(scene)
        if kind == "cast":
            UUID(d["uuid"])
            secure_url(config["media_origin"], mock=config.get("mock", False))
            for asset in d.get("asset_urls", {}).values():
                secure_url(asset["url"])
                if asset["mime"] not in {"audio/mpeg", "audio/wav", "audio/ogg"}:
                    raise Rejected("invalid_audio")
        if kind == "printer" and not ID.fullmatch(d.get("queue", "")):
            raise Rejected("invalid_printer_queue")


def providers(config):
    validate_devices(config)
    if config.get("mock", False):
        return {name: MockProvider() for name in ("hue", "kasa", "cast", "printer")}
    return {
        "hue": Hue(),
        "kasa": Kasa(),
        "cast": Cast(config.get("media_origin", "https://localhost")),
        "printer": Printer(),
    }
