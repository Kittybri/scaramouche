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
            lo = max(1, min(100, float(d.get("min_brightness", 10))))
            hi = max(lo, min(100, float(d.get("max_brightness", 70))))
            if not lo <= p["value"] <= hi:
                raise Rejected("brightness_out_of_range")
            payload = {"dimming": {"brightness": p["value"]}}
        elif action == "color":
            if p["name"] not in d.get("colors", {}):
                raise Rejected("preset_denied")
            payload = {"color": {"xy": d["colors"][p["name"]]}}
        elif action == "color_temperature":
            if type(p["mirek"]) is not int or not 153 <= p["mirek"] <= 500:
                raise Rejected("color_temperature_out_of_range")
            payload = {"color_temperature": {"mirek": p["mirek"]}}
        elif action == "scene":
            if p["name"] not in d.get("scenes", {}):
                raise Rejected("preset_denied")
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
                if response.status in {401, 403}:
                    raise Rejected("hue_authentication_failed")
                if response.status != 200:
                    raise Rejected("hue_bridge_error")
                result = await response.json()
                if result.get("errors"):
                    raise Rejected("hue_api_error")
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
        self.managed = {}
        self.guard = threading.Lock()

    async def execute(self, d, action, p, media=None):
        if action in {"pause", "stop"}:
            return await asyncio.to_thread(self._control_managed, d, action)
        cancelled = threading.Event()
        self.cancellations.add(cancelled)
        try:
            return await asyncio.to_thread(self._run, d, action, p, media, cancelled)
        finally:
            cancelled.set()
            self.cancellations.discard(cancelled)

    def _control_managed(self, d, action):
        """Control only a session this process started; never adopt ambient media."""
        with self.guard:
            managed = self.managed.get(d["uuid"])
        if not managed:
            if action == "stop":
                return "completed"  # Idempotent stop is safe when nothing is ours.
            raise Rejected("no_managed_cast_session")
        controller, cancelled = managed
        if action == "pause":
            controller.pause()
        else:
            cancelled.set()
            controller.stop()
        return "completed"

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
                lo = max(0, min(0.5, float(d.get("min_volume", 0.1))))
                hi = max(lo, min(0.5, float(d.get("max_volume", 0.5))))
                if not lo <= p["value"] <= hi:
                    raise Rejected("volume_out_of_range")
                cast.set_volume(p["value"])
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
                lo = max(0, min(0.5, float(d.get("min_volume", 0.1))))
                hi = max(lo, min(0.5, float(d.get("max_volume", 0.5))))
                if not lo <= p["volume"] <= hi:
                    raise Rejected("volume_out_of_range")
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
                with self.guard:
                    self.managed[d["uuid"]] = (controller, cancelled)
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
            with self.guard:
                current = self.managed.get(d["uuid"])
                if current and current[1] is cancelled:
                    self.managed.pop(d["uuid"], None)
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
    from uuid import UUID
    from home.protocol import ID

    for d in config.get("devices", {}).values():
        kind = d["type"]
        if kind not in {"printer", "computer"}:
            if not isinstance(d.get("host"), str):
                raise Rejected("missing_device_host")
            lan_host(d["host"])
        if kind == "hue":
            try:
                UUID(d["resource_id"])
            except (KeyError, TypeError, ValueError) as exc:
                raise Rejected("invalid_hue_resource") from exc
            if not isinstance(d.get("application_key"), str) or not d["application_key"]:
                raise Rejected("missing_hue_key")
            if d.get("tls_fingerprint"):
                try:
                    valid_pin = len(bytes.fromhex(d["tls_fingerprint"])) == 32
                except (TypeError, ValueError):
                    valid_pin = False
                if not valid_pin:
                    raise Rejected("invalid_tls_pin")
            elif not isinstance(d.get("ca_file"), str) or not d["ca_file"]:
                raise Rejected("missing_hue_tls_trust")
            elif not config.get("mock", False) and not Path(d["ca_file"]).is_file():
                raise Rejected("missing_hue_tls_trust")
            colors = d.get("colors", {})
            scenes = d.get("scenes", {})
            if not isinstance(colors, dict) or not isinstance(scenes, dict):
                raise Rejected("invalid_hue_presets")
            for name, xy in colors.items():
                if (
                    not ID.fullmatch(name)
                    or not isinstance(xy, dict)
                    or set(xy) != {"x", "y"}
                ):
                    raise Rejected("invalid_color")
                try:
                    values = [float(v) for v in xy.values()]
                except (TypeError, ValueError) as exc:
                    raise Rejected("invalid_color") from exc
                if not all(0 <= value <= 1 for value in values) or sum(values) > 1:
                    raise Rejected("invalid_color")
            for name, scene in scenes.items():
                try:
                    if not ID.fullmatch(name):
                        raise ValueError
                    UUID(scene)
                except (TypeError, ValueError) as exc:
                    raise Rejected("invalid_scene") from exc
        if kind == "kasa":
            if "child_id" in d and (
                not isinstance(d["child_id"], str) or not d["child_id"]
            ):
                raise Rejected("invalid_kasa_child")
            for key in ("username", "password"):
                if key in d and not isinstance(d[key], str):
                    raise Rejected("invalid_kasa_credentials")
        if kind == "cast":
            try:
                UUID(d["uuid"])
            except (KeyError, TypeError, ValueError) as exc:
                raise Rejected("invalid_cast_uuid") from exc
            secure_url(config.get("media_origin", ""), mock=config.get("mock", False))
            duration = d.get("max_play_seconds", 12)
            if type(duration) is not int or not 1 <= duration <= 12:
                raise Rejected("invalid_cast_duration")
            assets = d.get("asset_urls", {})
            if not isinstance(assets, dict):
                raise Rejected("invalid_cast_assets")
            for name, asset in assets.items():
                if not ID.fullmatch(name) or not isinstance(asset, dict) or set(asset) != {"url", "mime"}:
                    raise Rejected("invalid_cast_assets")
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
