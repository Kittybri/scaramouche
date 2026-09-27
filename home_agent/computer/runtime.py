"""Deterministic local sensing/privacy policy; no LLM, raw history or screen files."""

from __future__ import annotations
import asyncio
import base64
import io
import os
import re
import time
from urllib.parse import urlsplit
import aiohttp
from home.companion import (
    CATEGORIES,
    CAPABILITIES,
    ACTION_CAPABILITY,
    NOTIFICATIONS,
    EVENTS,
)
from home.protocol import Rejected, secure_url
from .base import Platform

BLOCKED = (
    "chrome",
    "chromium",
    "safari",
    "firefox",
    "brave",
    "opera",
    "browser",
    "msedge",
    "terminal",
    "password",
    "1password",
    "bitwarden",
    "keychain",
    "keepass",
    "lastpass",
    "dashlane",
    "auth",
    "bank",
    "finance",
    "wallet",
    "health",
    "medical",
    "loginwindow",
    "screensaver",
)
WINDOW_BLOCKS = (
    "password",
    "sign in",
    "log in",
    "login",
    "private",
    "incognito",
    "bank",
    "account",
    "2fa",
    "verification",
    "medical",
    "health",
    "credential",
    "secret",
    "token",
    ".env",
)


def validate_config(config):
    for key in (
        "enabled",
        "screen_allowed",
        "audio_allowed",
        "notifications_allowed",
        "lock_allowed",
        "development_events",
    ):
        if key in config and type(config[key]) is not bool:
            raise Rejected("invalid_companion_boolean")
    for bundle, app in config.get("apps", {}).items():
        if (
            not isinstance(bundle, str)
            or not isinstance(app, dict)
            or app.get("category", "unknown") not in CATEGORIES
        ):
            raise Rejected("invalid_app_mapping")
        label = app.get("label", "Unknown app")
        if (
            not isinstance(label, str)
            or len(label) > 64
            or any(ord(c) < 32 for c in label)
        ):
            raise Rejected("invalid_app_label")
        if app.get("screen_allowed") and app.get("category") in {
            "browser",
            "communication",
            "unknown",
        }:
            raise Rejected("unsafe_screen_category")
        path = app.get("path", "")
        if app.get("launch_allowed") and (
            not path.startswith("/Applications/")
            or not path.endswith(".app")
            or ".." in path.split("/")
        ):
            raise Rejected("invalid_app_path")
    for url in config.get("urls", {}).values():
        secure_url(url)
    if not set(config.get("capabilities", [])) <= CAPABILITIES:
        raise Rejected("invalid_companion_capability")
    for key in ("blocked_apps", "blocked_window_patterns", "lock_safe_apps"):
        if not isinstance(config.get(key, []), list) or any(
            not isinstance(s, str) or not s for s in config.get(key, [])
        ):
            raise Rejected("invalid_privacy_filter")
    return config


class Computer:
    def __init__(self, config, platform=None):
        self.config = validate_config(config)
        self.platform = platform or Platform()
        self.consent = {"enabled": False, "screen": False}
        self.current = None
        self.started = 0
        self.last_sent = 0
        self.last_screen = None
        self.previous = None
        self.event = ""

    @property
    def enabled(self):
        return (
            self.config.get("enabled", False)
            and self.consent["enabled"]
            and os.getenv("COMPANION_AGENT_ENABLED", "false").lower() == "true"
        )

    def clear(self):
        self.current = self.previous = None
        self.started = self.last_sent = 0
        self.event = ""

    async def control(self, consent):
        self.consent = {
            "enabled": consent.get("enabled") is True,
            "screen": consent.get("screen") is True,
        }
        if not self.enabled:
            self.clear()
            await self.platform.stop()

    def tick(self, now=None, stamp=None):
        now = time.monotonic() if now is None else now
        stamp = time.time() if stamp is None else stamp
        if not self.enabled or "computer_presence" not in self.config.get(
            "capabilities", []
        ):
            self.clear()
            return None
        try:
            sample = self.platform.sample()
        except Exception:
            self.clear()
            return {
                "app": "",
                "category": "unknown",
                "idle_state": "UNKNOWN",
                "duration_seconds": 0,
                "updated_at": stamp,
                "event": "",
            }
        state = (
            "LOCKED"
            if sample.locked
            else (
                "AWAY"
                if sample.idle_seconds >= max(300, self.config.get("away_seconds", 900))
                else (
                    "IDLE"
                    if sample.idle_seconds
                    >= max(60, self.config.get("idle_seconds", 300))
                    else "ACTIVE"
                )
            )
        )
        key = (sample.bundle, state)
        if key != self.current:
            self.started = now
            self.current = key
        app = (
            self.config.get("apps", {}).get(sample.bundle, {})
            if state == "ACTIVE"
            else {}
        )
        duration = (
            min(86400, int(max(0, now - self.started) // 300) * 300)
            if state == "ACTIVE"
            else 0
        )
        value = {
            "app": app.get("label", "Unknown app") if state == "ACTIVE" else "",
            "category": app.get("category", "unknown"),
            "idle_state": state,
            "duration_seconds": duration,
            "event": self.event,
            "updated_at": stamp,
        }
        comparison = (key, duration // 1800, self.event)
        if comparison == self.previous and now - self.last_sent < 60:
            return None
        self.previous, self.last_sent, self.event = comparison, now, ""
        return value

    def permitted_screen(self, sample):
        app = self.config.get("apps", {}).get(sample.bundle, {})
        name, title = sample.bundle.lower(), sample.title.lower()
        return (
            self.enabled
            and self.consent["screen"]
            and self.config.get("screen_allowed", False)
            and "screen_context" in self.config.get("capabilities", [])
            and not sample.locked
            and sample.window > 0
            and sample.idle_seconds < max(60, self.config.get("idle_seconds", 300))
            and app.get("screen_allowed") is True
            and app.get("category") not in {"browser", "communication", "unknown", None}
            and not any(
                p.lower() in name
                for p in BLOCKED + tuple(self.config.get("blocked_apps", []))
            )
            and not any(
                p.lower() in title
                for p in WINDOW_BLOCKS
                + tuple(self.config.get("blocked_window_patterns", []))
            )
        )

    def screenshot(self, explicit=True, now=None):
        # No capture call before all policy/cadence checks. Recheck foreground after capture.
        now = time.monotonic() if now is None else now
        if not self.enabled or not self.consent["screen"]:
            raise Rejected("screen_disabled")
        interval = (
            60
            if explicit
            else max(900, self.config.get("screen_interval_seconds", 1800))
        )
        if self.last_screen is not None and now - self.last_screen < interval:
            raise Rejected("screen_cooldown")
        sample = self.platform.sample()
        if not self.permitted_screen(sample):
            raise Rejected("screen_blocked")
        self.last_screen = now
        raw = self.platform.capture(sample)
        try:
            after = self.platform.sample()
            if not self.permitted_screen(after) or (
                sample.bundle,
                sample.window,
                sample.title,
            ) != (after.bundle, after.window, after.title):
                raise Rejected("screen_changed")
            from PIL import Image, ImageDraw

            with Image.open(io.BytesIO(raw)) as source:
                if source.width * source.height > 40_000_000:
                    raise Rejected("screen_too_large")
                image = source.convert("RGB")
            # Always remove top 15% (titles/tabs); optional normalized rectangles are blacked out.
            image = image.crop((0, int(image.height * 0.15), image.width, image.height))
            draw = ImageDraw.Draw(image)
            for rect in self.config.get("redact_regions", []):
                if (
                    len(rect) != 4
                    or not all(type(v) in (int, float) and 0 <= v <= 1 for v in rect)
                    or rect[0] >= rect[2]
                    or rect[1] >= rect[3]
                ):
                    raise Rejected("invalid_redaction")
                draw.rectangle(
                    (
                        int(rect[0] * image.width),
                        int(rect[1] * image.height),
                        int(rect[2] * image.width),
                        int(rect[3] * image.height),
                    ),
                    fill="black",
                )
            image.thumbnail((1024, 768))
            with io.BytesIO() as output:
                image.save(output, format="JPEG", quality=60)
                data = output.getvalue()
            image.close()
            if len(data) > 350_000:
                raise Rejected("screen_too_large")
            return {
                "image": base64.b64encode(data).decode(),
                "app": self.config["apps"][sample.bundle].get("label", "Unknown app"),
            }
        finally:
            raw = None  # No temp file/archive; Python cannot guarantee forensic RAM zeroization.

    async def execute(self, device, action, parameters, media):
        if not self.enabled or ACTION_CAPABILITY.get(action) not in self.config.get(
            "capabilities", []
        ):
            raise Rejected("companion_disabled_or_capability_denied")
        if action == "stop":
            await self.platform.stop()
            return "completed"
        if action == "screen":
            return self.screenshot()
        if action == "test":
            return "completed"
        if action == "notify":
            if not self.config.get("notifications_allowed", False):
                raise Rejected("notifications_disabled")
            await self.platform.action("notify", NOTIFICATIONS[parameters["template"]])
        elif action == "open_url":
            url = self.config.get("urls", {}).get(parameters["name"])
            if not url:
                raise Rejected("url_not_allowlisted")
            await self.platform.action(action, secure_url(url))
        elif action == "launch_app":
            app = next(
                (
                    v
                    for v in self.config.get("apps", {}).values()
                    if v.get("alias") == parameters["name"]
                ),
                {},
            )
            if app.get("launch_allowed") is not True:
                raise Rejected("app_not_allowlisted")
            await self.platform.action(action, app["path"])
        elif action == "lock":
            sample = self.platform.sample()
            # A small local safe-app allowlist is required. Unknown/workflow apps never lock.
            if (
                not self.config.get("lock_allowed", False)
                or sample.locked
                or sample.bundle not in self.config.get("lock_safe_apps", [])
                or any(
                    s in sample.title.lower()
                    for s in (
                        "upload",
                        "install",
                        "update",
                        "render",
                        "export",
                        "record",
                        "call",
                        "meeting",
                        "payment",
                    )
                )
            ):
                raise Rejected("lock_unsafe_or_disabled")
            await self.platform.action(action, None)
        elif action == "volume":
            if not self.config.get("audio_allowed", False):
                raise Rejected("audio_disabled")
            await self.platform.action(action, min(0.5, max(0, parameters["value"])))
        elif action == "play":
            if not self.config.get("audio_allowed", False) or not media:
                raise Rejected("audio_disabled")
            origin = secure_url(self.config.get("media_origin", ""))
            url = media.get("url", "")
            if (
                urlsplit(url).netloc != urlsplit(origin).netloc
                or urlsplit(url).scheme != "https"
                or not url.startswith(origin + "/audio/")
                or not re.fullmatch(r"/audio/[0-9a-f]{48}", urlsplit(url).path)
                or urlsplit(url).query
                or urlsplit(url).fragment
                or urlsplit(url).username
                or urlsplit(url).password
            ):
                raise Rejected("audio_origin_denied")
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=8)
            ) as session:
                async with session.get(url, allow_redirects=False) as response:
                    if response.status != 200:
                        raise Rejected("audio_unavailable")
                    data = bytearray()
                    async for chunk in response.content.iter_chunked(65536):
                        data.extend(chunk)
                        if len(data) > 5_000_000:
                            raise Rejected("audio_too_large")
            try:
                await self.platform.action(
                    "play", (bytes(data), min(0.5, max(0, parameters["volume"])))
                )
            finally:
                data.clear()
        else:
            raise Rejected("unsupported_local_action")
        return "completed"

    async def close(self):
        await self.control({})
