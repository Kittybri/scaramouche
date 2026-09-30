"""The same deterministic policy is enforced independently at hub and relay."""

from __future__ import annotations
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import math
from .protocol import Rejected, validate, ID

TYPES = {
    "computer": {
        "notify",
        "lock",
        "launch_app",
        "open_url",
        "screen",
        "play",
        "volume",
        "stop",
        "test",
    },
    "hue": {"power", "brightness", "color", "color_temperature", "scene", "test"},
    "kasa": {"power", "test"},
    "cast": {"play", "pause", "stop", "volume", "test"},
    "printer": {"print_note", "test"},
}
MODES = {"DISABLED", "MANUAL", "CONFIRM", "AUTONOMOUS_SAFE"}


def registry(devices):
    if not isinstance(devices, dict) or len(devices) > 50:
        raise Rejected("invalid_registry")
    for key, d in devices.items():
        if (
            not ID.fullmatch(key)
            or not isinstance(d, dict)
            or d.get("type") not in TYPES
        ):
            raise Rejected("invalid_device_config")
        if not isinstance(d.get("agent"), str) or not ID.fullmatch(d["agent"]):
            raise Rejected("invalid_device_agent")
        actions = d.get("actions", [])
        if (
            d.get("mode", "DISABLED") not in MODES
            or not isinstance(actions, list)
            or len(actions) != len(set(actions))
            or not set(actions) <= TYPES[d["type"]]
        ):
            raise Rejected("invalid_device_policy")
        if d["type"] == "computer":
            from .companion import CAPABILITIES

            if (
                type(d.get("owner_user_id")) is not int
                or d["owner_user_id"] <= 0
                or not set(d.get("capabilities", [])) <= CAPABILITIES
                or d.get("mode", "DISABLED") == "AUTONOMOUS_SAFE"
            ):
                raise Rejected("invalid_computer_policy")
        if d["type"] == "kasa" and d.get("category", "NEVER_AUTOMATE") not in {
            "DECORATIVE",
            "LIGHTING",
            "LOW_RISK",
            "NEVER_AUTOMATE",
        }:
            raise Rejected("invalid_plug_category")
        try:
            ZoneInfo(d.get("timezone", "UTC"))
            in_window(0, d.get("hours", [8, 22]))
        except (KeyError, TypeError, ValueError):
            raise Rejected("invalid_device_policy")
        for field in (
            "enabled",
            "confirmation_required",
            "allow_alarm",
            "allow_alarm_quiet",
            "allow_roommate",
        ):
            if field in d and type(d[field]) is not bool:
                raise Rejected("invalid_policy_boolean")
        for field in ("users", "guilds"):
            values = d.get(field, [])
            if not isinstance(values, list) or any(
                type(v) is not int or v < (1 if field == "users" else 0) for v in values
            ):
                raise Rejected("invalid_policy_actors")
        if not isinstance(d.get("bots", []), list) or not set(d.get("bots", [])) <= {
            "scaramouche",
            "wanderer",
        }:
            raise Rejected("invalid_policy_bots")
        if "friendly_name" in d and (
            not isinstance(d["friendly_name"], str)
            or not 1 <= len(d["friendly_name"].strip()) <= 64
        ):
            raise Rejected("invalid_device_alias")
        aliases = d.get("aliases", [])
        if not isinstance(aliases, list) or any(
            not isinstance(alias, str)
            or not 1 <= len(alias.strip()) <= 64
            for alias in aliases
        ):
            raise Rejected("invalid_device_alias")
        for field in (
            "min_brightness",
            "max_brightness",
            "min_volume",
            "max_volume",
            "cooldown_seconds",
            "max_changes_hour",
            "max_jobs_day",
            "max_pages_per_job",
        ):
            if field in d and (
                type(d[field]) not in (int, float)
                or not math.isfinite(d[field])
                or d[field] < 0
            ):
                raise Rejected("invalid_policy_limit")
        for low, high in (
            ("min_brightness", "max_brightness"),
            ("min_volume", "max_volume"),
        ):
            if low in d and high in d and float(d[low]) > float(d[high]):
                raise Rejected("invalid_policy_limit")
        for field in ("min_brightness", "max_brightness"):
            if field in d and not 1 <= float(d[field]) <= 100:
                raise Rejected("invalid_policy_limit")
        for field in ("min_volume", "max_volume"):
            if field in d and not 0 <= float(d[field]) <= 0.5:
                raise Rejected("invalid_policy_limit")
        if "cooldown_seconds" in d and not 60 <= float(d["cooldown_seconds"]) <= 86400:
            raise Rejected("invalid_policy_limit")
        if "max_changes_hour" in d and (
            type(d["max_changes_hour"]) is not int
            or not 1 <= d["max_changes_hour"] <= 6
        ):
            raise Rejected("invalid_policy_limit")
        if "max_jobs_day" in d and (
            type(d["max_jobs_day"]) is not int
            or not 0 <= d["max_jobs_day"] <= 3
        ):
            raise Rejected("invalid_policy_limit")
        if d["type"] == "printer" and (
            d.get("document_types", ["text/plain"]) != ["text/plain"]
            or type(d.get("max_pages_per_job", 1)) is not int
            or d.get("max_pages_per_job", 1) != 1
        ):
            raise Rejected("invalid_printer_policy")
    return devices


def in_window(hour, window):
    start, end = window
    if not (
        type(start) is int and type(end) is int and 0 <= start < 24 and 0 <= end < 24
    ):
        raise Rejected("invalid_hours")
    return (
        True
        if start == end
        else (start <= hour < end if start < end else hour >= start or hour < end)
    )


def authorize(command, devices, enabled, now=None):
    c = validate(command, now)
    if not enabled:
        raise Rejected("automation_disabled")
    d = devices.get(c["device_id"])
    if not d or not d.get("enabled", False) or d.get("mode", "DISABLED") == "DISABLED":
        raise Rejected("device_disabled_or_unknown")
    if c["action"] not in d.get("actions", []) or c["action"] not in TYPES[d["type"]]:
        raise Rejected("action_denied")
    if (
        c["bot"] not in d.get("bots", [])
        or c["user_id"] not in d.get("users", [])
        or c["guild_id"] not in d.get("guilds", [])
    ):
        raise Rejected("actor_denied")
    if d["type"] == "computer":
        from .companion import ACTION_CAPABILITY

        if (
            c["user_id"] != d["owner_user_id"]
            or c["guild_id"] != 0
            or c["trigger"] not in {"manual", "proposal"}
            or ACTION_CAPABILITY[c["action"]] not in d.get("capabilities", [])
        ):
            raise Rejected("computer_owner_or_capability_denied")
        if c["action"] == "lock" and not c["confirmed"]:
            raise Rejected("confirmation_required")
    hour = (
        datetime.fromtimestamp(now, timezone.utc)
        .astimezone(ZoneInfo(d.get("timezone", "UTC")))
        .hour
        if now is not None
        else datetime.now(ZoneInfo(d.get("timezone", "UTC"))).hour
    )
    if c["action"] != "stop" and not in_window(hour, d.get("hours", [8, 22])):
        # Explicit requested alarms are the only per-device quiet-hours exception.
        if c["trigger"] != "alarm" or not d.get("allow_alarm_quiet", False):
            raise Rejected("outside_allowed_hours")
    mode = d.get("mode", "DISABLED")
    auto = c["trigger"] in {"autonomous", "alarm", "roommate"}
    if auto and mode != "AUTONOMOUS_SAFE":
        raise Rejected("autonomy_denied")
    if (
        d["type"] == "kasa"
        and c["trigger"] != "manual"
        and d.get("category", "NEVER_AUTOMATE") == "NEVER_AUTOMATE"
    ):
        raise Rejected("unsafe_plug")
    if c["trigger"] == "roommate" and not d.get("allow_roommate", False):
        raise Rejected("roommate_denied")
    if c["trigger"] == "alarm" and not d.get("allow_alarm", False):
        raise Rejected("alarm_denied")
    if (
        (
            mode == "CONFIRM"
            or d.get("confirmation_required", False)
            or c["trigger"] == "proposal"
            or (d["type"] == "printer" and c["action"] == "print_note")
        )
        and not c["confirmed"]
        and c["action"] != "stop"
    ):
        raise Rejected("confirmation_required")
    p = c["parameters"]
    if c["action"] == "brightness":
        lo = max(1, min(100, float(d.get("min_brightness", 10))))
        hi = max(lo, min(100, float(d.get("max_brightness", 70))))
        if not lo <= p["value"] <= hi:
            raise Rejected("brightness_out_of_range")
    if c["action"] in {"volume", "play"}:
        key = "volume" if c["action"] == "play" else "value"
        lo = max(0, min(0.5, float(d.get("min_volume", 0.1))))
        hi = max(lo, min(0.5, float(d.get("max_volume", 0.5))))
        if not lo <= p[key] <= hi:
            raise Rejected("volume_out_of_range")
    if c["action"] in {"color", "scene"} and p["name"] not in d.get(
        "colors" if c["action"] == "color" else "scenes", {}
    ):
        raise Rejected("preset_denied")
    if c["action"] == "color_temperature":
        if type(p["mirek"]) is not int or not 153 <= p["mirek"] <= 500:
            raise Rejected("color_temperature_out_of_range")
    if c["action"] == "print_note" and (
        int(d.get("max_pages_per_job", 1)) < 1
        or "text/plain" not in d.get("document_types", ["text/plain"])
    ):
        raise Rejected("document_denied")
    return c
