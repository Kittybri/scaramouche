"""Fail-closed configuration loading and cross-reference validation."""

from __future__ import annotations

import json
import math
from pathlib import Path

from .permissions import registry
from .protocol import ID, Rejected, secret_ok, secure_url


def _unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise Rejected("duplicate_configuration_key")
        value[key] = item
    return value


def load_config(path):
    """Load JSON while rejecting duplicate keys that normal json.loads hides."""
    try:
        return json.loads(Path(path).read_text(), object_pairs_hook=_unique_object)
    except Rejected:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise Rejected("invalid_configuration_json") from exc


def _identity_secrets(value, error):
    if not isinstance(value, dict) or not value:
        raise Rejected(error)
    for identity, secret in value.items():
        if not isinstance(identity, str) or not ID.fullmatch(identity):
            raise Rejected(error)
        if not secret_ok(secret):
            raise Rejected("weak_credential")


def _positive_ids(value, error, *, allow_zero=False):
    if not isinstance(value, list) or any(
        type(item) is not int or item < (0 if allow_zero else 1) for item in value
    ):
        raise Rejected(error)


def _owntracks(value):
    if not isinstance(value, dict):
        raise Rejected("invalid_owntracks_configuration")
    for account, item in value.items():
        if not ID.fullmatch(account) or not isinstance(item, dict):
            raise Rejected("invalid_owntracks_configuration")
        if type(item.get("enabled", False)) is not bool:
            raise Rejected("invalid_owntracks_configuration")
        if type(item.get("user_id")) is not int or item["user_id"] <= 0:
            raise Rejected("invalid_owntracks_configuration")
        if not isinstance(item.get("username"), str) or not item["username"]:
            raise Rejected("invalid_owntracks_configuration")
        if not secret_ok(item.get("password")):
            raise Rejected("weak_credential")
        bots = item.get("bots", [])
        if not isinstance(bots, list) or not bots or not set(bots) <= {
            "scaramouche",
            "wanderer",
        }:
            raise Rejected("invalid_owntracks_configuration")
        for field, low, high in (
            ("debounce_seconds", 60, 86400),
            ("max_accuracy_m", 10, 1000),
        ):
            number = item.get(field, 300 if field == "debounce_seconds" else 200)
            if type(number) not in (int, float) or not math.isfinite(number) or not low <= number <= high:
                raise Rejected("invalid_owntracks_configuration")
        zones = item.get("zones", {})
        if not isinstance(zones, dict) or len(zones) > 20:
            raise Rejected("invalid_owntracks_configuration")
        for name, zone in zones.items():
            if not ID.fullmatch(name) or not isinstance(zone, dict):
                raise Rejected("invalid_owntracks_zone")
            if set(zone) != {"latitude", "longitude", "radius_meters"}:
                raise Rejected("invalid_owntracks_zone")
            lat, lon, radius = zone["latitude"], zone["longitude"], zone["radius_meters"]
            if any(type(n) not in (int, float) or not math.isfinite(n) for n in (lat, lon, radius)):
                raise Rejected("invalid_owntracks_zone")
            if not (-90 <= lat <= 90 and -180 <= lon <= 180 and 10 <= radius <= 5000):
                raise Rejected("invalid_owntracks_zone")


def validate_hub_config(config):
    if not isinstance(config, dict):
        raise Rejected("invalid_configuration")
    for field in ("enabled", "mock", "trust_loopback_proxy"):
        if field in config and type(config[field]) is not bool:
            raise Rejected("invalid_configuration_boolean")
    _identity_secrets(config.get("clients"), "invalid_client_configuration")
    _identity_secrets(config.get("agents"), "invalid_agent_configuration")
    if not set(config["clients"]) <= {"scaramouche", "wanderer"}:
        raise Rejected("invalid_client_configuration")
    _positive_ids(config.get("admins", []), "invalid_admin_configuration")
    retention = config.get("audit_retention_seconds", 30 * 86400)
    if (
        type(retention) not in (int, float)
        or not math.isfinite(retention)
        or not 60 <= retention <= 30 * 86400
    ):
        raise Rejected("invalid_audit_retention")
    devices = registry(config.get("devices", {}))
    known_agents = set(config["agents"])
    for item in devices.values():
        if item.get("agent") not in known_agents:
            raise Rejected("unknown_device_agent")
    _owntracks(config.get("owntracks", {}))
    if any(item["type"] == "cast" for item in devices.values()) and "public_url" not in config:
        raise Rejected("missing_media_origin")
    if "public_url" in config:
        secure_url(config["public_url"], mock=config.get("mock", False))
    return config


def validate_agent_config(config):
    if not isinstance(config, dict):
        raise Rejected("invalid_configuration")
    for field in ("enabled", "mock"):
        if field in config and type(config[field]) is not bool:
            raise Rejected("invalid_configuration_boolean")
    identity = config.get("agent_id")
    if not isinstance(identity, str) or not ID.fullmatch(identity):
        raise Rejected("invalid_agent_configuration")
    if not secret_ok(config.get("secret")):
        raise Rejected("weak_credential")
    secure_url(config.get("url", ""), websocket=True, mock=config.get("mock", False))
    retention = config.get("audit_retention_seconds", 30 * 86400)
    if (
        type(retention) not in (int, float)
        or not math.isfinite(retention)
        or not 60 <= retention <= 30 * 86400
    ):
        raise Rejected("invalid_audit_retention")
    devices = registry(config.get("devices", {}))
    for item in devices.values():
        if item.get("agent") != identity:
            raise Rejected("wrong_device_agent")
    from home_agent.devices import validate_devices

    validate_devices(config)
    return config
