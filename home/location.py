"""Reduce OwnTracks input to short-lived user-owned zone events, never coordinates."""

from __future__ import annotations
import math
import time
from .protocol import Rejected, ID


def zone_for(payload, config):
    zones = config.get("zones", {})
    if payload.get("_type") == "transition":
        label = payload.get("desc")
        if (
            label not in zones
            or not ID.fullmatch(label)
            or payload.get("event") not in {"enter", "leave"}
        ):
            raise Rejected("unknown_transition")
        return label if payload["event"] == "enter" else "AWAY"
    if payload.get("_type") != "location":
        raise Rejected("unsupported_location")
    lat, lon = payload.get("lat"), payload.get("lon")
    if (
        any(type(n) not in (int, float) or not math.isfinite(n) for n in (lat, lon))
        or not -90 <= lat <= 90
        or not -180 <= lon <= 180
    ):
        raise Rejected("invalid_coordinates")
    accuracy = payload.get("acc", 0)
    if (
        type(accuracy) not in (int, float)
        or not math.isfinite(accuracy)
        or accuracy < 0
        or accuracy > max(10, min(1000, float(config.get("max_accuracy_m", 200))))
    ):
        raise Rejected("inaccurate_location")
    for name, z in list(zones.items())[:20]:
        if not ID.fullmatch(name):
            continue
        a, b = math.radians(lat), math.radians(float(z["latitude"]))
        distance = (
            6371000
            * 2
            * math.asin(
                min(
                    1,
                    math.sqrt(
                        math.sin((a - b) / 2) ** 2
                        + math.cos(a)
                        * math.cos(b)
                        * math.sin(math.radians(lon - float(z["longitude"])) / 2) ** 2
                    ),
                )
            )
        )
        if distance <= max(10, min(5000, float(z.get("radius_meters", 100)))):
            return name
    return "AWAY"


async def ingest(store, user_id, config, payload, now=None):
    now = time.time() if now is None else now
    if not config.get("enabled", False) or not await store.get(
        f"location_optin:{user_id}", False
    ):
        raise Rejected("location_disabled")
    stamp = payload.get("tst")
    if (
        type(stamp) not in (int, float)
        or not math.isfinite(stamp)
        or not now - 300 <= stamp <= now + 10
    ):
        raise Rejected("stale_location")
    zone = zone_for(payload, config)
    async with store.db() as db:
        await db.execute("BEGIN IMMEDIATE")
        row = await (
            await db.execute(
                "SELECT payload FROM home_state WHERE key=?",
                (f"location_last:{user_id}",),
            )
        ).fetchone()
        import json

        old = json.loads(row[0]) if row else {}
        if (
            payload.get("_type") == "transition"
            and payload.get("event") == "leave"
            and old.get("zone") != payload.get("desc")
        ):
            return False
        if (
            stamp <= old.get("source_ts", 0)
            or zone == old.get("zone")
            or now - old.get("ts", 0)
            < max(60, int(config.get("debounce_seconds", 300)))
        ):
            return False
        state = {"zone": zone, "ts": now, "source_ts": stamp}
        await db.execute(
            "INSERT INTO home_state VALUES(?,?) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload",
            (f"location_last:{user_id}", json.dumps(state)),
        )
        for bot in config.get("bots", []):
            if bot not in {"scaramouche", "wanderer"}:
                continue
            if old.get("zone") and old["zone"] != "AWAY":
                await db.execute(
                    "INSERT INTO home_events(ts,user_id,bot,kind,zone) VALUES(?,?,?,?,?)",
                    (now, user_id, bot, "location.left", old["zone"]),
                )
            if zone != "AWAY":
                await db.execute(
                    "INSERT INTO home_events(ts,user_id,bot,kind,zone) VALUES(?,?,?,?,?)",
                    (now, user_id, bot, "location.entered", zone),
                )
        await db.commit()
    return True


async def delete_location(store, user_id, disable=True):
    async with store.db() as db:
        await db.execute("BEGIN IMMEDIATE")
        await db.execute(
            "DELETE FROM home_events WHERE user_id=? AND kind LIKE 'location.%'",
            (user_id,),
        )
        await db.execute(
            "DELETE FROM home_state WHERE key=?", (f"location_last:{user_id}",)
        )
        if disable:
            await db.execute(
                "INSERT INTO home_state VALUES(?,?) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload",
                (f"location_optin:{user_id}", "false"),
            )
        await db.commit()


async def events(store, user_id, bot):
    async with store.db() as db:
        await db.execute("BEGIN IMMEDIATE")
        rows = await (
            await db.execute(
                "SELECT id,ts,kind,zone FROM home_events WHERE user_id=? AND bot=? AND consumed=0 AND ts>? ORDER BY id DESC LIMIT 2",
                (user_id, bot, time.time() - 900),
            )
        ).fetchall()
        await db.execute(
            "UPDATE home_events SET consumed=1 WHERE user_id=? AND bot=?",
            (user_id, bot),
        )
        await db.commit()
    return [dict(r) for r in rows]
