"""Companion extension of the existing relay; consent durable, observations RAM-only."""

import time
from .protocol import Rejected, sign
from .companion import presence


class CompanionHub:
    def __init__(self, hub):
        self.hub = hub
        self.current = {}
        self.health = {}
        self.development = {}

    def device(self, uid, bot):
        matches = [
            (key, d)
            for key, d in self.hub.devices.items()
            if d["type"] == "computer"
            and d.get("owner_user_id") == uid
            and bot in d.get("bots", [])
        ]
        if len(matches) != 1:
            raise Rejected("computer_not_configured")
        return matches[0]

    async def consent(self, key):
        return await self.hub.store.get(
            "pc_consent:" + key, {"enabled": False, "screen": False}
        )

    def expire(self):
        for key, value in list(self.current.items()):
            try:
                presence(value)
            except Rejected:
                self.current.pop(key, None)
        self.development = {
            k: v for k, v in self.development.items() if v[1] > time.time()
        }

    async def sync(self, aid, ws):
        for key, d in self.hub.devices.items():
            if d["type"] == "computer" and d.get("agent") == aid:
                consent = await self.consent(key)
                if (
                    not await self.hub.enabled(key)
                    or not d.get("enabled", False)
                    or d.get("mode", "DISABLED") == "DISABLED"
                ):
                    consent = {"enabled": False, "screen": False}
                await ws.send_json(
                    sign(
                        self.hub.config["agents"][aid],
                        "control:" + aid,
                        {"type": "companion", "device": key, "consent": consent},
                    )
                )

    def disconnect(self, aid):
        self.health.pop(aid, None)
        for key, d in self.hub.devices.items():
            if d.get("agent") == aid:
                self.current.pop(key, None)
                self.development.pop(key, None)

    async def ingest(self, aid, payload):
        if not isinstance(payload, dict) or set(payload) != {"device", "presence"}:
            raise Rejected("invalid_computer_event")
        key = payload["device"]
        d = self.hub.devices.get(key, {})
        if d.get("type") != "computer" or d.get("agent") != aid:
            raise Rejected("computer_agent_mismatch")
        if "computer_presence" not in d.get("capabilities", []):
            raise Rejected("computer_presence_denied")
        if (
            not d.get("enabled", False)
            or d.get("mode", "DISABLED") == "DISABLED"
            or not (await self.consent(key)).get("enabled")
            or not await self.hub.enabled(key)
        ):
            self.current.pop(key, None)
            return
        if payload["presence"] is None:
            self.current.pop(key, None)
        else:
            self.current[key] = presence(payload["presence"])
            if "development_events" not in d.get("capabilities", []):
                self.current[key]["event"] = ""
            if self.current[key].get("event"):
                self.development[key] = (self.current[key]["event"], time.time() + 120)

    async def rpc(self, op, uid, bot):
        key, d = self.device(uid, bot)
        consent = await self.consent(key)
        if op in {"pc_on", "pc_off", "pc_forget", "pc_screen_on", "pc_screen_off"}:
            self.current.pop(key, None)
            self.development.pop(key, None)
            if op == "pc_on":
                consent = {"enabled": True, "screen": False}
            elif op in {"pc_off", "pc_forget"}:
                consent = {"enabled": False, "screen": False}
            else:
                consent["screen"] = op == "pc_screen_on" and consent.get(
                    "enabled", False
                )
            await self.hub.store.put("pc_consent:" + key, consent)
            if op == "pc_forget":
                async with self.hub.store.db() as db:
                    await db.execute(
                        "DELETE FROM home_audit WHERE device=? AND user_id=?",
                        (key, uid),
                    )
                    await db.execute(
                        "DELETE FROM home_state WHERE key LIKE 'proposal:%' AND json_extract(payload,'$.device_id')=?",
                        (key,),
                    )
                    await db.commit()
                self.hub.vault.items = {
                    k: v for k, v in self.hub.vault.items.items() if v["user_id"] != uid
                }
            ws = self.hub.agents.get(d.get("agent"))
            if ws:
                await self.sync(d["agent"], ws)
        elif op != "pc_status":
            raise Rejected("unknown_pc_operation")
        online = (
            d.get("agent") in self.hub.agents
            and time.time() - self.hub.last_health.get(d.get("agent"), 0) < 30
        )
        current = None
        if online and consent.get("enabled") and await self.hub.enabled(key):
            try:
                current = presence(self.current.get(key))
            except Rejected:
                self.current.pop(key, None)
        event, expires = self.development.get(key, ("", 0))
        if current and expires > time.time():
            current["event"] = event
        elif expires <= time.time():
            self.development.pop(key, None)
        return {
            "ok": True,
            "device": key,
            "online": online,
            "consent": consent,
            "presence": current,
            "mode": d.get("mode", "DISABLED"),
            "capabilities": d.get("capabilities", []),
            "local_status": self.health.get(d.get("agent"), {}) if online else {},
        }

    async def allowed(self, c):
        d = self.hub.devices.get(c["device_id"], {})
        if d.get("type") == "computer":
            consent = await self.consent(c["device_id"])
            if (
                not consent.get("enabled")
                or c["action"] == "screen"
                and not consent.get("screen")
            ):
                raise Rejected("companion_optin_required")
