"""TLS control endpoint: outbound relay connections, signed bot RPC, OwnTracks ingress."""

from __future__ import annotations
import asyncio
import base64
import hmac
import json
import os
import ssl
import time
from dataclasses import dataclass
from aiohttp import web, WSMsgType
from .protocol import Rejected, verify, sign, validate, failure_category, secret_ok
from .permissions import registry, authorize
from .storage import Store
from .location import ingest, events, delete_location
from .media import AudioVault
from .config import validate_hub_config, load_config


@dataclass
class PendingAction:
    agent_id: str
    device_id: str
    future: asyncio.Future


class Hub:
    def __init__(self, config):
        self.config = validate_hub_config(config)
        self.devices = registry(config.get("devices", {}))
        self.store = Store(
            config.get("database", "home.db"),
            config.get("audit_retention_seconds", 30 * 86400),
        )
        self.agents = {}
        self.connecting = set()
        self.pending = {}
        self.last_health = {}
        from .companion_hub import CompanionHub

        self.companion = CompanionHub(self)
        self.vault = AudioVault(
            config.get("public_url", "https://localhost"), config.get("mock", False)
        )
        self.app = web.Application(client_max_size=7_100_000, middlewares=[self.guard])
        self.app.router.add_post("/rpc/{client}", self.rpc)
        self.app.router.add_get("/agent/{agent}", self.agent)
        self.app.router.add_post("/owntracks/{account}", self.owntracks)
        self.app.router.add_get("/audio/{key}", self.audio)
        self.app.on_startup.append(self.start)
        self.app.on_cleanup.append(self.close)

    async def start(self, app):
        await self.store.init()
        self.maintenance = asyncio.create_task(self.maintain())

    async def maintain(self):
        while True:
            try:
                await self.store.clean()
            except Exception:
                pass  # Retry next minute after a transient database failure.
            self.vault.cleanup()
            self.companion.expire()
            await asyncio.sleep(60)

    async def close(self, app):
        self.maintenance.cancel()
        await asyncio.gather(self.maintenance, return_exceptions=True)
        for ws in list(self.agents.values()):
            await ws.close()
        for pending in list(self.pending.values()):
            if not pending.future.done():
                pending.future.cancel()
        self.vault.items.clear()

    @web.middleware
    async def guard(self, request, handler):
        peer = (
            request.transport.get_extra_info("peername") if request.transport else None
        )
        loopback = bool(peer and peer[0] in {"127.0.0.1", "::1"})
        tls = request.secure or (
            loopback
            and self.config.get("trust_loopback_proxy", False)
            and request.headers.get("X-Forwarded-Proto") == "https"
        )
        if not tls and not (loopback and self.config.get("mock", False)):
            return web.json_response({"ok": False, "error": "tls_required"}, status=403)
        try:
            return await handler(request)
        except Rejected as exc:
            return web.json_response({"ok": False, "error": str(exc)}, status=400)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Never render/log exception text, payloads, credentials or coordinates.
            return web.json_response(
                {"ok": False, "error": "request_failed"}, status=400
            )

    async def enabled(self, device=None):
        if (
            not self.config.get("enabled", False)
            or os.getenv("HOME_AUTOMATION_ENABLED", "").lower() == "false"
        ):
            return False
        return not await self.store.get("disabled", False) and not (
            device and await self.store.get("disabled:" + device, False)
        )

    async def authenticate(self, identity, secret, domain, envelope):
        payload = verify(secret, domain, envelope)
        if not await self.store.claim(
            "nonce:" + identity + ":" + envelope["nonce"], 120
        ):
            raise Rejected("replayed_envelope")
        if not await self.store.rate(identity):
            raise Rejected("rate_limited")
        return payload

    async def rpc(self, request):
        client = request.match_info["client"]
        secret = self.config.get("clients", {}).get(client, "")
        payload = await self.authenticate(
            "client:" + client, secret, "rpc:" + client, await request.json()
        )
        if not isinstance(payload, dict) or type(payload.get("user_id")) is not int:
            raise Rejected("invalid_rpc")
        uid = payload["user_id"]
        op = payload.get("op")
        if isinstance(op, str) and op.startswith("pc_"):
            return web.json_response(await self.companion.rpc(op, uid, client))
        admin = uid in self.config.get("admins", [])
        if (
            op
            in {
                "status",
                "devices",
                "agent",
                "permissions",
                "audit",
                "disable",
                "enable",
            }
            and not admin
        ):
            raise Rejected("admin_required")
        if op in {"disable", "enable"}:
            device = payload.get("device")
            if device is not None and device not in self.devices:
                raise Rejected("unknown_device")
            await self.store.put(
                "disabled" + (":" + device if device else ""), op == "disable"
            )
            if op in {"disable", "enable"}:
                await asyncio.gather(
                    *(
                        ws.send_json(
                            sign(
                                self.config["agents"][aid],
                                "control:" + aid,
                                {"type": op, "device": device},
                            )
                        )
                        for aid, ws in list(self.agents.items())
                    ),
                    return_exceptions=True,
                )
            if op == "disable":
                for pending in list(self.pending.values()):
                    if (
                        device is None or pending.device_id == device
                    ) and not pending.future.done():
                        pending.future.set_result({
                            "result": "cancelled", "device_id": pending.device_id,
                        })
            return web.json_response({"ok": True, "result": op})
        if op in {"status", "devices", "agent", "permissions"}:
            rows = {}
            for key, d in self.devices.items():
                rows[key] = {
                    "type": d["type"],
                    "mode": d.get("mode", "DISABLED"),
                    "actions": d.get("actions", []),
                    "enabled": await self.enabled(key) and d.get("enabled", False),
                    "agent_online": d.get("agent") in self.agents,
                }
                rows[key].update(await self.store.device_status(key, d))
            now = time.time()
            agents = {}
            for agent_id in self.config.get("agents", {}):
                last = self.last_health.get(agent_id, 0)
                age = round(max(0, now - last), 1) if last else None
                online = agent_id in self.agents
                agents[agent_id] = {
                    "online": online,
                    "health": (
                        "healthy" if online and age is not None and age <= 30
                        else "degraded" if online and age is not None and age <= 60
                        else "unavailable"
                    ),
                    "health_age_seconds": age,
                    "last_heartbeat": last,
                }
            health_counts = {
                label: sum(item["health"] == label for item in agents.values())
                for label in ("healthy", "degraded", "unavailable")
            }
            for device_id, item in rows.items():
                agent_health = agents.get(
                    self.devices[device_id].get("agent"), {}
                ).get("health", "unavailable")
                item["health"] = (
                    "unavailable" if agent_health == "unavailable"
                    else "degraded" if item["last_result"] in {
                        "failed", "provider_error", "offline", "timeout"
                    }
                    else agent_health
                )
            return web.json_response(
                {
                    "ok": True,
                    "enabled": bool(await self.enabled()),
                    "relay_configured": bool(self.config.get("agents")),
                    "relay_reachable": bool(agents) and all(
                        item["health"] in {"healthy", "degraded"}
                        for item in agents.values()
                    ),
                    "health_summary": health_counts,
                    "devices": rows,
                    "agents": agents,
                    "recent_actions": await self.store.audit(),
                }
            )
        if op == "audit":
            return web.json_response({"ok": True, "actions": await self.store.audit()})
        if op in {"location_on", "location_off", "location_delete", "events"}:
            accounts = [
                a
                for a in self.config.get("owntracks", {}).values()
                if a.get("user_id") == uid and client in a.get("bots", [])
            ]
            if not accounts:
                raise Rejected("location_user_not_configured")
            if op == "location_on":
                await self.store.put(f"location_optin:{uid}", True)
            elif op in {"location_off", "location_delete"}:
                await delete_location(self.store, uid)
            return web.json_response(
                {
                    "ok": True,
                    "events": (
                        await events(self.store, uid, client) if op == "events" else []
                    ),
                }
            )
        if op == "audio":
            if not await self.enabled():
                raise Rejected("automation_disabled")
            if not any(
                uid in d.get("users", [])
                and client in d.get("bots", [])
                and d["type"] in {"cast", "computer"}
                for d in self.devices.values()
            ):
                raise Rejected("audio_user_denied")
            key = self.vault.add(payload.get("data"), payload.get("mime"), uid, client)
            return web.json_response({"ok": True, "asset": key})
        if op == "confirm":
            request_id = payload.get("request_id", "")
            guild_id = payload.get("guild_id")
            if (
                not isinstance(request_id, str)
                or type(guild_id) is not int
                or guild_id < 0
            ):
                raise Rejected("unknown_confirmation")
            proposal = await self.store.consume_proposal(
                request_id, uid, guild_id, client
            )
            c = validate(proposal)
            c = dict(c, confirmed=True)
        elif op == "action":
            c = validate(payload.get("command"))
            if c["user_id"] != uid or c["bot"] != client or c["confirmed"]:
                raise Rejected("identity_mismatch")
        else:
            raise Rejected("unknown_operation")
        try:
            await self.companion.allowed(c)
            c = authorize(c, self.devices, await self.enabled(c["device_id"]))
        except Rejected as exc:
            if str(exc) == "confirmation_required":
                await self.store.put("proposal:" + c["request_id"], c)
                return web.json_response(
                    {
                        "ok": True,
                        "confirmation_required": c["request_id"],
                        "expires_at": c["expires_at"],
                    }
                )
            await self.store.rejection(c, failure_category(exc))
            raise
        return web.json_response(await self.dispatch(c))

    async def dispatch(self, c):
        d = self.devices[c["device_id"]]
        aid = d.get("agent")
        await self.store.reserve(c, d)
        ws = self.agents.get(aid)
        if not ws or ws.closed:
            await self.store.result(c["request_id"], "offline")
            return {"ok": False, "error": "relay_offline"}
        if c["action"] not in {"stop", "pause"} and any(
            pending.agent_id == aid for pending in self.pending.values()
        ):
            await self.store.result(c["request_id"], "provider_error")
            return {"ok": False, "error": "relay_busy"}
        media = None
        future = asyncio.get_running_loop().create_future()
        self.pending[c["request_id"]] = PendingAction(aid, c["device_id"], future)
        try:
            if c["action"] == "play" and c["parameters"]["asset"] not in d.get(
                "assets", []
            ):
                media = self.vault.resolve(
                    c["parameters"]["asset"], c["user_id"], c["bot"]
                )
            if not await self.enabled(c["device_id"]):
                raise Rejected("automation_disabled")
            envelope = sign(
                self.config["agents"][aid],
                "command:" + aid,
                {"command": c, "media": media},
            )
            await ws.send_json(envelope)
            result = await asyncio.wait_for(
                future, max(0.1, min(25, c["expires_at"] - time.time()))
            )
            outcome = result.get("result", "failed")
            category = result.get("error_category", "provider_error")
            if category not in {"permission", "expired", "offline", "provider_error"}:
                category = "provider_error"
            audit_outcome = (
                category
                if outcome == "failed" else outcome
            )
            await self.store.result(c["request_id"], audit_outcome)
            response = {"ok": outcome in {"completed", "submitted"}, "result": outcome}
            if outcome == "failed":
                response["error"] = audit_outcome
            if c["action"] == "screen" and outcome == "completed":
                await self.companion.allowed(c)
                screen = result.get("screen", {})
                if (
                    not isinstance(screen.get("image"), str)
                    or len(screen["image"]) > 470000
                ):
                    raise Rejected("invalid_screen")
                response["screen"] = screen
            return response
        except asyncio.TimeoutError:
            await self.store.result(c["request_id"], "timeout")
            return {"ok": False, "error": "relay_timeout"}
        except asyncio.CancelledError:
            await self.store.result(c["request_id"], "cancelled")
            raise
        except Rejected as exc:
            category = failure_category(exc)
            await self.store.result(c["request_id"], category)
            return {"ok": False, "error": category}
        except Exception:
            await self.store.result(c["request_id"], "provider_error")
            return {"ok": False, "error": "provider_error"}
        finally:
            self.pending.pop(c["request_id"], None)
            if media:
                self.vault.items.pop(c["parameters"]["asset"], None)

    async def agent(self, request):
        aid = request.match_info["agent"]
        secret = self.config.get("agents", {}).get(aid, "")
        try:
            envelope = json.loads(
                base64.b64decode(request.headers.get("X-Home-Auth", ""), validate=True)
            )
        except Exception:
            raise Rejected("authentication_failed")
        payload = await self.authenticate(
            "agent:" + aid, secret, "agent-connect:" + aid, envelope
        )
        if payload != {"agent_id": aid}:
            raise Rejected("identity_mismatch")
        if aid in self.agents or aid in self.connecting:
            raise Rejected("duplicate_agent")
        self.connecting.add(aid)
        ws = web.WebSocketResponse(heartbeat=15, max_msg_size=500000)
        try:
            await ws.prepare(request)
            # Reconcile persistent kill switches before accepting any new actions.
            for device in [None] + [
                k for k, d in self.devices.items() if d.get("agent") == aid
            ]:
                disabled = await self.store.get(
                    "disabled" + (":" + device if device else ""), False
                )
                await ws.send_json(
                    sign(
                        secret,
                        "control:" + aid,
                        {"type": "disable" if disabled else "enable", "device": device},
                    )
                )
            await self.companion.sync(aid, ws)
            self.agents[aid] = ws
            self.last_health[aid] = time.time()
            async for msg in ws:
                if msg.type != WSMsgType.TEXT:
                    continue
                data = json.loads(msg.data)
                if data.get("type") in {"hello", "health"}:
                    advertised = data.get("devices", [])
                    expected = {
                        k for k, d in self.devices.items() if d.get("agent") == aid
                    }
                    if (
                        not isinstance(advertised, list)
                        or len(advertised) != len(set(advertised))
                        or set(advertised) != expected
                    ):
                        await ws.close(code=1008)
                        break
                    self.last_health[aid] = time.time()
                    self.companion.health[aid] = {
                        key: data.get("computer_status", {}).get(key) is True
                        for key in ("sensing", "screens", "audio")
                    }
                    await self.store.clean()
                elif data.get("type") == "computer":
                    payload = await self.authenticate(
                        "computer:" + aid,
                        secret,
                        "computer:" + aid,
                        data.get("envelope"),
                    )
                    await self.companion.ingest(aid, payload)
                elif data.get("type") == "result" and data.get("result") in {
                    "completed",
                    "submitted",
                    "failed",
                    "cancelled",
                }:
                    pending = self.pending.get(data.get("request_id"))
                    if (
                        pending
                        and pending.agent_id == aid
                        and data.get("device_id") == pending.device_id
                        and not pending.future.done()
                    ):
                        pending.future.set_result(data)
                else:
                    await ws.close(code=1008)
                    break
        except Exception:
            await ws.close(code=1008)
        finally:
            self.companion.disconnect(aid)
            self.connecting.discard(aid)
            if self.agents.get(aid) is ws:
                self.agents.pop(aid, None)
            for pending in list(self.pending.values()):
                if pending.agent_id == aid and not pending.future.done():
                    pending.future.set_result({
                        "result": "failed", "device_id": pending.device_id,
                    })
        return ws

    async def owntracks(self, request):
        account = self.config.get("owntracks", {}).get(
            request.match_info["account"], {}
        )
        from aiohttp import BasicAuth

        try:
            auth = BasicAuth.decode(request.headers.get("Authorization", ""))
        except Exception:
            raise Rejected("authentication_failed")
        if (
            not secret_ok(account.get("password", ""))
            or not hmac.compare_digest(auth.login, str(account.get("username", "")))
            or not hmac.compare_digest(auth.password, account["password"])
        ):
            raise Rejected("authentication_failed")
        uid = account["user_id"]
        if not await self.store.claim(f"owntracks_rate:{uid}:{int(time.time()//2)}", 4):
            raise Rejected("rate_limited")
        if request.content_length and request.content_length > 8192:
            raise Rejected("oversized_location")
        raw = await request.content.read(8193)
        if len(raw) > 8192:
            raise Rejected("oversized_location")
        await ingest(self.store, uid, account, json.loads(raw))
        await self.store.clean()
        return web.json_response([])

    async def audio(self, request):
        value = self.vault.fetch(request.match_info["key"])
        return web.Response(
            body=value["data"],
            content_type=value["mime"],
            headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"},
        )


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--validate", action="store_true")
    args = parser.parse_args()
    config = load_config(args.config)
    hub = Hub(config)
    if args.validate:
        print("Home hub configuration valid; credentials redacted.")
        return
    context = None
    if config.get("tls_cert") and config.get("tls_key"):
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(config["tls_cert"], config["tls_key"])
    elif not config.get("trust_loopback_proxy") and not config.get("mock"):
        raise SystemExit("TLS certificate or trusted loopback HTTPS proxy required")
    host = "127.0.0.1" if context is None else config.get("bind", "127.0.0.1")
    web.run_app(
        hub.app,
        host=host,
        port=int(config.get("port", 8765)),
        ssl_context=context,
        access_log=None,
        print=None,
    )


if __name__ == "__main__":
    main()
