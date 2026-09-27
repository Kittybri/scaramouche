"""TLS control endpoint: outbound relay connections, signed bot RPC, OwnTracks ingress."""

from __future__ import annotations
import asyncio
import base64
import hmac
import json
import os
import ssl
import time
from pathlib import Path
from aiohttp import web, WSMsgType
from .protocol import Rejected, verify, sign, validate, secret_ok
from .permissions import registry, authorize
from .storage import Store
from .location import ingest, events, delete_location
from .media import AudioVault


class Hub:
    def __init__(self, config):
        self.config = config
        for field in ("enabled", "mock", "trust_loopback_proxy"):
            if field in config and type(config[field]) is not bool:
                raise Rejected("invalid_configuration_boolean")
        self.devices = registry(config.get("devices", {}))
        self.store = Store(config.get("database", "home.db"))
        self.agents = {}
        self.connecting = set()
        self.pending = {}
        self.last_health = {}
        self.vault = AudioVault(
            config.get("public_url", "https://localhost"), config.get("mock", False)
        )
        for secret in list(config.get("clients", {}).values()) + list(
            config.get("agents", {}).values()
        ):
            if not secret_ok(secret):
                raise Rejected("weak_credential")
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
            await asyncio.sleep(60)

    async def close(self, app):
        self.maintenance.cancel()
        await asyncio.gather(self.maintenance, return_exceptions=True)
        for ws in list(self.agents.values()):
            await ws.close()
        for _, future in list(self.pending.values()):
            if not future.done():
                future.cancel()
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
                for aid, future in list(self.pending.values()):
                    if (
                        device is None or self.devices[device].get("agent") == aid
                    ) and not future.done():
                        future.set_result({"result": "cancelled"})
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
            return web.json_response(
                {
                    "ok": True,
                    "enabled": bool(await self.enabled()),
                    "devices": rows,
                    "agents": {
                        a: {
                            "online": a in self.agents,
                            "last_heartbeat": self.last_health.get(a, 0),
                        }
                        for a in self.config.get("agents", {})
                    },
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
                and d["type"] == "cast"
                for d in self.devices.values()
            ):
                raise Rejected("audio_user_denied")
            key = self.vault.add(payload.get("data"), payload.get("mime"), uid, client)
            return web.json_response({"ok": True, "asset": key})
        if op == "confirm":
            proposal = await self.store.get(
                "proposal:" + str(payload.get("request_id", ""))
            )
            if not proposal or proposal["user_id"] != uid or proposal["bot"] != client:
                raise Rejected("unknown_confirmation")
            c = validate(proposal)
            c["confirmed"] = True
        elif op == "action":
            c = validate(payload.get("command"))
            if c["user_id"] != uid or c["bot"] != client or c["confirmed"]:
                raise Rejected("identity_mismatch")
        else:
            raise Rejected("unknown_operation")
        try:
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
        if any(agent == aid for agent, _ in self.pending.values()):
            await self.store.result(c["request_id"], "failed")
            return {"ok": False, "error": "relay_busy"}
        media = None
        future = asyncio.get_running_loop().create_future()
        self.pending[c["request_id"]] = (aid, future)
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
            await self.store.result(c["request_id"], outcome)
            return {"ok": outcome in {"completed", "submitted"}, "result": outcome}
        except asyncio.TimeoutError:
            await self.store.result(c["request_id"], "timeout")
            return {"ok": False, "error": "relay_timeout"}
        except asyncio.CancelledError:
            await self.store.result(c["request_id"], "cancelled")
            raise
        except Exception:
            await self.store.result(c["request_id"], "failed")
            return {"ok": False, "error": "action_failed"}
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
        ws = web.WebSocketResponse(heartbeat=15, max_msg_size=16000)
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
                        or not set(advertised) <= expected
                    ):
                        await ws.close(code=1008)
                        break
                    self.last_health[aid] = time.time()
                    await self.store.clean()
                elif data.get("type") == "result" and data.get("result") in {
                    "completed",
                    "submitted",
                    "failed",
                    "cancelled",
                }:
                    pair = self.pending.get(data.get("request_id"))
                    if pair and pair[0] == aid and not pair[1].done():
                        pair[1].set_result(data)
                else:
                    await ws.close(code=1008)
                    break
        except Exception:
            await ws.close(code=1008)
        finally:
            self.connecting.discard(aid)
            if self.agents.get(aid) is ws:
                self.agents.pop(aid, None)
            for agent, future in list(self.pending.values()):
                if agent == aid and not future.done():
                    future.set_result({"result": "failed"})
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
    config = json.loads(Path(args.config).read_text())
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
