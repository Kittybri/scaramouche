"""Outbound WSS relay with durable replay protection and independent local policy."""

from __future__ import annotations
import asyncio
import base64
import json
import logging
import random
import signal
import time
import aiohttp
from home.protocol import (
    Rejected,
    canonical,
    sign,
    verify,
    secure_url,
    validate,
    failure_category,
)
from home.permissions import registry, authorize
from home.storage import Store
from home.config import validate_agent_config, load_config
from .devices import providers


class Agent:
    def __init__(self, config, adapters=None):
        # Vendor debug/error output may contain device credentials or audio URLs.
        # Structured result codes are the only diagnostics exported by this relay.
        for name in ("kasa", "pychromecast", "zeroconf"):
            logger = logging.getLogger(name)
            logger.propagate = False
            if not any(isinstance(h, logging.NullHandler) for h in logger.handlers):
                logger.addHandler(logging.NullHandler())
        self.config = validate_agent_config(config)
        self.id = config["agent_id"]
        self.secret = config["secret"]
        self.url = secure_url(
            config["url"], websocket=True, mock=config.get("mock", False)
        )
        self.devices = registry(config.get("devices", {}))
        self.store = Store(
            config.get("database", "home-agent.db"),
            config.get("audit_retention_seconds", 30 * 86400),
        )
        self.adapters = adapters or providers(config)
        from .computer.runtime import Computer
        from .computer.base import Platform
        import sys

        pc = config.get("companion", {})
        platform = Platform()
        if pc.get("enabled") and not config.get("mock") and sys.platform == "darwin":
            from .computer.macos import MacOS

            platform = MacOS()
        self.computer = Computer(pc, platform)
        self.adapters["computer"] = self.computer
        self.computer_devices = [
            key for key, d in self.devices.items() if d["type"] == "computer"
        ]
        if len(self.computer_devices) > 1:
            raise Rejected("one_computer_per_agent")
        self.stopping = asyncio.Event()
        self.lock = asyncio.Lock()
        self.tasks = set()

    async def execute(self, envelope):
        payload = verify(self.secret, "command:" + self.id, envelope)
        if not await self.store.claim("nonce:" + envelope["nonce"], 120):
            raise Rejected("replayed_envelope")
        if not isinstance(payload, dict) or set(payload) != {"command", "media"}:
            raise Rejected("malformed_request")
        c = validate(payload["command"])
        device_type = self.devices.get(c["device_id"], {}).get("type")
        interrupt = (
            (c["action"] == "stop" and device_type == "computer")
            or (c["action"] in {"pause", "stop"} and device_type == "cast")
        )
        if self.lock.locked() and not interrupt:
            raise Rejected("agent_busy")
        if interrupt:
            # Stop/pause must be able to interrupt a bounded bot-owned session.
            authorize(
                c,
                self.devices,
                self.config.get("enabled", False)
                and (self.computer.enabled if device_type == "computer" else True)
                and not await self.store.get("disabled", False)
                and not await self.store.get("disabled:" + c["device_id"], False),
            )
            await self.store.reserve(c, self.devices[c["device_id"]])
            error_category = "provider_error"
            try:
                if device_type == "computer":
                    await self.computer.platform.stop()
                    result = "completed"
                else:
                    d = dict(self.devices[c["device_id"]], _expires_at=c["expires_at"])
                    result = await self.adapters["cast"].execute(
                        d, c["action"], c["parameters"], payload["media"]
                    )
                result = result if result in {"completed", "submitted"} else "failed"
            except asyncio.CancelledError:
                await self.store.result(c["request_id"], "cancelled")
                raise
            except Exception as exc:
                result = "failed"
                error_category = failure_category(exc)
            await self.store.result(
                c["request_id"], error_category if result == "failed" else result
            )
            response = {
                "type": "result",
                "request_id": c["request_id"],
                "device_id": c["device_id"],
                "result": result,
            }
            if result == "failed":
                response["error_category"] = error_category
            return response
        async with self.lock:
            enabled = (
                self.config.get("enabled", False)
                and not await self.store.get("disabled", False)
                and not await self.store.get("disabled:" + c["device_id"], False)
            )
            import os

            enabled = (
                enabled and os.getenv("HOME_AUTOMATION_ENABLED", "").lower() != "false"
            )
            if self.devices.get(c["device_id"], {}).get("type") == "computer":
                enabled = enabled and self.computer.enabled
            c = authorize(c, self.devices, enabled)
            d = dict(self.devices[c["device_id"]], _expires_at=c["expires_at"])
            await self.store.reserve(c, d)
            error_category = "provider_error"
            try:
                remaining = c["expires_at"] - time.time()
                if remaining <= 0:
                    raise Rejected("expired_command")
                result = await asyncio.wait_for(
                    self.adapters[d["type"]].execute(
                        d, c["action"], c["parameters"], payload["media"]
                    ),
                    min(24, remaining),
                )
                screen = (
                    result
                    if c["action"] == "screen" and isinstance(result, dict)
                    else None
                )
                result = (
                    "completed"
                    if screen
                    else (
                        result
                        if isinstance(result, str)
                        and result in {"completed", "submitted"}
                        else "failed"
                    )
                )
            except asyncio.CancelledError:
                await self.store.result(c["request_id"], "cancelled")
                raise
            except Exception as exc:
                result = "failed"
                error_category = failure_category(exc)
            await self.store.result(
                c["request_id"], error_category if result == "failed" else result
            )
            response = {
                "type": "result",
                "request_id": c["request_id"],
                "device_id": c["device_id"],
                "result": result,
            }
            if result == "failed":
                response["error_category"] = error_category
            if result == "completed" and c["action"] == "screen":
                response["screen"] = screen
            return response

    async def health(self, ws):
        while not self.stopping.is_set():
            await ws.send_json(
                {
                    "type": "health",
                    "devices": list(self.devices),
                    "capabilities": sorted({d["type"] for d in self.devices.values()}),
                    "computer_status": {
                        "sensing": self.computer.enabled,
                        "screens": self.computer.enabled
                        and self.computer.consent["screen"]
                        and self.computer.config.get("screen_allowed", False),
                        "audio": self.computer.enabled
                        and self.computer.config.get("audio_allowed", False),
                        "capabilities": self.computer.config.get("capabilities", []),
                    },
                }
            )
            await self.store.clean()
            if self.computer_devices:
                device = self.computer_devices[0]
                if (
                    not self.devices[device].get("enabled", False)
                    or self.devices[device].get("mode", "DISABLED") == "DISABLED"
                ):
                    await self.computer.control({})
                if not self.computer.enabled:
                    await self.computer.platform.stop()
                import os

                if (
                    os.getenv("HOME_AUTOMATION_ENABLED", "").lower() == "false"
                    or not self.config.get("enabled", False)
                    or await self.store.get("disabled", False)
                    or await self.store.get("disabled:" + device, False)
                ):
                    await self.computer.control({})
                event = await self.store.get("pc_development_event", {})
                if event:
                    await self.store.put("pc_development_event", {})
                    from home.companion import EVENTS

                    if (
                        self.computer.enabled
                        and self.computer.config.get("development_events", False)
                        and "development_events"
                        in self.computer.config.get("capabilities", [])
                        and event.get("event") in EVENTS
                        and event.get("expires", 0) > time.time()
                    ):
                        self.computer.event = event["event"]
                value = self.computer.tick()
                if value is not None or not self.computer.enabled:
                    await ws.send_json(
                        {
                            "type": "computer",
                            "envelope": sign(
                                self.secret,
                                "computer:" + self.id,
                                {"device": device, "presence": value},
                            ),
                        }
                    )
            await asyncio.sleep(10)

    async def handle(self, ws, data):
        try:
            result = await self.execute(data)
            await ws.send_json(result)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Only echo an authenticated, format-validated request ID, never vendor errors.
            try:
                payload = verify(self.secret, "command:" + self.id, data)
                c = validate(payload["command"])
                rid = c["request_id"]
                import re

                if re.fullmatch(r"[0-9a-f]{32}", rid):
                    await ws.send_json(
                        {
                            "type": "result",
                            "request_id": rid,
                            "device_id": c["device_id"],
                            "result": "failed",
                            "error_category": failure_category(exc),
                        }
                    )
            except Exception:
                pass

    async def connect_once(self):
        auth = base64.b64encode(
            canonical(
                sign(self.secret, "agent-connect:" + self.id, {"agent_id": self.id})
            )
        ).decode()
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=None, sock_connect=15)
        ) as session:
            async with session.ws_connect(
                self.url + "/agent/" + self.id,
                headers={"X-Home-Auth": auth},
                heartbeat=15,
                max_msg_size=32000,
            ) as ws:
                await ws.send_json(
                    {
                        "type": "hello",
                        "devices": list(self.devices),
                        "capabilities": sorted(
                            {d["type"] for d in self.devices.values()}
                        ),
                    }
                )
                health = asyncio.create_task(self.health(ws))
                try:
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        data = json.loads(msg.data)
                        try:
                            control = verify(self.secret, "control:" + self.id, data)
                        except Rejected:
                            control = None
                        if control:
                            if not await self.store.claim(
                                "control:" + data["nonce"], 120
                            ):
                                continue
                            if (
                                control.get("type") == "companion"
                                and control.get("device") in self.computer_devices
                            ):
                                await self.computer.control(control.get("consent", {}))
                                if not self.computer.enabled:
                                    for task in list(self.tasks):
                                        task.cancel()
                                    await self.store.put("pc_development_event", {})
                                    async with self.store.db() as db:
                                        await db.execute(
                                            "DELETE FROM home_audit WHERE device=?",
                                            (control["device"],),
                                        )
                                        await db.commit()
                            elif control.get("type") in {"disable", "enable"}:
                                device = control.get("device")
                                if device is not None and device not in self.devices:
                                    continue
                                await self.store.put(
                                    "disabled" + (":" + device if device else ""),
                                    control["type"] == "disable",
                                )
                                if control["type"] == "disable":
                                    for task in list(self.tasks):
                                        task.cancel()
                                    for provider in self.adapters.values():
                                        await provider.close()
                        else:
                            if len(self.tasks) >= 2:
                                continue
                            task = asyncio.create_task(self.handle(ws, data))
                            self.tasks.add(task)
                            task.add_done_callback(self.tasks.discard)
                finally:
                    await self.computer.close()
                    health.cancel()
                    for task in list(self.tasks):
                        task.cancel()
                    await asyncio.gather(
                        health, *list(self.tasks), return_exceptions=True
                    )

    async def run(self):
        await self.store.init()
        attempt = 0
        try:
            while not self.stopping.is_set():
                try:
                    await self.connect_once()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass
                attempt = min(attempt + 1, 6)
                try:
                    await asyncio.wait_for(
                        self.stopping.wait(), timeout=backoff(attempt)
                    )
                except asyncio.TimeoutError:
                    pass
        finally:
            for provider in self.adapters.values():
                await provider.close()


def backoff(attempt):
    return min(60, 2 ** max(0, min(6, attempt))) + random.uniform(0, 1)


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--validate", action="store_true")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--verify-computer", action="store_true")
    from home.companion import EVENTS

    parser.add_argument("--event", choices=sorted(EVENTS))
    args = parser.parse_args()
    config = load_config(args.config)

    async def launch():
        agent = Agent(config)
        if args.validate or args.status:
            print("Home relay configuration valid; credentials redacted.")
            print(
                "Companion configured:",
                bool(agent.computer.config.get("enabled")),
                "No sensing or actions performed.",
            )
            return
        if args.event:
            if not agent.computer.config.get("development_events", False):
                raise SystemExit("Development events are disabled.")
            await agent.store.init()
            await agent.store.put(
                "pc_development_event",
                {"event": args.event, "expires": time.time() + 120},
            )
            print(
                "Local completion event submitted (expires in 120 seconds). No output/files read."
            )
            return
        if args.verify_computer:
            await agent.computer.control({"enabled": True})
            print(json.dumps(agent.computer.tick()))
            await agent.computer.close()
            return
        print(
            "Visible home/companion relay starting. Ctrl-C stops it. Screens require explicit opt-in; no hidden persistence."
        )
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, agent.stopping.set)
        task = asyncio.create_task(agent.run())
        await agent.stopping.wait()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(launch())


if __name__ == "__main__":
    main()
