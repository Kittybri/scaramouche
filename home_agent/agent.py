"""Outbound WSS relay with durable replay protection and independent local policy."""

from __future__ import annotations
import asyncio
import base64
import json
import logging
import random
import signal
import time
from pathlib import Path
import aiohttp
from home.protocol import (
    Rejected,
    canonical,
    sign,
    verify,
    secure_url,
    validate,
    secret_ok,
)
from home.permissions import registry, authorize
from home.storage import Store
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
        self.config = config
        self.id = config["agent_id"]
        self.secret = config["secret"]
        for field in ("enabled", "mock"):
            if field in config and type(config[field]) is not bool:
                raise Rejected("invalid_configuration_boolean")
        if not secret_ok(self.secret):
            raise Rejected("weak_credential")
        self.url = secure_url(
            config["url"], websocket=True, mock=config.get("mock", False)
        )
        self.devices = registry(config.get("devices", {}))
        self.store = Store(config.get("database", "home-agent.db"))
        self.adapters = adapters or providers(config)
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
        if self.lock.locked():
            raise Rejected("agent_busy")
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
            c = authorize(c, self.devices, enabled)
            d = dict(self.devices[c["device_id"]], _expires_at=c["expires_at"])
            await self.store.reserve(c, d)
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
                result = result if result in {"completed", "submitted"} else "failed"
            except asyncio.CancelledError:
                await self.store.result(c["request_id"], "cancelled")
                raise
            except Exception:
                result = "failed"
            await self.store.result(c["request_id"], result)
            return {"type": "result", "request_id": c["request_id"], "result": result}

    async def health(self, ws):
        while not self.stopping.is_set():
            await ws.send_json(
                {
                    "type": "health",
                    "devices": list(self.devices),
                    "capabilities": sorted({d["type"] for d in self.devices.values()}),
                }
            )
            await self.store.clean()
            await asyncio.sleep(10)

    async def handle(self, ws, data):
        try:
            result = await self.execute(data)
            await ws.send_json(result)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Only echo an authenticated, format-validated request ID, never vendor errors.
            try:
                payload = verify(self.secret, "command:" + self.id, data)
                rid = payload["command"]["request_id"]
                import re

                if re.fullmatch(r"[0-9a-f]{32}", rid):
                    await ws.send_json(
                        {"type": "result", "request_id": rid, "result": "failed"}
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
                            if control.get("type") in {"disable", "enable"}:
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
                            if len(self.tasks) >= 1:
                                continue
                            task = asyncio.create_task(self.handle(ws, data))
                            self.tasks.add(task)
                            task.add_done_callback(self.tasks.discard)
                finally:
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
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text())

    async def launch():
        agent = Agent(config)
        if args.validate:
            print("Home relay configuration valid; credentials redacted.")
            return
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
