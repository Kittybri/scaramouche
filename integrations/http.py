from __future__ import annotations

import asyncio
import aiohttp


class IntegrationError(RuntimeError):
    pass


class IntegrationAuthError(IntegrationError):
    pass


class AsyncJSONClient:
    def __init__(self, timeout_seconds: float = 12.0, *, session_factory=None, sleep=None):
        self.timeout = aiohttp.ClientTimeout(total=max(2.0, min(30.0, timeout_seconds)))
        self.session_factory = session_factory or aiohttp.ClientSession
        self.sleep = sleep or asyncio.sleep

    async def request(self, method: str, url: str, *, headers=None, json=None, data=None, expected=(200, 201)):
        for attempt in range(2):
            try:
                async with self.session_factory(timeout=self.timeout) as session:
                    async with session.request(method, url, headers=headers, json=json, data=data) as response:
                        if response.status in expected:
                            if response.status == 204:
                                return None
                            return await response.json(content_type=None)
                        if response.status in {401, 403}:
                            raise IntegrationAuthError(f"authentication rejected ({response.status})")
                        if response.status == 429 or response.status >= 500:
                            if attempt == 0:
                                retry_after = response.headers.get("Retry-After", "1")
                                try:
                                    delay = max(0.25, min(3.0, float(retry_after)))
                                except ValueError:
                                    delay = 1.0
                                await self.sleep(delay)
                                continue
                        raise IntegrationError(f"service request failed ({response.status})")
            except asyncio.TimeoutError as exc:
                if attempt == 0:
                    continue
                raise IntegrationError("service request timed out") from exc
            except aiohttp.ClientError as exc:
                if attempt == 0:
                    await self.sleep(0.25)
                    continue
                raise IntegrationError("service connection failed") from exc
        raise IntegrationError("service request failed")
