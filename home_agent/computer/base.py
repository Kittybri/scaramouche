"""Platform boundary; unsupported platforms fail closed."""

from dataclasses import dataclass


@dataclass
class Sample:
    bundle: str = ""
    idle_seconds: float = 0
    locked: bool = True
    window: int = 0
    title: str = ""  # Local privacy filter only. Never serialized.


class Platform:
    def sample(self):
        raise RuntimeError("platform_unavailable")

    def capture(self, sample):
        raise RuntimeError("screen_unavailable")

    async def action(self, action, value):
        raise RuntimeError("action_unavailable")

    async def stop(self):
        pass
