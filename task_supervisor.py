"""Small asyncio supervisor for named, restartable bot workers."""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass, field
import logging
import time
from typing import Awaitable, Callable


WorkerFactory = Callable[[], Awaitable[None]]


@dataclass(frozen=True)
class TaskSpec:
    name: str
    factory: WorkerFactory
    critical: bool = False
    restart: bool = True
    base_backoff: float = 5.0
    max_backoff: float = 120.0
    healthy_after: float = 300.0


@dataclass
class TaskHealth:
    name: str
    critical: bool
    state: str = "STOPPED"
    starts: int = 0
    failures: int = 0
    restarts: int = 0
    consecutive_failures: int = 0
    last_started_at: float | None = None
    last_success_at: float | None = None
    last_progress_at: float | None = None
    last_failure_at: float | None = None
    last_error_category: str | None = None
    next_restart_at: float | None = None
    progress: dict[str, object] = field(default_factory=dict)


class TaskSupervisor:
    """Own one runner per named worker and restart unexpected exits safely."""

    def __init__(self, *, logger: logging.Logger | None = None,
                 clock: Callable[[], float] = time.time):
        self.logger = logger or logging.getLogger(__name__)
        self._clock = clock
        self._state = "RUNNING"
        self._specs: dict[str, TaskSpec] = {}
        self._health: dict[str, TaskHealth] = {}
        self.tasks: dict[str, asyncio.Task] = {}

    @property
    def state(self) -> str:
        return self._state

    def register(self, spec: TaskSpec) -> None:
        existing = self._specs.get(spec.name)
        if existing and existing != spec and spec.name in self.tasks and not self.tasks[spec.name].done():
            raise RuntimeError(f"cannot replace running worker: {spec.name}")
        self._specs[spec.name] = spec
        self._health.setdefault(spec.name, TaskHealth(spec.name, spec.critical)).critical = spec.critical

    def ensure_started(self, name: str) -> asyncio.Task:
        if name not in self._specs:
            raise KeyError(f"worker is not registered: {name}")
        current = self.tasks.get(name)
        if current and not current.done():
            return current
        # Explicit startup may resume a previously stopped supervisor. A completed
        # callback never calls this, so shutdown cannot accidentally respawn work.
        if self._state == "STOPPED":
            self._state = "RUNNING"
        if self._state != "RUNNING":
            raise RuntimeError("task supervisor is stopping")
        task = asyncio.create_task(self._run(self._specs[name]), name=f"supervisor:{name}")
        self.tasks[name] = task
        return task

    @staticmethod
    def restart_delay(spec: TaskSpec, consecutive_failures: int) -> float:
        # Cap the exponent before calculating it; a worker that fails for days
        # must not eventually overflow while computing an already-capped delay.
        exponent = min(30, max(0, consecutive_failures - 1))
        return min(
            spec.max_backoff,
            spec.base_backoff * (2 ** exponent),
        )

    async def _run(self, spec: TaskSpec) -> None:
        health = self._health[spec.name]
        try:
            while self._state == "RUNNING":
                health.starts += 1
                if health.starts > 1:
                    health.restarts += 1
                health.state = "RUNNING"
                health.last_started_at = self._clock()
                health.next_restart_at = None
                try:
                    await spec.factory()
                    if self._state != "RUNNING":
                        break
                    raise RuntimeError("worker exited unexpectedly")
                except asyncio.CancelledError:
                    health.state = "CANCELLED" if self._state == "RUNNING" else "STOPPED"
                    raise
                except Exception as exc:
                    now = self._clock()
                    runtime = now - (health.last_started_at or now)
                    if runtime >= spec.healthy_after:
                        health.consecutive_failures = 0
                    health.failures += 1
                    health.consecutive_failures += 1
                    health.last_failure_at = now
                    health.last_error_category = type(exc).__name__
                    self.logger.exception(
                        "supervised worker failed",
                        extra={"task_name": spec.name, "critical": spec.critical,
                               "error_category": type(exc).__name__},
                    )
                    if not spec.restart or self._state != "RUNNING":
                        health.state = "FAILED"
                        return
                    delay = self.restart_delay(spec, health.consecutive_failures)
                    health.state = "BACKING_OFF"
                    health.next_restart_at = now + delay
                    await asyncio.sleep(delay)
        finally:
            if self._state != "RUNNING":
                health.state = "STOPPED"
                health.next_restart_at = None

    def mark_progress(self, name: str, **progress: object) -> None:
        health = self._health.get(name)
        spec = self._specs.get(name)
        if not health or not spec:
            return
        now = self._clock()
        health.last_progress_at = now
        health.last_success_at = now
        health.progress.update(progress)
        if health.state == "DEGRADED":
            health.state = "RUNNING"
            health.consecutive_failures = 0
        if health.last_started_at is not None and now - health.last_started_at >= spec.healthy_after:
            health.consecutive_failures = 0
            health.last_error_category = None

    def mark_iteration_failure(self, name: str, error: BaseException) -> None:
        health = self._health.get(name)
        if not health:
            return
        now = self._clock()
        health.state = "DEGRADED"
        health.failures += 1
        health.consecutive_failures += 1
        health.last_failure_at = now
        health.last_error_category = type(error).__name__

    def health(self, name: str) -> TaskHealth | None:
        return self._health.get(name)

    def snapshot(self) -> list[dict[str, object]]:
        return [asdict(self._health[name]) for name in sorted(self._health)]

    async def stop(self) -> None:
        if self._state == "STOPPED":
            return
        self._state = "STOPPING"
        active = [task for task in self.tasks.values() if not task.done()]
        for task in active:
            task.cancel()
        if active:
            await asyncio.gather(*active, return_exceptions=True)
        for health in self._health.values():
            health.state = "STOPPED"
            health.next_restart_at = None
        self.tasks.clear()
        self._state = "STOPPED"
