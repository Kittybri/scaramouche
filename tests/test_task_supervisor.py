import asyncio
import logging

from task_supervisor import TaskSpec, TaskSupervisor


def run(coro):
    return asyncio.run(coro)


def test_normal_start_and_duplicate_start_use_one_runner():
    async def scenario():
        supervisor = TaskSupervisor(logger=logging.getLogger("test.supervisor"))
        entered = asyncio.Event()
        release = asyncio.Event()

        async def worker():
            entered.set()
            await release.wait()

        supervisor.register(TaskSpec("worker", worker, critical=True, base_backoff=.01))
        first = supervisor.ensure_started("worker")
        second = supervisor.ensure_started("worker")
        await entered.wait()
        health = supervisor.health("worker")
        assert first is second
        assert health.state == "RUNNING"
        assert health.starts == 1
        assert len(supervisor.tasks) == 1
        await supervisor.stop()

    run(scenario())


def test_unexpected_failure_restarts_with_bounded_backoff():
    async def scenario():
        supervisor = TaskSupervisor(logger=logging.getLogger("test.supervisor"))
        attempts = 0
        running = asyncio.Event()

        async def worker():
            nonlocal attempts
            attempts += 1
            if attempts <= 5:
                raise RuntimeError("boom")
            running.set()
            await asyncio.Event().wait()

        supervisor.register(TaskSpec(
            "worker", worker, base_backoff=.001, max_backoff=.004, healthy_after=.01,
        ))
        supervisor.ensure_started("worker")
        await asyncio.wait_for(running.wait(), timeout=.2)
        health = supervisor.health("worker")
        assert attempts == 6
        assert health.failures == 5
        assert health.restarts == 5
        assert health.consecutive_failures == 5
        assert health.state == "RUNNING"
        assert len(supervisor.tasks) == 1
        await supervisor.stop()

    run(scenario())


def test_restart_backoff_is_exponential_and_capped():
    spec = TaskSpec("worker", lambda: None, base_backoff=5, max_backoff=120)
    assert [TaskSupervisor.restart_delay(spec, count) for count in (1, 2, 3, 6, 9, 10_000)] == [
        5, 10, 20, 120, 120, 120,
    ]


def test_healthy_progress_resets_failure_streak():
    async def scenario():
        supervisor = TaskSupervisor(logger=logging.getLogger("test.supervisor"))
        attempts = 0
        healthy = asyncio.Event()

        async def worker():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("first start failed")
            await asyncio.sleep(.015)
            supervisor.mark_progress("worker", iteration=1)
            healthy.set()
            await asyncio.Event().wait()

        supervisor.register(TaskSpec(
            "worker", worker, base_backoff=.001, max_backoff=.01, healthy_after=.01,
        ))
        supervisor.ensure_started("worker")
        await asyncio.wait_for(healthy.wait(), timeout=.2)
        health = supervisor.health("worker")
        assert health.consecutive_failures == 0
        assert health.last_success_at is not None
        await supervisor.stop()

    run(scenario())


def test_shutdown_cancellation_is_not_failure_or_restart():
    async def scenario():
        supervisor = TaskSupervisor(logger=logging.getLogger("test.supervisor"))
        entered = asyncio.Event()

        async def worker():
            entered.set()
            await asyncio.Event().wait()

        supervisor.register(TaskSpec("worker", worker, base_backoff=.001))
        supervisor.ensure_started("worker")
        await entered.wait()
        await supervisor.stop()
        health = supervisor.health("worker")
        assert health.state == "STOPPED"
        assert health.failures == 0
        assert health.restarts == 0
        assert supervisor.tasks == {}

    run(scenario())


def test_shutdown_while_backing_off_prevents_respawn():
    async def scenario():
        supervisor = TaskSupervisor(logger=logging.getLogger("test.supervisor"))
        attempts = 0

        async def worker():
            nonlocal attempts
            attempts += 1
            raise RuntimeError("fail immediately")

        supervisor.register(TaskSpec("worker", worker, base_backoff=.05, max_backoff=.05))
        supervisor.ensure_started("worker")
        for _ in range(20):
            if supervisor.health("worker").state == "BACKING_OFF":
                break
            await asyncio.sleep(.001)
        assert supervisor.health("worker").state == "BACKING_OFF"
        await supervisor.stop()
        await asyncio.sleep(.06)
        assert attempts == 1
        assert supervisor.health("worker").state == "STOPPED"

    run(scenario())


def test_iteration_failure_is_visible_and_progress_recovers():
    supervisor = TaskSupervisor(logger=logging.getLogger("test.supervisor"))

    async def worker():
        await asyncio.Event().wait()

    supervisor.register(TaskSpec("worker", worker))
    supervisor.mark_iteration_failure("worker", TimeoutError())
    assert supervisor.health("worker").state == "DEGRADED"
    assert supervisor.health("worker").last_error_category == "TimeoutError"
    supervisor.mark_progress("worker", scan=1)
    assert supervisor.health("worker").state == "RUNNING"
    assert supervisor.health("worker").consecutive_failures == 0
