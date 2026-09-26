import asyncio
import time

import pytest

from agency import ActionType, AgencyPolicy
from agent_config import AgentConfig
from environment_state import EnvironmentMonitor
from heartbeat import HeartbeatCoordinator
from internal_state import willingness_context
from self_model import SelfModelStore


def run(coro):
    return asyncio.run(coro)


def setup(tmp_path, **values):
    config = AgentConfig(**values)
    store = SelfModelStore(str(tmp_path / "agent.db"), config)
    run(store.init())
    return store, HeartbeatCoordinator(store, EnvironmentMonitor(config=config), config)


def test_action_allowlist_and_permissions():
    policy = AgencyPolicy()
    assert policy.validate({"action": "NO_ACTION", "reason": "quiet"}).action is ActionType.NO_ACTION
    with pytest.raises(ValueError):
        policy.validate({"action": "RUN_CODE", "reason": "no"})
    with pytest.raises(PermissionError):
        policy.validate({"action": "RESPOND", "reason": "test"}, permission_allowed=False)
    with pytest.raises(PermissionError):
        policy.validate({"action": "SEND_PROACTIVE_MESSAGE", "reason": "test", "user_id": 1, "channel_id": 2}, muted=True)


def test_willingness_never_changes_permission():
    resistant = willingness_context("do it", repeated_count=5, permission_allowed=True)
    forbidden = willingness_context("please", repeated_count=0, permission_allowed=False, trust=100)
    assert resistant["level"] == "resistant" and resistant["permission_allowed"] is True
    assert forbidden["level"] == "forbidden" and forbidden["permission_allowed"] is False


def test_routine_heartbeat_does_not_call_llm(tmp_path):
    store, heartbeat = setup(tmp_path)
    calls = []

    async def generator(prompt):
        calls.append(prompt)
        return "reflection"

    result = run(heartbeat.tick(reflection_generator=generator, proactive_candidates=[]))
    assert result.action.action is ActionType.NO_ACTION
    assert calls == []


def test_meaningful_event_can_reflect_and_budget_limits(tmp_path):
    store, heartbeat = setup(tmp_path, reflection_threshold=7, autonomous_calls_per_hour=1, autonomous_calls_per_day=1)
    run(store.record_event("conflict", "A conflict mattered.", importance=9, related_user_id=7))
    calls = []

    async def generator(prompt):
        calls.append(prompt)
        return "I sharpened the answer because admitting concern felt worse."

    result = run(heartbeat.tick(reflection_generator=generator, proactive_candidates=[]))
    assert result.reflected is True and len(calls) == 1
    assert run(heartbeat.reserve_autonomous_call()) is False


def test_proactive_cooldown_and_opt_out(tmp_path):
    store, heartbeat = setup(tmp_path, proactive_cooldown_seconds=3600, proactive_user_cooldown_seconds=3600)
    candidate = {"user_id": 3, "channel_id": 4, "reason": "unfinished conversation",
                 "relationship_significance": 80, "proactive": True, "channel_sendable": True}
    first = run(heartbeat.tick(proactive_candidates=[candidate]))
    assert first.action.action is ActionType.SEND_PROACTIVE_MESSAGE
    run(store.record_action("SEND_PROACTIVE_MESSAGE", "completed", "sent", related_user_id=3, channel_id=4))
    second = run(heartbeat.tick(proactive_candidates=[candidate], now=time.time() + 1))
    assert second.action.action is ActionType.NO_ACTION
    opted_out = dict(candidate, user_id=5, channel_id=6, proactive=False)
    third = run(heartbeat.tick(proactive_candidates=[opted_out], now=time.time() + 2))
    assert third.action.action is ActionType.NO_ACTION


def test_environment_state_is_sanitized_and_provider_degrades(tmp_path):
    store, heartbeat = setup(tmp_path)
    heartbeat.environment.record_provider_failure()
    heartbeat.environment.record_provider_failure()
    result = run(heartbeat.tick(proactive_candidates=[]))
    snapshot = result.environment.sanitized()
    assert snapshot["provider_status"] == "degraded"
    rendered = result.environment.prompt_fragment().lower()
    assert "api_key" not in rendered and "/users/" not in rendered and "hostname" not in rendered
