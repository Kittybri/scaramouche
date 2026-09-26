import asyncio
import os
import sqlite3
import time

from agent_config import AgentConfig
from self_model import SelfModelStore


def run(coro):
    return asyncio.run(coro)


def make_store(tmp_path, **overrides):
    config = AgentConfig(**overrides) if overrides else AgentConfig()
    store = SelfModelStore(str(tmp_path / "self.db"), config)
    run(store.init())
    return store


def test_state_and_reflections_persist(tmp_path):
    store = make_store(tmp_path)
    run(store.apply_mood_event("conflict", {"irritation": 4, "attachment": 2}))
    reflection_id = run(store.add_reflection("conflict", "I answered sharply.", "The remark landed.", importance=8))
    reopened = SelfModelStore(store.db_path)
    run(reopened.init())
    state = run(reopened.get_state())
    assert state["dimensions"]["irritation"] == 4
    assert run(reopened.recent_reflections(1))[0]["id"] == reflection_id


def test_belief_confidence_and_contradiction_accumulate(tmp_path):
    store = make_store(tmp_path)
    belief_id = run(store.add_belief("I do not care whether they reply.", confidence=.8, scope_user_id=42))
    supported = run(store.add_belief_evidence(belief_id, "support", 2, "He did not follow up."))
    contradicted = run(store.add_belief_evidence(belief_id, "contradict", 4, "He initiated contact."))
    assert supported["confidence"] > .8
    assert contradicted["confidence"] < supported["confidence"]
    contradictions = run(store.list_contradictions(0, user_id=42))
    assert contradictions[0]["pressure"] == 4


def test_goal_progress_expiry_deduplication_and_limit(tmp_path):
    store = make_store(tmp_path, max_active_goals=2)
    first = run(store.add_goal("Revisit the unfinished question.", dedupe_key="question:1", priority=4))
    assert run(store.add_goal("Same", dedupe_key="question:1", priority=5)) == first
    run(store.update_goal(first, progress=.5))
    expiring = run(store.add_goal("Temporary", priority=3, expires_ts=time.time() - 1))
    assert run(store.expire_goals()) == 1
    assert any(goal["id"] == first and goal["progress"] == .5 for goal in run(store.list_goals()))
    with sqlite3.connect(store.db_path) as db:
        assert db.execute("SELECT status FROM self_goals WHERE id=?", (expiring,)).fetchone()[0] == "failed"


def test_mood_bounds_and_decay(tmp_path):
    config = AgentConfig(mood_decay_per_hour=1.0)
    store = SelfModelStore(str(tmp_path / "self.db"), config)
    run(store.init())
    run(store.apply_mood_event("provoked", {"irritation": 99, "curiosity": -99}))
    before = run(store.get_state())["dimensions"]
    assert before["irritation"] == 10
    assert before["curiosity"] == 0
    with sqlite3.connect(store.db_path) as db:
        db.execute("UPDATE self_state SET last_decay_ts=?", (time.time() - 7200,))
        db.commit()
    assert run(store.decay_mood()) is True
    after = run(store.get_state())["dimensions"]
    assert after["irritation"] < before["irritation"]
    assert after["curiosity"] > before["curiosity"]


def test_user_scoped_context_does_not_cross(tmp_path):
    store = make_store(tmp_path)
    run(store.add_goal("Private goal for user 1", related_user_id=1))
    run(store.add_goal("Private goal for user 2", related_user_id=2))
    context = run(store.context(1)).prompt_fragment()
    assert "Private goal for user 1" in context
    assert "Private goal for user 2" not in context


def test_backup_and_delete_user_data(tmp_path):
    store = make_store(tmp_path)
    belief = run(store.add_belief("Scoped", scope_user_id=88))
    run(store.add_belief_evidence(belief, "contradict", 2, "Evidence"))
    run(store.add_goal("Scoped goal", related_user_id=88))
    destination = run(store.backup(str(tmp_path / "copy.db")))
    assert os.path.exists(destination)
    run(store.delete_user_scoped_data(88))
    assert run(store.list_beliefs(user_id=88)) == []
    assert run(store.list_goals(user_id=88)) == []
