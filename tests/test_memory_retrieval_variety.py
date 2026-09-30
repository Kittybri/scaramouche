import asyncio
import random
import sqlite3
import time

from memory import Memory
from memory_retrieval import MemoryCandidate, MemoryRetriever, candidate_fragment


NOW = 2_000_000_000.0


def run(coro):
    return asyncio.run(coro)


def candidate(text, *, kind="manual", weight=5, last_used=0, source="memory_bank", record_id=None):
    return MemoryCandidate(
        source, kind, text, weight, NOW - 86400, last_used, record_id,
    )


def retriever(*, maximum=1, seed=4):
    return MemoryRetriever(
        max_fragments=maximum, rng=random.Random(seed), wall_clock=lambda: NOW,
    )


def test_relevant_memory_beats_higher_weight_irrelevant_memory():
    result = retriever().retrieve([
        candidate("User loves ramen.", weight=10),
        candidate("User was anxious before their job interview.", weight=7),
    ], "My interview is tomorrow.", user_id=1)
    assert [item.text for item in result.selected] == [
        "User was anxious before their job interview."
    ]


def test_kind_specific_intent_prefers_promise_conflict_and_preference():
    choices = [
        candidate("I would return after the trip.", kind="promise", weight=3),
        candidate("The disagreement ended badly.", kind="fight", weight=3),
        candidate("They prefer green tea.", kind="preference", weight=3),
    ]
    assert retriever().retrieve(choices, "Did I ever promise you something?", user_id=1).selected[0].kind == "promise"
    choices = [candidate(item.text, kind=item.kind, weight=item.weight) for item in choices]
    assert retriever().retrieve(choices, "Why were you mad after our conflict?", user_id=1).selected[0].kind == "fight"
    choices = [candidate(item.text, kind=item.kind, weight=item.weight) for item in choices]
    assert retriever().retrieve(choices, "What is my favorite preference?", user_id=1).selected[0].kind == "preference"


def test_recent_use_penalty_selects_other_similarly_relevant_memory():
    result = retriever().retrieve([
        candidate("The interview made them nervous on Monday.", weight=7, last_used=NOW - 60),
        candidate("They practiced interview answers while nervous.", weight=7),
    ], "I am nervous about the interview.", user_id=1)
    assert result.selected[0].text.startswith("They practiced")
    assert result.suppressed_recent_count == 1


def test_relevance_floor_returns_no_forced_nostalgia():
    result = retriever().retrieve([
        candidate("User likes pizza.", weight=10),
        candidate("A rainy day in Mondstadt.", weight=8),
    ], "Can you explain this Python error?", user_id=1)
    assert result.selected == []


def test_generic_remember_and_open_conflict_do_not_force_unrelated_memory():
    result = retriever(maximum=2).retrieve([
        candidate("A completely unrelated milestone about pizza.", kind="milestone", weight=8, source="milestone"),
        candidate("An argument about a broken vase.", kind="conflict", weight=9, source="conflict"),
        candidate("A callback about buying new shoes.", kind="callback", weight=7, source="callback"),
    ], "Do you remember my interview?", user_id=1, conflict_open=True)
    assert result.selected == []


def test_serious_context_suppresses_inside_joke():
    result = retriever().retrieve([
        candidate("We joked about being nervous.", kind="inside_joke", weight=8, source="inside_joke"),
        candidate("They were nervous and needed comfort.", kind="comfort", weight=5),
    ], "I am nervous and need help.", user_id=1, serious=True)
    assert result.selected[0].kind == "comfort"
    assert all(item.kind != "inside_joke" for item in result.selected)


def test_budget_and_duplicate_event_arbitration():
    result = retriever(maximum=2).retrieve([
        candidate("They were nervous before the interview.", source="callback", kind="callback", weight=7),
        candidate("User was nervous before their interview.", source="memory_bank", kind="vulnerability", weight=8),
        candidate("Their interview anxiety mattered.", source="continuity", kind="continuity", weight=6),
        candidate("They promised to report how the interview went.", kind="promise", weight=6),
    ], "Remember my nervous interview and my promise?", user_id=1)
    assert len(result.selected) <= 2
    rendered = " ".join(candidate_fragment(item) for item in result.selected)
    assert rendered.lower().count("nervous") <= 1
    assert result.suppressed_duplicate_count >= 1


def test_runtime_recall_penalty_varies_same_relevant_query():
    selector = retriever(maximum=1)
    candidates = [
        candidate("First interview memory.", weight=7),
        candidate("Second interview memory.", weight=7),
    ]
    first = selector.retrieve(candidates, "interview memory", user_id=8).selected[0]
    selector.mark_selected(8, [first])
    fresh_candidates = [
        candidate("First interview memory.", weight=7),
        candidate("Second interview memory.", weight=7),
    ]
    second = selector.retrieve(fresh_candidates, "interview memory", user_id=8).selected[0]
    assert second.text != first.text
    selector.forget_user(8)
    assert len(selector._recent) == 0


def test_snapshot_and_last_used_are_user_and_channel_scoped(tmp_path):
    memory = Memory("test", str(tmp_path / "local.db"), str(tmp_path / "shared.db"))
    run(memory.init())
    now = time.time()
    with sqlite3.connect(memory.db_path) as db:
        db.executemany(
            "INSERT INTO memory_bank(user_id,kind,memory,weight,last_used,ts) VALUES(?,?,?,?,0,?)",
            [
                (1, "vulnerability", "Alpha interview nerves", 7, now),
                (1, "manual", "Alpha ramen", 10, now),
                (2, "manual", "Bravo private memory", 10, now),
            ],
        )
        db.executemany(
            "INSERT INTO messages(user_id,channel_id,role,content,ts,bot_name) VALUES(?,?,?,?,?,?)",
            [
                (1, 10, "user", "Alpha same-channel history", now - 3 * 86400, "test"),
                (1, 11, "user", "Alpha other-channel private history", now - 3 * 86400, "test"),
                (2, 10, "user", "Bravo same-channel private history", now - 3 * 86400, "test"),
            ],
        )
    snapshot = run(memory.get_memory_retrieval_snapshot(1, 10, milestone_scope="test:user:1"))
    all_text = " ".join(
        item.get("text", "") for items in snapshot.values() for item in items
    )
    assert "Alpha same-channel history" in all_text
    assert "other-channel private" not in all_text
    assert "Bravo" not in all_text

    rows = snapshot["memory_bank"]
    selected = retriever().retrieve([
        MemoryCandidate("memory_bank", row["kind"], row["text"], row["weight"], row["ts"], row["last_used"], row["id"])
        for row in rows
    ], "My interview makes me nervous", user_id=1).selected
    run(memory.mark_memory_events_used(1, [selected[0].record_id]))
    with sqlite3.connect(memory.db_path) as db:
        states = dict(db.execute("SELECT memory,last_used FROM memory_bank WHERE user_id=1"))
        other_user = db.execute(
            "SELECT last_used FROM memory_bank WHERE user_id=2"
        ).fetchone()[0]
    assert states[selected[0].text] > 0
    untouched = "Alpha ramen" if selected[0].text != "Alpha ramen" else "Alpha interview nerves"
    assert states[untouched] == 0
    assert other_user == 0
