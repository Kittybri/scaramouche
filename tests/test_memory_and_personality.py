import asyncio
import sqlite3

from anti_repeat import build_prompt_guard, repeated_shape, response_shape
from character_identity import IMPLEMENTATION_AWARENESS, attachment_guard, implementation_answer_hint
from memory import Memory


def run(coro):
    return asyncio.run(coro)


def test_memory_paths_are_instance_local_and_existing_schema_survives(tmp_path):
    old_path = tmp_path / "old.db"
    with sqlite3.connect(old_path) as db:
        db.execute("CREATE TABLE users(user_id INTEGER PRIMARY KEY, username TEXT, display_name TEXT)")
        db.execute("INSERT INTO users(user_id,username,display_name) VALUES(1,'old','Old User')")
    first = Memory("one", db_path=str(old_path), shared_db_path=str(tmp_path / "shared1.db"))
    second = Memory("two", db_path=str(tmp_path / "two.db"), shared_db_path=str(tmp_path / "shared2.db"))
    run(first.init())
    run(second.init())
    run(second.upsert_user(2, "new", "New User"))
    assert run(first.get_user(1))["display_name"] == "Old User"
    assert run(first.get_user(2)) is None
    assert run(second.get_user(2))["display_name"] == "New User"


def test_memory_backup(tmp_path):
    memory = Memory("test", db_path=str(tmp_path / "memory.db"), shared_db_path=str(tmp_path / "shared.db"))
    run(memory.init())
    destination = run(memory.backup(str(tmp_path / "memory-copy.db")))
    with sqlite3.connect(destination) as db:
        assert db.execute("PRAGMA quick_check").fetchone()[0] == "ok"


def test_repeated_opening_and_structural_pattern_detected():
    recent = ["Fine. That was predictable.", "Right. That was obvious.", "Enough. That was tedious."]
    assert response_shape(recent[0])["sentence_count"] == 2
    assert repeated_shape("Good. That was expected.", recent)
    assert "structural shape" in build_prompt_guard("scaramouche", recent)


def test_character_prompt_invariants():
    assert "persistent character identity" in IMPLEMENTATION_AWARENESS
    assert "do not prove consciousness" in IMPLEMENTATION_AWARENESS
    assert implementation_answer_hint("How does your Python code work?")
    assert implementation_answer_hint("What are you eating?") == ""
    high_attachment = attachment_guard(8, 90)
    assert "Do not become a generic sweet assistant" in high_attachment
    assert "pride remains" in high_attachment
