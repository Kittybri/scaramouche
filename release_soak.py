"""Accelerated, offline soak for bounded persistence and runtime state."""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sqlite3
import tempfile
import time
import tracemalloc

from anti_repeat import get_runtime_recent, remember_output, _PATTERN_HISTORY
from internal_state import perceive_message
from memory import Memory
from runtime_cache import BoundedTTLCache, BoundedTTLSet
from self_model import SelfModelStore
from self_model_policy import SelfModelEvent, SelfModelPolicy
from world_store import WorldStore


def _count(db, table, where="", params=()):
    return db.execute(f"SELECT COUNT(*) FROM {table} {where}", params).fetchone()[0]


async def soak(turns: int) -> dict:
    task_count_before = len(asyncio.all_tasks())
    with tempfile.TemporaryDirectory(prefix="scara-soak-") as raw:
        root = Path(raw)
        local, shared, model_path = root / "scara.db", root / "shared.db", root / "self.db"
        mem = Memory("scaramouche", str(local), str(shared))
        model = SelfModelStore(str(model_path))
        policy = SelfModelPolicy(model)
        world = WorldStore(shared)
        await mem.init()
        await model.init()
        await world.init()

        cache = BoundedTTLCache(ttl_seconds=3600, max_entries=128)
        seen = BoundedTTLSet(ttl_seconds=3600, max_entries=256)
        tracemalloc.start()
        started = time.monotonic()
        users = 24
        for index in range(turns):
            uid = 10000 + index % users
            channel = 20000 + index % 40
            text = f"ordinary release soak turn {index} for user {uid}"
            await mem.upsert_user(uid, f"u{uid}", f"User {uid}")
            await mem.add_message(uid, channel, "user", text)
            await mem.add_memory_event(uid, "conversation", text, 1)
            cache[index] = {"uid": uid}
            seen.add(index)
            remember_output(
                "scaramouche",
                f"Tch. Release soak reply {index}; answer remains bounded.",
                user_id=uid, channel_id=channel,
            )
            event = perceive_message(text)
            await policy.observe(SelfModelEvent(
                event.event_type, uid, event.importance, event.summary,
                event.mood_deltas, relationship_significance=10,
            ))
            if index < 40:
                await mem.set_duo_session(
                    channel, "both", "release soak", "scaramouche",
                    initiator_user_id=uid, awaiting_bot="wanderer",
                    autoplay_turns=1, ttl_seconds=120,
                )
            if index < 100:
                await world.claim(
                    f"soak:{index}", "soak_once", {"index": index, "user_id": uid}
                )

        for channel in range(20000, 20040):
            await mem.clear_duo_session(channel)
        for index in range(min(100, turns)):
            await world.remove(f"soak:{index}")
        cache.prune()
        seen.prune()
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        with sqlite3.connect(local) as db:
            local_quick = db.execute("PRAGMA quick_check").fetchone()[0]
            memory_rows = _count(db, "memory_bank")
            message_rows = _count(db, "messages")
            pending_deletions = _count(
                db, "privacy_deletion_jobs",
                "WHERE status IN ('PENDING','IN_PROGRESS','RETRYABLE')",
            )
        with sqlite3.connect(shared) as db:
            shared_quick = db.execute("PRAGMA quick_check").fetchone()[0]
            open_duo = _count(db, "duo_sessions")
            world_rows = _count(db, "persistent_world_events")
        with sqlite3.connect(model_path) as db:
            model_quick = db.execute("PRAGMA quick_check").fetchone()[0]
            active_goals = _count(db, "self_goals", "WHERE status='active'")
            open_contradictions = _count(
                db, "self_contradictions", "WHERE status='open'"
            )
        task_count_after = len(asyncio.all_tasks())
        pattern_users = len(_PATTERN_HISTORY.users)
        pattern_channels = len(_PATTERN_HISTORY.channels)
        report = {
            "turns": turns,
            "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
            "task_count_before": task_count_before,
            "task_count_after": task_count_after,
            "cache_entries": len(cache),
            "set_entries": len(seen),
            "runtime_recent": len(get_runtime_recent("scaramouche", 1000)),
            "pattern_user_scopes": pattern_users,
            "pattern_channel_scopes": pattern_channels,
            "message_rows": message_rows,
            "memory_rows": memory_rows,
            "open_duo_sessions": open_duo,
            "world_rows": world_rows,
            "pending_deletions": pending_deletions,
            "active_goals": active_goals,
            "open_contradictions": open_contradictions,
            "sqlite": [local_quick, shared_quick, model_quick],
            "db_bytes": {
                "local": local.stat().st_size,
                "shared": shared.stat().st_size,
                "self_model": model_path.stat().st_size,
            },
            "tracemalloc_current_bytes": current,
            "tracemalloc_peak_bytes": peak,
        }
        report["passed"] = (
            task_count_after == task_count_before
            and len(cache) <= 128 and len(seen) <= 256
            and report["runtime_recent"] <= 80
            and memory_rows <= users * 16
            and not open_duo and not world_rows and not pending_deletions
            and not active_goals and not open_contradictions
            and all(value == "ok" for value in report["sqlite"])
        )
        return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--turns", type=int, default=600)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = asyncio.run(soak(max(1, min(args.turns, 5000))))
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.write_text(rendered + "\n")
    print(rendered)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
