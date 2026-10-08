"""Deterministic cross-repository release checks; never contacts live services."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time


_WORKER = r'''
import asyncio, json, os, sqlite3, sys, time
repo, bot_name, data_dir, operations = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
sys.path.insert(0, repo)
os.chdir(data_dir)
os.environ["MEMORY_DATA_DIR"] = data_dir
import memory as memory_module

local_path = os.path.join(data_dir, bot_name + ".db")
shared_path = os.path.join(data_dir, "shared_state.db")
try:
    mem = memory_module.Memory(bot_name, db_path=local_path, shared_db_path=shared_path)
except TypeError:
    mem = memory_module.Memory(bot_name)
    mem.db_path = local_path
    mem.shared_db_path = shared_path
    memory_module.DB_PATH = local_path
    memory_module.SHARED_DB_PATH = shared_path

async def main():
    errors = []
    locked = 0
    started = time.monotonic()
    try:
        await mem.init()
    except Exception as exc:
        print(json.dumps({"bot": bot_name, "fatal": type(exc).__name__, "detail": str(exc)[:120]}))
        return 2
    for index in range(operations):
        user_id = 1000 + (index % 12)
        channel_id = 5000 + (index % 24)
        try:
            await mem.upsert_user(user_id, f"user{user_id}", f"User {user_id}")
            await mem.add_message(user_id, channel_id, "user", f"{bot_name}:{index}")
            await mem.record_bot_banter(
                "scaramouche:wanderer", bot_name, f"line:{bot_name}:{index}", "audit"
            )
            await mem.set_duo_session(
                channel_id, "both", f"audit-{index}", bot_name,
                initiator_user_id=user_id, awaiting_bot=("wanderer" if bot_name == "scaramouche" else "scaramouche"),
                autoplay_turns=1, ttl_seconds=120,
            )
            await mem.consume_shared_cooldown(f"audit:{bot_name}:{index}", 0)
        except Exception as exc:
            name = type(exc).__name__
            detail = str(exc).lower()
            locked += int("locked" in detail)
            errors.append(name)
    pending_tasks = max(0, len(asyncio.all_tasks()) - 1)
    print(json.dumps({
        "bot": bot_name, "operations": operations, "errors": errors,
        "locked": locked, "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
        "pending_tasks": pending_tasks,
    }))
    return 0 if not errors else 1

raise SystemExit(asyncio.run(main()))
'''


def _last_json(text: str) -> dict:
    for line in reversed((text or "").splitlines()):
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    return {"fatal": "missing_worker_result", "output": (text or "")[-500:]}


def _database_metrics(path: Path) -> dict:
    with sqlite3.connect(path, timeout=30) as db:
        quick = db.execute("PRAGMA quick_check").fetchone()[0]
        tables = {
            row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        counts = {}
        for table in (
            "shared_users", "bot_banter_log", "duo_sessions",
            "shared_cooldowns", "schema_migrations",
        ):
            if table in tables:
                counts[table] = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        duo_columns = []
        if "duo_sessions" in tables:
            duo_columns = [row[1] for row in db.execute("PRAGMA table_info(duo_sessions)")]
    return {
        "path": str(path), "bytes": path.stat().st_size,
        "quick_check": quick, "counts": counts, "duo_columns": duo_columns,
    }


def shared_sqlite_stress(scara: Path, wanderer: Path, *, operations: int) -> dict:
    with tempfile.TemporaryDirectory(prefix="bot-release-audit-") as raw:
        data = Path(raw)
        processes = []
        started = time.monotonic()
        for repo, bot_name in ((scara, "scaramouche"), (wanderer, "wanderer")):
            processes.append(subprocess.Popen(
                [sys.executable, "-c", _WORKER, str(repo), bot_name, str(data), str(operations)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            ))
        workers = []
        for process in processes:
            stdout, stderr = process.communicate(timeout=180)
            result = _last_json(stdout)
            result.update(returncode=process.returncode, stderr=stderr[-1000:])
            workers.append(result)
        database_paths = sorted(data.glob("*.db"))
        databases = [_database_metrics(path) for path in database_paths]
        required_duo = {
            "channel_id", "mode", "initiator_bot", "initiator_user_id",
            "awaiting_bot", "autoplay_remaining", "expires_ts",
        }
        shared = next(item for item in databases if item["path"].endswith("shared_state.db"))
        passed = (
            all(item.get("returncode") == 0 and not item.get("errors") for item in workers)
            and all(item["quick_check"] == "ok" for item in databases)
            and required_duo.issubset(shared["duo_columns"])
        )
        return {
            "passed": passed,
            "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
            "workers": workers,
            "databases": databases,
            "total_operations": operations * 2,
        }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--wanderer", required=True, type=Path)
    parser.add_argument("--operations", type=int, default=250)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = shared_sqlite_stress(
        Path(__file__).resolve().parent,
        args.wanderer.resolve(),
        operations=max(1, min(args.operations, 5000)),
    )
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.write_text(rendered + "\n")
    print(rendered)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
