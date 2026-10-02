from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys


def test_memory_data_dir_controls_local_and_shared_database_paths(tmp_path):
    env = dict(os.environ)
    env["MEMORY_DATA_DIR"] = str(tmp_path)
    env.pop("BOT_DATA_DIR", None)
    script = (
        "import json; from memory import Memory; "
        "m=Memory('scaramouche'); "
        "print(json.dumps({'local': m.db_path, 'shared': m.shared_db_path}))"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    paths = json.loads(result.stdout.strip())
    assert paths == {
        "local": str(tmp_path / "scaramouche.db"),
        "shared": str(tmp_path / "shared_state.db"),
    }
