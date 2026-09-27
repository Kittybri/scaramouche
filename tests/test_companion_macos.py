"""Native implementation with fake frameworks/processes; does not invoke macOS APIs."""

import asyncio
import ast
import sys
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock
import pytest
from home_agent.computer.macos import MacOS, SCRIPTS


def test_fixed_scripts_and_data_arguments(monkeypatch):
    async def check():
        proc = NS(wait=AsyncMock(return_value=0), returncode=0)
        launch = AsyncMock(return_value=proc)
        monkeypatch.setattr(asyncio, "create_subprocess_exec", launch)
        mac = MacOS()
        await mac.action("notify", 'a" & dangerous text')
        assert launch.call_args.args == (
            "/usr/bin/osascript",
            "-e",
            SCRIPTS["notify"],
            'a" & dangerous text',
        )
        await mac.action("volume", 9)
        assert launch.call_args.args[-1] == "50"
        await mac.action("lock", None)
        assert launch.call_args.args == ("/usr/bin/osascript", "-e", SCRIPTS["lock"])
        with pytest.raises(RuntimeError):
            await mac.action("shutdown", None)
        assert launch.await_count == 3

    asyncio.run(check())


def test_mac_audio_cleanup_and_stop(monkeypatch):
    async def check():
        sound = Mock()
        sound.isPlaying.return_value = False
        sound.initWithData_.return_value = sound
        monkeypatch.setitem(sys.modules, "AppKit", NS(NSSound=NS(alloc=lambda: sound)))
        monkeypatch.setitem(
            sys.modules,
            "Foundation",
            NS(NSData=NS(dataWithBytes_length_=lambda b, n: b)),
        )
        mac = MacOS()
        await mac.action("play", (b"fake", 0.25))
        sound.setVolume_.assert_called_once_with(0.25)
        sound.stop.assert_called()
        assert mac.sound is None
        sound.play.return_value = False
        with pytest.raises(RuntimeError):
            await mac.action("play", (b"fake", 0.25))
        assert mac.sound is None

    asyncio.run(check())


def test_remote_code_boundary_static():
    root = Path(__file__).parents[1] / "home_agent" / "computer"
    for path in root.glob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if isinstance(node.func, ast.Name):
                assert node.func.id not in {"eval", "exec", "compile"}
            if isinstance(node.func, ast.Attribute):
                assert node.func.attr not in {
                    "system",
                    "popen",
                    "create_subprocess_shell",
                }
                if node.func.attr == "create_subprocess_exec":
                    assert path.name == "macos.py"
                    assert (
                        isinstance(node.args[0], ast.Constant)
                        and node.args[0].value == "/usr/bin/osascript"
                    )
                    assert (
                        isinstance(node.args[2], ast.Subscript)
                        and node.args[2].value.id == "SCRIPTS"
                    )
