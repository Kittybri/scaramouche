from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class IntegrationConfig:
    values: dict[str, Any]

    def __repr__(self) -> str:
        return "IntegrationConfig(<redacted>)"

    def section(self, name: str) -> dict[str, Any]:
        value = self.values.get(name, {})
        return dict(value) if isinstance(value, dict) else {}

    def google_account(self, user_id: int) -> dict[str, Any]:
        accounts = self.section("google").get("accounts", {})
        value = accounts.get(str(int(user_id)), {}) if isinstance(accounts, dict) else {}
        return dict(value) if isinstance(value, dict) else {}


def load_integration_config() -> IntegrationConfig:
    """Load one structured JSON value/file. Never logs configuration contents."""
    raw = (os.getenv("BOT_INTEGRATIONS_JSON") or "").strip()
    path = (os.getenv("BOT_INTEGRATIONS_CONFIG") or "").strip()
    try:
        if path:
            raw = Path(path).expanduser().read_text(encoding="utf-8")
        values = json.loads(raw) if raw else {}
    except (OSError, json.JSONDecodeError):
        values = {}
    return IntegrationConfig(values if isinstance(values, dict) else {})
