"""Central configuration for Scaramouche's persistent self-model.

The defaults deliberately favor silence and deterministic work.  Environment
variables are deployment knobs; none of them grant additional permissions.
"""

from __future__ import annotations

from dataclasses import dataclass
import os


def _int(name: str, default: int, minimum: int = 0) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


def _float(name: str, default: float, minimum: float = 0.0) -> float:
    try:
        return max(minimum, float(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class AgentConfig:
    heartbeat_interval_seconds: int = 300
    reflection_threshold: int = 7
    proactive_cooldown_seconds: int = 21_600
    proactive_user_cooldown_seconds: int = 86_400
    max_active_goals: int = 8
    max_reflections: int = 250
    mood_decay_per_hour: float = 0.35
    contradiction_threshold: float = 6.0
    autonomous_calls_per_hour: int = 2
    autonomous_calls_per_day: int = 8
    absence_threshold_seconds: int = 259_200
    provider_backoff_seconds: int = 1_800
    cpu_warning_percent: float = 90.0
    memory_warning_percent: float = 90.0
    disk_warning_percent: float = 92.0
    response_shape_history: int = 40

    @classmethod
    def from_env(cls) -> "AgentConfig":
        return cls(
            heartbeat_interval_seconds=_int("SELF_HEARTBEAT_SECONDS", 300, 60),
            reflection_threshold=_int("SELF_REFLECTION_THRESHOLD", 7, 1),
            proactive_cooldown_seconds=_int("SELF_PROACTIVE_COOLDOWN_SECONDS", 21_600, 300),
            proactive_user_cooldown_seconds=_int("SELF_PROACTIVE_USER_COOLDOWN_SECONDS", 86_400, 900),
            max_active_goals=_int("SELF_MAX_ACTIVE_GOALS", 8, 1),
            max_reflections=_int("SELF_MAX_REFLECTIONS", 250, 20),
            mood_decay_per_hour=_float("SELF_MOOD_DECAY_PER_HOUR", 0.35),
            contradiction_threshold=_float("SELF_CONTRADICTION_THRESHOLD", 6.0, 1.0),
            autonomous_calls_per_hour=_int("SELF_AUTONOMOUS_CALLS_PER_HOUR", 2, 0),
            autonomous_calls_per_day=_int("SELF_AUTONOMOUS_CALLS_PER_DAY", 8, 0),
            absence_threshold_seconds=_int("SELF_ABSENCE_THRESHOLD_SECONDS", 259_200, 3600),
            provider_backoff_seconds=_int("SELF_PROVIDER_BACKOFF_SECONDS", 1_800, 60),
            cpu_warning_percent=_float("SELF_CPU_WARNING_PERCENT", 90.0, 1.0),
            memory_warning_percent=_float("SELF_MEMORY_WARNING_PERCENT", 90.0, 1.0),
            disk_warning_percent=_float("SELF_DISK_WARNING_PERCENT", 92.0, 1.0),
            response_shape_history=_int("SELF_RESPONSE_SHAPE_HISTORY", 40, 10),
        )


CONFIG = AgentConfig.from_env()
