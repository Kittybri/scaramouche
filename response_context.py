"""Typed, deterministic state used while producing one character response.

This module deliberately contains no provider calls or persistence writes.  It
keeps persisted relationship facts separate from their response-time
interpretation so prompt construction and post-response learning can reuse the
same calculations.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable
import time
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from interaction_policy import InteractionContext, ResolvedCharacterState
from memory_retrieval import MemoryRetrievalResult
from anti_repeat import PatternScopeSamples
from relationship_engine import (
    compute_emotional_arc,
    describe_relationship_progression,
    detect_emotional_triggers,
    detect_scenario,
    extract_memory_events,
    infer_scene_update,
)


@dataclass(frozen=True)
class ResponseRequest:
    user_id: int
    channel_id: int
    user_message: str
    display_name: str
    author_mention: str
    use_search: bool = False
    extra_context: str = ""
    is_owner: bool = False
    channel_obj: Any = None
    is_dm: bool = False
    prior_last_active: float | None = None


@dataclass(frozen=True)
class RawRelationshipState:
    """Persisted values, before response-time precedence is applied."""

    mood: int = 0
    affection: int = 0
    trust: int = 0
    drift: int = 0
    summary: str = ""
    style_profile: dict[str, Any] = field(default_factory=dict)
    conflict_open: bool = False
    conflict_summary: str = ""
    callback_memory: str = ""
    repair_count: int = 0

    @classmethod
    def from_user(cls, user: dict | None) -> "RawRelationshipState":
        user = user or {}
        return cls(
            mood=int(user.get("mood", 0) or 0),
            affection=int(user.get("affection", 0) or 0),
            trust=int(user.get("trust", 0) or 0),
            drift=int(user.get("drift_score", 0) or 0),
            summary=str(user.get("memory_summary") or ""),
            style_profile=dict(user.get("style_profile") or {}),
            conflict_open=bool(user.get("conflict_open", False)),
            conflict_summary=str(user.get("conflict_summary") or ""),
            callback_memory=str(user.get("callback_memory") or ""),
            repair_count=int(user.get("repair_count", 0) or 0),
        )


@dataclass(frozen=True)
class TimeContext:
    now: datetime
    days_since_last_seen: float
    prompt: str
    requested_timezone: str
    used_fallback: bool = False


@dataclass(frozen=True)
class InteractionLearning:
    """Message-derived signals calculated once and reused after generation."""

    scenario: str
    triggers: tuple[str, ...]
    positive_score: int
    negative_score: int
    memory_events: tuple[tuple[str, str, int], ...]
    scene_update: dict[str, Any]


@dataclass(frozen=True)
class DerivedResponseState:
    emotional_arc: str
    progression: str
    repeated_message_count: int
    response_length_hint: str
    time: TimeContext
    learning: InteractionLearning


@dataclass
class PromptFragments:
    """Ordered prompt categories from low priority to authoritative context."""

    identity: list[str] = field(default_factory=list)
    raw_state: list[str] = field(default_factory=list)
    derived_state: list[str] = field(default_factory=list)
    behavioral: list[str] = field(default_factory=list)
    memory: list[str] = field(default_factory=list)
    world: list[str] = field(default_factory=list)
    factual: list[str] = field(default_factory=list)

    def ordered(self) -> list[str]:
        return [
            *self.identity,
            *self.raw_state,
            *self.derived_state,
            *self.behavioral,
            *self.memory,
            *self.world,
            *self.factual,
        ]


@dataclass
class ResponseContext:
    """Everything needed to explain and produce one response."""

    request: ResponseRequest
    interaction: InteractionContext
    user: dict[str, Any]
    raw: RawRelationshipState
    derived: DerivedResponseState
    resolved: ResolvedCharacterState
    history: list[dict[str, str]]
    recent_replies: list[str]
    self_dimensions: dict[str, float]
    repeat_patterns: PatternScopeSamples = field(default_factory=PatternScopeSamples)
    self_prompt: str = ""
    fragments: PromptFragments = field(default_factory=PromptFragments)
    partner_prompt: str = ""
    duo_prompt: str = ""
    channel_prompt: str = ""
    environment_prompt: str = ""
    search_sources: str = ""
    memory_retrieval: MemoryRetrievalResult | None = None
    system_prompt: str = ""
    user_prompt: str = ""


@dataclass(frozen=True)
class GeneratedResponse:
    text: str
    context: ResponseContext
    used_search: bool
    search_sources: str
    provider_attempts: int
    fallback_used: bool = False


def select_response_length(serious: bool, depth: str, random_value: float) -> str:
    """Preserve the existing RP-depth distributions with injected randomness."""
    if serious:
        return "Use enough words to answer the current need clearly; do not force brevity."
    if depth == "low":
        return "One sentence."
    if depth == "high":
        if random_value < .3:
            return "2-3 sentences."
        if random_value < .75:
            return "A few sentences."
        return "Longer, dramatic."
    if random_value < .34:
        return "2-5 words only."
    if random_value < .67:
        return "One sentence."
    if random_value < .86:
        return "2-3 sentences."
    if random_value < .95:
        return "A few sentences."
    return "Longer, dramatic."


def resolve_time_context(
    user: dict | None,
    prior_last_active: float | None,
    *,
    clock: Callable[[], float] = time.time,
    now_factory: Callable[..., datetime] = datetime.now,
) -> TimeContext:
    """Build date context, falling back only for malformed timezone values."""
    user = user or {}
    requested = user.get("timezone_name") or "America/Los_Angeles"
    used_fallback = False
    try:
        now = now_factory(ZoneInfo(requested))
    except (ZoneInfoNotFoundError, TypeError, ValueError):
        now = now_factory()
        used_fallback = True
    previous = prior_last_active if prior_last_active is not None else user.get("last_active", 0)
    days_ago = round((clock() - previous) / 86400, 1) if previous else 0
    prompt = f"DATE:{now.strftime('%A %b %d %Y')}|HOUR:{now.hour}|LAST_SEEN:{days_ago}d_ago"
    return TimeContext(now, days_ago, prompt, str(requested), used_fallback)


def sentiment_scores(message: str) -> tuple[int, int]:
    """Return the exact lightweight sentiment scores used by relationship learning."""
    lowered = message.lower()
    positive = sum([
        any(word in lowered for word in [
            "haha", "lol", "lmao", "hehe", "cute", "nice", "cool", "fun",
            "good", "great", "enjoy", "happy", "excited", "interesting",
            "wow", "omg", "yes", "yay", "please", "😂", "😭", "❤", "💜", "🥺",
        ]),
        lowered.endswith("!") and len(message) > 8,
        "?" in message and len(message) > 15,
        len(message) > 100,
    ])
    negative = sum([
        any(word in lowered for word in [
            "ugh", "ew", "boring", "whatever", "idc", "nope", "wrong", "bad",
            "hate", "worst", "terrible", "awful", "seriously", "really", "😒", "🙄",
        ]),
        message.count("...") > 1,
    ])
    return positive, negative


def derive_response_state(
    *,
    bot_name: str,
    message: str,
    display_name: str,
    is_dm: bool,
    serious: bool,
    user: dict | None,
    raw: RawRelationshipState,
    history: list[dict[str, str]],
    prior_last_active: float | None,
    random_value: float,
) -> DerivedResponseState:
    """Calculate all deterministic prompt and learning features exactly once."""
    user = user or {}
    normalized = message.strip().lower()
    repeated_count = sum(
        1 for item in history[-16:]
        if item.get("role") == "user" and item.get("content", "").strip().lower() == normalized
    )
    emotional_arc = compute_emotional_arc(
        raw.affection,
        raw.trust,
        user.get("slow_burn", 0),
        raw.conflict_open,
        raw.repair_count,
    )
    progression = describe_relationship_progression(
        bot_name,
        raw.affection,
        raw.trust,
        romance_mode=bool(user.get("romance_mode")),
        conflict_open=raw.conflict_open,
        slow_burn=user.get("slow_burn", 0),
    )
    positive, negative = sentiment_scores(message)
    learning = InteractionLearning(
        scenario=detect_scenario(message, is_dm=is_dm),
        triggers=tuple(detect_emotional_triggers(message)),
        positive_score=positive,
        negative_score=negative,
        memory_events=tuple(extract_memory_events(message)),
        scene_update=dict(infer_scene_update(message, display_name) or {}),
    )
    return DerivedResponseState(
        emotional_arc=emotional_arc,
        progression=progression,
        repeated_message_count=repeated_count,
        response_length_hint=select_response_length(
            serious, user.get("rp_depth", "medium"), random_value,
        ),
        time=resolve_time_context(user, prior_last_active),
        learning=learning,
    )
