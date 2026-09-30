"""Selective metacognitive reflection planning."""

from __future__ import annotations

from dataclasses import dataclass
import time

from agent_config import AgentConfig, CONFIG


@dataclass(frozen=True)
class ReflectionRequest:
    event_ids: tuple[int, ...]
    trigger: str
    observation: str
    importance: int
    related_user_id: int | None
    related_goal_id: int | None = None
    related_belief_id: int | None = None


class ReflectionEngine:
    def __init__(self, config: AgentConfig = CONFIG):
        self.config = config
        self._last_requested_at = 0.0

    def select(self, events: list[dict], *, now: float | None = None) -> ReflectionRequest | None:
        """Use cheap heuristics before any LLM call is considered."""
        now = now or time.time()
        meaningful = [e for e in events if int(e.get("importance", 0)) >= self.config.reflection_threshold]
        if not meaningful:
            return None
        meaningful.sort(key=lambda item: (int(item.get("importance", 0)), float(item.get("ts", 0))), reverse=True)
        top = meaningful[0]
        if now - self._last_requested_at < 900:
            return None
        self._last_requested_at = now
        # A persisted reflection belongs to at most one user. Global events may
        # accompany that user's events, but events about another user may not.
        user_id = top.get("related_user_id")
        selected = [
            event for event in meaningful
            if event.get("related_user_id") in ({None, user_id} if user_id is not None else {None})
        ][:4]
        goal_ids = {e.get("related_goal_id") for e in selected if e.get("related_goal_id") is not None}
        belief_ids = {e.get("related_belief_id") for e in selected if e.get("related_belief_id") is not None}
        goal_id = next(iter(goal_ids)) if len(goal_ids) == 1 else None
        belief_id = next(iter(belief_ids)) if len(belief_ids) == 1 else None
        observation = " | ".join(str(e.get("summary", ""))[:240] for e in selected)
        return ReflectionRequest(
            tuple(int(e["id"]) for e in selected), str(top.get("event_type", "event")),
            observation, int(top.get("importance", 1)), int(user_id) if user_id else None,
            int(goal_id) if goal_id else None, int(belief_id) if belief_id else None,
        )

    @staticmethod
    def prompt(request: ReflectionRequest) -> str:
        return (
            "Write a private self-reflection for Scaramouche's persistent self-model. "
            "This is an implementation-generated metacognitive note, not a claim of consciousness.\n"
            f"Trigger: {request.trigger}\nObservation: {request.observation}\n"
            "Return one concise interpretation under 70 words. Focus on why he reacted, any conflict "
            "between behavior and self-image, and what may matter later. No dialogue, no roleplay narration, "
            "no claims of biological sensation."
        )
