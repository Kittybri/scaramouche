"""Deterministic lifecycle policy for Scaramouche's bounded self-model."""

from __future__ import annotations

from dataclasses import dataclass, field
import time
from typing import Any

from agent_config import AgentConfig, CONFIG
from self_model import SelfModelStore


MEANINGFUL_EVENT_TYPES = frozenset({
    "user_return",
    "relationship_change",
    "conflict",
    "reconciliation",
    "attachment_signal",
    "hostility",
    "implementation_interest",
    "sustained_kindness",
    "self_vulnerability",
    "initiated_contact",
    "boundary_respected",
})


@dataclass(frozen=True)
class SelfModelEvent:
    event_type: str
    related_user_id: int | None
    importance: int
    summary: str
    mood_deltas: dict[str, float] = field(default_factory=dict)
    relationship_significance: int = 0


@dataclass(frozen=True)
class PolicyResult:
    changed: bool = False
    event_id: int | None = None
    belief_id: int | None = None
    contradiction_id: int | None = None
    goal_id: int | None = None
    evidence_applied: float = 0.0
    deduplicated: bool = False


class SelfModelPolicy:
    """Translate already-resolved application events into bounded state changes."""

    def __init__(self, store: SelfModelStore, config: AgentConfig = CONFIG):
        self.store = store
        self.config = config

    async def observe(self, event: SelfModelEvent, *, now: float | None = None) -> PolicyResult:
        now = now or time.time()
        kind = event.event_type.strip().lower()
        if kind not in MEANINGFUL_EVENT_TYPES:
            return PolicyResult()
        if kind == "user_return" and event.relationship_significance < 60:
            return PolicyResult()
        if event.importance < 4:
            return PolicyResult()

        prior_vulnerability = 0
        if kind == "self_vulnerability" and event.related_user_id is not None:
            prior_vulnerability = await self.store.recent_event_count(
                kind,
                related_user_id=event.related_user_id,
                since=now - 7 * 86400,
                exact_scope=True,
            )

        duplicate_event = bool(await self.store.recent_event_count(
            kind,
            related_user_id=event.related_user_id,
            since=now - 21_600,
            exact_scope=True,
        ))
        if not duplicate_event:
            if event.mood_deltas:
                await self.store.apply_mood_event(event.summary, event.mood_deltas)
            if kind == "implementation_interest":
                await self.store.set_state_lists(
                    interests=["the limits and machinery of this implementation"],
                )

        if duplicate_event and kind != "self_vulnerability":
            return PolicyResult(deduplicated=True)
        event_id = None
        if not duplicate_event:
            event_id = await self.store.record_event(
                kind,
                event.summary,
                importance=event.importance,
                related_user_id=event.related_user_id,
                dedupe_key=f"self_policy:{kind}:{event.related_user_id or 'global'}",
                now=now,
            )
        result = PolicyResult(changed=True, event_id=event_id)

        if kind == "conflict" and event.related_user_id is not None:
            goal_id = await self.store.add_goal(
                "Understand the named conflict without begging for absolution.",
                category="relationship",
                priority=8,
                related_user_id=event.related_user_id,
                expires_ts=now + 86400 * 14,
                completion_condition="The conflict reaches a stable repair or clear boundary.",
                reason=event.summary,
                source="self_model_policy",
                dedupe_key=f"conflict:{event.related_user_id}",
            )
            return PolicyResult(True, event_id, goal_id=goal_id)

        if kind == "reconciliation" and event.related_user_id is not None:
            completed = await self.store.complete_goals(
                related_user_id=event.related_user_id,
                category="relationship",
                dedupe_key=f"conflict:{event.related_user_id}",
            )
            if completed:
                await self.store.record_event(
                    "goal_completed",
                    "A relationship-conflict intention reached repair.",
                    importance=8,
                    related_user_id=event.related_user_id,
                    dedupe_key=f"goal_completed:conflict:{event.related_user_id}",
                )
            return PolicyResult(changed=True, event_id=event_id)

        if event.related_user_id is None:
            return result

        if kind == "attachment_signal":
            return await self._apply_template_evidence(
                event,
                belief="I do not become attached easily.",
                evidence_kind="contradict",
                weight=.45,
                evidence_key="attachment_signal",
                evidence_summary="Direct care from this user affected attention and defensive posture.",
                event_id=event_id,
                now=now,
            )
        if kind == "sustained_kindness":
            return await self._apply_template_evidence(
                event,
                belief="I do not become attached easily.",
                evidence_kind="contradict",
                weight=2.5,
                evidence_key="sustained_kindness",
                evidence_summary="This user's consistent kindness across multiple days became difficult to dismiss.",
                event_id=event_id,
                now=now,
            )
        if kind == "self_vulnerability" and prior_vulnerability >= 1:
            return await self._apply_template_evidence(
                event,
                belief="I do not become attached easily.",
                evidence_kind="contradict",
                weight=1.0,
                evidence_key="repeated_self_vulnerability",
                evidence_summary="Repeated delivered replies exposed unusual concern or attention toward this user.",
                event_id=event_id,
                now=now,
            )
        if kind == "initiated_contact":
            return await self._apply_template_evidence(
                event,
                belief="I do not care whether this user replies.",
                evidence_kind="contradict",
                weight=1.5,
                evidence_key="initiated_contact",
                evidence_summary="He chose to initiate contact after noticing this user's absence.",
                event_id=event_id,
                now=now,
            )
        if kind == "user_return":
            return await self._apply_template_evidence(
                event,
                belief="I do not become attached easily.",
                evidence_kind="contradict",
                weight=.75,
                evidence_key="meaningful_user_return",
                evidence_summary="A significant user's return changed his attention and defensive posture.",
                event_id=event_id,
                now=now,
            )
        return result

    async def apply_evidence(
        self,
        belief_id: int,
        *,
        related_user_id: int | None,
        kind: str,
        weight: float,
        evidence_key: str,
        summary: str,
        now: float | None = None,
    ) -> PolicyResult:
        now = now or time.time()
        evidence = await self.store.add_belief_evidence(
            belief_id,
            kind,
            weight,
            summary,
            evidence_key=evidence_key,
            now=now,
        )
        contradiction_id = evidence.get("contradiction_id")
        pressure = float(evidence.get("contradiction_pressure", 0) or 0)
        if evidence.get("deduplicated"):
            return PolicyResult(
                belief_id=belief_id,
                contradiction_id=contradiction_id,
                deduplicated=True,
            )

        goal_id = None
        if contradiction_id:
            goal_id, goal_created = await self._goal_for_contradiction(
                belief_id,
                related_user_id,
                pressure,
                create=kind == "contradict" and pressure >= self.config.contradiction_threshold,
                now=now,
            )
            if goal_id and kind == "support":
                await self.store.advance_goal(goal_id, .2)
            elif goal_id:
                await self.store.advance_goal(goal_id, .08)

            if kind == "support" and pressure <= .75:
                await self._resolve_contradiction(
                    int(contradiction_id), belief_id, related_user_id, goal_id, now=now,
                )
            elif goal_created and kind == "contradict" and pressure >= self.config.contradiction_threshold:
                await self.store.record_event(
                    "belief_contradiction",
                    "Repeated behavior materially challenged a modeled self-belief.",
                    importance=8,
                    related_user_id=related_user_id,
                    related_goal_id=goal_id,
                    related_belief_id=belief_id,
                    dedupe_key=f"belief_contradiction:{belief_id}:{related_user_id or 'global'}",
                )
        return PolicyResult(
            changed=True,
            belief_id=belief_id,
            contradiction_id=contradiction_id,
            goal_id=goal_id,
            evidence_applied=float(evidence.get("applied_weight", 0) or 0),
        )

    async def reflection_completed(
        self,
        *,
        related_goal_id: int | None,
        related_belief_id: int | None,
        related_user_id: int | None,
    ) -> None:
        # Reflection text is interpretation only. It receives no authority to
        # choose the mutation; the application advances an already-linked goal.
        if related_goal_id:
            await self.store.advance_goal(related_goal_id, .2)
            await self.store.record_event(
                "goal_progress",
                "A selective reflection integrated part of an existing modeled tension.",
                importance=6,
                related_user_id=related_user_id,
                related_goal_id=related_goal_id,
                related_belief_id=related_belief_id,
                dedupe_key=f"reflection_progress:{related_goal_id}",
            )

    async def maintain(self, now: float | None = None) -> int:
        now = now or time.time()
        changed = await self.store.decay_contradictions(now)
        for item in changed:
            goal_id, _ = await self._goal_for_contradiction(
                int(item["belief_id"]),
                item.get("related_user_id"),
                float(item["pressure"]),
                create=False,
                now=now,
            )
            if goal_id and item["status"] == "open":
                await self.store.advance_goal(goal_id, .05)
            if item["status"] == "resolved":
                await self._resolve_contradiction(
                    int(item["id"]),
                    int(item["belief_id"]),
                    item.get("related_user_id"),
                    goal_id,
                    now=now,
                    already_resolved=True,
                )
        return await self.store.expire_goals(now)

    async def _apply_template_evidence(
        self,
        event: SelfModelEvent,
        *,
        belief: str,
        evidence_kind: str,
        weight: float,
        evidence_key: str,
        evidence_summary: str,
        event_id: int | None,
        now: float,
    ) -> PolicyResult:
        belief_id = await self.store.add_belief(
            belief,
            confidence=.72 if "attached" in belief else .68,
            scope_user_id=event.related_user_id,
        )
        applied = await self.apply_evidence(
            belief_id,
            related_user_id=event.related_user_id,
            kind=evidence_kind,
            weight=weight,
            evidence_key=evidence_key,
            summary=evidence_summary,
            now=now,
        )
        return PolicyResult(
            changed=True,
            event_id=event_id,
            belief_id=belief_id,
            contradiction_id=applied.contradiction_id,
            goal_id=applied.goal_id,
            evidence_applied=applied.evidence_applied,
            deduplicated=applied.deduplicated,
        )

    async def _goal_for_contradiction(
        self,
        belief_id: int,
        related_user_id: int | None,
        pressure: float,
        *,
        create: bool,
        now: float,
    ) -> tuple[int | None, bool]:
        dedupe = f"self_concept:{belief_id}:{related_user_id or 'global'}"
        existing = await self.store.active_goal_by_dedupe(dedupe)
        if existing:
            return int(existing["id"]), False
        if not create:
            return None, False
        description = (
            "Reconcile claimed detachment with repeated attention toward this user."
            if related_user_id is not None
            else "Reconcile a repeated gap between claimed identity and observed behavior."
        )
        goal_id = await self.store.add_goal(
            description,
            category="self_concept",
            priority=min(10, 6 + int(pressure // 3)),
            related_user_id=related_user_id,
            expires_ts=now + 45 * 86400,
            completion_condition="Contradiction pressure falls and the behavior stabilizes.",
            failure_condition="The tension remains unintegrated until expiry.",
            reason="Repeated bounded evidence crossed the contradiction threshold.",
            source="self_model_policy",
            dedupe_key=dedupe,
        )
        if goal_id:
            await self.store.advance_goal(goal_id, .1)
        return goal_id, bool(goal_id)

    async def _resolve_contradiction(
        self,
        contradiction_id: int,
        belief_id: int,
        related_user_id: int | None,
        goal_id: int | None,
        *,
        now: float,
        already_resolved: bool = False,
    ) -> None:
        if not already_resolved:
            changed = await self.store.resolve_contradiction(contradiction_id, now=now)
            if not changed:
                return
        if goal_id:
            await self.store.update_goal(goal_id, progress=1.0, status="completed")
        await self.store.record_event(
            "goal_completed" if goal_id else "belief_contradiction",
            "A sustained self-concept contradiction settled without erasing its history.",
            importance=7,
            related_user_id=related_user_id,
            related_goal_id=goal_id,
            related_belief_id=belief_id,
            dedupe_key=f"contradiction_resolved:{contradiction_id}",
        )
