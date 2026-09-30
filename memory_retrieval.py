"""Cheap, user-scoped hybrid memory retrieval with no provider calls."""
from __future__ import annotations

from dataclasses import dataclass, field
from difflib import SequenceMatcher
import random
import re
import time
from typing import Callable

from runtime_cache import BoundedTTLCache


RELEVANCE_THRESHOLD = 0.38
MAX_MEMORY_FRAGMENTS = 2

_STOP_WORDS = {
    "a", "about", "again", "all", "am", "an", "and", "are", "as", "at",
    "be", "been", "before", "but", "by", "did", "do", "does", "ever", "for",
    "from", "had", "has", "have", "he", "her", "him", "his", "how", "i", "if",
    "in", "is", "it", "its", "me", "my", "of", "on", "or", "our", "she",
    "so", "that", "the", "their", "them", "they", "this", "to", "tomorrow",
    "us", "user", "was", "we", "were", "what", "when", "where", "which", "who", "why",
    "will", "with", "you", "your",
}

_ALIASES = {
    "anxious": "nervous", "anxiety": "nervous", "worried": "nervous",
    "worry": "nervous", "interviews": "interview", "job": "interview",
    "jobs": "interview", "promised": "promise", "promises": "promise",
    "swore": "promise", "swear": "promise", "angry": "conflict",
    "mad": "conflict", "fight": "conflict", "fighting": "conflict",
    "argument": "conflict", "upset": "hurt", "wounded": "hurt",
    "afraid": "scared", "terrified": "scared", "apologized": "repair",
    "apology": "repair", "forgive": "repair", "forgiven": "repair",
    "jokes": "joke", "funny": "joke", "laughed": "joke",
    "favorites": "preference", "favorite": "preference", "likes": "preference",
}

_KIND_INTENTS = {
    "promise": {"promise", "swear", "commit", "owed"},
    "conflict": {"conflict", "hurt", "betray", "abandon", "repair", "sorry"},
    "repair": {"repair", "sorry", "forgive", "apology", "fix"},
    "preference": {"preference", "like", "love", "favorite", "hate"},
    "joke": {"joke", "laugh", "funny", "remember"},
    "comfort": {"comfort", "scared", "nervous", "lonely", "cry", "help"},
    "relationship": {"remember", "relationship", "together", "us", "care"},
}

_KIND_GROUPS = {
    "promise": "promise", "unforgettable": "relationship",
    "fight": "conflict", "betrayal": "conflict", "slight": "conflict",
    "conflict": "conflict", "repair": "repair", "vulnerability": "comfort",
    "comfort": "comfort", "inside_joke": "joke", "joke": "joke",
    "confession": "relationship", "bond": "relationship",
    "milestone": "relationship", "callback": "relationship",
    "preference": "preference", "topic": "preference",
}


def normalize_tokens(text: str) -> set[str]:
    """Normalize enough lexical variation for bounded relevance scoring."""
    words = re.findall(r"[a-z0-9']+", (text or "").lower())
    normalized: set[str] = set()
    for word in words:
        word = word.strip("'")
        if not word or word in _STOP_WORDS:
            continue
        word = _ALIASES.get(word, word)
        if len(word) > 5 and word.endswith("ing"):
            word = word[:-3]
        elif len(word) > 4 and word.endswith("ed"):
            word = word[:-2]
        elif len(word) > 4 and word.endswith("s"):
            word = word[:-1]
        normalized.add(_ALIASES.get(word, word))
    return normalized


@dataclass
class MemoryCandidate:
    source: str
    kind: str
    text: str
    weight: float
    created_at: float
    last_used: float = 0.0
    record_id: int | None = None
    relevance: float = 0.0
    importance: float = 0.0
    recency: float = 0.0
    novelty: float = 0.0
    final_score: float = 0.0

    @property
    def identity(self) -> str:
        compact = " ".join(sorted(normalize_tokens(self.text)))[:180]
        # Source-independent so the same event cannot bypass cooldown merely
        # because it also exists as a callback, milestone, or continuity hook.
        return compact


@dataclass
class MemoryRetrievalResult:
    candidates_considered: int
    selected: list[MemoryCandidate] = field(default_factory=list)
    fragments: list[str] = field(default_factory=list)
    suppressed_recent_count: int = 0
    suppressed_duplicate_count: int = 0
    relevance_threshold: float = RELEVANCE_THRESHOLD


def _kind_group(kind: str) -> str:
    return _KIND_GROUPS.get((kind or "").lower(), (kind or "memory").lower())


def _lexical_relevance(query_tokens: set[str], candidate_tokens: set[str]) -> float:
    if not query_tokens or not candidate_tokens:
        return 0.0
    overlap = len(query_tokens & candidate_tokens)
    if not overlap:
        return 0.0
    coverage = overlap / max(1, min(len(query_tokens), len(candidate_tokens)))
    jaccard = overlap / max(1, len(query_tokens | candidate_tokens))
    return min(1.0, 0.75 * coverage + 0.25 * jaccard)


def _kind_relevance(kind: str, query_tokens: set[str]) -> float:
    group = _kind_group(kind)
    if group == "relationship":
        # Broad relationship records still need lexical/emotional evidence;
        # "remember" alone must not summon an unrelated milestone.
        return 0.0
    intent = _KIND_INTENTS.get(group, set())
    return 1.0 if query_tokens & intent else 0.0


def _recency_score(created_at: float, now: float) -> float:
    if not created_at:
        return 0.0
    age_days = max(0.0, now - created_at) / 86400
    if age_days <= 7:
        return 1.0
    if age_days <= 30:
        return 0.65
    if age_days <= 180:
        return 0.3
    return 0.0


def _novelty_score(last_used: float, now: float) -> tuple[float, bool]:
    if not last_used:
        return 1.0, False
    age = max(0.0, now - last_used)
    if age < 10 * 60:
        return -2.2, True
    if age < 3600:
        return -1.35, True
    if age < 86400:
        return -0.55, True
    if age < 7 * 86400:
        return 0.0, False
    return 0.65, False


def _near_duplicate(left: MemoryCandidate, right: MemoryCandidate) -> bool:
    left_tokens, right_tokens = normalize_tokens(left.text), normalize_tokens(right.text)
    if left_tokens and right_tokens:
        overlap = len(left_tokens & right_tokens) / max(1, min(len(left_tokens), len(right_tokens)))
        if overlap >= 0.78:
            return True
    return SequenceMatcher(None, left.text.lower(), right.text.lower()).ratio() >= 0.82


class MemoryRetriever:
    """Score a bounded candidate set, then vary only among close, relevant options."""

    def __init__(
        self,
        *,
        threshold: float = RELEVANCE_THRESHOLD,
        max_fragments: int = MAX_MEMORY_FRAGMENTS,
        rng: random.Random | None = None,
        wall_clock: Callable[[], float] = time.time,
    ):
        self.threshold = float(threshold)
        self.max_fragments = max(0, min(2, int(max_fragments)))
        self.rng = rng or random.Random()
        self.wall_clock = wall_clock
        self._recent = BoundedTTLCache(
            ttl_seconds=7 * 86400, max_entries=4096,
        )

    def score(
        self,
        candidate: MemoryCandidate,
        query: str,
        *,
        user_id: int,
        serious: bool,
        conflict_open: bool,
    ) -> tuple[float, bool]:
        now = self.wall_clock()
        runtime_used = float(self._recent.get((int(user_id), candidate.identity), 0) or 0)
        last_used = max(float(candidate.last_used or 0), runtime_used)
        query_tokens = normalize_tokens(query)
        candidate_tokens = normalize_tokens(candidate.text)
        candidate.relevance = _lexical_relevance(query_tokens, candidate_tokens)
        kind_match = _kind_relevance(candidate.kind, query_tokens)
        candidate.importance = max(0.0, min(1.0, float(candidate.weight or 0) / 10.0))
        candidate.recency = _recency_score(float(candidate.created_at or 0), now)
        candidate.novelty, was_recent = _novelty_score(last_used, now)

        group = _kind_group(candidate.kind)
        emotional = 1.0 if (
            query_tokens & {"hurt", "scared", "nervous", "lonely", "care", "repair", "conflict"}
            and group in {"comfort", "conflict", "repair", "relationship"}
        ) else 0.0
        unresolved = 1.0 if (
            conflict_open and group in {"conflict", "repair"}
            and (candidate.relevance > 0 or kind_match > 0 or emotional > 0)
        ) else 0.0
        relationship = 1.0 if (
            query_tokens & {"remember", "promise", "together", "care", "relationship"}
            and group in {"promise", "relationship", "conflict", "repair"}
            and (candidate.relevance > 0 or kind_match > 0)
        ) else 0.0
        irrelevant = 1.0 if not any((candidate.relevance, kind_match, emotional, unresolved, relationship)) else 0.0
        stale_low_value = 1.0 if (
            candidate.created_at and now - candidate.created_at > 180 * 86400
            and candidate.weight < 4
        ) else 0.0
        joke_penalty = 1.0 if serious and group == "joke" else 0.0

        candidate.final_score = (
            0.58 * candidate.relevance
            + 0.24 * candidate.importance
            + 0.08 * candidate.recency
            + 0.10 * candidate.novelty
            + 0.34 * kind_match
            + 0.16 * emotional
            + 0.28 * unresolved
            + 0.16 * relationship
            - 0.32 * irrelevant
            - 0.16 * stale_low_value
            - 1.0 * joke_penalty
            - 0.30 * float(was_recent)
        )
        return candidate.final_score, was_recent

    def retrieve(
        self,
        candidates: list[MemoryCandidate],
        query: str,
        *,
        user_id: int,
        serious: bool = False,
        conflict_open: bool = False,
    ) -> MemoryRetrievalResult:
        recent_count = 0
        for candidate in candidates:
            _, was_recent = self.score(
                candidate, query, user_id=user_id, serious=serious,
                conflict_open=conflict_open,
            )
            recent_count += int(was_recent)
        ranked = sorted(candidates, key=lambda item: item.final_score, reverse=True)

        unique: list[MemoryCandidate] = []
        duplicate_count = 0
        for candidate in ranked:
            if candidate.final_score < self.threshold:
                continue
            if any(_near_duplicate(candidate, kept) for kept in unique):
                duplicate_count += 1
                continue
            unique.append(candidate)

        selected: list[MemoryCandidate] = []
        available = unique[:8]
        while available and len(selected) < self.max_fragments:
            top_score = available[0].final_score
            close = [item for item in available[:3] if item.final_score >= top_score - 0.16]
            weights = [max(0.02, item.final_score - self.threshold + 0.08) for item in close]
            chosen = self.rng.choices(close, weights=weights, k=1)[0]
            selected.append(chosen)
            available.remove(chosen)
            # Prefer another memory family when two options are otherwise close.
            if available and _kind_group(available[0].kind) == _kind_group(chosen.kind):
                alternatives = [item for item in available if _kind_group(item.kind) != _kind_group(chosen.kind)]
                if alternatives and alternatives[0].final_score >= available[0].final_score - 0.08:
                    available.remove(alternatives[0])
                    available.insert(0, alternatives[0])

        return MemoryRetrievalResult(
            candidates_considered=len(candidates), selected=selected,
            suppressed_recent_count=recent_count,
            suppressed_duplicate_count=duplicate_count,
            relevance_threshold=self.threshold,
        )

    def mark_selected(self, user_id: int, candidates: list[MemoryCandidate]) -> None:
        now = self.wall_clock()
        for candidate in candidates:
            self._recent[(int(user_id), candidate.identity)] = now

    def forget_user(self, user_id: int) -> None:
        uid = int(user_id)
        for key in list(self._recent):
            if key[0] == uid:
                self._recent.pop(key, None)


def candidate_fragment(candidate: MemoryCandidate) -> str:
    text = " ".join((candidate.text or "").split())[:220]
    labels = {
        "memory_bank": "MEMORY_BANK", "callback": "CALLBACK",
        "milestone": "MILESTONE", "inside_joke": "SHARED_JOKE",
        "historical_message": "RECALL", "continuity": "CONTINUITY",
        "topic": "TOPIC_CONTEXT", "conflict": "CONFLICT_OPEN",
        "summary": "SUMMARY",
    }
    label = labels.get(candidate.source, "MEMORY")
    return f"{label}:{candidate.kind}|{text}" if candidate.kind else f"{label}:{text}"
