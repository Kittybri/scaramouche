"""Deterministic, low-cost character flourishes used by the Discord runtime."""

from __future__ import annotations

import re

SERIOUS_WORDS = {
    "suicide", "kill myself", "self harm", "abuse", "assault", "emergency",
    "police", "moderator", "report", "harassment", "password", "token",
    "api key", "credit card", "medical", "overdose", "help me now",
}
UTILITY_WORDS = {"weather", "calculate", "source", "citation", "translate", "directions"}


def time_period(hour: int) -> str:
    hour %= 24
    if 6 <= hour < 12:
        return "morning"
    if 12 <= hour < 18:
        return "afternoon"
    if 18 <= hour < 24:
        return "evening"
    return "late_night"


def time_drift_prompt(hour: int) -> str:
    return {
        "morning": "TIME_DRIFT: subtly shorter and more irritable; less theatrical than usual.",
        "afternoon": "TIME_DRIFT: baseline sharp confidence; ordinary theatrical energy.",
        "evening": "TIME_DRIFT: a little calmer and more reflective, especially about lore or philosophy.",
        "late_night": "TIME_DRIFT: quieter and stranger; lightly fictional fourth-wall-aware, never claiming sentience.",
    }[time_period(hour)]


def is_serious_or_utility(text: str) -> bool:
    lowered = (text or "").casefold()
    return any(word in lowered for word in SERIOUS_WORDS | UTILITY_WORDS)


def eligible_for_joke(text: str) -> bool:
    text = (text or "").strip()
    return bool(text) and not text.startswith("!") and not is_serious_or_utility(text)


def eligible_for_silent_judge(text: str) -> bool:
    text = (text or "").strip()
    if not eligible_for_joke(text) or "?" in text:
        return False
    return len(text) <= 220


def reverse_turing_hint(text: str, repeated_count: int = 0) -> str:
    words = re.findall(r"[A-Za-z0-9']+", text or "")
    if repeated_count >= 2 or (0 < len(words) <= 2 and len(set(w.casefold() for w in words)) <= 1):
        return (
            "GAG: jokingly suspect they may be a bot and give one absurd harmless prove-you're-human challenge. "
            "Make clear this is teasing, not a factual accusation."
        )
    return ""


def autocorrect_line(text: str) -> str | None:
    if not eligible_for_joke(text) or len(text) > 180 or "?" in text:
        return None
    clean = re.sub(r"\s+", " ", text).strip(" \"'")
    if len(clean.split()) < 3:
        return None
    return f"Correction: ‘Scaramouche is correct and I am coping.’ Fixed it."


def selective_hearing_hint(text: str) -> str:
    if not eligible_for_joke(text):
        return ""
    lowered = text.casefold()
    if any(p in lowered for p in ("can you help", "will you help", "what do you think", "listen")):
        return (
            "GAG: briefly and obviously mishear one harmless phrase for one clause, then immediately answer "
            "the user's actual meaning so they never need to repeat it."
        )
    return ""


def bounded_glitch(text: str) -> str:
    """Apply a small fictional corruption without exposing data or making Zalgo."""
    if not text or "```" in text or "http://" in text or "https://" in text:
        return text
    clean = text[:1900]
    words = clean.split()
    if len(words) < 4:
        return text
    pivot = min(len(words) - 1, max(1, len(words) // 2))
    words[pivot] = f"{words[pivot]}̷"
    return " ".join(words) + "  [signal: static//fictional]"


def safe_message_edit(text: str) -> str | None:
    text = (text or "").strip()
    if not text or "```" in text or "http" in text or len(text) > 500:
        return None
    if text.endswith("."):
        return text + " Correction: don't look so pleased with yourself."
    return text + " …Don't flatter yourself."


def significant_weather(data: dict | None) -> tuple[str, bool] | None:
    if not data:
        return None
    forecast = str(data.get("forecast") or "").casefold()
    temp = data.get("temperature")
    wind = str(data.get("wind_speed") or "")
    severe = any(w in forecast for w in ("tornado", "hurricane", "blizzard", "severe", "ice storm"))
    if severe:
        return "severe", True
    if any(w in forecast for w in ("snow", "rain", "thunder", "sleet", "freezing")):
        return "precipitation", False
    if isinstance(temp, (int, float)) and (temp >= 100 or temp <= 25):
        return "temperature", False
    match = re.search(r"(\d+)", wind)
    if match and int(match.group(1)) >= 30:
        return "wind", False
    return None
