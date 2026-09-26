"""Explicit provider model selection; avoids process-wide monkey patches."""

from __future__ import annotations

import os


MODEL_ALIASES = {
    "llama-3.3-70b-versatile": "openai/gpt-oss-120b",
    "llama-3.1-8b-instant": "openai/gpt-oss-20b",
}


def resolve_groq_model(value: str | None = None) -> str:
    configured = (value or os.getenv("GROQ_MODEL") or "openai/gpt-oss-120b").strip()
    return MODEL_ALIASES.get(configured, configured)


GROQ_TEXT_MODEL = resolve_groq_model()
GROQ_VISION_MODEL = os.getenv("GROQ_VISION_MODEL", "llama-3.2-90b-vision-preview").strip()
