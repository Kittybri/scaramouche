"""Typed runtime data and focused diagnostics for the Discord message pipeline."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
import sqlite3

import discord


@dataclass
class PreparedMessage:
    user: dict
    user_id: int
    channel_id: int
    guild_id: int | None
    is_dm: bool
    is_owner: bool
    romance: bool
    content: str
    previous_last_active: float
    reference_message: object | None = None
    message_count: int = 0
    milestone: bool = False


def command_context_matches(ctx) -> bool:
    """Use discord.py's parser result; a bare exclamation mark is punctuation."""
    return bool(getattr(ctx, "prefix", None) and getattr(ctx, "invoked_with", None))


def error_category(error: BaseException) -> str:
    if isinstance(error, asyncio.CancelledError):
        return "cancelled"
    if isinstance(error, discord.Forbidden):
        return "discord_forbidden"
    if isinstance(error, discord.NotFound):
        return "discord_not_found"
    if isinstance(error, discord.HTTPException):
        return "discord_http"
    if isinstance(error, asyncio.TimeoutError):
        return "timeout"
    if isinstance(error, sqlite3.IntegrityError):
        return "sqlite_integrity"
    if isinstance(error, sqlite3.OperationalError):
        return "sqlite_operational"
    return "unexpected"


def log_operation_error(
    logger: logging.Logger,
    *,
    subsystem: str,
    operation: str,
    error: BaseException,
    message=None,
    interaction=None,
) -> str:
    """Log useful identifiers without logging message content or private payloads."""
    if isinstance(error, asyncio.CancelledError):
        raise error
    category = error_category(error)
    extra = {
        "subsystem": subsystem,
        "operation": operation,
        "error_category": category,
        "exception_type": type(error).__name__,
        "message_id": getattr(message, "id", 0),
        "user_id": getattr(getattr(message, "author", None), "id", 0),
        "channel_id": getattr(getattr(message, "channel", None), "id", 0),
        "guild_id": getattr(getattr(message, "guild", None), "id", 0),
        "response_path": getattr(interaction, "response_path", ""),
    }
    if category == "discord_not_found":
        logger.info("message pipeline operation became stale", extra=extra)
    elif category in {"discord_forbidden", "discord_http", "timeout"}:
        extra["discord_status"] = getattr(error, "status", None)
        logger.warning("message pipeline operation unavailable", extra=extra)
    elif category.startswith("sqlite_"):
        logger.error("message pipeline persistence failure", extra=extra, exc_info=error)
    else:
        logger.exception("unexpected message pipeline failure", extra=extra, exc_info=error)
    return category
