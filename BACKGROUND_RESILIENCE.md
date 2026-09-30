# Background Resilience

## Task classification

The bot has four centrally named, supervised workers:

| Worker | Class | Why |
| --- | --- | --- |
| `self-heartbeat` | Critical long-running | Maintains self-model/environment work and bounded proactive decisions. |
| `temporary-setting-restore` | Critical long-running | Keeps legacy restoration receipts visible and recoverable. |
| `weather-proactive` | Optional long-running | Opt-in convenience; its loss does not compromise recovery or safety. |
| `duo-autoplay` | Optional long-running | Opt-in scene continuity; its loss does not compromise recovery or safety. |

`status_rotation`, `reminder_checker`, and `daily_reset` remain
`discord.ext.tasks.loop` jobs. Their framework owns their lifecycle; focused error
boundaries prevent one reminder from aborting a batch, and loop error callbacks
report unexpected termination.

Server-chaos and advanced-voice maintenance are feature-owned long-running loops.
They retain their existing `on_ready` duplicate guards, shutdown hooks, recovery
records, and owner alerts. Per-interaction work created by `_spawn_transient` is
one-shot and is never restarted.

## Supervisor policy

`TaskSupervisor` owns exactly one runner for each registered worker. Unexpected
exit or exception records health and restarts the worker with exponential backoff:
5, 10, 20, 40, 80, then at most 120 seconds. A worker that has run healthily for
five minutes resets the failure streak, so an old failure does not permanently
force the maximum delay. This supervision adds no model calls.

Cancellation is shutdown, not failure. `CancelledError` propagates, does not
increase failure counters, and never schedules a restart. Supervisor lifecycle is
explicitly `RUNNING`, `STOPPING`, or `STOPPED`. Shutdown changes state before it
cancels restart sleeps and workers, then cancels/awaits transient tasks; the bot
subsequently cancels Discord task loops and closes Discord.

Repeated `on_ready` events call the idempotent start path. A running worker or a
worker already backing off keeps its single supervisor runner; reconnect cannot
create a second copy. Runtime initialization is marked complete only after both
stores and initial beliefs succeed, so a later ready event can retry a failed
initialization without starting dependent workers early.

## Health and diagnostics

Each supervised worker records state, criticality, starts, failures, restarts,
consecutive failures, start/progress/success/failure timestamps, last error
category, next restart time, and sanitized progress fields. Critical loops publish
successful iteration timestamps; restoration also publishes the pending legacy
receipt count. No prompts, message text, reflection text, secrets, or user content
are stored in task health.

The owner-only `!tasks` / `!taskhealth` command shows compact supervised-worker and
Discord-loop health. It warns when legacy restoration receipts are pending while
the restoration worker is unhealthy. It is diagnostic only and never performs a
restoration.

## Error accounting and isolation

Reflection generation is separated from reflection persistence. A successful
provider response records provider success before SQLite reflection/event writes.
A later database failure remains a persistence failure and does not falsely count
against Groq. Cancellation propagates through generation and persistence.

Weather and duo scans isolate individual candidates/sessions. Malformed duo state
is skipped and, when safely identifiable, cleared. Restoration scans retain failed
or legacy receipts. Reminder delivery isolates each reminder and distinguishes
database, provider/network, unavailable-target, and Discord HTTP failures.

## Proactive delivery semantics

The order is reservation, generation, Discord send, durable `delivered` marker,
then independent best-effort memory/cooldown/self-model writes. Once Discord has
accepted a send, later bookkeeping failures never turn it into a failed send.
`Forbidden`, `NotFound`, and other Discord HTTP failures are terminal for that
attempt and do not create an uncontrolled retry loop.

If the `delivered` marker itself cannot be written, the original `pending`
reservation remains the conservative duplicate barrier through the existing user
cooldown. This deliberately biases toward at-most-once delivery. It is not an
exactly-once guarantee: Discord and SQLite do not share a distributed transaction,
and a sufficiently long database outage may eventually expire a pending receipt.

## Remaining limitations

- A live but internally hung coroutine is visible through stale progress time but
  is not force-cancelled by an aggressive watchdog.
- Feature-owned server-chaos and advanced-voice loops are reported by their own
  established recovery/owner-alert paths rather than the central `!tasks` table.
- Discord task loops use discord.py lifecycle semantics; they are not wrapped in a
  second restart system.
- Legacy slowmode receipts lack the applied-value evidence needed for safe
  compare-before-restore and therefore remain for explicit reconciliation.
