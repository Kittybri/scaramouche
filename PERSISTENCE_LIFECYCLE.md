# Persistence lifecycle

This document describes the storage lifecycle shared by the Scaramouche runtime. It is operational documentation, not a promise that every transient interaction is retained.

## Database scopes and migrations

`Memory` owns two independent SQLite scopes:

- **local** (`scaramouche.db` by default): users, messages, preferences, scenes, reminders, trivia, relationship memory, and the privacy-deletion ledger;
- **shared** (`shared_state.db` by default): cross-bot users, duo sessions, cooldowns, milestones, and the Scaramouche/Wanderer relationship.

Each file has its own rows in `schema_migrations(scope, version, name, applied_at)`. The current versions are local **3** and shared **1**. Ordered migration definitions live in `db_migrations.py`; new schema changes must be appended, never inserted or renumbered.

Older deployed databases do not have migration metadata. Initialization creates the metadata table, introspects the existing allowlisted columns, adds only columns that are absent, and then records the compatible version. It does not recreate tables, truncate rows, or reset relationship values. An empty database first receives the normal base schema and then follows the same migration path. Standalone chaos and voice recovery components use the same centralized preference-column definition when they start before `Memory.init`; the next full initialization records the formal versions.

Every migration runs as `BEGIN IMMEDIATE -> schema change -> version record -> COMMIT`. A cancellation or failure rolls the migration back, so an unapplied version can be retried on the next startup. The migration runner rechecks each version after acquiring the write lock. SQLite busy timeouts, bounded WAL-negotiation retries, and the version primary key allow Scaramouche and Wanderer to initialize a shared file concurrently without duplicate records.

`!persistence` is owner-only and reports local/shared current versions, pending versions, migration error categories, pending privacy jobs, and cache entry counts. It never prints database contents.

## Privacy deletion saga

The full reset crosses databases and services, so it cannot be one atomic SQL transaction. `PrivacyDeletionCoordinator` records an opaque job ID and completion markers in `privacy_deletion_jobs`. It stores no username, conversation excerpt, face data, prompt, or deleted content.

Required stages, in order, are:

1. bot-local memory;
2. shared user memory;
3. persistent-world and Grudge Journal user records;
4. face templates and consent generation;
5. user-scoped self-model records;
6. server-chaos preferences and active participation;
7. voice-social records and active VC participation;
8. per-user ephemeral runtime entries;
9. companion/PC deletion, when applicable to the owner.

Each stage is idempotent and checkpointed only after success. A failure produces `RETRYABLE`, retains completed stage names, and does not claim that deletion finished. Repeating the existing reset confirmation resumes the same job and skips completed stages, including after restart. No aggressive remote retry loop is used. Completed ledger rows contain only minimal audit metadata and are pruned after 30 days.

`Memory.reset_user_local` and `Memory.reset_user_shared` are separately checkpointed. The original `reset_user` remains as a compatible wrapper. Shared bot-pair state in `bot_relationships` is global and is deliberately not user deletion data. Duo sessions initiated by the deleted user are removed; unrelated duo sessions remain.

DM scene history deliberately uses the Discord user ID as its stable `scene_state.channel_id` key. Reset therefore removes the row whose channel key equals the user ID. Guild-channel scenes and other users' DM scenes are retained.

Some server recovery records may outlive a user reset only while needed to restore a temporary channel/server mutation safely. They are **minimal transactional recovery**, not character memory. Personal messages, face templates, relationship memory, and social records are **delete now**. The deletion job itself is **bounded audit metadata**.

Face deletion increments the consent generation before removing every profile owned by the user. That invalidates an in-flight enrollment snapshot and prevents it from saving over a deletion. Repeated deletion is safe. Companion deletion is required only for the configured owner; an unavailable authenticated relay leaves the job retryable rather than silently succeeding. Raw screenshot and voice audio remain ephemeral and are not added to the ledger.

## Runtime collection policy

| Collection | Classification | Lifetime / maximum | Restart behavior |
|---|---|---:|---|
| `_background_tasks` | active task registry | explicit startup/shutdown lifecycle | cleared |
| `_transient_tasks` | active task registry | task completion callback | cleared |
| `_tedtalk_active` | ephemeral in-flight set | `finally` cleanup | cleared |
| `_typing_gag_inflight` | ephemeral in-flight set | `finally` cleanup | cleared |
| `_processed_msgs` | dedup structure | 1 hour / 500 | cleared |
| `_tedtalk_cache` | bounded cache | 2 hours / 128 | cleared |
| `_weather_cache` | bounded cache | 1 hour / 256 locations | cleared |
| `_voice_state_cache` | bounded cache | 30 days / 2,048 users | cleared |
| `_presence_activity` | bounded cache | 6 hours / 4,096 member-guild pairs | cleared; member removal also evicts |
| `_hostages` | ephemeral gag session | 24 hours / 512 users | cleared intentionally |
| `_member_announcement_last_sent` | safe small cooldown map | 24-hour lazy pruning / 2,048 guilds | cleared |

`BoundedTTLCache` uses a monotonic clock, lazy stale pruning, and deterministic oldest-entry eviction. No cleanup thread and no external cache service are required. Voice smoothing formulas, Fish Audio behavior, and user-visible character behavior are unchanged.

## Operational rules

- Back up both SQLite files before a manual schema intervention.
- Never delete or edit a `schema_migrations` row to force a production migration. Diagnose and repair the underlying schema instead.
- A `RETRYABLE` deletion is incomplete. Restore the unavailable subsystem and use the user's reset confirmation again.
- Do not put personal content in migration logs, deletion logs, or audit records.
- The shared schema remains additive and compatible with the current Wanderer generation; changes requiring a coordinated Wanderer release belong in a separate batch.
