# Tiered trolling integration

Built on `feature/server-chaos-games`, not the older default branch. Trolling behavior is Scaramouche-only; Wanderer's shared-record pruner also preserves permanent preferences. The supplied module was adapted, not copied verbatim. No new LLM/API generation, dependency, memory schema or relationship-engine changes.

## Feature mapping

| Supplied idea | Integrated behavior |
| --- | --- |
| Gaslight edit | Rare visible amendment to the bot's own reply. Original words remain, so saved conversation history is not contradicted. Fresh-read ownership/content and consent checks before edit. |
| Typing interruption | One existing typing hook, default ON per user with persistent opt-out, quiet-hours aware and shared-budget limited. Requires recent harmless public context. Never claims to read a draft, typing speed or backspaces. |
| Silent Judge | One existing reply hook, default ON per user with persistent opt-out. Harmless statements only; commands, questions, media, active trivia/duo and sensitive content continue normally. Reaction failure falls through. |
| Phantom Ping | Owner-only `!phantomping @user` uses the durable ping path without target consent. Ordinary `!pranks phantom` still requires consent. Invocation stays visible. |
| VC kidnapping | `!kidnap @user` delegates to the existing voluntary `!vcgame interrogate` flow. Existing voice consent, invitation/acceptance, channel permissions and recovery still apply. No forced hidden-room code is added. |
| Muzzle | `!muzzle @user [seconds]` enables a 10–300 second, channel-scoped labeled-parody session. Originals are never deleted or impersonated. `!unmuzzle @user` ends it. |
| Slowmode trap | `!slowtrap` requests 10-second slowmode for 90 seconds using durable before/after receipts. Existing manual changes are preserved on restoration. |
| Webhook identity theft | Replaced by `!parodyas @user` while replying to that consenting user's harmless public message. Normal bot identity and clear PARODY label; no copied avatar/name/webhook. Existing `!impersonate` is untouched. |
| Fake server wipe | `!serverwipe` and existing `!fakewipe` share one owner-only path. Every countdown frame says PRETEND/no deletions. No destructive API or claims about a user's panic/perception. |
| Owner preference | Flavor in preference responses. Owner-only ping authority is separately checked; never bypasses Discord permissions, guild settings or cooldowns. Heavy parody and VC consent remain required. |

All manual performance commands are owner-only; `!trollprefs` controls only the caller's participation. The existing unrelated `!impersonate` command retains its previous behavior and permissions.

## Heavy-feature guild configuration (OFF by default)

Typing teases, Silent Judge reactions and self-message edits do not consult guild
configuration, per-feature server flags, allowed-channel lists, or the runtime
chaos switch. They operate in eligible public text channels in any joined guild.
Personal opt-outs, safety/arbitration pauses, quiet hours, mute/proactive choices,
Discord access, shared rate budgets and cooldowns still apply. No DMs, restricted
channels or private-content redistribution are introduced. Heavy features and
owner-triggered performances retain the configured guild/channel gates below.

Merge this into the existing guild entry under `server_chaos.guilds.<guild ID>` in the integrations JSON. Keep existing fields, allowlisted channels and other feature settings:

```json
{
  "features": {
    "trolling": true,
    "parody": true,
    "ping": true
  },
  "parody_mode": "MANUAL",
  "trolling": {
    "typing": true,
    "judge": true,
    "edits": true,
    "parody": true,
    "phantomping": true,
    "kidnap": false,
    "muzzle": true,
    "parodyas": true,
    "slowtrap": false,
    "serverwipe": true
  }
}
```

The parent guild must already be enabled, with explicit `allowed_channels`, and runtime chaos control must be enabled. No configuration has been activated by this change. Slowmode needs Manage Channels. `kidnap` additionally needs the existing advanced-VC interrogation configuration, allowed text/voice channels and Discord permissions; the existing flow requires the invoker's Manage Channels permission even when they own the bot. It may use the already-configured Groq/Fish voice session after consent—this integration adds no separate model calls or audio receiver.

## Personal choices

```
!trollprefs typing on
!trollprefs judge on
!trollprefs edits on
!trollprefs parody on
!trollprefs status
!trollprefs off
```

Light choices (`typing`, `judge`, `edits`, `nicknames`) default ON and never expire.
Explicit OFF values, including legacy expired OFF records, stay off until changed.
`!trollprefs off` disables all choices and ends the caller's active parody session.
Individual `... off` controls remain available. The nickname preference is reserved:
this build has no prank that edits a user's nickname; its cosmetic nickname feature
only changes the bot's own nickname.

Parody remains OFF by default, needs both `!trollprefs parody on` and
`!pranks parody on`, and expires after 30 days. Editing a light choice does not
extend parody consent. Ordinary `!pranks phantom` still needs `!pranks ping on`;
owner-only `!phantomping` does not. Muzzle, gossip, court, and VC consent are unchanged.
`!pranks off` stores permanent OFF choices; privacy forget erases those choices
and cancels pending personal activities. A returning forgotten user gets the
normal defaults. Daytime/quiet-hour, proactive and mute checks still apply.
Proactive/DM defaults are unchanged by this patch.

## Limits, recovery and privacy

- Shared chaos budget covers actual gags, not just local timers. Typing: at least three days per user; reaction-only: one day; edits: two days; phantom and parody retain their existing seven-/three-day rules. Countdown: 90 days. Failed attempts may consume a reservation.
- Muzzle activation is a configuration receipt with its own daily cooldown; actual parody consumes the shared budget. Because parody is deliberately rare, this is usually at most one labeled response, not rewriting every message.
- No messages or command invocations are deleted by this module. Only the existing phantom implementation deletes its own tracked prank.
- Delayed edits are optional presentation: bounded task count, cancelled/awaited on shutdown, not replayed after restart. Original text remains in Discord and memory. Personal preference changes after scheduling cancel the pending edit even if subsequently re-enabled. Heavy-feature server controls do not cancel light edits. Other messages/edited sources are not overwritten.
- Cosmetic slowmode uses the existing durable restoration manager. Parody sessions use existing WorldStore receipts with expiry; the chaos maintenance/forget/restore-all paths now include them. Both bots' shared emergency control still disables these activities.
- No new tables or user-profile columns. `chaos_trollprefs`/`chaos_trollsession` records contain IDs, flags and expiry, not private message content. Existing terminal-receipt retention applies. Only heavy opt-in consent expires; light choices persist.
- The conservative game-text filter excludes many otherwise harmless everyday sentences intentionally. Disabled/unavailable gags never block normal answers.
- The existing rare memory quiz, fake typing and bounded glitch are unrelated prior features and were not rewritten. The old duplicate delayed-edit task and random Silent Judge/typing implementations were replaced, not stacked.
- Discord has no atomic conditional message/channel edit. Fresh-read checks reduce, but cannot eliminate, a precisely simultaneous external edit race. Existing recovery needs a running bot and appropriate permissions.

## Files and validation

`trolling_features.py` owns adapted policy, routing, bounded tasks and command registration. `bot.py` has narrow existing-hook replacements and initialization/help wiring. `server_chaos/service.py` extends existing opt-out/expiry/emergency cleanup to the new records. `tests/test_trolling_features.py` adds behavioral coverage; existing character-hook tests now point to the adapted path. `memory.py`, `relationship_engine.py`, voice foundation and dependencies are unchanged.

Tests exercise light defaults/persistent opt-out, heavy consent/configuration/quiet-hour/mute gates, sensitive and utility exclusion, concurrent budget limits, visible edit ownership/source/preference rechecks, shutdown and restore/re-enable cancellation, owner restrictions and ping bypass, labeled-parody opt-out, expiry/forget/emergency cleanup, slowmode manual-edit preservation, existing feature adapters, countdown labeling, command-name collision prevention and absence of destructive/webhook/new-generation code. Discord operations are mocked; SQLite transactions are real. Offline startup uses an empty environment and disabled dotenv, with no network login.

The initial integration below was validated offline. Current staging deployment
and voice evidence are tracked in `STAGING_VALIDATION.md`; no live prank or server
cosmetic change is required to validate the consent-default correction.

### Final results (2026-09-28)

- Complete Python 3.9.6 suite: **395 passed, 3 skipped** (77.73s).
- Python 3.12.14 optional voice/advanced-VC/chaos/trolling suite: **177 passed** (12.82s).
- Final focused trolling + character-hook suite after help wiring: **49 passed** (20.82s), including **37 new trolling cases**.
- Actual offline bot import: **124 commands**, original `impersonate` intact, typing intent enabled, no login/configuration/tasks activated. Import after a closed event loop, help delivery and shutdown pass.
- Compile, `git diff --check` and formatting checks passed; memory/relationship/voice/dependency files have zero diff. Targeted token/private-key pattern scan found no matches in the new files (not a comprehensive security certification).
- The base skips are optional VAD, voice-receive and Opus paths, exercised in the optional environment. Existing LibreSSL/audioop warnings remain.
- Full-suite regression caught import-time asyncio lock creation on Python 3.9; initialization is now lazy and covered by a dedicated regression test. Pending edit revision checks also prevent restore/opt-out followed by re-enable from reviving stale work.

Branch: `feature/opt-in-trolling`; base: `feature/server-chaos-games` at `be34f0dc4317a41ef59fc65a5c881ada78ea91e4`. This is a stacked review change, not a direct update to the older `scaramouche` branch.
