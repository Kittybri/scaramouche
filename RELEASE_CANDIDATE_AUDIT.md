# Full-System Release-Candidate Audit

Audit date: 2026-09-30  
Release state: **READY_FOR_STAGING**  
Scope: integration hardening only; no feature expansion and no merges performed.

This verdict means the automated and offline integration gates pass and the
confirmed release blockers found during this audit were repaired. It does **not**
mean production-proven: the controlled checks in `RELEASE_VALIDATION.md` remain
mandatory before a production rollout.

## 1. Exact source heads audited

| Repository | Audited source branch | Audited source SHA |
|---|---|---|
| `Kittybri/scaramouche` | `fix/home-device-integrations` | `7420328e801b98c2270eef7e1dd30b8d0f304d0f` |
| `Kittybri/Wanderer` | `feature/server-chaos-games` | `f955975f795d0200d843403877ef52db3bbda3b3` |

Both source working trees were clean before the audit. Remotes were fetched and
the named branches still resolved to those SHAs. Release changes are stacked on
those exact heads in each repository's `release/full-system-hardening` branch.

## 2. PR stack and ancestry

Scaramouche's audited chain contains, once each, the server-chaos and opt-in
trolling features followed by Repair Batches 1–9B: interaction arbitration,
message pipeline, response context, persistence, memory/variety, search,
background resilience, self-model lifecycle, cloud, and home-device hardening.
The source PR is #21, open and GitHub reports it mergeable.

Wanderer's audited chain contains the persistent-world, home, companion,
full-duplex voice, advanced-VC, and server-chaos layers. The source PR is #8,
open and GitHub reports it mergeable.

`git fsck --full --no-dangling`, ancestry inspection, conflict-marker search,
and working-tree inspection found no dropped ancestry, duplicated feature
commit, merge artifact, or reintroduced alternate implementation.

## 3. Architecture map

```text
Discord events
  -> bot-specific command/message arbitration
  -> safety/privacy/consent/permission gates
  -> structured sessions and direct/media handling
  -> bot-specific response composition + provider boundary
  -> deliver to Discord
  -> persist delivered assistant state

Per-bot local SQLite       Shared SQLite
  users/relationship       shared users/duo relationship
  messages/memory          persistent world/chaos/social state
  self-model (Scara)       voice-social/cooldowns/evidence

Optional adapters
  grounded search | cloud accounts | home relay/agent | companion | voice
```

Scaramouche's typed `InteractionContext`/`ResponseContext`, migrations, task
supervisor, and self-model remain Scaramouche-specific. Wanderer retains its
own lighter pipeline and character implementation; this audit did not wholesale
port Scaramouche internals.

## 4. Character integrity

**PASS.** Prompt snapshots, interaction-policy tests, relationship tests, and
source review preserve Scaramouche as proud, cutting, theatrical, defensive,
petty, intelligent, mischievous/malicious, and resistant to admitting
attachment. Safety changes constrain when a bit may fire, not his personality.

**PASS.** Wanderer remains calmer, observant, protective, useful, increasingly
trusting, occasionally mischievous, and able to contradict Scaramouche. His
protective and credential guards are neutral policy gates; ordinary prompt and
relationship behavior remains bot-specific.

**PASS.** Separate local DBs, bot names, prompts, relationship stores, voice
settings, and provider composition prevent prompt/state contamination. Shared
tables carry intentionally shared world/duo/social facts, not a shared base
personality. No evidence showed “warm Scaramouche” defaults leaking into
Scaramouche or trolling defaults leaking into Wanderer.

## 5. Interaction precedence

**PASS after repair.** Scaramouche's staged policy remained ordered as bot-loop
suppression → credential guard → command → privacy/boundary/serious policy →
structured ownership → direct/media/world → optional bits → normal response.

Wanderer had a confirmed precedence defect: serious/protective messages could
reach milestones or optional behavior, and optional commands could win. The
release branch now classifies credentials before commands, blocks playful or
coercive commands in protective context, and routes protective messages before
milestones/gimmicks. Privacy-deletion-pending users are also blocked from new
memory except for the retry command.

The collision suites cover command/direct combinations, safety versus optional
features, duo/session ownership, and authoritative delivery. No tested input
produced two ordinary Discord replies.

## 6. Message pipeline and delivered-state integrity

**PASS after repair.** Scaramouche's existing staged pipeline and delivery
receipt behavior passed regression tests.

Wanderer had a confirmed delivered-state defect in rare softness, delayed
silence, private confession, command interaction helpers, media, partner invite,
proactive, and voice-fallback paths. Those paths now commit assistant memory,
achievements, duo progress, or proactive cooldowns only after successful Discord
delivery. Failed guarded sends return without manufacturing a successful
conversation turn.

Text, command, proactive, media, voice fallback, duo autoplay, and integration
result paths were reviewed. Real Discord HTTP behavior remains a live gate.

## 7. Prompt composition and budgets

**PASS with bounded inputs.** Scaramouche composes identity, raw relationship,
derived character state, selected memory, world, integration data, factual
evidence, partner/duo, self-model, environment, channel context, and then the
authoritative resolved-state directive immediately before the current user
message. Tests assert the final authority ordering and external-data policy.

Major contributors are bounded: conversation history (configuration-enforced
count/per-message/total characters), channel context (bounded message count and
150 characters per message), memory selection (two fragments), self-model
belief/goal/contradiction limits, search (three pages, 700 evidence characters
per source, 6,000 factual-context characters), integration result limits, and
anti-repeat sample windows. The response-context and grounding tests passed;
no unbounded database or webpage dump entered the tested prompt path.

An all-active upper-pressure assembly using the configured/bounded contributor
sizes measured 7,875 system-prompt characters plus 30,766 user-context
characters (38,641 total) and exactly one final authoritative directive. This
was an offline composition measurement, not a provider call.

Provider tests verify one normal generation call, explicit counted retry/fallback
behavior, removal of fabricated citations, and no claim of current verification
when retrieval fails. Autonomous self-model/heartbeat budgets are persisted and
survive restart.

## 8. Persistence and schema lifecycle

**PASS.** Both repositories initialized cleanly from temporary databases. The
Scaramouche migration lifecycle rejects unsupported/future state and records
versions; Wanderer's compatible older local migration style remains a bounded
technical difference. SQLite quick checks passed after unit, stress, and soak
workloads.

No dynamic user-provided table or column identifier was introduced. The reset
repair uses static allowlisted table names discovered from `sqlite_master` only
to decide whether a compatible optional table exists.

## 9. Shared SQLite compatibility and contention

**PASS.** `release_audit.py` launched the real Scaramouche and Wanderer memory
implementations in separate subprocesses against one temporary shared SQLite
database.

Result: 500 composite iterations (250 per process), 0 errors, 0
`database is locked` events, 0 leftover process tasks, and `quick_check=ok` for
all three databases. Final shared state included 12 users, 40 bounded banter
rows, 24 duo rows, and 500 cooldown rows. The compatible additive duo columns
were present. The final rerun elapsed time was 80.852 seconds.

Shared drift classification:

| Area | Classification | Evidence |
|---|---|---|
| `world_store.py` | compatible / identical | Same implementation |
| home protocol | compatible implementation difference | Scaramouche's additive `failure_category` preserves wire compatibility |
| voice receive/session/features | compatible / largely identical | Both voice suites pass |
| social/chaos migration helpers | compatible implementation difference | Scaramouche centralizes migration; Wanderer uses guarded additive `ALTER` |
| shared duo/world schemas | compatible | Dual-process stress and schema checks pass |
| prompts, relationship, awareness, anti-repeat | intentionally character-specific | Separate character behavior |
| privacy table coverage before repair | release blocker | Fixed in both release branches |

No remaining unintentional shared-schema drift was demonstrated.

## 10. Privacy and deletion

**PASS after blocker repairs.** Scaramouche's resumable deletion job could
previously be followed by a new message that recreated data, and its shared
cleanup did not know about several Wanderer-created user tables. Pending deletion
now blocks new processing except retry/status paths; shared cleanup conditionally
removes all known compatible user scopes; final local/shared cleanup runs after
subsystems that may touch preferences.

Wanderer's previous reset was non-resumable and incomplete. It now has a
checkpointed deletion coordinator covering local memory/preferences/reminders/
RPG state, shared user/attention/duo/world/face/event/evidence/opinion data,
world, face, chaos, voice-social, companion, and final cleanup. A failed stage
resumes without repeating completed stages. Tests seed a second user and verify
that user is unchanged.

Reconnect/heartbeat recreation is prevented by the pending-deletion gate.
Verification that a separately deployed home hub/agent has no pending proposal
for the deleted user is retained as a live multi-process check in the manifest;
the local test process cannot prove the state of an external deployment.

## 11. Memory, relationship, and self-model

**PASS.** Multi-user tests keep message, memory, callbacks, relationship, and
self-model records scoped by user/channel. `RawRelationshipState` and resolved
self-concept remain distinct; self-model evidence affects stance/attention but
does not directly rewrite relationship values.

Long-running policy tests cover kindness, conflict, repair, belief evidence,
contradiction deduplication/resolution, goal expiry/completion, stale pressure,
and persistent autonomous budgets. Memory candidates and recent reply/pattern
histories are capped. Character snapshots remain Scaramouche-specific through
development.

## 12. Anti-repeat and character variety

**PASS.** Exact and rhetorical-pattern defenses, per-user/per-channel histories,
cooldowns, opening controls, and relevance-aware memory arbitration passed.
Serious and factual modes retain direct language and evidence rather than being
forced through novelty rewrites. The soak ended with bounded pattern scopes (24
user and 40 channel) and no runtime-cache growth beyond configured capacity.

## 13. Search and factual grounding

**PASS.** Tests exercise query detection, provider fallback, source fetch,
redirect/SSRF defenses, content-size limits, HTML/text extraction, ranking,
bounded evidence, citations, timeouts, blocked providers, zero sources,
contradictory/stale evidence, and malicious page instructions. Current answers
cannot claim verification when usable sources are absent; fake citation numbers
are removed.

Calendar/integration data and public web evidence use separate marked untrusted
blocks. Provider data cannot modify identity, permission, consent, privacy, or
home-action directives.

## 14. Cloud integrations

**PASS in mocks; live account validation required.** Tests cover account-scoped
Calendar, Tasks, Sheets, Spotify, Steam, MAL, and GitHub behavior; bounded reads;
private-field handling; provider failures; and external-data injection.
Write proposals bind user/action/payload/expiry, are single-use, and reject wrong
user, altered payload, expiry, or replay. Optional missing credentials degrade
without blocking core chat.

## 15. Home relay and companion

**PASS in protocol/network mocks; physical validation required.** Tests cover
bot→client→HTTP hub→authorization→WebSocket agent→provider→correlated result,
identity binding for each bot, confirmations, replay/expiry/payload mismatch,
duplicate frames, global/device disable, disconnect/reconnect, hub restart,
stale-work rejection, audit receipts, and companion capability restrictions.

Scaramouche's additive failure category remains compatible with Wanderer.
Model-generated prose is not a physical command: structured parsing, permission,
proposal, confirmation, and relay validation separate language from authority.

## 16. Background workers and failure isolation

**PASS.** Scaramouche supervisor tests cover cancellation, bounded exponential
backoff, restart accounting, duplicate prevention, health state, and shutdown.
Discord loops plus chaos, voice, home, companion, and reconnect workers were
reviewed for bounded waits/cooldowns and independent exception handling.

Failure tests isolate Groq, search/fetch, Google, Spotify, GitHub, Steam, MAL,
home, companion, and voice paths. Unrelated core behavior remains available in
the automated model. Discord gateway and long real network storms remain live
validation; mocks cannot prove hosting/network behavior.

## 17. Voice

**PASS automated; live voice is not claimed.** Both full suites cover receive
attribution, bot/self filtering, VAD, bounded queues, STT failure, generation
staleness, interruption/playback cancellation, delivered-memory semantics,
cleanup, reconnect, consent, and two-bot loop resistance. Scaramouche voice
prompting prohibits narration without imposing the previously rejected hard
two-sentence/36-word limit.

Real Discord DAVE/encrypted receive, Opus, human speech, Groq STT, Fish Audio,
barge-in, echo filtering, two live bots, and latency are explicitly retained in
`RELEASE_VALIDATION.md`.

## 18. Chaos, restoration, and persistent world

**PASS.** Chaos tests cover opt-in eligibility, shared budgets, reversible apply
and receipts, restart restoration, idempotency, and compare-before-restore so a
newer administrator edit is not overwritten. Cross-bot shared state avoids the
tested duplicate mutation/restoration/wager paths.

Persistent-world tests cover grudges, dreams, birthdays, callbacks, shared bot
relationship, retained events, limits, restart recovery, and one-time delivery.
The soak ended with no orphan duo sessions or pending restore/delete work.

## 19. Configuration and security

**PASS automated.** Optional integrations can be absent without import/boot
failure in the test environment. Security-sensitive home configuration rejects
weak secrets and malformed permissions. Migration tests reject incompatible DB
state.

Secret-pattern review found no production credential committed. Test fixtures
contain deliberately fake credential-shaped strings solely to verify the guard.
Web, cloud, screen/media, and device metadata are treated as external data;
tests assert they cannot become privileged instructions. Dynamic SQL remains
allowlisted. Real token rotation and deployment-variable review remain an owner
operational responsibility.

## 20. Rate, model-call, and soak results

Discord sends retain application pacing, cooldowns, bounded proactive budgets,
quiet hours, chaos budgets, and discord.py's native HTTP handling. Normal
generation tests assert one provider call; retry, anti-repeat, grounding,
reflection, proactive, and duo calls are separately gated and bounded. Persisted
hour/day autonomous budgets cannot be reset by a process restart.

`release_soak.py --turns 600` passed an accelerated workload spanning Memory,
WorldStore, self-model policy, caches, anti-repeat histories, duo sessions, and
one-time events:

- task count 1 before / 1 after
- runtime cache 128/128 and bounded set 256/256
- pattern scopes 24 users / 40 channels
- 384 retained memory rows (24 users × configured 16 cap), 600 message rows
- 0 open duo sessions, 0 pending deletion jobs, 0 active goals, 0 contradictions
- all three SQLite `quick_check` results `ok`
- DB sizes: local 266,240 B; shared 94,208 B; self-model 106,496 B
- traced memory 200,705 B current / 211,802 B peak
- elapsed 99.655 seconds

No continuously growing task count, corrupt DB, orphan session, runaway goal or
contradiction, or repeated one-time event was observed.

## 21. Operations, health, and Cloudflare check

Existing owner diagnostics are sufficient without a new dashboard. `!bothealth`
and self-model/worker diagnostics expose provider/environment status, DB health,
autonomous budgets, worker restart/failure state, and integration configuration;
privacy job counts and home/agent status are available through their existing
diagnostics. The validation manifest tells the operator what to verify live.

The recurring `Workers Builds: scaramouche` / `Workers Builds: wanderer` checks
originate from GitHub App **Cloudflare Workers and Pages** (app ID 85455). They
completed with `FAILURE`, not “pending.” The details links point to Cloudflare
dashboard Workers Builds. Neither repository contains Wrangler, Workers, Pages,
or Cloudflare deployment configuration; repository rulesets are empty and the
source branches are not protected by this check. These Python Discord bots deploy
through Railway/host processes, so available evidence indicates a stale or
misattached account-level Cloudflare Git integration rather than a code release
gate.

Corrective action is dashboard-side: in Cloudflare, inspect the `scaramouche` and
`wanderer` Workers Builds projects and disconnect these repositories if they are
not intentionally deployed there, or configure the intended Workers projects.
No repository code change can correct that account-controlled check, and this
audit did not disable it.

## 22. Findings and fixes

| Severity | Finding | Disposition |
|---|---|---|
| BLOCKER | Scaramouche pending deletion allowed later processing to recreate user data | Fixed and regression-tested |
| BLOCKER | Scaramouche shared reset omitted Wanderer-created user-scoped tables | Fixed with optional-table compatible cleanup |
| BLOCKER | Wanderer reset was incomplete and non-resumable | Fixed with checkpointed multi-subsystem deletion |
| BLOCKER | Wildcard user-scope cleanup could treat user `1` as a prefix of user `10` | Fixed with delimiter-aware exact/suffix matching and collision tests |
| BLOCKER | Wanderer accepted credential-like disclosure into normal command/model flow | Fixed with pre-model credential guard |
| HIGH | Wanderer serious/protective messages could lose to optional behaviors/commands | Fixed with early authoritative routing |
| HIGH | Wanderer stored assistant output/cooldowns after guarded send failure | Fixed across confirmed paths |
| MEDIUM | Wanderer retains ad-hoc additive migration code instead of Scaramouche's centralized helper | Compatible today; defer architectural convergence |
| MEDIUM | Both GitHub PRs display unrelated failed Cloudflare Workers checks | Dashboard-owner correction required; not a code gate |
| LOW | urllib3 emits a LibreSSL compatibility warning in the local Python 3.9 environment | Use an OpenSSL-backed supported runtime for deployment |
| LIVE_VALIDATION_ONLY | Real Discord, DAVE/Opus, provider accounts, Fish, home devices, and chaos staging | Execute `RELEASE_VALIDATION.md` |

No unresolved automated BLOCKER or HIGH finding remains on the release branches.

## 23. Test evidence and recommendation

Fresh final automated results on the release working trees:

- Scaramouche: **652 passed, 1 skipped, 1 warning** in 102.17 seconds.
- Wanderer: **206 passed, 1 skipped** in 46.45 seconds.
- Wanderer focused release-hardening suite: **7 passed**; the complete 206-test
  run includes its final form.
- Cross-process shared DB: **PASS**, 500 composite iterations, zero lock errors.
- Soak: **PASS**, 600 turns with bounded tasks/caches/state and healthy DBs.
- Command registration: Scaramouche **160/160 unique**; Wanderer **155/155
  prefix commands plus 5 slash commands**, with no duplicate prefix names.
- Compile/import checks: passed for changed modules; imports report only the
  documented local LibreSSL warning.
- Secret scan and `git diff --check`: pass, with fake security-test literals
  classified as fixtures rather than credentials.

Recommendation: **READY_FOR_STAGING**. Keep both PRs unmerged until their exact
release heads are reviewed and the applicable live checklist is completed. Any
failure of privacy isolation, confirmation, restoration, or two-bot loop safety
returns the candidate to `NOT_READY`.
