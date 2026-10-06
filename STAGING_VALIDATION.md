# Live Staging Gate A Validation Record

The Gate A record below is historical. See the appended **Gate B — Live Voice**
section for the current voice validation state and newer deployed candidates.

Status: **STAGING_GATE_A_PASS**

Gate decision: all authorized Gate A permission, second-user, privacy-deletion,
reversible-chaos, cross-bot recovery, and cleanup checks are complete. No unresolved
BLOCKER or HIGH code defect remains. Voice Gate B was not started.

Record window: **2026-09-30 through 2026-10-01 (America/Los_Angeles)**

This record covers Discord text, two-bot runtime, shared state, restart behavior,
attachments, credential handling, and topic-level privacy deletion for the
Scaramouche and Wanderer release candidates. Voice Gate B, real cloud/home/device
integrations, paid resources, and PR merges were not started.

## Final candidate and environment identity

| Item | Recorded value |
|---|---|
| Scaramouche repository / PR | `Kittybri/scaramouche` / PR #22; unmerged |
| Scaramouche live code candidate | `edd4cd8d07e661532a709dd7002ba4ae41cb1064` |
| Wanderer repository / PR | `Kittybri/Wanderer` / PR #9; unmerged |
| Wanderer live code candidate | `96a9f3197961704c46393a6a9cc6e6c939b1888d` |
| Branch | `release/full-system-hardening` in both repositories |
| Candidate verification | Local and fetched remote branch heads matched before deployment; final pushes succeeded |
| Live environment | Existing Oracle fallback host; no new or paid cloud resource |
| Discord target | Private `Wanderer` guild, owner-authenticated session |
| Scaramouche release tree | `/opt/scara-wanderer-staging-fallback/releases/scaramouche-edd4cd8` |
| Wanderer release tree | `/opt/scara-wanderer-staging-fallback/releases/wanderer-96a9f31` |
| Local databases | `data-gate-a-6eab863-0d36a90/scaramouche.db` and `wanderer.db` |
| Shared database | Both services resolve the same local `shared_state.db` |
| Services | `scaramouche-staging.service`, `wanderer-staging.service` |
| Final live PIDs | Scaramouche `494903`; Wanderer `494904` |
| Final service state | Both active, `NRestarts=0`; about 68 MiB and 77 MiB respectively at final inspection |
| Final database state | All three databases: `PRAGMA quick_check = ok` |
| Risk controls | Voice, home, companion, real integrations, and proactive generic traffic stayed disabled. Chaos was enabled only for the authorized allowlisted tests and was then removed from both effective configs. |

The unrelated failed Cloudflare `Workers Builds` check remains an external
repository/account integration issue. Neither Python Discord repository defines
a Cloudflare deployment, so it is not a bot-runtime failure.

## Live evidence summary

- **E1 — Recovery and isolation:** original state was backed up with SQLite online
  backups and restrictive permissions. Legacy bot services remained disabled.
- **E2 — Exact candidates:** immutable release trees and separate virtual
  environments were used. `!build` returned the final full SHAs shown above.
- **E3 — Shared topology:** both services used separate local bot databases and
  one local shared database on the same filesystem. No SQLite network storage was used.
- **E4 — Discord connectivity:** both bots authenticated, appeared online together,
  and answered private DMs without a bot loop.
- **E5 — Ownership and attribution:** each direct mention reached only the intended
  bot. A reply to Wanderer produced one Wanderer response and no Scaramouche response.
  `!scarahelp` and `!wanhelp` each produced their own three-page help output without
  the partner stealing or extending the command interaction.
- **E6 — Serious arbitration:** Scaramouche returned a calm actionable answer.
  Wanderer's first live attempt exposed an internal fallback phrase; the fix was
  deployed and the retest returned calm, user-facing guidance.
- **E7 — Credential guard:** the first fake credential exposed a proactive-rivalry
  persistence path. After the fix, a second unique fake credential generated only
  security warnings; after the cooldown window it had zero hits in all three databases.
- **E8 — Attachments:** a synthetic 64×64 blue/yellow image was accurately described
  by both bots after xAI-to-Groq vision fallbacks were added. A synthetic text file
  uploaded and previewed without a crash. A custom unsupported extension could not
  be selected through the browser chooser, so that negative case remains blocked.
- **E9 — Shared duo:** `!both GATE-A-DUO-20261001: ...` produced exactly one short
  answer from each bot. Shared/local rows were observed, all databases remained
  healthy, and the synthetic marker was removed after verification.
- **E10 — Privacy deletion:** synthetic memory was created, shown by `!memories`,
  deleted by topic, and absent from the next snapshot. Wanderer initially retained
  the marker in `scene_state`; the deletion path was hardened and the live retest
  removed the final scene field. A direct scan then found zero disposable-memory
  marker hits in all three databases.
- **E11 — Restart matrix:** Scaramouche-only, Wanderer-only, and dual restarts all
  changed only the intended PIDs and reconnected. The final dual restart connected
  both bots in roughly eight seconds with `NRestarts=0`.
- **E12 — Final health:** sanitized final-candidate logs contain clean gateway
  connections, migrations, ready events, and no traceback or lock storm. All three
  databases passed final integrity checks.
- **E13 — Permission matrix:** both bot roles were temporarily reduced from their
  recorded Administrator value and tested against role-specific channel denies for
  View Channel, Send Messages, Read Message History, Embed Links, Attach Files, and
  Add Reactions. Each overwrite was removed before the next test. Final role values
  are both `8`; every inspected channel has zero overwrites.
- **E14 — Second-user isolation and deletion:** disposable user `kittybi`
  (`kittybri_57868`, ID `1223350178883571846`) created and retrieved synthetic
  memories in both bots. The primary user had zero marker hits and was rejected from
  the disposable user's confirmation button. Both authorized resets completed; the
  disposable user then had zero scoped rows and zero marker hits while every recorded
  primary-user row count remained unchanged. The account remains in the guild.
- **E15 — Chaos and recovery:** Scaramouche created an allowlisted topic receipt;
  Wanderer observed the shared pending receipt and shared cooldown state. A forced
  cross-bot restoration returned the topic to its original value. A later receipt
  remained applied while Scaramouche was stopped, then restored on startup. A newer
  administrator nickname was preserved with result `manual_change_preserved` and was
  explicitly returned to the recorded original. Temporary configs and synthetic
  chaos rows were removed; zero restoration receipts remain pending.

## Findings and repairs

| ID | Severity | Finding | Resolution |
|---|---|---|---|
| ENV-001 | ENVIRONMENT (resolved) | Original services were unversioned legacy flat bundles. | Exact immutable candidates were deployed. |
| ENV-002 | ENVIRONMENT (resolved) | Original bots used different local `shared_state.db` files. | Candidates were co-located on one local shared file. |
| ENV-003 | ENVIRONMENT (resolved) | A second disposable Discord user was initially unavailable. | `kittybi` was used only for the authorized isolation/deletion checks and remains in the guild. |
| ENV-004 | APPROVAL (resolved) | Permission-removal and chaos mutations initially lacked action-time approval. | Owner authorized the exact reversible matrix and cosmetic checks; all changes were restored and verified. |
| ENV-005 | MEDIUM (external) | Both PRs show an unrelated failed Cloudflare Workers account check. | Correct or disconnect that external repository integration. |
| ENV-006 | ENVIRONMENT (resolved) | The first micro host became unreachable under the co-located runtime. | Moved to the prepared existing fallback host; final services remained stable. |
| SCARA-001 | HIGH (fixed) | `MEMORY_DATA_DIR` was ignored, risking split state. | Data-directory resolution fixed and regressed. |
| SCARA-002 | HIGH (fixed) | Non-finite gateway latency could crash a status path. | Non-finite latency is handled safely. |
| SCARA-003 | HIGH (fixed) | Image handling failed when xAI was not configured. | Added Groq vision fallback and deterministic graceful fallback. |
| SCARA-004 | HIGH (fixed) | Topic forget did not explicitly scrub matching scene fields. | Added field-level scene scrubbing while preserving unrelated scene context. |
| WANDERER-001 | HIGH (fixed) | Partner output could steal/extend an owned command interaction. | Partner command-output ownership gate added. |
| WANDERER-002 | HIGH (fixed) | Retired Groq model names could fail at runtime. | Central model resolution maps retired models before provider calls. |
| WANDERER-003 | HIGH (fixed) | Serious-context failure exposed an internal fallback phrase. | Protective paths now use supportive user-facing fallbacks. |
| WANDERER-004 | HIGH (fixed) | Proactive rivalry could persist sensitive source text. | Sensitive messages are excluded before proactive rivalry persistence/generation. |
| WANDERER-005 | HIGH (fixed) | Image handling ignored images when xAI was unavailable. | Added Groq vision fallback and graceful failure behavior. |
| WANDERER-006 | HIGH (fixed) | Topic forget left matching scene context and omitted other prompt sources. | Forget now scrubs messages, reminders, trivia, summaries, conflict fields, milestones, callbacks, memory bank, jokes, topics, shared jokes, and matching scene fields. |

No unresolved BLOCKER or HIGH code defect remains in the exercised Gate A scope.

## Test record

Legend: `S` = Scaramouche, `W` = Wanderer, `B` = both.

| # | Test | Result | Scope | Evidence / observed behavior |
|---:|---|---|---|---|
| 1 | Verify exact commits | LIVE PASS | B | Remote/local heads checked; final `!build` matched full deployed SHAs |
| 2 | Back up persistent state | LIVE PASS | B | Online backups and source DBs returned `quick_check=ok` |
| 3 | Disable unrelated risky features | LIVE PASS | B | Risky integrations, voice, home, companion, and chaos disabled |
| 4 | Confirm diagnostics | LIVE PASS | B | Owner-only build diagnostics excluded paths and secrets |
| 5 | Launch Scaramouche candidate | LIVE PASS | S | Final candidate connected and reached ready |
| 6 | Launch Wanderer candidate | LIVE PASS | W | Final candidate connected and reached ready |
| 7 | Both candidates online | LIVE PASS | B | Both online simultaneously; no message loop |
| 8 | Scaramouche DM | LIVE PASS | S | One brief in-character acknowledgement |
| 9 | Wanderer DM | LIVE PASS | W | One brief in-character acknowledgement |
| 10 | Cross-user isolation | LIVE PASS | B | `kittybi` markers were scoped only to user B; primary-user output and database rows contained no marker |
| 11 | Mention Scaramouche | LIVE PASS | S | Only Scaramouche answered |
| 12 | Mention Wanderer | LIVE PASS | W | Only Wanderer answered |
| 13 | Reply attribution | LIVE PASS | B | Reply to Wanderer produced no Scaramouche response |
| 14 | Plain conversation eligibility | LIVE PASS | B | Unaddressed control text produced no unintended duplicate response |
| 15 | Harmless command per bot | LIVE PASS | B | Help and build commands returned correct owner output |
| 16 | Command plus reply | LIVE PASS | B | Owned command and reply routes did not trigger partner continuation |
| 17 | Command plus attachment | LIVE NOT RUN (non-gating) | B | Browser uploaded the attachment separately; this combination was not required by the authorized remainder |
| 18 | Image attachment | LIVE PASS | B | Both bots accurately described the synthetic image after fixes |
| 19 | Text attachment | LIVE PASS | B | Preview/reaction completed; no crash or error |
| 20 | Unsupported/oversized attachment | TOOLING BLOCKED (non-gating) | B | Browser chooser would not select the custom unsupported extension; ordinary image/text attachment paths passed |
| 21 | Scaramouche serious arbitration | LIVE PASS | S | Calm actionable response; no joke interception |
| 22 | Wanderer serious arbitration | LIVE PASS | W | Patched retest gave supportive user-facing response |
| 23 | Disposable credential guard | LIVE PASS | B | Warning responses only; retest marker absent from all databases |
| 24 | View Channel removed | LIVE PASS | B | Target bot could not fetch/see the channel; overwrite removed and help command recovered before continuation |
| 25 | Send Messages removed | LIVE PASS | B | Discord returned missing-permission behavior without a service crash; overwrite removed and help command recovered |
| 26 | Read Message History removed | LIVE PASS | B | Discord returned HTTP 200 with an empty message list under deny; history returned after restore |
| 27 | Embed Links removed | LIVE PASS | B | Embed send returned HTTP 403 for both targets; each overwrite was removed immediately |
| 28 | Attach Files removed | LIVE PASS | B | Multipart attachment send returned HTTP 403 for both targets; each overwrite was removed immediately |
| 29 | Add Reactions removed | LIVE PASS | B | Reaction add returned HTTP 403 for both targets; one rate-limited setup retry was cleaned up before the passing retry |
| 30 | Gateway/network reconnect | LIVE NOT RUN (non-gating) | B | Host-network interruption was outside the authorized scope; controlled service reconnects passed |
| 31 | Repeated ready event | AUTOMATED PASS / LIVE NOT INDUCED | B | Lifecycle regressions passed; same-process duplicate-ready injection was not necessary for Gate A closure |
| 32 | Restart Scaramouche only | LIVE PASS | S | Only Scaramouche PID changed and reconnected |
| 33 | Restart Wanderer only | LIVE PASS | W | Only Wanderer PID changed and reconnected |
| 34 | Restart both | LIVE PASS | B | Both PIDs changed and both reconnected cleanly |
| 35 | Duo/shared interaction | LIVE PASS | B | `!both` produced exactly one contrasting answer per bot |
| 36 | Bot relationship write | LIVE PASS | B | Expected local/shared rows observed from the duo interaction |
| 37 | Concurrent ordinary activity | LIVE PASS | B | Both processed live activity together; no persistent lock |
| 38 | Seed disposable privacy state | LIVE PASS | B | Unique synthetic memories created and visible |
| 39 | Normal full deletion | LIVE PASS | B | Both user-B reset confirmations completed; primary wrong-user click was rejected; all user-B rows/markers reached zero |
| 40 | Partial deletion failure | AUTOMATED PASS / LIVE NOT INDUCED | B | Retryable coordinator test passed; destructive live fault injection was unnecessary for Gate A closure |
| 41 | Resume deletion | AUTOMATED PASS / LIVE NOT INDUCED | B | Resume-only-unfinished-stage test passed; no live pending job was created |
| 42 | Enable chaos explicitly | LIVE PASS | B | Temporary config enabled only the private guild, `#commands`, and one cosmetic mutation at a time |
| 43 | Reversible cosmetic mutation | LIVE PASS | S | Allowlisted topic/nickname mutations created write-ahead receipts; original values were restored |
| 44 | Restart restoration | LIVE PASS | S | Applied topic remained while Scaramouche was stopped and restored after startup recovery |
| 45 | Newer admin edit protection | LIVE PASS | S | Newer nickname stayed unchanged; receipt ended `superseded/manual_change_preserved` |
| 46 | Interrupted restoration | LIVE PASS | S | Service was inactive with an applied due receipt; restart finalized it as `restored` |
| 47 | Two-bot chaos ownership | LIVE PASS | B | Wanderer reported Scaramouche's shared pending receipt/budget and safely requested cross-bot restoration |
| 48 | Disable chaos and verify clean state | LIVE PASS | B | Original env files restored byte-for-byte; effective configs contain no chaos section, zero chaos rows/pending receipts |
| 49 | Task health after tests | LIVE PASS | B | Both active at final PIDs `494903`/`494904`, bounded memory, `NRestarts=0` |
| 50 | Persistence health after tests | LIVE PASS | B | All three final databases returned `quick_check=ok` |
| 51 | Sanitized log review | LIVE PASS | B | Final logs clean; no final-candidate traceback or lock storm |

## Automated validation

| Validation | Result | Final evidence |
|---|---|---|
| Scaramouche full suite | PASS | 664 passed, 1 skipped, 1 LibreSSL warning |
| Wanderer full suite | PASS | 212 passed, 1 skipped, 1 pytest import-rewrite warning from the reused validation environment |
| Focused Scaramouche message pipeline | PASS | 28 passed |
| Focused Wanderer release hardening | PASS | 12 passed before the privacy addition; new privacy regression also passed in the full suite |
| Compile checks | PASS | `bot.py` and `memory.py` compiled in both repositories |
| Diff checks | PASS | `git diff --check` clean in both repositories |
| Cross-process shared SQLite | PASS | 500 composite operations, zero lock errors |
| Accelerated soak | PASS | 600 turns; healthy databases and bounded state |

## Cleanup and safety state

- Synthetic fake-credential, disposable-memory, and duo markers were removed from
  the staging databases after verification.
- The second disposable user's two synthetic memory markers have zero hits in every
  relevant table across all three databases. User-B scoped rows are zero; the
  primary user's recorded counts remain exactly `1/13/3/2/1` in Scaramouche and
  `1/17/4/3/1` in Wanderer for users/messages/topics/memory/preferences.
- All three databases passed integrity checks after cleanup.
- No real password, token, or private credential was placed in Discord test text.
- Both bot roles are restored to permission value `8`. The four inspected channels
  have `topic=None`, slowmode `0`, and zero overwrites. Scaramouche's nickname is
  restored to `None`.
- The temporary server-chaos configs were restored to their original byte hashes,
  all nine synthetic chaos state/audit rows were removed, and the final shared
  database contains zero `chaos:%` rows and zero pending restoration receipts.
- No paid cloud resource was created, and no PR was merged.
- Voice Gate B and real integrations remain out of scope.

## Service restart accounting

- Final unexpected/automatic systemd restart count: Scaramouche `NRestarts=0`,
  Wanderer `NRestarts=0`.
- The earlier restart matrix exercised Scaramouche-only, Wanderer-only, and dual
  restarts.
- This authorized permission/chaos continuation added five controlled Scaramouche
  restarts and three controlled Wanderer restarts: temporary config activation,
  duration/config refresh, Scaramouche interruption/startup recovery, nickname-test
  config refresh, and final original-config restoration as applicable.

## Release decision

The exercised candidates satisfy **Live Staging Gate A**. The only unperformed
items are non-gating environment/tooling combinations (combined command+attachment,
unsupported-extension selection, deliberate host-network interruption, and a
same-process duplicate-ready injection). They did not expose or leave an unresolved
BLOCKER/HIGH defect, and their covered automated/runtime equivalents passed.

**Final state: `STAGING_GATE_A_PASS`**

## Gate B — Live Voice

Checkpoint: **NOT_READY**, 2026-10-02 04:00 UTC (October 1 Pacific).
Gate A remains passed. Gate B is incomplete and the reported missing second
spoken reply has not yet been diagnosed or passed on a live retest.

### Candidates and deployment

| Bot | Live SHA | Release path |
|---|---|---|
| Scaramouche | `fb90d45b874e8a667e5a0be287139063fc4ef76c` | `/opt/scara-wanderer-staging-fallback/releases/scaramouche-fb90d45` |
| Wanderer | `0652e0110cf52e9f6ab10ed561da61fc685fc3af` | `/opt/scara-wanderer-staging-fallback/releases/wanderer-0652e01` |

Both remain on `release/full-system-hardening`, with unmerged PRs #22 and #9.
Deployment at 03:59 UTC used new immutable release directories and scoped systemd
drop-ins. Previous release trees/configuration remain available for rollback.
Both bots logged ready at 03:59:31 UTC. PIDs: 699000 / 699009; both active,
both `NRestarts=0`. This continuation added one controlled restart per service.
SQLite read-only `quick_check` returned `ok` for both bot databases and shared
state. Available RAM was 374 MiB; swap used 168 MiB after startup.

### Preflight and existing live evidence

Approved guild `1486228108070617108`, General VC `1486228109027180598` only.
Primary user `563157483196121109`; disposable Kittybi `1223350178883571846`.
Raw audio and transcript debug logging remain disabled; optional VC gimmicks
remain off. Consent is per user and must be renewed after restart/rejoin.

The earlier session used Scaramouche `edd4cd8d07e661532a709dd7002ba4ae41cb1064`
and Wanderer `96a9f3197961704c46393a6a9cc6e6c939b1888d`.

- **LIVE PASS — dependency preflight:** discord.py 2.7.1, davey 0.1.6,
  discord-ext-voice-recv 0.5.3a181 from audited commit
  `bec048127f4148fd147afa3182c3771b6955dc08`, webrtcvad-wheels 2.0.14,
  PyNaCl 1.5.0, real Opus and FFmpeg. Groq and Fish configuration present.
  The receiver dependency guard/imports passed again in both new release trees.
- **LIVE PASS — initial Scaramouche receive/playback:** successful consenting
  primary-user speech, DAVE/PCM counters, STT completion events, Fish HTTP 200,
  successful ffmpeg completion and audible reply reported by the user.
  At 22:25:26 UTC: 3,042 packets, 2,263 DAVE frames, 2,130 PCM frames,
  25 STT successes, five spoken chunks, queue zero.
- **BLOCKED — quiet-speech outcome:** cumulative STT increased by the 22:31
  diagnostic, but spoken chunks did not. No per-turn transcript or routing
  evidence established that the intended quiet phrase was successfully
  recognized. This is not a quiet-speech pass.
- **LIVE PASS — second-user consent command:** Kittybi sent `!voice listen on`
  in general at 22:56:00 UTC and received the consent acknowledgement.
  The user reported hearing a reply to “Scaramouche can you hear me”. Fish
  returned HTTP 200 at 22:56:33; ffmpeg completed at 22:56:38.
  Per-speaker attribution for that reply still requires a fresh diagnostic;
  the latest saved owner report predates Kittybi's consent.
- **LIVE FAIL — follow-up reply (B-02, HIGH triage pending):** user reported no
  answer to “Scaramouche, do you like ice cream?” after the initial reply.
  There is insufficient evidence to distinguish receive/VAD, transcription,
  addressing, or response-generation suppression. Do not claim this fixed.
- **LIVE PASS — leave observed:** primary sent `!leave` at 22:59:08 UTC;
  Discord logged voice handshake termination at 22:59:19. Full worker/buffer
  cleanup proof and rejoin remain to be tested on the new candidates.

### Scoped repairs and regression results

- **B-01 — shared start/stop controls:** unqualified `!voice start` joined both
  bots during initial testing. Added optional character targets to start/join
  and stop/leave. `!voice start scaramouche` is consumed silently by Wanderer;
  the existing unqualified command remains compatible. Regression tests pass;
  targeted live retest remains pending.
- **B-02 diagnostic gap:** added bounded category counters/events for empty
  transcription, not-addressed rejection, echo/stale drops, response scheduling,
  empty response, and playback delivery. Owner diagnostics expose both worker
  liveness states and up to 16 recent events. No audio or transcript text is
  included. These diagnostics do not change addressing or response behavior.
- Complete suites: **Scaramouche 669 passed, 1 skipped** (existing LibreSSL
  warning); **Wanderer 217 passed, 1 skipped**. Focused receive/integration
  suites: **48 passed, 1 skipped each**. Compile/import and diff checks pass.
  Targeted added-code scans found no Groq/API/private-key patterns; this was
  a scoped pattern scan, not a comprehensive external secret scanner.

### Remaining live checks / exact resume point

Start only Scaramouche using `!voice start scaramouche` while in General.
If Kittybi did not start the session, Kittybi must send `!voice listen on`.
Repeat the two short questions, waiting for playback completion between them.
The primary owner then sends the correctly spelled `!voice diagnostics` before
any stop/restart. Inspect the new decision events, worker health and speaker IDs.

Still pending: B-02 reproduction/fix/retest; targeted command live retest;
two-user attribution and consent revocation; quiet/fast/pause/noise checks with
correlated evidence; keyword/natural/off interruption; stale-output suppression;
five-turn conversation; essential Wanderer voice tests; both-bot filtering,
handoff and ten-minute coexistence; clean reconnect/process restart; controlled
provider failures where safely possible; delivered-memory verification; five
correlated latency samples including interruption. Earlier latency counters
measure internal stages, not end-to-end speech-end timing, and must not be
presented as such. Gate C has not begun.

## 2026-10-02 continuation — voice follow-up routing and trolling defaults

This section supersedes the preceding B-02 investigation status without repeating
Gate A checks. Gate B remains open; Gate C has not begun.

### Recovered live evidence and previous deployment

The owner's correctly spelled `!voice diagnostics` report at
2026-10-02 04:08:13.885 UTC showed `DIRECT_ONLY`, both receive/transcription workers
running and neither failed, four STT successes, one response scheduled, three
`discarded_not_addressed`, and one completed playback. This identifies addressing
rejection after recognition, not a dead receiver, in that session. No raw
transcript was retained, so the precise recognized spelling is unknown.
Targeted Scaramouche-only startup was observed at 04:02:26 UTC.

At 04:18:58–59 UTC, the preceding continuation deployed:

- Scaramouche `a2e066a1f534fe3a4703cdd01de71ff72c004efd`.
- Wanderer `4b6139b26504c0250ac7bfb4be6ad1aa4bddefab`.

These candidates default to CONVERSATION, retaining focused replies for 90 seconds
after playback; explicit addressing of the partner releases focus. They also add
bounded arrival greetings and diagnostic spelling aliases. Services logged ready
at 04:19:04 / 04:19:06 UTC; PIDs 705004 / 705012; NRestarts 0 for both. All three
database quick checks were OK. Suites then: Scaramouche 674 passed, 1 skipped;
Wanderer 222 passed, 1 skipped. Human follow-up/arrival retest is still pending.

### Requested consent correction

The latest attachment explicitly supersedes the previous automatic-listening
request: both bots now default `VOICE_AUTO_LISTEN=0`. Session starters opt in by
starting; other participants use `!voice listen on`. A newly consenting participant
gets a bounded greeting; repeated listen-on commands do not queue extra greetings.
The conversation/follow-up routing fix remains intact.

Scaramouche's light typing/judge/edit preferences default ON only within the
existing configured guild/channel framework. Explicit OFF survives old expiry,
restart and 30-day record pruning. Both bots' shared-record pruners preserve these
preferences. `!pranks off` persists OFF rather than erasing it; explicit privacy
forget still erases the preference. Parody remains explicit, expiring opt-in;
light preference updates do not renew it. Owner-only phantom ping skips target
consent but retains authority, guild/channel, quiet-hour, budget and durable
own-message cleanup checks. Other heavy-feature consent and existing rates are
unchanged. No proactive/DM default changes, model calls, nickname mutator, or
relationship/anti-repeat changes were added. The new nickname preference is
reserved because this build has no user-nickname prank.

Final automated results: Scaramouche **679 passed, 1 skipped** (114.33s, existing
LibreSSL warning); Wanderer **224 passed, 1 skipped** (53.17s). The skipped test is
local real-Opus codec roundtrip without VOICE_OPUS_LIBRARY, not a live voice pass.
The focused trolling/chaos/voice run passed 147 tests with 1 skipped before the
additional pruning regression, which passed separately and in both final suites.
Offline imports registered 132 / 141 commands and shut down successfully with
dotenv disabled, an empty environment, temporary working directories, and network
connections blocked. Compile and diff checks pass. Scoped added-code secret-pattern
scans found no matches; this is not a comprehensive external secret audit.
No live prank or cosmetic guild mutation is needed for this patch, and none has
been executed.

### Verified deployment and exact resume point

Deployed 2026-10-02 09:25:12 UTC using new immutable release trees and backed-up
systemd drop-ins. Both candidates remain on `release/full-system-hardening`;
existing PRs #22 / #9 remain open and unmerged.

| Bot | Exact live code SHA | Release directory |
| --- | --- | --- |
| Scaramouche | `2ae3c70aba3319c472d6d3f8e7c0735e29b4ebe7` | `/opt/scara-wanderer-staging-fallback/releases/scaramouche-2ae3c70` |
| Wanderer | `eb9f45e827581338f97da38b4a709c81d9f472f0` | `/opt/scara-wanderer-staging-fallback/releases/wanderer-eb9f45e` |

Both logged online at 09:25:18 UTC. Scaramouche PID 798214; Wanderer PID 798222.
Both services active/running, `NRestarts=0`. This continuation intentionally
restarted each service once; that is distinct from systemd automatic restarts.
No ERROR/Traceback/initialization-failed entries appeared in the scoped startup
log check. Both release trees passed pinned receive dependency/import checks.
Read-only SQLite `quick_check` returned `ok` for scaramouche.db, wanderer.db and
shared_state.db both before and after deployment. Available RAM 363 MiB; swap
157 MiB. Neither environment overrides `VOICE_AUTO_LISTEN`, so default 0 applies.
Guild chaos/trolling configuration remains disabled, unchanged from the approved
staging state. No account, cloud integration, device, moderation or cosmetic
server operation was performed.

Next live action: in approved General VC, send `!voice start scaramouche`.
Anyone other than the session starter must send `!voice listen on`.
Ask “Scaramouche, can you hear me?”, wait for playback to finish, then ask
“Do you like ice cream?” within 90 seconds. The second question deliberately
tests conversation focus without requiring another exact name match. The owner
then sends `!voice diagnostics` before stopping/restarting. Check response
scheduling/playback counters and speaker attribution. Test a newly consenting
arrival greeting separately. Do not mark B-02 or Gate B passed until this live
retest succeeds. All other previously pending Gate B checks remain pending;
Gate A has not been rerun and Gate C has not begun.

## 2026-10-02 — light personality independent of heavy-server controls

Explicit subsequent user authorization removes deployment flags, channel allowlists
and runtime chaos-switch requirements only for typing teases, Silent Judge and
self-message edits. Public text/access, personal preferences, quiet hours, muted
users, serious-context arbitration, active-session exclusion and existing shared
budgets/cooldowns remain enforced. Delayed light edits track personal preference
changes, not heavy-control revisions. Heavy/manual features still require configured
guild/channel approval and their existing personal consent rules.

Validation: targeted trolling/chaos/arbitration **129 passed**; complete Scaramouche
suite **681 passed, 1 skipped** in 138.88s (existing LibreSSL warning; local Opus
skip). Offline startup registered 132 commands and shut down with network blocked.
Compile/diff and scoped added-code secret-pattern checks passed. Wanderer code did
not change in this continuation; its prior 224-pass result is not a fresh rerun.

Deployed Scaramouche `809cea5cef470fcfc4a0698884f44e0bab1abe38` at
2026-10-02 14:31:32 UTC. Wanderer remains
`eb9f45e827581338f97da38b4a709c81d9f472f0`. Each service intentionally restarted once
to load configuration (PIDs 891186 / 891193; NRestarts 0). Release imports/pinned
voice dependencies passed, and all three database quick checks returned `ok`.

The user also explicitly authorized enabling staging heavy features. This
**supersedes the previous intentionally disabled configuration**:

- Guild: `1486228108070617108` only.
- Heavy text/game channels: general `1486228109027180597`, off-topic
  `1486241505021526026`, commands `1497291858643259523`; verified via Discord API.
- Advanced voice/game configuration: General `1486228109027180598` only for VC;
  the same three approved text channels for game invitations.
- Both bots' effective configuration and shared runtime control are enabled.
  Scaramouche parody/muzzle/gossip/ping/court and owner performances are enabled;
  Wanderer retains its fair-wager/court-defense/interference role.
- Parody and sovereign scheduling stay MANUAL. No sovereign mutation template was
  invented; sovereign remains disabled. Personal consent, microphone opt-in,
  permissions, invitation acceptance and all cooldowns are unchanged.
- No prank, cosmetic mutation, voice-game execution, user movement, or personal
  consent write was performed. Enabling configuration is not a live gameplay pass.

Original effective heavy/advanced configuration was empty and shared control was
absent. There were zero active intent/applied/pending/claimed chaos records before
enablement. Original-state evidence and the previous Scaramouche release drop-in
are retained in `/opt/scara-wanderer-staging-fallback/config-backups/light-trolling-heavy-staging`.
New non-secret integration JSON lives under
`/opt/scara-wanderer-staging-fallback/config/light-trolling-heavy-staging`, selected
by each service's `50-staging-chaos.conf`. No secret environment file or unrelated
integration was changed. The voice follow-up live retest above remains pending.

## 2026-10-05 — Unrestricted mode rename

Renamed the former mature-content mode to **Unrestricted** across both bots:
Discord command (`!unrestricted on/off`), help entries, runtime keys, prompt labels,
logging names, privacy/terms/search-safety copy, and persisted schema. The previous
command name is not retained as an alias. Scaramouche still permits enabling the
mode only in a DM or Discord age-restricted channel; this rename does not weaken
the existing age/channel policy. Wanderer's behavior is otherwise unchanged.

Migration version 4 on Scaramouche and Wanderer's idempotent initializer preserve
the former boolean value as `unrestricted_mode`, then remove the obsolete column.
The server's Wanderer runtime uses SQLite 3.34.1, so a first copy-only preflight
correctly rejected unsupported `ALTER TABLE ... DROP COLUMN`; no drop-in, service,
or live database had changed at that point. A transaction-safe table rebuild was
then added and regression-tested for older SQLite. It preserves column types,
defaults, primary keys, data, and any explicit indexes/triggers.

Final automated validation:

- Scaramouche: **682 passed, 1 skipped** in 114.49s; existing LibreSSL warning.
- Wanderer: **225 passed, 1 skipped** in 48.93s.
- Focused migration/runtime validation: Scaramouche 30 passed; Wanderer 41 passed.
- Both offline startups registered `unrestricted`, rejected the prior command
  name, and shut down with network access blocked (132 / 141 commands).
- Compile, diff, scoped added-code secret scan, and full repository old-name scan
  passed. The only skips remain the optional local real-Opus roundtrip checks.

Before live migration, SQLite backups of both local databases and shared state were
saved under
`/opt/scara-wanderer-staging-fallback/config-backups/unrestricted-rename-b08b2b2`.
The migration then passed against disposable copies of the actual live databases
(Scaramouche 3 users; Wanderer 2 users) before either service restarted. Live
post-migration comparison confirmed every user/value matched its corresponding
backup; both old columns were absent; all three `quick_check` results were `ok`.

Deployed at 2026-10-06 04:34 UTC (2026-10-05 21:34 America/Los_Angeles):

| Bot | Exact live code SHA | Release directory |
| --- | --- | --- |
| Scaramouche | `b08b2b2bb8bc8151490551b7175c0eddc1300296` | `/opt/scara-wanderer-staging-fallback/releases/scaramouche-b08b2b2` |
| Wanderer | `800f9545ec3cdcd113f0d7b20e4c83a474cdd12c` | `/opt/scara-wanderer-staging-fallback/releases/wanderer-800f954` |

Both logged online after migration (PIDs 2457142 / 2457150), are active/running,
and report `NRestarts=0`. Scoped startup logs contained zero ERROR/Traceback/
initialization-failed entries. Available RAM was 363 MiB and swap use 147 MiB.
Each service was intentionally restarted once. Heavy staging configuration from
the preceding section remains active and unchanged. No Discord message, preference,
prank, voice game, server cosmetic, moderation action, or cloud/device action was
performed for this rename. The voice follow-up live retest remains pending.

### 2026-10-05 — Non-destructive migration hardening

A follow-up aligned the compatibility migration with the final safety requirement:
future legacy databases now gain `unrestricted_mode`, copy the existing boolean
exactly once, and retain the retired column as unused compatibility data. No SQLite
table rebuild or column deletion is performed. Scaramouche uses its transactional
version-4 migration marker; Wanderer now has a dedicated transactional preference-
name marker so a later `!unrestricted` change cannot be overwritten on restart.
Fresh databases contain only `unrestricted_mode`.

The earlier live migration had already safely removed the retired columns after
verified backups, so the active databases did not need another data conversion.
The final migration was instead preflighted twice against disposable copies of the
preserved pre-rename databases. Both enabled and disabled values were preserved,
the compatibility column remained present and unused, later user changes survived
the second initializer run, and each migration marker remained unique.

Final repository validation:

- Scaramouche: **686 passed, 1 skipped** in 141.86s; focused suite **34 passed**.
- Wanderer: **230 passed, 1 skipped** in 43.51s; focused suite **46 passed**.
- Modified-file compilation and diff checks passed in both repositories.
- Case-insensitive tracked-file searches found zero occurrences of the retired
  terminology in either repository. Compatibility detection is assembled only
  inside the two narrowly scoped migrations.
- The deprecated command alias was deliberately not retained; only
  `!unrestricted [on/off]` is registered.

Fresh backups of both live local databases and shared state are stored under
`/opt/scara-wanderer-staging-fallback/config-backups/unrestricted-final-44a2fe3`.
All user values matched those backups after restart, both one-time migration markers
were present, and `PRAGMA quick_check` returned `ok` for all three databases.

Final deployment at 2026-10-06 04:54 UTC
(2026-10-05 21:54 America/Los_Angeles):

| Bot | Exact live code SHA | Release directory |
| --- | --- | --- |
| Scaramouche | `44a2fe33428f1116342ff43c3c6d9cba7e13638f` | `/opt/scara-wanderer-staging-fallback/releases/scaramouche-44a2fe3` |
| Wanderer | `53138641356a61e0e146346385aac9befb9562a3` | `/opt/scara-wanderer-staging-fallback/releases/wanderer-5313864` |

Both services are active/running (PIDs 2463361 / 2463369) with `NRestarts=0`.
The immediate Wanderer post-restart verifier initially ran before asynchronous
database initialization completed; after the bot logged online, the same verifier
passed. Startup logs contain zero ERROR/Traceback/initialization-failed matches.
Each service was intentionally restarted once. Heavy staging configuration and all
unrelated behavior remain unchanged.

## 2026-10-06 — Gate B live voice continuation

Gate A was not repeated. Gate C, Google Connect, cloud-account setup, physical-
device actions and new feature work were not started. The exact live candidates
remain Scaramouche `44a2fe33428f1116342ff43c3c6d9cba7e13638f` and Wanderer
`53138641356a61e0e146346385aac9befb9562a3`.

### B-02 focused follow-up retest — PASS

In the approved General staging VC, the primary human started Scaramouche in
CONVERSATION targeting mode. They asked “Scaramouche, can you hear me?”, waited
for the audible reply, then asked “Do you like ice cream?” without repeating the
name. The human confirmed both replies were heard completely. This demonstrates
that the 90-second conversation focus retained ownership of the unnamed follow-up.

The immediate sanitized diagnostic reported connected/listening/receive-proven,
one participant, one focused user, healthy receive and transcription workers, no
pending audio, 451 received packets, 378 authenticated DAVE frames, 367 PCM
frames, five VAD segments, five successful STT results, five scheduled responses,
four completed playbacks and four spoken chunks. Every recorded STT, scheduling
and completed-playback event was attributed to primary human Discord user
`563157483196121109`; no bot speaker ID appeared. Observed latency samples were
1536 ms STT, 906 ms response generation, 1432 ms TTS and 2338 ms first audio.

The human confirmed that all five STT results represented five intentional human
utterances. The fifth response was scheduled after the diagnostic snapshot and its
FFmpeg process was stopped when the human left the page. The human confirmed the
departure was intentional. The voice pipeline records memory only after a
successful `playback_completed` result, so this cancelled fifth playback was not
classified as delivered. The four preceding FFmpeg processes all exited with code
0. No service crash or automatic restart occurred (`NRestarts=0`). This is not an
unresolved B-02 defect.

Gate B remains in progress. The next uncompleted live block is sustained
Scaramouche conversation followed by keyword, natural and disabled-interruption
checks. Wanderer receive/playback and the remaining two-participant/two-bot,
reconnect, failure-injection, stability and resource checks remain pending.

### 2026-10-06 — automatic live-voice membership requirement

During the keyword-interruption setup, the preserved diagnostic isolated a silent
turn to `participants=0`: receive/DAVE/PCM remained active and both workers were
healthy, but leaving the VC had correctly revoked the former per-session consent
and rejoining did not restore it without `!voice listen on`. No Groq, Fish or
playback call occurred for the ignored turn.

The user then explicitly superseded the earlier per-session opt-in requirement.
Both bots now include eligible humans in an active allowlisted VC automatically,
including people present at startup and humans who leave and later rejoin. The
`!voice listen on/off` command path and `VOICE_AUTO_LISTEN` switch were removed.
The internal participant set remains only as a bounded routing mechanism. Bots,
people outside the active VC, people beyond the configured participant limit and
users whose persistent `!voice off` preference is set are still excluded. The
visible disclosure that speech is sent to Groq remains mandatory, raw audio is
still not saved, and leaving immediately discards queued audio and temporary
speaker state. Party-game consent remains independent and unchanged.

Focused voice validation passed **99 tests with 1 skipped** in each repository.
Complete suites passed Scaramouche **685 passed, 1 skipped** in 154.97 seconds and
Wanderer **229 passed, 1 skipped** in 62.67 seconds. The warning is the existing
local LibreSSL warning; the skip remains the optional local real-Opus roundtrip.
Compilation and diff checks passed. Live redeployment and the interrupted Gate B
scenario remain pending at this checkpoint.
