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

The next user-directed refinement makes every automatic arrival greeting include
the human's sanitized display name. A later leave/rejoin during the same live bot
session uses a separate return pool (for example, “Look who came back”) rather
than the first-arrival pool. Leaving cancels any still-pending greeting task, so a
real rejoin can receive its return line while duplicate Discord events remain
suppressed. Initial humans present when the bot starts are marked as already seen,
so their next arrival is correctly treated as a return. Both focused voice suites
again passed **99 tests with 1 skipped**. Complete suites passed Scaramouche
**685 passed, 1 skipped** in 123.58 seconds and Wanderer **229 passed, 1 skipped**
in 54.28 seconds. Compilation and diff checks passed; live deployment evidence
for this refinement follows after completion.

Final automatic-membership + named-arrival deployment at 2026-10-06 16:34 UTC:

| Bot | Exact live code SHA | Release directory |
| --- | --- | --- |
| Scaramouche | `6134aee740dff3c1031be046ce1c5506fcf8905d` | `/opt/scara-wanderer-staging-fallback/releases/scaramouche-6134aee` |
| Wanderer | `c9056927bae76b64c77a51250727891f6e12a9e7` | `/opt/scara-wanderer-staging-fallback/releases/wanderer-c905692` |

Pinned receive-dependency and release-import checks passed before activation.
Pre-deployment SQLite backups and the previous voice drop-ins are stored under
`/opt/scara-wanderer-staging-fallback/config-backups/voice-arrival-greetings-20261006`.
Post-restart `PRAGMA quick_check` returned `ok` for Scaramouche, Wanderer and
shared state. Scaramouche PID 2680074 and Wanderer PID 2680082 are active/running;
both report `NRestarts=0`. The two implementation rounds in this continuation
intentionally restarted each service twice; no automatic restart occurred.
Both bots logged online, and the scoped startup log contains zero ERROR,
Traceback or initialization-failed matches. Host memory was 951 MiB total with
346 MiB available; swap use remained 148 MiB of 3062 MiB. Heavy staging settings,
Unrestricted migration state and unrelated integrations were unchanged.

The interrupted keyword-interruption live scenario must be restarted on these
exact candidates. Named arrival and same-session return lines require live human
confirmation before they are marked passed. Gate B remains in progress.

### Named arrival and same-session return — PASS

On the exact candidates above, the primary human started Scaramouche in the
approved General staging VC. The public status reported automatic listening and
one participant; no per-session enrollment command was used. When disposable
staging user `kittybi` joined, the user audibly confirmed Scaramouche said
“kittybi, what do you want?” When the same user left, waited, and rejoined while
Scaramouche remained connected, the user audibly confirmed the distinct return
line “kittybi again? You just left. What is it this time?”

This passes first-arrival name inclusion, automatic membership, same-session
arrival history, and separate rejoin wording. The disposable user remained in
the guild and no unrelated state was changed. Gate B remains in progress; the
next unfinished block is the restarted keyword, natural and interruption-off
sequence on Scaramouche, followed by the still-pending Wanderer and two-bot
voice checks.

### Keyword interruption defect and repaired candidates — LIVE RETEST PENDING

The first keyword-interruption attempt on Scaramouche failed: while a long reply
was playing, the disposable staging speaker repeatedly said a form of
“Scaramouche, stop,” but playback continued. The immediate sanitized diagnostic
showed connected/listening/receive-proven, `KEYWORD` mode, two participants, one
focused speaker, healthy receive/transcribe workers, seven successful STT
results and multiple STT completions interleaved with completed playback. It
contained zero `voice_interrupted` events and zero cancelled responses. This
showed that no successful cancellation was recorded. The exact, start-anchored
keyword matcher did not tolerate realistic leading words, split names or minor
Groq spelling variation, and was hardened accordingly. The original diagnostic
does not contain transcripts, so it cannot prove that spelling variation caused
that particular live failure. No transcript or raw audio was logged.

Both bots now use a bounded spoken-command matcher. It accepts stop/wait/no/
listen and the two-word hold-on/shut-up commands within a short opening window,
preserves direct character-name barge-in, and tolerates split or slightly
misspelled character names. It does not use substring matching (for example,
“stopping” is not “stop”). Sanitized counters now distinguish a detected
keyword, an attempted cancellation, a missed keyword while busy and a command
that arrived only after playback ended.

Focused voice suites passed Scaramouche **90 passed, 1 skipped** and Wanderer
**93 passed, 1 skipped**. Complete suites passed Scaramouche **695 passed,
1 skipped** in 137.63 seconds and Wanderer **239 passed, 1 skipped** in 56.58
seconds. The skip remains the optional local real-Opus roundtrip; the only
warning remains the existing local LibreSSL warning.

The exact repaired candidates were deployed at 2026-10-06 17:08 UTC:

| Bot | Exact live code SHA | Release directory |
| --- | --- | --- |
| Scaramouche | `25d4a5af78de5714ad7a851d224a025c731b8ad2` | `/opt/scara-wanderer-staging-fallback/releases/scaramouche-25d4a5a` |
| Wanderer | `5fa2025ed5c30ed0e5ed3ec1a5ed4a429eb0051e` | `/opt/scara-wanderer-staging-fallback/releases/wanderer-5fa2025` |

Compilation and release-import checks passed before activation. Pre-restart
SQLite and voice drop-in backups are under
`/opt/scara-wanderer-staging-fallback/config-backups/voice-interruption-20261006`.
Both services are active/running with `NRestarts=0`; startup reported both bots
online and zero ERROR/Traceback/initialization-failed matches. `PRAGMA
quick_check` returned `ok` for Scaramouche, Wanderer and shared state. Host memory
was 951 MiB total with 335 MiB available; swap remained 148 MiB of 3062 MiB.

The failed keyword scenario must now be repeated once on Scaramouche. Do not
mark keyword interruption passed until audible stop behavior and the new
sanitized cancellation counters both confirm it. Gate B remains in progress.

### Subsequent silent-turn investigation — receive/playback observed

After starting the repaired Scaramouche candidate at 17:11 UTC, the human
reported no reply. A 17:12 UTC diagnostic showed one enrolled/focused participant
and healthy workers but empty receive metrics/events. This snapshot cannot
distinguish absence of incoming audio from pre-eligibility packet drops. The
bot's receiver currently counts packets only after speaker eligibility checks.

After the requested spoken “Scaramouche, can you hear me?” probe, a later
diagnostic from the same process/session showed 165 eligible packets, 110 DAVE
frames, 102 PCM frames, two STT successes, two scheduled responses, and five
completed playback chunks. All events were attributed to the primary staging
user `563157483196121109`. Both workers remained healthy and listening. One
keyword detection arrived after playback had finished; no live cancellation has
yet been demonstrated. The human subsequently confirmed the reply to the probe
was audibly heard. Receive, transcription, scheduling and audible playback are
therefore observed in this session; the preceding silent interval's cause is
still unproven.

Additional receiver diagnostics are prepared in both repositories:
callback/RTP counts, unknown/ineligible-speaker drops, listener failure state,
mapped participant counts and DAVE readiness. They report neither payloads nor
keys. The affected receive/session/integration suites each passed **66 passed,
1 skipped**. These diagnostics are not deployed at this checkpoint; the live
code remains `25d4a5a` / `5fa2025` and the current voice session is preserved.

### Short speech segmentation repair — live confirmation pending

At 10:20 AM PDT the primary user reported another unanswered “Scaramouche, are
you there?” The same-session diagnostic advanced from 165 to 202 eligible
packets, 110 to 131 DAVE frames and 102 to 121 PCM frames, but STT and VAD
utterance totals remained two. Thus this interval produced no completed speech
segment; it was not an LLM/TTS request failure. These counters cannot establish
the content of the received frames or exclude upstream microphone clipping.

Inspection reproduced a segmenter defect: three 100ms speech fragments separated
by 60ms VAD-negative gaps supplied 300ms total speech yet produced zero
utterances. The early-onset rule repeatedly deleted each fragment before it
could reach the 120ms start threshold. Both bots now retain the bounded candidate
until ordinary end-of-speech timeout. Minimum speech duration, maximum buffer
size, per-user separation and consecutive-speech barge-in rules remain enforced.
Regression tests verify fragmented speech survives while short isolated noises
are still rejected. New VAD counters report speech/non-speech frames and dropped
short candidates without storing audio or transcripts. The previously prepared
receiver diagnostics are included in this deployment.

Complete validation: Scaramouche **700 passed, 1 skipped** in 162.51s;
Wanderer **244 passed, 1 skipped** in 65.81s. The optional Opus skip and existing
LibreSSL warning remain. One preliminary timing-sensitive focused test run was
interrupted after hanging; the later complete suites finished successfully.

Deployed at **10:27 AM PDT, October 6, 2026**:

| Bot | Exact live code SHA |
| --- | --- |
| Scaramouche | `5daea6e5617167bf7ae94b22d3cfca2e5ea8e3c5` |
| Wanderer | `efab40fbebe8790268782ea3f0ef855bb557c625` |

Release imports, pinned receive-dependency checks and compilation passed before
activation. SQLite online backups and previous drop-ins are retained under
`/opt/scara-wanderer-staging-fallback/config-backups/voice-segmentation-5daea6e`.
All three live databases returned `quick_check=ok`. Both services logged online,
with PIDs 2701258 / 2701259 and `NRestarts=0`; each was intentionally restarted
once. No startup ERROR/Traceback/initialization-failed entry was observed. Host
available RAM was 342 MiB of 951 MiB; swap use was 148 MiB of 3062 MiB.

The reproduced defect is fixed in code, but the reported silent phrase still
requires a live retry on this candidate. Keyword interruption and the remainder
of Gate B are not marked passed. No Gate A checks were repeated and Gate C has
not begun.

### Continued silent session — no incoming RTP observed (October 6)

The human reported another unanswered “Scaramouche, are you there?” after
approximately 28 minutes connected. The **10:57 AM PDT** owner diagnostic on
live Scaramouche `5daea6e5617167bf7ae94b22d3cfca2e5ea8e3c5` showed:

- `udp_callbacks=1728`, `non_audio_datagrams=1728`; no RTP, DAVE, PCM, VAD,
  transcription, response, or playback events counted in this session.
- Connected/listening, healthy receive and transcribe workers, DAVE ready,
  two mapped speakers and one mapped/enrolled participant; no reader failure.
- Service journal contained the successful 17:28 UTC voice connection but no
  subsequent speech-provider requests in the inspected interval.

This failure precedes speech segmentation and Groq. It does **not** prove the
previous segmenter repair resolved the reported silence, nor distinguish a
client microphone/transmission problem from Discord delivery to the receiver.
Read-only Discord inspection showed Mute and Deafen unchecked, the system
default internal microphone selected, and input volume 100%. No audio settings
were changed, no participant was disconnected, and no service was restarted.
Requested a five-second human speech probe with confirmation of Discord's
green speaking indicator to isolate the next layer. Gate B remains blocked on
live receive reliability; no additional live scenario is marked passed.

### Receive startup registration repair — candidate validation

The human confirmed Discord's green speaking indicator appears. This confirms
client-side voice activity, not end-to-end RTP delivery. Source inspection found
neither our receive startup nor the pinned receiver/discord.py connect path
sends an initial speaking-state update. Discord's API issue
<https://github.com/discord/discord-api-docs/issues/808> documents receive-only
connections not receiving media until their SSRC/speaking state is registered,
including a successful silent (`speaking=0`) update. This is a strong candidate
for the current zero-RTP session, not yet a live-confirmed root cause.

Both bots now register `SpeakingState.none` after attaching the receiver. This
sends control signaling only, not audio, and leaves DAVE decryption requirements
unchanged. Startup failure/cancellation stops the attached receiver rather than
leaving a half-started listener. The sanitized `receive_handshake_sent` counter
confirms successful signaling without claiming media receipt.

Two new regression cases failed before the repair and pass afterward: silent
registration after listener attachment and listener cleanup on signaling
failure. Scaramouche's focused voice suites passed **70 passed, 1 skipped**.
Full-suite validation and deployment results follow; a fresh connection must
still receive and answer human speech before this incident is marked resolved.

Full validation completed: Scaramouche **702 passed, 1 skipped, 1 warning** in
216.80s; Wanderer **246 passed, 1 skipped, 1 warning** in 77.33s. The skip is
optional Opus and the warning is local LibreSSL. The initial Wanderer `tests/`
run alone passed 230 tests; the subsequent complete discovery includes its
16 root-level tests. A mistaken explicit root-test invocation in Scaramouche
collected no tests; its full suite above completed successfully.

Deployed at approximately **11:10 AM PDT, October 6, 2026**:

| Bot | Exact code SHA / release PR head at deployment |
| --- | --- |
| Scaramouche | `ea8e43a9e9d59e2a8217ca16ace058f848ed2361` |
| Wanderer | `2716d464e31265e9f200b6174f94462ab310538e` |

Both existing release branches were pushed, with no PR merge. Release imports,
compilation and pinned dependency checks passed. SQLite online backups and
previous service drop-ins are preserved under
`/opt/scara-wanderer-staging-fallback/config-backups/voice-segmentation-ea8e43a`.
All three databases returned `quick_check=ok`. Each service was deliberately
restarted once; PIDs are 2719372 / 2719373, active/running, `NRestarts=0`.
Both logged online with no startup ERROR/Traceback/initialization-failed matches
in the inspected deployment interval. Host available RAM: 312/951 MiB; swap:
148/3062 MiB. User microphone settings and Discord connection were not changed.

Checkpoint: start a **fresh** Scaramouche session with `!voice start scaramouche`,
say “Scaramouche, are you there?”, then inspect owner diagnostics for
`receive_handshake_sent=1`, RTP/DAVE/PCM, STT and completed audible playback.
The pre-restart diagnostics requested after green-ring confirmation were not
received before deployment. Live success must not be inferred from unit tests.
Current state: **STAGING_GATE_B_BLOCKED** pending this human live retry and the
remaining Gate B scenarios. No Gate C work begun.

### Post-registration live retry failed — October 6, 11:27 AM PDT

The human started a fresh Scaramouche session at 11:26 AM and reported no reply.
The 11:27 AM owner diagnostic confirms `receive_handshake_sent=1`, but all
61 UDP callbacks were non-audio datagrams: zero RTP/DAVE/PCM/STT/playback events.
Both workers were healthy, reader listening, DAVE ready, two mapped speakers and
one mapped participant. The live service still runs `ea8e43a`, PID 2719372,
`NRestarts=0`. Therefore the initial speaking-state registration **did not resolve
the live incident**; it must not be reported as the confirmed root cause.

Discord's visible connection details showed average ping 19ms, last ping 20ms,
and end-to-end encryption. Client green-ring activity was confirmed by the
human; this still does not establish outbound media delivery. No audio payload,
credentials, or raw recording were captured. No new code change or service
restart was performed during this investigation.

Next isolated recovery check requested: leave the bot connected, disconnect and
rejoin only the primary human account, wait for any arrival greeting to finish,
then ask the same question. Record greeting and question-answer outcomes
separately and correlate fresh diagnostics. Awaiting the human action; still
**STAGING_GATE_B_BLOCKED**. No passed Gate A/B tests repeated.

### Human reconnect recovered speech; delayed-stop repair (October 6)

The human confirmed hearing both the rejoin greeting and a reply to the question.
Their subsequent pasted owner report proves receive/transcribe/playback in that
session: 1564 eligible RTP packets, 1286 DAVE frames, 1233 PCM frames, seven STT
successes, seven scheduled responses, eight completed chunks, healthy workers
and no pending audio. This confirms recovery after human rejoin, **not** that
fresh receive-only joins are reliable. The underlying zero-RTP incident remains
unresolved.

The same report includes 278 generic receive errors and one cancelled response
with detection=4705ms, stop-call=9ms, total barge-in=4714ms. Three keyword events
arrived after playback. No raw transcript exists to determine which packet
errors or utterances contributed, and detection timing includes the utterance
itself; 4705ms must not be characterized as pure processing latency.

Source inspection reproduced a separate blocking defect: an ordinary follow-up
during playback made the sole STT worker await that playback, preventing a later
stop utterance from being transcribed. The new failing-before/passing-after test
holds playback open, submits an ordinary follow-up followed by “stop”, and checks
cancellation plus suppression of the superseded reply.

Both bots now keep one latest pending response outside the STT loop. Waiting or
superseding that response never cancels active playback; interruption-off mode
is preserved. Pending work is discarded on interruption, speaker revocation,
session stop, or age expiry; user/epoch checks precede dispatch. Regression
coverage includes latest-turn replacement, normal completion, revocation, stop,
and expiry without unintended playback cancellation. Sanitized queue-wait and
speech-onset-to-STT timings distinguish backlog from utterance duration.

Receive failures now additionally count fixed categories: RTP parsing, transport
decryption, DAVE decryption, unverified DAVE output, and packet routing. Aggregate
errors are retained, all encrypted/authentication checks remain fail-closed,
and exception text, keys, raw packets and transcripts are never logged. These
are diagnostic categories, **not a claimed fix for the 278 historical errors**.
Both focused suites passed 74 tests with one optional skip before the additional
expiry regression. Full-suite/deployment results and live retry remain pending.

Validation completed: Scaramouche **707 passed, 1 skipped, 1 warning** in
219.44s; Wanderer **251 passed, 1 skipped, 1 warning** in 80.46s. The final focused
voice suites each passed **75 passed, 1 skipped**. The optional Opus skip and
local LibreSSL warning remain. Preliminary full runs were interrupted to correct
an invalid synthetic expiry-test threshold; a focused run exposed an existing
20ms test sleep racing response scheduling. That test now waits for the response
condition with a one-second deadline; production timing limits were not loosened.

Deployed at **12:22 PM PDT, October 6, 2026**:

| Bot | Exact functional code SHA |
| --- | --- |
| Scaramouche | `ca5af7120de8d91bb860d1d28739c73d555f35c0` |
| Wanderer | `b98912758d49fe7ef39d42c7df90b3e9ba23467b` |

Release imports/compilation/pinned dependency checks succeeded. Both release
branches were pushed; neither PR was merged. Online SQLite backups and previous
drop-ins are preserved under
`/opt/scara-wanderer-staging-fallback/config-backups/voice-segmentation-ca5af71`.
All three databases returned `quick_check=ok`. Both services are active/running,
PIDs 2746361 / 2746360, `NRestarts=0`; one intentional restart each in this batch.
Both logged online, with no startup ERROR/Traceback/initialization-failed matches
in the inspected deployment window. RAM available: 320/951 MiB; swap:145/3062 MiB.

At 12:23 PM, issued `!voice start scaramouche` in approved staging #general with
the primary human still connected to General. Required live test: establish
audible conversation, then during a longer answer say an ordinary follow-up
without his name, followed by “Scaramouche, stop”. Inspect the new receive-error
categories and STT queue timing alongside audible cancellation. Do not infer
live interruption success or fresh-join reliability from the automated tests.
Final checkpoint remains **STAGING_GATE_B_BLOCKED** pending human speech and
remaining voice scenarios. Gate C not started.

## Connected Accounts / Google Phase 1 — isolated feature branch (2026-10-06)

This user-authorized feature batch is separate from unfinished voice validation.
Gate A is not repeated. Gate B remains **STAGING_GATE_B_BLOCKED**; no voice code
or Fish Audio behavior is changed here.

Latest user-supplied voice diagnostic (not a new test performed in this batch):
713 RTP packets, 571 DAVE frames, 557 PCM frames; receive errors split into 61
unverified drops and 81 decrypt errors; three STT successes; one deferred response,
one dropped pending response and one cancellation. Keyword detection was 2,650 ms,
stop 8 ms, total barge-in 2,658 ms; a subsequent response was scheduled. This is
evidence of a detected interruption/cancellation, not proof of all remaining live
voice scenarios. The user explicitly wants an in-character interruption reaction.
No limitation or narration/personality change is introduced.

Verified release bases (local HEAD and actual GitHub release refs before branching):
Scaramouche `5548cfa8895acfe0521417688e620a05e408b38e`;
Wanderer `b98912758d49fe7ef39d42c7df90b3e9ba23467b`.
Scaramouche's difference from functional `ca5af7120de8d91bb860d1d28739c73d555f35c0`
was staging documentation only. Both new branches are
`feature/connected-accounts-google`; existing release PRs #22 / #9 remain open
and unmerged. No completed work was discarded.

Feature implementation and operator instructions: [CONNECTED_ACCOUNTS.md](CONNECTED_ACCOUNTS.md).
The shared authorization foundation is wired into both bots. Wanderer's actual
base lacked the hardened Calendar/Tasks command runtime; the guarded implementation
was reused, including bounded reads, exact stored write proposals, bot-created
Calendar markers, Tasks fields and owner-only allowlisted Sheets.

Read-only host recheck: both staging services active, working directories still
`/opt/scara-wanderer-staging-fallback/releases/scaramouche-ca5af71` and
`/opt/scara-wanderer-staging-fallback/releases/wanderer-b989127`, each
`NRestarts=0`. No deployment or intentional restart in this feature batch.
RAM available 330/951 MiB; swap used 148/3062 MiB at that check.

Google live validation is **BLOCKED_BY_OPERATOR_CONFIGURATION**: no OAuth app
client/secret/master-key/HTTPS callback configuration was present at preflight.
Publishing status, test-user allowlist and verification status are unknown.
No Google login, consent, provider read/write, callback deployment or cloud-account
write was performed. Automated fake-provider tests are not live Google evidence.
The operator must configure the named HTTPS origin, Google Web application client,
Calendar/Tasks APIs and approved test user before the user personally signs in.

Initial full regression passes before additional race hardening:
Scaramouche 755 passed / 1 skipped / 1 warning; Wanderer 299 passed / 1 skipped /
1 warning. Final expanded-suite results will be appended below; these are not a
claim that live account linking passed.

Expanded validation found and corrected test-harness issues without changing
production voice behavior. Wanderer's video-worker test installs a partial vision
module during collection; the new real-bot registration test now isolates that stub
and imports under an active Python 3.9 event loop. The combined connection/video
check passed **68 tests**. A later Wanderer full run passed **312 tests, 1 skipped,
1 warning** before the final shared voice-test cleanup refinement.

One Scaramouche expanded full run was interrupted after **714 passing tests**
because the existing keyword-diagnostic voice test was waiting indefinitely for
its manually started transcription worker to cancel. It is not counted as a
complete suite pass. Isolated connection + voice tests passed **120 tests**.
Three test cleanups now stop session eligibility before cancelling their manually
owned worker, with a bounded wait, matching the existing managed shutdown order.
Assertions and all production voice files remain unchanged. Final complete suites
are rerun on that candidate; no Gate B live pass is inferred from these tests.

Compile/import checks and dependency consistency passed. The shared connection
implementation is equivalent in both repositories (blank-line-only differences
excluded). The focused staged-file credential-pattern scan found **zero findings**
across 21 Scaramouche and 25 Wanderer files; no real Google credentials were used.
This is a scoped pattern scan, not a claim of an independent security audit.

Final complete regression on the feature candidates:

| Repository | Passed | Skipped | Warnings | Duration |
| --- | ---: | ---: | ---: | ---: |
| Scaramouche | 768 | 1 | 1 | 776.27 seconds |
| Wanderer | 312 | 1 | 1 | 178.24 seconds |

Each includes 61 connected-account regression cases. The existing optional Opus
skip and local LibreSSL warning remain. Scaramouche's 45-second faulthandler
diagnostic printed stacks during slow existing stress/voice tests; the run
continued and exited successfully, with no failed tests. Encryption, migration,
synthetic two-user/two-bot isolation, replay/wrong-user rejection, atomic refresh,
revocation/deletion races and restart persistence passed. Synthetic SQLite
`quick_check` returned `ok`; no live database was migrated by this batch.

Wanderer feature commit: `9026ec0bd036b4c5f8f5f98209cf437eebc0e608`, draft
[PR #10](https://github.com/Kittybri/Wanderer/pull/10), stacked on the release
branch. Its external `Workers Builds: wanderer` check failed; the existing release
PR #9 also reports that same failing check. GitHub exposes no diagnostic cause in
the check summary beyond a Cloudflare build link. This is an unresolved external
check, not a green PR or a demonstrated new code regression. No Cloudflare
configuration or deployment was manually changed to resolve it.

Google live authorization, provider reads, unconfirmed write previews, live
refresh and service-restart persistence remain **BLOCKED_BY_OPERATOR_CONFIGURATION**.
Automated persistence tests do not replace these live checks. No real Calendar or
Tasks writes were performed. Gate B remains **STAGING_GATE_B_BLOCKED**.

### Staging infrastructure preparation — 2026-10-06 continuation

Verified both clean feature worktrees and matching open/draft PR heads before
changes: Scaramouche #23 `908e60363cbf1ffd3253b669420b07a60061f2e6`;
Wanderer #10 `9026ec0bd036b4c5f8f5f98209cf437eebc0e608`. Both are mergeable;
both external Cloudflare Workers checks now report failure (also present on
release baselines). Neither PR was merged.

Oracle remains on functional Scaramouche `ca5af7120de8d91bb860d1d28739c73d555f35c0`
and Wanderer `b98912758d49fe7ef39d42c7df90b3e9ba23467b`; both services are
active/running with `NRestarts=0`. No restart or running-code switch this continuation.
Reviewed feature archives were extracted into `releases/scaramouche-908e603` and
`releases/wanderer-9026ec0` under `/opt/scara-wanderer-staging-fallback`, but are
**staged, not activated**. Compile/import checks pass in both existing Python 3.9.25
environments; both `pip check` results are clean. No dependency upgrade was needed.

Both live bots resolve their canonical shared store to
`/opt/scara-wanderer-staging-fallback/data-gate-a-6eab863-0d36a90/shared_state.db`.
Online backups of all three databases are preserved in the root-only directory
`/opt/scara-wanderer-staging-fallback/config-backups/google-connect-phase1-20261006`.
All three preflight/backup integrity checks passed. Additive connections migration
version 1 was applied to the canonical shared store and repeated idempotently:
`quick_check=ok`, zero foreign-key violations, zero linked accounts.

A cryptographically random 32-byte master key was generated directly on Oracle in
`/opt/scara-wanderer-staging-fallback/env/connections-key.env` (root-owned, 0600).
Its value was never displayed or copied into Git/chat. This file is not yet wired
into any service. Preserve it on continuation; do not generate a replacement.
Idempotent preparation script is in local `oracle_ops/deploy_staging/` and the host
`uploads/google-connect-phase1-20261006/prepare.py` (no embedded secrets).

Network preflight: no nginx/certbot installation, callback listener, or configured
TLS proxy. Firewalld public zone exposes SSH only (plus dhcpv6-client service).
Oracle VCN ingress has not yet been verified. No firewall/DNS/TLS changes made.
Available RAM 377/951 MiB; swap used 174/3062 MiB; disk available 48 GiB.

User approved existing Google project `sheets-editor-510419` (Sheets editor).
Browser inspection confirms External / Testing, one approved test user matching
the signed-in owner account, no authorized domains and no homepage/privacy URL.
Calendar and Tasks are absent from the 24 enabled APIs. Calendar API activation
page explicitly links API terms; handed the Enable step to the user as requested
for legal acceptance. No Google configuration write, credential creation, consent
or provider operation has been performed. The account is already signed in.

User does not know whether they own a domain. No usable hostname was found in
the inspected bot deployment or OAuth branding. A free DuckDNS staging subdomain
was proposed, not registered; Google callback acceptance and HTTPS still require
verification. Do not claim the user owns no domains outside this inspected scope.

Resume after the user reviews/enables Calendar and chooses/registers a hostname.
Complete Tasks enablement with any required personal acceptance; then configure
hostname/TLS, OAuth Web client and protected credentials, activate services and
hand off actual Google account consent to the user. Live Google remains blocked;
no real Calendar/Tasks writes are permitted in validation.

Follow-up: user enabled Calendar personally; Google service details now explicitly
show `calendar-json.googleapis.com` Enabled. User requested a free hostname.
Opened DuckDNS registration/sign-in page and handed off personal sign-in. No
hostname registered yet. Proposed `kittybri-bots.duckdns.org` remains unverified
for availability. On return, do not capture the whole signed-in DuckDNS page or
screenshots: it may display its persistent account token. Inspect only narrowly
scoped non-secret domain controls. Do not ask the user to share that token.

### Hostname and infrastructure checkpoint — 2026-10-06 evening PDT

User completed DuckDNS sign-in and CAPTCHA personally. Registered
`kittybri-bots.duckdns.org` and changed its A record to staging Oracle
`163.192.24.7`; independent DNS resolution confirms that address. Only domain
controls were inspected; no DuckDNS token was read/output or installed.

Google Tasks API was enabled in the approved existing project, with the Enabled
status and Disable API control verified afterward. Calendar was already enabled
by the user. Prepared a **separate**, not-yet-submitted Web OAuth client named
`Scaramouche + Wanderer Staging` with sole redirect URI
`https://kittybri-bots.duckdns.org/oauth/google/callback`; no JavaScript origins.
The existing Chrome extension client is untouched. The user was asked to review,
click Create and download JSON locally, without sharing its contents. No client
secret is installed yet. User acceptance of Let's Encrypt subscriber agreement
v1.8 was separately requested before certificate issuance/automatic renewal.

Installed nginx (Oracle package 1.20.1-28.0.1.el9_8.6) and Certbot 3.1.0 from
official configured Oracle repositories. The initial metadata job for the unrelated
large OCI-included repository caused memory pressure and was stopped before its
package transaction; a restricted-repository installation completed successfully.
Both bot services remained active with `NRestarts=0`; final observed available
RAM 363/951 MiB, swap used 418/3062 MiB.

Added a dedicated OCI NSG `bots-google-staging-https`, attached only to the verified
staging VNIC, with stateful inbound TCP 80 and 443. Existing NSG assignments and
security-list rules were preserved; baseline is saved locally in the private
`oracle_ops/deploy_staging/google-connect-phase1-20261006/network-baseline.json`.
Firewalld now permits HTTP/HTTPS in addition to its prior services.

Nginx is enabled/active with one worker, request logging disabled, and only the
HTTP ACME challenge directory exposed. Public verification returned HTTP 200 and
the expected synthetic marker at
`http://kittybri-bots.duckdns.org/.well-known/acme-challenge/staging-network-check`.
Plain HTTP `/oauth/google/callback` returned 404 as intended. This is network
readiness evidence, **not** OAuth callback health or HTTPS success.

Installed (but did not start/enable) `connections-staging.service`; it uses the
reviewed Scaramouche candidate, existing runtime and canonical shared SQLite path.
Protected non-secret connection configuration and existing root-only key file are
referenced. A missing `env/connections-google.env` prevents premature startup.
Original nginx configuration is backed up alongside database backups. Operational
scripts/configurations are preserved in local and remote `google-connect-phase1-20261006`
staging folders. No bot code activation/restart, certificate issuance, Google
account link, provider read or provider write has occurred at this checkpoint.

Next user check: Google UI explicitly reports `OAuth client created` for the
prepared Web client. Download JSON was invoked; the browser download-event wait
timed out, but the resulting file was independently located in local Downloads.
Local validation confirms Web application, project `sheets-editor-510419`, exactly
the registered HTTPS callback and nonempty app credentials. File permission was
restricted to 0600. No credential values were output. Credentials are not yet
installed on Oracle. Let's Encrypt agreement/issuance approval remains pending;
do not infer it from the user's “check now” request.

### HTTPS and reviewed-candidate activation — 2026-10-06 evening PDT

User explicitly accepted Let's Encrypt certificate issuance and automatic renewal.
Certbot obtained a valid certificate for `kittybri-bots.duckdns.org` (issuer YE2;
expiry 2027-01-05 00:56:15 UTC / January 4, 2027 4:56:15 PM PST). Registration did
not transmit a personal email address. `certbot-renew.timer` is enabled/active;
the deploy hook validates and reloads nginx after renewal.

Google Web client JSON was transferred through SSH stdin directly to a validating
root-side installer, never displayed or placed in Git. The resulting
`env/connections-google.env` is root-owned 0600. Master-key and non-secret config
files are also root-owned 0600. Original local download remains owner-only 0600;
it has not been deleted. Do not expose it in screenshots or logs.

`connections-staging.service` and nginx are enabled/active. SELinux remains
Enforcing; port TCP/8787 has HTTP port labeling and the HTTP relay boolean is on
for reverse proxying (general httpd network-connect boolean was not enabled).
The callback itself listens only on loopback. Public HTTPS `/health` returned
200 `ok` with certificate validation enabled and no-store/referrer/CSP headers.
Missing-state `/oauth/google/callback` returned fixed safe HTTP 400. Plain HTTP
OAuth remains unavailable (404). This does not establish a successful real OAuth
exchange or user authorization.

Activated exact reviewed feature candidates with new `60-google-connections.conf`
drop-ins, retaining earlier voice/trolling settings and taking copies of prior
drop-in directories. Functional deployed SHAs:

| Service | Exact deployed functional SHA |
| --- | --- |
| Scaramouche staging | `908e60363cbf1ffd3253b669420b07a60061f2e6` |
| Wanderer staging | `9026ec0bd036b4c5f8f5f98209cf437eebc0e608` |
| Connections staging | `908e60363cbf1ffd3253b669420b07a60061f2e6` |

Both bots logged online after one intentional deployment restart each. All three
services are active/running, `NRestarts=0`, with zero startup ERROR lines or
tracebacks in the inspected startup windows. In-memory environment checks confirm
the three processes have identical nonempty OAuth settings/master key, and both
bot data directories resolve to the callback's canonical shared database. Checks
reported only booleans, never credentials. All three databases: `quick_check=ok`.
Linked account count is zero. Host observation: available RAM 301/951 MiB, swap
used 179/3062 MiB. Both feature PRs remain open/draft/unmerged at the reviewed heads.

Next handoff is the user's real Discord `!connections` flow. The existing project's
public consent branding remains `Sheets editor` to avoid silently renaming its
separate Chrome-extension client. The user must personally review requested
identity/Calendar/Tasks permissions and privately confirm the bot account link.
No real Google Calendar/Tasks read or write, grant validation, authenticated token
refresh, or post-link restart persistence has occurred yet. No provider writes
are permitted during the remaining preview-only validation.

Certificate renewal dry run completed successfully, including the nginx deploy
hook. The first dry run was stopped during Certbot's randomized scheduling delay;
the repeated validation used `--no-random-sleep-on-renew` only for that manual test.
The normal scheduled renewal retains its default jitter. Hook stderr contained
nginx's successful configuration-test messages; the command exited 0 and reported
all simulated renewals successful. HTTPS health was rechecked after the reload.

### Real account link and read-only validation — 2026-10-06 19:15 PDT

User reported completing Connect Google. Canonical database verification found
one CONNECTED Google account bound to the expected primary Discord user, encrypted
credential material present, and no pending OAuth session. The live Google
userinfo subject matched the identity sealed at confirmation; email verification
was true. No identity, credential, calendar event, or task contents were printed.

Scaramouche grant is enabled. Wanderer is independently denied with BOT_DISABLED;
no grant was silently added. User was asked to use `!google permissions` privately
and enable Wanderer. That action is still pending at this checkpoint.

Using the exact deployed candidate's ConnectedGoogleRuntime and canonical store:

| Live validation | Result |
| --- | --- |
| Scaramouche bounded Calendar read | PASS |
| Scaramouche bounded Tasks read | PASS |
| Confirmed Discord / Google subject binding | PASS |
| Authenticated encrypted credential decryption | PASS |
| Independent Wanderer grant enforcement | PASS: denied while disabled |
| Unconfirmed Calendar creation proposal | PASS: dry-run only |
| Unconfirmed Tasks creation proposal | PASS: dry-run only |
| Provider requests during both previews | Zero |
| Provider Calendar/Tasks writes sent | Zero |
| Ephemeral synthetic previews cleared | PASS |
| Real OAuth token refresh and sealed persistence | PASS |

Validation used an HTTP guard that permits only GET for Calendar/Tasks; no
mutating request was attempted or sent. Neither proposal was confirmed. The
proposals were exercised directly through deployed runtime methods, not through
Discord UI; no claim of additional command/UI interaction is made. Their in-memory
pending store was cleared without deleting the connected account.

Refresh was exercised once by conditionally expiring only this account's local
access-token cache expiry, then invoking the ordinary service refresh path with
real wall time. Google accepted the refresh; the service preserved the account
revision, persisted encrypted refreshed credentials and a future expiry, recorded
last_refresh_at, and released its refresh lease. No scope/grant was changed.

Restarted connections-staging and wanderer-staging once each after linking, then
reran identity/decryption/Scaramouche reads successfully using the Wanderer release
runtime. All three services active; NRestarts=0; inspected current-start journals
had zero ERROR/traceback lines. All three SQLite quick checks returned ok. HTTPS
health HTTP 200 with TLS verification success. Host available RAM 336/951 MiB,
swap used 173/3062 MiB. Scaramouche was deliberately not restarted yet to preserve
the user's pending permission-button interaction.

Functional SHAs and draft PR heads remain unchanged: Scaramouche #23
`908e60363cbf1ffd3253b669420b07a60061f2e6`; Wanderer #10
`9026ec0bd036b4c5f8f5f98209cf437eebc0e608`. Existing automated totals remain
768 passed / 1 skipped (Scaramouche), 312 passed / 1 skipped (Wanderer), one warning
each; not rerun this checkpoint because no application code changed.

Remaining: user's explicit Wanderer grant, live reads with that bot grant,
Scaramouche post-link restart and persistence recheck, and private Discord
`!google status` / `!google permissions` UI confirmation. Do not claim staging
fully complete yet. No Phase 2 work or PR merge occurred.

### Independent Wanderer grant and restart persistence — 2026-10-06 19:28 PDT

User reported enabling Wanderer. Live canonical status now confirms both distinct
grants enabled, with the same confirmed primary Discord / Google identity binding.
Using the Wanderer candidate runtime, both bots' bounded Calendar and Tasks reads
passed. Both bots' Calendar/Tasks proposals were dry-run only, made zero provider
requests, and were cleared from the diagnostic runtime's ephemeral pending store.
No Calendar or Tasks mutation was attempted or sent.

Then intentionally restarted both bots and the connection service together once.
Repeated validation using the Scaramouche candidate runtime passed for both bot
grants, both provider reads, encrypted credential decryption, Google subject binding,
and no-write previews. Google remains CONNECTED with Calendar and Tasks available.
The real OAuth refresh test from the preceding checkpoint also remains passed.

| Deployment / health | Result |
| --- | --- |
| Scaramouche RELEASE_SHA | `908e60363cbf1ffd3253b669420b07a60061f2e6` |
| Wanderer RELEASE_SHA | `9026ec0bd036b4c5f8f5f98209cf437eebc0e608` |
| Callback code | Same Scaramouche candidate |
| All three services | active; automatic NRestarts=0 each |
| Inspected current-start ERROR/traceback lines | 0 each |
| shared_state / scaramouche / wanderer database quick_check | ok / ok / ok |
| Callback HTTPS health | 200; TLS verification succeeded |
| nginx / certbot renewal timer | active; renewal timer enabled |
| Host available RAM / total | 332 / 951 MiB |
| Host swap used / total | 173 / 3062 MiB |
| Scaramouche #23 / Wanderer #10 | open, draft, unmerged at above heads |

Intentional restarts during this Phase 1 activation/validation sequence: Scaramouche
two (deployment plus post-link validation); Wanderer three (deployment plus two
post-link validations); callback initial start plus two post-link restarts. These
are distinct from systemd's automatic NRestarts counters, all zero.

Google OAuth configuration was last directly observed External / Testing with one
approved test user. No publishing change was made. HTTPS hostname remains
`kittybri-bots.duckdns.org`, redirect path `/oauth/google/callback`. Certificate
renewal simulation already passed; neither TLS nor master key was regenerated.

Application code and prior automated totals are unchanged. `git diff --check`
passed for the updated validation record. No complete suite was rerun for these
infrastructure-only live checks.

**Infrastructure and authenticated runtime validation: PASS. Final Discord command
display confirmation remains pending.** The in-app Discord tab is logged out, so
it cannot currently supply UI evidence for `!google status` / `!google permissions`.
Asked the user to run both commands privately with each bot and report only whether
connected, module availability, and both grants are displayed. Do not conflate
backend runtime verification with command-UI verification. No known backend blocker
was observed; no Phase 2 work, real provider write, or PR merge occurred.

Next Phase 2 recommendation after final command confirmation: choose and scope
one read-only provider module, including minimal scopes and revocation/deletion
tests, before implementing anything. No additional Google scope has been requested.

### Phase 1 staging completion — 2026-10-06, user-reported 20:27 PDT

User confirmed receiving the expected status and permissions displays from both
bots and supplied Wanderer's two Connected Accounts responses. Both show Google
Connected, modules calendar/tasks, Scaramouche Allowed, Wanderer Allowed, and the
notice that writes require a separate exact confirmation. Account identity remained
masked. This is user-provided Discord command evidence, not a new direct browser
observation; the in-app Discord tab remains logged out.

**Google Connected Accounts Phase 1 staging: PASS.** This closes the final pending
command-display check above. Live authenticated reads, independent grants,
encrypted credential use, real token refresh, restart persistence, no-write
previews, HTTPS/renewal, service health, and database integrity were established in
the preceding checkpoints. No real Calendar/Tasks write was performed. No known
Phase 1 staging blocker remains. This does not change any separate voice gate.

No application code, deployed SHA, secret, Google publishing state, or PR state was
changed for this confirmation. Feature PRs remain draft/unmerged at the previously
verified heads. Existing suite totals remain 768 passed / 1 skipped for Scaramouche
and 312 passed / 1 skipped for Wanderer, one warning each; suites were not rerun for
this documentation-only update. OAuth remains a Testing deployment, not a public
production launch. Phase 2 remains unstarted and requires separate authorization.

### Google command discovery follow-up — 2026-10-06 20:44 PDT

User requested visible `!google` guidance in both character help menus and slash
commands so friends can connect their own accounts. Added a prominent first-page
help description to `!scarahelp` and `!wanhelp` / `!wandererhelp`, without increasing
embed field counts. Added `/google` to both bots; it defers privately and returns
the same user-bound account controls as an ephemeral response, including when DMs
are closed. Prefix commands and aliases retain existing behavior. No OAuth scope,
test-user allowlist, grant default, confirmation rule or publishing change occurred.

Deployment and draft PR heads:
- Scaramouche #23: `e42c9f1f14c18e559b069a304eea3c807a64f286`.
- Wanderer #10: `aad50ac9b9b2a83e2f9135f0a06b47ee22d9557a`.
- Callback remains on `908e60363cbf1ffd3253b669420b07a60061f2e6`;
  this change affects bot discovery/UI only.

Read-only Discord registration checks found nine legacy Scaramouche global slash
commands absent from its local tree. Scaramouche therefore upserts only `/google`
instead of bulk-syncing and deleting those registrations. Wanderer retains its
existing full-tree startup sync. After deployment Discord reports all nine old
Scaramouche commands plus google, and all five old Wanderer commands plus google.
Both new commands have no administrator/default member permission restriction and
are enabled for DMs. Registration was verified through Discord's API; invocation
privacy, caller binding, sanitized failures, actual help rendering limits and
single-command upsert preservation were verified by regression tests. No claim
of a fresh browser slash invocation: Discord remains logged out in the in-app tab.

Targeted tests: Scaramouche 73 passed, Wanderer 67 passed, one existing local
LibreSSL warning each. Both git diff checks passed. Full suites were not rerun for
this bounded UI/discovery change. Both bots restarted once for these candidates;
all services active, automatic NRestarts=0, inspected current-start ERROR/traceback
counts zero, all three database quick checks ok. Live reads and private no-write
previews passed again for both grants using both new runtime candidates; encrypted
identity and grants persisted. No provider write was attempted or sent. Both PRs
remain open/draft/unmerged. Old release directories and protected configuration
remain intact; only a new WorkingDirectory drop-in selects each new bot release.

Friends can now discover `/google`, but Google Testing still requires their own
Google accounts to be explicitly approved as test users by the operator. No friend
was automatically granted access and no account was linked on another user's behalf.

### Additional Google test user — 2026-10-06

At the user's explicit request and subsequent Save confirmation, added their
second Google account to the existing project's OAuth test-user allowlist.
Google Cloud Audience now shows two test users; the original entry is preserved
and publishing remains Testing. No Discord link, bot grant, OAuth consent,
Calendar/Tasks read or write was performed for this second account. The user must
personally complete any subsequent connection flow from their intended Discord
account. No credential or full test-user email was added to this record.

User subsequently reported completing the second connection. Read-only canonical
store inspection confirms two separate Discord-owned Google records, both
CONNECTED with encrypted credentials present, no pending final confirmation,
and quick_check=ok. The primary Discord account retains both bot grants; the
secondary account currently grants only Scaramouche. No grant was added on the
user's behalf. This check verifies stored link state, not the secondary account's
Google identity or live provider reads; no secondary provider content was fetched.

After the user's subsequent grant confirmation, read-only inspection confirms
Scaramouche and Wanderer are now both enabled for the secondary account. The
primary account remains separately CONNECTED with both grants; shared database
quick_check remains ok. No provider read/write or account relinking was performed
for this grant verification.

## Google Phase 1 closure audit — 2026-10-06

This section supersedes earlier completion shorthand for the expanded closure
request. No Phase 2 work, real Calendar/Tasks mutation, or PR merge is authorized
or performed. Canonical live evidence remains in this file.

### Candidate and regression evidence

Current deployed/tested functional heads (including help discovery and `/google`):
- Scaramouche: `e42c9f1f14c18e559b069a304eea3c807a64f286`.
- Wanderer: `aad50ac9b9b2a83e2f9135f0a06b47ee22d9557a`.
- Callback remains `908e60363cbf1ffd3253b669420b07a60061f2e6`; provider and store
  implementation is unchanged by the subsequent bot UI/discovery commits.

Fresh complete suites, not the earlier 768/312 runs or targeted discovery runs:

| Check | Scaramouche | Wanderer |
| --- | --- | --- |
| Complete `python -m pytest -q` | 774 passed, 1 skipped, 1 warning; 133.11s | 318 passed, 1 skipped, 1 warning; 70.65s |
| Connected accounts + discovery rerun | 67 passed | Included in 83-pass connections/discovery/release-hardening rerun |
| Persistence/migration/privacy rerun | 14 passed | Privacy coordinator cases included in 83-pass rerun and full suite |
| Tracked Python compile checks | 133 files passed | 76 files passed |
| Tracked-file credential-pattern scan | 179 files, zero matches | 109 files, one reviewed fixture-only match; no credential |
| `git diff --check` | PASS | PASS |

Full suites exercise actual bot imports, prefix/slash registration, encrypted
connections, migrations, privacy deletion, refresh rotation/concurrency, stale
account races and user-bound Discord UI. One auxiliary Scaramouche targeted command
initially referenced Wanderer's nonexistent `test_release_hardening.py` and ran no
tests; corrected paths were rerun successfully as listed above. This did not affect
the successful full-suite run.

Both skips are the optional real Opus encode/decode test requiring
VOICE_OPUS_LIBRARY; separately verified with `-rs` (16 passed / 1 skipped per bot).
The warnings are the local urllib3/LibreSSL environment warning. No live voice
testing occurred. Wanderer's scan match is an assertion containing only the
literal private-key BEGIN delimiter in tests/test_release_hardening.py:70, used to
test secret detection; no key body exists there. Findings were reported by location
and rule only, never by credential value.

### Live two-user isolation and disconnect

The diagnostic imported each candidate's ConnectedGoogleRuntime and real canonical
service, and resolved two separate Discord-owned Google records. For every live
Calendar/Tasks request, a guarded GET-only HTTP client verified the outgoing bearer
token's Google userinfo subject against the subject sealed to that Discord user's
record before forwarding the read. A and B subjects were distinct. Reports contain
only synthetic LOCAL_USER_A / LOCAL_USER_B labels and success booleans; no event,
task, subject, email, token or response body was output. These are local probe
markers, not independently seeded provider-content canaries. No Calendar event or
Task was created to manufacture evidence.

Before disconnect: all eight reads passed (two users x two bots x two modules),
with each request bound to the intended Google subject. No cross-user token/data
routing was observed on those requests. The checks call deployed runtime methods;
they are not a claim of observing all eight commands in Discord UI.

With explicit user authorization, disconnected B through the service's normal
disconnect operation with an expected account revision. Google revocation returned
DISCONNECTED. B's connected-account row, grants and OAuth sessions were removed;
status became NOT_CONNECTED and both bots were denied for both modules. A's complete
account row (including encrypted credentials), grants and sessions compared
byte-for-byte identical immediately before/after B's disconnect and after A's
subsequent successful Calendar/Tasks reads with both bots. Shared quick_check=ok.

User personally reconnected B using Wanderer's normal OAuth/Discord confirmation
flow. New canonical state is CONNECTED with Wanderer allowed; as designed,
Scaramouche requires a fresh explicit grant. The first strict two-bot retest stopped
at the missing grant (diagnostic assertion, not a provider/code failure). A
Wanderer-only reconnect retest passed Calendar/Tasks reads for both users, with
distinct Google subjects and correct outgoing request identity checks. The user has
been asked to enable Scaramouche from B's `/google` panel. No grant was fabricated.

**Expanded closure state: READY_FOR_MERGE for Google Connected Accounts Phase 1.**
The renewed grant and final both-bot post-reconnect retest have now passed, as
recorded in the final closure evidence below. This is not a Production launch or
a claim that external Cloudflare checks are green.

### Infrastructure and unchanged validation

HTTPS callback `https://kittybri-bots.duckdns.org/oauth/google/callback` is live;
fresh `/health` check returned 200 with certificate verification. Prior successful
renewal simulation, encrypted refresh and restart-persistence evidence is retained
above. Both original accounts/grants survived bot discovery deployment restarts.
No new token-refresh claim is inferred solely from a successful read.

All three SQLite quick checks passed. Three services remain active with automatic
NRestarts=0. Latest host sample: 350/951 MiB available RAM, 176/3062 MiB swap used.
Current-start Scaramouche logs include two legacy `/dashboard` CommandNotFound
tracebacks (tree.py lookup), outside Google Phase 1; the global registration exists
but that legacy handler is absent locally. Preserved registrations were not deleted
to mask this issue. No unrelated repair was attempted. Wanderer and callback had
zero inspected ERROR/traceback lines. Do not report all service logs as error-free.

Both feature PRs (#23 / #10) are open, draft, MERGEABLE. Cloudflare Workers checks
fail independently of passing local suites; matching failures were verified on
both release-base SHAs (`5548cfa...` / `b989127...`). No Cloudflare changes were made.
Mergeability is not equivalent to every external check passing.

### Exact remaining Testing-to-Production preparation (not performed)

Read-only Cloud Console inspection shows External / Testing, two test users, Publish
app disabled until branding is complete, app name `Sheets editor`, blank homepage,
privacy-policy and terms URLs, and authorized domain `kittybri-bots.duckdns.org`.
Verification Center says verification is not required while Testing. Data Access
currently declares userinfo.email and the separate existing Sheets scope; it does
not declare the bots' runtime Calendar/Tasks scopes. No existing Sheets client or
scope was removed or repurposed.

1. Plan a separate production project/client from this staging project, preserving
   the existing Sheets application. Select accurate bot branding and maintain valid
   support/developer contacts.
2. Publish a public homepage describing the bots and a matching privacy policy
   covering Google data access, use, encrypted storage, sharing, retention, deletion
   and disconnect. Link both in branding. Terms are optional for Google's stated
   homepage requirements; supply them if applicable rather than claiming mandatory.
3. Verify ownership of applicable authorized domains in Search Console. DuckDNS
   issuance/TLS alone is not Google domain-ownership verification; determine whether
   this subdomain is acceptable or use a domain under the operator's verified control.
4. Declare the exact actual scopes: openid/email, calendar.events and tasks; justify
   write scopes for the existing separately confirmed write features. Do not add
   Gmail/Drive/other Phase 2 scopes. Resolve the Sheets branding/scope mismatch through
   the separate production design, not by disrupting the existing client.
5. For a public launch, complete brand and applicable sensitive-scope verification,
   including scope justifications and a consent/feature demonstration. A limited
   personal-use app for a few known users may qualify for an exception; this is not
   the same as public verification and retains unverified-app constraints.
6. After required Google approval/operator authorization, configure production
   client credentials and redirect, publish appropriately, reauthorize and repeat
   identity/isolation/revocation tests. No production project, publishing action or
   verification submission was performed during this Phase 1 audit.

Sources checked for this checklist:
- https://developers.google.com/identity/protocols/oauth2/policies
- https://developers.google.com/identity/protocols/oauth2/production-readiness/sensitive-scope-verification
- https://support.google.com/cloud/answer/13464321

### Final reconnect and two-account restart evidence — 2026-10-06 21:25 PDT

Canonical inspection confirmed the user enabled B's Scaramouche grant. Repeated
all eight live identity-checked reads using the Wanderer candidate: PASS. Restarted
both bots and the callback once and compared both accounts' complete account rows,
encrypted credential bytes, bot grants and session state before/after: exactly
equal. Repeated all eight reads using the Scaramouche candidate after restart:
PASS. Both Google subjects remain distinct; both bots resolve the intended user.
No synthetic provider items were created and no provider contents were reported.
Calendar/Tasks writes sent throughout these probes: zero.

All three services are active with NRestarts=0. The inspected new-start error counts
are zero; this does not erase the earlier unrelated dashboard errors recorded above.
All three SQLite quick checks pass. Final host sample: 343/951 MiB available RAM,
176/3062 MiB swap used. This closure added one controlled restart per service.
The earlier actual Google token-refresh success remains valid evidence; refresh
rotation/concurrency tests passed in the complete current suites. Callback HTTPS,
Testing/two approved test users, original and secondary human authorization,
independent grants, `/google` discovery, disconnect isolation, reconnect isolation,
and two-account restart persistence are all now covered.

Full suites also reran successfully on the first pushed documentation checkpoint
heads: Scaramouche `329633f43c604446b4541c87b7999013f703d15b` — 774 passed,
1 skipped, 1 warning in 124.07s; Wanderer
`ffe984ac510c7e3dd519d4abc55ca9857ceb9658` — 318 passed, 1 skipped, 1 warning
in 66.48s. Subsequent closure commits modify documentation only; deployed functional
SHAs remain e42c9f1 / aad50ac. Exact final PR heads and their final full-suite results
are reported in the handoff, without embedding a commit's own SHA into its contents.

**No remaining Google Phase 1 code/live-validation blocker is known.** PRs remain
draft and unmerged; an operator still must handle the independently failing/pending
Cloudflare integration according to repository merge policy. Google Production
preparation remains the explicit checklist above. No Phase 2 work was started.

## Tarot command recovery — 2026-10-07

This separate regression repair does not repeat or change Gate A, Gate B, or
Google Phase 1 verdicts. Canonical detailed audit: TAROT_RESTORATION.md.

## Live restoration evidence — 2026-10-07 18:35 PDT

Deployed functional candidates:
- Scaramouche: `28da58e125e06a6bc7752b970a3e9cac8a7b9213`
- Wanderer: `5f64866c8335b8ed9fc1170240085e3c0278d86c`

Both bot services are active, with one controlled restart each and NRestarts=0.
New-start inspected error counts are zero. The connection service was not restarted
or reconfigured; its process and working directory remained unchanged.

Both candidates verified all 78 original artwork files and rendered all three
spreads on Oracle. The original tarot SQLite database was backed up before the
additive session migration. Existing preferences, daily draws and history rows
were compared before/after and are unchanged. Tarot quick_check is ok; the bot
and shared-state databases also each report ok.

Discord's live global-command API lists /tarot, /dailycard, /tarothistory and
/tarotsettings for BOTH bots. Existing /google remains registered. Scaramouche's
five older remote slash registrations were preserved, not bulk-deleted; their
missing local handlers remain an outstanding audit finding. No human end-to-end
Discord tarot button/reading test was observed during this deployment.

Google account/grant rows and protected environment-file hashes are unchanged.
The callback health endpoint returned HTTP 200 with successful TLS verification.
No provider write occurred. Host sample: 322/951 MiB available RAM and
179/3062 MiB swap used. No voice, DNS or Google configuration was changed.

Deployment uses only the new 80-tarot.conf systemd drop-in per bot. Rollback is to
move that drop-in aside, reload systemd and restart the bot units; prior releases
remain available. The original tarot database backup is root-protected under
config-backups/tarot-restoration-20261007 in the existing staging root.

Full candidate test totals remain Scaramouche 794 passed / 1 skipped and Wanderer
338 passed / 1 skipped. Subsequent evidence commits are documentation-only.
Draft repair PRs: Scaramouche #24 and Wanderer #11; neither is merged.

Outstanding non-tarot gaps are explicitly documented in TAROT_RESTORATION.md and
the legacy/current command manifests. In particular, Scaramouche still lacks the
legacy RPG/birthday/document-editing/recovery command groups and several local
slash handlers; both bots lack the old provider-status aliases. Intentional
Unrestricted renames and biometric privacy changes were not reverted. The
historical migration responsible for the omissions has not been established.
