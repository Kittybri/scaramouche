# Live Staging Gate A Validation Record

Status: **STAGING_GATE_A_BLOCKED**

Block reason: **BLOCKED_BY_APPROVAL_AND_TEST_IDENTITY** — all safe, pre-approved live checks are complete. Temporary Discord permission removal and server-chaos mutations still require action-time approval, and no second disposable Discord user was available for destructive cross-user/full-reset tests.

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
| Final live PIDs | Scaramouche `467186`; Wanderer `467187` |
| Final service state | Both active, `NRestarts=0`; about 68 MiB and 78 MiB respectively at final inspection |
| Final database state | All three databases: `PRAGMA quick_check = ok` |
| Risk controls | Voice, home, companion, real integrations, proactive generic traffic, and chaos stayed disabled |

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

## Findings and repairs

| ID | Severity | Finding | Resolution |
|---|---|---|---|
| ENV-001 | ENVIRONMENT (resolved) | Original services were unversioned legacy flat bundles. | Exact immutable candidates were deployed. |
| ENV-002 | ENVIRONMENT (resolved) | Original bots used different local `shared_state.db` files. | Candidates were co-located on one local shared file. |
| ENV-003 | ENVIRONMENT (open) | No second disposable Discord user is available. | Cross-user and destructive full-reset live tests remain blocked. |
| ENV-004 | APPROVAL (open) | Permission-removal and chaos mutations require fresh action-time approval. | Keep disabled until the owner explicitly approves the exact reversible mutations. |
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
| 10 | Cross-user isolation | BLOCKED | B | Second disposable user unavailable; owner data was not repurposed |
| 11 | Mention Scaramouche | LIVE PASS | S | Only Scaramouche answered |
| 12 | Mention Wanderer | LIVE PASS | W | Only Wanderer answered |
| 13 | Reply attribution | LIVE PASS | B | Reply to Wanderer produced no Scaramouche response |
| 14 | Plain conversation eligibility | LIVE PASS | B | Unaddressed control text produced no unintended duplicate response |
| 15 | Harmless command per bot | LIVE PASS | B | Help and build commands returned correct owner output |
| 16 | Command plus reply | LIVE PASS | B | Owned command and reply routes did not trigger partner continuation |
| 17 | Command plus attachment | BLOCKED | B | Browser uploaded attachment separately; combined command case not observed |
| 18 | Image attachment | LIVE PASS | B | Both bots accurately described the synthetic image after fixes |
| 19 | Text attachment | LIVE PASS | B | Preview/reaction completed; no crash or error |
| 20 | Unsupported/oversized attachment | BLOCKED | B | Browser chooser would not select custom unsupported extension |
| 21 | Scaramouche serious arbitration | LIVE PASS | S | Calm actionable response; no joke interception |
| 22 | Wanderer serious arbitration | LIVE PASS | W | Patched retest gave supportive user-facing response |
| 23 | Disposable credential guard | LIVE PASS | B | Warning responses only; retest marker absent from all databases |
| 24 | View Channel removed | BLOCKED | B | Requires fresh action-time permission-change approval |
| 25 | Send Messages removed | BLOCKED | B | Requires fresh action-time permission-change approval |
| 26 | Read Message History removed | BLOCKED | B | Requires fresh action-time permission-change approval |
| 27 | Embed Links removed | BLOCKED | B | Requires fresh action-time permission-change approval |
| 28 | Attach Files removed | BLOCKED | B | Requires fresh action-time permission-change approval |
| 29 | Add Reactions removed | BLOCKED | B | Requires fresh action-time permission-change approval |
| 30 | Gateway/network reconnect | BLOCKED | B | Explicit host-network interruption was not induced |
| 31 | Repeated ready event | BLOCKED | B | Process restarts passed; same-process repeated-ready not induced |
| 32 | Restart Scaramouche only | LIVE PASS | S | Only Scaramouche PID changed and reconnected |
| 33 | Restart Wanderer only | LIVE PASS | W | Only Wanderer PID changed and reconnected |
| 34 | Restart both | LIVE PASS | B | Both PIDs changed and both reconnected cleanly |
| 35 | Duo/shared interaction | LIVE PASS | B | `!both` produced exactly one contrasting answer per bot |
| 36 | Bot relationship write | LIVE PASS | B | Expected local/shared rows observed from the duo interaction |
| 37 | Concurrent ordinary activity | LIVE PASS | B | Both processed live activity together; no persistent lock |
| 38 | Seed disposable privacy state | LIVE PASS | B | Unique synthetic memories created and visible |
| 39 | Normal full deletion | BLOCKED | B | Destructive full reset not run against owner's real profile; no disposable user |
| 40 | Partial deletion failure | AUTOMATED PASS / LIVE BLOCKED | B | Retryable coordinator test passed; live fault injection not performed |
| 41 | Resume deletion | AUTOMATED PASS / LIVE BLOCKED | B | Resume-only-unfinished-stage test passed; no live pending job created |
| 42 | Enable chaos explicitly | BLOCKED | B | Requires fresh action-time mutation approval |
| 43 | Reversible cosmetic mutation | BLOCKED | B | Requires fresh action-time mutation approval |
| 44 | Restart restoration | BLOCKED | B | No chaos receipt created without approval |
| 45 | Newer admin edit protection | BLOCKED | B | No cosmetic target mutated without approval |
| 46 | Interrupted restoration | BLOCKED | B | No chaos receipt created without approval |
| 47 | Two-bot chaos ownership | BLOCKED | B | Chaos intentionally remained disabled |
| 48 | Disable chaos and verify clean state | LIVE PASS | B | Chaos stayed disabled; no pending mutation receipt |
| 49 | Task health after tests | LIVE PASS | B | Both active, bounded memory/tasks, `NRestarts=0` |
| 50 | Persistence health after tests | LIVE PASS | B | All three final databases returned `quick_check=ok` |
| 51 | Sanitized log review | LIVE PASS | B | Final logs clean; no final-candidate traceback or lock storm |

## Automated validation

| Validation | Result | Final evidence |
|---|---|---|
| Scaramouche full suite | PASS | 664 passed, 1 skipped, 1 LibreSSL warning |
| Wanderer full suite | PASS | 212 passed, 1 skipped |
| Focused Scaramouche message pipeline | PASS | 28 passed |
| Focused Wanderer release hardening | PASS | 12 passed before the privacy addition; new privacy regression also passed in the full suite |
| Compile checks | PASS | `bot.py` and `memory.py` compiled in both repositories |
| Diff checks | PASS | `git diff --check` clean in both repositories |
| Cross-process shared SQLite | PASS | 500 composite operations, zero lock errors |
| Accelerated soak | PASS | 600 turns; healthy databases and bounded state |

## Cleanup and safety state

- Synthetic fake-credential, disposable-memory, and duo markers were removed from
  the staging databases after verification.
- All three databases passed integrity checks after cleanup.
- No real password, token, or private credential was placed in Discord test text.
- No Discord permissions or guild cosmetics were changed.
- No paid cloud resource was created, and no PR was merged.
- Voice Gate B and real integrations remain out of scope.

## Release decision

The exercised Gate A code is **READY_FOR_THE_REMAINING_STAGING_CHECKS**. It is not
yet a full `STAGING_GATE_A_PASS` because tests 10, 17, 20, 24–31, 39–47 still lack
applicable live evidence. The permission and chaos rows may proceed only after the
owner explicitly approves those exact reversible Discord mutations at action time.

**Final state: `STAGING_GATE_A_BLOCKED` (`BLOCKED_BY_APPROVAL_AND_TEST_IDENTITY`)**
