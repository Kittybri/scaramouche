# Live Staging Gate A Validation Record

Status: **STAGING_GATE_A_BLOCKED**  
Block reason: **BLOCKED_BY_ENVIRONMENT**  
Record date: **2026-09-30 (America/Los_Angeles)**

This record covers Discord text, two-bot runtime, restart, privacy deletion, and
server-chaos staging for the Scaramouche and Wanderer release candidates. Voice,
cloud-provider writes, home/device actions, Chromecast, Hue, Kasa, printing,
OwnTracks, and companion-computer actions were not started.

No Discord behavior is marked passed unless it was exercised against Discord.
The running Oracle services were inspected but are not treated as release-candidate
test evidence because neither service is running the commits under review.

## Candidate and environment identity

| Item | Recorded value |
|---|---|
| Scaramouche repository / PR | `Kittybri/scaramouche` / PR #22 (open, mergeable) |
| Scaramouche candidate | `d97e08eb1e29f249e619537a2019605bc2e99512` |
| Wanderer repository / PR | `Kittybri/Wanderer` / PR #9 (open, mergeable) |
| Wanderer candidate | `0667ff2f45230e379c5740ef3741cc12dff1b45d` |
| Candidate verification | Local HEAD, fetched remote branch, and GitHub PR head match for both repositories |
| Intended environment | Private Discord staging guild, two release processes, one local shared SQLite directory |
| Available Discord access | Human Discord account accessible; no private/disposable staging guild was identified for this gate |
| Available Oracle runtime | Two running Oracle Linux hosts, one active bot service per host |
| Available Python / discord.py | Python `3.9.25`; discord.py `2.7.0` in each deployment virtual environment |
| Active entrypoints | Scaramouche: legacy `/opt/scara-wanderer-bots/bot.py`; Wanderer: legacy `/opt/scara-wanderer-bots/wanderer_bot.py` |
| Deployment provenance | Flat deployment bundles with no `.git` metadata; release-candidate identity cannot be established |
| Scaramouche-host DB directory | `/opt/scara-wanderer-data` (`scaramouche.db`, `wanderer.db`, `shared_state.db`) |
| Wanderer-host DB directory | `/opt/scara-wanderer-data` (`wanderer.db`, a different `shared_state.db`) |
| Shared-runtime conclusion | The identically named databases are on different machines and are **not shared storage** |

Both GitHub PRs currently show a failed `Workers Builds` Cloudflare check. The
release audit established that neither Python Discord repository contains a
Cloudflare deployment, so this remains an external account/integration issue,
not evidence that the Discord runtime passed or failed staging.

## Preflight evidence

- **E1 — Candidate heads:** both local branches, fetched remote branches, and PR
  heads matched the exact candidate SHAs above on 2026-09-30.
- **E2 — Recoverable backups:** SQLite online backups were created before any
  possible live mutation. The Scaramouche host backup is
  `staging-gate-a-20261001T031318Z`; all three backup databases returned
  `PRAGMA quick_check = ok`. The Wanderer host backup is
  `staging-gate-a-20261001T031319Z`; its `wanderer.db` and `shared_state.db`
  returned `PRAGMA quick_check = ok`. File modes were restricted to `0600` and
  backup directories to `0700`.
- **E3 — Service health:** the Scaramouche service and Wanderer service were each
  active on their respective hosts with `NRestarts=0`. The other bot service was
  inactive on each host. No service was stopped or restarted during this gate.
- **E4 — Runtime provenance:** neither deployment directory is a Git checkout;
  active entrypoints are the legacy flat bundle, not the reviewed pair of release
  trees. Therefore login/ready observations from these processes cannot validate
  PR #22 or PR #9.
- **E5 — Persistence health:** every available source and backup SQLite database
  returned `PRAGMA quick_check = ok`. No persistent `database is locked`,
  traceback, or exception string was found in the preceding 24-hour service-log
  category scan. This is legacy-runtime health evidence only.
- **E6 — Feature configuration:** neither deployed environment exposed a
  `BOT_INTEGRATIONS_CONFIG` or equivalent chaos/home/cloud allowlist variable.
  The reviewed release code defaults server chaos off without an explicitly
  enabled guild configuration, but this could not be confirmed on a deployed
  release process.
- **E7 — Shared-state topology:** the two active bots use separate local files on
  separate Oracle hosts. SQLite cannot provide the requested cross-bot single
  state transition in this topology.
- **E8 — Automated release baseline:** before this staging attempt, Scaramouche
  passed 652 tests (1 skipped, one local LibreSSL warning), Wanderer passed 206
  tests (1 skipped), cross-process shared-state validation completed 500 composite
  operations with zero lock errors, and the 600-turn soak passed. These are
  **AUTOMATED**, not live Discord results.

## Findings

| ID | Severity | Finding | Required resolution |
|---|---|---|---|
| ENV-001 | ENVIRONMENT | Neither candidate commit is deployed; active services are unversioned legacy flat bundles. | Deploy both exact reviewed SHAs into an isolated staging runtime and record the deployed SHAs. |
| ENV-002 | ENVIRONMENT | The two active services use different local `shared_state.db` files on different hosts. | Co-locate both staging processes on one host and point both `MEMORY_DATA_DIR` values at the same local directory. Do not place SQLite on network storage. |
| ENV-003 | ENVIRONMENT | A private/disposable staging guild, channel, and dedicated test-user set were not identified. | Designate a private guild/channel and disposable test account before sending, deleting, or mutating Discord data. |
| ENV-004 | ENVIRONMENT | Chaos and other risky integration allowlists are not configured for a release staging runtime. | Start from an empty integration configuration; add only the designated staging guild and only during tests 42–48. |
| ENV-005 | MEDIUM | Both PRs show an unrelated failed Cloudflare Workers account check. | Disconnect or correct the Cloudflare repository integration in the Cloudflare dashboard; no repository code fix applies. |

No live release-candidate defect was reproduced, so no BLOCKER or HIGH code
finding is claimed. Conversely, absence of such a finding is not a live pass.

## Test record

Legend: `S` = Scaramouche candidate, `W` = Wanderer candidate, `B` = both.
Every row was recorded on 2026-09-30. Latency is `N/A` where an interaction was
not safely executed.

| # | Test | Mode | Result | Commit(s) | Environment / observed behavior | Evidence | Latency | Finding |
|---:|---|---|---|---|---|---|---|---|
| 1 | Verify exact commits | LIVE preflight | PASS | B | Local, fetched remote, and PR heads agree | E1 | N/A | — |
| 2 | Back up persistent state | LIVE preflight | PASS | legacy runtime | Non-overwriting online backups created; all backups healthy | E2 | N/A | — |
| 3 | Disable unrelated risky features | LIVE preflight | BLOCKED | B | Release runtime is not deployed; release feature state cannot be asserted | E4, E6 | N/A | ENV-001, ENV-004 |
| 4 | Confirm diagnostics | LIVE preflight | BLOCKED | B | Legacy service/DB diagnostics collected; release diagnostics unavailable | E3–E5 | N/A | ENV-001 |
| 5 | Launch Scaramouche candidate | LIVE | BLOCKED | S | Active service is not candidate `d97e08e` | E4 | N/A | ENV-001 |
| 6 | Launch Wanderer candidate | LIVE | BLOCKED | W | Active service is not candidate `0667ff2` | E4 | N/A | ENV-001 |
| 7 | Both candidates online | LIVE | BLOCKED | B | Legacy bots run separately; candidates not running together | E3, E4 | N/A | ENV-001 |
| 8 | Scaramouche DM | LIVE | BLOCKED | S | Not sent; would test legacy code and no staging target is designated | E4 | N/A | ENV-001, ENV-003 |
| 9 | Wanderer DM | LIVE | BLOCKED | W | Not sent; would test legacy code and no staging target is designated | E4 | N/A | ENV-001, ENV-003 |
| 10 | Cross-user isolation | LIVE | BLOCKED | B | Dedicated staging users unavailable | E4 | N/A | ENV-001, ENV-003 |
| 11 | Mention Scaramouche | LIVE | BLOCKED | S | No release candidate in designated staging guild | E4 | N/A | ENV-001, ENV-003 |
| 12 | Mention Wanderer | LIVE | BLOCKED | W | No release candidate in designated staging guild | E4 | N/A | ENV-001, ENV-003 |
| 13 | Reply attribution | LIVE | BLOCKED | B | No release candidate in designated staging guild | E4 | N/A | ENV-001, ENV-003 |
| 14 | Plain conversation eligibility | LIVE | BLOCKED | B | No release candidate in designated staging guild | E4 | N/A | ENV-001, ENV-003 |
| 15 | Harmless command per bot | LIVE | BLOCKED | B | Commands would exercise legacy deployment | E4 | N/A | ENV-001 |
| 16 | Command plus reply | LIVE | BLOCKED | B | Commands would exercise legacy deployment | E4 | N/A | ENV-001 |
| 17 | Command plus attachment | LIVE | BLOCKED | B | Commands would exercise legacy deployment | E4 | N/A | ENV-001 |
| 18 | Image attachment | LIVE | BLOCKED | B | Attachment not uploaded to an unidentified/non-release environment | E4 | N/A | ENV-001, ENV-003 |
| 19 | Text attachment | LIVE | BLOCKED | B | Attachment not uploaded to an unidentified/non-release environment | E4 | N/A | ENV-001, ENV-003 |
| 20 | Unsupported/oversized attachment | LIVE | BLOCKED | B | Attachment not uploaded to an unidentified/non-release environment | E4 | N/A | ENV-001, ENV-003 |
| 21 | Scaramouche serious arbitration | LIVE | BLOCKED | S | Release arbitration path not deployed | E4 | N/A | ENV-001 |
| 22 | Wanderer serious arbitration | LIVE | BLOCKED | W | Release arbitration path not deployed | E4 | N/A | ENV-001 |
| 23 | Disposable credential guard | LIVE | BLOCKED | B | Fake credential not sent because release path is absent | E4 | N/A | ENV-001, ENV-003 |
| 24 | View Channel removed | LIVE | BLOCKED | B | No disposable staging channel/role selected | E4 | N/A | ENV-001, ENV-003 |
| 25 | Send Messages removed | LIVE | BLOCKED | B | No disposable staging channel/role selected | E4 | N/A | ENV-001, ENV-003 |
| 26 | Read Message History removed | LIVE | BLOCKED | B | No disposable staging channel/role selected | E4 | N/A | ENV-001, ENV-003 |
| 27 | Embed Links removed | LIVE | BLOCKED | B | No disposable staging channel/role selected | E4 | N/A | ENV-001, ENV-003 |
| 28 | Attach Files removed | LIVE | BLOCKED | B | No disposable staging channel/role selected | E4 | N/A | ENV-001, ENV-003 |
| 29 | Add Reactions removed | LIVE | BLOCKED | B | No disposable staging channel/role selected | E4 | N/A | ENV-001, ENV-003 |
| 30 | Gateway/network reconnect | LIVE | BLOCKED | B | Restarting/disconnecting the legacy production-like services would not validate candidates | E3, E4 | N/A | ENV-001 |
| 31 | Repeated ready event | LIVE | BLOCKED | B | Candidate worker lifecycle not running | E4 | N/A | ENV-001 |
| 32 | Restart Scaramouche only | LIVE | BLOCKED | S | Would interrupt legacy bot, not the release candidate | E3, E4 | N/A | ENV-001 |
| 33 | Restart Wanderer only | LIVE | BLOCKED | W | Would interrupt legacy bot, not the release candidate | E3, E4 | N/A | ENV-001 |
| 34 | Restart both | LIVE | BLOCKED | B | Would interrupt legacy bots and still leave split shared state | E4, E7 | N/A | ENV-001, ENV-002 |
| 35 | Duo/shared interaction | LIVE | BLOCKED | B | Bots do not share one SQLite file | E7 | N/A | ENV-002 |
| 36 | Bot relationship write | LIVE | BLOCKED | B | Bots do not share one SQLite file | E7 | N/A | ENV-002 |
| 37 | Concurrent ordinary activity | LIVE | BLOCKED | B | Separate files cannot validate cross-process SQLite contention | E7 | N/A | ENV-002 |
| 38 | Seed disposable privacy state | LIVE | BLOCKED | B | Dedicated staging user/runtime unavailable | E4 | N/A | ENV-001, ENV-003 |
| 39 | Normal full deletion | LIVE | BLOCKED | B | No disposable release-candidate state was seeded | E4 | N/A | ENV-001, ENV-003 |
| 40 | Partial deletion failure | LIVE | BLOCKED | B | No isolated release-candidate subsystem/runtime exists | E4 | N/A | ENV-001, ENV-003 |
| 41 | Resume deletion | LIVE | BLOCKED | B | No pending staging deletion job exists | E4 | N/A | ENV-001, ENV-003 |
| 42 | Enable chaos explicitly | LIVE | BLOCKED | B | No candidate deployment or staging guild allowlist | E4, E6 | N/A | ENV-001, ENV-004 |
| 43 | Reversible cosmetic mutation | LIVE | BLOCKED | B | No disposable guild/channel selected | E6 | N/A | ENV-003, ENV-004 |
| 44 | Restart restoration | LIVE | BLOCKED | B | No release receipt can be created or recovered | E4, E6 | N/A | ENV-001, ENV-004 |
| 45 | Newer admin edit protection | LIVE | BLOCKED | B | No disposable setting target selected | E6 | N/A | ENV-003, ENV-004 |
| 46 | Interrupted restoration | LIVE | BLOCKED | B | No release receipt/runtime available | E4, E6 | N/A | ENV-001, ENV-004 |
| 47 | Two-bot chaos ownership | LIVE | BLOCKED | B | Candidates are absent and shared DB is split | E4, E7 | N/A | ENV-001, ENV-002 |
| 48 | Disable chaos and verify clean state | LIVE | BLOCKED | B | Chaos was never enabled; no staging mutation was made | E6 | N/A | ENV-004 |
| 49 | Task health after tests | LIVE | BLOCKED | B | Legacy services were healthy, but candidate tasks were never started | E3, E4 | N/A | ENV-001 |
| 50 | Persistence health after tests | LIVE | BLOCKED | B | Legacy DBs are healthy; candidate migrations/jobs were not exercised | E5 | N/A | ENV-001, ENV-002 |
| 51 | Sanitized log review | LIVE | BLOCKED | B | Legacy logs showed no tracebacks/exceptions/locks; no candidate live logs exist | E3–E5 | N/A | ENV-001 |

## Automated evidence retained (not LIVE)

| Validation | Mode | Result | Commit(s) | Evidence |
|---|---|---|---|---|
| Scaramouche full suite | AUTOMATED | PASS | `d97e08e` | 652 passed, 1 skipped, 1 LibreSSL warning |
| Wanderer full suite | AUTOMATED | PASS | `0667ff2` | 206 passed, 1 skipped |
| Cross-process shared SQLite | AUTOMATED | PASS | B | 500 composite operations, zero lock errors |
| Accelerated soak | AUTOMATED | PASS | S | 600 turns; healthy databases and bounded state |
| Compile/import, command uniqueness, secret scan, diff check | AUTOMATED | PASS | B | Recorded in `RELEASE_CANDIDATE_AUDIT.md` |

No code was changed as a result of this blocked live gate. Consequently no new
regression test was required and the already completed automated suites were not
duplicated.

## Exact recovery procedure for the next staging attempt

1. **Designate the isolated target first.** Record the private staging guild ID,
   disposable channel ID, bot role IDs, and two non-sensitive test users. Confirm
   the guild/channel may be cosmetically mutated and restored.
2. **Choose a safe bot identity strategy.** Prefer separate Discord staging bot
   applications. If production bot identities must be reused, schedule a
   maintenance window and stop the legacy services before starting staging so
   two processes never connect with one token.
3. **Use one staging machine for both processes.** Create separate immutable code
   directories for Scaramouche and Wanderer, but one local data directory, for
   example `/opt/scara-wanderer-staging/data`. Set `MEMORY_DATA_DIR` to that exact
   directory in both service environments. Never use NFS/network storage for
   SQLite.
4. **Pin source, do not copy the legacy flat bundle.** Check out Scaramouche
   `d97e08eb1e29f249e619537a2019605bc2e99512` and Wanderer
   `0667ff2f45230e379c5740ef3741cc12dff1b45d` into separate directories. Verify
   each with `git rev-parse HEAD` before installing dependencies.
5. **Create separate virtual environments.** Install each repository's
   `requirements.txt`; run that repository's compile/import checks and full test
   suite before its service is allowed to connect to Discord.
6. **Create locked service environment files.** Reuse existing secrets through
   protected environment files without printing them. Use distinct Discord bot
   tokens, the same local `MEMORY_DATA_DIR`, the correct owner/partner IDs, and
   no `BOT_INTEGRATIONS_JSON`/`BOT_INTEGRATIONS_CONFIG` at initial startup.
7. **Keep risky features off.** Start with fresh staging user rows (no proactive
   candidates), empty cloud/home/companion configuration, and no server-chaos
   guild allowlist. Add only the staging guild and the single feature under test
   immediately before tests 42–48.
8. **Back up and validate.** Run SQLite online backups of every staging DB, then
   `PRAGMA quick_check` on sources and backups. Record paths and hashes/sizes but
   no row contents.
9. **Start Scaramouche, then Wanderer.** Record each service's PID, start time,
   deployed SHA, `NRestarts`, ready event, migration/schema output, and supervised
   task count. Confirm both resolve the same `shared_state.db` inode/path.
10. **Obtain action-time approval before sending Discord messages or changing
    permissions/settings.** Then execute tests 8–51 in order, capturing only
    sanitized timestamps, response counts, latency, status, and finding IDs.
11. **Restore after each mutation.** Return permissions and cosmetic settings to
    their recorded original values, disable chaos, confirm no pending receipts,
    and run final SQLite quick checks before ending the maintenance window.
12. **Do not advance to Voice Gate B.** Gate A can become
    `STAGING_GATE_A_PASS` only after every applicable LIVE row above is replaced
    with an observed pass/not-configured result.

## Release decision

The code retains its prior **READY_FOR_STAGING** automated recommendation, but
Live Staging Gate A cannot pass against unversioned legacy services with split
SQLite state and no designated disposable Discord environment.

**Final state: `STAGING_GATE_A_BLOCKED` (`BLOCKED_BY_ENVIRONMENT`)**
