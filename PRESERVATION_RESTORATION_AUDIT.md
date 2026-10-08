# Preservation and command restoration audit — 2026-10-07

## Scope and verdict
Preservation infrastructure is implemented for scaramouche. No Google Production,
Phase 2, OAuth configuration, voice architecture, database architecture, or
personality redesign is included. No PR is merged.

The historical registration/name comparison is complete for the recorded known
Oracle snapshots (12 SHA-256-identified Python sources each) and current imported
runtime, including dynamically installed command groups and event listeners.
It is **NOT a complete functional restoration** or proof that every inherited
command works safely. Keep command-surface restoration OPEN while the omissions
below remain. Do not call the current reduced surface the entire intended feature set.
Google Production work remains paused.

## Safe repairs in this batch
- Both bots: status / aistatus / bot-specific status alias restored using current
  provider observations. No API probe, provider switch, secret or raw exception
  output. Owner detail goes to DM only; normal replies retain distinct voices.
- Scaramouche: botban / banfrombot and botunban / unbanfrombot resolve to the current
  permission-checked mute/unmute callbacks (bot-local silence, not Discord bans).
  bothealth resolves to current owner-only taskhealth. Old unsafe handlers not copied.
- Both help menus retain their original character-authored content and gain a
  runtime public command/alias/slash index. Owner controls remain separately
  classified. Wanderer now paginates rich help within Discord limits.
- Tarot is unchanged: no old engine snapshot replacement, artwork regeneration,
  history migration or loss. Existing 20 tarot regressions exercise all spreads,
  full Celtic output, settings/history, user isolation, deletion revocation,
  credential/high-stakes guards and prefix/slash registration.

## Durable protection
Root AGENTS.md plus scoped instructions under connections, tests, home,
voice_conversation and preservation. Flat privacy/character/tarot modules stay in
place and are covered by root instructions/CODEOWNERS/tests. CODEOWNERS routes
review to @Kittybri (including itself, rules, manifests, privacy, voice,
Connected Accounts and personality). No branch protection or ruleset changed.

preservation/commands.json is the reviewed runtime-supported expectation, with
unresolved historical surfaces retained explicitly. It covers canonical qualified
prefix commands, aliases, slash groups/subcommands, callback modules, cogs,
extensions, event listeners, access partitions and help entries.
preservation/features.json contains verified component classes, callable wiring,
required commands and privacy stages. Missing features are NOT marked PRESENT.
preservation/historical_audit.json records every historical command, alias, slash
subcommand and help token with disposition/evidence/current equivalent. No biometric
removal is labeled intentional merely because the command disappeared.

CI imports the actual bot with synthetic keys and disposable stores; removal
mutation tests detect prefix/alias/slash/group/module/cog/extension/listener/help/
privacy-wiring loss. A Git-base diff gate requires an explicit intentional-change
record for manifest removals. Human review must validate that authorization.
Neither instructions nor CODEOWNERS alone make silent removal impossible.

## Current manifest counts
- Canonical prefix entries (including subcommands): 172
- Alias declarations: 54
- Slash entries including groups: 5
- Public help tokens: 209
- Owner canonical entries: 20
- Runtime cogs/extensions: 0/0; these builds use
  direct/dynamic installers, not the old Cog loader.
- Feature families: tarot, rpg/games, party_games, birthdays, voice, connected_accounts, calendar/tasks, privacy_deletion, memory, relationships, duo, search, home/device, media/vision, document_tools, recovery/admin, provider_status, cloud_integrations, command_help, character.

## Known rename and equivalents
The feature manifest also explicitly tracks dashboard availability: missing in
Scaramouche, registered with callable slash handlers in Wanderer.

NSFW -> Unrestricted is user-authorized, backed by rename history and current
channel/age gates. No NSFW alias is reintroduced. Scaramouche bothealth now uses
taskhealth; persistence/build supplement owner diagnostics. selfbackup is only a
partial substitute for historical backupmemory: no shared-secret backup is added.
Grudges/worldprefs are not substitutes for world/cases/worldadd; January-3
character birthday behavior is not user birthday registration/greetings.

## Unresolved legacy canonical commands
- achievements — ACCIDENTALLY_MISSING: No equivalent fully wired handler found. Scaramouche lacks old achievement/world methods; partial bulk sync would delete other remote subcommands. Retain as unresolved until complete groups can be restored with current privacy/channel scope and command admission.
- backupmemory — UNSAFE_TO_RESTORE_AS_IS: selfbackup backs up local memory, not the old local-plus-shared recovery contract; not an equivalent rename. Restore requires approved shared-store backup/encryption/retention handling without copying Connected Accounts secrets.
- banchannel — UNSAFE_TO_RESTORE_AS_IS: Legacy owner/admin paths expose stored user/server data or mutate blocks/server membership; prerequisites, current persisted authorization and confirmation routes are absent in Scaramouche. No broad old-admin import.
- bannedchannels — UNSAFE_TO_RESTORE_AS_IS: Legacy owner/admin paths expose stored user/server data or mutate blocks/server membership; prerequisites, current persisted authorization and confirmation routes are absent in Scaramouche. No broad old-admin import.
- birthday — ACCIDENTALLY_MISSING: Registration AND local/shared birthday methods/checker missing. Character January-3 birthday_event is distinct, not user-birthday replacement. Safe restoration must reconcile historical shared_birthday_profiles/claims, quiet hours, deletion and delivery deduplication; not a command-only fix.
- blockall — UNSAFE_TO_RESTORE_AS_IS: Legacy owner/admin paths expose stored user/server data or mutate blocks/server membership; prerequisites, current persisted authorization and confirmation routes are absent in Scaramouche. No broad old-admin import.
- blockdm — UNSAFE_TO_RESTORE_AS_IS: Legacy owner/admin paths expose stored user/server data or mutate blocks/server membership; prerequisites, current persisted authorization and confirmation routes are absent in Scaramouche. No broad old-admin import.
- cases — ACCIDENTALLY_MISSING: No equivalent fully wired handler found. Scaramouche lacks old achievement/world methods; partial bulk sync would delete other remote subcommands. Retain as unresolved until complete groups can be restored with current privacy/channel scope and command admission.
- dmlist — UNSAFE_TO_RESTORE_AS_IS: Legacy owner/admin paths expose stored user/server data or mutate blocks/server membership; prerequisites, current persisted authorization and confirmation routes are absent in Scaramouche. No broad old-admin import.
- exportface — UNSAFE_TO_RESTORE_AS_IS: Private user-scoped face_controls generation guards replace legacy owner/global template access (Scaramouche 7ca9c84). Export/import removal authorization is not conclusively established; do not call it intentional solely from absence.
- fixdoc — UNSAFE_TO_RESTORE_AS_IS: Legacy handler overwrites Google Docs through a service account/remote helper without the current user-bound exact-confirmation flow. Restoring this here conflicts with explicit no Docs/Phase 2 scope.
- gamerank1 — UNSAFE_TO_RESTORE_AS_IS: Scaramouche RPG registration, engine and Memory RPG methods are absent. Legacy in-flight AI/button work lacks current deletion-generation fencing; global leaderboard can disclose across guilds. Requires adapted persistence, cancellation and arbitration, not blind snapshot import.
- importface — UNSAFE_TO_RESTORE_AS_IS: Private user-scoped face_controls generation guards replace legacy owner/global template access (Scaramouche 7ca9c84). Export/import removal authorization is not conclusively established; do not call it intentional solely from absence.
- leaveserver — UNSAFE_TO_RESTORE_AS_IS: Legacy owner/admin paths expose stored user/server data or mutate blocks/server membership; prerequisites, current persisted authorization and confirmation routes are absent in Scaramouche. No broad old-admin import.
- logs — UNSAFE_TO_RESTORE_AS_IS: Legacy owner/admin paths expose stored user/server data or mutate blocks/server membership; prerequisites, current persisted authorization and confirmation routes are absent in Scaramouche. No broad old-admin import.
- owneronly — UNSAFE_TO_RESTORE_AS_IS: Legacy owner/admin paths expose stored user/server data or mutate blocks/server membership; prerequisites, current persisted authorization and confirmation routes are absent in Scaramouche. No broad old-admin import.
- reban — UNSAFE_TO_RESTORE_AS_IS: Legacy owner/admin paths expose stored user/server data or mutate blocks/server membership; prerequisites, current persisted authorization and confirmation routes are absent in Scaramouche. No broad old-admin import.
- rebanchannels — UNSAFE_TO_RESTORE_AS_IS: Legacy owner/admin paths expose stored user/server data or mutate blocks/server membership; prerequisites, current persisted authorization and confirmation routes are absent in Scaramouche. No broad old-admin import.
- rebuildmemory — UNSAFE_TO_RESTORE_AS_IS: Legacy history replay can re-import forgotten text and relationship data. Requires deletion tombstones, credential/sensitive filters, scoped stored confirmation and per-user isolation before reinstatement; old callback is not a safe drop-in.
- rebuildrank — UNSAFE_TO_RESTORE_AS_IS: Legacy history replay can re-import forgotten text and relationship data. Requires deletion tombstones, credential/sensitive filters, scoped stored confirmation and per-user isolation before reinstatement; old callback is not a safe drop-in.
- rebuildrelationship — UNSAFE_TO_RESTORE_AS_IS: Legacy history replay can re-import forgotten text and relationship data. Requires deletion tombstones, credential/sensitive filters, scoped stored confirmation and per-user isolation before reinstatement; old callback is not a safe drop-in.
- rebuildstats — UNSAFE_TO_RESTORE_AS_IS: Legacy history replay can re-import forgotten text and relationship data. Requires deletion tombstones, credential/sensitive filters, scoped stored confirmation and per-user isolation before reinstatement; old callback is not a safe drop-in.
- rpg1 — UNSAFE_TO_RESTORE_AS_IS: Scaramouche RPG registration, engine and Memory RPG methods are absent. Legacy in-flight AI/button work lacks current deletion-generation fencing; global leaderboard can disclose across guilds. Requires adapted persistence, cancellation and arbitration, not blind snapshot import.
- rpg1reset — UNSAFE_TO_RESTORE_AS_IS: Scaramouche RPG registration, engine and Memory RPG methods are absent. Legacy in-flight AI/button work lacks current deletion-generation fencing; global leaderboard can disclose across guilds. Requires adapted persistence, cancellation and arbitration, not blind snapshot import.
- rpgstats1 — UNSAFE_TO_RESTORE_AS_IS: Scaramouche RPG registration, engine and Memory RPG methods are absent. Legacy in-flight AI/button work lacks current deletion-generation fencing; global leaderboard can disclose across guilds. Requires adapted persistence, cancellation and arbitration, not blind snapshot import.
- servers — UNSAFE_TO_RESTORE_AS_IS: Legacy owner/admin paths expose stored user/server data or mutate blocks/server membership; prerequisites, current persisted authorization and confirmation routes are absent in Scaramouche. No broad old-admin import.
- unbanchannel — UNSAFE_TO_RESTORE_AS_IS: Legacy owner/admin paths expose stored user/server data or mutate blocks/server membership; prerequisites, current persisted authorization and confirmation routes are absent in Scaramouche. No broad old-admin import.
- unblockdm — UNSAFE_TO_RESTORE_AS_IS: Legacy owner/admin paths expose stored user/server data or mutate blocks/server membership; prerequisites, current persisted authorization and confirmation routes are absent in Scaramouche. No broad old-admin import.
- whois — UNSAFE_TO_RESTORE_AS_IS: Legacy owner/admin paths expose stored user/server data or mutate blocks/server membership; prerequisites, current persisted authorization and confirmation routes are absent in Scaramouche. No broad old-admin import.
- world — ACCIDENTALLY_MISSING: No equivalent fully wired handler found. Scaramouche lacks old achievement/world methods; partial bulk sync would delete other remote subcommands. Retain as unresolved until complete groups can be restored with current privacy/channel scope and command admission.
- worldadd — ACCIDENTALLY_MISSING: No equivalent fully wired handler found. Scaramouche lacks old achievement/world methods; partial bulk sync would delete other remote subcommands. Retain as unresolved until complete groups can be restored with current privacy/channel scope and command admission.

## Security boundaries and precise remaining work
Scaramouche's RPG needs its missing engine and storage restored with durable
per-user deletion generations, post-generation checks, guarded/stale UI claims,
guild-scoped leaderboard behavior and structured interaction ownership. Reusing
the old async handlers could recreate state after a privacy reset.

Scaramouche user birthdays need reconciliation of historical shared profiles,
per-bot/year claims, quiet hours, retry/deduplication and deletion before enabling
a checker. Copying only the registration command would silently omit those behaviors.

Legacy Docs editing performs in-place provider writes via service-account/remote
helpers without Phase 1's per-user confirmation binding. No Docs setup, credentials
or provider calls are authorized here. Wanderer's inherited fixdoc remains
registered but is NOT certified safe by this manifest; do not mistake registration
coverage for provider-write validation.

History rebuild/recovery requires explicit scoped confirmation and deletion
tombstone checks so forgotten chat is not re-ingested. Older owner/admin controls
also need fail-closed owner configuration and current authorization/confirmation
checks before reinstatement. Older face import/export must not bypass user-scoped
enrollment generations or expose biometric templates.

Scaramouche's missing slash groups include dashboard/world/prefs/duo and direct
/scaramouche. Existing remote names must not be bulk-removed. Restore complete
groups against current command admission and scoped stores; do not replace them
with dummy unavailable callbacks or sync partial groups just to green a manifest.

## Validation limits
No human tarot button click, real game session, birthday delivery, provider write,
physical-device action, or voice session is claimed exercised in this batch.
Live service/registration checks and exact deployment evidence are recorded in the
canonical Scaramouche STAGING_VALIDATION.md once performed.

## Final local validation
- Full Scaramouche suite: 803 passed, 1 skipped, 1 warning (125.49s).
- Full Wanderer suite: 347 passed, 1 skipped, 1 warning (73.13s).
- Nine preservation tests per repository pass, including runtime import, mutation
  detection, audit coverage, help limits, distinct status presentation and permission
  boundary checks. Existing 20 tarot cases pass in each full suite.
- Optional local Opus roundtrip remains skipped; warning is urllib3/LibreSSL.
- Compile/import, command/alias/slash/module/listener/help/feature contracts, and
  git diff --check pass. Existing complete suites include privacy, user isolation,
  credential guards and arbitration coverage; this is not a claim of exhaustive
  live execution of all inherited features.
- Staged secret-pattern scan: zero findings. Whole tracked-file scan:
  Scaramouche 208 files, zero findings; Wanderer 139 files, one known fixture file
  (tests/test_release_hardening.py: numeric dummy key and bare private-key header
  used by credential-detector assertions). No real credential found.
- Initial full runs each caught an outdated AST help-test fixture lacking the new
  registry dependency. Fixed the fixture, retaining Google visibility/size assertions.
  A new permission-test selector was refined to use command decorators rather than
  a duplicated Python callback name. A dashboard contract correctly required the
  callable .callback on decorated slash Command objects. All final checks pass.
