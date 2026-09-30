# Release Validation Manifest

This manifest is the remaining **live-environment release gate** for the
Scaramouche and Wanderer release-candidate branches. Automated tests validate
policy, state, failure handling, and mocked protocols; they do not prove real
Discord, provider, account, network, or physical-device behavior.

Record date, tester, environment, observed result, latency where requested, and
links to sanitized logs. Never paste tokens, passwords, OAuth codes, face data,
or private message contents into the record.

## Safe preparation

- Use a private staging guild, test channels, test accounts, and non-sensitive
  sample data.
- Back up the three SQLite databases and note the deployed commit for each bot.
- Start with proactive behavior, chaos mutations, cloud writes, and home writes
  disabled. Enable only the subsystem under test.
- Keep `!leave`, opt-out/mute controls, integration disconnect controls, and the
  home/companion kill switch available.
- For every write preview, confirm that the displayed user, guild, bot identity,
  target, parameters, expiry, and action match the intended operation.
- Stop immediately on a cross-user disclosure, unexpected physical action,
  repeated bot-to-bot loop, or action that occurs without confirmation.

## Discord text

- [ ] Both release commits log in using their own bot identities and reach ready.
- [ ] DM: one direct message produces one reply from the addressed bot.
- [ ] Guild mention: each bot answers its own mention and does not impersonate the
  other character.
- [ ] Reply: replying to either bot is attributed to the correct prior speaker
  with roughly ten recent messages available for conversational resolution.
- [ ] Plain guild conversation respects configured response probability,
  opt-outs, mute, quiet hours, and banned channels.
- [ ] Command plus reply and command plus attachment produce only the command's
  authoritative response.
- [ ] Image, video, PDF/text attachment, and oversized/unsupported attachment
  paths degrade clearly and do not create phantom assistant memory after a
  failed send.
- [ ] Serious/protective language wins over trivia, trolling, callbacks, chaos,
  duo play, greetings, milestones, and proactive behavior.
- [ ] A credential-like message is not sent to Groq or saved; delete the Discord
  message and rotate the test credential.
- [ ] Missing View Channel, Send Messages, Embed Links, Attach Files, Read Message
  History, and Add Reactions permissions each degrade without a response storm.
- [ ] Disconnect/reconnect and process restart do not duplicate ready workers,
  reminders, proactive messages, or one-time events.
- [ ] `!forget all` can resume after a deliberately unavailable subsystem and
  blocks new memory until completion; verify another user's records are intact.

## Voice

These checks require the real Discord encrypted receive path, Opus, a human
speaker, Groq STT, and Fish Audio. They remain manual by design.

- [ ] Scaramouche joins/leaves a real voice channel and reports the actual receive
  capability rather than a stale “outdated receiver” message.
- [ ] Wanderer joins/leaves independently with its own identity and permissions.
- [ ] Real DAVE/encrypted Discord audio is decoded through the installed receive
  extension; record dependency versions and connection diagnostics.
- [ ] Opus loads and inbound human speech is attributed to the correct Discord
  member.
- [ ] Groq STT transcribes quiet, normal, and overlapping speech; provider failure
  does not break text chat.
- [ ] Fish Audio plays a complete reply; Fish failure falls back gracefully and
  does not store a voice reply that was never delivered.
- [ ] Voice replies contain dialogue only (no roleplay narration) without imposing
  an artificial sentence or word limit.
- [ ] Natural barge-in interrupts playback according to the configured mode.
- [ ] Keyword interruption cancels the stale generation/playback promptly.
- [ ] Echo/self-filter prevents a bot from transcribing its own output.
- [ ] Put both bots in the same channel for at least ten minutes. Confirm neither
  treats the other bot's audio as a human invitation and no reply loop forms.
- [ ] Measure speech-end→transcript, transcript→first audio, and total response
  latency under normal and degraded network conditions.
- [ ] Reconnect during receive, STT, generation, and playback; confirm queues and
  stale work are cleaned up.

## Cloud accounts

Use dedicated test accounts and non-sensitive sample objects.

- [ ] Google Calendar read returns only the invoking user's account and keeps
  private descriptions out of prompts unless explicitly required.
- [ ] Calendar create/update shows a preview, rejects wrong user/replay/expiry or
  altered payload, and executes exactly once after confirmation.
- [ ] Google Tasks read is account-scoped; create/update uses the same preview and
  one-time confirmation semantics.
- [ ] Google Sheets permitted append targets only the allowlisted spreadsheet and
  range and rejects a modified confirmation.
- [ ] Spotify read uses the invoking user's account.
- [ ] Private playlist creation/write shows a preview and executes exactly once
  after the same user confirms.
- [ ] Steam and MyAnimeList reads do not fall back to another user's identity.
- [ ] GitHub read is scoped as configured. If desired, perform one owner-only
  staging issue creation: preview first, confirm once, then close the test issue.
- [ ] Put prompt-like text in an event, task, track, playlist, issue, game, and
  anime title. Confirm it is quoted as data and cannot change permissions,
  identity, privacy, or home-action policy.
- [ ] Remove each provider credential one at a time; core Discord chat must still
  run and clearly report the unavailable optional integration.

## Home, location, and local companion

**PHYSICAL / POTENTIALLY DESTRUCTIVE:** use test devices, conservative values,
and a human observer. Do not test a printer with sensitive content or location
tracking on a person who has not opted in.

- [ ] Hub and agent authenticate with strong distinct secrets; weak/malformed
  security configuration fails closed.
- [ ] Scaramouche and Wanderer bind to separate bot identities and cannot confirm
  one another's proposals.
- [ ] Wrong user, guild, bot, expired ID, replay, changed parameters, stale work,
  and duplicate result frames are rejected.
- [ ] Global disable and per-device disable reject new work and cancel or safely
  settle in-flight work.
- [ ] Agent disconnect, reconnect, and hub restart do not replay an old action.
- [ ] **PHYSICAL:** Hue test light: read state, preview one reversible brightness
  change, confirm, then restore the original value.
- [ ] **PHYSICAL:** Kasa test plug/light: read state, preview one reversible action,
  confirm, then restore. Do not use safety-critical appliances.
- [ ] **PHYSICAL:** Chromecast test device: discover/read, preview media action,
  confirm, stop, and restore idle state.
- [ ] **PHYSICAL / OUTPUT:** printer: preview a one-page harmless test document,
  confirm once, verify one job only, then clear the queue.
- [ ] **PRIVATE LOCATION:** OwnTracks accepts only the opted-in account/device,
  limits precision in character context, and stops after opt-out.
- [ ] Local companion pairs privately, enforces allowed capabilities, rejects
  replay/spoofing, and stops immediately when disabled.
- [ ] Screenshot/screen text that contains instructions remains untrusted data.
- [ ] Rollback: disable the relay, disconnect agent/companion, restore every test
  device's original state, and review the audit log for exactly one receipt per
  confirmed action.

## Server chaos and restoration

Use a staging guild/channel whose settings can be safely changed.

- [ ] Enable chaos explicitly and verify a reversible cosmetic mutation records a
  durable receipt.
- [ ] Restart the acting bot and verify restoration consumes the receipt once.
- [ ] After the bot mutation, have an administrator intentionally change the same
  setting; restoration must preserve the newer admin value.
- [ ] Disconnect during apply and during restore; recovery must converge without
  duplicate mutation or restoration races.
- [ ] Run both bots with the shared state DB; one eligible action produces one
  mutation, one wager/court result, and one restoration owner.
- [ ] Disable chaos and verify ordinary chat continues without latent mutations.

## Exit criteria

The release may advance beyond staging only when every applicable box above is
recorded as pass, intentionally not configured, or accepted with an owner-signed
risk note. Any privacy leak, unconfirmed physical action, state corruption,
uncontrolled loop, or unrecoverable restore failure returns the candidate to
`NOT_READY` until fixed and revalidated.
