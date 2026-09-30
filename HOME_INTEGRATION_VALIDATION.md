# Home Integration Validation

This checklist validates Repair Batch 9B without granting the language model new
device authority. Automated tests are mock-only. Every live step below is manual,
explicit, and should be performed with harmless devices in view.

## Architecture and trust boundaries

```text
Discord request
→ deterministic HomeBot command/proposal
→ signed HomeClient HTTPS RPC
→ hub identity + replay + policy authorization
→ optional atomic user/bot/guild confirmation
→ signed command over the registered agent's outbound WSS
→ independent agent replay + policy authorization
→ one registered provider/device
→ device-bound result
→ hub audit + bounded Discord result
```

The signed command's request ID, device, action, parameters, user, guild, bot,
trigger, issue time, and expiry remain unchanged. Confirmation changes only the
`confirmed` flag on a copied command. The hub correlates results to request, agent,
and device. Duplicate/stale results are ignored. Both sides default off and enforce
the registry independently.

The hub holds bot/agent/OwnTracks secrets, policy, zone configuration and short-lived
media. The LAN agent holds provider credentials and local device addresses. Discord
bots hold only their individual client secret and safe aliases. No provider secret,
IP, coordinate, audio body, screenshot, or arbitrary chat text belongs in audits or
diagnostics.

## Offline validation (no hardware actions)

Use private config copies with mode `0600`. The strict loader rejects duplicate JSON
keys, weak credentials, unknown agent references, inconsistent actions, invalid
hours/limits, bad Hue trust fields, bad Cast UUID/origin, and bad printer queues.

```sh
.venv-home/bin/python -m home.hub --config /etc/character-home/hub.json --validate
.venv-home/bin/python -m home_agent.agent --config /etc/character-home/agent.json --validate
```

These commands parse and validate only. They do not connect to a bridge, discover a
plug/Cast, print, inspect the screen, ingest location, or perform an action. Keep
`enabled:false` during this phase. Do not use `mock:true` in production; mock mode
still validates provider shapes but replaces all hardware adapters.

## Protocol, modes, triggers, and confirmation

- Envelopes are HMAC-SHA256, domain-separated, nonce/replay checked, and valid for at
  most 30 seconds over TLS/WSS.
- `DISABLED` permits nothing. `MANUAL` permits explicit allowlisted commands.
  `CONFIRM` proposes first. `AUTONOMOUS_SAFE` permits only configured bounded triggers.
- Triggers are `manual`, `proposal`, `autonomous`, `alarm`, and `roommate`.
- Natural language is a zero-LLM parser over explicit safe aliases. It only proposes.
- A proposal is bound to its exact request, user, bot, and guild and is consumed once.
- Printer notes always confirm. A `NEVER_AUTOMATE` Kasa device accepts manual requests
  only; proposal/autonomous/roommate/alarm requests are rejected.
- Brightness, volume, and color-temperature values outside policy are rejected, not
  silently clamped.

## Diagnostics, audit, and disable

In the owner's DM, use `!home status`, `!home devices`, `!home agent`,
`!home permissions`, and `!home audit`. Confirm that output contains only logical
IDs, modes/actions, enabled state, relay reachability, safe health categories,
heartbeat age, cooldown, and bounded results. It must never show local addresses,
keys, TLS pins, tokens, queue internals, coordinates, audio, or screenshots.

`!home test <device>` performs the provider's bounded test operation. It is still
authorized, cooldown-limited, device-scoped, and audited. `!home disable [device]`
persists at the hub, propagates to agents, cancels pending work best-effort, and is
reconciled after reconnect. `HOME_AUTOMATION_ENABLED=false` plus a service restart is
the deployment kill switch. A command already accepted by hardware cannot always be
undone.

Audits contain only request ID, time, bot, user ID, logical device, action, trigger,
and result. Failure categories are `permission`, `expired`, `offline`,
`provider_error`, `timeout`, `cancelled`, or the completion state. Default retention
is 30 days. Location state/events expire within one day; proposals expire promptly.

## Mock regression suite

```sh
.venv/bin/python -m pytest -q \
  tests/test_home_bridge.py tests/test_home_network.py \
  tests/test_home_providers.py tests/test_home_bot.py \
  tests/test_home_hardening.py tests/test_companion.py \
  tests/test_companion_bot.py tests/test_companion_macos.py \
  tests/test_companion_network.py
```

The loopback suite uses real HTTP/WebSocket transports and both bot identities but
only mock providers/fake desktop APIs. It covers HMAC/replay/expiry, confirmation,
wrong identity/guild, exact agent advertisement, request-agent-device correlation,
duplicates, reconnect, offline/timeout/disable, OwnTracks privacy, media capability
URLs, companion consent, and provider cleanup. It must never touch live hardware.

## Manual live-device checklist

Start with hub, agent, and each device disabled. Enable one reviewed device at a
time. Watch the physical device and audit after every action. Return it to disabled
when finished.

### Hue

1. Run both offline validators.
2. Start hub/agent and verify a healthy heartbeat without an action.
3. Run `!home test <hue-id>`; confirm an exact configured bridge/resource probe.
4. Manually request power on/off.
5. Request one in-range brightness, then verify an out-of-range value is rejected.
6. Request one configured color; verify an unlisted color is rejected.
7. Recall one static configured scene; verify an unlisted scene is rejected.
8. Disable the device and verify test/action denial plus relay reconciliation.
9. Disconnect the bridge and verify bounded offline/provider-error behavior.
10. Reconnect it and run test again; confirm no stale action replays.

### Kasa

1. Run both offline validators with an exact private host and optional child ID.
2. Run manual `test`; confirm no unrelated discovery target is selected.
3. Manually toggle one harmless reviewed outlet and verify the exact child only.
4. Keep category `NEVER_AUTOMATE`; verify proposal, autonomous, roommate, and alarm
   triggers are rejected even when confirmation is attempted.
5. Disconnect the outlet; verify bounded offline failure and cleanup.

Never register heating, medical, refrigeration, cooking, networking, computer-power,
or life-safety loads.

### Chromecast / Google Cast

1. Validate exact private host, UUID, media origin, and static asset allowlist.
2. Run `test`; confirm selection requires both configured UUID and host.
3. Cast one short approved asset/TTS proposal and confirm it explicitly.
4. Verify in-range volume and rejection above the configured maximum.
5. Stop the bot-started session; repeat stop to verify idempotence.
6. Make the media origin inaccessible and verify activation fails closed.
7. Disconnect the Cast and verify bounded offline behavior.
8. Reconnect the agent and confirm no stale audio begins. Confirm pause/stop never
   adopts media that another phone/app started.

### Printer

1. Validate the exact local CUPS queue name.
2. Run `test`; it may query printer attributes but must not submit a document.
3. Request one fixed template and verify user/bot/guild confirmation before printing.
4. Verify cooldown, one-copy/one-page behavior, and daily limit.
5. Stop the printer/CUPS queue and verify a bounded offline/print failure without
   automatic resubmission.

### OwnTracks

1. Verify HTTPS Basic authentication and rejection of bad/unknown accounts.
2. Opt in privately, enter one configured zone, and verify only a zone event appears.
3. Leave it and verify one debounced leave event.
4. Send a poor-accuracy fix and verify rejection.
5. Run `!home location delete`; verify opt-out and deletion of current/event state.
6. Inspect normal logs/database/prompt context and confirm no raw coordinates,
   addresses, inferred routine, or Basic credential is present.

### Computer companion

1. Enable in DM and verify explicit consent state.
2. Use the visible local kill switch; verify sensing/actions stop.
3. Send one fixed notification.
4. Play one bounded audio item, then stop it.
5. Launch one configured app alias.
6. Verify an unknown app alias is rejected and no shell is used.
7. Verify screen capture is off by default.
8. Opt into screen separately.
9. Use look once; confirm raw pixels are ephemeral and only the bounded summary returns.
10. Request lock and verify one exact confirmation before the local action.
11. Run unlink/delete; verify consent, presence, summaries, and device audit removal.
12. Take the relay offline; verify no retry/stale execution, then reconnect and check
    consent/disable reconciliation.

## Known limitations

There is one hub worker and one active non-stop action per agent, no durable action
queue or high availability, no continuous audio, and no device-state subscription.
Cast media URLs are short-lived bearer capabilities. Printer completion depends on
CUPS/device reporting. `test` proves a provider path, not long-term physical health.
Mock tests cannot establish real model/firmware compatibility, LAN routing, TLS
termination, audible playback, paper handling, or OS permissions; the manual checks
above are required before production enablement.
