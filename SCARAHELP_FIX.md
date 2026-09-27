# Scaramouche help hotfix — 2026-09-27

## Confirmed live cause

The Oracle deployment's first help embed contained 29 fields. Its journal recorded
Discord HTTP 400 / 50035: `In embeds.0.fields: Must be 25 or fewer in length.`
That first send failed before the remaining pages could be sent. The repository
copy had fewer help entries and did not reproduce the live overflow, so the fix
was tested against the actual deployed help function as well as synthetic menus.

## Fix

`help_delivery.py` splits growing menus at 25 fields and 6,000 characters, then
batches within Discord's 10-embed/6,000-character message limits. It preserves the
source embeds using deep copies, retains every entry, suppresses mentions, and
falls back to complete chunked plain text without repeating successful batches.
Malformed/oversized individual entries use plain text instead of truncating them.

The live menu now produces four valid embeds with 25, 4, 20 and 24 fields,
preserving all 73 entries including tarot, RPG, duo and settings entries.

## Validation and deployment

- Six new pagination/fallback regression tests; focused help/character suite: 18 passed.
- Full Scaramouche suite: 166 passed; one pre-existing urllib3/LibreSSL warning.
- Isolated execution of the actual deployed help function passed locally and
  on Oracle's discord.py 2.7.0 environment, without LLM or Discord test messages.
- Server source was hash-checked before applying the patch to avoid overwriting
  concurrent edits. Only the three help sends were replaced, and the helper added.
- Original live `bot.py` backup: `/opt/scara-wanderer-bots/help-backup-ijYGcs/bot.py`.
- Scaramouche's service was restarted; no Wanderer changes or feature-batch deploys.

No PR was merged. Live chat delivery still needs the user's next `!scarahelp`
invocation; validation did not post synthetic messages into their server.

Reference: [Discord embed limits](https://docs.discord.com/developers/resources/message#embed-limits).
