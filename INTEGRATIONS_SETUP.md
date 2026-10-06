# Optional integrations

For Google Calendar/Tasks, use [Connected Accounts](CONNECTED_ACCOUNTS.md), not
manual per-user tokens. The legacy configuration instructions below remain relevant
to the other adapters and owner-only allowlisted Sheets, not Google onboarding.

All integrations are disabled unless one structured configuration is supplied through either
`BOT_INTEGRATIONS_CONFIG=/absolute/path/integrations.json` or `BOT_INTEGRATIONS_JSON='{...}'`.
Never commit that file or JSON value.
On the Oracle host, keep the file outside the repository when practical and restrict it with `chmod 600`.

See `CLOUD_INTEGRATIONS.md` for the current per-user configuration schema,
capability matrix, OAuth lifecycle, command reference, and live-validation
checklist. Google always uses explicit Discord-user account mappings. Spotify,
Steam, and MyAnimeList support the same mapping style; legacy single-account
sections are owner-only compatibility paths.

Use the minimum scopes needed: GitHub Issues write on allowlisted repositories; Spotify current playback and playlist modify-private; Google Tasks, Calendar, or Sheets scopes only for enabled adapters. Google account entries are keyed by Discord user ID to prevent account crossover. Calendar creates and bot-event updates are previews unless the caller explicitly confirms them; arbitrary user events cannot be updated. Sheets writes require an allowlisted spreadsheet ID. No adapter exposes delete operations. Letterboxd scraping is deliberately unsupported.

`!integrations` shows safe local readiness without printing credentials or calling
providers. `!integrations test` performs explicit read-only health checks.
`!githubissue owner/repo | title | body` creates a short-lived proposal; execute
the exact proposal with `!githubissue confirm REQUEST_ID`. GitHub `dry_run` must
also be `false` before a real issue can be created.

Discord presence commentary requires the Presence Intent in the Developer Portal. Soundboard playback requires `SOUNDBOARD_GUILD_IDS`, `SOUNDBOARD_ASSETS_JSON`, Connect/Speak permissions, FFmpeg, PyNaCl, and davey; these Python voice dependencies are declared in `requirements.txt`. New-member interviews require `NEW_MEMBER_INTERVIEW_GUILD_IDS` and Create Public Threads permission. Priority Speaker cannot be toggled by discord.py during playback; assign the bot an appropriate role manually if desired.

The soundboard JSON maps only `sigh`, `scoff`, `slow_clap`, `buzzer`, `exhale`, or `chuckle` to local, authorized audio files. No audio asset is bundled. Restart the bot after changing configuration.

`TATTLETALE_COOLDOWN_SECONDS` controls verified public-channel callbacks (default seven days, bounded to one hour–30 days).
