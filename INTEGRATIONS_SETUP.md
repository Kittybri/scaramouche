# Optional integrations

All integrations are disabled unless one structured configuration is supplied through either
`BOT_INTEGRATIONS_CONFIG=/absolute/path/integrations.json` or `BOT_INTEGRATIONS_JSON='{...}'`.
Never commit that file or JSON value.
On the Oracle host, keep the file outside the repository when practical and restrict it with `chmod 600`.

```json
{
  "github": {"token": "...", "allowed_repositories": ["owner/repo"], "dry_run": true},
  "spotify": {"client_id": "...", "client_secret": "...", "access_token": "...", "refresh_token": "...", "expires_at": 0, "user_id": "..."},
  "google": {"accounts": {"DISCORD_USER_ID": {"client_id": "...", "client_secret": "...", "access_token": "...", "refresh_token": "...", "expires_at": 0, "allowed_spreadsheets": ["spreadsheet-id"]}}},
  "steam": {"api_key": "..."},
  "myanimelist": {"client_id": "..."}
}
```

Use the minimum scopes needed: GitHub Issues write on allowlisted repositories; Spotify current playback and playlist modify-private; Google Tasks, Calendar, or Sheets scopes only for enabled adapters. Google account entries are keyed by Discord user ID to prevent account crossover. Calendar creates and bot-event updates are previews unless the caller explicitly confirms them; arbitrary user events cannot be updated. Sheets writes require an allowlisted spreadsheet ID. No adapter exposes delete operations. Letterboxd scraping is deliberately unsupported.

`!integrations` shows readiness without printing credentials. `!githubissue owner/repo | title | body` previews; add `| confirm` and set GitHub `dry_run` to `false` for a real, rate-limited write.

Discord presence commentary requires the Presence Intent in the Developer Portal. Soundboard playback requires `SOUNDBOARD_GUILD_IDS`, `SOUNDBOARD_ASSETS_JSON`, Connect/Speak permissions, FFmpeg, PyNaCl, and davey; these Python voice dependencies are declared in `requirements.txt`. New-member interviews require `NEW_MEMBER_INTERVIEW_GUILD_IDS` and Create Public Threads permission. Priority Speaker cannot be toggled by discord.py during playback; assign the bot an appropriate role manually if desired.

The soundboard JSON maps only `sigh`, `scoff`, `slow_clap`, `buzzer`, `exhale`, or `chuckle` to local, authorized audio files. No audio asset is bundled. Restart the bot after changing configuration.
