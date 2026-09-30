from __future__ import annotations

import time
from .http import AsyncJSONClient, IntegrationAuthError, IntegrationError


class SpotifyService:
    def __init__(self, config: dict, client: AsyncJSONClient | None = None):
        self.config = dict(config)
        self.client = client or AsyncJSONClient()

    @property
    def ready(self) -> bool:
        return bool(self.config.get("access_token") or (self.config.get("refresh_token") and self.config.get("client_id") and self.config.get("client_secret")))

    async def _token(self) -> str:
        token = str(self.config.get("access_token") or "")
        if token and float(self.config.get("expires_at") or 0) > time.time() + 30:
            return token
        refresh = str(self.config.get("refresh_token") or "")
        if not refresh:
            raise IntegrationAuthError("Spotify authorization is not configured")
        if not self.config.get("client_id") or not self.config.get("client_secret"):
            raise IntegrationAuthError("Spotify refresh credentials are incomplete")
        import base64
        basic = base64.b64encode(f"{self.config.get('client_id','')}:{self.config.get('client_secret','')}".encode()).decode()
        try:
            result = await self.client.request(
                "POST", "https://accounts.spotify.com/api/token",
                headers={"Authorization": f"Basic {basic}", "Content-Type": "application/x-www-form-urlencoded"},
                data={"grant_type": "refresh_token", "refresh_token": refresh},
            )
        except IntegrationError as exc:
            raise IntegrationAuthError("Spotify token refresh was rejected") from exc
        self.config["access_token"] = result.get("access_token", "")
        if not self.config["access_token"]:
            raise IntegrationAuthError("Spotify token refresh returned no access token")
        self.config["expires_at"] = time.time() + int(result.get("expires_in", 3600))
        return str(self.config["access_token"])

    async def _headers(self) -> dict:
        return {"Authorization": f"Bearer {await self._token()}"}

    async def currently_playing(self) -> dict | None:
        return await self.client.request("GET", "https://api.spotify.com/v1/me/player/currently-playing", headers=await self._headers(), expected=(200, 204))

    async def get_playlist(self, playlist_id: str) -> dict:
        from urllib.parse import quote
        if not str(playlist_id).strip():
            raise ValueError("playlist_id is required")
        return await self.client.request("GET", f"https://api.spotify.com/v1/playlists/{quote(str(playlist_id), safe='')}", headers=await self._headers())

    async def create_playlist(self, name: str, *, description: str = "Created by the Discord bot") -> dict:
        user_id = str(self.config.get("user_id") or "").strip()
        if not user_id:
            raise ValueError("Spotify user_id is not configured")
        return await self.client.request("POST", f"https://api.spotify.com/v1/users/{user_id}/playlists", headers=await self._headers(), json={"name":name[:100], "description":description[:300], "public":False})

    async def add_tracks(self, playlist_id: str, uris: list[str]) -> dict:
        from urllib.parse import quote
        if not str(playlist_id).strip():
            raise ValueError("playlist_id is required")
        if len(uris) > 100:
            raise ValueError("Spotify accepts at most 100 tracks per request")
        requested = [str(uri).strip() for uri in uris if str(uri).strip()]
        clean = [uri for uri in requested if uri.startswith("spotify:track:") and len(uri) > len("spotify:track:")]
        if not clean or len(clean) != len(requested):
            raise ValueError("all Spotify URIs must use spotify:track:")
        return await self.client.request("POST", f"https://api.spotify.com/v1/playlists/{quote(str(playlist_id), safe='')}/tracks", headers=await self._headers(), json={"uris":clean})
