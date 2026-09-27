from __future__ import annotations

import time
from .http import AsyncJSONClient, IntegrationAuthError


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
            if token:
                return token
            raise IntegrationAuthError("Spotify authorization is not configured")
        import base64
        basic = base64.b64encode(f"{self.config.get('client_id','')}:{self.config.get('client_secret','')}".encode()).decode()
        result = await self.client.request(
            "POST", "https://accounts.spotify.com/api/token",
            headers={"Authorization": f"Basic {basic}", "Content-Type": "application/x-www-form-urlencoded"},
            data={"grant_type": "refresh_token", "refresh_token": refresh},
        )
        self.config["access_token"] = result.get("access_token", "")
        self.config["expires_at"] = time.time() + int(result.get("expires_in", 3600))
        return str(self.config["access_token"])

    async def _headers(self) -> dict:
        return {"Authorization": f"Bearer {await self._token()}"}

    async def currently_playing(self) -> dict | None:
        return await self.client.request("GET", "https://api.spotify.com/v1/me/player/currently-playing", headers=await self._headers(), expected=(200, 204))

    async def get_playlist(self, playlist_id: str) -> dict:
        return await self.client.request("GET", f"https://api.spotify.com/v1/playlists/{playlist_id}", headers=await self._headers())

    async def create_playlist(self, name: str, *, description: str = "Created by the Discord bot") -> dict:
        user_id = str(self.config.get("user_id") or "").strip()
        if not user_id:
            raise ValueError("Spotify user_id is not configured")
        return await self.client.request("POST", f"https://api.spotify.com/v1/users/{user_id}/playlists", headers=await self._headers(), json={"name":name[:100], "description":description[:300], "public":False})

    async def add_tracks(self, playlist_id: str, uris: list[str]) -> dict:
        clean = [uri for uri in uris[:100] if str(uri).startswith("spotify:track:")]
        if not clean:
            raise ValueError("no valid Spotify track URIs")
        return await self.client.request("POST", f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks", headers=await self._headers(), json={"uris":clean})
