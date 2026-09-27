from __future__ import annotations

from urllib.parse import urlencode
import time
from .http import AsyncJSONClient, IntegrationError


class SteamService:
    def __init__(self, config: dict, client: AsyncJSONClient | None = None):
        self.api_key = str(config.get("api_key") or "")
        self.client = client or AsyncJSONClient()
        self._cache: dict[str, tuple[float, dict]] = {}

    @property
    def ready(self) -> bool:
        return bool(self.api_key)

    async def recently_played(self, steam_id: str) -> dict:
        if not self.ready or not str(steam_id).isdigit():
            raise ValueError("Steam API key and explicit numeric steam_id are required")
        cached = self._cache.get(str(steam_id))
        if cached and time.time() - cached[0] < 900:
            return cached[1]
        query = urlencode({"key":self.api_key, "steamid":steam_id, "format":"json"})
        result = await self.client.request("GET", f"https://api.steampowered.com/IPlayerService/GetRecentlyPlayedGames/v1/?{query}")
        self._cache[str(steam_id)] = (time.time(), result)
        return result


class MyAnimeListService:
    def __init__(self, config: dict, client: AsyncJSONClient | None = None):
        self.client_id = str(config.get("client_id") or "")
        self.client = client or AsyncJSONClient()
        self._cache: dict[str, tuple[float, dict]] = {}

    @property
    def ready(self) -> bool:
        return bool(self.client_id)

    async def anime_list(self, username: str) -> dict:
        if not self.ready or not username.strip():
            raise ValueError("MyAnimeList client_id and explicit username are required")
        key = username.strip().lower()
        cached = self._cache.get(key)
        if cached and time.time() - cached[0] < 900:
            return cached[1]
        query = urlencode({"limit":"20", "fields":"list_status"})
        result = await self.client.request("GET", f"https://api.myanimelist.net/v2/users/{username.strip()}/animelist?{query}", headers={"X-MAL-CLIENT-ID":self.client_id})
        self._cache[key] = (time.time(), result)
        return result


class LetterboxdService:
    """Boundary only: Letterboxd has no generally available stable public API."""
    ready = False
    limitation = "No generally available supported API is configured; scraping is intentionally disabled."

    async def activity(self, username: str) -> dict:
        raise IntegrationError(self.limitation)
