"""Optional external-service adapters. All adapters are disabled when unconfigured."""

from .config import IntegrationConfig, load_integration_config
from .github_service import GitHubIssueService
from .spotify import SpotifyService
from .google_services import GoogleCalendarService, GoogleSheetsService, GoogleTasksService
from .activity_sources import SteamService, MyAnimeListService, LetterboxdService

__all__ = [
    "IntegrationConfig", "load_integration_config", "GitHubIssueService", "SpotifyService",
    "GoogleCalendarService", "GoogleSheetsService", "GoogleTasksService",
    "SteamService", "MyAnimeListService", "LetterboxdService",
]
