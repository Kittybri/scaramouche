from __future__ import annotations

from .http import AsyncJSONClient, IntegrationAuthError


class GitHubIssueService:
    def __init__(self, config: dict, client: AsyncJSONClient | None = None):
        self.token = str(config.get("token") or "").strip()
        self.allowed = {str(repo).lower() for repo in config.get("allowed_repositories", []) if repo}
        self.dry_run = bool(config.get("dry_run", True))
        self.client = client or AsyncJSONClient()

    @property
    def ready(self) -> bool:
        return bool(self.token and self.allowed)

    async def create_issue(self, repository: str, title: str, body: str, *, confirmed: bool = False) -> dict:
        repository = (repository or "").strip().lower()
        if repository not in self.allowed:
            raise PermissionError("repository is not allowlisted")
        payload = {"title": (title or "").strip()[:180], "body": (body or "").strip()[:5000]}
        if not payload["title"]:
            raise ValueError("issue title is required")
        if self.dry_run or not confirmed:
            return {"dry_run": True, "repository": repository, **payload}
        if not self.token:
            raise IntegrationAuthError("GitHub token is not configured")
        result = await self.client.request(
            "POST", f"https://api.github.com/repos/{repository}/issues",
            headers={"Authorization": f"Bearer {self.token}", "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"},
            json=payload,
        )
        return {"dry_run": False, "number": int(result.get("number", 0)), "url": str(result.get("html_url", ""))}
