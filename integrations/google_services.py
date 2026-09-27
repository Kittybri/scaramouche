from __future__ import annotations

import time
from datetime import datetime
from .http import AsyncJSONClient, IntegrationAuthError


class _GoogleBase:
    def __init__(self, account: dict, client: AsyncJSONClient | None = None):
        self.account = dict(account)
        self.client = client or AsyncJSONClient()

    @property
    def ready(self) -> bool:
        return bool(self.account.get("access_token") or self.account.get("refresh_token"))

    async def _token(self) -> str:
        token = str(self.account.get("access_token") or "")
        if token and float(self.account.get("expires_at") or 0) > time.time() + 30:
            return token
        refresh = str(self.account.get("refresh_token") or "")
        if not refresh:
            if token:
                return token
            raise IntegrationAuthError("Google account is not authorized")
        result = await self.client.request("POST", "https://oauth2.googleapis.com/token", data={
            "client_id":self.account.get("client_id", ""), "client_secret":self.account.get("client_secret", ""),
            "refresh_token":refresh, "grant_type":"refresh_token",
        })
        self.account["access_token"] = result.get("access_token", "")
        self.account["expires_at"] = time.time() + int(result.get("expires_in", 3600))
        return str(self.account["access_token"])

    async def _headers(self) -> dict:
        return {"Authorization": f"Bearer {await self._token()}", "Content-Type":"application/json"}


class GoogleTasksService(_GoogleBase):
    async def list_tasks(self, tasklist: str = "@default") -> dict:
        return await self.client.request("GET", f"https://tasks.googleapis.com/tasks/v1/lists/{tasklist}/tasks", headers=await self._headers())

    async def create_task(self, title: str, *, notes: str = "", due: datetime | None = None, tasklist: str = "@default") -> dict:
        payload = {"title":title[:250], "notes":notes[:2000]}
        if due:
            if due.tzinfo is None:
                raise ValueError("due datetime must include a timezone")
            payload["due"] = due.isoformat()
        return await self.client.request("POST", f"https://tasks.googleapis.com/tasks/v1/lists/{tasklist}/tasks", headers=await self._headers(), json=payload)

    async def update_task(self, task_id: str, changes: dict, tasklist: str = "@default") -> dict:
        allowed = {key:value for key, value in changes.items() if key in {"title", "notes", "due", "status"}}
        if "due" in allowed:
            due = allowed["due"]
            if isinstance(due, datetime):
                if due.tzinfo is None:
                    raise ValueError("due datetime must include a timezone")
                allowed["due"] = due.isoformat()
            elif isinstance(due, str):
                parsed = datetime.fromisoformat(due.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    raise ValueError("due datetime must include a timezone")
            else:
                raise ValueError("due must be a timezone-aware datetime or ISO string")
        return await self.client.request("PATCH", f"https://tasks.googleapis.com/tasks/v1/lists/{tasklist}/tasks/{task_id}", headers=await self._headers(), json=allowed)


class GoogleCalendarService(_GoogleBase):
    async def upcoming(self, *, calendar_id: str = "primary", time_min: datetime) -> dict:
        if time_min.tzinfo is None:
            raise ValueError("time_min must include a timezone")
        from urllib.parse import quote, urlencode
        query = urlencode({"timeMin":time_min.isoformat(), "singleEvents":"true", "orderBy":"startTime", "maxResults":"20"})
        return await self.client.request("GET", f"https://www.googleapis.com/calendar/v3/calendars/{quote(calendar_id, safe='')}/events?{query}", headers=await self._headers())

    async def create_event(self, summary: str, start: datetime, end: datetime, *, calendar_id: str = "primary", description: str = "", confirmed: bool = False) -> dict:
        if start.tzinfo is None or end.tzinfo is None:
            raise ValueError("calendar datetimes must include a timezone")
        from urllib.parse import quote
        payload = {"summary":summary[:250], "description":description[:2000], "start":{"dateTime":start.isoformat()}, "end":{"dateTime":end.isoformat()},
                   "extendedProperties":{"private":{"created_by":"scara-wanderer-bots"}}}
        if not confirmed:
            return {"dry_run": True, "operation": "create_event", "payload": payload}
        return await self.client.request("POST", f"https://www.googleapis.com/calendar/v3/calendars/{quote(calendar_id, safe='')}/events", headers=await self._headers(), json=payload)

    async def update_bot_event(self, event_id: str, existing_event: dict, changes: dict, *, calendar_id: str = "primary", confirmed: bool = False) -> dict:
        marker = existing_event.get("extendedProperties", {}).get("private", {}).get("created_by")
        if marker != "scara-wanderer-bots":
            raise PermissionError("only bot-created events may be updated")
        from urllib.parse import quote
        allowed = {key:value for key, value in changes.items() if key in {"summary", "description", "start", "end"}}
        for key in ("start", "end"):
            value = allowed.get(key)
            if value and isinstance(value, dict) and value.get("dateTime"):
                parsed = datetime.fromisoformat(str(value["dateTime"]).replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    raise ValueError("calendar datetimes must include a timezone")
        if not confirmed:
            return {"dry_run": True, "operation": "update_event", "event_id": event_id, "changes": allowed}
        return await self.client.request("PATCH", f"https://www.googleapis.com/calendar/v3/calendars/{quote(calendar_id, safe='')}/events/{event_id}", headers=await self._headers(), json=allowed)


class GoogleSheetsService(_GoogleBase):
    async def append_score_rows(self, spreadsheet_id: str, rows: list[list[object]], *, range_name: str = "Scores!A:D") -> dict:
        if not spreadsheet_id or not rows:
            raise ValueError("spreadsheet and rows are required")
        allowed_ids = {str(item) for item in self.account.get("allowed_spreadsheets", []) if item}
        if spreadsheet_id not in allowed_ids:
            raise PermissionError("spreadsheet is not allowlisted for scoreboard writes")
        from urllib.parse import quote, urlencode
        query = urlencode({"valueInputOption":"USER_ENTERED", "insertDataOption":"INSERT_ROWS"})
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{quote(range_name, safe='')}:append?{query}"
        return await self.client.request("POST", url, headers=await self._headers(), json={"values":rows[:100]})
