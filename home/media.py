"""Bounded ephemeral Cast audio capability URLs, stored in memory only."""

from __future__ import annotations
import base64
import secrets
import time
from urllib.parse import urlsplit
from .protocol import Rejected, secure_url


class AudioVault:
    def __init__(self, base_url, mock=False):
        self.base_url = secure_url(base_url, mock=mock)
        if urlsplit(self.base_url).path not in {"", "/"}:
            raise Rejected("audio_root_origin_required")
        self.items = {}

    def cleanup(self):
        now = time.time()
        self.items = {
            key: value for key, value in self.items.items() if value["expires"] > now
        }

    def add(self, encoded, mime, user_id, bot):
        self.cleanup()
        if (
            not isinstance(encoded, str)
            or len(encoded) > 7_000_000
            or mime not in {"audio/mpeg", "audio/wav", "audio/ogg"}
        ):
            raise Rejected("invalid_audio")
        try:
            data = base64.b64decode(encoded, validate=True)
        except Exception:
            raise Rejected("invalid_audio")
        valid = mime == "audio/mpeg" and (
            data.startswith(b"ID3")
            or (len(data) > 2 and data[0] == 255 and data[1] & 224 == 224)
        )
        valid |= (
            mime == "audio/wav" and data.startswith(b"RIFF") and data[8:12] == b"WAVE"
        )
        valid |= mime == "audio/ogg" and data.startswith(b"OggS")
        if not valid or not 16 <= len(data) <= 5 * 1024 * 1024 or len(self.items) >= 20:
            raise Rejected("invalid_audio")
        key = secrets.token_hex(24)
        self.items[key] = {
            "data": data,
            "mime": mime,
            "expires": time.time() + 120,
            "user_id": user_id,
            "bot": bot,
            "reads": 0,
        }
        return key

    def resolve(self, key, user_id, bot):
        self.cleanup()
        value = self.items.get(key)
        if not value or value["user_id"] != user_id or value["bot"] != bot:
            raise Rejected("expired_or_unauthorized_audio")
        return {
            "url": self.base_url + "/audio/" + key,
            "mime": value["mime"],
            "expires": value["expires"],
        }

    def fetch(self, key):
        self.cleanup()
        value = self.items.get(key)
        if not value or value["reads"] >= 20:
            raise Rejected("expired_audio")
        value["reads"] += 1
        return value
