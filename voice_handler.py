"""
voice_handler.py — Audio transcription (OpenAI Whisper) + TTS (Fish Audio)

Fish Audio "Wanderer ENG" voice model:
  ID: a47d67c3f6174379b8f158124ce80d0b
  Source: https://fish.audio/m/a47d67c3f6174379b8f158124ce80d0b/
  Built from Scaramouche/Wanderer's EN game voice lines by the community.

Get a Fish Audio API key (free tier available):
  https://fish.audio/developers/
"""

import os
import io
import tempfile
import asyncio
import aiohttp

# ── Hardcoded Scaramouche voice ID (Fish Audio "Wanderer ENG") ────────────────
SCARAMOUCHE_VOICE_ID = "a47d67c3f6174379b8f158124ce80d0b"
FISH_AUDIO_TTS_URL   = "https://api.fish.audio/v1/tts"


# ── Download helper ────────────────────────────────────────────────────────────
async def download_file(url: str) -> bytes:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            return await resp.read()


# ── STT: Whisper via OpenAI API ───────────────────────────────────────────────
async def transcribe_audio(audio_bytes: bytes, filename: str, openai_key: str) -> str:
    try:
        import openai
        client = openai.AsyncOpenAI(api_key=openai_key)
        ext = ("." + filename.rsplit(".", 1)[-1]) if "." in filename else ".ogg"
        with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as f:
            f.write(audio_bytes)
            tmp_path = f.name
        try:
            with open(tmp_path, "rb") as f:
                transcript = await client.audio.transcriptions.create(
                    model="whisper-1", file=f, response_format="text"
                )
            return str(transcript).strip()
        finally:
            os.unlink(tmp_path)
    except ImportError:
        return "[openai package not installed — pip install openai]"
    except Exception as e:
        return f"[Transcription failed: {e}]"


# ── TTS: Fish Audio ───────────────────────────────────────────────────────────
async def generate_tts_fish_audio(text: str, api_key: str) -> bytes | None:
    if not api_key:
        return None
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
    }
    payload = {
        "text":         text[:1500],
        "reference_id": SCARAMOUCHE_VOICE_ID,
        "format":       "mp3",
        "mp3_bitrate":  192,
        "latency":      "balanced",
        "temperature":  0.65,
        "top_p":        0.75,
        "chunk_length": 250,
        "normalize":    True,
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                FISH_AUDIO_TTS_URL, headers=headers, json=payload,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status == 200:
                    return await resp.read()
                body = await resp.text()
                print(f"[Fish Audio] HTTP {resp.status}: {body[:300]}")
                return None
    except asyncio.TimeoutError:
        print("[Fish Audio] Timed out")
        return None
    except Exception as e:
        print(f"[Fish Audio] Exception: {e}")
        return None


# ── TTS fallback: gTTS ────────────────────────────────────────────────────────
async def generate_tts_gtts(text: str) -> bytes | None:
    try:
        from gtts import gTTS
        buf = io.BytesIO()
        gTTS(text=text[:800], lang="en", slow=False).write_to_fp(buf)
        buf.seek(0)
        return buf.read()
    except ImportError:
        print("[gTTS] Not installed — pip install gtts")
        return None
    except Exception as e:
        print(f"[gTTS] Exception: {e}")
        return None


# ── Public entry point ────────────────────────────────────────────────────────
async def get_audio(text: str, fish_audio_key: str) -> bytes | None:
    """Try Fish Audio (Scaramouche voice) first, then gTTS fallback."""
    if fish_audio_key:
        audio = await generate_tts_fish_audio(text, fish_audio_key)
        if audio:
            return audio
        print("[Voice] Fish Audio failed, falling back to gTTS")
    return await generate_tts_gtts(text)


def is_audio_attachment(att) -> bool:
    ct = (att.content_type or "").lower()
    return (
        "audio" in ct
        or att.filename.lower().endswith((".ogg", ".mp3", ".wav", ".m4a", ".webm", ".flac"))
    )
