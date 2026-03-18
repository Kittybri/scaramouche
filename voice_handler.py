"""
voice_handler.py — TTS via Fish Audio (Scaramouche community voice)

Fish Audio "Wanderer ENG" voice model:
  ID: a47d67c3f6174379b8f158124ce80d0b
  Source: https://fish.audio/m/a47d67c3f6174379b8f158124ce80d0b/
  Built from Scaramouche's EN game voice lines by the community.

Get a free Fish Audio API key at: https://fish.audio/developers/
"""

import io
import asyncio
import aiohttp

SCARAMOUCHE_VOICE_ID = "fb95ab47841a4db189cb35fb619d4ea1"
FISH_AUDIO_TTS_URL   = "https://api.fish.audio/v1/tts"


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


async def get_audio(text: str, fish_audio_key: str) -> bytes | None:
    """Try Fish Audio (Scaramouche voice) first, then gTTS fallback."""
    if fish_audio_key:
        audio = await generate_tts_fish_audio(text, fish_audio_key)
        if audio:
            return audio
        print("[Voice] Fish Audio failed, falling back to gTTS")
    return await generate_tts_gtts(text)
