"""
voice_handler.py — Fish Audio TTS (definitive correct implementation)

Voice: https://fish.audio/m/fb95ab47841a4db189cb35fb619d4ea1

BEFORE THIS WILL WORK — do this once:
  1. Go to https://fish.audio/m/fb95ab47841a4db189cb35fb619d4ea1
  2. Click "Add to My Voices"
  Without this step, the API ignores the reference_id.

Correct API call (from official docs):
  POST https://api.fish.audio/v1/tts
  Headers:
    authorization: Bearer YOUR_KEY
    content-type: application/msgpack
    model: s2-pro               ← THIS WAS MISSING. Without it = wrong voice.
  Body: ormsgpack.packb({ text, reference_id, format, ... })
"""

import io
import asyncio
import httpx
import ormsgpack

SCARAMOUCHE_VOICE_ID = "fb95ab47841a4db189cb35fb619d4ea1"
FISH_API_URL         = "https://api.fish.audio/v1/tts"


def _fish_tts_blocking(text: str, api_key: str) -> bytes | None:
    """
    Synchronous Fish Audio call using httpx + ormsgpack.
    Runs in a thread pool so it doesn't block Discord's event loop.
    """
    try:
        payload = ormsgpack.packb({
            "text":         text[:1500],
            "reference_id": SCARAMOUCHE_VOICE_ID,
            "format":       "mp3",
            "mp3_bitrate":  192,
            "latency":      "balanced",
            "normalize":    True,
            "chunk_length": 200,
        })

        headers = {
            "authorization": f"Bearer {api_key}",
            "content-type":  "application/msgpack",
            "model":         "s2-pro",   # required — selects the voice model engine
        }

        with httpx.Client(timeout=30) as client:
            resp = client.post(FISH_API_URL, content=payload, headers=headers)

        if resp.status_code == 200:
            audio = resp.content
            print(f"[Fish Audio] ✓ {len(audio):,} bytes")
            return audio
        else:
            print(f"[Fish Audio] HTTP {resp.status_code}: {resp.text[:300]}")
            return None

    except Exception as e:
        print(f"[Fish Audio] {type(e).__name__}: {e}")
        return None


async def generate_tts_fish_audio(text: str, api_key: str) -> bytes | None:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _fish_tts_blocking, text, api_key)


async def generate_tts_gtts(text: str) -> bytes | None:
    """Fallback — generic Google TTS, no API key."""
    try:
        from gtts import gTTS
        buf = io.BytesIO()
        gTTS(text=text[:800], lang="en", slow=False).write_to_fp(buf)
        buf.seek(0)
        return buf.read()
    except Exception as e:
        print(f"[gTTS] {e}")
        return None


async def get_audio(text: str, fish_audio_key: str) -> bytes | None:
    if fish_audio_key:
        audio = await generate_tts_fish_audio(text, fish_audio_key)
        if audio:
            return audio
        print("[Voice] Fish Audio failed — falling back to gTTS")
    return await generate_tts_gtts(text)


async def get_audio_mooded(text: str, fish_audio_key: str, mood: int = 0) -> bytes | None:
    """
    Mood-adjusted TTS. Low mood = slower, colder. High mood = slightly warmer.
    Adjusts chunk_length and mp3_bitrate as proxies for pacing.
    """
    if not fish_audio_key:
        return await generate_tts_gtts(text)

    # Map mood (-10 to +10) to chunk_length (100=slower/deliberate, 250=normal, 350=faster)
    if mood <= -6:
        chunk = 120   # Very slow, deliberate, cold
    elif mood <= -1:
        chunk = 180   # Measured contempt
    elif mood <= 5:
        chunk = 220   # Neutral
    else:
        chunk = 260   # Slightly less clipped — almost tolerable

    try:
        import ormsgpack, httpx

        payload = ormsgpack.packb({
            "text":         text[:1500],
            "reference_id": SCARAMOUCHE_VOICE_ID,
            "format":       "mp3",
            "mp3_bitrate":  192,
            "latency":      "balanced",
            "normalize":    True,
            "chunk_length": chunk,
        })
        headers = {
            "authorization": f"Bearer {fish_audio_key}",
            "content-type":  "application/msgpack",
            "model":         "s2-pro",
        }
        with httpx.Client(timeout=30) as client:
            resp = client.post(FISH_API_URL, content=payload, headers=headers)
        if resp.status_code == 200:
            return resp.content
        return await generate_tts_gtts(text)
    except Exception as e:
        print(f"[Fish Audio Mooded] {e}")
        return await generate_tts_gtts(text)

