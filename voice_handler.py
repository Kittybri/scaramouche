"""
voice_handler.py — TTS via Fish Audio (fish_audio_sdk package)

Voice: https://fish.audio/m/fb95ab47841a4db189cb35fb619d4ea1
"""

import io
import asyncio

SCARAMOUCHE_VOICE_ID = "fb95ab47841a4db189cb35fb619d4ea1"


def _fish_tts_blocking(text: str, api_key: str) -> bytes | None:
    """
    Uses fish_audio_sdk (with underscore) — the package that works via chunks.
    Exact usage from official docs:
      from fish_audio_sdk import Session, TTSRequest
      for chunk in session.tts(TTSRequest(text=..., reference_id=...)):
    """
    try:
        from fish_audio_sdk import Session, TTSRequest

        session = Session(api_key)
        chunks = []
        for chunk in session.tts(TTSRequest(
            text=text[:1500],
            reference_id=SCARAMOUCHE_VOICE_ID,
            format="mp3",
            latency="balanced",
        )):
            chunks.append(chunk)

        if not chunks:
            print("[Fish Audio] No chunks returned")
            return None

        result = b"".join(chunks)
        print(f"[Fish Audio] Success — {len(result)} bytes")
        return result

    except Exception as e:
        print(f"[Fish Audio] {type(e).__name__}: {e}")
        return None


async def generate_tts_fish_audio(text: str, api_key: str) -> bytes | None:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _fish_tts_blocking, text, api_key)


async def generate_tts_gtts(text: str) -> bytes | None:
    try:
        from gtts import gTTS
        buf = io.BytesIO()
        gTTS(text=text[:800], lang="en", slow=False).write_to_fp(buf)
        buf.seek(0)
        return buf.read()
    except Exception as e:
        print(f"[gTTS] Error: {e}")
        return None


async def get_audio(text: str, fish_audio_key: str) -> bytes | None:
    if fish_audio_key:
        audio = await generate_tts_fish_audio(text, fish_audio_key)
        if audio:
            return audio
        print("[Voice] Fish Audio failed — falling back to gTTS")
    return await generate_tts_gtts(text)
