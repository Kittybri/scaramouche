"""
voice_handler.py — Fish Audio TTS (official SDK, correct usage)

Voice: https://fish.audio/m/fb95ab47841a4db189cb35fb619d4ea1

Official SDK docs: https://github.com/fishaudio/fish-audio-python
  from fishaudio import FishAudio
  from fishaudio.types import TTSConfig
  client = FishAudio(api_key=...)
  config = TTSConfig(reference_id=..., format="mp3", latency="balanced")
  audio: bytes = client.tts.convert(text="...", config=config)
"""

import io
import asyncio

SCARAMOUCHE_VOICE_ID = "fb95ab47841a4db189cb35fb619d4ea1"


def _fish_tts_blocking(text: str, api_key: str) -> bytes | None:
    """
    Synchronous Fish Audio call — runs in a thread to avoid blocking Discord.
    Uses the correct import: fishaudio (not fish_audio_sdk).
    TTSConfig carries reference_id — do NOT pass it directly to convert().
    convert() returns bytes directly.
    """
    try:
        from fishaudio import FishAudio
        from fishaudio.types import TTSConfig

        client = FishAudio(api_key=api_key)

        config = TTSConfig(
            reference_id=SCARAMOUCHE_VOICE_ID,
            format="mp3",
            latency="balanced",
        )

        audio: bytes = client.tts.convert(text=text[:1500], config=config)

        if not audio:
            print("[Fish Audio] Empty response returned")
            return None

        print(f"[Fish Audio] Success — {len(audio)} bytes")
        return audio

    except Exception as e:
        print(f"[Fish Audio] {type(e).__name__}: {e}")
        return None


async def generate_tts_fish_audio(text: str, api_key: str) -> bytes | None:
    """Run the blocking SDK call in a thread pool executor."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _fish_tts_blocking, text, api_key)


async def generate_tts_gtts(text: str) -> bytes | None:
    """Fallback TTS — generic Google voice, no API key needed."""
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
    """Try Fish Audio first, fall back to gTTS only if it fails."""
    if fish_audio_key:
        audio = await generate_tts_fish_audio(text, fish_audio_key)
        if audio:
            return audio
        print("[Voice] Fish Audio failed — falling back to gTTS")
    return await generate_tts_gtts(text)
