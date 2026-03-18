"""
voice_handler.py — TTS via Fish Audio official SDK

Voice: https://fish.audio/m/fb95ab47841a4db189cb35fb619d4ea1
"""

import io
import asyncio
from gtts import gTTS

SCARAMOUCHE_VOICE_ID = "fb95ab47841a4db189cb35fb619d4ea1"


def _blocking_fish_tts(text: str, api_key: str) -> bytes | None:
    """
    Synchronous Fish Audio call, run in a thread so it doesn't block Discord.
    The SDK's convert() returns an iterable of audio chunks — we collect them all.
    """
    try:
        from fishaudio import FishAudio
        client = FishAudio(api_key=api_key)
        chunks = []
        for chunk in client.tts.convert(
            text=text[:1500],
            reference_id=SCARAMOUCHE_VOICE_ID,
            format="mp3",
            mp3_bitrate=192,
            latency="balanced",
            chunk_length=200,
        ):
            chunks.append(chunk)
        if not chunks:
            print("[Fish Audio] No audio chunks returned")
            return None
        return b"".join(chunks)
    except Exception as e:
        print(f"[Fish Audio] Error: {e}")
        return None


async def generate_tts_fish_audio(text: str, api_key: str) -> bytes | None:
    """Run the blocking Fish Audio SDK call in a thread pool."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _blocking_fish_tts, text, api_key)


async def generate_tts_gtts(text: str) -> bytes | None:
    try:
        buf = io.BytesIO()
        gTTS(text=text[:800], lang="en", slow=False).write_to_fp(buf)
        buf.seek(0)
        return buf.read()
    except Exception as e:
        print(f"[gTTS] Error: {e}")
        return None


async def get_audio(text: str, fish_audio_key: str) -> bytes | None:
    """Try Fish Audio (Scaramouche voice) first, fall back to gTTS."""
    if fish_audio_key:
        audio = await generate_tts_fish_audio(text, fish_audio_key)
        if audio:
            return audio
        print("[Voice] Fish Audio failed — falling back to gTTS")
    return await generate_tts_gtts(text)
