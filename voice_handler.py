"""
voice_handler.py — TTS via Fish Audio official SDK (Scaramouche voice)

Voice: https://fish.audio/m/fb95ab47841a4db189cb35fb619d4ea1
SDK:   pip install fish-audio-sdk
"""

import io
import asyncio
from gtts import gTTS

SCARAMOUCHE_VOICE_ID = "fb95ab47841a4db189cb35fb619d4ea1"


async def generate_tts_fish_audio(text: str, api_key: str) -> bytes | None:
    try:
        from fishaudio import AsyncFishAudio
        client = AsyncFishAudio(api_key=api_key)
        audio = await client.tts.convert(
            text=text[:1500],
            reference_id=SCARAMOUCHE_VOICE_ID,
        )
        return audio
    except Exception as e:
        print(f"[Fish Audio] Error: {e}")
        return None


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
    if fish_audio_key:
        audio = await generate_tts_fish_audio(text, fish_audio_key)
        if audio:
            return audio
        print("[Voice] Fish Audio failed, falling back to gTTS")
    return await generate_tts_gtts(text)
