"""
voice_handler.py — TTS via Fish Audio SDK (correct usage)

Voice: https://fish.audio/m/fb95ab47841a4db189cb35fb619d4ea1

IMPORTANT: In Railway, set the variable name as FISH_API_KEY
(the SDK reads this automatically)
"""

import io
import asyncio

SCARAMOUCHE_VOICE_ID = "fb95ab47841a4db189cb35fb619d4ea1"


def _fish_tts_blocking(text: str, api_key: str) -> bytes | None:
    try:
        from fishaudio import FishAudio
        from fishaudio.types import TTSConfig

        # Pass api_key explicitly so we control the env var name
        client = FishAudio(api_key=api_key)

        # reference_id MUST go inside TTSConfig, not convert() directly
        config = TTSConfig(
            reference_id=SCARAMOUCHE_VOICE_ID,
            format="mp3",
            latency="balanced",
        )

        audio: bytes = client.tts.convert(text=text[:1500], config=config)
        print(f"[Fish Audio] Success — {len(audio)} bytes")
        return audio

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
