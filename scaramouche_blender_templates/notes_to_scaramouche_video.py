import argparse
import importlib.util
import json
import math
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import httpx
import imageio_ffmpeg
import ormsgpack
from dotenv import load_dotenv
from gtts import gTTS
from PIL import Image, ImageDraw, ImageEnhance, ImageFilter, ImageFont


ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_BLEND = ROOT_DIR / "scaramouche_presentation_templates_with_scaramouche.blend"
BOT_CODE_ROOT = ROOT_DIR.parent / "scara_wanderer_bots"
DEFAULT_BLENDER = Path(
    os.getenv("BLENDER_BIN")
    or (r"C:\Program Files\Blender Foundation\Blender 4.2\blender.exe" if os.name == "nt" else "/usr/bin/blender")
)
DEFAULT_PLATE_SCRIPT = ROOT_DIR / "render_scaramouche_plates.py"
DEFAULT_FISH_API_URL = "https://api.fish.audio/v1/tts"
FALLBACK_VOICE_ID = "fb95ab47841a4db189cb35fb619d4ea1"
SCENE_MOODS = {
    "01_Lecture_Explainer": "lecture",
    "02_Side_Screen_Breakdown": "lecture",
    "03_Close_Confession": "confession",
    "04_Mission_Briefing": "mission",
    "05_Duo_Debate": "debate",
}
SCARA_OPENERS = [
    "Pay attention.",
    "Try to keep up.",
    "I will make this simple enough for you to follow.",
]
SCARA_TRANSITIONS = [
    "Next point.",
    "Now for the part people usually get wrong.",
    "Continue.",
]
SCARA_CLOSERS = [
    "Remember that, or embarrass yourself later.",
    "Keep that in mind.",
    "You will need that in a moment.",
]
RESAMPLING = getattr(Image, "Resampling", Image)


@dataclass
class Section:
    title: str
    points: list[str]
    narration: str


def load_environment():
    root_env = Path(__file__).resolve().parent.parent / ".env"
    bot_env = BOT_CODE_ROOT / ".env"
    if root_env.exists():
        load_dotenv(root_env)
    if bot_env.exists():
        load_dotenv(bot_env)


def maybe_import_voice_handler():
    voice_path = BOT_CODE_ROOT / "voice_handler.py"
    if not voice_path.exists():
        return None
    spec = importlib.util.spec_from_file_location("scaramouche_voice_handler", voice_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def slugify(text: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", text.lower()).strip("-")
    return slug or "scaramouche-lesson"


def read_notes(notes_arg: str) -> str:
    candidate = Path(notes_arg)
    if candidate.exists() and candidate.is_file():
        text = candidate.read_text(encoding="utf-8", errors="ignore")
    else:
        text = notes_arg
    if "\\n" in text and "\n" not in text:
        text = text.replace("\\n", "\n")
    return text.lstrip("\ufeff")


def sentence_split(text: str) -> list[str]:
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return []
    parts = re.split(r"(?<=[.!?])\s+", text)
    return [part.strip() for part in parts if part.strip()]


def clean_point(text: str) -> str:
    text = text.strip().replace("â€¢", " ")
    text = re.sub(r"^(?:[-*]|\u2022|\d+[.)]?|\(?\d+\))\s*", "", text)
    return re.sub(r"\s+", " ", text).strip(" -")


def derive_title(notes: str) -> str:
    for line in notes.splitlines():
        stripped = line.strip().lstrip("\ufeff")
        if stripped.startswith("#"):
            return stripped.lstrip("#").strip()
        if len(stripped) > 8:
            return stripped[:80]
    return "Scaramouche Lesson"


def split_sections(notes: str, max_sections: int = 5) -> list[Section]:
    lines = notes.splitlines()
    raw_sections = []
    current_title = None
    current_body = []

    def flush():
        nonlocal current_title, current_body
        body_text = "\n".join(current_body).strip()
        if body_text:
            raw_sections.append((current_title or "", body_text))
        current_title = None
        current_body = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            if current_body:
                current_body.append("")
            continue
        if stripped.startswith("#"):
            flush()
            current_title = stripped.lstrip("#").strip()
        else:
            current_body.append(stripped)
    flush()

    if not raw_sections:
        blocks = [block.strip() for block in re.split(r"\n\s*\n", notes) if block.strip()]
        raw_sections = [("", block) for block in blocks]

    normalized = []
    for heading, body in raw_sections:
        points = []
        for line in body.splitlines():
            cleaned = clean_point(line)
            if cleaned:
                points.append(cleaned)
        if not points:
            points = sentence_split(body)
        if not points:
            continue
        title = heading or points[0][:50].rstrip(".")
        normalized.append((title, points))

    if not normalized:
        fallback_title = derive_title(notes)
        normalized = [(fallback_title, sentence_split(notes)[:5] or [notes.strip()])]

    while len(normalized) > max_sections:
        a_title, a_points = normalized[-2]
        b_title, b_points = normalized[-1]
        normalized[-2] = (a_title, a_points + b_points)
        normalized.pop()

    sections = []
    for idx, (title, points) in enumerate(normalized[:max_sections], start=1):
        title = title.strip() or f"Section {idx}"
        points = [point for point in points if point][:4]
        narration = build_narration(title, points, idx == 1)
        sections.append(Section(title=title, points=points, narration=narration))

    if len(sections) < 3:
        expanded = []
        for section in sections:
            if len(section.points) >= 4 and len(expanded) < 5:
                midpoint = math.ceil(len(section.points) / 2)
                first = section.points[:midpoint]
                second = section.points[midpoint:]
                expanded.append(Section(section.title, first, build_narration(section.title, first, len(expanded) == 0)))
                if second and len(expanded) < 5:
                    continued = f"{section.title} Continued"
                    expanded.append(Section(continued, second, build_narration(continued, second, False)))
            else:
                expanded.append(section)
        sections = expanded[:5]

    return sections


def build_narration(title: str, points: list[str], is_intro: bool) -> str:
    if is_intro:
        intro = f"{SCARA_OPENERS[0]} {title}."
    else:
        intro = f"{SCARA_TRANSITIONS[(len(title) + len(points)) % len(SCARA_TRANSITIONS)]} {title}."
    lines = [intro]
    for point in points[:3]:
        sentence = point.strip()
        if sentence and sentence[-1] not in ".!?":
            sentence += "."
        lines.append(sentence)
    lines.append(SCARA_CLOSERS[(len(title) + len(lines)) % len(SCARA_CLOSERS)])
    return " ".join(lines)


def ensure_output_dir(base_name: str) -> Path:
    out_dir = ROOT_DIR / "outputs" / slugify(base_name)
    suffix = 1
    final = out_dir
    while final.exists():
        suffix += 1
        final = ROOT_DIR / "outputs" / f"{slugify(base_name)}-{suffix}"
    final.mkdir(parents=True, exist_ok=True)
    return final


def font_candidates(*names: str) -> ImageFont.FreeTypeFont:
    font_dirs = [
        Path(r"C:\Windows\Fonts"),
        Path("/usr/share/fonts/truetype/dejavu"),
        Path("/usr/share/fonts/truetype/liberation2"),
        Path("/usr/share/fonts"),
    ]
    for name in names:
        for font_dir in font_dirs:
            candidate = font_dir / name
            if candidate.exists():
                return ImageFont.truetype(str(candidate), size=40)
    return ImageFont.load_default()


def wrap_text(draw: ImageDraw.ImageDraw, text: str, font, max_width: int) -> list[str]:
    words = text.split()
    if not words:
        return []
    lines = []
    current = words[0]
    for word in words[1:]:
        trial = f"{current} {word}"
        if draw.textbbox((0, 0), trial, font=font)[2] <= max_width:
            current = trial
        else:
            lines.append(current)
            current = word
    lines.append(current)
    return lines


def draw_vertical_gradient(image: Image.Image, top_rgb: tuple[int, int, int], bottom_rgb: tuple[int, int, int]):
    width, height = image.size
    draw = ImageDraw.Draw(image)
    for y in range(height):
        t = y / max(1, height - 1)
        color = tuple(int(top_rgb[idx] * (1 - t) + bottom_rgb[idx] * t) for idx in range(3))
        draw.line((0, y, width, y), fill=color)


def draw_scara_accent_shapes(draw: ImageDraw.ImageDraw):
    draw.ellipse((1220, -160, 1980, 580), fill=(104, 32, 48))
    draw.ellipse((1420, 120, 2040, 760), fill=(44, 14, 22))
    draw.rectangle((0, 0, 1920, 38), fill=(186, 62, 84))
    draw.rounded_rectangle((126, 108, 1794, 954), radius=42, fill=(15, 18, 29), outline=(195, 79, 98), width=3)
    draw.rounded_rectangle((132, 114, 1788, 948), radius=38, outline=(57, 23, 32), width=2)


def create_slide_images(title: str, sections: list[Section], slides_dir: Path) -> list[Path]:
    slides_dir.mkdir(parents=True, exist_ok=True)
    title_font = font_candidates("segoeuib.ttf", "arialbd.ttf")
    section_font = font_candidates("georgiab.ttf", "arialbd.ttf")
    body_font = font_candidates("segoeui.ttf", "arial.ttf")
    small_font = font_candidates("segoeui.ttf", "arial.ttf")

    slide_paths = []
    for idx, section in enumerate(sections, start=1):
        image = Image.new("RGB", (1920, 1080), (8, 10, 17))
        draw_vertical_gradient(image, (10, 12, 21), (36, 16, 24))
        draw = ImageDraw.Draw(image)
        draw_scara_accent_shapes(draw)

        draw.text((178, 162), title.upper(), font=title_font.font_variant(size=44), fill=(246, 242, 238))
        draw.rounded_rectangle((172, 236, 860, 310), radius=24, fill=(168, 56, 79))
        draw.text((208, 252), section.title, font=section_font.font_variant(size=34), fill=(251, 244, 240))
        draw.text((1520, 166), f"{idx:02d}", font=title_font.font_variant(size=72), fill=(240, 116, 136))
        draw.text((1548, 252), "Scaramouche Briefing", font=small_font.font_variant(size=26), fill=(203, 187, 192))

        y = 368
        for point in section.points:
            wrapped = wrap_text(draw, point, body_font.font_variant(size=34), 1260)
            box_height = 56 + (max(1, len(wrapped)) - 1) * 38
            draw.rounded_rectangle((176, y - 10, 1650, y + box_height), radius=26, fill=(25, 29, 43), outline=(82, 36, 48), width=2)
            draw.rounded_rectangle((194, y + 12, 230, y + 48), radius=12, fill=(189, 73, 96))
            bullet_lines = wrap_text(draw, point, body_font.font_variant(size=34), 1320)
            text_y = y + 6
            for line in bullet_lines:
                draw.text((258, text_y), line, font=body_font.font_variant(size=34), fill=(235, 233, 236))
                text_y += 38
            y += box_height + 26

        footer = f"Slide {idx} of {len(sections)}"
        draw.text((1540, 986), footer, font=small_font.font_variant(size=24), fill=(180, 164, 171))
        draw.text((176, 986), "Sharper than a study guide.", font=small_font.font_variant(size=24), fill=(161, 145, 150))
        output = slides_dir / f"slide_{idx:02d}.png"
        image.save(output)
        slide_paths.append(output)
    return slide_paths


def split_for_tts(text: str, max_chars: int = 1200) -> list[str]:
    sentences = sentence_split(text)
    chunks = []
    current = ""
    for sentence in sentences:
        if not current:
            current = sentence
            continue
        if len(current) + 1 + len(sentence) <= max_chars:
            current += " " + sentence
        else:
            chunks.append(current)
            current = sentence
    if current:
        chunks.append(current)
    return chunks or [text[:max_chars]]


def get_voice_id() -> str:
    module = maybe_import_voice_handler()
    if module is not None and hasattr(module, "resolve_voice_id"):
        resolved = (module.resolve_voice_id(bot_name="scaramouche") or "").strip()
        if resolved:
            return resolved
    for env_name in (
        "SCARAMOUCHE_FISH_VOICE_ID",
        "SCARAMOUCHE_VOICE_ID",
        "FISH_AUDIO_REFERENCE_ID",
        "FISH_REFERENCE_ID",
    ):
        value = (os.getenv(env_name) or "").strip()
        if value:
            return value
    if module is not None and getattr(module, "VOICE_ID", ""):
        return module.VOICE_ID
    return FALLBACK_VOICE_ID


def fish_tts_chunk(text: str, api_key: str, voice_id: str) -> bytes:
    module = maybe_import_voice_handler()
    if module is not None and hasattr(module, "_fish_tts_blocking"):
        audio_bytes = module._fish_tts_blocking(
            text,
            api_key,
            220,
            voice_id=voice_id,
            bot_name="scaramouche",
        )
        if audio_bytes:
            return audio_bytes
    payload = ormsgpack.packb(
        {
            "text": text[:1500],
            "reference_id": voice_id,
            "format": "mp3",
            "mp3_bitrate": 192,
            "latency": "balanced",
            "normalize": True,
            "chunk_length": 230,
        }
    )
    headers = {
        "authorization": f"Bearer {api_key}",
        "content-type": "application/msgpack",
        "model": "s2-pro",
    }
    api_url = (os.getenv("FISH_API_URL") or DEFAULT_FISH_API_URL).strip()
    with httpx.Client(timeout=120) as client:
        response = client.post(api_url, content=payload, headers=headers)
    response.raise_for_status()
    return response.content


def gtts_chunk(text: str, output_path: Path):
    tts = gTTS(text=text, lang="en", slow=False)
    tts.save(str(output_path))


def concat_audio(ffmpeg_exe: str, inputs: list[Path], output_path: Path):
    list_file = output_path.with_suffix(".concat.txt")
    list_file.write_text("\n".join(f"file '{path.as_posix()}'" for path in inputs), encoding="utf-8")
    cmd = [
        ffmpeg_exe,
        "-y",
        "-f",
        "concat",
        "-safe",
        "0",
        "-i",
        str(list_file),
        "-c",
        "copy",
        str(output_path),
    ]
    subprocess.run(cmd, check=True)


def synthesize_audio(text: str, output_dir: Path) -> Path:
    ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
    audio_dir = output_dir / "audio_chunks"
    audio_dir.mkdir(exist_ok=True)
    chunks = split_for_tts(text)
    api_key = (os.getenv("FISH_AUDIO_API_KEY") or "").strip()
    voice_id = get_voice_id()
    if api_key:
        print(f"[Scaramouche TTS] Using Fish Audio (voice: {voice_id[:8]}...)")
    else:
        print("[Scaramouche TTS] WARNING: FISH_AUDIO_API_KEY not set — falling back to gTTS (generic voice)")
    chunk_paths = []

    for idx, chunk in enumerate(chunks, start=1):
        chunk_path = audio_dir / f"chunk_{idx:02d}.mp3"
        if api_key:
            audio_bytes = fish_tts_chunk(chunk, api_key, voice_id)
            chunk_path.write_bytes(audio_bytes)
        else:
            gtts_chunk(chunk, chunk_path)
        chunk_paths.append(chunk_path)

    final_audio = output_dir / "scaramouche_narration.mp3"
    if len(chunk_paths) == 1:
        chunk_paths[0].replace(final_audio)
    else:
        concat_audio(ffmpeg_exe, chunk_paths, final_audio)
    return final_audio


def probe_media_duration(media_path: Path) -> float:
    ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
    proc = subprocess.run(
        [ffmpeg_exe, "-i", str(media_path)],
        capture_output=True,
        text=True,
    )
    text = proc.stderr or proc.stdout
    match = re.search(r"Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)", text)
    if not match:
        return 12.0
    hours, minutes, seconds = match.groups()
    return int(hours) * 3600 + int(minutes) * 60 + float(seconds)


def build_spec(title: str, sections: list[Section], slide_paths: list[Path]) -> dict:
    total_words = max(1, sum(len(section.narration.split()) for section in sections))
    consumed_words = 0
    segments = []

    for section, slide_path in zip(sections, slide_paths):
        words = len(section.narration.split())
        start_ratio = consumed_words / total_words
        consumed_words += words
        end_ratio = consumed_words / total_words
        segments.append(
            {
                "title": section.title,
                "narration": section.narration,
                "slide_path": str(slide_path),
                "start_ratio": start_ratio,
                "end_ratio": end_ratio,
            }
        )

    transcript = " ".join(section.narration for section in sections)
    return {
        "title": title,
        "transcript": transcript,
        "segments": segments,
    }


def render_plates(
    blender_exe: Path,
    blend_path: Path,
    scene_name: str,
    spec_path: Path,
    plates_dir: Path,
    width: int,
    height: int,
    engine: str,
    *,
    presenter_only: bool = False,
):
    mood = SCENE_MOODS.get(scene_name, "lecture")
    cmd = [
        str(blender_exe),
        "-b",
        "--python",
        str(DEFAULT_PLATE_SCRIPT),
        "--",
        "--blend",
        str(blend_path),
        "--scene",
        scene_name,
        "--spec",
        str(spec_path),
        "--output-dir",
        str(plates_dir),
        "--mood",
        mood,
        "--width",
        str(width),
        "--height",
        str(height),
        "--engine",
        engine,
    ]
    if presenter_only:
        cmd.extend(["--presenter-only", "true"])
    subprocess.run(cmd, check=True)


def word_viseme(word: str) -> str:
    token = "".join(ch for ch in word.lower() if ch.isalpha())
    if not token:
        return "rest"
    if token.endswith(("m", "n")):
        return "n"
    for char in token:
        if char in "aeiou":
            return char
    return "rest"


def build_segment_frame_labels(text: str, frames: int) -> list[str]:
    words = [word for word in text.split() if word.strip()]
    if not words:
        return ["rest"] * max(1, frames)
    labels = ["rest"] * max(1, frames)
    per_word = max(1.0, frames / max(1, len(words)))
    for idx, word in enumerate(words):
        start = int(round(idx * per_word))
        end = min(len(labels), max(start + 1, int(round((idx + 1) * per_word))))
        viseme = word_viseme(word)
        word_frames = end - start
        if word_frames <= 1:
            if start < len(labels):
                labels[start] = viseme
        else:
            mouth_end = end - 1 if word_frames >= 3 else end
            for pos in range(start, min(mouth_end, len(labels))):
                labels[pos] = viseme
            if word_frames >= 3 and end - 1 < len(labels):
                labels[end - 1] = "rest"
    return labels


def build_frame_plan(spec: dict, duration_seconds: float, fps: int) -> list[tuple[int, str]]:
    total_frames = max(1, int(math.ceil(duration_seconds * fps)))
    segments = spec.get("segments", [])
    plan = []
    for index, segment in enumerate(segments, start=1):
        start = int(round(segment.get("start_ratio", 0.0) * total_frames))
        end = int(round(segment.get("end_ratio", 1.0) * total_frames))
        if index == len(segments):
            end = total_frames
        frames = max(1, end - start)
        labels = build_segment_frame_labels(segment.get("narration", ""), frames)
        plan.extend((index, label) for label in labels)
    while len(plan) < total_frames:
        plan.append((len(segments) or 1, "rest"))

    blink_every = max(fps * 3, 24)
    for blink_frame in range(fps * 2, len(plan), blink_every):
        for offset in range(2):
            frame = blink_frame + offset
            if frame < len(plan):
                segment_index, _ = plan[frame]
                plan[frame] = (segment_index, "blink")
    return plan[:total_frames]


def expand_bbox(bbox: tuple[int, int, int, int], size: tuple[int, int], padding_ratio: float = 0.06) -> tuple[int, int, int, int]:
    width, height = size
    x0, y0, x1, y1 = bbox
    pad_x = int(width * padding_ratio)
    pad_y = int(height * padding_ratio)
    return (
        max(0, x0 - pad_x),
        max(0, y0 - pad_y),
        min(width, x1 + pad_x),
        min(height, y1 + pad_y),
    )


def crop_presenter_sprite(source_path: str) -> Image.Image | None:
    if not source_path or not os.path.exists(source_path):
        return None
    rgba = Image.open(source_path).convert("RGBA")
    bbox = rgba.getchannel("A").getbbox()
    if not bbox:
        return None
    bbox = expand_bbox(bbox, rgba.size, padding_ratio=0.05)
    sprite = rgba.crop(bbox)
    left = int(sprite.width * 0.10)
    right = max(left + 1, int(sprite.width * 0.90))
    top = int(sprite.height * 0.01)
    bottom = max(top + 1, int(sprite.height * 0.98))
    return sprite.crop((left, top, right, bottom))


def contain_image(image: Image.Image, target_size: tuple[int, int], *, anchor_bottom: bool = False) -> Image.Image:
    canvas = Image.new("RGBA", target_size, (0, 0, 0, 0))
    if image.width <= 0 or image.height <= 0:
        return canvas
    ratio = min(target_size[0] / image.width, target_size[1] / image.height)
    resized = image.resize(
        (
            max(1, int(round(image.width * ratio))),
            max(1, int(round(image.height * ratio))),
        ),
        RESAMPLING.LANCZOS,
    )
    x = (target_size[0] - resized.width) // 2
    if anchor_bottom:
        y = target_size[1] - resized.height
    else:
        y = (target_size[1] - resized.height) // 2
    canvas.alpha_composite(resized, (x, y))
    return canvas


def _scara_mouth_strength(viseme: str) -> float:
    return {
        "rest": 0.0,
        "blink": 0.0,
        "n": 0.22,
        "i": 0.28,
        "u": 0.34,
        "e": 0.38,
        "a": 0.54,
        "o": 0.60,
    }.get(viseme, 0.0)


def animate_scaramouche_sprite(sprite: Image.Image, viseme: str) -> Image.Image:
    strength = _scara_mouth_strength(viseme)
    if strength <= 0.0:
        return sprite
    result = sprite.copy()
    width, height = result.size
    shadow = Image.new("RGBA", result.size, (0, 0, 0, 0))
    shadow_draw = ImageDraw.Draw(shadow)
    cx = int(width * 0.50)
    cy = int(height * 0.36)
    mouth_w = max(14, int(width * (0.080 + 0.19 * strength)))
    mouth_h = max(8, int(height * (0.018 + 0.095 * strength)))
    shadow_draw.rounded_rectangle(
        (cx - mouth_w // 2, cy - mouth_h // 2, cx + mouth_w // 2, cy + mouth_h // 2),
        radius=max(3, mouth_h // 2),
        fill=(40, 8, 14, 170 + int(80 * strength)),
    )
    shadow = shadow.filter(ImageFilter.GaussianBlur(radius=1))
    result.alpha_composite(shadow)
    return result


def paste_with_shadow(base: Image.Image, overlay: Image.Image, position: tuple[int, int], blur_radius: int = 18):
    shadow = Image.new("RGBA", overlay.size, (0, 0, 0, 0))
    shadow.putalpha(overlay.getchannel("A"))
    shadow = shadow.filter(ImageFilter.GaussianBlur(radius=blur_radius))
    shadow_layer = Image.new("RGBA", base.size, (0, 0, 0, 0))
    shadow_layer.alpha_composite(shadow, (position[0] + 12, position[1] + 16))
    shadow_layer = ImageEnhance.Brightness(shadow_layer).enhance(0.42)
    base.alpha_composite(shadow_layer)
    base.alpha_composite(overlay, position)


def build_presenter_frame(
    title: str,
    segment: dict,
    viseme: str,
    slide_path: str,
    presenter_path: str | None,
    frame_size: tuple[int, int],
    slide_cache: dict[str, Image.Image],
    presenter_cache: dict[str, Image.Image | None],
) -> Image.Image:
    width, height = frame_size
    background = Image.new("RGB", frame_size, (10, 12, 18))
    draw_vertical_gradient(background, (11, 14, 24), (39, 14, 21))
    frame = background.convert("RGBA")
    draw = ImageDraw.Draw(frame)

    draw.rectangle((0, 0, width, 24), fill=(185, 60, 84))
    draw.ellipse((width - 360, -80, width + 120, 320), fill=(92, 30, 44))
    draw.ellipse((-120, height - 260, 300, height + 160), fill=(48, 18, 24))

    title_font = font_candidates("segoeuib.ttf", "arialbd.ttf").font_variant(size=34)
    label_font = font_candidates("georgiab.ttf", "arialbd.ttf").font_variant(size=23)
    body_font = font_candidates("segoeui.ttf", "arial.ttf").font_variant(size=22)
    small_font = font_candidates("segoeui.ttf", "arial.ttf").font_variant(size=18)

    screen_x, screen_y, screen_w, screen_h = 42, 116, 804, 452
    presenter_x, presenter_y, presenter_w, presenter_h = 888, 82, 344, 566

    draw.rounded_rectangle(
        (screen_x - 10, screen_y - 12, screen_x + screen_w + 10, screen_y + screen_h + 12),
        radius=34,
        fill=(13, 16, 24, 242),
        outline=(196, 78, 101),
        width=3,
    )
    draw.rounded_rectangle(
        (presenter_x, presenter_y, presenter_x + presenter_w, presenter_y + presenter_h),
        radius=34,
        fill=(16, 20, 31, 236),
        outline=(196, 78, 101),
        width=3,
    )
    draw.rounded_rectangle((screen_x, 62, screen_x + 402, 106), radius=18, fill=(176, 60, 84))
    draw.text((screen_x + 20, 73), title.upper()[:44], font=title_font, fill=(248, 244, 241))
    draw.rounded_rectangle((presenter_x + 22, presenter_y + 20, presenter_x + presenter_w - 22, presenter_y + 74), radius=20, fill=(0, 0, 0, 94))
    draw.text((presenter_x + 36, presenter_y + 32), "Scaramouche", font=title_font, fill=(250, 242, 244))

    slide = slide_cache.get(slide_path)
    if slide is None:
        slide = Image.open(slide_path).convert("RGBA")
        slide_cache[slide_path] = slide
    slide_box = contain_image(slide, (screen_w, screen_h))
    screen_mask = Image.new("L", (screen_w, screen_h), 0)
    ImageDraw.Draw(screen_mask).rounded_rectangle((0, 0, screen_w, screen_h), radius=28, fill=255)
    clipped_slide = Image.new("RGBA", (screen_w, screen_h), (0, 0, 0, 0))
    clipped_slide.paste(slide_box, mask=screen_mask)
    frame.alpha_composite(clipped_slide, (screen_x, screen_y))
    draw.rounded_rectangle((screen_x, screen_y, screen_x + screen_w, screen_y + screen_h), radius=28, outline=(92, 38, 49), width=2)

    presenter_sprite = presenter_cache.get(presenter_path or "")
    if presenter_sprite is None and presenter_path:
        presenter_sprite = crop_presenter_sprite(presenter_path)
        presenter_cache[presenter_path] = presenter_sprite
    if presenter_sprite is not None:
        presenter_sprite = animate_scaramouche_sprite(presenter_sprite, viseme)
        sprite = contain_image(presenter_sprite, (presenter_w - 42, presenter_h - 116), anchor_bottom=True)
        paste_with_shadow(frame, sprite, (presenter_x + 20, presenter_y + 96))

    draw.rounded_rectangle((screen_x + 10, screen_y + screen_h + 20, screen_x + 292, screen_y + screen_h + 62), radius=18, fill=(18, 23, 34, 220), outline=(196, 78, 101), width=2)
    draw.text((screen_x + 26, screen_y + screen_h + 31), segment.get("title", "Current section"), font=label_font, fill=(244, 240, 241))

    draw.rounded_rectangle(
        (presenter_x + 22, presenter_y + presenter_h - 92, presenter_x + presenter_w - 22, presenter_y + presenter_h - 34),
        radius=18,
        fill=(0, 0, 0, 94),
        outline=(90, 96, 112),
        width=2,
    )
    draw.text((presenter_x + 38, presenter_y + presenter_h - 73), "Live commentary", font=body_font, fill=(235, 239, 244))

    draw.text((screen_x, height - 38), "Rendered briefing", font=small_font, fill=(188, 170, 175))
    draw.text((presenter_x + 24, height - 38), "Live presenter plate", font=small_font, fill=(188, 170, 175))
    return frame


def assemble_frame_images(plates_manifest: dict, spec: dict, frame_plan: list[tuple[int, str]], frames_dir: Path, frame_size: tuple[int, int]):
    frames_dir.mkdir(parents=True, exist_ok=True)
    slide_cache: dict[str, Image.Image] = {}
    presenter_cache: dict[str, Image.Image | None] = {}
    segments = spec.get("segments", [])
    title = spec.get("title", "Scaramouche Briefing")
    for idx, (segment_index, viseme) in enumerate(frame_plan, start=1):
        segment = plates_manifest["plates"].get(str(segment_index), {})
        source = segment.get(viseme) or segment.get("rest")
        if not source or segment_index - 1 >= len(segments):
            continue
        frame = build_presenter_frame(
            title,
            segments[segment_index - 1],
            viseme,
            segments[segment_index - 1].get("slide_path", ""),
            source,
            frame_size,
            slide_cache,
            presenter_cache,
        )
        target = frames_dir / f"frame_{idx:05d}.png"
        frame.save(target)


def encode_video(frames_dir: Path, fps: int, audio_path: Path, final_video: Path):
    ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
    cmd = [
        ffmpeg_exe,
        "-y",
        "-framerate",
        str(fps),
        "-i",
        str(frames_dir / "frame_%05d.png"),
        "-i",
        str(audio_path),
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-shortest",
        str(final_video),
    ]
    subprocess.run(cmd, check=True)


def main():
    parser = argparse.ArgumentParser(description="Create a Scaramouche presentation video from notes.")
    parser.add_argument("notes", help="Notes text or a path to a notes file.")
    parser.add_argument("--title", default="", help="Optional lesson title.")
    parser.add_argument("--output-dir", default="", help="Optional output directory override.")
    parser.add_argument("--scene", default="01_Lecture_Explainer", help="Template scene to render.")
    parser.add_argument("--blend", default=str(DEFAULT_BLEND), help="Path to the populated Scaramouche blend.")
    parser.add_argument("--blender", default=str(DEFAULT_BLENDER), help="Path to Blender binary.")
    parser.add_argument("--width", type=int, default=1280, help="Output video width.")
    parser.add_argument("--height", type=int, default=720, help="Output video height.")
    parser.add_argument("--engine", default="BLENDER_WORKBENCH", help="Render engine for the one-click pipeline.")
    parser.add_argument("--fps", type=int, default=12, help="Output video fps for the fast talking-head mode.")
    args = parser.parse_args()

    load_environment()

    notes_text = read_notes(args.notes)
    title = args.title.strip() or derive_title(notes_text)
    sections = split_sections(notes_text)
    output_dir = Path(args.output_dir) if args.output_dir else ensure_output_dir(title)
    output_dir.mkdir(parents=True, exist_ok=True)
    slides_dir = output_dir / "slides"
    slide_paths = create_slide_images(title, sections, slides_dir)
    spec = build_spec(title, sections, slide_paths)

    transcript_path = output_dir / "transcript.txt"
    transcript_path.write_text(spec["transcript"], encoding="utf-8")

    spec_path = output_dir / "presentation_spec.json"
    spec_path.write_text(json.dumps(spec, indent=2), encoding="utf-8")

    audio_path = synthesize_audio(spec["transcript"], output_dir)
    duration_seconds = probe_media_duration(audio_path)
    plates_dir = output_dir / "plates"
    frames_dir = output_dir / "frames"
    final_video = output_dir / "scaramouche_presentation.mp4"

    render_plates(
        Path(args.blender),
        Path(args.blend),
        args.scene,
        spec_path,
        plates_dir,
        args.width,
        args.height,
        args.engine,
        presenter_only=True,
    )
    plates_manifest = json.loads((plates_dir / "plates_manifest.json").read_text(encoding="utf-8"))
    frame_plan = build_frame_plan(spec, duration_seconds, args.fps)
    assemble_frame_images(plates_manifest, spec, frame_plan, frames_dir, (args.width, args.height))
    encode_video(frames_dir, args.fps, audio_path, final_video)

    summary = {
        "title": title,
        "scene": args.scene,
        "slides": [str(path) for path in slide_paths],
        "transcript": str(transcript_path),
        "audio": str(audio_path),
        "spec": str(spec_path),
        "plates_dir": str(plates_dir),
        "frames_dir": str(frames_dir),
        "final_video": str(final_video),
    }
    (output_dir / "run_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
