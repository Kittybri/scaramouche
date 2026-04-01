import argparse
import importlib.util
import json
import math
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import imageio_ffmpeg
from PIL import Image, ImageChops, ImageDraw, ImageEnhance, ImageFilter, ImageFont


ROOT_DIR = Path(__file__).resolve().parent
WANDERER_ROOT = ROOT_DIR.parent / "wanderer_blender_templates"
SCARA_SCRIPT = ROOT_DIR / "notes_to_scaramouche_video.py"
WANDERER_SCRIPT = WANDERER_ROOT / "notes_to_wanderer_video.py"
DUO_RENDER_SCENE = "01_Lecture_Explainer"
RESAMPLING = getattr(Image, "Resampling", Image)
SCARA_TEXTURE_CANDIDATES = [
    ROOT_DIR.parent / "_scara_fbx_extract" / "Scaramouche" / "Avatar_Boy_Catalyst_Scaramouche_Tex_Face_Diffuse.png",
    ROOT_DIR.parent / "_scara_fbx_extract" / "Scaramouche" / "face.png",
    ROOT_DIR.parent / "_scara_source_extract2" / "textures" / "face.png",
]
SCARA_PREVIEW_CANDIDATES = [
    ROOT_DIR.parent / "can-i-rename-scaramouche.jpg.jpeg",
    ROOT_DIR.parent / "genshin-impact-scaramouche-build-1.jpg",
]


@dataclass
class DebateTurn:
    speaker: str
    title: str
    text: str
    slide_path: Path | None = None
    audio_path: Path | None = None
    duration_s: float = 0.0
    segment_index: int = 0
    voice_id: str = ""


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


SCARA = load_module("scara_video_module", SCARA_SCRIPT)
WANDERER = load_module("wanderer_video_module", WANDERER_SCRIPT)


def ensure_output_dir(base_name: str) -> Path:
    slug = SCARA.slugify(f"{base_name}-duo-debate")
    out_dir = ROOT_DIR / "outputs" / slug
    suffix = 1
    final = out_dir
    while final.exists():
        suffix += 1
        final = ROOT_DIR / "outputs" / f"{slug}-{suffix}"
    final.mkdir(parents=True, exist_ok=True)
    return final


def font_candidates(*names: str):
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


def build_scara_debate_line(section_title: str, points: list[str], idx: int) -> str:
    opener_pool = [
        f"{section_title}. Pay attention.",
        f"{section_title}. Do try to keep up.",
        f"{section_title}. This is the part with actual value.",
    ]
    closer_pool = [
        "Power decides who gets to shape the outcome.",
        "What matters is leverage, not sentiment.",
        "Usefulness comes first. The rest is decoration.",
    ]
    lines = [opener_pool[idx % len(opener_pool)]]
    for point in points[:2]:
        sentence = point.strip()
        if sentence and sentence[-1] not in ".!?":
            sentence += "."
        lines.append(sentence)
    lines.append(closer_pool[idx % len(closer_pool)])
    return " ".join(lines)


def build_wanderer_debate_line(section_title: str, points: list[str], idx: int) -> str:
    opener_pool = [
        f"{section_title}. Slow down and look at it properly.",
        f"{section_title}. There is more to this than force.",
        f"{section_title}. Start with what can actually be observed.",
    ]
    closer_pool = [
        "Intent and consequence matter more than noise.",
        "You cannot separate the result from the cost.",
        "What people choose to ignore usually matters most.",
    ]
    lines = [opener_pool[idx % len(opener_pool)]]
    for point in points[:2]:
        sentence = point.strip()
        if sentence and sentence[-1] not in ".!?":
            sentence += "."
        lines.append(sentence)
    lines.append(closer_pool[idx % len(closer_pool)])
    return " ".join(lines)


def build_debate_turns(notes_text: str, max_sections: int = 4) -> tuple[str, list[DebateTurn]]:
    title = SCARA.derive_title(notes_text)
    sections = SCARA.split_sections(notes_text, max_sections=max_sections)
    turns: list[DebateTurn] = []
    for idx, section in enumerate(sections, start=1):
        turns.append(
            DebateTurn(
                speaker="scaramouche",
                title=section.title,
                text=build_scara_debate_line(section.title, section.points, idx - 1),
            )
        )
        turns.append(
            DebateTurn(
                speaker="wanderer",
                title=section.title,
                text=build_wanderer_debate_line(section.title, section.points, idx - 1),
            )
        )
    return title, turns


def create_duo_slide_images(title: str, turns: list[DebateTurn], slides_dir: Path) -> list[Path]:
    slides_dir.mkdir(parents=True, exist_ok=True)
    title_font = font_candidates("segoeuib.ttf", "arialbd.ttf")
    section_font = font_candidates("georgiab.ttf", "arialbd.ttf")
    body_font = font_candidates("segoeui.ttf", "arial.ttf")
    small_font = font_candidates("segoeui.ttf", "arial.ttf")

    slide_paths: list[Path] = []
    for idx, turn in enumerate(turns, start=1):
        image = Image.new("RGB", (1920, 1080), (8, 11, 17))
        draw_vertical_gradient(image, (10, 13, 19), (16, 20, 30))
        draw = ImageDraw.Draw(image)

        draw.rectangle((0, 0, 960, 1080), fill=(22, 10, 16))
        draw.rectangle((960, 0, 1920, 1080), fill=(10, 28, 34))
        draw.ellipse((1180, -180, 1950, 500), fill=(23, 72, 86))
        draw.ellipse((-220, -180, 540, 500), fill=(95, 26, 42))
        draw.rounded_rectangle((90, 84, 1830, 998), radius=44, fill=(12, 16, 24), outline=(95, 104, 126), width=2)
        draw.rounded_rectangle((126, 132, 820, 208), radius=24, fill=(167, 55, 79))
        draw.rounded_rectangle((1100, 132, 1794, 208), radius=24, fill=(66, 144, 154))

        draw.text((170, 148), "Scaramouche", font=title_font.font_variant(size=34), fill=(250, 241, 244))
        draw.text((1148, 148), "Wanderer", font=title_font.font_variant(size=34), fill=(237, 247, 248))
        content_left = 500
        content_right = 1420
        content_center = (content_left + content_right) // 2
        turn_font = title_font.font_variant(size=72)
        title_text = title
        title_box = draw.textbbox((0, 0), title_text, font=title_font.font_variant(size=52))
        draw.text((content_center - (title_box[2] - title_box[0]) // 2, 248), title_text, font=title_font.font_variant(size=52), fill=(242, 245, 248))
        section_text = turn.title
        section_box = draw.textbbox((0, 0), section_text, font=section_font.font_variant(size=34))
        draw.text((content_center - (section_box[2] - section_box[0]) // 2, 320), section_text, font=section_font.font_variant(size=34), fill=(199, 210, 222))
        turn_box = draw.textbbox((0, 0), f"{idx:02d}", font=turn_font)
        draw.text((content_right - (turn_box[2] - turn_box[0]), 238), f"{idx:02d}", font=turn_font, fill=(186, 196, 214))

        if turn.speaker == "scaramouche":
            draw.rounded_rectangle((content_left, 410, content_left + 370, 482), radius=24, fill=(201, 82, 103))
            draw.text((content_left + 34, 428), "Current speaker: Scaramouche", font=small_font.font_variant(size=28), fill=(255, 245, 247))
            listener_text = "Wanderer is listening"
        else:
            draw.rounded_rectangle((content_right - 370, 410, content_right, 482), radius=24, fill=(92, 185, 195))
            draw.text((content_right - 336, 428), "Current speaker: Wanderer", font=small_font.font_variant(size=28), fill=(247, 253, 255))
            listener_text = "Scaramouche is waiting"
        listener_box = draw.textbbox((0, 0), listener_text, font=small_font.font_variant(size=26))
        draw.text((content_center - (listener_box[2] - listener_box[0]) // 2, 500), listener_text, font=small_font.font_variant(size=26), fill=(175, 189, 202))

        body_lines = wrap_text(draw, turn.text, body_font.font_variant(size=31), content_right - content_left)
        y = 560
        for line in body_lines[:6]:
            line_box = draw.textbbox((0, 0), line, font=body_font.font_variant(size=31))
            draw.text((content_center - (line_box[2] - line_box[0]) // 2, y), line, font=body_font.font_variant(size=31), fill=(232, 238, 244))
            y += 42

        draw.text((148, 980), "Duo report render", font=small_font.font_variant(size=24), fill=(158, 168, 180))
        draw.text((1560, 980), f"Turn {idx} of {len(turns)}", font=small_font.font_variant(size=24), fill=(158, 168, 180))

        output = slides_dir / f"slide_{idx:02d}.png"
        image.save(output)
        slide_paths.append(output)
        turn.slide_path = output
    return slide_paths


def speaker_module(speaker: str):
    return SCARA if speaker == "scaramouche" else WANDERER


def apply_character_audio_profile(source_path: Path, speaker: str) -> Path:
    return source_path


def synthesize_turn_audio(turn: DebateTurn, output_dir: Path) -> tuple[Path, float]:
    module = speaker_module(turn.speaker)
    api_key = (os.getenv("FISH_AUDIO_API_KEY") or "").strip()
    audio_dir = output_dir / "audio_chunks"
    audio_dir.mkdir(parents=True, exist_ok=True)
    raw_path = audio_dir / f"{turn.speaker}_{turn.segment_index:02d}_raw.mp3"
    if api_key:
        turn.voice_id = (module.get_voice_id() or "").strip() or "missing"
        print(f"[duo-video] speaker={turn.speaker} voice_id={turn.voice_id}")
        audio_bytes = module.fish_tts_chunk(turn.text, api_key, turn.voice_id)
        raw_path.write_bytes(audio_bytes)
    else:
        turn.voice_id = "gtts-fallback"
        print(f"[duo-video] speaker={turn.speaker} voice_id=gtts-fallback")
        module.gtts_chunk(turn.text, raw_path)
    final_path = apply_character_audio_profile(raw_path, turn.speaker)
    return final_path, module.probe_media_duration(final_path)


def concat_audio(inputs: list[Path], output_path: Path):
    ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
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


def build_character_spec(title: str, turns: list[DebateTurn]) -> dict:
    return {
        "title": title,
        "transcript": " ".join(turn.text for turn in turns),
        "segments": [
            {
                "title": turn.title,
                "narration": turn.text,
                "slide_path": str(turn.slide_path),
                "start_ratio": 0.0,
                "end_ratio": 1.0,
            }
            for turn in turns
        ],
    }


def render_character_plates(turns: list[DebateTurn], output_dir: Path, width: int, height: int, engine: str):
    if not turns:
        return None
    module = speaker_module(turns[0].speaker)
    plates_dir = output_dir / f"{turns[0].speaker}_plates"
    spec_path = output_dir / f"{turns[0].speaker}_spec.json"
    spec_path.write_text(json.dumps(build_character_spec(turns[0].title, turns), indent=2), encoding="utf-8")
    previous = os.environ.get("DUO_PRESENTER_ONLY")
    os.environ["DUO_PRESENTER_ONLY"] = "1"
    try:
        module.render_plates(
            Path(os.getenv("BLENDER_BIN") or str(module.DEFAULT_BLENDER)),
            Path(module.DEFAULT_BLEND),
            DUO_RENDER_SCENE,
            spec_path,
            plates_dir,
            width,
            height,
            engine,
        )
    finally:
        if previous is None:
            os.environ.pop("DUO_PRESENTER_ONLY", None)
        else:
            os.environ["DUO_PRESENTER_ONLY"] = previous
    return json.loads((plates_dir / "plates_manifest.json").read_text(encoding="utf-8"))


def build_turn_frame_labels(turn: DebateTurn, fps: int) -> list[str]:
    module = speaker_module(turn.speaker)
    frames = max(1, int(math.ceil(turn.duration_s * fps)))
    labels = module.build_segment_frame_labels(turn.text, frames)
    if frames >= fps * 4:
        blink_index = min(len(labels) - 1, max(1, fps * 2))
        labels[blink_index] = "blink"
    return labels


def speaker_palette(speaker: str) -> tuple[tuple[int, int, int], tuple[int, int, int], tuple[int, int, int]]:
    if speaker == "scaramouche":
        return (70, 18, 28), (146, 44, 68), (250, 238, 243)
    return (14, 40, 46), (57, 135, 149), (236, 248, 249)


def first_existing_path(paths: list[Path]) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def scaramouche_texture_path() -> Path | None:
    return first_existing_path(SCARA_TEXTURE_CANDIDATES)


def scaramouche_preview_path() -> Path | None:
    return first_existing_path(SCARA_PREVIEW_CANDIDATES)


def average_corner_colors(image: Image.Image) -> list[tuple[int, int, int]]:
    rgba = image.convert("RGBA")
    width, height = rgba.size
    points = [
        (0, 0),
        (width - 1, 0),
        (0, height - 1),
        (width - 1, height - 1),
        (width // 2, 0),
        (width // 2, height - 1),
    ]
    return [rgba.getpixel(point)[:3] for point in points]


def subject_mask(image: Image.Image) -> Image.Image:
    rgba = image.convert("RGBA")
    width, height = rgba.size
    colors = average_corner_colors(rgba)
    mask = Image.new("L", rgba.size, 0)
    pixels = rgba.load()
    mask_pixels = mask.load()
    for y in range(height):
        for x in range(width):
            r, g, b, a = pixels[x, y]
            if a == 0:
                continue
            distance = min(abs(r - cr) + abs(g - cg) + abs(b - cb) for cr, cg, cb in colors)
            if distance > 44:
                mask_pixels[x, y] = 255
    return mask.filter(ImageFilter.MaxFilter(5))


def expand_bbox(bbox: tuple[int, int, int, int], size: tuple[int, int], padding_ratio: float = 0.08):
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


def crop_subject(image: Image.Image) -> tuple[Image.Image | None, tuple[int, int, int, int] | None]:
    rgba = image.convert("RGBA")
    alpha_bbox = rgba.getchannel("A").getbbox()
    if alpha_bbox:
        alpha_bbox = expand_bbox(alpha_bbox, rgba.size, padding_ratio=0.04)
        return rgba.crop(alpha_bbox), alpha_bbox
    mask = subject_mask(rgba)
    bbox = mask.getbbox()
    if not bbox:
        return None, None
    bbox = expand_bbox(bbox, rgba.size)
    subject = Image.new("RGBA", rgba.size, (0, 0, 0, 0))
    subject.paste(rgba, mask=mask)
    return subject.crop(bbox), bbox


def stable_segment_key(image_path: str | None, speaker: str) -> tuple[str, str]:
    if not image_path:
        return (speaker, "fallback")
    stem = Path(image_path).stem
    if stem.startswith("segment_"):
        parts = stem.split("_")
        if len(parts) >= 3:
            return (speaker, "_".join(parts[:2]))
    return (speaker, stem)


def focus_presenter_crop(subject: Image.Image, speaker: str) -> Image.Image:
    if subject.width <= 0 or subject.height <= 0:
        return subject
    if speaker == "wanderer":
        left = int(subject.width * 0.40)
        right = int(subject.width * 0.60)
        top = int(subject.height * 0.00)
        bottom = int(subject.height * 0.64)
        return subject.crop((left, top, max(left + 1, right), max(top + 1, bottom)))
    if speaker == "scaramouche":
        left = int(subject.width * 0.34)
        right = int(subject.width * 0.62)
        top = int(subject.height * 0.00)
        bottom = int(subject.height * 0.58)
        return subject.crop((left, top, max(left + 1, right), max(top + 1, bottom)))
    return subject


def scaramouche_preview_image(panel_size: tuple[int, int]) -> Image.Image | None:
    preview_path = scaramouche_preview_path()
    if not preview_path:
        return None
    source = Image.open(preview_path).convert("RGBA")
    if preview_path.name.lower().startswith("can-i-rename-scaramouche"):
        source = source.crop((source.width // 2, 0, source.width, source.height))
    return contain_image(source, (panel_size[0] - 44, panel_size[1] - 112))


def poor_presenter_render(bbox: tuple[int, int, int, int] | None, size: tuple[int, int], speaker: str) -> bool:
    if not bbox:
        return True
    width, height = size
    x0, y0, x1, y1 = bbox
    box_w = x1 - x0
    box_h = y1 - y0
    if speaker == "scaramouche":
        if y0 < int(height * 0.08) and box_h > int(height * 0.78):
            return True
        if box_w > int(width * 0.55) and box_h > int(height * 0.72):
            return True
        if box_w < int(width * 0.22) or box_h < int(height * 0.36):
            return True
    return False


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
    mouth_box = (
        int(width * 0.38),
        int(height * 0.395),
        int(width * 0.62),
        int(height * 0.515),
    )
    lower = result.crop(mouth_box)
    extra_height = max(5, int(height * 0.030 + height * 0.120 * strength))
    stretched = lower.resize((lower.width, lower.height + extra_height), RESAMPLING.LANCZOS)
    masked = Image.new("RGBA", result.size, (0, 0, 0, 0))
    masked.alpha_composite(stretched, (mouth_box[0], mouth_box[1]))
    result.alpha_composite(masked)

    shadow = Image.new("RGBA", result.size, (0, 0, 0, 0))
    shadow_draw = ImageDraw.Draw(shadow)
    cx = (mouth_box[0] + mouth_box[2]) // 2
    cy = int(height * 0.452)
    mouth_w = max(12, int(width * (0.100 + 0.22 * strength)))
    mouth_h = max(8, int(height * (0.028 + 0.090 * strength)))
    shadow_draw.ellipse(
        (cx - mouth_w // 2, cy - mouth_h // 2, cx + mouth_w // 2, cy + mouth_h // 2),
        fill=(36, 8, 12, 160 + int(120 * strength)),
    )
    shadow = shadow.filter(ImageFilter.GaussianBlur(radius=max(1, int(width * 0.012))))
    result.alpha_composite(shadow)
    return result


def contain_image(image: Image.Image, target_size: tuple[int, int], anchor_bottom: bool = True) -> Image.Image:
    canvas = Image.new("RGBA", target_size, (0, 0, 0, 0))
    if image.width == 0 or image.height == 0:
        return canvas
    ratio = min(target_size[0] / image.width, target_size[1] / image.height)
    resized = image.resize(
        (
            max(1, int(image.width * ratio)),
            max(1, int(image.height * ratio)),
        ),
        RESAMPLING.LANCZOS,
    )
    x = (target_size[0] - resized.width) // 2
    y = target_size[1] - resized.height if anchor_bottom else (target_size[1] - resized.height) // 2
    canvas.alpha_composite(resized, (x, y))
    return canvas


def fallback_presenter_card(size: tuple[int, int], speaker: str, active: bool) -> Image.Image:
    primary, secondary, text = speaker_palette(speaker)
    image = Image.new("RGBA", size, (0, 0, 0, 0))
    bg = Image.new("RGB", size, primary)
    draw_vertical_gradient(bg, primary, secondary)
    image.alpha_composite(bg.convert("RGBA"))
    draw = ImageDraw.Draw(image)
    title_font = font_candidates("segoeuib.ttf", "arialbd.ttf").font_variant(size=max(24, size[0] // 11))
    small_font = font_candidates("segoeui.ttf", "arial.ttf").font_variant(size=max(16, size[0] // 18))
    border = secondary if active else (110, 118, 130)
    draw.rounded_rectangle((2, 2, size[0] - 3, size[1] - 3), radius=28, outline=border, width=4)
    draw.rounded_rectangle((24, 22, size[0] - 24, 84), radius=22, fill=(0, 0, 0, 96))
    draw.rounded_rectangle((24, 22, size[0] - 24, 84), radius=22, fill=(0, 0, 0, 90))
    draw.text((40, 34), speaker.title(), font=title_font, fill=text)
    preview = scaramouche_preview_image(size) if speaker == "scaramouche" else None
    if preview is not None:
        image.alpha_composite(preview, (22, 96))
    else:
        draw.polygon(
            [
                (size[0] * 0.18, size[1] * 0.84),
                (size[0] * 0.34, size[1] * 0.26),
                (size[0] * 0.48, size[1] * 0.70),
                (size[0] * 0.62, size[1] * 0.18),
                (size[0] * 0.82, size[1] * 0.84),
            ],
            fill=(0, 0, 0, 44),
        )
        draw.ellipse((40, 118, 124, 202), outline=(255, 255, 255, 34), width=3)
        draw.ellipse((size[0] - 124, 118, size[0] - 40, 202), outline=(255, 255, 255, 34), width=3)
        draw.line((64, 160, size[0] - 64, 160), fill=(255, 255, 255, 28), width=2)
        draw.line((size[0] // 2, 108, size[0] // 2, size[1] - 114), fill=(255, 255, 255, 22), width=2)
    subtitle = "Live commentary" if speaker == "scaramouche" else "Measured analysis"
    draw.text((40, size[1] - 72), subtitle, font=small_font, fill=(238, 242, 247))
    footer = "Portrait preview active" if preview is not None else "Fallback portrait active"
    draw.text((40, size[1] - 46), footer, font=small_font, fill=(208, 216, 228))
    return image


def build_presenter_panel(
    image_path: str | None,
    speaker: str,
    active: bool,
    active_label: str,
    panel_size: tuple[int, int],
    cache: dict[tuple[str, str, bool, str, int, int], Image.Image],
    crop_cache: dict[tuple[str, str], tuple[int, int, int, int] | None],
) -> Image.Image:
    key = (speaker, image_path or "fallback", active, active_label, panel_size[0], panel_size[1])
    if key in cache:
        return cache[key].copy()

    primary, secondary, text = speaker_palette(speaker)
    panel = Image.new("RGBA", panel_size, (0, 0, 0, 0))
    bg = Image.new("RGB", panel_size, primary)
    draw_vertical_gradient(bg, primary, secondary)
    bg_rgba = bg.convert("RGBA")
    bg_rgba.putalpha(236 if active else 214)
    panel.alpha_composite(bg_rgba)
    draw = ImageDraw.Draw(panel)
    outline = secondary if active else (120, 130, 144)
    fill_overlay = (255, 255, 255, 16 if active else 8)
    draw.rounded_rectangle((0, 0, panel_size[0] - 1, panel_size[1] - 1), radius=28, outline=outline, width=4, fill=fill_overlay)
    draw.rounded_rectangle((18, 18, panel_size[0] - 18, 72), radius=20, fill=(0, 0, 0, 96))
    name_font = font_candidates("segoeuib.ttf", "arialbd.ttf").font_variant(size=max(24, panel_size[0] // 10))
    draw.text((34, 28), speaker.title(), font=name_font, fill=text)

    presenter = None
    bbox = None
    if image_path and os.path.exists(image_path):
        source = Image.open(image_path).convert("RGBA")
        segment_key = stable_segment_key(image_path, speaker)
        bbox = crop_cache.get(segment_key)
        if bbox is None:
            presenter, bbox = crop_subject(source)
            crop_cache[segment_key] = bbox
        if bbox:
            presenter = source.crop(bbox)
    if presenter is None or poor_presenter_render(bbox, source.size if image_path and os.path.exists(image_path) else panel_size, speaker):
        panel = fallback_presenter_card(panel_size, speaker, active)
        cache[key] = panel.copy()
        return panel

    presenter = focus_presenter_crop(presenter, speaker)
    if speaker == "scaramouche":
        presenter = animate_scaramouche_sprite(presenter, active_label)
    if speaker == "scaramouche":
        target_box = (panel_size[0] - 42, panel_size[1] - 104)
    else:
        target_box = (panel_size[0] - 58, panel_size[1] - 126)
    sprite = contain_image(presenter, target_box, anchor_bottom=True)
    if not active:
        sprite = ImageEnhance.Brightness(sprite).enhance(0.72)
    panel.alpha_composite(sprite, (29, 90))
    cache[key] = panel.copy()
    return panel


def paste_with_shadow(base: Image.Image, overlay: Image.Image, position: tuple[int, int]):
    shadow = Image.new("RGBA", overlay.size, (0, 0, 0, 0))
    alpha = overlay.getchannel("A")
    shadow.putalpha(alpha)
    shadow = shadow.filter(ImageFilter.GaussianBlur(radius=14))
    shadow_layer = Image.new("RGBA", base.size, (0, 0, 0, 0))
    shadow_layer.alpha_composite(shadow, (position[0] + 10, position[1] + 16))
    shadow_layer = ImageEnhance.Brightness(shadow_layer).enhance(0.45)
    base.alpha_composite(shadow_layer)
    base.alpha_composite(overlay, position)


def manifest_plate(manifest: dict | None, segment_index: int, label: str) -> str | None:
    if not manifest:
        return None
    plates = manifest.get("plates", {})
    segment = plates.get(str(segment_index), {})
    return segment.get(label) or segment.get("rest")


def compose_frame(
    turn: DebateTurn,
    active_label: str,
    scara_path: str | None,
    wanderer_path: str | None,
    frame_size: tuple[int, int],
    slide_cache: dict[str, Image.Image],
    panel_cache: dict[tuple[str, str, bool, str, int, int], Image.Image],
    crop_cache: dict[tuple[str, str], tuple[int, int, int, int] | None],
) -> Image.Image:
    slide_key = str(turn.slide_path)
    if slide_key not in slide_cache:
        background = Image.open(slide_key).convert("RGBA").resize(frame_size, RESAMPLING.LANCZOS)
        slide_cache[slide_key] = background
    frame = slide_cache[slide_key].copy()

    width, height = frame_size
    panel_w = int(width * 0.245)
    panel_h = int(height * 0.64)
    left_pos = (int(width * 0.02), int(height * 0.245))
    right_pos = (width - panel_w - int(width * 0.02), int(height * 0.245))

    scara_panel = build_presenter_panel(scara_path, "scaramouche", turn.speaker == "scaramouche", active_label, (panel_w, panel_h), panel_cache, crop_cache)
    wanderer_panel = build_presenter_panel(wanderer_path, "wanderer", turn.speaker == "wanderer", active_label, (panel_w, panel_h), panel_cache, crop_cache)

    paste_with_shadow(frame, scara_panel, left_pos)
    paste_with_shadow(frame, wanderer_panel, right_pos)

    ticker = Image.new("RGBA", (width - 120, 54), (7, 10, 16, 188))
    ticker_draw = ImageDraw.Draw(ticker)
    accent = speaker_palette(turn.speaker)[1]
    ticker_draw.rounded_rectangle((0, 0, ticker.width - 1, ticker.height - 1), radius=20, outline=accent, width=3)
    ticker_font = font_candidates("segoeuib.ttf", "arialbd.ttf").font_variant(size=20)
    ticker_draw.text((26, 14), f"{turn.speaker.title()} has the floor", font=ticker_font, fill=(245, 248, 251))
    frame.alpha_composite(ticker, ((width - ticker.width) // 2, height - 74))

    return frame


def assemble_frames(turns: list[DebateTurn], scara_manifest: dict | None, wanderer_manifest: dict | None, frames_dir: Path, fps: int, frame_size: tuple[int, int]):
    frames_dir.mkdir(parents=True, exist_ok=True)
    slide_cache: dict[str, Image.Image] = {}
    panel_cache: dict[tuple[str, str, bool, str, int, int], Image.Image] = {}
    crop_cache: dict[tuple[str, str], tuple[int, int, int, int] | None] = {}
    frame_index = 1
    for turn in turns:
        labels = build_turn_frame_labels(turn, fps)
        counterpart_index = turn.segment_index
        scara_rest = manifest_plate(scara_manifest, counterpart_index, "rest")
        wanderer_rest = manifest_plate(wanderer_manifest, counterpart_index, "rest")
        for label in labels:
            scara_path = manifest_plate(scara_manifest, turn.segment_index, label) if turn.speaker == "scaramouche" else scara_rest
            wanderer_path = manifest_plate(wanderer_manifest, turn.segment_index, label) if turn.speaker == "wanderer" else wanderer_rest
            frame = compose_frame(turn, label, scara_path, wanderer_path, frame_size, slide_cache, panel_cache, crop_cache)
            target = frames_dir / f"frame_{frame_index:05d}.png"
            frame.save(target)
            frame_index += 1


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
    parser = argparse.ArgumentParser(description="Create a Scaramouche/Wanderer duo debate video from notes.")
    parser.add_argument("notes", help="Notes text or a path to a notes file.")
    parser.add_argument("--title", default="", help="Optional debate title.")
    parser.add_argument("--output-dir", default="", help="Optional output directory override.")
    parser.add_argument("--width", type=int, default=1280, help="Output video width.")
    parser.add_argument("--height", type=int, default=720, help="Output video height.")
    parser.add_argument("--engine", default="BLENDER_WORKBENCH", help="Render engine for the one-click pipeline.")
    parser.add_argument("--fps", type=int, default=10, help="Output video fps.")
    args = parser.parse_args()

    SCARA.load_environment()
    notes_text = SCARA.read_notes(args.notes)
    title, turns = build_debate_turns(notes_text)
    if args.title.strip():
        title = args.title.strip()
    output_dir = Path(args.output_dir) if args.output_dir else ensure_output_dir(title)
    output_dir.mkdir(parents=True, exist_ok=True)

    for index, turn in enumerate([t for t in turns if t.speaker == "scaramouche"], start=1):
        turn.segment_index = index
    for index, turn in enumerate([t for t in turns if t.speaker == "wanderer"], start=1):
        turn.segment_index = index

    slides_dir = output_dir / "slides"
    create_duo_slide_images(title, turns, slides_dir)

    audio_inputs = []
    for turn in turns:
        audio_path, duration = synthesize_turn_audio(turn, output_dir)
        turn.audio_path = audio_path
        turn.duration_s = duration
        audio_inputs.append(audio_path)

    final_audio = output_dir / "duo_debate_narration.mp3"
    concat_audio(audio_inputs, final_audio)

    scara_turns = [turn for turn in turns if turn.speaker == "scaramouche"]
    wanderer_turns = [turn for turn in turns if turn.speaker == "wanderer"]
    scara_manifest = render_character_plates(scara_turns, output_dir, args.width, args.height, args.engine)
    wanderer_manifest = render_character_plates(wanderer_turns, output_dir, args.width, args.height, args.engine)

    frames_dir = output_dir / "frames"
    assemble_frames(turns, scara_manifest, wanderer_manifest, frames_dir, args.fps, (args.width, args.height))
    final_video = output_dir / "duo_debate_presentation.mp4"
    encode_video(frames_dir, args.fps, final_audio, final_video)

    summary = {
        "title": title,
        "slides": [str(turn.slide_path) for turn in turns],
        "audio": str(final_audio),
        "final_video": str(final_video),
        "scara_manifest": str(output_dir / "scaramouche_plates" / "plates_manifest.json"),
        "wanderer_manifest": str(output_dir / "wanderer_plates" / "plates_manifest.json"),
        "duo_scene": DUO_RENDER_SCENE,
        "voices": {
            turn.speaker: turn.voice_id
            for turn in turns
        },
    }
    (output_dir / "run_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
