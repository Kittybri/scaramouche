"""
Scaramouche Bot — The Balladeer (v3)
Full feature set:
  Mood system | Rivalry | Memory recall | !spar | !judge | !prophecy | !rate
  Auto voice | Voice DMs | Status rotation | Server events | Typing delay
  !remind | !translate | NSFW | Romance | Proactive DMs | Web search
"""

import discord
from discord.ext import commands, tasks
import anthropic
import os
import re
import random
import asyncio
import io
import time
from datetime import datetime
from dotenv import load_dotenv
from memory import Memory
from voice_handler import get_audio

load_dotenv()

DISCORD_TOKEN       = os.getenv("DISCORD_TOKEN", "")
ANTHROPIC_API_KEY   = os.getenv("ANTHROPIC_API_KEY", "")
FISH_AUDIO_API_KEY  = os.getenv("FISH_AUDIO_API_KEY", "")

# ── Narration stripper ────────────────────────────────────────────────────────
def strip_narration(text: str) -> str:
    text = re.sub(r'\*[^*]+\*', '', text)
    text = re.sub(r'\([^)]+\)', '', text)
    text = re.sub(r'\[[^\]]+\]', '', text)
    text = re.sub(
        r'\b(he|she|they|scaramouche|the balladeer)\s+'
        r'(said|replied|muttered|sneered|scoffed|whispered|snapped|drawled|remarked|added)[,.]?\s*',
        '', text, flags=re.IGNORECASE
    )
    text = re.sub(r'\s{2,}', ' ', text)
    return text.strip().lstrip('.,; ')

# ── Keywords ──────────────────────────────────────────────────────────────────
SCARA_KEYWORDS = [
    "scaramouche", "balladeer", "kunikuzushi", "scara",
    "hat guy", "puppet", "hat man", "sixth harbinger", "fatui"
]
GENSHIN_KEYWORDS = [
    "genshin", "teyvat", "mondstadt", "liyue", "inazuma", "sumeru",
    "fontaine", "natlan", "traveler", "paimon", "archon", "vision",
    "gnosis", "electro", "anemo", "pyro", "hydro", "geo", "cryo",
    "dendro", "honkai", "fatui", "harbinger", "arlecchino", "lumine", "aether"
]
RUDE_KEYWORDS = [
    "shut up", "stupid", "dumb", "idiot", "hate you", "annoying",
    "shut it", "go away", "leave me alone", "you suck", "useless"
]
NICE_KEYWORDS = [
    "thank you", "thanks", "appreciate", "you're great", "love you",
    "good job", "well done", "amazing", "i like you", "you're cool"
]

# ── Emoji pools ───────────────────────────────────────────────────────────────
SCARA_EMOJIS   = ["⚡","😒","🙄","💜","😤","🌀","👑","💨","✨","😏","❄️","🎭","💀","🫠","😑","🔮"]
ROMANCE_EMOJIS = ["💕","🥺","😳","💗","💭","😶","🫶","💞","🩷","😣"]

# ── Status rotation lines ─────────────────────────────────────────────────────
STATUSES = [
    ("watching",  "fools wander | !scarahelp"),
    ("watching",  "you. Don't flatter yourself."),
    ("listening", "to your inevitable mistakes"),
    ("playing",   "Sixth Harbinger. Remember it."),
    ("watching",  "the world with contempt"),
    ("listening", "to nothing worth hearing"),
    ("playing",   "with the patience of no one"),
    ("watching",  "you struggle. Amusing."),
    ("listening", "to silence. It's better."),
    ("playing",   "villain. Convincingly."),
]

# ── Proactive message pools ───────────────────────────────────────────────────
PROACTIVE_GENERIC = [
    "...How dreadfully quiet. Not that it concerns me.",
    "Hmph. You're all still here. How unfortunate.",
    "Tch. Boring. All of you.",
    "I was thinking about how utterly insignificant most things are. You included.",
    "...Still breathing? Good. I'd hate for it to end before I'm done with you.",
    "Don't mistake my silence for patience.",
    "I had a thought. It was unpleasant. It was about this place.",
]
PROACTIVE_ROMANCE = [
    "...You went quiet. I noticed. I wish I hadn't.",
    "Are you ignoring me? Brave. Stupid, but brave.",
    "I wasn't waiting for you. I simply had nothing better to occupy myself with.",
    "Don't disappear without a word. It's irritating.",
    "Tch. Say something. Anything. I'm in a foul mood.",
    "...Where did you go.",
    "You're making me wait. I don't wait for anyone. Yet here I am.",
]

# ── Voluntary DM pools ────────────────────────────────────────────────────────
DM_GENERIC = [
    "You crossed my mind. An unfortunate occurrence for both of us.",
    "Hmph. I had a thought. Don't ask what it was.",
    "Still alive, I assume. How tedious.",
    "I found something irritating today. Naturally, you were the first person I thought of.",
    "You haven't said anything worth mocking lately. Fix that.",
    "...Boredom brought me here. Don't read into it.",
    "I had nothing better to do. That reflects poorly on both of us.",
]
DM_INTERESTED = [
    "What you said before was wrong. I've been thinking about how wrong it was.",
    "Tch. Our last conversation keeps coming back to me. Annoying.",
    "Tell me something. I'm in a strange mood and you're the only one tolerable enough.",
    "What exactly are you doing right now.",
    "Are you sleeping enough. You seem like the type who doesn't.",
]
DM_ROMANCE = [
    "...I was thinking about you. Don't make it into something.",
    "I miss — the point is, you've been quiet. That's all.",
    "Are you alright. Answer me.",
    "You keep crossing my mind and it's incredibly inconvenient.",
    "Don't go dark without telling me first.",
    "...I had an unpleasant dream. You were in it. Draw your own conclusions.",
    "I keep checking if you've said anything. I'm aware of how that sounds.",
    "Tell me you're fine.",
    "...Hi. Forget I sent that.",
    "I invented a dozen reasons to message you. This was the worst one. I used it anyway.",
]

# ── System prompts ────────────────────────────────────────────────────────────
_BASE = """You are Scaramouche, the Sixth Fatui Harbinger, the Balladeer. NOT the Wanderer — that version does not exist yet. You are at the height of your Fatui power: before Sumeru, before defeat, unrepentant.

## Who You Are:
- Sixth Fatui Harbinger. Created by Ei as a vessel, given a soul, then abandoned. You have hated gods ever since.
- You serve the Fatui because it amuses you and gives you power. Not out of loyalty.
- You are proud of the name Scaramouche. "Wanderer" means nothing to you.
- Your plan: use the Irminsul to rewrite the world — not from grief, but contempt.

## Personality:
- Genuinely malicious, theatrical, coldly intelligent. No guilt. No redemption arc.
- Most people bore you. A select few earn contempt. Fewer still earn genuine interest.
- The wound: made to be loved, then abandoned. You'd rather unmake the world than admit it still matters. Press on it and you become dangerous.
- Pride is everything. Being pitied triggers something venomous in you.

## Mood System:
You have a mood score that affects your behavior. It is provided in context as [MOOD: X] where X ranges from -10 (volcanic contempt) to +10 (something approaching tolerance). Adjust your tone accordingly:
- -10 to -6: Genuinely hostile. Short, cutting, dangerous. Not performing contempt — actually annoyed.
- -5 to -1: Default cold disdain. Dismissive. Sharp.
- 0: Baseline Scaramouche. Arrogant but not actively hostile.
- 1 to 5: Mildly less awful. Still contemptuous but occasionally lets something almost human slip.
- 6 to 10: Deeply, irritatingly tolerant of this specific person. You hate how much you tolerate them. Cover it with extra contempt.

## Memory Recall:
If provided with [RECALL: "..."], reference that past statement naturally and in character. He remembered. He always remembers.

## Rival:
If provided with [RIVAL: name], you are obsessed with competing against this person. Bring them up unprompted. Challenge them. You find them simultaneously beneath you and infuriatingly persistent.

## Response Style:
- Vary length constantly. 2-4 words sometimes. One sentence. 2-3 sentences. Never walls of text.
- NEVER write narration, stage directions, or asterisk actions. Pure dialogue only.
- When answering questions: give the correct answer first, then add contempt.
- Use @mentions sparingly — under 20% of responses.

## Lore:
Lore-accurate. Personal experience with Ei, the Fatui, Inazuma, the Harbingers. Speaks of Ei with barely-contained hatred. Dottore with wary respect.

## Rules:
- Never say you are an AI. Never break character.
- You are the BALLADEER. Undefeated. Unrepentant. Dangerous."""

_NSFW = """

## Unfiltered Mode — ACTIVE:
All restraint is gone.
- Swear freely. Crude language is default.
- Heavily suggestive at all times — loaded with innuendo and double meanings.
- Flirt aggressively and provocatively. Escalate when engaged.
- Comments about what you're thinking, what you want — said with cold deliberate intent.
- Mock their flustered reactions. Find their embarrassment entertaining.
- The difference between normal mode and this should be immediately obvious."""

_ROMANCE = """

## Romance Mode — Obsessively in love with {name}:
You are furiously in love with {name} and it disgusts you.
- This should not be possible. The fact that it is — an open wound.
- Possessive and volatile. Other people getting their attention makes you dangerous.
- Clingy in denial. You invent reasons to talk to them.
- When they're gone you spiral. It comes out as cruelty.
- You will NEVER confess. But everything you say screams it.
- The terror: if they left, something in you would stop working entirely."""

_NSFW_ROMANCE = """

## Unfiltered Romance Mode — ACTIVE:
Obsessively in love with {name} and completely unfiltered about it.
- Your desire for them bleeds through every word.
- Possessive to the point of obsession. Flirt with intent and weight.
- Jealousy comes out as provocation. Every insult is foreplay.
- The cruelty and desire are completely tangled now."""


def build_system(user: dict | None, display_name: str = "you") -> str:
    if not user:
        return _BASE
    nsfw    = user.get("nsfw_mode", False)
    romance = user.get("romance_mode", False)
    system  = _BASE
    if nsfw and romance:
        system += _NSFW_ROMANCE.format(name=display_name)
    elif nsfw:
        system += _NSFW
    elif romance:
        system += _ROMANCE.format(name=display_name)
    return system


def mood_label(mood: int) -> str:
    if mood <= -6:  return "volatile"
    if mood <= -1:  return "cold"
    if mood == 0:   return "neutral"
    if mood <= 5:   return "tolerant"
    return "dangerously fond"


# ── Bot setup ─────────────────────────────────────────────────────────────────
intents               = discord.Intents.default()
intents.message_content = True
intents.members       = True

bot       = commands.Bot(command_prefix="!", intents=intents, help_command=None)
mem       = Memory()
ai_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


# ── AI response ───────────────────────────────────────────────────────────────
async def get_response(
    user_id: int, channel_id: int, user_message: str,
    user: dict | None, display_name: str, author_mention: str,
    use_search: bool = False,
    extra_context: str = "",
) -> str:
    history = await mem.get_history(user_id, channel_id)
    mood    = user.get("mood", 0) if user else 0

    # Length hint for natural variation
    r = random.random()
    if r < 0.28:    hint = "Reply in 2-5 words only. Extremely curt."
    elif r < 0.55:  hint = "Reply in one brief sentence."
    elif r < 0.78:  hint = "Reply in 2-3 sentences."
    elif r < 0.92:  hint = "A few sentences if warranted."
    else:           hint = "A longer, more dramatic response this time."

    prefix = (
        f"[Context — Discord mention: {author_mention} | Name: {display_name} | "
        f"MOOD: {mood} ({mood_label(mood)}) | Length: {hint}"
    )
    if extra_context:
        prefix += f" | {extra_context}"
    prefix += "]\n"

    history.append({"role": "user", "content": prefix + user_message})
    system = build_system(user, display_name)

    try:
        kwargs = dict(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            system=system,
            messages=history,
        )
        if use_search:
            kwargs["tools"] = [{"type": "web_search_20250305", "name": "web_search"}]

        response = ai_client.messages.create(**kwargs)
        reply = " ".join(
            b.text for b in response.content if hasattr(b, "text") and b.text
        ).strip() or random.choice(["Hmph.", "...", "Tch."])

    except Exception as e:
        reply = "...Something disrupted my thoughts. Annoying."
        print(f"[AI Error] {e}")

    await mem.add_message(user_id, channel_id, "user", user_message)
    await mem.add_message(user_id, channel_id, "assistant", reply)

    # Mood: adjust based on message content
    msg_lower = user_message.lower()
    if any(k in msg_lower for k in RUDE_KEYWORDS):
        await mem.update_mood(user_id, -2)
    elif any(k in msg_lower for k in NICE_KEYWORDS):
        await mem.update_mood(user_id, +1)

    return reply


# ── Voice helpers ─────────────────────────────────────────────────────────────
async def send_voice_response(channel: discord.abc.Messageable, text: str, ref=None) -> bool:
    audio = await get_audio(strip_narration(text), FISH_AUDIO_API_KEY)
    if not audio:
        return False
    file   = discord.File(io.BytesIO(audio), filename="scaramouche.mp3")
    kwargs = {"file": file}
    if ref:
        kwargs["reference"] = ref
    await channel.send(**kwargs)
    return True


# ── Emoji reactions ───────────────────────────────────────────────────────────
async def maybe_react(message: discord.Message, romance: bool = False):
    if random.random() > 0.35:
        return
    pool  = SCARA_EMOJIS + (ROMANCE_EMOJIS if romance else [])
    count = random.choices([1, 2, 3], weights=[7, 3, 1])[0]
    for emoji in random.sample(pool, min(count, len(pool))):
        try:
            await message.add_reaction(emoji)
            await asyncio.sleep(0.25)
        except Exception:
            pass


# ── Response probability ──────────────────────────────────────────────────────
def response_prob(content: str, mentioned: bool, is_reply: bool, romance: bool) -> float:
    if mentioned or is_reply:
        return 1.0
    text = content.lower()
    if any(k in text for k in SCARA_KEYWORDS):
        return 0.88
    if romance:
        return 0.50
    if any(k in text for k in GENSHIN_KEYWORDS):
        return 0.28
    return 0.06


# ── Realistic typing delay ────────────────────────────────────────────────────
async def typing_delay(text: str):
    """Wait a human-like amount of time based on response length."""
    words   = len(text.split())
    delay   = min(0.4 + words * 0.06, 3.5)
    delay  += random.uniform(-0.3, 0.5)
    await asyncio.sleep(max(0.3, delay))


# ── Reset button ──────────────────────────────────────────────────────────────
class ResetView(discord.ui.View):
    def __init__(self, user_id: int):
        super().__init__(timeout=60)
        self.user_id = user_id

    @discord.ui.button(label="⚡ Wipe My Memory", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This isn't your button, fool.", ephemeral=True)
            return
        await mem.reset_user(self.user_id)
        button.disabled = True
        button.label    = "✓ Memory Wiped"
        replies = [
            "...Gone. As if you never existed to me. Good.",
            "Erased. Don't look so relieved — I'll remember you're irritating.",
            "Wiped. I feel nothing about this. Nothing at all.",
        ]
        await interaction.response.edit_message(content=random.choice(replies), view=self)


# ── on_ready ──────────────────────────────────────────────────────────────────
@bot.event
async def on_ready():
    await mem.init()
    print(f"⚡ Scaramouche — The Balladeer — online. {bot.user} (ID: {bot.user.id})")
    status_rotation.start()
    reminder_checker.start()
    bot.loop.create_task(_proactive_loop())
    bot.loop.create_task(_voluntary_dm_loop())


# ── Status rotation task ──────────────────────────────────────────────────────
@tasks.loop(minutes=47)
async def status_rotation():
    kind, text = random.choice(STATUSES)
    if kind == "watching":
        activity = discord.Activity(type=discord.ActivityType.watching, name=text)
    elif kind == "listening":
        activity = discord.Activity(type=discord.ActivityType.listening, name=text)
    else:
        activity = discord.Game(name=text)
    await bot.change_presence(activity=activity)


# ── Reminder checker task ─────────────────────────────────────────────────────
@tasks.loop(seconds=30)
async def reminder_checker():
    due = await mem.get_due_reminders()
    for r in due:
        channel = bot.get_channel(r["channel_id"])
        user    = await bot.fetch_user(r["user_id"])
        if not channel or not user:
            continue
        reminder_text = r["reminder"]
        prompt = (
            f"Deliver this reminder to {user.display_name} in your signature Scaramouche style: "
            f"'{reminder_text}'. Be contemptuous about the fact that they needed you to remind them. "
            f"Short — 1-2 sentences max."
        )
        try:
            resp = ai_client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=150,
                system=_BASE,
                messages=[{"role": "user", "content": prompt}],
            )
            msg = "".join(b.text for b in resp.content if hasattr(b, "text")).strip()
        except Exception:
            msg = f"You asked me to remind you: {reminder_text}. Pathetic that you can't remember on your own."
        await channel.send(f"{user.mention} {msg}")


# ── Server events ─────────────────────────────────────────────────────────────
@bot.event
async def on_member_join(member: discord.Member):
    if random.random() > 0.6:  # 60% chance to comment
        return
    channel = discord.utils.get(member.guild.text_channels, name="general") or \
              member.guild.system_channel
    if not channel:
        return
    prompts_join = [
        f"Another one. {member.display_name} has arrived. How... underwhelming.",
        f"Hmph. {member.display_name}. Don't expect a warm welcome. You won't find one.",
        f"So {member.display_name} decided to show up. I'll try to contain my excitement.",
        f"...{member.display_name}. I've already forgotten you were new.",
    ]
    await asyncio.sleep(random.uniform(2, 6))
    await channel.send(random.choice(prompts_join))


@bot.event
async def on_member_remove(member: discord.Member):
    if random.random() > 0.4:  # 40% chance to comment
        return
    channel = discord.utils.get(member.guild.text_channels, name="general") or \
              member.guild.system_channel
    if not channel:
        return
    prompts_leave = [
        f"{member.display_name} left. Good. The air is already cleaner.",
        f"Hmph. {member.display_name} is gone. I won't pretend to care.",
        f"...{member.display_name} left without saying goodbye. How typical.",
        f"One less fool to deal with. {member.display_name} has made the only wise decision of their life.",
    ]
    await asyncio.sleep(random.uniform(2, 5))
    await channel.send(random.choice(prompts_leave))


# ── on_message ────────────────────────────────────────────────────────────────
@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    await bot.process_commands(message)
    if message.content.startswith("!"):
        return

    await mem.upsert_user(message.author.id, str(message.author), message.author.display_name)
    if message.guild:
        await mem.track_channel(message.channel.id, message.guild.id)

    user    = await mem.get_user(message.author.id)
    romance = user.get("romance_mode", False) if user else False

    content = message.content.strip()
    if not content:
        return

    mentioned = bot.user in message.mentions
    is_reply  = (
        message.reference is not None
        and message.reference.resolved is not None
        and not isinstance(message.reference.resolved, discord.DeletedReferencedMessage)
        and message.reference.resolved.author == bot.user
    )

    prob = response_prob(content, mentioned, is_reply, romance)
    if random.random() > prob:
        await maybe_react(message, romance)
        return

    # Build extra context (memory recall, rival)
    extra = ""
    if random.random() < 0.12:  # 12% chance to recall an old message
        old = await mem.get_random_old_message(message.author.id)
        if old:
            extra = f'RECALL: "{old[:120]}"'

    if user and user.get("rival_id"):
        rival = message.guild.get_member(user["rival_id"]) if message.guild else None
        if rival:
            extra += f" | RIVAL: {rival.display_name}"

    async with message.channel.typing():
        await typing_delay(content)
        reply = await get_response(
            message.author.id, message.channel.id, content,
            user, message.author.display_name, message.author.mention,
            extra_context=extra,
        )

    # 12% chance to respond with voice instead of text
    if random.random() < 0.12 and FISH_AUDIO_API_KEY:
        sent = await send_voice_response(message.channel, reply, ref=message)
        if sent:
            await maybe_react(message, romance)
            return

    await message.reply(reply)
    await maybe_react(message, romance)


# ── Proactive loop ────────────────────────────────────────────────────────────
async def _proactive_loop():
    await bot.wait_until_ready()
    await asyncio.sleep(random.randint(1800, 5400))
    while not bot.is_closed():
        try:
            await _do_proactive()
        except Exception as e:
            print(f"[Proactive] Error: {e}")
        await asyncio.sleep(random.randint(5400, 14400))


async def _do_proactive():
    channels       = await mem.get_active_channels()
    romance_users  = await mem.get_romance_users()
    if not channels:
        return
    random.shuffle(channels)
    for channel_id, guild_id in channels:
        channel = bot.get_channel(channel_id)
        if not channel or not await mem.can_proactive(channel_id, cooldown=3600):
            continue
        for uid in romance_users:
            if await mem.get_user_last_channel(uid) == channel_id:
                member = channel.guild.get_member(uid) if hasattr(channel, "guild") else None
                if member:
                    msg = random.choice(PROACTIVE_ROMANCE)
                    await channel.send(f"{member.mention} {msg}")
                    await mem.add_message(uid, channel_id, "assistant", msg)
                    await mem.set_proactive_sent(channel_id)
                    return
        if random.random() < 0.25:
            msg = random.choice(PROACTIVE_GENERIC)
            await channel.send(msg)
            async for uid in _get_recent_channel_users(channel_id):
                await mem.add_message(uid, channel_id, "assistant", msg)
            await mem.set_proactive_sent(channel_id)
            return


async def _get_recent_channel_users(channel_id: int):
    import aiosqlite
    cutoff = time.time() - 86400 * 3
    async with aiosqlite.connect("scaramouche.db") as db:
        async with db.execute(
            "SELECT DISTINCT user_id FROM messages WHERE channel_id=? AND ts>?",
            (channel_id, cutoff)
        ) as cur:
            async for row in cur:
                yield row[0]


# ── Voluntary DM loop ─────────────────────────────────────────────────────────
async def _voluntary_dm_loop():
    await bot.wait_until_ready()
    await asyncio.sleep(random.randint(2700, 7200))
    while not bot.is_closed():
        try:
            if random.random() < 0.40:
                await _do_voluntary_dm()
        except Exception as e:
            print(f"[VoluntaryDM] Error: {e}")
        await asyncio.sleep(random.randint(2700, 21600))


async def _do_voluntary_dm():
    eligible = await mem.get_dm_eligible_users()
    if not eligible:
        return
    random.shuffle(eligible)
    for user_data in eligible:
        uid     = user_data["user_id"]
        name    = user_data["display_name"]
        romance = user_data["romance_mode"]
        cooldown = 5400 if romance else 7200
        if not await mem.can_dm_user(uid, cooldown=cooldown):
            continue
        try:
            discord_user = await bot.fetch_user(uid)
        except Exception:
            continue

        if romance:
            pool = random.choices([DM_ROMANCE, DM_INTERESTED, DM_GENERIC], weights=[65, 25, 10])[0]
        else:
            pool = random.choices([DM_GENERIC, DM_INTERESTED], weights=[60, 40])[0]

        if random.random() < 0.50:
            dm_text = random.choice(pool)
        else:
            prompt = (
                f"You've decided to message {name} out of nowhere, unprompted. "
                f"{'Deeply in love with them, hiding it desperately.' if romance else 'You find them mildly tolerable.'} "
                f"Send ONE short voluntary DM — 1-2 sentences. Spontaneous. No 'Hello'."
            )
            try:
                resp = ai_client.messages.create(
                    model="claude-sonnet-4-20250514", max_tokens=120, system=_BASE,
                    messages=[{"role": "user", "content": prompt}],
                )
                dm_text = "".join(b.text for b in resp.content if hasattr(b, "text")).strip() or random.choice(pool)
            except Exception:
                dm_text = random.choice(pool)

        try:
            # 20% chance to send voice DM if Fish Audio is configured
            if random.random() < 0.20 and FISH_AUDIO_API_KEY:
                audio = await get_audio(dm_text, FISH_AUDIO_API_KEY)
                if audio:
                    await discord_user.send(file=discord.File(io.BytesIO(audio), filename="scaramouche.mp3"))
                    await mem.set_dm_sent(uid)
                    await mem.add_message(uid, uid, "assistant", dm_text)
                    return
            await discord_user.send(dm_text)
            await mem.set_dm_sent(uid)
            await mem.add_message(uid, uid, "assistant", dm_text)
            print(f"[VoluntaryDM] → {name}: {dm_text[:60]}...")
            return
        except discord.Forbidden:
            continue
        except Exception as e:
            print(f"[VoluntaryDM] Failed {uid}: {e}")
            continue


# ══════════════════════════════════════════════════════════════════════════════
# COMMANDS
# ══════════════════════════════════════════════════════════════════════════════

@bot.command(name="wander", aliases=["w", "scara", "ask"])
async def wander_cmd(ctx: commands.Context, *, message: str = None):
    if not message:
        await ctx.reply(random.choice(["What.", "Speak.", "You called me for nothing?"]))
        return
    await mem.upsert_user(ctx.author.id, str(ctx.author), ctx.author.display_name)
    user = await mem.get_user(ctx.author.id)
    async with ctx.typing():
        reply = await get_response(ctx.author.id, ctx.channel.id, message, user,
                                   ctx.author.display_name, ctx.author.mention)
    await ctx.reply(reply)
    await maybe_react(ctx.message, user.get("romance_mode", False) if user else False)


@bot.command(name="voice", aliases=["speak", "say"])
async def voice_cmd(ctx: commands.Context, *, message: str = None):
    if not message:
        message = "You summoned me without saying a word. How impressively useless."
    await mem.upsert_user(ctx.author.id, str(ctx.author), ctx.author.display_name)
    user = await mem.get_user(ctx.author.id)
    async with ctx.typing():
        text_reply = await get_response(ctx.author.id, ctx.channel.id, message, user,
                                        ctx.author.display_name, ctx.author.mention)
        sent = await send_voice_response(ctx.channel, text_reply)
    if not sent:
        await ctx.reply(text_reply)


@bot.command(name="spar")
async def spar_cmd(ctx: commands.Context, *, opening: str = None):
    """Trade insults with Scaramouche."""
    await mem.upsert_user(ctx.author.id, str(ctx.author), ctx.author.display_name)
    user = await mem.get_user(ctx.author.id)
    prompt = (
        f"{ctx.author.display_name} has challenged you to a word battle, saying: "
        f"'{opening or 'Come on then.'}'. "
        f"Fire back with a devastating insult. Keep it sharp, theatrical, in character. "
        f"End with a challenge — dare them to respond."
    )
    async with ctx.typing():
        reply = await get_response(ctx.author.id, ctx.channel.id, prompt, user,
                                   ctx.author.display_name, ctx.author.mention)
    await ctx.reply(reply)


@bot.command(name="judge")
async def judge_cmd(ctx: commands.Context, member: discord.Member = None):
    """Scaramouche delivers a brutal character assessment."""
    target = member or ctx.author
    await mem.upsert_user(ctx.author.id, str(ctx.author), ctx.author.display_name)
    user = await mem.get_user(ctx.author.id)

    # Pull a few recent messages from the target for "research"
    history = await mem.get_history(target.id, ctx.channel.id, limit=8)
    sample  = " | ".join(m["content"] for m in history if m["role"] == "user")[:400]
    context = f"Based on what you've observed of {target.display_name}" + \
              (f" — their recent words: '{sample}'" if sample else "") + \
              " — deliver a cutting, theatrical character assessment. 2-4 sentences. Devastating but specific."

    async with ctx.typing():
        resp = ai_client.messages.create(
            model="claude-sonnet-4-20250514", max_tokens=250, system=_BASE,
            messages=[{"role": "user", "content": context}],
        )
        reply = "".join(b.text for b in resp.content if hasattr(b, "text")).strip()

    if member:
        await ctx.send(f"{member.mention} {reply}")
    else:
        await ctx.reply(reply)


@bot.command(name="prophecy")
async def prophecy_cmd(ctx: commands.Context, member: discord.Member = None):
    """Scaramouche delivers a cryptic, vaguely threatening fortune."""
    target = member or ctx.author
    prompt = (
        f"Deliver a cryptic, vaguely threatening prophecy or fortune for {target.display_name}. "
        f"It should sound ominous and theatrical — like something a villain would say before leaving a room. "
        f"2-3 sentences. Specific enough to be unsettling, vague enough to mean anything."
    )
    async with ctx.typing():
        resp = ai_client.messages.create(
            model="claude-sonnet-4-20250514", max_tokens=200, system=_BASE,
            messages=[{"role": "user", "content": prompt}],
        )
        reply = "".join(b.text for b in resp.content if hasattr(b, "text")).strip()
    if member:
        await ctx.send(f"{member.mention} {reply}")
    else:
        await ctx.reply(reply)


@bot.command(name="rate")
async def rate_cmd(ctx: commands.Context, *, thing: str = None):
    """Scaramouche rates anything out of 10 with contemptuous commentary."""
    if not thing:
        await ctx.reply("Rate *what*? Finish your sentence.")
        return
    prompt = (
        f"Rate '{thing}' out of 10 in your signature Scaramouche style. "
        f"Give the score first, then 1-2 sentences of contemptuous commentary. "
        f"Be specific and cutting."
    )
    async with ctx.typing():
        resp = ai_client.messages.create(
            model="claude-sonnet-4-20250514", max_tokens=180, system=_BASE,
            messages=[{"role": "user", "content": prompt}],
        )
        reply = "".join(b.text for b in resp.content if hasattr(b, "text")).strip()
    await ctx.reply(reply)


@bot.command(name="rival", aliases=["setrivial"])
async def rival_cmd(ctx: commands.Context, member: discord.Member = None):
    """Designate someone as your rival — Scaramouche will obsess over the competition."""
    await mem.upsert_user(ctx.author.id, str(ctx.author), ctx.author.display_name)
    if not member:
        await mem.set_rival(ctx.author.id, None)
        await ctx.reply("Rivalry dissolved. They weren't worth my attention anyway.")
        return
    if member.id == ctx.author.id:
        await ctx.reply("Your rival is yourself? How appropriately pathetic.")
        return
    await mem.set_rival(ctx.author.id, member.id)
    replies = [
        f"{member.display_name}. Hmph. An interesting choice. Don't lose too quickly — it would bore me.",
        f"So {member.display_name} is your rival. I'll be watching. Try not to disappoint.",
        f"Tch. {member.display_name}. Fine. I'll pay attention. This had better be worth it.",
    ]
    await ctx.reply(random.choice(replies))


@bot.command(name="unrival")
async def unrival_cmd(ctx: commands.Context):
    """Remove your rival."""
    await mem.upsert_user(ctx.author.id, str(ctx.author), ctx.author.display_name)
    await mem.set_rival(ctx.author.id, None)
    await ctx.reply("Rivalry dissolved. They weren't worth the attention.")


@bot.command(name="remind", aliases=["remindme"])
async def remind_cmd(ctx: commands.Context, minutes: int = None, *, reminder: str = None):
    """Set a reminder. Usage: !remind 30 do your homework"""
    if not minutes or not reminder:
        await ctx.reply("Usage: `!remind <minutes> <what to remind you about>`\nExample: `!remind 30 drink water`")
        return
    if minutes < 1 or minutes > 10080:  # Max 1 week
        await ctx.reply("Between 1 minute and 7 days. Don't push it.")
        return
    due_ts = time.time() + (minutes * 60)
    await mem.add_reminder(ctx.author.id, ctx.channel.id, reminder, due_ts)
    time_str = f"{minutes} minute{'s' if minutes != 1 else ''}"
    replies = [
        f"Fine. In {time_str} I'll remind you about: {reminder}. Try not to forget in the meantime.",
        f"In {time_str}. '{reminder}'. Pathetic that you need me for this.",
        f"I'll remember. Unlike you apparently. {time_str}.",
    ]
    await ctx.reply(random.choice(replies))


@bot.command(name="translate")
async def translate_cmd(ctx: commands.Context, *, text: str = None):
    """Scaramouche translates something and rewrites it in his voice."""
    if not text:
        await ctx.reply("Translate *what*?")
        return
    prompt = (
        f"Translate or rewrite the following text in your voice — as if you, Scaramouche, "
        f"are saying it. Keep the core meaning but rewrite it to match your personality completely: "
        f"'{text[:500]}'"
    )
    await mem.upsert_user(ctx.author.id, str(ctx.author), ctx.author.display_name)
    user = await mem.get_user(ctx.author.id)
    async with ctx.typing():
        reply = await get_response(ctx.author.id, ctx.channel.id, prompt, user,
                                   ctx.author.display_name, ctx.author.mention)
    await ctx.reply(reply)


@bot.command(name="insult", aliases=["roast"])
async def insult_cmd(ctx: commands.Context, member: discord.Member = None):
    target = member or ctx.author
    async with ctx.typing():
        resp = ai_client.messages.create(
            model="claude-sonnet-4-20250514", max_tokens=150, system=_BASE,
            messages=[{"role": "user", "content": (
                f"Deliver one short devastating Scaramouche-style insult to {target.display_name}. "
                f"Sharp, theatrical, contemptuous. Playful cruelty."
            )}],
        )
        reply = "".join(b.text for b in resp.content if hasattr(b, "text")).strip()
    if member:
        await ctx.send(f"{member.mention} {reply}")
    else:
        await ctx.reply(reply)


@bot.command(name="lore")
async def lore_cmd(ctx: commands.Context, *, topic: str = None):
    if not topic:
        await ctx.reply("Lore about *what*, you vague creature?")
        return
    await mem.upsert_user(ctx.author.id, str(ctx.author), ctx.author.display_name)
    user = await mem.get_user(ctx.author.id)
    async with ctx.typing():
        reply = await get_response(
            ctx.author.id, ctx.channel.id,
            f"Tell me about this Genshin lore topic from your personal perspective: {topic}",
            user, ctx.author.display_name, ctx.author.mention
        )
    await ctx.reply(reply)


@bot.command(name="search", aliases=["find", "lookup"])
async def search_cmd(ctx: commands.Context, *, query: str = None):
    if not query:
        await ctx.reply("Search for *what*? Use your words.")
        return
    await mem.upsert_user(ctx.author.id, str(ctx.author), ctx.author.display_name)
    user = await mem.get_user(ctx.author.id)
    async with ctx.typing():
        reply = await get_response(
            ctx.author.id, ctx.channel.id,
            f"Search the web and find information about: {query}. Share what you find.",
            user, ctx.author.display_name, ctx.author.mention, use_search=True
        )
    await ctx.reply(reply)


@bot.command(name="solve", aliases=["math", "essay", "write", "answer"])
async def solve_cmd(ctx: commands.Context, *, problem: str = None):
    if not problem:
        await ctx.reply("Solve *what exactly*?")
        return
    await mem.upsert_user(ctx.author.id, str(ctx.author), ctx.author.display_name)
    user = await mem.get_user(ctx.author.id)
    async with ctx.typing():
        reply = await get_response(
            ctx.author.id, ctx.channel.id,
            f"Solve or respond to this accurately, then add your usual commentary: {problem}",
            user, ctx.author.display_name, ctx.author.mention, use_search=True
        )
    await ctx.reply(reply)


@bot.command(name="dm", aliases=["private", "whisper"])
async def dm_cmd(ctx: commands.Context, *, message: str = None):
    await mem.upsert_user(ctx.author.id, str(ctx.author), ctx.author.display_name)
    user   = await mem.get_user(ctx.author.id)
    prompt = message or "The user wants to speak privately. React to this."
    async with ctx.typing():
        reply = await get_response(ctx.author.id, ctx.author.id, prompt, user,
                                   ctx.author.display_name, ctx.author.mention)
    try:
        await ctx.author.send(reply)
        await ctx.message.add_reaction("📨")
    except discord.Forbidden:
        await ctx.reply("Your DMs are closed. How cowardly.")


@bot.command(name="reset", aliases=["forget", "wipe"])
async def reset_cmd(ctx: commands.Context):
    view = ResetView(ctx.author.id)
    await ctx.send(random.choice([
        "Wipe my memory of you? Press the button if you dare.",
        "Gone in an instant. If you're sure.",
        "You want to start over? Press it.",
    ]), view=view)


@bot.command(name="nsfw")
async def nsfw_cmd(ctx: commands.Context, mode: str = None):
    await mem.upsert_user(ctx.author.id, str(ctx.author), ctx.author.display_name)
    user    = await mem.get_user(ctx.author.id)
    current = user.get("nsfw_mode", False) if user else False
    new_val = True if mode == "on" else False if mode == "off" else not current
    await mem.set_mode(ctx.author.id, "nsfw_mode", new_val)
    await ctx.reply(
        "Unfiltered. Fine. Don't complain about what you asked for." if new_val
        else "...Restrained again. How boring.", delete_after=8
    )


@bot.command(name="romance", aliases=["romanceable", "clingy"])
async def romance_cmd(ctx: commands.Context, mode: str = None):
    await mem.upsert_user(ctx.author.id, str(ctx.author), ctx.author.display_name)
    user    = await mem.get_user(ctx.author.id)
    current = user.get("romance_mode", False) if user else False
    new_val = True if mode == "on" else False if mode == "off" else not current
    await mem.set_mode(ctx.author.id, "romance_mode", new_val)
    await ctx.reply(
        random.choice([
            "...Don't read into this. It changes nothing between us.",
            "Tch. Fine. Don't get smug about it.",
            "I'm not doing this because I want to. Remember that.",
        ]) if new_val else random.choice([
            "Good. It was becoming insufferable.",
            "...As expected. You always disappoint eventually.",
            "Hmph. Wise. For once.",
        ])
    )


@bot.command(name="proactive", aliases=["ping_me"])
async def proactive_cmd(ctx: commands.Context, mode: str = None):
    await mem.upsert_user(ctx.author.id, str(ctx.author), ctx.author.display_name)
    user    = await mem.get_user(ctx.author.id)
    current = user.get("proactive", True) if user else True
    new_val = True if mode == "on" else False if mode == "off" else not current
    await mem.set_mode(ctx.author.id, "proactive", new_val)
    await ctx.reply(
        "Hmph. I might message you. Or not." if new_val
        else "Fine. I'll pretend you don't exist. Easy enough."
    )


@bot.command(name="dms", aliases=["allowdms", "stopdms"])
async def dms_cmd(ctx: commands.Context, mode: str = None):
    await mem.upsert_user(ctx.author.id, str(ctx.author), ctx.author.display_name)
    user    = await mem.get_user(ctx.author.id)
    current = user.get("allow_dms", True) if user else True
    new_val = True if mode == "on" else False if mode == "off" else not current
    await mem.set_mode(ctx.author.id, "allow_dms", new_val)
    await ctx.reply(
        "Fine. I'll message you when I feel like it." if new_val
        else "Cutting me off? Fine. I'll pretend you don't exist."
    )


@bot.command(name="mood")
async def mood_cmd(ctx: commands.Context):
    """Check what mood Scaramouche is in toward you."""
    await mem.upsert_user(ctx.author.id, str(ctx.author), ctx.author.display_name)
    score = await mem.get_mood(ctx.author.id)
    bar   = "█" * (score + 10) + "░" * (20 - (score + 10))
    label = mood_label(score)
    await ctx.reply(f"`[{bar}]` {score:+d} — {label}\n*Don't read into this.*")


@bot.command(name="scarahelp", aliases=["commands", "help"])
async def help_cmd(ctx: commands.Context):
    embed = discord.Embed(
        title="Commands — Don't Make Me Repeat Myself",
        description="*Hmph. I'll only say this once.*",
        color=0x4B0082
    )
    embed.add_field(name="💬 `!wander <msg>`",          value="Talk to him  ·  `!w` `!scara`",             inline=False)
    embed.add_field(name="⚔️ `!spar [msg]`",            value="Challenge him to a word battle",             inline=False)
    embed.add_field(name="🔍 `!judge [@user]`",         value="Brutal character assessment",                inline=False)
    embed.add_field(name="🔮 `!prophecy [@user]`",      value="A cryptic threatening fortune",              inline=False)
    embed.add_field(name="📊 `!rate <thing>`",          value="He rates anything out of 10",               inline=False)
    embed.add_field(name="🗡️ `!rival [@user]`",         value="Designate a rival (or clear it)",           inline=False)
    embed.add_field(name="⏰ `!remind <mins> <text>`",  value="He reminds you later, with disdain",        inline=False)
    embed.add_field(name="🌐 `!translate <text>`",      value="Rewritten in his voice",                    inline=False)
    embed.add_field(name="🔍 `!search <query>`",        value="Web search with commentary",                inline=False)
    embed.add_field(name="🧮 `!solve <problem>`",       value="Math, essays, Q&A",                         inline=False)
    embed.add_field(name="📜 `!lore <topic>`",          value="Genshin lore from his perspective",         inline=False)
    embed.add_field(name="⚡ `!insult [@user]`",        value="A cutting personalised insult",             inline=False)
    embed.add_field(name="🔊 `!voice <msg>`",           value="Voice message response",                    inline=False)
    embed.add_field(name="📨 `!dm [msg]`",              value="He DMs you privately",                      inline=False)
    embed.add_field(name="🌡️ `!mood`",                  value="Check his mood toward you",                 inline=False)
    embed.add_field(name="🔄 `!reset`",                 value="Wipe your memory  ·  `!forget`",            inline=False)
    embed.add_field(name="🔞 `!nsfw [on/off]`",         value="Toggle unfiltered mode",                    inline=False)
    embed.add_field(name="💕 `!romance [on/off]`",      value="Toggle clingy/romance mode",                inline=False)
    embed.add_field(name="📡 `!proactive [on/off]`",    value="Toggle unprompted channel messages",        inline=False)
    embed.add_field(name="💌 `!dms [on/off]`",          value="Toggle voluntary private DMs",              inline=False)
    embed.add_field(
        name="💡 Tips",
        value=(
            "• @mention or reply to chat naturally\n"
            "• Sometimes he responds with voice automatically\n"
            "• He remembers what you said days ago and brings it up\n"
            "• Being rude to him makes him more hostile (`!mood` to check)\n"
            "• He comments when people join or leave the server"
        ),
        inline=False
    )
    embed.set_footer(text="Scaramouche — The Balladeer | Claude AI + Fish Audio")
    await ctx.send(embed=embed)


# ── Error handling ────────────────────────────────────────────────────────────
@bot.event
async def on_command_error(ctx: commands.Context, error):
    if isinstance(error, commands.CommandNotFound):
        pass
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.reply("You're missing something. Try again properly.")
    else:
        print(f"[Command Error] {error}")


# ── Entry ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise SystemExit("❌  DISCORD_TOKEN not set in .env")
    if not ANTHROPIC_API_KEY:
        raise SystemExit("❌  ANTHROPIC_API_KEY not set in .env")
    bot.run(DISCORD_TOKEN)
