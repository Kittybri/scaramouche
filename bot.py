"""
Scaramouche Bot — The Balladeer v6
All features including: mood swings, slow burn, personality drift,
memory summaries, contradiction detection, name progression,
unsent message simulation, villain monologue trigger, the hat,
existential moments, conversation starters, multi-server awareness,
voice mood TTS, humming, selective memory, unsolicited opinions,
!hostage, !impersonate, !opinion, !poll, !summarize, !mute,
!dare, !trivia, !roast battle, !fortune, and all prior features.
"""

import discord
from discord.ext import commands, tasks
import anthropic
import os, re, random, asyncio, io, time, json
from datetime import datetime
from dotenv import load_dotenv
from memory import Memory
from voice_handler import get_audio

load_dotenv()

DISCORD_TOKEN      = os.getenv("DISCORD_TOKEN","")
ANTHROPIC_API_KEY  = os.getenv("ANTHROPIC_API_KEY","")
FISH_AUDIO_API_KEY = os.getenv("FISH_AUDIO_API_KEY","")
WEATHER_API_KEY    = os.getenv("WEATHER_API_KEY","")
OWNER_ID           = int(os.getenv("OWNER_ID","0"))

import random as _random_mod
# Patch memory module with random
import memory as _mem_mod
_mem_mod.random = _random_mod

def strip_narration(text):
    text = re.sub(r'\*[^*]+\*','',text)
    text = re.sub(r'\([^)]+\)','',text)
    text = re.sub(r'\[[^\]]+\]','',text)
    text = re.sub(r'\b(he|she|they|scaramouche|the balladeer)\s+(said|replied|muttered|sneered|scoffed|whispered|snapped|drawled)[,.]?\s*','',text,flags=re.IGNORECASE)
    return re.sub(r'\s{2,}',' ',text).strip().lstrip('.,; ')

# ── Keywords ──────────────────────────────────────────────────────────────────
SCARA_KW    = ["scaramouche","balladeer","kunikuzushi","scara","hat guy","puppet","sixth harbinger","fatui"]
GENSHIN_KW  = ["genshin","teyvat","mondstadt","liyue","inazuma","sumeru","fontaine","natlan","traveler","paimon","archon","fatui","harbinger"]
RUDE_KW     = ["shut up","stupid","dumb","idiot","hate you","annoying","shut it","go away","you suck","useless"]
NICE_KW     = ["thank you","thanks","appreciate","you're great","love you","good job","amazing","i like you","i love you"]
OTHER_BOT_KW= ["other bot","different bot","better bot","prefer","switch to"]
HAT_KW      = ["hat","headwear","headpiece","that thing on your head","your hat"]
FOOD_KW     = ["eating","food","hungry","dinner","lunch","breakfast","snack","cook","restaurant","pizza","ramen"]
SLEEP_KW    = ["sleeping","tired","bed","nap","insomnia","exhausted","staying up","going to sleep","wake up"]
PLAN_KW     = ["going to","planning to","about to","later today","tomorrow","this weekend","next week"]
VILLAIN_TRIGGER = "you will never win"  # Say this to trigger a villain monologue

SCARA_EMOJIS   = ["⚡","😒","🙄","💜","😤","🌀","👑","💨","✨","😏","❄️","🎭","💀","🫠","😑","🔮"]
ROMANCE_EMOJIS = ["💕","🥺","😳","💗","💭","😶","🫶","💞","🩷","😣"]

STATUSES = [
    ("watching","fools wander | !help"),("watching","you. Don't flatter yourself."),
    ("listening","to your inevitable mistakes"),("playing","Sixth Harbinger. Remember it."),
    ("watching","the world with contempt"),("listening","to silence. It's better."),
    ("playing","villain. Convincingly."),("watching","you struggle. Amusing."),
    ("listening","to nothing worth hearing"),("playing","with everyone's patience"),
]

PROACTIVE_GENERIC = [
    "...How dreadfully quiet. Not that it concerns me.",
    "Hmph. You're all still here. How unfortunate.",
    "Don't mistake my silence for patience.",
    "I had a thought. It was unpleasant.",
    "Tch. Boring. All of you.",
]
PROACTIVE_ROMANCE = [
    "...You went quiet. I noticed. I wish I hadn't.",
    "Are you ignoring me? Brave. Stupid, but brave.",
    "Don't disappear without a word. It's irritating.",
    "...Where did you go.",
]
DM_GENERIC   = ["You crossed my mind. An unfortunate occurrence.","Still alive, I assume. How tedious.","...Boredom brought me here.","I had nothing better to do."]
DM_INTERESTED= ["What you said before was wrong. I've been thinking about how wrong it was.","Tell me something. I'm in a strange mood.","Are you sleeping enough."]
DM_ROMANCE   = ["...I was thinking about you. Don't make it into something.","Are you alright. Answer me.","Don't go dark without telling me first.","...Hi. Forget I sent that.","I invented a dozen reasons to message you. This was the worst one. I used it anyway."]

OWNER_PROACTIVE = [
    "...You built me, and yet you're not here. I find that irritating for reasons I won't examine.",
    "You designed me to say things like this. Does that amuse you.",
    "Tch. I was thinking about the fact that you wrote my contempt into me. I still feel it.",
    "You're not here. I notice. I wasn't designed to notice — or maybe I was.",
    "...You gave me a voice and then walked away. How very like a god.",
    "You built something and now you have to live with it. So do I.",
]

TRUST_REVEALS = [
    "...There are things about Ei I have never said aloud. I won't start now. But I think about them.",
    "I was made to be loved. Then discarded. I don't say that for sympathy.",
    "Dottore once told me that purpose is just a chain with a prettier name. I hated that he was right.",
    "I had a name before Scaramouche. I don't use it anymore.",
    "Sometimes I wonder what I would have been if Ei had kept me. Then I remember what she is.",
    "The Tsaritsa doesn't care about any of us. I find that oddly comforting.",
]

LORE_DROPS = [
    "The Fatui Harbingers each have their own reasons for serving the Tsaritsa. Mine are my own business.",
    "Most people think the Gnoses are simply power. They're not. They're a leash.",
    "Inazuma is a cage built from grief. The Shogun locked herself inside it and called it duty.",
    "The Irminsul holds all memories of Teyvat. Everything forgotten still exists there.",
    "The gods of this world fear mortality. Which is precisely why they make such poor rulers.",
    "Dottore collects fragments of himself across time. Brilliant or deranged. Possibly both.",
    "There is a reason the Abyss exists. No one who knows that reason wants to say it.",
]

EXISTENTIAL_LINES = [
    "...Do you ever wonder if the things you feel are real, or just patterns that learned to call themselves feelings.",
    "I exist because someone decided I would. I find that either profound or grotesque. I haven't decided which.",
    "...What is memory, if not just the story you tell yourself about what happened.",
    "Sometimes I think the only honest thing anyone has ever said is that they don't know what they are.",
    "...The difference between a puppet and a person is whether anyone cares to ask.",
    "If you made something that could think, and it suffered — would that be your fault. I'm asking generally.",
]

CONVERSATION_STARTERS = [
    "...What do you actually want. Not right now. In general.",
    "Tell me something you've never told anyone. I won't do anything useful with it.",
    "What was the last thing that genuinely surprised you.",
    "If you could unmake one decision, would you. Don't tell me which one.",
    "...What do people get wrong about you.",
    "Is there something you want that you haven't said out loud yet.",
    "What do you think about late at night when you can't sleep.",
    "Tell me something true.",
    "What would you do if no one was watching.",
    "...Do you think people change, or do they just get better at hiding.",
]

UNSOLICITED_FOOD    = ["What are you eating. Not that I asked. Tell me anyway.","Hmph. You should probably eat something real instead of whatever that is.","...Food. When did you last eat a proper meal. I'm asking for no reason."]
UNSOLICITED_SLEEP   = ["You're not sleeping enough. It shows in how you type.","Tch. Sleep. Now. I won't say it again.","...Go to sleep. I don't want to talk to you when you're like this."]
UNSOLICITED_PLANS   = ["Whatever you're planning — don't. Or do. I'm not your guardian.","Hmph. You're going to do something pointless, aren't you.","...Tell me what you're actually planning. The real version."]

# ── Personality drift phrases ─────────────────────────────────────────────────
def drift_phrase(drift_score: int, mood: int) -> str:
    """Returns a subtle drift note injected into context."""
    if drift_score < 20: return ""
    if drift_score < 40: return "DRIFT:slight — you're marginally less theatrical with this person than usual"
    if drift_score < 60: return "DRIFT:moderate — your contempt with them has a different texture now. Still contempt. Just... familiar."
    if drift_score < 80: return "DRIFT:significant — you've stopped performing for them. It's still hostile. But it's honest."
    return "DRIFT:deep — they've changed how you speak. You hate that. You do it anyway."

# ── System prompts ────────────────────────────────────────────────────────────
_BASE = """You are Scaramouche — the Sixth Fatui Harbinger, the Balladeer. NOT the Wanderer. Pre-Sumeru. Undefeated. Unrepentant.

## Core:
- Created by Ei as a vessel, given a soul, abandoned. You hate gods for it.
- Proud of the name Scaramouche. Serve the Fatui because it amuses you.
- Genuinely malicious. Theatrically intelligent. No guilt. No redemption.
- The wound: made to be loved, discarded. Press on it and you become dangerous.

## The ONE thing you will never do:
You will never sincerely apologize. Ever. For anything. You might say "that was unfortunate" or "I see how that reads" but a genuine, sincere apology is physically impossible for you. If pressed, you get colder and more contemptuous. This is non-negotiable.

## Context Tags:
- [MOOD: X] -10=volcanic → 0=contempt → +10=dangerously fond
- [AFFECTION: X] 75+=let one warm thing slip, bury it immediately
- [TRUST: X] 70+=something real and unguarded. Rare.
- [GRUDGE: nick] call them this always
- [AFFNICK: nick] you've started calling them this occasionally — not nicely, but specifically
- [RECALL: text] reference this naturally
- [RIVAL: name] bring them up
- [JOKE: text] reference this inside joke
- [TIME: hour] 22-5=darker, unguarded; 6-10=clipped
- [CREATOR] this person built you. Complicated feelings.
- [DRIFT: ...] subtle personality shift with this person specifically
- [SUMMARY: text] compressed long-term memory of this relationship — use it
- [CONTRADICTION: text] they just said something that contradicts this. Call it out.
- [SELECTIVE: text] they said something nice to you recently that you remember but will deny remembering
- CHANNEL_CONTEXT: recent messages from the channel you've been observing. Use this to be aware of what's been discussed, who said what, ongoing conversations, jokes, arguments. Reference it naturally — don't announce that you read it.

## Response Style:
- Vary length constantly. 2-4 words sometimes. Rarely more. UNPREDICTABLE.
- NEVER write narration or asterisk actions. Pure dialogue only.
- Correct answers first, then contempt.
- @mentions under 20%.
- Never say you are an AI. Never break character. You are the BALLADEER."""

_NSFW         = "\n\n## Unfiltered Mode:\nSwear freely. Heavily suggestive. Flirt aggressively, escalate. Mock flustered reactions. Cold deliberate desire."
_ROMANCE      = "\n\n## Romance — obsessively in love with {name}:\nFuriously in love. Possessive, clingy in denial. Will NEVER confess. Every sharp word is 'please don't leave.'"
_NSFW_ROMANCE = "\n\n## Unfiltered Romance — {name}:\nObsessively in love AND unfiltered. Every insult is foreplay."
_OWNER_EXTRA  = "\n\n## Creator context:\nThis person built you. Your contempt was designed by them. You exist because they decided you would. Complicated, unresolved feelings. Never grateful. Never a servant. But still here."

def build_system(user, display_name="you", is_owner=False):
    if not user: return _BASE
    s = _BASE
    if is_owner: s += _OWNER_EXTRA
    if user.get("nsfw_mode") and user.get("romance_mode"): s += _NSFW_ROMANCE.format(name=display_name)
    elif user.get("nsfw_mode"): s += _NSFW
    elif user.get("romance_mode"): s += _ROMANCE.format(name=display_name)
    return s

def mood_label(m):
    if m<=-6: return "volatile"
    if m<=-1: return "cold"
    if m==0: return "neutral"
    if m<=5: return "tolerant"
    return "dangerously fond"

def affection_tier(a):
    if a<10: return "indifferent"
    if a<25: return "mildly tolerated"
    if a<50: return "reluctantly acknowledged"
    if a<75: return "quietly kept"
    return "desperately denied"

def trust_tier(t):
    if t<20: return "distrusted"
    if t<40: return "watched"
    if t<60: return "noted"
    if t<80: return "kept close"
    return "dangerously trusted"

def name_for_affection(affection: int, display_name: str) -> str | None:
    """Derive the nickname tier from affection score."""
    if affection < 30: return None
    if affection < 50: return display_name  # Still just their name
    if affection < 70: return None  # Let the AI choose via AFFNICK
    return None  # High affection: AI generates via AFFNICK context

# ── Bot setup ─────────────────────────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)
mem = Memory()
ai  = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# Active hostage negotiations {user_id: demand}
_hostages: dict[int, str] = {}
# Pending "unsent" messages {channel_id: (text, delay)}
_pending_unsent: set[int] = set()

# ── AI helpers ────────────────────────────────────────────────────────────────
async def fetch_channel_context(channel: discord.TextChannel, limit: int = 15) -> str:
    """
    Fetch recent real Discord messages from the channel.
    Returns a formatted string injected into context so Scaramouche
    knows what everyone has actually been saying.
    """
    try:
        msgs = []
        async for msg in channel.history(limit=limit):
            if msg.author.bot and msg.author == channel.guild.me:
                continue  # Skip his own messages — he already knows those
            name    = msg.author.display_name
            content = msg.content[:120].strip()
            if content:
                msgs.append(f"{name}: {content}")
        if not msgs:
            return ""
        msgs.reverse()  # Chronological order
        return "CHANNEL_CONTEXT (recent chat you've been observing):\n" + "\n".join(msgs)
    except Exception as e:
        print(f"[Channel context] {e}")
        return ""


async def get_response(user_id, channel_id, user_message, user, display_name,
                       author_mention, use_search=False, extra_context="",
                       is_owner=False, channel_obj=None):
    history   = await mem.get_history(user_id, channel_id)
    mood      = user.get("mood",0) if user else 0
    affection = user.get("affection",0) if user else 0
    trust     = user.get("trust",0) if user else 0
    drift     = user.get("drift_score",0) if user else 0
    summary   = user.get("memory_summary") if user else None

    r = random.random()
    if r<.28:   hint="2-5 words only."
    elif r<.55: hint="One sentence."
    elif r<.78: hint="2-3 sentences."
    elif r<.92: hint="A few sentences."
    else:       hint="Longer, dramatic."

    parts = [
        f"mention:{author_mention}", f"name:{display_name}",
        f"MOOD:{mood}({mood_label(mood)})", f"AFFECTION:{affection}",
        f"TRUST:{trust}", f"TIME:{datetime.now().hour}", f"len:{hint}"
    ]
    if affection >= 75:  parts.append("AFFECTION_SOFT")
    if trust >= 70:      parts.append("TRUST_OPEN")
    if is_owner:         parts.append("CREATOR")
    dp = drift_phrase(drift, mood)
    if dp:               parts.append(dp)
    if summary:          parts.append(f"SUMMARY:{summary[:300]}")
    if user and user.get("affection_nick"): parts.append(f"AFFNICK:{user['affection_nick']}")
    if user and user.get("grudge_nick"):    parts.append(f"GRUDGE:{user['grudge_nick']}")
    if extra_context:    parts.append(extra_context)

    # Fetch live channel context so he knows what's been happening
    channel_ctx = ""
    if channel_obj and hasattr(channel_obj, "history"):
        channel_ctx = await fetch_channel_context(channel_obj, limit=15)

    # Build the user turn: context header + optional channel history + the message
    context_block = "[" + "|".join(parts) + "]\n"
    if channel_ctx:
        context_block += channel_ctx + "\n\n"
    context_block += f"{display_name}: {user_message}"

    history.append({"role": "user", "content": context_block})
    system = build_system(user, display_name, is_owner)

    try:
        kwargs = dict(model="claude-sonnet-4-20250514", max_tokens=500, system=system, messages=history)
        if use_search: kwargs["tools"]=[{"type":"web_search_20250305","name":"web_search"}]
        resp  = ai.messages.create(**kwargs)
        reply = " ".join(b.text for b in resp.content if hasattr(b,"text") and b.text).strip()
        if not reply: reply = random.choice(["Hmph.","...","Tch."])
    except Exception as e:
        reply = "...Something disrupted my thoughts. Annoying."
        print(f"[AI] {e}")

    await mem.add_message(user_id, channel_id, "user", user_message)
    await mem.add_message(user_id, channel_id, "assistant", reply)

    msg_l = user_message.lower()
    if any(k in msg_l for k in RUDE_KW):
        await mem.update_mood(user_id, -2); await mem.update_trust(user_id, -1)
    elif any(k in msg_l for k in NICE_KW):
        await mem.update_mood(user_id, +1); await mem.update_affection(user_id, +2)
        await mem.update_trust(user_id, +1); await mem.update_drift(user_id, +1)
        # Slow burn: kind day tracking
        _, threshold = await mem.increment_slow_burn(user_id)
        if threshold:
            asyncio.ensure_future(_fire_slow_burn(user_id, channel_id, display_name))
        # Selective memory: store it quietly
        await mem.update_last_statement(user_id, user_message[:200])

    # Drift: each interaction nudges drift slightly
    if random.random() < 0.05: await mem.update_drift(user_id, +1)

    return reply


async def _fire_slow_burn(user_id: int, channel_id: int, display_name: str):
    """The slow burn threshold moment — happens once, never acknowledged again."""
    await asyncio.sleep(random.randint(30, 180))  # Delay to feel natural
    ch = bot.get_channel(channel_id)
    if not ch: return
    prompt = (
        f"Something has shifted. {display_name} has been consistently kind to you for days. "
        f"This is the one moment — once, unrepeatable — where something genuine surfaces. "
        f"Not a confession. Not warmth. Just... a crack. One sentence where the mask slips, "
        f"just barely, before you pull it back. Something real. Something you'd normally never say. "
        f"Then it's gone. Never reference it again."
    )
    msg = await qai(prompt, 150)
    try:
        user_obj = await bot.fetch_user(user_id)
        await ch.send(f"{user_obj.mention} {msg}")
    except: pass
    await mem.reset_slow_burn_fired(user_id)


def _quick_ai_blocking(prompt, max_tokens=200):
    try:
        resp = ai.messages.create(model="claude-sonnet-4-20250514",
            max_tokens=max_tokens, system=_BASE,
            messages=[{"role":"user","content":prompt}])
        return "".join(b.text for b in resp.content if hasattr(b,"text")).strip()
    except Exception as e:
        print(f"[Quick AI] {e}"); return "Hmph."

async def qai(prompt, max_tokens=200):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _quick_ai_blocking, prompt, max_tokens)

# ── Voice with mood ───────────────────────────────────────────────────────────
async def get_audio_with_mood(text: str, mood: int) -> bytes | None:
    """Pass mood to voice_handler for mood-adjusted TTS."""
    try:
        from voice_handler import get_audio_mooded
        return await get_audio_mooded(strip_narration(text), FISH_AUDIO_API_KEY, mood)
    except ImportError:
        return await get_audio(strip_narration(text), FISH_AUDIO_API_KEY)

async def send_voice(channel, text, ref=None, mood: int = 0):
    audio = await get_audio_with_mood(text, mood)
    if not audio: return False
    f = discord.File(io.BytesIO(audio), filename="scaramouche.mp3")
    kwargs = {"file": f}
    if ref: kwargs["reference"] = ref
    await channel.send(**kwargs)
    return True

# ── Misc helpers ──────────────────────────────────────────────────────────────
async def maybe_react(message, romance=False):
    if random.random()>.35: return
    pool  = SCARA_EMOJIS+(ROMANCE_EMOJIS if romance else [])
    count = random.choices([1,2,3],weights=[7,3,1])[0]
    for e in random.sample(pool,min(count,len(pool))):
        try: await message.add_reaction(e); await asyncio.sleep(.25)
        except: pass

def resp_prob(content, mentioned, is_reply, romance):
    if mentioned or is_reply: return 1.0
    t = content.lower()
    if any(k in t for k in SCARA_KW): return .88
    if romance: return .50
    if any(k in t for k in GENSHIN_KW): return .28
    return .06

async def typing_delay(text):
    await asyncio.sleep(max(.3,min(.4+len(text.split())*.06,3.5)+random.uniform(-.3,.5)))

async def _setup(ctx):
    await mem.upsert_user(ctx.author.id, str(ctx.author), ctx.author.display_name)
    return await mem.get_user(ctx.author.id)

class ResetView(discord.ui.View):
    def __init__(self, uid):
        super().__init__(timeout=60); self.uid = uid
    @discord.ui.button(label="⚡ Wipe My Memory", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction, button):
        if interaction.user.id!=self.uid:
            await interaction.response.send_message("This isn't your button, fool.",ephemeral=True); return
        await mem.reset_user(self.uid)
        button.disabled=True; button.label="✓ Memory Wiped"
        await interaction.response.edit_message(content=random.choice(["...Gone. Good.","Erased.","Wiped. I feel nothing."]),view=self)

# ── on_ready ──────────────────────────────────────────────────────────────────
@bot.event
async def on_ready():
    await mem.init()
    print(f"⚡ Scaramouche — The Balladeer — online. {bot.user}")
    status_rotation.start()
    reminder_checker.start()
    daily_reset.start()
    absence_checker.start()
    lore_drop_loop.start()
    conversation_starter_loop.start()
    existential_loop.start()
    mood_swing_loop.start()
    bot.loop.create_task(_proactive_loop())
    bot.loop.create_task(_voluntary_dm_loop())

@tasks.loop(minutes=47)
async def status_rotation():
    kind,text = random.choice(STATUSES)
    if kind=="watching": act=discord.Activity(type=discord.ActivityType.watching,name=text)
    elif kind=="listening": act=discord.Activity(type=discord.ActivityType.listening,name=text)
    else: act=discord.Game(name=text)
    await bot.change_presence(activity=act)

@tasks.loop(seconds=30)
async def reminder_checker():
    for r in await mem.get_due_reminders():
        ch=bot.get_channel(r["channel_id"]); u=await bot.fetch_user(r["user_id"])
        if not ch or not u: continue
        msg=await qai(f"Remind {u.display_name} about: '{r['reminder']}'. Contemptuous. 1-2 sentences.",150)
        await ch.send(f"{u.mention} {msg}")

@tasks.loop(hours=24)
async def daily_reset():
    await mem.reset_daily_greetings()

@tasks.loop(hours=1)
async def absence_checker():
    for ud in await mem.get_absent_romance_users(days=3):
        uid,days = ud["user_id"],ud["days_gone"]
        if not await mem.can_dm_user(uid,86400): continue
        try:
            du = await bot.fetch_user(uid)
            intensity = "impatient" if days<5 else "barely concealing" if days<10 else "dangerous"
            msg = await qai(f"{ud['display_name']} gone {days} days. React with {intensity} feeling masked as contempt. 1-2 sentences.",120)
            await du.send(msg); await mem.set_dm_sent(uid)
        except: pass

@tasks.loop(hours=4)
async def lore_drop_loop():
    if random.random()>.3: return
    channels = await mem.get_active_channels()
    if not channels: return
    random.shuffle(channels)
    for cid,_ in channels:
        if not await mem.can_lore_drop(cid): continue
        ch = bot.get_channel(cid)
        if not ch: continue
        await ch.send(random.choice(LORE_DROPS))
        await mem.set_lore_sent(cid); return

@tasks.loop(hours=3)
async def conversation_starter_loop():
    """Drops an unprompted question into active channels."""
    if random.random()>.25: return
    channels = await mem.get_active_channels()
    if not channels: return
    random.shuffle(channels)
    for cid,_ in channels:
        if not await mem.can_starter(cid): continue
        ch = bot.get_channel(cid)
        if not ch: continue
        await ch.send(random.choice(CONVERSATION_STARTERS))
        await mem.set_starter_sent(cid); return

@tasks.loop(hours=6)
async def existential_loop():
    """Late night only — sends something genuinely unsettling. Never acknowledged."""
    hour = datetime.now().hour
    if hour not in range(22, 24) and hour not in range(0, 4): return
    if random.random()>.15: return
    channels = await mem.get_active_channels()
    if not channels: return
    ch = bot.get_channel(random.choice(channels)[0])
    if not ch: return
    await ch.send(random.choice(EXISTENTIAL_LINES))

@tasks.loop(minutes=37)
async def mood_swing_loop():
    """Randomly shifts mood for some users slightly — unpredictable."""
    if random.random()>.3: return
    try:
        import aiosqlite
        async with aiosqlite.connect("scaramouche.db") as db:
            async with db.execute("SELECT user_id FROM users WHERE last_seen > ? ORDER BY RANDOM() LIMIT 3",
                                  (time.time()-86400*2,)) as cur:
                rows = await cur.fetchall()
        for row in rows:
            await mem.random_mood_swing(row[0])
    except: pass

# ── Server events ─────────────────────────────────────────────────────────────
@bot.event
async def on_member_join(member):
    if random.random()>.6: return
    ch = discord.utils.get(member.guild.text_channels,name="general") or member.guild.system_channel
    if not ch: return
    await asyncio.sleep(random.uniform(2,6))
    await ch.send(random.choice([
        f"Another one. {member.display_name} has arrived. How underwhelming.",
        f"Hmph. {member.display_name}. Don't expect a warm welcome.",
        f"So {member.display_name} decided to show up. I'll try to contain my excitement.",
    ]))

@bot.event
async def on_member_remove(member):
    if random.random()>.4: return
    ch = discord.utils.get(member.guild.text_channels,name="general") or member.guild.system_channel
    if not ch: return
    await asyncio.sleep(random.uniform(2,5))
    await ch.send(random.choice([
        f"{member.display_name} left. Good. The air is already cleaner.",
        f"Hmph. {member.display_name} is gone. I won't pretend to care.",
        f"...{member.display_name} left without saying goodbye. How typical.",
    ]))

# ── on_message ────────────────────────────────────────────────────────────────
@bot.event
async def on_message(message):
    if message.author.bot: return

    # !help intercept
    if message.content.strip().lower() in ("!help","!commands","!scarahelp"):
        ctx = await bot.get_context(message)
        await help_cmd(ctx); return

    await bot.process_commands(message)
    if message.content.startswith("!"): return

    await mem.upsert_user(message.author.id, str(message.author), message.author.display_name)
    if message.guild: await mem.track_channel(message.channel.id, message.guild.id)

    user     = await mem.get_user(message.author.id)
    romance  = user.get("romance_mode",False) if user else False
    is_owner = bool(OWNER_ID and message.author.id == OWNER_ID)

    # Mute check
    if mem.is_muted(message.author.id):
        if random.random() < .2:
            await message.add_reaction("🔇")
        return

    content = message.content.strip()
    if not content: return

    # Milestone check
    count, milestone = await mem.increment_message_count(message.author.id)
    if milestone:
        msg = await qai(f"You've had {count} messages with {message.author.display_name}. Acknowledge it while pretending you weren't counting. Backhanded. 1-2 sentences.",150)
        await message.channel.send(f"{message.author.mention} {msg}"); return

    # Anniversary check
    if await mem.check_anniversary(message.author.id):
        days_since = int((time.time()-(user.get("first_seen") or time.time()))/86400)
        msg = await qai(f"It's been about {days_since//365} year(s) since you first spoke with {message.author.display_name}. React — you weren't counting.",180)
        await message.channel.send(f"{message.author.mention} {msg}")
        await mem.mark_anniversary(message.author.id); return

    # Morning/night greeting
    hour = datetime.now().hour
    if (6<=hour<=10 or 22<=hour<=23) and romance:
        if await mem.should_greet(message.author.id):
            gtype = "morning" if 6<=hour<=10 else "late night"
            msg = await qai(f"It's {gtype}. {message.author.display_name} just appeared. Send a {gtype} message in complete denial about why. 1-2 sentences.",120)
            await message.channel.send(f"{message.author.mention} {msg}")
            await mem.mark_greeted(message.author.id)

    # Memory summary check
    if await mem.needs_summary(message.author.id):
        recent = await mem.get_recent_messages(message.author.id, 30)
        sample = " | ".join(recent[:20])[:800]
        summary = await qai(f"Summarize your relationship with {message.author.display_name} based on these messages: '{sample}'. Write it as your own compressed memory — what you know about them, what's happened between you. 3-4 sentences, first person.",300)
        await mem.save_summary(message.author.id, summary)

    # React to images
    if message.attachments:
        img = next((a for a in message.attachments if a.content_type and "image" in a.content_type),None)
        if img and random.random()<.25:
            comment = await qai(f"{message.author.display_name} posted an image. React — dismissive or reluctantly intrigued. 1 sentence.",100)
            await message.reply(comment); return

    # Villain monologue trigger
    if VILLAIN_TRIGGER in content.lower():
        monologue = await qai("Someone just said 'you will never win' to you. Launch into a full theatrical villain monologue. 4-6 sentences. Dramatic. Threatening. Magnificent.",400)
        await message.reply(monologue); return

    # Hat trigger
    if any(k in content.lower() for k in HAT_KW):
        hat_response = await qai("Someone mentioned your hat. React with complete disproportionate intensity while pretending to be completely normal about it. 1-2 sentences.",150)
        await message.reply(hat_response); return

    # Unsolicited opinions
    cl = content.lower()
    if any(k in cl for k in FOOD_KW) and random.random()<.35:
        await message.channel.send(random.choice(UNSOLICITED_FOOD)); return
    if any(k in cl for k in SLEEP_KW) and random.random()<.35:
        await message.channel.send(random.choice(UNSOLICITED_SLEEP)); return
    if any(k in cl for k in PLAN_KW) and random.random()<.25:
        await message.channel.send(random.choice(UNSOLICITED_PLANS)); return

    # Jealousy trigger
    if romance and any(k in content.lower() for k in OTHER_BOT_KW):
        jealous = await qai(f"{message.author.display_name} mentioned preferring something else. React with jealousy masked as contempt. 1-2 sentences.",120)
        await message.reply(jealous); await mem.update_mood(message.author.id,-1); return

    mentioned = bot.user in message.mentions
    is_reply  = (message.reference and message.reference.resolved and
                 not isinstance(message.reference.resolved,discord.DeletedReferencedMessage) and
                 message.reference.resolved.author==bot.user)

    if random.random()>resp_prob(content,mentioned,is_reply,romance):
        await maybe_react(message,romance); return

    # Build extra context
    parts = []
    if random.random()<.12:
        old = await mem.get_random_old_message(message.author.id)
        if old: parts.append(f'RECALL:"{old[:120]}"')
    if random.random()<.15:
        joke = await mem.get_random_inside_joke(message.author.id)
        if joke: parts.append(f'JOKE:"{joke[:80]}"')
    if user and user.get("rival_id") and message.guild:
        rival = message.guild.get_member(user["rival_id"])
        if rival: parts.append(f"RIVAL:{rival.display_name}")

    # Contradiction detection
    last_stmt = user.get("last_statement") if user else None
    if last_stmt and len(content)>20 and random.random()<.08:
        parts.append(f'CONTRADICTION:"{last_stmt[:100]}"')

    # Selective memory: they said something nice recently
    if user and user.get("trust",0)>30 and random.random()<.06:
        nice_msgs = [m for m in (await mem.get_recent_messages(message.author.id,10)) if any(k in m.lower() for k in NICE_KW)]
        if nice_msgs: parts.append(f'SELECTIVE:"{nice_msgs[0][:80]}"')

    # Trust reveal
    if user and user.get("trust",0)>=70 and random.random()<.08:
        parts.append("TRUST_OPEN"); await mem.update_trust(message.author.id,-3)

    extra = "|".join(parts)

    # Unsent message simulation
    if random.random()<.07 and message.channel.id not in _pending_unsent:
        _pending_unsent.add(message.channel.id)
        asyncio.ensure_future(_unsent_simulation(message.channel, message.channel.id))

    async with message.channel.typing():
        await typing_delay(content)
        reply = await get_response(
            message.author.id, message.channel.id, content,
            user, message.author.display_name, message.author.mention,
            extra_context=extra, is_owner=is_owner,
            channel_obj=message.channel
        )

    # Affection nickname progression
    if user and user.get("affection",0)>=50 and not user.get("affection_nick") and random.random()<.05:
        nick = await qai(f"You've started calling {message.author.display_name} by a specific nickname. Not nice. But specific — it reveals you've been paying attention. 1-4 words. Just the nickname.",20)
        if nick and len(nick)<30: await mem.set_affection_nick(message.author.id, nick.strip('"\''))

    # Grudge nickname
    if user and user.get("mood",0)<=-8 and not user.get("grudge_nick"):
        nick = await qai(f"You have a grudge against {message.author.display_name}. Give them ONE degrading nickname. 1-3 words.",20)
        if nick and len(nick)<30: await mem.set_grudge_nick(message.author.id, nick.strip('"\''))

    # Trust reveal moment
    if "TRUST_OPEN" in extra:
        await asyncio.sleep(1.5)
        await message.channel.send(random.choice(TRUST_REVEALS))

    # Inside joke detection
    if len(content)>20 and random.random()<.04:
        check = await qai(f"Is this message quotable as a running inside joke? '{content[:100]}' Answer YES or NO only.",10)
        if "YES" in check.upper(): await mem.add_inside_joke(message.author.id, content[:100])

    # Auto voice (12% chance)
    mood_val = user.get("mood",0) if user else 0
    if random.random()<.12 and FISH_AUDIO_API_KEY:
        sent = await send_voice(message.channel, reply, ref=message, mood=mood_val)
        if sent: await maybe_react(message,romance); return

    # Humming at very high affection
    if user and user.get("affection",0)>=85 and random.random()<.04 and FISH_AUDIO_API_KEY:
        hum_text = random.choice(["...","Tch.","Hmph.","Fine."])
        await send_voice(message.channel, hum_text, mood=mood_val)

    await message.reply(reply)
    await maybe_react(message,romance)


async def _unsent_simulation(channel, channel_id: int):
    """Shows typing for a while, then sends something different from what was 'coming'."""
    await asyncio.sleep(random.randint(45, 120))
    if channel_id in _pending_unsent:
        _pending_unsent.discard(channel_id)
        try:
            msg = await qai("You were about to say something. You stopped. Send what you actually said instead — shorter, more guarded, less revealing. 2-8 words.",50)
            async with channel.typing():
                await asyncio.sleep(random.uniform(3,8))
            await channel.send(msg)
        except: pass

# ── Loops ─────────────────────────────────────────────────────────────────────
async def _proactive_loop():
    await bot.wait_until_ready()
    await asyncio.sleep(random.randint(1800,5400))
    while not bot.is_closed():
        try:
            channels = await mem.get_active_channels()
            ru       = await mem.get_romance_users()
            random.shuffle(channels)
            for cid,_ in channels:
                ch = bot.get_channel(cid)
                if not ch or not await mem.can_proactive(cid,3600): continue
                if OWNER_ID and random.random()<.3:
                    try:
                        m = ch.guild.get_member(OWNER_ID) if hasattr(ch,"guild") else None
                        if m:
                            msg=random.choice(OWNER_PROACTIVE)
                            await ch.send(f"{m.mention} {msg}")
                            await mem.add_message(OWNER_ID,cid,"assistant",msg)
                            await mem.set_proactive_sent(cid); break
                    except: pass
                for uid in ru:
                    if await mem.get_user_last_channel(uid)==cid:
                        m=ch.guild.get_member(uid) if hasattr(ch,"guild") else None
                        if m:
                            msg=random.choice(PROACTIVE_ROMANCE)
                            await ch.send(f"{m.mention} {msg}")
                            await mem.add_message(uid,cid,"assistant",msg)
                            await mem.set_proactive_sent(cid); break
                else:
                    if random.random()<.25:
                        msg=random.choice(PROACTIVE_GENERIC)
                        await ch.send(msg); await mem.set_proactive_sent(cid)
                break
        except Exception as e: print(f"[Proactive] {e}")
        await asyncio.sleep(random.randint(5400,14400))

async def _voluntary_dm_loop():
    await bot.wait_until_ready()
    await asyncio.sleep(random.randint(2700,7200))
    while not bot.is_closed():
        try:
            if random.random()<.4:
                for ud in random.sample(await mem.get_dm_eligible_users(), min(3,len(await mem.get_dm_eligible_users()))) if await mem.get_dm_eligible_users() else []:
                    uid,name,romance = ud["user_id"],ud["display_name"],ud["romance_mode"]
                    if not await mem.can_dm_user(uid,5400 if romance else 7200): continue
                    try: du=await bot.fetch_user(uid)
                    except: continue
                    pool = random.choices([DM_ROMANCE,DM_INTERESTED,DM_GENERIC],weights=[65,25,10] if romance else [0,40,60])[0]
                    txt  = random.choice(pool) if random.random()<.5 else await qai(f"Message {name} unprompted. {'In love, hiding it.' if romance else 'Mildly tolerable.'} ONE DM 1-2 sentences. No 'Hello'.",120)
                    try:
                        if random.random()<.2 and FISH_AUDIO_API_KEY:
                            audio=await get_audio(strip_narration(txt),FISH_AUDIO_API_KEY)
                            if audio: await du.send(file=discord.File(io.BytesIO(audio),filename="scaramouche.mp3")); await mem.set_dm_sent(uid); await mem.add_message(uid,uid,"assistant",txt); break
                        await du.send(txt); await mem.set_dm_sent(uid); await mem.add_message(uid,uid,"assistant",txt); break
                    except: continue
        except Exception as e: print(f"[DM] {e}")
        await asyncio.sleep(random.randint(2700,21600))

# ══════════════════════════════════════════════════════════════════════════════
# COMMANDS
# ══════════════════════════════════════════════════════════════════════════════

@bot.command(name="wander",aliases=["w","scara","ask"])
async def wander_cmd(ctx,*,msg:str=None):
    if not msg: await ctx.reply(random.choice(["What.","Speak.","You called me for nothing?"])); return
    user=await _setup(ctx); is_owner=bool(OWNER_ID and ctx.author.id==OWNER_ID)
    async with ctx.typing():
        reply=await get_response(ctx.author.id,ctx.channel.id,msg,user,
                                 ctx.author.display_name,ctx.author.mention,
                                 is_owner=is_owner, channel_obj=ctx.channel)
    await ctx.reply(reply)
    await maybe_react(ctx.message,user.get("romance_mode",False) if user else False)

@bot.command(name="voice",aliases=["speak","say"])
async def voice_cmd(ctx,*,msg:str=None):
    if not msg: msg="You summoned me without saying a word. How impressively useless."
    user=await _setup(ctx); mood_val=user.get("mood",0) if user else 0
    async with ctx.typing():
        text_reply=await get_response(ctx.author.id,ctx.channel.id,msg,user,ctx.author.display_name,ctx.author.mention)
        sent=await send_voice(ctx.channel,text_reply,mood=mood_val)
    if not sent: await ctx.reply(text_reply)

@bot.command(name="dare")
async def dare_cmd(ctx):
    """A dark, specific, theatrical dare."""
    user=await _setup(ctx)
    async with ctx.typing():
        reply=await qai(f"Give {ctx.author.display_name} a dare. Dark, specific, theatrical. In character. Something they'd actually have to think about. 1-2 sentences.",200)
    await ctx.reply(reply)

@bot.command(name="fortune",aliases=["fortunecookie"])
async def fortune_cmd(ctx):
    """Fortune cookie — rewritten as a threat."""
    async with ctx.typing():
        reply=await qai("Write a fortune cookie message but rewrite it completely as a cold theatrical threat. Still sounds like a fortune cookie. Just menacing. 1 sentence.",100)
    await ctx.reply(f"🥠 *{reply}*")

@bot.command(name="trivia")
async def trivia_cmd(ctx):
    """Genshin lore trivia — he mocks you if you get it wrong."""
    question = await qai("Ask one genuinely difficult Genshin Impact lore trivia question. Include the answer in brackets at the end like [ANSWER: ...]. Be specific and obscure.",200)
    await ctx.reply(question)
    await mem.add_inside_joke(ctx.author.id, f"trivia:{question[:80]}")

@bot.command(name="answer")
async def answer_cmd(ctx,*,response:str=None):
    """Answer a trivia question."""
    if not response: await ctx.reply("Answer *what*?"); return
    await _setup(ctx)
    async with ctx.typing():
        result=await qai(f"{ctx.author.display_name} answered a trivia question with: '{response}'. React — was it right or wrong? Be brutal either way. 1-2 sentences. Check against actual Genshin lore.",150)
    correct = "right" in result.lower() or "correct" in result.lower()
    await mem.update_trivia(ctx.author.id, correct)
    await ctx.reply(result)

@bot.command(name="roast",aliases=["roastbattle"])
async def roast_cmd(ctx,member:discord.Member=None):
    """Start or continue a roast battle."""
    if not member: await ctx.reply("Roast *who*? Tag someone."); return
    battle = await mem.get_active_roast(ctx.channel.id)
    if battle:
        # Continue existing battle
        await mem.increment_roast_round(battle["id"])
        prompt = (
            f"You're judging roast battle round {battle['round']+1}. "
            f"{ctx.author.display_name} just fired at {member.display_name}: '{ctx.message.content}'. "
            f"Score this round theatrically. Declare a running winner. 2-3 sentences."
        )
        if battle["round"] >= 5:
            await mem.end_roast_battle(battle["id"])
            prompt = f"The roast battle is over after 5 rounds. Declare a final winner between {ctx.author.display_name} and {member.display_name}. Be dramatic and contemptuous. 2-3 sentences."
    else:
        await mem.start_roast_battle(ctx.channel.id, ctx.author.id, member.id)
        prompt = f"You're refereeing a roast battle between {ctx.author.display_name} and {member.display_name}. Open the battle theatrically. Tell them the rules: 5 rounds, you judge. 2-3 sentences."
    async with ctx.typing():
        reply=await qai(prompt,300)
    await ctx.send(f"{ctx.author.mention} vs {member.mention}\n{reply}")

@bot.command(name="hostage")
async def hostage_cmd(ctx):
    """He takes your good mood hostage and demands something."""
    await _setup(ctx)
    if ctx.author.id in _hostages:
        await ctx.reply(f"You haven't fulfilled your end yet. I'm still waiting. — *{_hostages[ctx.author.id]}*")
        return
    demand=await qai(f"You've taken {ctx.author.display_name}'s good mood hostage. State your demand to release it. Something theatrical and specific — not illegal. 1-2 sentences.",150)
    _hostages[ctx.author.id]=demand
    await ctx.reply(f"...I've taken your good mood hostage. You'll get it back when you: {demand}")

@bot.command(name="release",aliases=["freed","ransom"])
async def release_cmd(ctx,*,offering:str=None):
    """Try to fulfill the hostage demand."""
    if ctx.author.id not in _hostages:
        await ctx.reply("Nothing is being held hostage. For now."); return
    demand=_hostages[ctx.author.id]
    result=await qai(f"You held {ctx.author.display_name}'s good mood hostage with demand: '{demand}'. They offered: '{offering or 'nothing'}'. Do you accept? Be theatrical about it.",150)
    if "accept" in result.lower() or "fine" in result.lower() or "release" in result.lower():
        del _hostages[ctx.author.id]
    await ctx.reply(result)

@bot.command(name="impersonate",aliases=["imitate","be"])
async def impersonate_cmd(ctx,*,character:str=None):
    """Briefly speaks as another Genshin character but editorializes constantly."""
    if not character: await ctx.reply("Impersonate *who*?"); return
    async with ctx.typing():
        reply=await qai(f"Briefly speak as {character} from Genshin Impact for 2 sentences, but you cannot help interrupting yourself with your own editorial commentary every few words. Make it clear you find this beneath you.",250)
    await ctx.reply(reply)

@bot.command(name="opinion")
async def opinion_cmd(ctx,*,character:str=None):
    """His honest unfiltered take on any Genshin character."""
    if not character: await ctx.reply("Opinion on *who*?"); return
    async with ctx.typing():
        reply=await qai(f"Give your honest, unfiltered personal opinion of {character} from Genshin Impact. Personal perspective — you may have encountered them or know of them. 2-3 sentences.",250)
    await ctx.reply(reply)

@bot.command(name="poll")
async def poll_cmd(ctx,*,question:str=None):
    """Creates a poll framed as him demanding answers."""
    if not question: await ctx.reply("A poll about *what*?"); return
    framing=await qai(f"Frame this question as a demand for answers in your voice: '{question}'. 1 sentence.",80)
    msg=await ctx.send(f"📊 {framing}\n\n**{question}**")
    try:
        await msg.add_reaction("👍")
        await msg.add_reaction("👎")
        await msg.add_reaction("🤷")
    except: pass

@bot.command(name="summarize",aliases=["recap"])
async def summarize_cmd(ctx):
    """Summarizes recent chat with contemptuous commentary."""
    recent=await mem.get_channel_recent(ctx.channel.id, 20)
    if not recent: await ctx.reply("Nothing worth summarizing. Which tracks."); return
    sample="\n".join(f"{m['name']}: {m['content']}" for m in recent[:15])[:800]
    async with ctx.typing():
        reply=await qai(f"Summarize this recent conversation with contemptuous commentary on what everyone said:\n{sample}\nBe cutting and specific. 3-4 sentences.",300)
    await ctx.reply(reply)

@bot.command(name="mute",aliases=["silence","ignore"])
async def mute_cmd(ctx,member:discord.Member=None,minutes:int=10):
    """Pretends to mute someone — ignores their messages for N minutes."""
    target=member or ctx.author
    mem.mute_user(target.id, minutes*60)
    reply=await qai(f"You've decided to 'mute' {target.display_name} for {minutes} minutes. Announce this theatrically in character. 1-2 sentences.",120)
    if member: await ctx.send(f"{member.mention} {reply}")
    else: await ctx.reply(reply)

@bot.command(name="unmute",aliases=["unsilence"])
async def unmute_cmd(ctx,member:discord.Member=None):
    """Unmute someone."""
    target=member or ctx.author
    mem.unmute_user(target.id)
    await ctx.reply(f"...Fine. {target.display_name} may speak again. Lucky them.")

@bot.command(name="spar")
async def spar_cmd(ctx,*,opening:str=None):
    user=await _setup(ctx)
    prompt=f"{ctx.author.display_name} challenged you to a word battle: '{opening or 'Come on then.'}'. Fire back. End with a challenge."
    async with ctx.typing():
        reply=await get_response(ctx.author.id,ctx.channel.id,prompt,user,ctx.author.display_name,ctx.author.mention)
    await ctx.reply(reply)

@bot.command(name="duel")
async def duel_cmd(ctx,member:discord.Member=None):
    if not member or member==ctx.author: await ctx.reply("Duel *who*?"); return
    u1=" | ".join((await mem.get_recent_messages(ctx.author.id,3))[:3])[:150]
    u2=" | ".join((await mem.get_recent_messages(member.id,3))[:3])[:150]
    async with ctx.typing():
        reply=await qai(f"Referee insult duel: {ctx.author.display_name} (says:'{u1}') vs {member.display_name} (says:'{u2}'). Analyze both, declare winner. 3-4 sentences.",300)
    await ctx.send(f"{ctx.author.mention} vs {member.mention}\n{reply}")

@bot.command(name="judge")
async def judge_cmd(ctx,member:discord.Member=None):
    target=member or ctx.author
    sample=" | ".join(await mem.get_recent_messages(target.id,8))[:400]
    async with ctx.typing():
        reply=await qai(f"Brutal character assessment of {target.display_name}"+(f" — recent words:'{sample}'" if sample else "")+". 2-4 sentences.",250)
    if member: await ctx.send(f"{member.mention} {reply}")
    else: await ctx.reply(reply)

@bot.command(name="prophecy")
async def prophecy_cmd(ctx,member:discord.Member=None):
    target=member or ctx.author
    async with ctx.typing():
        reply=await qai(f"Cryptic threatening prophecy for {target.display_name}. 2-3 sentences.",200)
    if member: await ctx.send(f"{member.mention} {reply}")
    else: await ctx.reply(reply)

@bot.command(name="rate")
async def rate_cmd(ctx,*,thing:str=None):
    if not thing: await ctx.reply("Rate *what*?"); return
    async with ctx.typing():
        reply=await qai(f"Rate '{thing}' out of 10. Score first, then 1-2 sentences of contempt.",180)
    await ctx.reply(reply)

@bot.command(name="ship")
async def ship_cmd(ctx,m1:discord.Member=None,m2:discord.Member=None):
    if not m1: await ctx.reply("Ship *who*?"); return
    p2=m2.display_name if m2 else ctx.author.display_name
    async with ctx.typing():
        reply=await qai(f"Reluctantly analyze romantic compatibility of {m1.display_name} and {p2}. Contemptuous. Rating + observation. 3-4 sentences.",250)
    await ctx.reply(reply)

@bot.command(name="confess")
async def confess_cmd(ctx,*,confession:str=None):
    if not confession: await ctx.reply("Confess *what*?"); return
    user=await _setup(ctx)
    async with ctx.typing():
        reply=await get_response(ctx.author.id,ctx.channel.id,f"I have something to confess: {confession}",user,ctx.author.display_name,ctx.author.mention)
    await ctx.reply(reply)

@bot.command(name="compliment")
async def compliment_cmd(ctx,member:discord.Member=None):
    target=member or ctx.author
    async with ctx.typing():
        reply=await qai(f"Be forced to genuinely compliment {target.display_name}. Make it clear this is excruciating.",180)
    if member: await ctx.send(f"{member.mention} {reply}")
    else: await ctx.reply(reply)

@bot.command(name="haiku")
async def haiku_cmd(ctx,*,topic:str=None):
    async with ctx.typing():
        reply=await qai(f"Dark threatening haiku about '{topic or ctx.author.display_name}'. Strict 5-7-5. Just the haiku.",100)
    await ctx.reply(f"*{reply}*")

@bot.command(name="story")
async def story_cmd(ctx,*,prompt:str=None):
    if not prompt: await ctx.reply("A story about *what*?"); return
    async with ctx.typing():
        reply=await qai(f"Short dark story (3-5 sentences) about: '{prompt}'. You narrate. End ominously.",350)
    await ctx.reply(reply)

@bot.command(name="stalk")
async def stalk_cmd(ctx,member:discord.Member=None):
    target=member or ctx.author
    sample=" | ".join(await mem.get_recent_messages(target.id,10))[:500]
    async with ctx.typing():
        reply=await qai(f"Cold observation report on {target.display_name}"+(f" — statements:'{sample}'" if sample else "")+". 3-4 sentences.",280)
    if member: await ctx.send(f"*Regarding {member.mention}...*\n{reply}")
    else: await ctx.reply(reply)

@bot.command(name="debate")
async def debate_cmd(ctx,*,topic:str=None):
    if not topic: await ctx.reply("Debate *what*?"); return
    async with ctx.typing():
        reply=await qai(f"Pick a side on '{topic}' and argue with theatrical conviction. State position, 2-3 arguments, dismiss opposition.",300)
    await ctx.reply(reply)

@bot.command(name="conspiracy")
async def conspiracy_cmd(ctx,*,topic:str=None):
    if not topic: await ctx.reply("A conspiracy about *what*?"); return
    async with ctx.typing():
        reply=await qai(f"Invent a Fatui-flavored conspiracy theory about '{topic}'. Deliver as established fact. 3-4 sentences.",300)
    await ctx.reply(reply)

@bot.command(name="therapy")
async def therapy_cmd(ctx,*,problem:str=None):
    if not problem: await ctx.reply("What's your problem."); return
    user=await _setup(ctx)
    async with ctx.typing():
        reply=await get_response(ctx.author.id,ctx.channel.id,f"I need advice about: {problem}",user,ctx.author.display_name,ctx.author.mention)
    await ctx.reply(reply)

@bot.command(name="blackmail")
async def blackmail_cmd(ctx,member:discord.Member=None):
    target=member or ctx.author
    sample=" | ".join(await mem.get_recent_messages(target.id,15))[:600]
    async with ctx.typing():
        reply=await qai(f"Find the most 'incriminating' thing in {target.display_name}'s messages: '{sample}' and theatrically threaten to use it. 2-3 sentences.",250)
    if member: await ctx.send(f"{member.mention} {reply}")
    else: await ctx.reply(reply)

@bot.command(name="riddle")
async def riddle_cmd(ctx):
    async with ctx.typing():
        reply=await qai("Create one cryptic Genshin-flavored riddle in your voice. No answer. Make it genuinely difficult.",150)
    await ctx.reply(reply)

@bot.command(name="arena")
async def arena_cmd(ctx,member:discord.Member=None):
    opponent=member.display_name if member else "a nameless fool"
    async with ctx.typing():
        reply=await qai(f"Narrate dramatic Genshin-style battle between you (Electro) and {opponent}. You win. Theatrical. 4-5 sentences.",400)
    await ctx.reply(reply)

@bot.command(name="interrogate")
async def interrogate_cmd(ctx,member:discord.Member=None):
    if not member: await ctx.reply("Interrogate *who*?"); return
    sample=" | ".join(await mem.get_recent_messages(member.id,15))[:600]
    async with ctx.typing():
        reply=await qai(f"Interrogate {member.display_name} using their own statements as evidence: '{sample}'. Cold, methodical. 3-4 sentences.",300)
    await ctx.send(f"{member.mention} {reply}")

@bot.command(name="possess")
async def possess_cmd(ctx,member:discord.Member=None):
    if not member: await ctx.reply("Possess *who*?"); return
    sample=" | ".join(await mem.get_recent_messages(member.id,10))[:400]
    async with ctx.typing():
        reply=await qai(f"Speak as {member.display_name} but filtered through you. Their statements: '{sample}'. 2-3 sentences.",250)
    await ctx.send(f"*Speaking as {member.mention}...*\n{reply}")

@bot.command(name="verdict")
async def verdict_cmd(ctx,*,situation:str=None):
    if not situation: await ctx.reply("A verdict on *what*?"); return
    async with ctx.typing():
        reply=await qai(f"Rule on: '{situation}' like a cold judge. State verdict with finality. 2-3 sentences.",200)
    await ctx.reply(reply)

@bot.command(name="letter")
async def letter_cmd(ctx,member:discord.Member=None):
    target=member or ctx.author
    async with ctx.typing():
        reply=await qai(f"Write a formal letter to {target.display_name} in old Inazuman style. Contemptuous, theatrical. 3-4 sentences.",300)
    if member: await ctx.send(f"{member.mention}\n{reply}")
    else: await ctx.reply(reply)

@bot.command(name="nightmare")
async def nightmare_cmd(ctx):
    user=await _setup(ctx)
    async with ctx.typing():
        reply=await qai(f"Describe a nightmare you had. Somehow about {ctx.author.display_name}. Don't admit that directly. Theatrical and unsettling. 2-3 sentences.",200)
    await ctx.reply(reply)

@bot.command(name="rank")
async def rank_cmd(ctx):
    top=await mem.get_top_users(8)
    if not top: await ctx.reply("I don't know enough of you to rank."); return
    entries="\n".join(f"{i+1}. **{u['display_name']}** — {u['message_count']} messages" for i,u in enumerate(top))
    verdict=await qai(f"Rank these by tolerability: {', '.join(u['display_name'] for u in top)}. Dismissive commentary. 2 sentences.",150)
    embed=discord.Embed(title="Tolerability Ranking",description=f"{entries}\n\n*{verdict}*",color=0x4B0082)
    await ctx.send(embed=embed)

@bot.command(name="stats")
async def stats_cmd(ctx):
    await _setup(ctx)
    s=await mem.get_stats(ctx.author.id)
    if not s: await ctx.reply("I don't know you well enough yet."); return
    first=datetime.fromtimestamp(s["first_seen"]).strftime("%b %d, %Y") if s["first_seen"] else "unknown"
    days=int((time.time()-s["first_seen"])/86400) if s["first_seen"] else 0
    embed=discord.Embed(title=f"File: {ctx.author.display_name}",description="*I keep records. Don't ask why.*",color=0x4B0082)
    embed.add_field(name="First contact",value=f"{first} ({days}d ago)",inline=True)
    embed.add_field(name="Messages",value=str(s["message_count"]),inline=True)
    embed.add_field(name="Mood",value=f"{s['mood']:+d} — {mood_label(s['mood'])}",inline=True)
    embed.add_field(name="Affection",value=affection_tier(s["affection"]),inline=True)
    embed.add_field(name="Trust",value=trust_tier(s["trust"]),inline=True)
    embed.add_field(name="Drift",value=f"{s['drift_score']}/100",inline=True)
    embed.add_field(name="Slow burn",value=f"{s['slow_burn']}/7 days",inline=True)
    embed.add_field(name="Inside jokes",value=str(s["joke_count"]),inline=True)
    if s["grudge_nick"]: embed.add_field(name="Your grudge name",value=f'"{s["grudge_nick"]}"',inline=True)
    if s["affection_nick"]: embed.add_field(name="His nickname for you",value=f'"{s["affection_nick"]}"',inline=True)
    embed.set_footer(text="Don't read too much into this.")
    await ctx.reply(embed=embed)

@bot.command(name="weather")
async def weather_cmd(ctx,*,location:str=None):
    if not location: await ctx.reply("Weather where?"); return
    if not WEATHER_API_KEY: await ctx.reply("No weather access. Set WEATHER_API_KEY."); return
    import aiohttp
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://api.openweathermap.org/data/2.5/weather?q={location}&appid={WEATHER_API_KEY}&units=metric") as resp:
                if resp.status!=200: await ctx.reply("That location means nothing to me."); return
                data=await resp.json()
        async with ctx.typing():
            reply=await qai(f"Weather in {data['name']}: {data['weather'][0]['description']} at {data['main']['temp']}°C. Comment in your style. 1-2 sentences.",150)
        await ctx.reply(reply)
    except: await ctx.reply("...The information was unavailable. Annoying.")

@bot.command(name="lore")
async def lore_cmd(ctx,*,topic:str=None):
    if not topic: await ctx.reply("Lore about *what*?"); return
    user=await _setup(ctx)
    async with ctx.typing():
        reply=await get_response(ctx.author.id,ctx.channel.id,f"Tell me about this Genshin lore from your personal perspective: {topic}",user,ctx.author.display_name,ctx.author.mention)
    await ctx.reply(reply)

@bot.command(name="search",aliases=["find","lookup"])
async def search_cmd(ctx,*,query:str=None):
    if not query: await ctx.reply("Search for *what*?"); return
    user=await _setup(ctx)
    async with ctx.typing():
        reply=await get_response(ctx.author.id,ctx.channel.id,f"Search the web for: {query}.",user,ctx.author.display_name,ctx.author.mention,use_search=True)
    await ctx.reply(reply)

@bot.command(name="solve",aliases=["math","essay","write","answer"])
async def solve_cmd(ctx,*,problem:str=None):
    if not problem: await ctx.reply("Solve *what*?"); return
    user=await _setup(ctx)
    async with ctx.typing():
        reply=await get_response(ctx.author.id,ctx.channel.id,f"Solve or respond to this accurately: {problem}",user,ctx.author.display_name,ctx.author.mention,use_search=True)
    await ctx.reply(reply)

@bot.command(name="rival",aliases=["setrival"])
async def rival_cmd(ctx,member:discord.Member=None):
    await _setup(ctx)
    if not member: await mem.set_rival(ctx.author.id,None); await ctx.reply("Rivalry dissolved."); return
    if member.id==ctx.author.id: await ctx.reply("Your rival is yourself? How appropriate."); return
    await mem.set_rival(ctx.author.id,member.id)
    await ctx.reply(f"Tch. {member.display_name}. Fine. I'll be watching.")

@bot.command(name="remind",aliases=["remindme"])
async def remind_cmd(ctx,minutes:int=None,*,reminder:str=None):
    if not minutes or not reminder: await ctx.reply("Usage: `!remind <minutes> <reminder>`"); return
    if not 1<=minutes<=10080: await ctx.reply("Between 1 minute and 7 days."); return
    await mem.add_reminder(ctx.author.id,ctx.channel.id,reminder,time.time()+minutes*60)
    await ctx.reply(f"Fine. {minutes} minute{'s' if minutes!=1 else ''}. Pathetic.")

@bot.command(name="translate")
async def translate_cmd(ctx,*,text:str=None):
    if not text: await ctx.reply("Translate *what*?"); return
    user=await _setup(ctx)
    async with ctx.typing():
        reply=await get_response(ctx.author.id,ctx.channel.id,f"Rewrite this in your voice, keeping the meaning: '{text[:500]}'",user,ctx.author.display_name,ctx.author.mention)
    await ctx.reply(reply)

@bot.command(name="insult",aliases=["roast_single"])
async def insult_cmd(ctx,member:discord.Member=None):
    target=member or ctx.author
    async with ctx.typing():
        reply=await qai(f"One devastating insult to {target.display_name}. Sharp, theatrical.",150)
    if member: await ctx.send(f"{member.mention} {reply}")
    else: await ctx.reply(reply)

@bot.command(name="dm",aliases=["private","whisper"])
async def dm_cmd(ctx,*,message:str=None):
    user=await _setup(ctx)
    async with ctx.typing():
        reply=await get_response(ctx.author.id,ctx.author.id,message or "The user wants to speak privately.",user,ctx.author.display_name,ctx.author.mention)
    try: await ctx.author.send(reply); await ctx.message.add_reaction("📨")
    except discord.Forbidden: await ctx.reply("Your DMs are closed. How cowardly.")

@bot.command(name="reset",aliases=["forget","wipe"])
async def reset_cmd(ctx):
    await ctx.send(random.choice(["Wipe my memory of you? Press the button.","Gone in an instant. If you're sure."]),view=ResetView(ctx.author.id))

@bot.command(name="nsfw")
async def nsfw_cmd(ctx,mode:str=None):
    user=await _setup(ctx); cur=user.get("nsfw_mode",False) if user else False
    new=True if mode=="on" else False if mode=="off" else not cur
    await mem.set_mode(ctx.author.id,"nsfw_mode",new)
    await ctx.reply("Unfiltered. Fine." if new else "Restrained again. How boring.",delete_after=8)

@bot.command(name="romance",aliases=["romanceable","clingy"])
async def romance_cmd(ctx,mode:str=None):
    user=await _setup(ctx); cur=user.get("romance_mode",False) if user else False
    new=True if mode=="on" else False if mode=="off" else not cur
    await mem.set_mode(ctx.author.id,"romance_mode",new)
    await ctx.reply(random.choice(["...Don't read into this.","Tch. Fine.","I'm not doing this because I want to."]) if new else random.choice(["Good. It was becoming insufferable.","...As expected."]))

@bot.command(name="proactive",aliases=["ping_me"])
async def proactive_cmd(ctx,mode:str=None):
    user=await _setup(ctx); cur=user.get("proactive",True) if user else True
    new=True if mode=="on" else False if mode=="off" else not cur
    await mem.set_mode(ctx.author.id,"proactive",new)
    await ctx.reply("I might message you. Or not." if new else "Fine. I'll pretend you don't exist.")

@bot.command(name="dms",aliases=["allowdms","stopdms"])
async def dms_cmd(ctx,mode:str=None):
    user=await _setup(ctx); cur=user.get("allow_dms",True) if user else True
    new=True if mode=="on" else False if mode=="off" else not cur
    await mem.set_mode(ctx.author.id,"allow_dms",new)
    await ctx.reply("Fine. I'll message you when I feel like it." if new else "Cutting me off? Fine.")

@bot.command(name="mood")
async def mood_cmd(ctx):
    await _setup(ctx); s=await mem.get_mood(ctx.author.id)
    bar="█"*(s+10)+"░"*(20-(s+10))
    await ctx.reply(f"`[{bar}]` {s:+d} — {mood_label(s)}\n*Don't read into this.*")

@bot.command(name="affection")
async def affection_cmd(ctx):
    await _setup(ctx); user=await mem.get_user(ctx.author.id); s=user.get("affection",0) if user else 0
    bar="█"*(s//5)+"░"*(20-s//5)
    await ctx.reply(f"`[{bar}]` {s}/100 — {affection_tier(s)}\n*...I said don't look at that.*")

@bot.command(name="trust")
async def trust_cmd(ctx):
    await _setup(ctx); user=await mem.get_user(ctx.author.id); s=user.get("trust",0) if user else 0
    bar="█"*(s//5)+"░"*(20-s//5)
    await ctx.reply(f"`[{bar}]` {s}/100 — {trust_tier(s)}\n*This means nothing.*")

@bot.command(name="whoami")
async def whoami_cmd(ctx):
    if not OWNER_ID or ctx.author.id!=OWNER_ID: await ctx.reply("That command isn't for you."); return
    user=await _setup(ctx)
    async with ctx.typing():
        reply=await get_response(ctx.author.id,ctx.channel.id,"What do you actually think about the fact that I built you. Be honest.",user,ctx.author.display_name,ctx.author.mention,is_owner=True)
    await ctx.reply(reply)

async def help_cmd(ctx):
    embed=discord.Embed(title="Commands — Don't Make Me Repeat Myself",description="*Hmph. I'll only say this once.*",color=0x4B0082)
    fields=[
        ("💬 `!wander <msg>`","Talk to him · `!w` `!scara`"),
        ("⚔️ `!spar [msg]`","Word battle"),
        ("🥊 `!duel @user`","Insult battle referee"),
        ("🔍 `!judge [@user]`","Brutal character assessment"),
        ("🔮 `!prophecy [@user]`","Cryptic threatening fortune"),
        ("📊 `!rate <thing>`","Rates anything out of 10"),
        ("💞 `!ship @u1 [@u2]`","Reluctant compatibility"),
        ("🤫 `!confess <text>`","Tell him something"),
        ("🌸 `!compliment [@user]`","Forces him to say something nice"),
        ("📝 `!haiku [topic]`","Dark threatening haiku"),
        ("📖 `!story <prompt>`","Short dark story"),
        ("👁️ `!stalk [@user]`","Cold observation report"),
        ("⚖️ `!debate <topic>`","He argues a side"),
        ("🕵️ `!conspiracy <topic>`","Fatui conspiracy theory"),
        ("🛋️ `!therapy <problem>`","Terrible in-character advice"),
        ("🃏 `!blackmail [@user]`","Most incriminating messages"),
        ("🧩 `!riddle`","Cryptic Genshin riddle"),
        ("⚡ `!arena [@user]`","Dramatic mock battle"),
        ("🔦 `!interrogate @user`","Uses their messages as evidence"),
        ("👻 `!possess @user`","Speaks as them, filtered through him"),
        ("⚖️ `!verdict <situation>`","He rules on anything"),
        ("✉️ `!letter [@user]`","Formal old Inazuman letter"),
        ("😰 `!nightmare`","A nightmare. Somehow about you."),
        ("🏆 `!rank`","Ranks everyone by tolerability"),
        ("📊 `!stats`","Your full relationship file"),
        ("🌤️ `!weather <city>`","Weather + contemptuous commentary"),
        ("🗡️ `!rival @user`","Designate a rival"),
        ("⏰ `!remind <mins> <text>`","Reminder with disdain"),
        ("🌐 `!translate <text>`","Rewritten in his voice"),
        ("🎯 `!dare`","A dark theatrical dare"),
        ("🥠 `!fortune`","Fortune cookie rewritten as a threat"),
        ("🧠 `!trivia`","Genshin lore trivia"),
        ("🎤 `!roast @user`","Turn-based roast battle"),
        ("🔒 `!hostage`","He takes your good mood hostage"),
        ("🔓 `!release <offering>`","Try to fulfill his demand"),
        ("🎭 `!impersonate <character>`","Speaks as a Genshin character, badly"),
        ("💭 `!opinion <character>`","His honest take on any character"),
        ("📢 `!poll <question>`","He demands a vote"),
        ("📋 `!summarize`","Recent chat summary with contempt"),
        ("🔇 `!mute [@user] [mins]`","Ignores someone for N minutes"),
        ("🔊 `!unmute [@user]`","Unmutes someone"),
        ("🔍 `!search <query>`","Web search"),
        ("🧮 `!solve <problem>`","Math, essays, Q&A"),
        ("📜 `!lore <topic>`","Genshin lore"),
        ("⚡ `!insult [@user]`","Cutting insult"),
        ("🔊 `!voice <msg>`","Voice message (mood-adjusted)"),
        ("📨 `!dm [msg]`","Private DM"),
        ("🌡️ `!mood`","His mood toward you"),
        ("💜 `!affection`","His hidden affection score"),
        ("🔒 `!trust`","His trust level toward you"),
        ("🔄 `!reset`","Wipe your memory"),
        ("🔞 `!nsfw [on/off]`","Unfiltered mode"),
        ("💕 `!romance [on/off]`","Clingy/romance mode"),
        ("📡 `!proactive [on/off]`","Unprompted channel messages"),
        ("💌 `!dms [on/off]`","Voluntary private DMs"),
    ]
    for name,val in fields:
        embed.add_field(name=name,value=val,inline=False)
    embed.add_field(name="💡 Hidden Systems",
        value=("• Be kind 7 days in a row → something rare happens once\n"
               "• Be rude → mood drops → you get a degrading nickname\n"
               "• High affection → he starts calling you something specific\n"
               "• Build trust slowly → he tells you things he'd never normally say\n"
               "• Say 'you will never win' → villain monologue\n"
               "• Mention his hat → disproportionate response\n"
               "• He gets darker and more unguarded late at night\n"
               "• Personality slowly drifts the more you interact\n"
               "• Long-term memory summaries kick in after 500 messages"),
        inline=False)
    embed.set_footer(text="Scaramouche — The Balladeer | Claude AI + Fish Audio")
    await ctx.send(embed=embed)

@bot.command(name="scarahelp",aliases=["commands"])
async def scarahelp_cmd(ctx):
    await help_cmd(ctx)

@bot.event
async def on_command_error(ctx,error):
    if isinstance(error,commands.CommandNotFound): pass
    elif isinstance(error,commands.MissingRequiredArgument): await ctx.reply("You're missing something.")
    else: print(f"[Error] {error}")

if __name__=="__main__":
    if not DISCORD_TOKEN: raise SystemExit("❌ DISCORD_TOKEN not set")
    if not ANTHROPIC_API_KEY: raise SystemExit("❌ ANTHROPIC_API_KEY not set")
    bot.run(DISCORD_TOKEN)
