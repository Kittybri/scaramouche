import os
import tempfile
import unittest
import ast
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

import aiosqlite

from character_bits import (
    autocorrect_line, bounded_glitch, eligible_for_silent_judge, is_serious_or_utility,
    reverse_turing_hint, safe_message_edit, selective_hearing_hint,
    significant_weather, time_drift_prompt, time_period,
)
from memory import Memory


BOT_SOURCE = Path(__file__).resolve().parents[1].joinpath("bot.py").read_text(encoding="utf-8")


def function_source(name: str) -> str:
    tree = ast.parse(BOT_SOURCE)
    node = next(item for item in ast.walk(tree) if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name == name)
    return ast.get_source_segment(BOT_SOURCE, node) or ""


class CharacterBitsTests(unittest.TestCase):
    def test_time_period_boundaries(self):
        self.assertEqual(time_period(0), "late_night")
        self.assertEqual(time_period(6), "morning")
        self.assertEqual(time_period(12), "afternoon")
        self.assertEqual(time_period(18), "evening")
        self.assertIn("subtly", time_drift_prompt(8))

    def test_serious_and_questions_are_not_silent_judged(self):
        self.assertTrue(is_serious_or_utility("I need emergency help"))
        self.assertFalse(eligible_for_silent_judge("Can you explain this?"))
        self.assertFalse(eligible_for_silent_judge("!weather Seattle"))

    def test_reverse_turing_is_only_a_hint(self):
        self.assertIn("jokingly", reverse_turing_hint("ok", repeated_count=2))
        self.assertEqual(reverse_turing_hint("This is a varied normal message", 0), "")
        self.assertIsNone(autocorrect_line("I need emergency medical help"))
        self.assertEqual(selective_hearing_hint("Can you help with an overdose?"), "")

    def test_glitch_is_bounded_and_skips_urls_or_code(self):
        original = "You really thought that complicated little plan would work."
        glitched = bounded_glitch(original)
        self.assertLessEqual(len(glitched), 2000)
        self.assertIn("fictional", glitched)
        self.assertEqual(bounded_glitch("See https://example.com"), "See https://example.com")
        self.assertNotIn("API_KEY", glitched)

    def test_safe_edit_and_weather(self):
        self.assertIsNone(safe_message_edit("https://example.com"))
        self.assertIsNotNone(safe_message_edit("I noticed you were absent."))
        self.assertEqual(significant_weather({"forecast": "Sunny", "temperature": 70, "wind_speed": "5 mph"}), None)
        self.assertEqual(significant_weather({"forecast": "Severe thunderstorms", "temperature": 70})[1], True)

    def test_fake_wipe_contains_no_destructive_discord_calls(self):
        source = function_source("fakewipe_cmd")
        for forbidden in ("delete_channel", ".delete(", ".kick(", ".ban(", "role.delete", "emoji.delete"):
            self.assertNotIn(forbidden, source)

    def test_typing_edit_and_timeout_guards_are_explicit(self):
        typing_source = function_source("on_typing")
        self.assertIn('getattr(user, "bot", False)', typing_source)
        self.assertIn("user.id == bot.user.id", typing_source)
        edit_source = function_source("_delayed_character_edit")
        self.assertIn("sent_message, \"author\"", edit_source)
        timeout_source = function_source("scaratimeout_cmd")
        self.assertIn("timedelta(seconds=60)", timeout_source)
        self.assertIn("member.top_role >= me.top_role", timeout_source)

    def test_scarahelp_uses_one_command_path_and_has_text_fallback(self):
        on_message_source = function_source("on_message")
        self.assertNotIn("await help_cmd(ctx)", on_message_source)
        help_source = function_source("help_cmd")
        self.assertIn("ctx.send(embeds=pages)", help_source)
        self.assertIn("_send_help_plaintext(ctx, pages)", help_source)
        self.assertIn('@bot.command(name="scarahelp", aliases=["commands"])', BOT_SOURCE)


class PersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.memory = Memory(
            "scaramouche",
            db_path=os.path.join(self.tmp.name, "bot.db"),
            shared_db_path=os.path.join(self.tmp.name, "shared.db"),
        )
        await self.memory.init()

    async def asyncTearDown(self):
        self.tmp.cleanup()

    async def test_anniversary_claim_is_atomic_once_per_year(self):
        tz = ZoneInfo("America/Los_Angeles")
        first = datetime(2024, 9, 25, 12, tzinfo=tz).timestamp()
        now = datetime(2026, 9, 26, 12, tzinfo=tz).timestamp()
        await self.memory.upsert_user(1, "user", "User")
        async with aiosqlite.connect(self.memory.db_path) as db:
            await db.execute("UPDATE users SET first_seen=?,message_count=20 WHERE user_id=1", (first,))
            await db.commit()
        self.assertEqual(await self.memory.claim_anniversary(1, now=now), 2)
        self.assertEqual(await self.memory.claim_anniversary(1, now=now + 60), 0)
        user = await self.memory.get_user(1)
        self.assertEqual(user["first_seen"], first)

    async def test_active_quiz_expires_and_material_has_provenance(self):
        await self.memory.upsert_user(2, "user", "User")
        await self.memory.add_message(2, 22, "assistant", "This is a sufficiently long real historical line for a memory quiz to use safely.")
        async with aiosqlite.connect(self.memory.db_path) as db:
            await db.execute("UPDATE messages SET ts=ts-7200 WHERE user_id=2")
            await db.commit()
        material = await self.memory.get_quizable_assistant_message(2)
        self.assertIsInstance(material["id"], int)
        await self.memory.set_active_trivia(22, 2, "q", "a", f"memory_message:{material['id']}")
        async with aiosqlite.connect(self.memory.db_path) as db:
            await db.execute("UPDATE active_trivia SET asked_ts=asked_ts-1000 WHERE channel_id=22")
            await db.commit()
        self.assertIsNone(await self.memory.get_active_trivia(22, timeout_seconds=900))

    async def test_temporary_setting_persists_until_due(self):
        await self.memory.save_temporary_channel_setting(99, "slowmode", 7, 1)
        due = await self.memory.get_due_temporary_channel_settings()
        self.assertEqual(due[0]["previous_value"], 7)
        await self.memory.clear_temporary_channel_setting(99, "slowmode")
        self.assertEqual(await self.memory.get_due_temporary_channel_settings(), [])

    async def test_weather_candidate_requires_configured_location_and_daily_cooldown(self):
        await self.memory.upsert_user(3, "user", "User")
        self.assertEqual(await self.memory.get_weather_candidates(), [])
        await self.memory.set_weather_location(3, "San Jose, CA")
        self.assertEqual((await self.memory.get_weather_candidates())[0]["weather_location"], "San Jose, CA")
        self.assertEqual(await self.memory.phrase_cooldown_remaining("user:3", "weather_proactive", 86400), 0)
        self.assertTrue(await self.memory.consume_phrase("user:3", "weather_proactive", 86400))
        self.assertGreater(await self.memory.phrase_cooldown_remaining("user:3", "weather_proactive", 86400), 0)
        self.assertFalse(await self.memory.consume_phrase("user:3", "weather_proactive", 86400))


if __name__ == "__main__":
    unittest.main()
