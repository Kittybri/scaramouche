import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import discord
from help_delivery import paginate_help, embed_batches, plaintext_chunks, send_help


def menu(count, size=30):
    page = discord.Embed(title="Commands", description="Help")
    for i in range(count):
        page.add_field(name=f"!command{i}", value=f"entry-{i}:" + "x" * size)
    return page


def test_live_29_field_regression_preserves_all_entries():
    original = menu(29)
    pages = paginate_help([original])
    assert [len(p.fields) for p in pages] == [25, 4]
    assert [(f.name, f.value) for p in pages for f in p.fields] == [(f.name, f.value) for f in original.fields]
    assert len(original.fields) == 29


def test_combined_character_and_message_embed_limits():
    pages = paginate_help([menu(55, 1000)] + [menu(1) for _ in range(15)])
    batches = list(embed_batches(pages))
    assert all(len(batch) <= 10 and sum(map(len, batch)) <= 6000 for batch in batches)
    assert all(len(p.fields) <= 25 and len(p) <= 6000 for p in pages)
    assert sum(len(p.fields) for p in pages) == 70


def test_valid_menu_sends_once_without_llm():
    ctx = SimpleNamespace(send=AsyncMock())
    asyncio.run(send_help(ctx, [menu(29)]))
    assert ctx.send.await_count == 1
    assert [len(p.fields) for p in ctx.send.call_args.kwargs["embeds"]] == [25, 4]
    assert not ctx.send.call_args.kwargs["allowed_mentions"].everyone


def test_embed_permission_failure_has_complete_text_fallback():
    original = menu(29, 100)
    error = discord.Forbidden(SimpleNamespace(status=403, reason="Forbidden"), "Missing Permissions")
    ctx = SimpleNamespace(send=AsyncMock(side_effect=[error, None, None, None]))
    asyncio.run(send_help(ctx, [original]))
    texts = [call.args[0] for call in ctx.send.call_args_list[1:]]
    assert texts and all(len(text) <= 1900 for text in texts)
    assert all(f"!command{i} —" in "".join(texts) for i in range(29))


def test_invalid_field_falls_back_without_truncating_tail():
    original = menu(1, 5000)
    chunks = list(plaintext_chunks([original]))
    assert all(len(c) <= 1900 for c in chunks)
    assert original.fields[0].value in "".join(chunks)
    ctx = SimpleNamespace(send=AsyncMock())
    asyncio.run(send_help(ctx, [original]))
    assert all("embeds" not in call.kwargs for call in ctx.send.call_args_list)


def test_failure_after_first_batch_does_not_repeat_delivered_page():
    first = menu(1, 1000)
    first.title = "ALREADY_SENT"
    second = menu(5, 1000)
    second.title = "STILL_NEEDED"
    error = discord.HTTPException(SimpleNamespace(status=400, reason="Bad Request"), "Invalid Form Body")
    async def fail_second_embed(*args, **kwargs):
        if kwargs.get("embeds") and kwargs["embeds"][0].title == "STILL_NEEDED":
            raise error
    ctx = SimpleNamespace(send=AsyncMock(side_effect=fail_second_embed))
    asyncio.run(send_help(ctx, [first, second]))
    fallback = "".join(c.args[0] for c in ctx.send.call_args_list if c.args)
    assert "STILL_NEEDED" in fallback and "ALREADY_SENT" not in fallback
