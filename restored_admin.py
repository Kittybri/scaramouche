"""Only legacy administration that needs no sensitive-data or mutation bypass."""
import discord


def install(bot, owner_id):
    @bot.command(name='servers')
    async def servers(ctx):
        if not owner_id or ctx.author.id!=owner_id:
            await ctx.send('That command is owner-only.'); return
        lines=[f'{discord.utils.escape_markdown(g.name)} — {g.id} — {g.member_count or 0} members'
               for g in sorted(bot.guilds,key=lambda g:g.id)]
        try:
            page='Servers I am in:\n'
            for line in lines or ['None.']:
                if len(page)+len(line)>1850:
                    await ctx.author.send(page,allowed_mentions=discord.AllowedMentions.none()); page=''
                page+=line[:250]+'\n'
            await ctx.author.send(page,allowed_mentions=discord.AllowedMentions.none())
        except discord.Forbidden:
            await ctx.send('Open your DMs for the private server inventory.')
    return servers
