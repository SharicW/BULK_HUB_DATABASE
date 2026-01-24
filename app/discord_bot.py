import os
import discord
from discord.ext import commands

from app.stats import add_discord_message

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
if not DISCORD_TOKEN:
    raise RuntimeError("DISCORD_TOKEN is not set")

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    print(f"✅ Discord bot started: {bot.user}")

@bot.event
async def on_message(message: discord.Message):
    if message.guild and not message.author.bot:
        add_discord_message(message.author.id, str(message.author))
    await bot.process_commands(message)

@bot.command()
async def scan(ctx: commands.Context):
    await ctx.send("🔥 Сканирую последние сообщения...")
    total = 0

    for channel in ctx.guild.text_channels:
        try:
            async for msg in channel.history(limit=50):
                if msg.author and not msg.author.bot:
                    add_discord_message(msg.author.id, str(msg.author))
                    total += 1
        except Exception:
            continue

    await ctx.send(f"Скан: {total} сообщений отправлено в запись")

if __name__ == "__main__":

    bot.run(DISCORD_TOKEN)
