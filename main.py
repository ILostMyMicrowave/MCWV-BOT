import os
import re
import asyncio
import sqlite3
import discord
import aiohttp
from PIL import Image, ImageDraw, ImageFont, ImageFilter
from io import BytesIO
import math
import random
from datetime import datetime, timezone

from discord import app_commands
from discord.ext import commands, tasks

from flask import Flask
from threading import Thread

# ---------------- GLOBALS ----------------
session = None
status_cooldown = {}

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is alive"


def run_web():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)


# start web server (keep-alive)
Thread(target=run_web).start()

def get_current_war(war_data, clan_data):
    war_config = war_data.get("data", {}).get("configData", {})
    active_battle_id = (
        war_config.get("Title")
        or war_data.get("data", {}).get("configName")
    )

    root = clan_data.get("data", {})
    battles = (
        root.get("Battles")
        or root.get("battles")
        or root.get("clanWar", {}).get("Battles")
        or root.get("Wars", {}).get("Battles")
        or {}
    )

    if not battles:
        return None, None

    if active_battle_id and active_battle_id in battles:
        battle_id = active_battle_id
    else:
        battle_id = list(battles.keys())[-1]

    return battle_id, battles.get(battle_id)

async def run_initial_presence_check(channel):
    try:
        users = db_get_all()
        if not users:
            return

        user_ids = [int(u[0]) for u in users]

        async with session.post(
            "https://presence.roblox.com/v1/presence/users",
            json={"userIds": user_ids}
        ) as pr:

            if pr.status != 200:
                return

            presences = (await pr.json()).get("userPresences", [])
            now_dt = datetime.now(timezone.utc)

            for p in presences:
                rid = str(p["userId"])
                status_cache[rid] = p["userPresenceType"]

                if p["userPresenceType"] == 0:
                    offline_since[rid] = now_dt

    except Exception as e:
        print("Initial sync error:", e)

from PIL import Image, ImageDraw, ImageFont
from io import BytesIO

def load_fonts():
    try:
        return {
            "title": ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 52),
            "big":   ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 34),
            "small": ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 24),
        }
    except:
        # fallback (still works, just worse)
        return {
            "title": ImageFont.load_default(),
            "big": ImageFont.load_default(),
            "small": ImageFont.load_default(),
        }

def generate_particles(count, width, height):
    return [
        (
            random.randint(60, width - 60),
            random.randint(60, height - 60),
            random.randint(2, 5)
        )
        for _ in range(count)
    ]


def glass_panel(base, xy, radius=40, fill=(30, 35, 55, 180)):
    layer = Image.new("RGBA", base.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)

    d.rounded_rectangle(xy, radius=radius, fill=fill)

    layer = layer.filter(ImageFilter.GaussianBlur(10))
    base.alpha_composite(layer)


def glow_bar(base, x, y, width, height, progress):
    glow = Image.new("RGBA", base.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(glow)

    filled = int(width * progress)

    d.rounded_rectangle(
        [x, y, x + filled, y + height],
        radius=25,
        fill=(80, 120, 255, 180)
    )

    glow = glow.filter(ImageFilter.GaussianBlur(20))
    base.alpha_composite(glow)

    d2 = ImageDraw.Draw(base)
    d2.rounded_rectangle(
        [x, y, x + filled, y + height],
        radius=25,
        fill=(90, 140, 255)
    )

def draw_shimmer_bar(img, x, y, width, height, progress, frame):
    base = ImageDraw.Draw(img)

    filled = int(width * progress)

    base.rounded_rectangle([x, y, x + width, y + height], radius=18, fill=(40, 40, 55))

    if filled <= 0:
        return

    base.rounded_rectangle([x, y, x + filled, y + height], radius=18, fill=(90, 140, 255))

    shimmer = Image.new("RGBA", img.size, (0, 0, 0, 0))
    sdraw = ImageDraw.Draw(shimmer)

    offset = (frame * 12) % (filled + 200)
    band = 120

    for i in range(-40, 40, 10):
        sdraw.polygon(
            [
                (x + offset + i - 200, y),
                (x + offset + i, y),
                (x + offset + i + band, y + height),
                (x + offset + i + band - 200, y + height),
            ],
            fill=(160, 200, 255, 60)
        )

    shimmer = shimmer.filter(ImageFilter.GaussianBlur(6))
    img.alpha_composite(shimmer)

async def fetch_roblox_avatar(user_id):
    try:
        url = (
            "https://thumbnails.roblox.com/v1/users/avatar-headshot"
            f"?userIds={user_id}&size=420x420&format=Png&isCircular=false"
        )

        async with session.get(url) as r:
            data = await r.json()

        image_url = data["data"][0]["imageUrl"]

        async with session.get(image_url) as r:
            avatar_bytes = await r.read()

        avatar = Image.open(BytesIO(avatar_bytes)).convert("RGBA")
        return avatar

    except Exception as e:
        print("Avatar fetch error:", e)
        return None

async def generate_profile_card(
    roblox_name,
    roblox_id,
    discord_tag,
    points,
    rank,
    animated=False
):
    WIDTH, HEIGHT = 1400, 600
    frames = []

    fonts = load_fonts()
    title_font = fonts["title"]
    big_font = fonts["big"]
    small_font = fonts["small"]

    particles = generate_particles(25, WIDTH, HEIGHT)

    avatar = await fetch_roblox_avatar(roblox_id)

    if avatar:
        avatar = avatar.resize((220, 220))
        mask = Image.new("L", (220, 220), 0)
        ImageDraw.Draw(mask).ellipse((0, 0, 220, 220), fill=255)
        avatar.putalpha(mask)

    for frame in range(6 if animated else 1):

        img = Image.new("RGBA", (WIDTH, HEIGHT))
        draw = ImageDraw.Draw(img)

        # background
        for y in range(HEIGHT):
            r = int(10 + (45 - 10) * (y / HEIGHT))
            g = int(15 + (30 - 15) * (y / HEIGHT))
            b = int(40 + (120 - 40) * (y / HEIGHT))
            draw.line([(0, y), (WIDTH, y)], fill=(r, g, b))

        # particles
        for i, (x, y, size) in enumerate(particles):
            offset = math.sin(frame * 0.6 + i) * 2 if animated else 0
            draw.ellipse([x+offset, y+offset, x+size+offset, y+size+offset], fill=(120,160,255,120))

        # panel
        draw.rounded_rectangle([40, 40, WIDTH-40, HEIGHT-40], radius=35, fill=(20,22,30))

        # avatar
        if avatar:
            img.paste(avatar, (80, 90), avatar)

        # name
        x, y = 350, 80
        draw.text((x, y), roblox_name, fill="white", font=title_font)

        for i in range(3):
            draw.text((x, y), roblox_name, fill=(90,140,255,40), font=title_font)

        # info
        draw.text((355, 175), f"Discord: {discord_tag}", fill=(200,200,200), font=small_font)
        draw.text((355, 215), f"Roblox ID: {roblox_id}", fill=(160,160,160), font=small_font)
        draw.text((355, 255), f"Rank: {rank}", fill=(180,180,180), font=small_font)

        # shimmer bar
        progress = min(points / 50_000_000, 1)

        draw_shimmer_bar(img, 350, 340, 800, 45, progress, frame)

        draw.text((350, 300), "WAR PROGRESS", fill=(180,180,180), font=small_font)
        draw.text((1170, 340), f"{int(progress*100)}%", fill="white", font=small_font)

        draw.text((355, 410), f"{points:,} TOTAL POINTS", fill="white", font=big_font)

        frames.append(img)

    buffer = BytesIO()

    if animated:
        frames[0].save(buffer, format="GIF", save_all=True,
                       append_images=frames[1:], duration=90, loop=0)
    else:
        frames[0].save(buffer, format="PNG")

    buffer.seek(0)
    return buffer

# ---------------- CONFIG ----------------
TOKEN = os.environ.get("DISCORD_TOKEN")

GUILD_ID                  = 1501608673250640055
CHANNEL_ID                = 1514663069639245904
ALLOWED_ROLE_ID           = 1501986357516701827  # staff role (run commands)
CLAN_MEMBER_ROLE_ID       = 1501986780667314246  # given on accept
CLAN_MEMBERS_CATEGORY_ID  = 1503109089931034785  # ticket moved here on accept
MEMBERS_CHANNEL_ID        = 1509276380674789617  # membership record posted here
LOG_CHANNEL_ID            = 1502001938705682622  # accept/action log
PS99_API                  = "https://ps99.biggamesapi.io/api/activeClanBattle"
CLAN_NAME                 = "MCWV"
CLAN_API                  = f"https://ps99.biggamesapi.io/api/clan/{CLAN_NAME}"
ROBLOX_USERS_API          = "https://users.roblox.com/v1/users"

if not TOKEN:
    raise ValueError("Missing DISCORD_TOKEN")

guild_obj = discord.Object(id=GUILD_ID)

# ---------------- BOT ----------------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)
cooldowns = commands.CooldownMapping.from_cooldown(
    1, 10, commands.BucketType.user
)
def check_cooldown(interaction: discord.Interaction):
    bucket = cooldowns.get_bucket(interaction)
    retry_after = bucket.update_rate_limit()
    return retry_after

session = None
bot_enabled = True
offline_ping_enabled = True
reminder_interval = 30        # minutes between offline reminders
reminder_channel_id = CHANNEL_ID  # channel where reminders are sent
ps99_war_active = False       # tracks last known PS99 war state
ps99_first_check = True       # suppresses announcement on first poll (mid-war startup)

# ---------------- DATABASE ----------------
import psycopg2
import os

DATABASE_URL = os.environ.get("DATABASE_URL")

conn = None
cur = None

def db_enabled():
    return conn is not None and cur is not None

if DATABASE_URL:
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        print("Database connected")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            roblox_id TEXT PRIMARY KEY,
            discord_id BIGINT,
            username TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """)

        conn.commit()

    except Exception as e:
        print("DB connection failed:", e)
        conn = None
        cur = None
else:
    print("DATABASE_URL not set - running without DB")


def db_add(rid, did, name):
    if not db_enabled():
        return

    cur.execute("""
        INSERT INTO users (roblox_id, discord_id, username)
        VALUES (%s, %s, %s)
        ON CONFLICT (roblox_id)
        DO UPDATE SET discord_id = EXCLUDED.discord_id,
                      username = EXCLUDED.username
    """, (rid, did, name))

    conn.commit()


def db_remove(did):
    if not db_enabled():
        return

    cur.execute("""
        DELETE FROM users WHERE discord_id = %s
    """, (did,))

    conn.commit()


def db_get_all():
    if not db_enabled():
        return []

    cur.execute("SELECT roblox_id, discord_id, username FROM users")
    return cur.fetchall()


def db_get_setting(key, default=None):
    if not db_enabled():
        return default

    cur.execute("SELECT value FROM settings WHERE key = %s", (key,))
    row = cur.fetchone()
    return row[0] if row else default


def db_set_setting(key, value):
    if not db_enabled():
        return

    cur.execute("""
        INSERT INTO settings (key, value)
        VALUES (%s, %s)
        ON CONFLICT (key)
        DO UPDATE SET value = EXCLUDED.value
    """, (key, str(value)))

    conn.commit()

# ---------------- STATUS ----------------
status_cache = {}
offline_since = {}  # roblox_id -> datetime (UTC) when they went offline

def status_text(v):
    return {
        0: "OFFLINE",
        1: "ONLINE",
        2: "IN GAME",
        3: "IN STUDIO"
    }.get(v, "UNKNOWN")

def format_points(n: int) -> str:
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.2f}B"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)

def format_duration(since: datetime) -> str:
    delta = datetime.now(timezone.utc) - since
    total = int(delta.total_seconds())
    hours, remainder = divmod(total, 3600)
    minutes = remainder // 60
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"

async def resolve_roblox_username(username: str):
    url = "https://users.roblox.com/v1/usernames/users"
    async with session.post(
        url,
        json={"usernames": [username], "excludeBannedUsers": False}
    ) as r:
        data = await r.json()
        results = data.get("data", [])
        if not results:
            return None
        return {
            "id": str(results[0]["id"]),
            "name": results[0]["name"]
        }

# ---------------- ROLE CHECK ----------------
def require_role():
    async def predicate(interaction: discord.Interaction) -> bool:
        role_ids = [r.id for r in interaction.user.roles]
        return ALLOWED_ROLE_ID in role_ids
    return app_commands.check(predicate)

@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CheckFailure):
        await interaction.response.send_message(
            "❌ You don't have permission to use this command.", ephemeral=True
        )
    else:
        print("Command error:", error)

# ---------------- STARTUP ----------------
@bot.event
async def on_ready():
    global session, reminder_interval, reminder_channel_id

    print(f"Logged in as {bot.user}")

    if session is None:
        session = aiohttp.ClientSession()

    saved_interval = db_get_setting("reminder_interval")
    if saved_interval is not None:
        reminder_interval = int(saved_interval)
        reminder_loop.change_interval(minutes=reminder_interval)
        print(f"Loaded reminder interval: {reminder_interval}m")

    saved_channel = db_get_setting("reminder_channel_id")
    if saved_channel is not None:
        reminder_channel_id = int(saved_channel)
        print(f"Loaded reminder channel: {reminder_channel_id}")

    try:
        synced = await bot.tree.sync(guild=guild_obj)
        print(f"Synced {len(synced)} commands")
    except Exception as e:
        print("Sync error:", e)

    if not check_loop.is_running():
        check_loop.start()

    if not reminder_loop.is_running():
        reminder_loop.start()

    if not war_poll_loop.is_running():
        war_poll_loop.start()

    if not clan_leave_loop.is_running():
        clan_leave_loop.start()

# ---------------- SLASH COMMANDS ----------------
@bot.tree.command(name="statstest", guild=guild_obj)
async def statstest(interaction: discord.Interaction):
    try:
        users = db_get_all()
        sample = users[0]
        await interaction.response.send_message(f"Sample row: {sample}", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"ERROR: {e}", ephemeral=True)

@bot.tree.command(name="dbtest", guild=guild_obj)
async def dbtest(interaction: discord.Interaction):
    try:
        users = db_get_all()
        await interaction.response.send_message(f"DB OK: {len(users)} users", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"DB ERROR: {e}", ephemeral=True)

@bot.tree.command(name="ping", description="Test command", guild=guild_obj)
@require_role()
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("pong")

@bot.tree.command(name="add", description="Link Roblox user", guild=guild_obj)
@require_role()
async def add(interaction: discord.Interaction, member: discord.Member, roblox_username: str):
    await interaction.response.defer()

    url = "https://users.roblox.com/v1/usernames/users"

    try:
        async with session.post(
            url,
            json={
                "usernames": [roblox_username],
                "excludeBannedUsers": False
            }
        ) as r:
            data = await r.json()

        results = data.get("data", [])

        if not results:
            return await interaction.followup.send(
                "❌ Roblox user not found.",
                ephemeral=True
            )

        rid = str(results[0]["id"])
        name = results[0]["name"]

        try:
            db_add(rid, member.id, name)
        except Exception as db_error:
            print("DB ERROR:", db_error)
            return await interaction.followup.send(
                "❌ Database error occurred.",
                ephemeral=True
            )

        await interaction.followup.send(
            f"✅ Linked {member.mention} → **{name}**"
        )

    except Exception as e:
        print("Roblox API error:", e)
        await interaction.followup.send(
            "❌ Roblox API error.",
            ephemeral=True
        )

@bot.tree.command(name="remove", description="Remove user", guild=guild_obj)
@require_role()
async def remove(interaction: discord.Interaction, member: discord.Member):

    db_remove(member.id)
    offline_since.pop(str(member.id), None)
    await interaction.response.send_message(f"✅ Removed {member.mention} from tracking.", ephemeral=True)

@bot.tree.command(
    name="list",
    description="Show all tracked users and their current Roblox status",
    guild=guild_obj
)
@require_role()
async def list_users(interaction: discord.Interaction):
    users = db_get_all()

    if not users:
        return await interaction.response.send_message(
            "No users are being tracked.",
            ephemeral=True
        )

    status_icons = {
        0: "⚫",
        1: "🟢",
        2: "🎮",
        3: "🔧"
    }

    # ---------------- SAFE SORT (string key fix) ----------------
    def sort_key(u):
        roblox_id = str(u[0])
        return status_cache.get(roblox_id, 0) == 0  # offline last

    online_lines = []
    offline_lines = []

    for roblox_id, discord_id, username in sorted(users, key=sort_key):
        rid = str(roblox_id)

        current = status_cache.get(rid, 0)
        icon = status_icons.get(current, "❓")

        extra = ""
        if current == 0 and rid in offline_since:
            extra = f" — {format_duration(offline_since[rid])}"

        line = f"{icon} <@{discord_id}> — **{username}**{extra}"

        if current == 0:
            offline_lines.append(line)
        else:
            online_lines.append(line)

    lines = online_lines + offline_lines

    online_count = len(online_lines)
    offline_count = len(offline_lines)

    embed = discord.Embed(
        title="📋 Tracked Members",
        description="\n".join(lines) if lines else "No status data available.",
        color=discord.Color.blurple()
    )

    embed.set_footer(
        text=f"🟢 {online_count} online  •  ⚫ {offline_count} offline  •  {len(users)} total"
    )

    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="offlinelist", description="Show only currently offline users and how long they've been offline", guild=guild_obj)
@require_role()
async def offlinelist(interaction: discord.Interaction):
    if not offline_since:
        return await interaction.response.send_message("✅ No tracked users are currently offline.", ephemeral=True)

    users = db_get_all()
    lines = []

    for rid, since in sorted(offline_since.items(), key=lambda x: x[1]):
        info = next((u for u in users if u[0] == rid), None)
        if not info:
            continue
        duration = format_duration(since)
        lines.append(f"⚫ <@{info[1]}> — **{info[2]}** (offline for **{duration}**)")

    if not lines:
        return await interaction.response.send_message("✅ No tracked users are currently offline.", ephemeral=True)

    embed = discord.Embed(
        title="⚫ Offline Users",
        description="\n".join(lines),
        color=discord.Color.dark_gray()
    )
    embed.set_footer(text=f"{len(lines)} user(s) offline")
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="toggleoffline", description="Toggle offline ping alerts on/off", guild=guild_obj)
@require_role()
async def toggleoffline(interaction: discord.Interaction):
    global offline_ping_enabled
    offline_ping_enabled = not offline_ping_enabled
    state = "**ON** ✅" if offline_ping_enabled else "**OFF** ❌"
    await interaction.response.send_message(
        f"Offline ping alerts are now {state}", ephemeral=True
    )

@bot.tree.command(name="setreminderinterval", description="Set how often (in minutes) offline reminders are sent", guild=guild_obj)
@require_role()
async def setreminderinterval(interaction: discord.Interaction, minutes: int):
    global reminder_interval
    if minutes < 5:
        return await interaction.response.send_message(
            "❌ Minimum interval is 5 minutes.", ephemeral=True
        )
    reminder_interval = minutes
    db_set_setting("reminder_interval", minutes)
    reminder_loop.change_interval(minutes=minutes)
    if reminder_loop.is_running():
        reminder_loop.restart()
    await interaction.response.send_message(
        f"⏱️ Offline reminders will now be sent every **{minutes} minute(s)**.", ephemeral=True
    )

@bot.tree.command(name="setreminderchanel", description="Set the channel where offline reminders are sent", guild=guild_obj)
@require_role()
async def setreminderchanel(interaction: discord.Interaction, channel: discord.TextChannel):
    global reminder_channel_id
    reminder_channel_id = channel.id
    db_set_setting("reminder_channel_id", channel.id)
    await interaction.response.send_message(
        f"📢 Offline reminders will now be sent to {channel.mention}.", ephemeral=True
    )

@bot.tree.command(name="clanwar", description="Toggle clan war tracking on/off", guild=guild_obj)
@require_role()
async def clanwar(interaction: discord.Interaction):
    global bot_enabled
    bot_enabled = not bot_enabled

    if bot_enabled:
        await interaction.response.send_message(
            "CLAN WAR TRACKING STARTED!! LETS GO MCWV!!!!!",
            allowed_mentions=discord.AllowedMentions(users=True)
        )
    else:
        offline_since.clear()
        status_cache.clear()
        await interaction.response.send_message("CLAN WAR OVER. GG EVERYONE!!")

@bot.tree.command(name="warinfo", description="Show current PS99 clan war details", guild=guild_obj)
async def warinfo(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=False)

    try:
        async with session.get(PS99_API) as r:
            if r.status != 200:
                return await interaction.followup.send(
                    "❌ Could not reach the PS99 API right now.",
                    ephemeral=True
                )
            war_data = await r.json()

        async with session.get(CLAN_API) as r:
            if r.status != 200:
                return await interaction.followup.send(
                    "❌ Could not reach the clan API right now.",
                    ephemeral=True
                )
            clan_data = await r.json()

    except Exception:
        return await interaction.followup.send(
            "❌ API request failed.",
            ephemeral=True
        )

    # ---------------- CURRENT WAR (COMPARE-STYLE) ----------------
    battle_id, battle = get_current_war(war_data, clan_data)

    if not battle:
        return await interaction.followup.send(
            "❌ Could not determine current war.",
            ephemeral=True
        )

    # ---------------- SAFE TIMING (FIXED SOURCE PRIORITY) ----------------
    war_config = war_data.get("data", {}).get("configData", {})

    start_ts = battle.get("StartTime") or war_config.get("StartTime")
    finish_ts = battle.get("FinishTime") or war_config.get("FinishTime")

    if not start_ts or not finish_ts:
        return await interaction.followup.send(
            "❌ War timing data missing.",
            ephemeral=True
        )

    now = datetime.now(timezone.utc).timestamp()
    total_duration = max(finish_ts - start_ts, 1)
    elapsed = now - start_ts
    progress = max(0.0, min(1.0, elapsed / total_duration))

    # ---------------- NAME ----------------
    friendly_name = re.sub(
        r'(\d+)',
        r' \1',
        re.sub(r'([A-Z])', r' \1', str(battle_id))
    ).strip()

    start_dt = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    finish_dt = datetime.fromtimestamp(finish_ts, tz=timezone.utc)

    # ---------------- CONTRIBUTIONS ----------------
    contributions = sorted(
        battle.get("PointContributions", []),
        key=lambda x: x.get("Points", 0),
        reverse=True
    )

    # ---------------- TOP CONTRIBUTOR (ROBLOX + DISCORD LINK) ----------------
    top_name = "Unknown"
    top_points = 0
    top_discord = "Not linked"

    if contributions:
        top = contributions[0]
        top_points = top.get("Points", 0)
        top_user_id = int(top.get("UserID", 0))

        db_users = db_get_all()
        linked = next((u for u in db_users if int(u[0]) == top_user_id), None)

        if linked:
            top_name = linked[2]
            top_discord = f"<@{linked[1]}>"
        else:
            top_name = str(top_user_id)

    # ---------------- STATUS ----------------
    if now < start_ts:
        status_line = "⏳ **UPCOMING**"
        color = discord.Color.gold()
        bar = "`" + "░" * 20 + "`"
        time_field = f"Starts {discord.utils.format_dt(start_dt, 'R')}"

    elif now > finish_ts:
        status_line = "🏁 **WAR ENDED**"
        color = discord.Color.dark_gray()
        bar = "`" + "█" * 20 + "`"
        time_field = f"Ended {discord.utils.format_dt(finish_dt, 'R')}"

    else:
        status_line = "⚔️ **ACTIVE — IN PROGRESS**"
        color = discord.Color.red()

        filled = int(progress * 20)
        bar = "`" + "█" * filled + "░" * (20 - filled) + f"` {int(progress * 100)}%"

        secs_left = int(finish_ts - now)
        h, rem = divmod(secs_left, 3600)
        m = rem // 60

        time_field = f"Ends {discord.utils.format_dt(finish_dt, 'R')} ({h}h {m}m left)"

    # ---------------- EMBED ----------------
    embed = discord.Embed(
        title=f"🎮 {friendly_name}",
        color=color
    )

    embed.add_field(name="Status", value=status_line, inline=False)
    embed.add_field(name="Progress", value=bar, inline=False)

    embed.add_field(
        name="🕐 Start",
        value=discord.utils.format_dt(start_dt, 'F'),
        inline=True
    )

    embed.add_field(
        name="🏁 End",
        value=discord.utils.format_dt(finish_dt, 'F'),
        inline=True
    )

    embed.add_field(name="⏱ Time", value=time_field, inline=False)

    # ---------------- HUD STATS ----------------
    embed.add_field(
        name="🥇 Top Contributor",
        value=f"**{top_name}**\n{top_discord}\n**{format_points(top_points)} pts**",
        inline=True
    )

    embed.set_footer(text="Data from ps99.biggamesapi.io • Updates every 5 min")

    await interaction.followup.send(embed=embed)
    
@bot.tree.command(name="leaderboard", description="Show MCWV clan war contribution leaderboard", guild=guild_obj)
async def leaderboard(interaction: discord.Interaction):
    await interaction.response.defer()

    try:
        async with session.get(PS99_API) as war_r, session.get(CLAN_API) as clan_r:
            if war_r.status != 200 or clan_r.status != 200:
                return await interaction.followup.send(
                    "❌ Could not reach the PS99 API.",
                    ephemeral=True
                )

            war_data = await war_r.json()
            clan_data = await clan_r.json()

    except Exception:
        return await interaction.followup.send(
            "❌ API request failed.",
            ephemeral=True
        )

    # ---------------- WAR CONFIG ----------------
    war_config = war_data.get("data", {}).get("configData", {})

    # ---------------- CURRENT WAR ----------------
    battle_id, battle = get_current_war(war_data, clan_data)

    if not battle:
        return await interaction.followup.send(
            "❌ No battle data found for MCWV.",
            ephemeral=True
        )

    # ---------------- CONTRIBUTIONS ----------------
    contributions = sorted(
        battle.get("PointContributions", []),
        key=lambda x: x.get("Points", 0),
        reverse=True
    )

    total_points = battle.get("Points", 0)

    if not contributions:
        return await interaction.followup.send(
            "❌ No contribution data yet for this war.",
            ephemeral=True
        )

    # ---------------- ROBLOX NAMES ----------------
    top = contributions[:15]
    user_ids = [e.get("UserID") for e in top if e.get("UserID") is not None]

    try:
        async with session.post(
            ROBLOX_USERS_API,
            json={"userIds": user_ids, "excludeBannedUsers": False}
        ) as r:
            roblox_data = await r.json()
            id_to_name = {u["id"]: u["name"] for u in roblox_data.get("data", [])}
    except Exception:
        id_to_name = {}

    # ---------------- DISCORD LOOKUP ----------------
    db_users = db_get_all()
    roblox_to_discord = {int(u[0]): u[1] for u in db_users}

    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    top_points = top[0]["Points"] if top else 1

    lines = []

    for i, entry in enumerate(top, 1):
        uid = entry.get("UserID")
        pts = entry.get("Points", 0)

        name = id_to_name.get(uid, f"Unknown ({uid})")
        medal = medals.get(i, f"`#{i:>2}`")

        bar_len = int((pts / top_points) * 10)
        bar = "█" * bar_len + "░" * (10 - bar_len)

        discord_mention = f" <@{roblox_to_discord[uid]}>" if uid in roblox_to_discord else ""

        lines.append(f"{medal} **{name}**{discord_mention}\n`{bar}` **{format_points(pts)}**")

    # ---------------- DISPLAY ----------------
    friendly = re.sub(r'(\d+)', r' \1', re.sub(r'([A-Z])', r' \1', battle_id)).strip()

    now = datetime.now(timezone.utc).timestamp()
    finish_ts = war_config.get("FinishTime")
    start_ts = war_config.get("StartTime", 0)

    is_active = False
    if finish_ts:
        is_active = start_ts <= now <= finish_ts

    embed = discord.Embed(
        title=f"🏆  {CLAN_NAME} — {friendly}",
        description="\n".join(lines),
        color=discord.Color.red() if is_active else discord.Color.dark_gold()
    )

    embed.add_field(
        name="🔢  Total Clan Points",
        value=f"**{format_points(total_points)}**",
        inline=True
    )

    embed.add_field(
        name="👥  Contributors",
        value=f"**{len(contributions)}**",
        inline=True
    )

    embed.add_field(
        name="Status",
        value="⚔️ Active" if is_active else "🏁 Ended",
        inline=True
    )

    embed.set_footer(
        text=f"Showing top {len(top)} of {len(contributions)} • ps99.biggamesapi.io"
    )

    await interaction.followup.send(embed=embed)
    
@bot.tree.command(
    name="mystats",
    description="Check a Roblox user's clan war contribution stats",
    guild=guild_obj
)
async def mystats(interaction: discord.Interaction, roblox_username: str):
    await interaction.response.defer()

    try:
        # ---------------- RESOLVE USER ----------------
        resolved = await resolve_roblox_username(roblox_username)
        if not resolved:
            return await interaction.followup.send(
                f"❌ Roblox user `{roblox_username}` not found.",
                ephemeral=True
            )

        roblox_id = int(resolved["id"])
        roblox_name = resolved["name"]

        # ---------------- DB LINK ----------------
        db_users = db_get_all()
        linked = next((u for u in db_users if int(u[0]) == roblox_id), None)
        discord_display = f"<@{linked[1]}>" if linked else "Not linked"

        # ---------------- API CALL ----------------
        async with session.get(PS99_API) as war_r, session.get(CLAN_API) as clan_r:
            if war_r.status != 200 or clan_r.status != 200:
                return await interaction.followup.send(
                    "❌ Could not reach the PS99 API.",
                    ephemeral=True
                )

            war_data = await war_r.json()
            clan_data = await clan_r.json()

        # ---------------- CURRENT WAR ----------------
        battle_id, battle = get_current_war(war_data, clan_data)

        if not battle:
            return await interaction.followup.send(
                "❌ Could not determine current battle.",
                ephemeral=True
            )

        # ---------------- CONTRIBUTIONS ----------------
        contributions = sorted(
            battle.get("PointContributions", []),
            key=lambda x: x.get("Points", 0),
            reverse=True
        )

        total_points = battle.get("Points", 0)

        user_entry = next(
            (e for e in contributions if int(e.get("UserID", 0)) == roblox_id),
            None
        )

        if not user_entry:
            embed = discord.Embed(
                title=f"📊 {roblox_name} — Stats",
                color=discord.Color.red()
            )
            embed.add_field(name="Discord", value=discord_display, inline=True)
            embed.description = "😴 No contributions recorded yet for this war."
            await interaction.followup.send(embed=embed)
            return

        pts = user_entry.get("Points", 0)
        pct = (pts / total_points * 100) if total_points else 0

        rank = next(
            (i + 1 for i, e in enumerate(contributions)
             if int(e.get("UserID", 0)) == roblox_id),
            None
        )

        total_players = len(contributions)
        if rank and total_players > 0:
            top_percent = (1 - ((rank - 1) / total_players)) * 100
        else:
            top_percent = 0

        top_pts = max(contributions[0].get("Points", 1), 1)
        bar_len = int((pts / top_pts) * 20)
        bar = "█" * bar_len + "░" * (20 - bar_len)

        medals = {1: "🥇", 2: "🥈", 3: "🥉"}
        rank_display = medals.get(rank, f"#{rank}" if rank else "—")

        friendly = re.sub(
            r'(\d+)', r' \1',
            re.sub(r'([A-Z])', r' \1', str(battle_id))
        ).strip()

        embed = discord.Embed(
            title=f"📊 {roblox_name} — {friendly}",
            color=discord.Color.red()
        )

        embed.add_field(name="Discord", value=discord_display, inline=True)
        embed.add_field(name="🏅 Rank", value=rank_display, inline=True)
        embed.add_field(name="⚔️ Points", value=format_points(pts), inline=True)
        embed.add_field(name="📈 Share", value=f"{pct:.1f}%", inline=True)
        embed.add_field(
            name="Clan Position",
            value=f"🏅 You below the top **{top_percent:.1f}%** of the clan",
            inline=False
        )
        embed.add_field(name="Progress vs #1", value=f"`{bar}`", inline=False)
        embed.add_field(name="🔢 Clan Total", value=format_points(total_points), inline=True)

        await interaction.followup.send(embed=embed)

    except Exception as e:
        print("[mystats error]", repr(e))
        await interaction.followup.send(
            "❌ Something went wrong while fetching stats.",
            ephemeral=True
        )

@bot.tree.command(
    name="profile",
    description="View a Roblox-linked user profile dashboard",
    guild=guild_obj
)
async def profile(interaction: discord.Interaction, roblox_username: str):
    await interaction.response.defer()

    try:
        # ---------------- RESOLVE USER ----------------
        resolved = await resolve_roblox_username(roblox_username)
        if not resolved:
            return await interaction.followup.send(
                f"❌ Roblox user `{roblox_username}` not found.",
                ephemeral=True
            )

        roblox_id = str(resolved["id"])
        roblox_name = resolved["name"]

        # ---------------- DB LOOKUP ----------------
        db_users = db_get_all()

        linked = next(
            (u for u in db_users if str(u[0]) == roblox_id),
            None
        )

        discord_id = linked[1] if linked else None
        discord_display = f"<@{discord_id}>" if discord_id else "Not linked"

        # ---------------- WAR DATA ----------------
        pts = 0
        rank = None
        battle = None

        try:
            # ensure session exists
            global session
            if session is None or session.closed:
                session = aiohttp.ClientSession()

            async with session.get(CLAN_API) as clan_r:
                if clan_r.status == 200:
                    clan_data = await clan_r.json()
                    battles = clan_data.get("data", {}).get("Battles", {})

                    now = datetime.now(timezone.utc).timestamp()

                    active_battle = None
                    for b_id, b_data in battles.items():
                        start = b_data.get("StartTime", 0)
                        end = b_data.get("FinishTime", 0)

                        if start <= now <= end:
                            active_battle = b_data
                            break

                    battle = active_battle

        except Exception as e:
            print("[profile] war API error:", e)

        # ---------------- STATS ----------------
        if battle:
            contributions = sorted(
                battle.get("PointContributions", []),
                key=lambda x: x.get("Points", 0),
                reverse=True
            )

            user_entry = next(
                (e for e in contributions if str(e.get("UserID")) == roblox_id),
                None
            )

            if user_entry:
                pts = user_entry.get("Points", 0)

                rank = next(
                    (i + 1 for i, e in enumerate(contributions)
                     if str(e.get("UserID")) == roblox_id),
                    None
                )

        # ---------------- IMAGE ----------------
        image_buffer = await generate_profile_card(
            roblox_name=roblox_name,
            roblox_id=int(roblox_id),
            discord_tag=discord_display,
            points=pts,
            rank=rank if rank is not None else 0,
            animated=False
        )

        file = discord.File(fp=image_buffer, filename="profile.png")

        # ---------------- EMBED ----------------
        embed = discord.Embed(
            title=f"📇 Player Profile — {roblox_name}",
            color=discord.Color.blurple()
        )

        embed.add_field(name="🎮 Roblox", value=roblox_name, inline=True)
        embed.add_field(name="🆔 User ID", value=roblox_id, inline=True)
        embed.add_field(name="💬 Discord", value=discord_display, inline=True)

        if battle:
            embed.add_field(
                name="⚔️ Current War",
                value=f"Points: **{pts:,}**",
                inline=False
            )
        else:
            embed.add_field(
                name="⚔️ Current War",
                value="No active war",
                inline=False
            )

        embed.set_image(url="attachment://profile.png")

        await interaction.followup.send(embed=embed, file=file)

    except Exception as e:
        print("[profile] error:", repr(e))
        await interaction.followup.send(
            f"❌ Profile failed: `{e}`",
            ephemeral=True
        )

@bot.tree.command(name="clanstats", description="Show MCWV clan overview — level, members, diamonds, and battle history", guild=guild_obj)
async def clanstats(interaction: discord.Interaction):
    await interaction.response.defer()

    try:
        async with session.get(CLAN_API) as r:
            if r.status != 200:
                return await interaction.followup.send("❌ Could not reach the PS99 API.", ephemeral=True)
            data = (await r.json()).get("data", {})
    except Exception:
        return await interaction.followup.send("❌ API request failed.", ephemeral=True)

    name        = data.get("Name", CLAN_NAME)
    level       = data.get("Level", "?")
    members     = data.get("Members", [])
    diamonds    = data.get("Diamonds", 0)
    battles     = data.get("Battles", {})

    # --- battle history stats ---
    total_battles = len(battles)
    best_pts = 0
    best_battle = ""
    contributor_totals = {}

    for bid, b in battles.items():
        clan_pts = b.get("Points", 0)
        if clan_pts > best_pts:
            best_pts = clan_pts
            best_battle = bid
        for entry in b.get("PointContributions", []):
            uid = entry["UserID"]
            contributor_totals[uid] = contributor_totals.get(uid, 0) + entry.get("Points", 0)

    # resolve best overall contributor name
    best_contributor_display = "—"
    if contributor_totals:
        top_uid = max(contributor_totals, key=lambda u: contributor_totals[u])
        top_pts = contributor_totals[top_uid]
        db_users = db_get_all()
        db_match = next((u for u in db_users if int(u[0]) == top_uid), None)
        if db_match:
            rname = db_match[2]
            discord_id = db_match[1]
            best_contributor_display = f"<@{discord_id}>\n{rname}\n{format_points(top_pts)} pts"
        else:
            # fallback to Roblox API
            try:
                async with session.get(f"{ROBLOX_USERS_API}/{top_uid}") as ur:
                    if ur.status == 200:
                        rname = (await ur.json()).get("name", str(top_uid))
                    else:
                        rname = str(top_uid)
            except Exception:
                rname = str(top_uid)
            best_contributor_display = f"{rname}\n{format_points(top_pts)} pts"

    # friendly battle name
    def friendly_battle(bid):
        if not bid:
            return "—"
        return re.sub(r'(\d+)', r' \1', re.sub(r'([A-Z])', r' \1', bid)).strip()

    embed = discord.Embed(
        title=f"🏰  {name}  —  Clan Overview",
        color=discord.Color.blurple()
    )
    embed.add_field(name="👥  Members",  value=f"**{len(members)}**",            inline=True)
    embed.add_field(name="💎  Diamonds", value=f"**{format_points(diamonds)}**", inline=True)

    embed.add_field(name="\u200b", value="─────────────────────── **Battle History** ───────────────────────", inline=False)

    embed.add_field(name="⚔️  Battles",  value=f"**{total_battles}**",           inline=True)
    embed.add_field(name="🔥  Best War", value=f"**{friendly_battle(best_battle)}**\n{format_points(best_pts)} pts", inline=True)
    embed.add_field(name="🌟  Best Overall Contributor", value=best_contributor_display, inline=False)

    embed.set_footer(text="ps99.biggamesapi.io")
    await interaction.followup.send(embed=embed)

@bot.tree.command(name="compare", description="Compare two linked clan members head-to-head in the current war", guild=guild_obj)
async def compare(interaction: discord.Interaction, member1: discord.Member, member2: discord.Member):
    await interaction.response.defer()

    db_users = db_get_all()

    def get_linked(m):
        return next((u for u in db_users if u[1] == m.id), None)

    link1 = get_linked(member1)
    link2 = get_linked(member2)

    errors = []
    if not link1:
        errors.append(f"❌ {member1.mention} is not linked to a Roblox account.")
    if not link2:
        errors.append(f"❌ {member2.mention} is not linked to a Roblox account.")
    if errors:
        return await interaction.followup.send("\n".join(errors), ephemeral=True)

    rid1, rid2 = int(link1[0]), int(link2[0])
    name1, name2 = link1[2], link2[2]

    try:
        async with session.get(PS99_API) as war_r, session.get(CLAN_API) as clan_r:
            if war_r.status != 200 or clan_r.status != 200:
                return await interaction.followup.send("❌ Could not reach the PS99 API.", ephemeral=True)
            war_data = await war_r.json()
            clan_data = await clan_r.json()
    except Exception:
        return await interaction.followup.send("❌ API request failed.", ephemeral=True)

    war_config = war_data.get("data", {}).get("configData", {})
    active_battle_id = war_config.get("Title") or war_data.get("data", {}).get("configName")
    battles = clan_data.get("data", {}).get("Battles", {})

    battle_id = None
    if active_battle_id and active_battle_id in battles:
        battle_id = active_battle_id
    elif battles:
        battle_id = list(battles.keys())[-1]

    if not battle_id:
        return await interaction.followup.send("❌ No battle data found.", ephemeral=True)

    battle = battles[battle_id]
    contributions = sorted(
        battle.get("PointContributions", []),
        key=lambda x: x.get("Points", 0),
        reverse=True
    )

    def get_entry(rid):
        entry = next((e for e in contributions if e["UserID"] == rid), None)
        rank = next((i + 1 for i, e in enumerate(contributions) if e["UserID"] == rid), None)
        pts = entry["Points"] if entry else 0
        return pts, rank

    pts1, rank1 = get_entry(rid1)
    pts2, rank2 = get_entry(rid2)

    friendly = re.sub(r'(\d+)', r' \1', re.sub(r'([A-Z])', r' \1', battle_id)).strip()
    now = datetime.now(timezone.utc).timestamp()
    finish_ts = war_config.get("FinishTime")
    is_active = finish_ts and war_config.get("StartTime", 0) <= now <= finish_ts
    color = discord.Color.red() if is_active else discord.Color.dark_gold()

    # head-to-head bar
    total = pts1 + pts2
    if total > 0:
        share1 = int((pts1 / total) * 20)
        share2 = 20 - share1
    else:
        share1 = share2 = 10
    hth_bar = f"{'█' * share1}{'░' * share2}"
    pct1 = (pts1 / total * 100) if total else 50.0
    pct2 = 100 - pct1

    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    rank1_display = medals.get(rank1, f"#{rank1}") if rank1 else "—"
    rank2_display = medals.get(rank2, f"#{rank2}") if rank2 else "—"

    winner = ""
    if pts1 > pts2:
        winner = f"\n🏆 **{name1}** is ahead"
    elif pts2 > pts1:
        winner = f"\n🏆 **{name2}** is ahead"
    else:
        winner = "\n🤝 Tied!"

    embed = discord.Embed(
        title=f"⚔️  {name1}  vs  {name2}",
        description=f"**{friendly}**{winner}",
        color=color
    )
    embed.add_field(name=f"📊 {name1}", value=f"**{format_points(pts1)}** pts\nRank: {rank1_display}", inline=True)
    embed.add_field(name="\u200b", value="\u200b", inline=True)
    embed.add_field(name=f"📊 {name2}", value=f"**{format_points(pts2)}** pts\nRank: {rank2_display}", inline=True)
    embed.add_field(
        name="Head-to-Head",
        value=f"`{hth_bar}`\n{name1} **{pct1:.0f}%** — **{pct2:.0f}%** {name2}",
        inline=False
    )
    status_str = "⚔️ Active" if is_active else "🏁 Ended"
    embed.set_footer(text=f"{status_str} • ps99.biggamesapi.io")
    await interaction.followup.send(embed=embed)

@bot.tree.command(name="accept", description="Accept an applicant inside a Tickets v2 ticket", guild=guild_obj)
@require_role()
async def accept(interaction: discord.Interaction, member: discord.Member):
    await interaction.response.defer(ephemeral=True)

    channel       = interaction.channel
    guild         = interaction.guild
    ticket_creator = member

    # --- ask for Roblox username ---
    await channel.send(
        f"👋 {ticket_creator.mention} — you've been accepted into **MCWV**! 🎉\n"
        f"Please reply with your **Roblox username** to complete your setup."
    )

    def from_creator(m):
        return m.author.id == ticket_creator.id and m.channel.id == channel.id and bool(m.content.strip())

    try:
        username_msg = await bot.wait_for("message", check=from_creator, timeout=120)
        roblox_input = username_msg.content.strip()
    except asyncio.TimeoutError:
        return await interaction.followup.send(
            "❌ Timed out waiting for Roblox username. Run `/accept` again to retry.",
            ephemeral=True
        )

    # --- ask for alts ---
    await channel.send(
        "Got it! If you have any other accounts **IN THE CLAN**, please reply with any **alt account usernames** "
        "(comma-separated if multiple), or type `none` if you have none."
    )

    try:
        alts_msg = await bot.wait_for("message", check=from_creator, timeout=90)
        alts_raw = alts_msg.content.strip()
    except asyncio.TimeoutError:
        alts_raw = "none"

    alts = [] if alts_raw.lower() == "none" else [a.strip() for a in alts_raw.split(",") if a.strip()]

    # --- validate Roblox username via API ---
    roblox_url = "https://users.roblox.com/v1/usernames/users"
    try:
        async with session.post(roblox_url, json={"usernames": [roblox_input], "excludeBannedUsers": False}) as r:
            body = await r.json()
            print(f"[accept] Roblox lookup for '{roblox_input}': HTTP {r.status} → {body}")
            if r.status != 200:
                return await interaction.followup.send(
                    f"❌ Roblox API returned an error (HTTP {r.status}). Try again in a moment.",
                    ephemeral=True
                )
            results = body.get("data", [])
            if not results:
                return await interaction.followup.send(
                    f"❌ Roblox user `{roblox_input}` not found. Please check the spelling and try again.",
                    ephemeral=True
                )
            roblox_id   = str(results[0]["id"])
            roblox_name = results[0]["name"]
    except Exception as e:
        print(f"[accept] Roblox API exception: {e}")
        return await interaction.followup.send("❌ Roblox API error. Try again in a moment.", ephemeral=True)

    # --- link in bot DB ---
    db_add(roblox_id, ticket_creator.id, roblox_name)

    actions = []
    errors  = []

    # --- give clan member role ---
    clan_role = guild.get_role(CLAN_MEMBER_ROLE_ID)
    if clan_role:
        try:
            await ticket_creator.add_roles(clan_role, reason=f"Accepted by {interaction.user}")
            actions.append(f"✅ Gave role **{clan_role.name}**")
        except Exception as e:
            errors.append(f"❌ Could not give role: {e}")
    else:
        errors.append("❌ Clan member role not found — check CLAN_MEMBER_ROLE_ID.")

    # --- move ticket to Clan Members category ---
    category = guild.get_channel(CLAN_MEMBERS_CATEGORY_ID)
    if category:
        try:
            await channel.edit(category=category, sync_permissions=True, reason="Member accepted")
            actions.append(f"✅ Moved ticket to **{category.name}**")
        except Exception as e:
            errors.append(f"❌ Could not move ticket: {e}")
    else:
        errors.append("❌ Clan Members category not found — check CLAN_MEMBERS_CATEGORY_ID.")

    # --- post membership record in members channel ---
    members_ch = guild.get_channel(MEMBERS_CHANNEL_ID)
    alts_str   = ", ".join(alts) if alts else "none"
    record_msg = (
        f"<#{channel.id}> {ticket_creator.mention}\n"
        f"user:{roblox_name}\n"
        f"alt:{alts_str}"
    )
    if members_ch:
        try:
            await members_ch.send(record_msg)
            actions.append(f"✅ Posted membership record in <#{MEMBERS_CHANNEL_ID}>")
        except Exception as e:
            errors.append(f"❌ Could not post membership record: {e}")
    else:
        errors.append("❌ Members channel not found — check MEMBERS_CHANNEL_ID.")

    # --- confirmation message in ticket ---
    await channel.send(
        f"✅ All done, {ticket_creator.mention}! Welcome to **MCWV**!\n"
        f"Roblox: **{roblox_name}**"
        + (f"\nAlts: **{alts_str}**" if alts else "")
    )

    # --- log all actions ---
    log_ch = guild.get_channel(LOG_CHANNEL_ID)
    if log_ch:
        log_embed = discord.Embed(
            title="✅  Member Accepted",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc)
        )
        log_embed.add_field(name="Staff",       value=interaction.user.mention,  inline=True)
        log_embed.add_field(name="New Member",  value=ticket_creator.mention,    inline=True)
        log_embed.add_field(name="Roblox",      value=roblox_name,               inline=True)
        log_embed.add_field(name="Alts",        value=alts_str,                  inline=True)
        log_embed.add_field(name="Ticket",      value=f"<#{channel.id}>",        inline=True)
        log_embed.add_field(name="Actions",     value="\n".join(actions) or "—", inline=False)
        if errors:
            log_embed.add_field(name="⚠️ Errors", value="\n".join(errors), inline=False)
        log_embed.set_footer(text=f"Member ID: {ticket_creator.id} • Roblox ID: {roblox_id}")
        try:
            await log_ch.send(embed=log_embed)
        except Exception:
            pass

    # --- report back to staff (ephemeral) ---
    summary = "\n".join(actions)
    if errors:
        summary += "\n\n⚠️ **Some steps failed:**\n" + "\n".join(errors)
    await interaction.followup.send(f"**Accept complete!**\n{summary}", ephemeral=True)

@bot.tree.command(
    name="kick",
    description="Remove a member (Discord or Roblox username)",
    guild=guild_obj
)
@require_role()
async def kick(interaction: discord.Interaction, target: str, reason: str = "No reason provided"):

    await interaction.response.defer(ephemeral=True)

    try:
        guild = interaction.guild
        db_users = db_get_all()

        member = None
        roblox_name = None
        roblox_id = None

        # ---------------- Discord detection ----------------
        import re
        match = re.match(r"<@!?(\d+)>", target)

        if match:
            discord_id = int(match.group(1))
        elif target.isdigit():
            discord_id = int(target)
        else:
            discord_id = None

        # ---------------- Resolve member ----------------
        if discord_id:
            member = guild.get_member(discord_id)
            if not member:
                try:
                    member = await guild.fetch_member(discord_id)
                except:
                    member = None

        # ---------------- Roblox fallback ----------------
        if not member:
            roblox_name = target.lower()
            linked = next((u for u in db_users if u[2].lower() == roblox_name), None)

            if not linked:
                return await interaction.followup.send("❌ User not found.", ephemeral=True)

            roblox_id = linked[0]
            discord_id = linked[1]

            member = guild.get_member(int(discord_id))
            if not member:
                try:
                    member = await guild.fetch_member(int(discord_id))
                except:
                    member = None

            roblox_name = linked[2]

        if not member:
            return await interaction.followup.send("❌ Member not found in server.", ephemeral=True)

        # ---------------- Linked info ----------------
        linked = next((u for u in db_users if u[1] == member.id), None)

        if linked:
            roblox_name = linked[2]
            roblox_id = linked[0]

        actions = []

        # ---------------- Role removal ----------------
        clan_role = guild.get_role(CLAN_MEMBER_ROLE_ID)

        if clan_role and clan_role in member.roles:
            await member.remove_roles(clan_role, reason=reason)
            actions.append("✅ Removed clan role")

        # ---------------- DB unlink ----------------
        if linked:
            db_remove(member.id)
            actions.append("✅ Unlinked account")

        # ---------------- Cache cleanup ----------------
        if roblox_id:
            offline_since.pop(str(roblox_id), None)
            status_cache.pop(str(roblox_id), None)

        # ---------------- RESPONSE (CRITICAL) ----------------
        await interaction.followup.send(
            f"❌ Kicked **{member.display_name}**\n" + "\n".join(actions),
            ephemeral=True
        )

    except Exception as e:
        await interaction.followup.send(f"❌ Kick failed: `{e}`", ephemeral=True)
        print("Kick error:", e)

    # ---------------- SEND CONFIRMATION ----------------
    embed = discord.Embed(
        title="⚠️ Confirm Kick",
        description=f"Are you sure you want to kick **{member.display_name}**?",
        color=discord.Color.orange()
    )

    embed.add_field(name="Roblox", value=roblox_name or "Unknown", inline=True)
    embed.add_field(name="Reason", value=reason, inline=False)

    await interaction.followup.send(embed=embed, view=KickConfirmView(), ephemeral=True)

@bot.tree.command(name="settings", description="Show current bot settings", guild=guild_obj)
@require_role()
async def settings(interaction: discord.Interaction):
    ping_state = "ON ✅" if offline_ping_enabled else "OFF ❌"

    if reminder_channel_id == CHANNEL_ID:
        channel_display = f"<#{CHANNEL_ID}> (default)"
    else:
        channel_display = f"<#{reminder_channel_id}>"

    war_state = "⚔️ Active" if bot_enabled else "💤 Inactive"
    embed = discord.Embed(title="⚙️ Bot Settings", color=discord.Color.blurple())
    embed.add_field(name="⚔️ War Tracking",      value=war_state,              inline=True)
    embed.add_field(name="🔔 Offline Alerts",    value=ping_state,             inline=True)
    embed.add_field(name="⏱️ Reminder Interval", value=f"{reminder_interval} min", inline=True)
    embed.add_field(name="📢 Reminder Channel",  value=channel_display,        inline=True)
    embed.add_field(name="🚨 Alert Channel",     value=f"<#{CHANNEL_ID}>",     inline=True)
    embed.set_footer(text="/toggleoffline • /setreminderinterval • /setreminderchanel")
    await interaction.response.send_message(embed=embed, ephemeral=True)

# ---------------- STATUS COMMAND ----------------
@bot.tree.command(name="status", description="Check Roblox status", guild=guild_obj)
async def status(interaction: discord.Interaction, member: discord.Member):

    # ---------------- COOLDOWN CHECK ----------------
    now = datetime.now(timezone.utc).timestamp()
    user_id = interaction.user.id

    last_used = status_cooldown.get(user_id, 0)

    if now - last_used < 5:
        remaining = round(5 - (now - last_used), 1)
        return await interaction.response.send_message(
            f"⏳ Slow down — wait {remaining}s before using this again.",
            ephemeral=True
        )

    status_cooldown[user_id] = now

    # ---------------- DB LOOKUP ----------------
    users = db_get_all()

    target = next((u for u in users if int(u[1]) == member.id), None)

    if not target:
        return await interaction.response.send_message("Not linked", ephemeral=True)

    roblox_id = str(target[0])   # 🔥 FIX: force string
    roblox_name = target[2]

    # ---------------- STATUS ----------------
    current = status_cache.get(roblox_id, 0)  # 🔥 FIX: string key

    status_icons = {
        0: "⚫",
        1: "🟢",
        2: "🎮",
        3: "🔧"
    }

    icon = status_icons.get(current, "❓")

    # ---------------- OFFLINE INFO ----------------
    extra = ""

    if current == 0 and roblox_id in offline_since:
        since_dt = offline_since[roblox_id]
        extra = f"\nOffline since {discord.utils.format_dt(since_dt, 'R')} ({format_duration(since_dt)})"

    # ---------------- RESPONSE ----------------
    await interaction.response.send_message(
        f"{icon} **{roblox_name}** — {status_text(current)}{extra}",
        ephemeral=True
    )


# ---------------- ROBLOX LOOP ----------------
@tasks.loop(minutes=2)
async def check_loop():
    users = db_get_all()

    if not users or not bot_enabled:
        return

    print("Loop running, users:", len(users))

    try:
        global session

        if session is None or session.closed:
            session = aiohttp.ClientSession()

        user_ids = [int(u[0]) for u in users]

        url = "https://presence.roblox.com/v1/presence/users"

        async with session.post(url, json={"userIds": user_ids}) as r:
            if r.status != 200:
                print("Presence API returned:", r.status)
                return

            data = await r.json()

    except Exception as e:
        print("Loop error (API fetch):", e)
        return

    try:
        for u in data.get("userPresences", []):

            rid = str(u["userId"])  # 🔥 STRING KEY CONSISTENCY
            current = u["userPresenceType"]

            old = status_cache.get(rid)
            status_cache[rid] = current

            # ---------------- DB SAVE ----------------
            try:
                cur.execute("""
                    INSERT INTO user_status (roblox_id, status, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (roblox_id)
                    DO UPDATE SET status = EXCLUDED.status, updated_at = NOW()
                """, (rid, current))
                conn.commit()

            except Exception as db_error:
                print("Loop error (DB write):", db_error)

            # no change → skip
            if old == current:
                continue

            now = datetime.now(timezone.utc)

            info = next((x for x in users if str(x[0]) == rid), None)
            if not info:
                continue

            # IN GAME → reset offline tracking
            if current == 2:
                offline_since.pop(rid, None)
                continue

            # first time seen offline
            if rid not in offline_since:
                offline_since[rid] = now

            # ping on leaving game
            try:
                if old == 2 and str(db_get_setting("offline_tracking")).lower() == "true":
                    channel = await bot.fetch_channel(CHANNEL_ID)

                    await channel.send(
                        f"⚫ <@{info[1]}> **({info[2]})** is no longer in game — {discord.utils.format_dt(now, 'R')}",
                        allowed_mentions=discord.AllowedMentions(users=True)
                    )

            except Exception as e:
                print("Loop error (ping):", e)

    except Exception as e:
        print("Loop error (processing):", e)
        
# ---------------- ROBLOX LOOP (every 2 min — detects transitions) ----------------
@tasks.loop(minutes=2)
async def check_loop():
    users = db_get_all()
    if not users or not bot_enabled:
        return

    print("Loop running, users:", len(users))  # DEBUG LINE

    try:
        global session

        if session is None or session.closed:
            session = aiohttp.ClientSession()

        user_ids = [int(u[0]) for u in users]
        url = "https://presence.roblox.com/v1/presence/users"

        async with session.post(url, json={"userIds": user_ids}) as r:
            if r.status != 200:
                print("Presence API returned:", r.status)
                return

            data = await r.json()

    except Exception as e:
        print("Loop error (API fetch):", e)
        return

    try:
        for u in data.get("userPresences", []):
            rid = str(u["userId"])
            current = u["userPresenceType"]

            old = status_cache.get(rid)
            status_cache[rid] = current

            # save to Neon (persistent state)
            try:
                cur.execute("""
                    INSERT INTO user_status (roblox_id, status, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (roblox_id)
                    DO UPDATE SET status = EXCLUDED.status, updated_at = NOW()
                """, (rid, current))
                conn.commit()
            except Exception as db_error:
                print("Loop error (DB write):", db_error)

            # no change → skip processing
            if old == current:
                continue

            now = datetime.now(timezone.utc)

            info = next((x for x in users if x[0] == rid), None)
            if not info:
                continue

            # IN GAME → reset offline tracking
            if current == 2:
                offline_since.pop(rid, None)
                continue

            # first time seen outside game
            if rid not in offline_since:
                offline_since[rid] = now

            # ONLY ping when leaving IN GAME
            if old is not None and old == 2 and str(db_get_setting("offline_tracking")).lower() == "true":
                channel = await bot.fetch_channel(CHANNEL_ID)
                await channel.send(
                    f"⚫ <@{info[1]}> **({info[2]})** is no longer in game — {discord.utils.format_dt(now, 'R')}",
                    allowed_mentions=discord.AllowedMentions(users=True)
                )

    except Exception as e:
        print("Loop error (processing):", e)


# ---------------- REMINDER LOOP (every 30 min — re-pings offline users) ----------------
@tasks.loop(minutes=30)
async def reminder_loop():

    if not bot_enabled or not offline_ping_enabled:
        return

    if not offline_since:
        return

    try:
        channel = await bot.fetch_channel(reminder_channel_id)
    except:
        return

    users = db_get_all()
    now = datetime.now(timezone.utc)

    lines = []

    for rid, since in offline_since.items():

        # ONLY skip IN GAME
        if status_cache.get(rid) == 2:
            continue

        info = next((x for x in users if x[0] == rid), None)
        if not info:
            continue

        duration = format_duration(since)
        lines.append(f"⚫ <@{info[1]}> **({info[2]})** is not in game - {duration}")

    if not lines:
        return

    await channel.send("\n\n".join(lines))

# ---------------- PS99 WAR POLL (SAFE STATE MACHINE VERSION) ----------------
@tasks.loop(minutes=2)
async def war_poll_loop():
    global bot_enabled, ps99_war_active, ps99_first_check

    try:
        async with session.get(PS99_API) as r:
            api_ok = r.status == 200
            data = await r.json() if api_ok else {}

        config = data.get("data", {}).get("configData", {})
        start = config.get("StartTime")
        finish = config.get("FinishTime")

        now = datetime.now(timezone.utc).timestamp()

        # SAFE CALCULATION
        currently_active = (
            api_ok and
            isinstance(start, (int, float)) and
            isinstance(finish, (int, float)) and
            start <= now <= finish
        )

        # FIRST RUN INITIALISATION
        if ps99_first_check:
            ps99_first_check = False
            ps99_war_active = currently_active
            bot_enabled = currently_active
            print(f"[INIT] War state set to {currently_active}")
            return

        # ALWAYS SYNC WAR STATE (no missed transitions)
        if ps99_war_active != currently_active:
            ps99_war_active = currently_active
            bot_enabled = currently_active

            channel = await bot.fetch_channel(CHANNEL_ID)

            if currently_active:
                await channel.send("⚠️ CLAN WAR STARTED!! LETS GO MCWV!!!!!")
                print("War started (state synced)")

                await run_initial_presence_check(channel)

            else:
                offline_since.clear()
                status_cache.clear()

                await channel.send("🛑 CLAN WAR OVER. GG EVERYONE!!")
                print("War ended (state synced)")

    except Exception as e:
        print("War poll error:", e)
# ---------------- CLAN LEAVE DETECTION (STAFF PANEL) ----------------

pending_clan_removals = {}

@tasks.loop(minutes=10)
async def clan_leave_loop():
    users = db_get_all()
    if not users:
        return

    try:
        async with session.get(CLAN_API) as r:
            if r.status != 200:
                return
            data = (await r.json()).get("data", {})
    except Exception as e:
        print("Clan leave loop error:", e)
        return

    clan_member_ids = {
        int(m["UserID"])
        for m in data.get("Members", [])
        if "UserID" in m
    }

    if not clan_member_ids:
        return

    guild = bot.get_guild(GUILD_ID)
    if not guild:
        return

    staff_channel = guild.get_channel(1501639281750442114)

    for roblox_id, discord_id, roblox_name in users:

        if int(roblox_id) in clan_member_ids:
            continue

        if roblox_id in pending_clan_removals:
            continue

        pending_clan_removals[roblox_id] = {
            "discord_id": discord_id,
            "roblox_name": roblox_name
        }

        embed = discord.Embed(
            title="🚨 Clan Leave Detected",
            description=f"**{roblox_name}** is no longer in the clan.",
            color=discord.Color.orange(),
            timestamp=datetime.now(timezone.utc)
        )

        embed.add_field(name="Discord User", value=f"<@{discord_id}>", inline=True)
        embed.add_field(name="Roblox", value=roblox_name, inline=True)
        embed.add_field(name="Action Required", value="Approve or ignore this removal.", inline=False)

        await staff_channel.send(
            embed=embed,
            view=ClanReviewView(roblox_id)
        )

@discord.ui.button(label="Approve Removal", style=discord.ButtonStyle.danger)
async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):

    try:
        if self.roblox_id not in pending_clan_removals:
            return await interaction.response.send_message("Already handled.", ephemeral=True)

        data = pending_clan_removals.pop(self.roblox_id)

        guild = interaction.guild
        member = guild.get_member(int(data["discord_id"]))

        if member is None:
            try:
                member = await guild.fetch_member(int(data["discord_id"]))
            except:
                member = None

        role = guild.get_role(CLAN_MEMBER_ROLE_ID)

        if member and role and role in member.roles:
            await member.remove_roles(role, reason="Staff approved clan removal")

        try:
            db_remove(data["discord_id"])
        except Exception as e:
            print("DB remove error:", e)

        await interaction.response.edit_message(
            content="✅ Member removed and processed.",
            embed=interaction.message.embeds[0],
            view=None
        )

    except Exception as e:
        print("Approve button error:", e)
        try:
            await interaction.response.send_message(
                "❌ Something went wrong while processing this.",
                ephemeral=True
            )
        except:
            pass

# ---------------- CLEANUP ----------------
@bot.event
async def on_ready():
    global session

    if session is None or session.closed:
        session = aiohttp.ClientSession()

    try:
        synced = await bot.tree.sync(guild=guild_obj)
        print(f"Synced {len(synced)} commands")
    except Exception as e:
        print("Sync error:", e)

    print(f"Logged in as {bot.user}")

    # start war loop safely (only once)
    if not war_poll_loop.is_running():
        war_poll_loop.start()

    # start check loop safely (ONLY FIX YOU NEEDED)
    if not check_loop.is_running():
        check_loop.start()
        print("check_loop started")


@bot.event
async def on_disconnect():
    global session
    if session:
        await session.close()
        
# ---------------- RUN ----------------
from threading import Thread
import asyncio

def run_flask():
    app.run(host="0.0.0.0", port=10000)

async def main():
    await bot.start(TOKEN)

if __name__ == "__main__":
    Thread(target=run_flask).start()
    asyncio.run(main())
