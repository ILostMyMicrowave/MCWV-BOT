import os
import re
import asyncio
import sqlite3
import discord
import aiohttp

from datetime import datetime, timezone
from discord import app_commands
from discord.ext import commands, tasks

from flask import Flask
from threading import Thread
import os

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is alive"

def run_web():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

Thread(target=run_web).start()

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
        async with session.post(url, json={
            "usernames": [roblox_username],
            "excludeBannedUsers": False
        }) as r:
            data = await r.json()

        results = data.get("data", [])

        if not results:
            return await interaction.followup.send("❌ Roblox user not found.", ephemeral=True)

        rid = str(results[0]["id"])
        name = results[0]["name"]

        # SAFE DB CALL (prevents bot crash)
        try:
            db_add(rid, member.id, name)
        except Exception as db_error:
            print("DB ERROR:", db_error)
            return await interaction.followup.send("❌ Database error occurred.", ephemeral=True)

        await interaction.followup.send(f"✅ Linked {member.mention} → **{name}**")

    except Exception as e:
        print("Roblox API error:", e)
        await interaction.followup.send("❌ Roblox API error.", ephemeral=True)

@bot.tree.command(name="remove", description="Remove user", guild=guild_obj)
@require_role()
async def remove(interaction: discord.Interaction, member: discord.Member):

    db_remove(member.id)
    offline_since.pop(str(member.id), None)
    await interaction.response.send_message(f"✅ Removed {member.mention} from tracking.", ephemeral=True)

@bot.tree.command(name="list", description="Show all tracked users and their current Roblox status", guild=guild_obj)
@require_role()
async def list_users(interaction: discord.Interaction):
    users = db_get_all()

    if not users:
        return await interaction.response.send_message("No users are being tracked.", ephemeral=True)

    status_icons = {0: "⚫", 1: "🟢", 2: "🎮", 3: "🔧"}

    def sort_key(u):
        return status_cache.get(u[0], 0) == 0  # offline (0) sorts last

    online_lines = []
    offline_lines = []
    for roblox_id, discord_id, username in sorted(users, key=sort_key):
        current = status_cache.get(roblox_id, 0)
        icon = status_icons.get(current, "❓")
        extra = ""
        if current == 0 and roblox_id in offline_since:
            extra = f" — {format_duration(offline_since[roblox_id])}"
        line = f"{icon} <@{discord_id}> — **{username}**{extra}"
        if current == 0:
            offline_lines.append(line)
        else:
            online_lines.append(line)

    lines = online_lines + offline_lines
    online_count  = len(online_lines)
    offline_count = len(offline_lines)

    embed = discord.Embed(
        title="📋 Tracked Members",
        description="\n".join(lines),
        color=discord.Color.blurple()
    )
    embed.set_footer(text=f"🟢 {online_count} online  •  ⚫ {offline_count} offline  •  {len(users)} total")
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
                return await interaction.followup.send("❌ Could not reach the PS99 API right now.", ephemeral=True)
            data = await r.json()
    except Exception as e:
        return await interaction.followup.send("❌ API request failed.", ephemeral=True)

    config = data.get("data", {}).get("configData", {})
    raw_name = config.get("Title") or data.get("data", {}).get("configName", "Unknown")
    start_ts = config.get("StartTime")
    finish_ts = config.get("FinishTime")

    if not start_ts or not finish_ts:
        return await interaction.followup.send("❌ No clan war data found.", ephemeral=True)

    now = datetime.now(timezone.utc).timestamp()
    total_duration = finish_ts - start_ts
    elapsed = now - start_ts

    # Friendly name (e.g. "AngelBattle2026" → "Angel Battle 2026")
    friendly_name = re.sub(r'(\d+)', r' \1', re.sub(r'([A-Z])', r' \1', raw_name)).strip()

    start_dt = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    finish_dt = datetime.fromtimestamp(finish_ts, tz=timezone.utc)

    if now < start_ts:
        status_line = "⏳  **UPCOMING**"
        color = discord.Color.gold()
        bar = "`" + "░" * 20 + "`"
        time_field = f"Starts {discord.utils.format_dt(start_dt, 'R')}"
    elif now > finish_ts:
        status_line = "🏁  **WAR ENDED**"
        color = discord.Color.dark_gray()
        bar = "`" + "█" * 20 + "`"
        time_field = f"Ended {discord.utils.format_dt(finish_dt, 'R')}"
    else:
        status_line = "⚔️  **ACTIVE — IN PROGRESS**"
        color = discord.Color.red()
        progress = max(0.0, min(1.0, elapsed / total_duration))
        filled = int(progress * 20)
        bar = "`" + "█" * filled + "░" * (20 - filled) + f"` {int(progress * 100)}%"
        secs_left = int(finish_ts - now)
        h, rem = divmod(secs_left, 3600)
        m = rem // 60
        time_field = f"Ends {discord.utils.format_dt(finish_dt, 'R')} ({h}h {m}m left)"

    embed = discord.Embed(
        title=f"🎮  {friendly_name}",
        color=color
    )
    embed.add_field(name="Status", value=status_line, inline=False)
    embed.add_field(name="Progress", value=bar, inline=False)
    embed.add_field(
        name="🕐  Start",
        value=discord.utils.format_dt(start_dt, 'F'),
        inline=True
    )
    embed.add_field(
        name="🏁  End",
        value=discord.utils.format_dt(finish_dt, 'F'),
        inline=True
    )
    embed.add_field(name="⏱️  Time", value=time_field, inline=False)
    embed.set_footer(text="Data from ps99.biggamesapi.io • Updates every 5 min")

    await interaction.followup.send(embed=embed)

@bot.tree.command(name="leaderboard", description="Show MCWV clan war contribution leaderboard", guild=guild_obj)
async def leaderboard(interaction: discord.Interaction):
    await interaction.response.defer()

    try:
        # Fetch active war name and clan data in parallel
        async with session.get(PS99_API) as war_r, session.get(CLAN_API) as clan_r:
            if war_r.status != 200 or clan_r.status != 200:
                return await interaction.followup.send("❌ Could not reach the PS99 API.", ephemeral=True)
            war_data = await war_r.json()
            clan_data = await clan_r.json()
    except Exception:
        return await interaction.followup.send("❌ API request failed.", ephemeral=True)

    # Determine which battle to show
    war_config = war_data.get("data", {}).get("configData", {})
    active_battle_id = war_config.get("Title") or war_data.get("data", {}).get("configName")
    battles = clan_data.get("data", {}).get("Battles", {})

    battle_id = None
    if active_battle_id and active_battle_id in battles:
        battle_id = active_battle_id
    elif battles:
        battle_id = list(battles.keys())[-1]

    if not battle_id:
        return await interaction.followup.send("❌ No battle data found for MCWV.", ephemeral=True)

    battle = battles[battle_id]
    contributions = sorted(
        battle.get("PointContributions", []),
        key=lambda x: x.get("Points", 0),
        reverse=True
    )
    total_points = battle.get("Points", 0)

    if not contributions:
        return await interaction.followup.send("❌ No contribution data yet for this war.", ephemeral=True)

    # Resolve Roblox usernames for top 15
    top = contributions[:15]
    user_ids = [e["UserID"] for e in top]
    try:
        async with session.post(ROBLOX_USERS_API, json={"userIds": user_ids, "excludeBannedUsers": False}) as r:
            roblox_data = await r.json()
            id_to_name = {u["id"]: u["name"] for u in roblox_data.get("data", [])}
    except Exception:
        id_to_name = {}

    # Build Discord mention lookup from our DB (roblox_id -> discord_id)
    db_users = db_get_all()
    roblox_to_discord = {int(u[0]): u[1] for u in db_users}

    # Rank medals
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    top_points = top[0]["Points"] if top else 1

    lines = []
    for i, entry in enumerate(top, 1):
        uid = entry["UserID"]
        pts = entry.get("Points", 0)
        name = id_to_name.get(uid, f"Unknown ({uid})")
        medal = medals.get(i, f"`#{i:>2}`")
        bar_len = int((pts / top_points) * 10)
        bar = "█" * bar_len + "░" * (10 - bar_len)
        discord_mention = f" <@{roblox_to_discord[uid]}>" if uid in roblox_to_discord else ""
        lines.append(f"{medal} **{name}**{discord_mention}\n`{bar}` **{format_points(pts)}**")

    # Friendly battle name
    friendly = re.sub(r'(\d+)', r' \1', re.sub(r'([A-Z])', r' \1', battle_id)).strip()

    now = datetime.now(timezone.utc).timestamp()
    finish_ts = war_config.get("FinishTime")
    is_active = finish_ts and war_config.get("StartTime", 0) <= now <= finish_ts

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
    status_str = "⚔️ Active" if is_active else "🏁 Ended"
    embed.add_field(name="Status", value=status_str, inline=True)
    embed.set_footer(text=f"Showing top {len(top)} of {len(contributions)} • ps99.biggamesapi.io")
    await interaction.followup.send(embed=embed)

@bot.tree.command(name="mystats", description="Check your (or another member's) clan war contribution stats", guild=guild_obj)
async def mystats(interaction: discord.Interaction, member: discord.Member = None):
    await interaction.response.defer()

    target = member or interaction.user
    db_users = db_get_all()
    linked = next((u for u in db_users if u[1] == target.id), None)

    if not linked:
        return await interaction.followup.send(
            f"❌ {target.mention} is not linked to a Roblox account. Use `/add` first.",
            ephemeral=True
        )

    roblox_id = int(linked[0])
    roblox_name = linked[2]

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
    total_points = battle.get("Points", 0)

    user_entry = next((e for e in contributions if e["UserID"] == roblox_id), None)
    rank = next((i + 1 for i, e in enumerate(contributions) if e["UserID"] == roblox_id), None)

    friendly = re.sub(r'(\d+)', r' \1', re.sub(r'([A-Z])', r' \1', battle_id)).strip()
    now = datetime.now(timezone.utc).timestamp()
    finish_ts = war_config.get("FinishTime")
    is_active = finish_ts and war_config.get("StartTime", 0) <= now <= finish_ts
    color = discord.Color.red() if is_active else discord.Color.dark_gold()

    embed = discord.Embed(
        title=f"📊  {roblox_name}  —  {friendly}",
        color=color
    )

    if not user_entry:
        embed.description = "😴  **No contributions recorded yet for this war.**"
        embed.set_footer(text="Get in the game and start contributing!")
        return await interaction.followup.send(embed=embed)

    pts = user_entry["Points"]
    pct = (pts / total_points * 100) if total_points else 0
    top_pts = contributions[0]["Points"] if contributions else 1
    bar_len = int((pts / top_pts) * 20)
    bar = "█" * bar_len + "░" * (20 - bar_len)

    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    rank_display = medals.get(rank, f"#{rank}")

    embed.add_field(name="🏅  Rank", value=f"**{rank_display}** of {len(contributions)}", inline=True)
    embed.add_field(name="⚔️  Points", value=f"**{format_points(pts)}**", inline=True)
    embed.add_field(name="📈  Share", value=f"**{pct:.1f}%** of clan total", inline=True)
    embed.add_field(name="Progress vs #1", value=f"`{bar}`", inline=False)
    embed.add_field(name="🔢  Clan Total", value=f"**{format_points(total_points)}**", inline=True)
    status_str = "⚔️ Active" if is_active else "🏁 Ended"
    embed.add_field(name="War Status", value=status_str, inline=True)
    embed.set_footer(text=f"Roblox: {roblox_name} • ps99.biggamesapi.io")
    await interaction.followup.send(embed=embed)

@bot.tree.command(name="profile", description="View a member's full profile — Discord, Roblox, and clan war stats", guild=guild_obj)
async def profile(interaction: discord.Interaction, member: discord.Member = None):
    await interaction.response.defer()

    target   = member or interaction.user
    db_users = db_get_all()
    linked   = next((u for u in db_users if u[1] == target.id), None)

    if not linked:
        return await interaction.followup.send(
            f"❌ {target.mention} is not linked to a Roblox account.",
            ephemeral=True
        )

    roblox_id   = int(linked[0])
    roblox_name = linked[2]

    # --- fetch Roblox profile info + war data in parallel ---
    try:
        async with session.get(f"{ROBLOX_USERS_API}/{roblox_id}") as rr, \
                   session.get(PS99_API) as war_r, \
                   session.get(CLAN_API) as clan_r:
            roblox_ok = rr.status == 200
            roblox_profile = await rr.json() if roblox_ok else {}
            if war_r.status != 200 or clan_r.status != 200:
                return await interaction.followup.send("❌ Could not reach the PS99 API.", ephemeral=True)
            war_data  = await war_r.json()
            clan_data = await clan_r.json()
    except Exception:
        return await interaction.followup.send("❌ API request failed.", ephemeral=True)

    # --- Roblox profile details ---
    display_name  = roblox_profile.get("displayName", roblox_name)
    roblox_url    = f"https://www.roblox.com/users/{roblox_id}/profile"
    created_raw   = roblox_profile.get("created", "")
    joined_str    = ""
    if created_raw:
        try:
            created_dt = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
            joined_str = discord.utils.format_dt(created_dt, "D")
        except Exception:
            pass

    # --- current online status ---
    status_icons = {0: "⚫ Offline", 1: "🟢 Online", 2: "🎮 In Game", 3: "🔧 Studio"}
    rid_str      = str(roblox_id)
    current_status = status_icons.get(status_cache.get(rid_str, 0), "❓ Unknown")
    if status_cache.get(rid_str, 0) == 0 and rid_str in offline_since:
        current_status += f" — {format_duration(offline_since[rid_str])}"

    # --- war stats ---
    war_config       = war_data.get("data", {}).get("configData", {})
    active_battle_id = war_config.get("Title") or war_data.get("data", {}).get("configName")
    battles          = clan_data.get("data", {}).get("Battles", {})

    battle_id = None
    if active_battle_id and active_battle_id in battles:
        battle_id = active_battle_id
    elif battles:
        battle_id = list(battles.keys())[-1]

    now_ts    = datetime.now(timezone.utc).timestamp()
    finish_ts = war_config.get("FinishTime")
    is_active = bool(finish_ts and war_config.get("StartTime", 0) <= now_ts <= finish_ts)
    color     = discord.Color.red() if is_active else discord.Color.dark_gold()

    friendly = ""
    war_section = ""
    if battle_id:
        friendly = re.sub(r'(\d+)', r' \1', re.sub(r'([A-Z])', r' \1', battle_id)).strip()
        battle      = battles[battle_id]
        contributions = sorted(
            battle.get("PointContributions", []),
            key=lambda x: x.get("Points", 0),
            reverse=True
        )
        total_points = battle.get("Points", 0)
        user_entry   = next((e for e in contributions if e["UserID"] == roblox_id), None)
        rank         = next((i + 1 for i, e in enumerate(contributions) if e["UserID"] == roblox_id), None)

    # --- build embed ---
    embed = discord.Embed(
        title=f"👤  {target.display_name}",
        color=color
    )
    embed.set_thumbnail(url=target.display_avatar.url)

    # Discord identity
    embed.add_field(name="🏷️  Discord",  value=f"{target.mention}",                      inline=True)
    embed.add_field(name="🎮  Roblox",   value=f"[{display_name}]({roblox_url})",         inline=True)
    embed.add_field(name="📡  Status",   value=current_status,                            inline=True)
    if joined_str:
        embed.add_field(name="📅  Roblox Joined", value=joined_str,                       inline=True)

    # War stats section
    if battle_id and user_entry:
        pts      = user_entry["Points"]
        pct      = (pts / total_points * 100) if total_points else 0
        top_pts  = contributions[0]["Points"] if contributions else 1
        bar_len  = int((pts / top_pts) * 20)
        bar      = "█" * bar_len + "░" * (20 - bar_len)
        medals   = {1: "🥇", 2: "🥈", 3: "🥉"}
        rank_str = medals.get(rank, f"#{rank}") if rank else "—"
        war_label = ("⚔️ " if is_active else "🏁 ") + friendly

        embed.add_field(name="\u200b", value=f"─────────────── **{war_label}** ───────────────", inline=False)
        embed.add_field(name="🏅  Rank",    value=rank_str,               inline=True)
        embed.add_field(name="⚔️  Points",  value=format_points(pts),     inline=True)
        embed.add_field(name="📈  Share",   value=f"{pct:.1f}%",          inline=True)
        embed.add_field(name="Progress vs #1", value=f"`{bar}`",          inline=False)
    elif battle_id:
        war_label = ("⚔️ " if is_active else "🏁 ") + friendly
        embed.add_field(name="\u200b", value=f"─────────────── **{war_label}** ───────────────", inline=False)
        embed.add_field(name="⚔️  War Stats", value="😴 No contributions yet", inline=False)

    embed.set_footer(text=f"Roblox ID: {roblox_id} • roblox.com/users/{roblox_id}/profile")
    await interaction.followup.send(embed=embed)

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
        "Got it! Now please reply with any **alt account usernames** "
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

@bot.tree.command(name="kick", description="Remove a member from the clan and unlink their Roblox account", guild=guild_obj)
@require_role()
async def kick(interaction: discord.Interaction, member: discord.Member, reason: str = "No reason provided"):
    await interaction.response.defer(ephemeral=True)

    guild = interaction.guild

    # --- get their linked Roblox info before removing ---
    db_users = db_get_all()
    linked = next((u for u in db_users if u[1] == member.id), None)
    roblox_name = linked[2] if linked else "Not linked"
    roblox_id   = linked[0] if linked else None

    actions = []
    errors  = []

    # --- remove clan member role ---
    clan_role = guild.get_role(CLAN_MEMBER_ROLE_ID)
    if clan_role and clan_role in member.roles:
        try:
            await member.remove_roles(clan_role, reason=f"Kicked by {interaction.user}: {reason}")
            actions.append(f"✅ Removed role **{clan_role.name}**")
        except Exception as e:
            errors.append(f"❌ Could not remove role: {e}")
    elif clan_role:
        actions.append("ℹ️ Member did not have the clan role")

    # --- unlink from bot DB ---
    if linked:
        db_remove(member.id)
        actions.append(f"✅ Unlinked Roblox account **{roblox_name}**")
    else:
        actions.append("ℹ️ No Roblox account was linked")

    # --- clear from offline tracking ---
    if roblox_id:
        offline_since.pop(roblox_id, None)
        status_cache.pop(roblox_id, None)
        actions.append("✅ Removed from offline tracking")

    # --- log ---
    log_ch = guild.get_channel(LOG_CHANNEL_ID)
    if log_ch:
        log_embed = discord.Embed(
            title="🚫  Member Kicked",
            color=discord.Color.red(),
            timestamp=datetime.now(timezone.utc)
        )
        log_embed.add_field(name="Staff",      value=interaction.user.mention, inline=True)
        log_embed.add_field(name="Member",     value=member.mention,           inline=True)
        log_embed.add_field(name="Roblox",     value=roblox_name,              inline=True)
        log_embed.add_field(name="Reason",     value=reason,                   inline=False)
        log_embed.add_field(name="Actions",    value="\n".join(actions) or "—",inline=False)
        if errors:
            log_embed.add_field(name="⚠️ Errors", value="\n".join(errors), inline=False)
        log_embed.set_footer(text=f"Member ID: {member.id}" + (f" • Roblox ID: {roblox_id}" if roblox_id else ""))
        try:
            await log_ch.send(embed=log_embed)
        except Exception:
            pass

    summary = "\n".join(actions)
    if errors:
        summary += "\n\n⚠️ **Errors:**\n" + "\n".join(errors)
    await interaction.followup.send(f"**{member.display_name}** has been kicked.\n{summary}", ephemeral=True)

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

@bot.tree.command(name="status", description="Check Roblox status", guild=guild_obj)
@require_role()
async def status(interaction: discord.Interaction, member: discord.Member):

    users = db_get_all()

    target = next((u for u in users if u[1] == member.id), None)

    if not target:
        return await interaction.response.send_message("Not linked", ephemeral=True)

    rid = target[0]
    current = status_cache.get(rid, 0)
    status_icons = {0: "⚫", 1: "🟢", 2: "🎮", 3: "🔧"}
    icon = status_icons.get(current, "❓")
    extra = ""
    if current == 0 and rid in offline_since:
        since_dt = offline_since[rid]
        extra = f"\nOffline since {discord.utils.format_dt(since_dt, 'R')} ({format_duration(since_dt)})"

    await interaction.response.send_message(
        f"{icon} **{target[2]}** — {status_text(current)}{extra}",
        ephemeral=True
    )

# ---------------- ROBLOX LOOP (every 2 min — detects transitions) ----------------
@tasks.loop(minutes=2)
async def check_loop():

    users = db_get_all()
    if not users or not bot_enabled:
        return

    try:
        user_ids = [int(u[0]) for u in users]
        url = "https://presence.roblox.com/v1/presence/users"

        async with session.post(url, json={"userIds": user_ids}) as r:

            if r.status != 200:
                return

            data = await r.json()

            for u in data.get("userPresences", []):

                rid = str(u["userId"])
                current = u["userPresenceType"]
                old = status_cache.get(rid)

                status_cache[rid] = current

                if old is None or old == current:
                    continue

                if current == 0:
                    went_offline = datetime.now(timezone.utc)
                    offline_since[rid] = went_offline

                    if offline_ping_enabled:
                        info = next((x for x in users if x[0] == rid), None)
                        if info:
                            channel = await bot.fetch_channel(CHANNEL_ID)
                            await channel.send(
                                f"⚫ <@{info[1]}> **({info[2]})** just went offline — {discord.utils.format_dt(went_offline, 'R')}",
                                allowed_mentions=discord.AllowedMentions(users=True)
                            )
                else:
                    offline_since.pop(rid, None)

    except Exception as e:
        print("Loop error:", e)

# ---------------- REMINDER LOOP (every 30 min — re-pings offline users) ----------------
@tasks.loop(minutes=30)
async def reminder_loop():

    if not bot_enabled or not offline_ping_enabled:
        return

    try:
        channel = await bot.fetch_channel(reminder_channel_id)
    except:
        return

    users = db_get_all()
    now = datetime.now(timezone.utc)

    for roblox_id, discord_id, username in users:

        rid = str(roblox_id)
        status = status_cache.get(rid)

        # ONLY skip if IN GAME
        if status == 2:
            continue

        # if we don't know status yet, assume pingable
        if status is None:
            status = 0

        # get or set offline start time
        if rid not in offline_since:
            offline_since[rid] = now

        duration = format_duration(offline_since[rid])

        await channel.send(
            f"⚫ <@{discord_id}> **({username})** is not in game — {duration}",
            allowed_mentions=discord.AllowedMentions(users=True)
        )

# ---------------- PS99 WAR POLL (every 5 min — auto-detects clan wars) ----------------
@tasks.loop(minutes=5)
async def war_poll_loop():
    global bot_enabled, ps99_war_active, ps99_first_check

    try:
        async with session.get(PS99_API) as r:
            if r.status != 200:
                return

            data = await r.json()
            config = data.get("data", {}).get("configData", {})
            start = config.get("StartTime")
            finish = config.get("FinishTime")

            if start is None or finish is None:
                return

            now = datetime.now(timezone.utc).timestamp()
            currently_active = start <= now <= finish

            if ps99_first_check:
                ps99_first_check = False
                ps99_war_active = currently_active
                if currently_active:
                    bot_enabled = True
                    print("PS99 clan war already in progress — tracking enabled silently")
                return

            if currently_active and not ps99_war_active:
                ps99_war_active = True
                bot_enabled = True
                alert_channel = await bot.fetch_channel(CHANNEL_ID)
                await alert_channel.send("CLAN WAR TRACKING STARTED!! LETS GO MCWV!!!!!")
                print("PS99 clan war started — tracking auto-enabled")

                # immediately check who is already offline and ping them
                users = db_get_all()
                if users:
                    try:
                        user_ids = [int(u[0]) for u in users]
                        async with session.post(
                            "https://presence.roblox.com/v1/presence/users",
                            json={"userIds": user_ids}
                        ) as pr:
                            if pr.status == 200:
                                presences = (await pr.json()).get("userPresences", [])
                                now_dt = datetime.now(timezone.utc)
                                for p in presences:
                                    rid = str(p["userId"])
                                    status_cache[rid] = p["userPresenceType"]
                                    if p["userPresenceType"] == 0 and offline_ping_enabled:
                                        offline_since[rid] = now_dt
                                        info = next((u for u in users if u[0] == rid), None)
                                        if info:
                                            await alert_channel.send(
                                                f"⚫ <@{info[1]}> **({info[2]})** is already offline — {discord.utils.format_dt(now_dt, 'R')}",
                                                allowed_mentions=discord.AllowedMentions(users=True)
                                            )
                    except Exception as e:
                        print(f"War start offline check error: {e}")

            elif not currently_active and ps99_war_active:
                ps99_war_active = False
                bot_enabled = False
                offline_since.clear()
                status_cache.clear()
                channel = await bot.fetch_channel(CHANNEL_ID)
                await channel.send("CLAN WAR OVER. GG EVERYONE!!")
                print("PS99 clan war ended — tracking auto-disabled")

    except Exception as e:
        print("War poll error:", e)

# ---------------- CLAN LEAVE DETECTION (every 10 min) ----------------
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

    clan_member_ids = {int(m["UserID"]) for m in data.get("Members", []) if "UserID" in m}
    if not clan_member_ids:
        return

    guild = bot.get_guild(GUILD_ID)
    if not guild:
        return

    log_ch  = guild.get_channel(LOG_CHANNEL_ID)
    clan_role = guild.get_role(CLAN_MEMBER_ROLE_ID)

    for roblox_id, discord_id, roblox_name in users:
        if int(roblox_id) in clan_member_ids:
            continue

        # this user is tracked but no longer in the clan — auto-remove
        print(f"[clan_leave_loop] {roblox_name} ({roblox_id}) left the clan — auto-removing")

        # remove role
        try:
            member = guild.get_member(discord_id) or await guild.fetch_member(discord_id)
            if clan_role and member and clan_role in member.roles:
                await member.remove_roles(clan_role, reason="Left PS99 clan (auto-detected)")
        except Exception as e:
            print(f"[clan_leave_loop] Could not remove role for {roblox_name}: {e}")

        # unlink from DB and clear tracking
        db_remove(discord_id)
        offline_since.pop(roblox_id, None)
        status_cache.pop(roblox_id, None)

        # log
        if log_ch:
            embed = discord.Embed(
                title="🚪  Member Left Clan (Auto-Detected)",
                color=discord.Color.orange(),
                timestamp=datetime.now(timezone.utc)
            )
            embed.add_field(name="Roblox",    value=roblox_name,                                         inline=True)
            embed.add_field(name="Discord",   value=f"<@{discord_id}>",                                   inline=True)
            embed.add_field(name="Action",    value="Role removed • Roblox account unlinked",             inline=False)
            embed.set_footer(text=f"Roblox ID: {roblox_id} • Detected via PS99 clan API")
            try:
                await log_ch.send(embed=embed)
            except Exception:
                pass

# ---------------- CLEANUP ----------------
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
