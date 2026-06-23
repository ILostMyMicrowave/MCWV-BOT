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
import time

from discord import app_commands
from discord.ext import commands, tasks

from flask import Flask
from threading import Thread

session = None
status_cooldown = {}

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is alive"


def run_web():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

Thread(target=run_web, daemon=True).start()

import math

def mastery_gap(level: int) -> int:
    xp = math.floor(0.25 * math.floor(level + 300 * (2 ** (level / 7))))
    if level == 98:
        return 1228825
    return xp

def xp_to_level(total_xp: int) -> int:
    level = 1
    remaining = int(total_xp or 0)

    while level < 99:
        needed = mastery_gap(level)
        if remaining < needed:
            break
        remaining -= needed
        level += 1

    return level

PROFILE_CACHE = {}
CACHE_TTL = 60  # adjust if you want

PS99_API = "https://ps99.biggamesapi.io"


async def get_profile_bundle(session, user_id, force=False):
    now = time.time()

    # ---------------- CACHE ----------------
    if not force and user_id in PROFILE_CACHE:
        cached_data, expiry = PROFILE_CACHE[user_id]
        if now < expiry:
            return cached_data

    timeout = aiohttp.ClientTimeout(total=10)

    url = f"{PS99_API}/v1/players/{user_id}?include=profile,inventory,extendedProfile"

    try:
        async with session.get(url, timeout=timeout) as r:
            data = await r.json()

    except Exception as e:
        print(f"[get_profile_bundle] request failed for {user_id}: {e}")
        empty_bundle = ({}, {}, {}, {})
        return empty_bundle

    # ---------------- SAFE ROOT HANDLING ----------------
    root = data.get("data", {}) if isinstance(data, dict) else {}

    if not isinstance(root, dict):
        print(f"[get_profile_bundle] bad root for {user_id}: {root}")
        empty_bundle = ({}, {}, {}, {})
        return empty_bundle

    account = root.get("account", {}) if isinstance(root.get("account", {}), dict) else {}
    views = root.get("views", {}) if isinstance(root.get("views", {}), dict) else {}

    public_views = account.get("publicViews", {}) if isinstance(account, dict) else {}

    def extract_view(view_name: str):
        view = views.get(view_name, {})
        if not isinstance(view, dict):
            return {}

        if view.get("available") is True:
            data_block = view.get("data", {})
            return data_block if isinstance(data_block, dict) else {}

        return {}

    profile_data = extract_view("profile")
    inventory_data = extract_view("inventory")
    extended_data = extract_view("extendedProfile")

    bundle = (extended_data, profile_data, inventory_data, public_views)

    # ---------------- CACHE ONLY VALID DATA ----------------
    if profile_data or inventory_data or extended_data:
        PROFILE_CACHE[user_id] = (bundle, now + CACHE_TTL)

    return bundle
    
def ensure_db_connection():
    global conn

    try:
        if conn is None or conn.closed:
            print("🔄 Reconnecting to database...")

            conn = psycopg2.connect(
                DATABASE_URL,
                sslmode="require"
            )

            conn.autocommit = False

    except Exception as e:
        print("Database reconnect failed:", repr(e))
        raise

def db_get_all_alts():
    if not db_enabled():
        return []

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT discord_id, roblox_id, username
                FROM user_alts
            """)
            return cur.fetchall()
    except Exception as e:
        conn.rollback()
        print("db_get_all_alts error:", e)
        return []

def db_get_main_link(discord_id):
    if not db_enabled():
        return None

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT roblox_id, username
                FROM users
                WHERE discord_id = %s
            """, (int(discord_id),))
            return cur.fetchone()
    except Exception as e:
        conn.rollback()
        print("db_get_main_link error:", e)
        return None


def db_get_alts(discord_id):
    if not db_enabled():
        return []

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT roblox_id, username
                FROM user_alts
                WHERE discord_id = %s
                ORDER BY username
            """, (int(discord_id),))
            return cur.fetchall()
    except Exception as e:
        conn.rollback()
        print("db_get_alts error:", e)
        return []


def db_get_all_tracked():
    if not db_enabled():
        return []

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT roblox_id, discord_id, username
                FROM (
                    SELECT roblox_id, discord_id, username FROM users
                    UNION ALL
                    SELECT roblox_id, discord_id, username FROM user_alts
                ) t
                ORDER BY discord_id, username
            """)
            return cur.fetchall()
    except Exception as e:
        conn.rollback()
        print("db_get_all_tracked error:", e)
        return []


def db_find_roblox_link(roblox_id):
    if not db_enabled():
        return None

    rid = str(roblox_id).strip()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT discord_id, username
                FROM users
                WHERE roblox_id = %s
            """, (rid,))
            row = cur.fetchone()
            if row:
                return {
                    "kind": "main",
                    "discord_id": row[0],
                    "username": row[1],
                    "roblox_id": rid
                }

            cur.execute("""
                SELECT discord_id, username
                FROM user_alts
                WHERE roblox_id = %s
            """, (rid,))
            row = cur.fetchone()
            if row:
                return {
                    "kind": "alt",
                    "discord_id": row[0],
                    "username": row[1],
                    "roblox_id": rid
                }

    except Exception as e:
        conn.rollback()
        print("db_find_roblox_link error:", e)

    return None

def db_add_alt(discord_id, roblox_id, username):
    if not db_enabled():
        return False, "Database is not available."

    did = int(discord_id)
    rid = str(roblox_id).strip()
    uname = str(username).strip()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT discord_id
                FROM users
                WHERE roblox_id = %s
            """, (rid,))
            row = cur.fetchone()
            if row:
                if int(row[0]) == did:
                    return False, "That Roblox account is already the main account for this user."
                return False, f"That Roblox account is already linked to <@{row[0]}>."

            cur.execute("""
                SELECT discord_id
                FROM user_alts
                WHERE roblox_id = %s
            """, (rid,))
            row = cur.fetchone()
            if row:
                if int(row[0]) == did:
                    return False, "That Roblox account is already added as an alt for this user."
                return False, f"That Roblox account is already linked as an alt for <@{row[0]}>."

            cur.execute("""
                INSERT INTO user_alts (discord_id, roblox_id, username)
                VALUES (%s, %s, %s)
            """, (did, rid, uname))

        conn.commit()
        return True, f"Added **{uname}** as an alt."

    except Exception as e:
        print("db_add_alt error:", repr(e))
        try:
            conn.rollback()
        except Exception as rollback_error:
            print("rollback failed:", repr(rollback_error))
        return False, f"Database error: {e}"


def db_remove_alt(discord_id, alt_value):
    if not db_enabled():
        return False, None, "Database is not available."

    did = int(discord_id)
    alt_clean = str(alt_value).strip()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM user_alts
                WHERE discord_id = %s
                  AND (
                        LOWER(username) = LOWER(%s)
                        OR roblox_id = %s
                  )
                RETURNING roblox_id, username
            """, (did, alt_clean, alt_clean))
            row = cur.fetchone()

        conn.commit()

        if not row:
            return False, None, "Alt not found."

        return True, row[0], f"Removed **{row[1]}**."

    except Exception as e:
        conn.rollback()
        print("db_remove_alt error:", e)
        return False, None, "Failed to remove alt."


def db_remove_all_links_for_discord(discord_id):
    if not db_enabled():
        return

    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM user_alts WHERE discord_id = %s", (int(discord_id),))
            cur.execute("DELETE FROM users WHERE discord_id = %s", (int(discord_id),))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print("db_remove_all_links_for_discord error:", e)

status_cache = {}
status_cache_time = {}
offline_since = {}
offline_ping_enabled = True
reminder_interval = 30
reminder_channel_id = None

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

from PIL import Image, ImageDraw, ImageFont, ImageFilter
from io import BytesIO
import random
import math

def load_fonts():
    try:
        return {
            "title": ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 52),
            "big":   ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 34),
            "small": ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 24),
        }
    except:
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

async def fetch_roblox_avatar(session, user_id):
    try:
        url = (
            "https://thumbnails.roblox.com/v1/users/avatar-headshot"
            f"?userIds={user_id}&size=720x720&format=Png&isCircular=false"
        )

        async with session.get(url) as r:
            data = await r.json()

        image_url = data["data"][0]["imageUrl"]

        async with session.get(image_url) as r:
            avatar_bytes = await r.read()

        avatar = Image.open(BytesIO(avatar_bytes)).convert("RGBA")
        avatar = avatar.resize((220, 220), Image.Resampling.LANCZOS)

        mask = Image.new("L", (220, 220), 0)
        ImageDraw.Draw(mask).ellipse((0, 0, 220, 220), fill=255)
        avatar.putalpha(mask)

        return avatar

    except Exception as e:
        print("Avatar fetch error:", e)
        return None

async def generate_profile_card(
    *,
    session,
    roblox_name,
    roblox_id,
    discord_tag,
    points,
    rank,
    top_points=1,
    animated=True,
    bar_progress=0.0
):
    WIDTH, HEIGHT = 1400, 600
    frames = []

    fonts = load_fonts()
    title_font = fonts["title"]
    big_font = fonts["big"]
    small_font = fonts["small"]

    particles = generate_particles(25, WIDTH, HEIGHT)
    avatar = await fetch_roblox_avatar(session, roblox_id)

    if avatar:
        avatar = avatar.resize((220, 220))

    for frame in range(8 if animated else 1):

        img = Image.new("RGBA", (WIDTH, HEIGHT))
        draw = ImageDraw.Draw(img)

        # -------- FAST ANIMATED GRADIENT --------
        shift = math.sin(frame * 0.35) * 80
        shift2 = math.cos(frame * 0.25) * 60

        step = 10  # bigger = faster, slightly less smooth

        for y in range(0, HEIGHT, step):
            ny = (y + shift2) / HEIGHT

            r_base = 20 + 60 * abs(math.sin(ny * 3.14 + frame * 0.2))
            g_base = 25 + 50 * abs(math.sin(ny * 3.14 + frame * 0.15))
            b_base = 60 + 120 * abs(math.sin(ny * 3.14 + frame * 0.25))

            color = (
                int(r_base),
                int(g_base),
                int(b_base)
            )

            draw.rectangle(
                [0, y, WIDTH, y + step],
                fill=color
            )

        # -------- PARTICLES --------
        for i, (px, py, size) in enumerate(particles):
            offset = math.sin(frame * 0.6 + i) * 2 if animated else 0

            draw.ellipse(
                [
                    px + offset,
                    py + offset,
                    px + size + offset,
                    py + size + offset
                ],
                fill=(120, 160, 255, 120)
            )

        # panel
        draw.rounded_rectangle(
            [40, 40, WIDTH - 40, HEIGHT - 40],
            radius=35,
            fill=(20, 22, 30)
        )

        # avatar
        if avatar:
            img.paste(avatar, (80, 90), avatar)

        # NAME
        draw.text((352, 72), roblox_name, fill=(80, 140, 255), font=title_font)
        draw.text((350, 70), roblox_name, fill="white", font=title_font)

        # info block
        draw.text((355, 175), discord_tag, fill=(200, 200, 200), font=small_font)
        draw.text((355, 215), f"Roblox ID: {roblox_id}", fill=(160, 160, 160), font=small_font)

        # rank
        draw.text(
            (355, 255),
            f"Rank: #{rank}" if rank else "Rank: Unranked",
            fill=(255, 220, 120) if rank else (150, 150, 150),
            font=small_font
        )

        # -------- WAR BAR --------
        bar_x, bar_y = 350, 340
        bar_w, bar_h = 800, 45

        top_points = max(int(top_points or 1), 1)
        points = max(int(points or 0), 0)

        progress = max(0.0, min(bar_progress, 1.0))
        filled = int(bar_w * progress)

        # background
        draw.rounded_rectangle(
            [bar_x, bar_y, bar_x + bar_w, bar_y + bar_h],
            radius=18,
            fill=(40, 40, 55)
        )

        # fill
        if filled > 0:
            draw.rounded_rectangle(
                [bar_x, bar_y, bar_x + filled, bar_y + bar_h],
                radius=18,
                fill=(90, 140, 255)
            )

        # labels
        draw.text((350, 300), "WAR PROGRESS", fill=(180, 180, 180), font=small_font)
        draw.text((bar_x + bar_w - 90, bar_y), f"{int(progress * 100)}%", fill="white", font=small_font)
        draw.text((355, 410), f"{points:,} POINTS", fill="white", font=big_font)

        frames.append(img)

    buffer = BytesIO()

    frames[0].save(
        buffer,
        format="GIF",
        save_all=True,
        append_images=frames[1:],
        duration=90,
        loop=0,
        disposal=2
    )

    buffer.seek(0)
    return buffer
    
class ListView(discord.ui.View):
    def __init__(self, users, alts_by_discord):
        super().__init__(timeout=300)
        self.users = users
        self.alts_by_discord = alts_by_discord
        self.mode = "ingame"

    @staticmethod
    def _status_icon(status: int) -> str:
        return {
            0: "⚫",
            1: "🟢",
            2: "🎮",
            3: "🔧",
        }.get(int(status), "❓")

    @staticmethod
    def _status_label(status: int) -> str:
        return {
            0: "Offline",
            1: "Online",
            2: "In Game",
            3: "Studio",
        }.get(int(status), "Unknown")

    def _status_for(self, roblox_id) -> int:
        return int(status_cache.get(str(roblox_id).strip(), 0) or 0)

    def _matches_mode(self, status: int) -> bool:
        if self.mode == "all":
            return True
        if self.mode == "ingame":
            return status == 2
        if self.mode == "offline":
            return status == 0
        return True

    def _mode_title(self) -> str:
        return {
            "all": "📋 Tracked Members",
            "ingame": "🎮 In Game Members",
            "offline": "⚫ Offline Members",
        }.get(self.mode, "📋 Tracked Members")

    def _append_section(self, parts: list[str], title: str, lines: list[str]):
        if not lines:
            return
        parts.append(f"__{title}__")
        parts.extend(lines)
        parts.append("")

    def build_embed(self) -> discord.Embed:
        main_sections = {0: [], 1: [], 2: [], 3: []}
        alt_sections = {0: [], 1: [], 2: [], 3: []}
        shown_alts = 0

        for roblox_id, discord_id, username in self.users:
            main_status = self._status_for(roblox_id)
            alts = self.alts_by_discord.get(int(discord_id), [])

            if self._matches_mode(main_status):
                main_sections[main_status].append(
                    f"{self._status_icon(main_status)} <@{discord_id}> — **{username}**"
                )

            for alt_roblox_id, alt_username in alts:
                alt_status = self._status_for(alt_roblox_id)
                if self._matches_mode(alt_status):
                    shown_alts += 1
                    alt_sections[alt_status].append(
                        f"{self._status_icon(alt_status)} **{alt_username}** — <@{discord_id}>"
                    )

        order = [2, 1, 3, 0] if self.mode == "all" else ([2] if self.mode == "ingame" else [0])

        parts: list[str] = []

        if self.mode == "all":
            for status in order:
                self._append_section(
                    parts,
                    f"{self._status_label(status)} Members",
                    main_sections[status]
                )
        else:
            status = order[0]
            self._append_section(
                parts,
                f"{self._status_label(status)} Members",
                main_sections[status]
            )

        alt_has_content = any(alt_sections[s] for s in order)
        if alt_has_content:
            parts.append("**Alts**")
            for status in order:
                self._append_section(
                    parts,
                    f"{self._status_label(status)} Alts",
                    alt_sections[status]
                )
            if parts and parts[-1] == "":
                parts.pop()

        description = "\n".join(parts).strip() or "None"
        if len(description) > 3900:
            description = description[:3890].rstrip() + "\n…"

        embed = discord.Embed(
            title=self._mode_title(),
            description=description,
            color=discord.Color.blurple()
        )
        embed.set_footer(text=f"{len(self.users)} tracked members • {shown_alts} alts shown")
        return embed

    @discord.ui.button(label="🎮 In Game", style=discord.ButtonStyle.success)
    async def show_ingame(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.mode = "ingame"
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(label="📋 All", style=discord.ButtonStyle.primary)
    async def show_all_members(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.mode = "all"
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(label="⚫ Offline", style=discord.ButtonStyle.secondary)
    async def show_offline(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.mode = "offline"
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

LEADERBOARD_PAGE_SIZE = 10


def chunk_list(items, size=100):
    for i in range(0, len(items), size):
        yield items[i:i + size]


class LeaderboardView(discord.ui.View):
    def __init__(self, entries, battle_title, total_points, is_active):
        super().__init__(timeout=300)
        self.entries = entries
        self.battle_title = battle_title
        self.total_points = total_points
        self.is_active = is_active
        self.page = 0
        self.max_points = max((e["points"] for e in entries), default=1) or 1

    def _total_pages(self) -> int:
        return max(1, (len(self.entries) + LEADERBOARD_PAGE_SIZE - 1) // LEADERBOARD_PAGE_SIZE)

    def _page_slice(self):
        start = self.page * LEADERBOARD_PAGE_SIZE
        end = start + LEADERBOARD_PAGE_SIZE
        return self.entries[start:end], start, end

    def _build_line(self, entry: dict) -> str:
        rank = entry["rank"]
        uid = entry["user_id"]
        name = entry["name"]
        pts = entry["points"]
        discord_id = entry.get("discord_id")

        medals = {1: "🥇", 2: "🥈", 3: "🥉"}
        medal = medals.get(rank, f"`#{rank:>2}`")

        bar_len = int((pts / self.max_points) * 10) if self.max_points > 0 else 0
        bar_len = max(0, min(10, bar_len))
        bar = "█" * bar_len + "░" * (10 - bar_len)

        profile_url = f"https://www.roblox.com/users/{uid}/profile"
        safe_name = discord.utils.escape_markdown(str(name))
        roblox_display = f"[{safe_name}]({profile_url})"

        discord_part = f" • <@{discord_id}>" if discord_id else ""

        return f"{medal} {roblox_display}{discord_part}\n`{bar}` **{format_points(pts)}**"

    def build_embed(self) -> discord.Embed:
        page_entries, start, end = self._page_slice()
        lines = [self._build_line(entry) for entry in page_entries]

        embed = discord.Embed(
            title=f"🏆 {CLAN_NAME} — {self.battle_title}",
            description="\n\n".join(lines) if lines else "No entries on this page.",
            color=discord.Color.red() if self.is_active else discord.Color.dark_gold()
        )

        embed.add_field(
            name="🔢 Total Clan Points",
            value=f"**{format_points(self.total_points)}**",
            inline=True
        )

        embed.add_field(
            name="👥 Contributors",
            value=f"**{len(self.entries)}**",
            inline=True
        )

        embed.add_field(
            name="📄 Page",
            value=f"**{self.page + 1}/{self._total_pages()}**",
            inline=True
        )

        embed.set_footer(
            text=f"Showing {start + 1}-{min(end, len(self.entries))} of {len(self.entries)} • ps99.biggamesapi.io"
        )
        return embed

    async def _move_page(self, interaction: discord.Interaction, delta: int):
        total_pages = self._total_pages()
        self.page = (self.page + delta) % total_pages
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(label="◀ Prev", style=discord.ButtonStyle.secondary)
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._move_page(interaction, -1)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.secondary)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._move_page(interaction, 1)

    @discord.ui.button(label="🔄 Refresh", style=discord.ButtonStyle.primary)
    async def refresh_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

# ---------------- CONFIG ----------------
TOKEN = os.environ.get("DISCORD_TOKEN")

GUILD_ID                  = 1501608673250640055
CHANNEL_ID                = 1514663069639245904
ALLOWED_ROLE_ID           = 1501986357516701827  # staff role (run commands)
CLAN_MEMBER_ROLE_ID       = 1501986780667314246  # given on accept
CLAN_MEMBERS_CATEGORY_ID  = 1503109089931034785  # ticket moved here on accept
MEMBERS_CHANNEL_ID        = 1509276380674789617  # membership record posted here
LOG_CHANNEL_ID            = 1502001938705682622  # accept/action log
PS99_API                  = "https://ps99.biggamesapi.io"
CLAN_NAME                 = "MCWV"
CLAN_API                  = f"https://ps99.biggamesapi.io/api/clan/{CLAN_NAME}"
ROBLOX_USERS_API          = "https://users.roblox.com/v1/users"


if not TOKEN:
    raise ValueError("Missing DISCORD_TOKEN")

guild_obj = discord.Object(id=GUILD_ID)

# ---------------- BOT ----------------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!!", intents=intents)
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

def db_enabled():
    return conn is not None

if DATABASE_URL:
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        print("Database connected")

        with conn.cursor() as cur:

            # ---------------- MAIN USERS ----------------
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    roblox_id TEXT PRIMARY KEY,
                    discord_id BIGINT,
                    username TEXT
                )
            """)

            # ---------------- SETTINGS ----------------
            cur.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)

            # ---------------- STATUS CACHE ----------------
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_status (
                    roblox_id TEXT PRIMARY KEY,
                    status INTEGER,
                    updated_at TIMESTAMP
                )
            """)

            # ---------------- ALTS SYSTEM (NEW) ----------------
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_alts (
                    discord_id BIGINT NOT NULL,
                    roblox_id TEXT NOT NULL,
                    username TEXT NOT NULL,
                    added_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (discord_id, roblox_id)
                )
            """)

        conn.commit()

    except Exception as e:
        print("DB connection failed:", e)
        conn = None

else:
    print("DATABASE_URL not set - running without DB")


def db_add(roblox_id, discord_id, username):
    if not db_enabled():
        return False, "Database is not available."

    global conn

    try:
        ensure_db_connection()

        rid = str(roblox_id).strip()
        did = int(discord_id)
        uname = str(username).strip()

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (roblox_id, discord_id, username)
                VALUES (%s, %s, %s)
                ON CONFLICT (roblox_id)
                DO UPDATE SET
                    discord_id = EXCLUDED.discord_id,
                    username = EXCLUDED.username
            """, (rid, did, uname))

        conn.commit()
        return True, f"Linked {uname}"

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass

        error = f"{type(e).__name__}: {e}"
        print("db_add error:", error)

        return False, error

def db_remove(did):
    if not db_enabled():
        return

    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM users
                WHERE discord_id = %s
            """, (int(did),))
        conn.commit()
    except Exception as e:
        print("db_remove error:", e)
        conn.rollback()


def db_get_all():
    if not db_enabled():
        return []

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT roblox_id, discord_id, username
                FROM users
            """)
            rows = cur.fetchall()

        return rows
    except Exception as e:
        print("db_get_all error:", e)
        return []


def db_get_setting(key, default=None):
    if not db_enabled():
        return default

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT value
                FROM settings
                WHERE key = %s
            """, (str(key),))
            row = cur.fetchone()

        return row[0] if row else default
    except Exception as e:
        print("db_get_setting error:", e)
        return default


def db_set_setting(key, value):
    if not db_enabled():
        return

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO settings (key, value)
                VALUES (%s, %s)
                ON CONFLICT (key)
                DO UPDATE SET value = EXCLUDED.value
            """, (str(key), str(value)))
        conn.commit()
    except Exception as e:
        print("db_set_setting error:", e)
        conn.rollback()


def db_set_user_status(rid, status):
    if not db_enabled():
        return

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO user_status (roblox_id, status, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (roblox_id)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    updated_at = NOW()
            """, (str(rid).strip(), int(status)))
        conn.commit()
    except Exception as e:
        print("db_set_user_status error:", e)
        conn.rollback()


def db_get_user_status(rid):
    if not db_enabled():
        return None

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT status
                FROM user_status
                WHERE roblox_id = %s
            """, (str(rid).strip(),))
            row = cur.fetchone()

        return row[0] if row else None
    except Exception as e:
        print("db_get_user_status error:", e)
        return None

def db_get_all_alts():
    if not db_enabled():
        return []

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT discord_id, roblox_id, username
                FROM user_alts
            """)
            return cur.fetchall()
    except Exception as e:
        print("db_get_all_alts error:", e)
        return []
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

        valid = 0

        for user_id in users:
            bundle = PROFILE_CACHE.get(user_id)
            if not bundle:
                continue

            extended, profile, inventory, public_views = bundle[0]

            if profile or inventory or extended:
                valid += 1

        await interaction.response.send_message(
            f"DB OK: {len(users)} users\nValid profiles: {valid}",
            ephemeral=True
        )

    except Exception as e:
        await interaction.response.send_message(f"DB ERROR: {e}", ephemeral=True)

@bot.tree.command(name="ping", description="Test command", guild=guild_obj)
@require_role()
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("pong")

@bot.tree.command(name="add", description="Link Roblox user", guild=guild_obj)
@require_role()
async def add(interaction: discord.Interaction, member: discord.Member, roblox_username: str):
    await interaction.response.defer(ephemeral=True)

    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession()

    url = "https://users.roblox.com/v1/usernames/users"

    try:
        async with session.post(
            url,
            json={
                "usernames": [roblox_username.strip()],
                "excludeBannedUsers": False
            }
        ) as r:
            if r.status != 200:
                return await interaction.followup.send(
                    f"❌ Roblox API error (HTTP {r.status}).",
                    ephemeral=True
                )

            data = await r.json()

        results = data.get("data", [])
        if not results:
            return await interaction.followup.send(
                "❌ Roblox user not found.",
                ephemeral=True
            )

        rid = str(results[0]["id"]).strip()
        name = str(results[0]["name"]).strip()

        ok, msg = db_add(rid, member.id, name)
        if not ok:
            return await interaction.followup.send(
                f"❌ {msg}",
                ephemeral=True
            )

        await interaction.followup.send(
            f"✅ Linked {member.mention} → **{name}**",
            ephemeral=True
        )

    except Exception as e:
        print("Roblox API error:", repr(e))
        try:
            conn.rollback()
        except Exception:
            pass

        await interaction.followup.send(
            f"❌ Roblox API error: `{type(e).__name__}: {e}`",
            ephemeral=True
        )

@bot.tree.command(
    name="list",
    description="Show all tracked users",
    guild=guild_obj
)
@require_role()
async def list_users(interaction: discord.Interaction):
    try:
        await interaction.response.defer(ephemeral=True)

        users = db_get_all()
        if not users:
            return await interaction.followup.send(
                "No users are being tracked.",
                ephemeral=True
            )

        alts_rows = db_get_all_alts()
        alts_by_discord = {}

        for discord_id, roblox_id, username in alts_rows:
            alts_by_discord.setdefault(int(discord_id), []).append(
                (str(roblox_id).strip(), str(username))
            )

        view = ListView(users, alts_by_discord)

        await interaction.followup.send(
            embed=view.build_embed(),
            view=view,
            ephemeral=True
        )

    except Exception as e:
        print("[LIST ERROR]", repr(e))
        if not interaction.response.is_done():
            await interaction.response.send_message(
                "❌ List command failed.",
                ephemeral=True
            )
            
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

@bot.tree.command(name="setreminderchannel", description="Set the channel where offline reminders are sent", guild=guild_obj)
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

    # ---------------- CURRENT WAR ----------------
    battle_id, battle = get_current_war(war_data, clan_data)

    if not battle:
        return await interaction.followup.send(
            "❌ Could not determine current war.",
            ephemeral=True
        )

    # ---------------- TIMING (BATTLE FIRST, CONFIG FALLBACK) ----------------
    war_config = war_data.get("data", {}).get("configData", {})

    start_ts = battle.get("StartTime")
    finish_ts = battle.get("FinishTime")

    if not start_ts or not finish_ts:
        start_ts = start_ts or war_config.get("StartTime")
        finish_ts = finish_ts or war_config.get("FinishTime")

    if not start_ts or not finish_ts:
        return await interaction.followup.send(
            "❌ War timing data missing.",
            ephemeral=True
        )

    now = datetime.now(timezone.utc).timestamp()
    total_duration = max(finish_ts - start_ts, 1)
    elapsed = max(0, now - start_ts)
    progress = max(0.0, min(1.0, elapsed / total_duration))

    start_dt = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    finish_dt = datetime.fromtimestamp(finish_ts, tz=timezone.utc)

    # Friendly war name
    friendly_name = re.sub(
        r'(\d+)',
        r' \1',
        re.sub(r'([A-Z])', r' \1', str(battle_id))
    ).strip()

    # ---------------- CONTRIBUTIONS ----------------
    contributions = sorted(
        battle.get("PointContributions", []),
        key=lambda x: x.get("Points", 0),
        reverse=True
    )

    total_points = battle.get("Points", 0)
    contributor_count = len(contributions)

    # ---------------- TOP CONTRIBUTOR ----------------
    top_name = "Unknown"
    top_discord = "Not linked"
    top_points = 0

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
        status_line = "⏳ UPCOMING"
        color = discord.Color.gold()
        bar = "`" + "░" * 20 + "`"
        time_field = f"Starts {discord.utils.format_dt(start_dt, 'R')}"

    elif now > finish_ts:
        status_line = "🏁 WAR ENDED"
        color = discord.Color.dark_gray()
        bar = "`" + "█" * 20 + "`"
        time_field = f"Ended {discord.utils.format_dt(finish_dt, 'R')}"

    else:
        status_line = "⚔️ ACTIVE — IN PROGRESS"
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
        description=f"**{status_line}**",
        color=color
    )

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

    embed.add_field(
        name="⏱ Time",
        value=time_field,
        inline=False
    )

    embed.add_field(
        name="🥇 Top Contributor",
        value=f"**{top_name}**\n{top_discord}\n**{format_points(top_points)} pts**",
        inline=True
    )

    embed.add_field(
        name="🔢 Clan Total",
        value=f"**{format_points(total_points)} pts**",
        inline=True
    )

    embed.add_field(
        name="👥 Contributors",
        value=f"**{contributor_count}**",
        inline=True
    )

    embed.set_footer(text="Data from ps99.biggamesapi.io • Updates every 5 min")

    await interaction.followup.send(embed=embed)
    
@bot.tree.command(
    name="leaderboard",
    description="Show MCWV clan war contribution leaderboard",
    guild=guild_obj
)
async def leaderboard(interaction: discord.Interaction):
    await interaction.response.defer()

    try:
        global session

        if session is None or session.closed:
            session = aiohttp.ClientSession()

        timeout = aiohttp.ClientTimeout(total=15)

        async with session.get(PS99_API, timeout=timeout) as war_r:
            if war_r.status != 200:
                return await interaction.followup.send(
                    "❌ Could not reach the PS99 war API.",
                    ephemeral=True
                )
            war_data = await war_r.json()

        async with session.get(CLAN_API, timeout=timeout) as clan_r:
            if clan_r.status != 200:
                return await interaction.followup.send(
                    "❌ Could not reach the PS99 clan API.",
                    ephemeral=True
                )
            clan_data = await clan_r.json()

        war_config = war_data.get("data", {}).get("configData", {})
        battle_id, battle = get_current_war(war_data, clan_data)

        if not battle:
            return await interaction.followup.send(
                "❌ No battle data found for MCWV.",
                ephemeral=True
            )

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

        # ---------------- ROBLOX USERNAME LOOKUP FOR ALL CONTRIBUTORS ----------------
        user_ids = []
        seen_ids = set()

        for entry in contributions:
            uid = entry.get("UserID")
            if uid is None:
                continue
            try:
                uid_int = int(uid)
            except Exception:
                continue

            if uid_int not in seen_ids:
                seen_ids.add(uid_int)
                user_ids.append(uid_int)

        id_to_name = {}
        try:
            for chunk in chunk_list(user_ids, 100):
                async with session.post(
                    ROBLOX_USERS_API,
                    json={
                        "userIds": chunk,
                        "excludeBannedUsers": False
                    },
                    timeout=timeout
                ) as r:
                    if r.status != 200:
                        continue

                    roblox_data = await r.json()
                    for u in roblox_data.get("data", []):
                        try:
                            uid = int(u["id"])
                            uname = str(u.get("name", f"Unknown ({uid})"))
                            id_to_name[uid] = uname
                        except Exception:
                            continue
        except Exception as e:
            print("[LEADERBOARD ROBLOX NAME ERROR]", repr(e))

        # ---------------- DISCORD LOOKUP ----------------
        tracked_rows = db_get_all_tracked()
        roblox_to_discord = {}
        for row in tracked_rows:
            try:
                rid = int(row[0])
                did = int(row[1])
                roblox_to_discord[rid] = did
            except Exception:
                continue

        battle_name = re.sub(
            r'(\d+)',
            r' \1',
            re.sub(r'([A-Z])', r' \1', str(battle_id))
        ).strip()

        now = datetime.now(timezone.utc).timestamp()
        finish_ts = war_config.get("FinishTime")
        start_ts = war_config.get("StartTime", 0)

        is_active = False
        if finish_ts:
            is_active = start_ts <= now <= finish_ts

        entries = []
        for rank, entry in enumerate(contributions, start=1):
            uid = entry.get("UserID")
            if uid is None:
                continue

            try:
                uid_int = int(uid)
            except Exception:
                continue

            pts = int(entry.get("Points", 0) or 0)
            name = id_to_name.get(uid_int, f"Unknown ({uid_int})")
            discord_id = roblox_to_discord.get(uid_int)

            entries.append({
                "rank": rank,
                "user_id": uid_int,
                "name": name,
                "points": pts,
                "discord_id": discord_id
            })

        if not entries:
            return await interaction.followup.send(
                "❌ No valid leaderboard entries found.",
                ephemeral=True
            )

        view = LeaderboardView(
            entries=entries,
            battle_title=battle_name,
            total_points=total_points,
            is_active=is_active
        )

        await interaction.followup.send(
            embed=view.build_embed(),
            view=view
        )

    except Exception as e:
        print("[LEADERBOARD ERROR]", repr(e))
        await interaction.followup.send(
            f"❌ Leaderboard failed.\n```{type(e).__name__}: {e}```",
            ephemeral=True
        )
    
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
            value=f"🏅 You outrank **{top_percent:.1f}%** of the clan",
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

class ProfileView(discord.ui.View):
    def __init__(self, extended_data, inventory_data, profile_data, roblox_name, public_views, roblox_id, session):
        super().__init__(timeout=120)

        self.extended = extended_data or {}
        self.inventory = inventory_data or {}
        self.profile = profile_data or {}
        self.roblox_name = roblox_name
        self.roblox_id = roblox_id
        self.public_views = public_views or {}
        self.session = session

    def _unwrap(self, data):
        if isinstance(data, dict) and isinstance(data.get("data"), dict):
            return data["data"]
        return data if isinstance(data, dict) else {}

    def _fmt(self, value):
        if isinstance(value, int):
            return f"{value:,}"
        return str(value)

    def _format_playtime(self, seconds):
        try:
            seconds = int(seconds or 0)
        except Exception:
            return "0h"

        days = seconds // 86400
        hours = (seconds % 86400) // 3600

        if days > 0:
            return f"{days}d {hours}h"
        if hours > 0:
            return f"{hours}h"
        return "0h"

    def _split_text(self, text, size=1900):
        return [text[i:i + size] for i in range(0, len(text), size)]

    # ---------------- PROFILE STATS BUTTON ----------------
    @discord.ui.button(label="💰 Profile Stats", style=discord.ButtonStyle.green)
    async def stats_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.public_views.get("extendedProfile", False):
            return await interaction.response.send_message(
                "🔒 Extended profile is private for this user.",
                ephemeral=True
            )

        extended = self._unwrap(self.extended)

        robux_spent = extended.get("RobuxSpent", 0)
        gamepasses = extended.get("Gamepasses", {})
        products = extended.get("Products", {})

        owned_passes = (
            [k for k, v in gamepasses.items() if v]
            if isinstance(gamepasses, dict)
            else []
        )

        product_text = "None"
        if isinstance(products, dict):
            lines = []
            for k, v in list(products.items())[:10]:
                if isinstance(v, dict):
                    lines.append(f"• {k} ×{v.get('count', 0)}")
                else:
                    lines.append(f"• {k}")
            product_text = "\n".join(lines) or "None"

        embed = discord.Embed(
            title=f"💰 Extended Stats — {self.roblox_name}",
            color=discord.Color.gold()
        )

        embed.add_field(name="💸 Robux Spent", value=self._fmt(robux_spent), inline=True)
        embed.add_field(
            name="🎟️ Gamepasses",
            value="\n".join(f"✔ {g}" for g in owned_passes) or "None",
            inline=False
        )
        embed.add_field(
            name="🧾 Products",
            value=product_text[:1024],
            inline=False
        )

        await interaction.response.send_message(embed=embed, ephemeral=True)

    # ---------------- INVENTORY BUTTON ----------------
    @discord.ui.button(label="🎒 Inventory", style=discord.ButtonStyle.blurple)
    async def inventory_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.public_views.get("inventory", False):
            return await interaction.response.send_message(
                "🔒 Inventory is private for this user.",
                ephemeral=True
            )

        inv = self._unwrap(self.inventory)
        equipped = inv.get("equipped", {})

        pets = equipped.get("pets", {}).get("list", [])
        enchants = equipped.get("enchants", {}).get("list", [])

        if not isinstance(pets, list):
            pets = []
        if not isinstance(enchants, list):
            enchants = []

        pets_text = "\n".join(
            f"🐾 {p.get('displayName', p.get('id', 'Unknown'))}"
            for p in pets
            if isinstance(p, dict)
        ) or "No pets equipped."

        enchants_text = "\n".join(
            f"✨ {e.get('displayName', e.get('id', 'Unknown'))} (Lvl {e.get('level', 0)})"
            for e in enchants
            if isinstance(e, dict)
        ) or "None"

        hoverboard = equipped.get("hoverboard", {})
        ultimate = equipped.get("ultimate", {})
        booth = equipped.get("booth", {})

        hoverboard_name = hoverboard.get("displayName", "None") if isinstance(hoverboard, dict) else "None"
        ultimate_name = ultimate.get("displayName", "None") if isinstance(ultimate, dict) else "None"
        booth_name = booth.get("displayName", "None") if isinstance(booth, dict) else "None"

        embed = discord.Embed(
            title=f"🎒 Inventory — {self.roblox_name}",
            color=discord.Color.blue()
        )

        embed.add_field(name="🐾 Equipped Pets", value=pets_text[:1024], inline=False)
        embed.add_field(name="⚡ Equipped Enchants", value=enchants_text[:1024], inline=False)
        embed.add_field(name="🛹 Hoverboard", value=hoverboard_name, inline=True)
        embed.add_field(name="⚡ Ultimate", value=ultimate_name, inline=True)
        embed.add_field(name="🏪 Booth", value=booth_name, inline=True)

        await interaction.response.send_message(embed=embed, ephemeral=True)

    # ---------------- GAME STATS BUTTON ----------------
    @discord.ui.button(label="🎖 Game Stats", style=discord.ButtonStyle.gray)
    async def rank_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.public_views.get("profile", False):
            return await interaction.response.send_message(
                "🔒 Profile stats are private for this user.",
                ephemeral=True
            )

        profile = self._unwrap(self.profile)

        stats = profile.get("Statistics", {})
        mastery = profile.get("Mastery", {})

        rank = profile.get("Rank", "Unknown")
        rebirths = profile.get("Rebirths", "Unknown")

        eggs = stats.get("Eggs Opened", 0)
        playtime = stats.get("Playtime", 0)
        huge = stats.get("Huge Pets Opened", 0)
        login = stats.get("Login Count", 0)

        mastery_lines = []

        for name, xp in list(mastery.items())[:8]:
            try:
                level = xp_to_level(int(xp))
                mastery_lines.append(f"• {name}: Level {level}")
            except Exception:
                mastery_lines.append(f"• {name}: Unknown")

        mastery_text = "\n".join(mastery_lines) or "None"

        embed = discord.Embed(
            title=f"🎖 Game Stats — {self.roblox_name}",
            color=discord.Color.gold()
        )

        embed.add_field(name="🏆 Rank", value=rank, inline=True)
        embed.add_field(name="🔁 Rebirths", value=rebirths, inline=True)
        embed.add_field(name="📈 Login Count", value=self._fmt(login), inline=True)

        embed.add_field(name="🥚 Eggs", value=self._fmt(eggs), inline=True)
        embed.add_field(name="⏱ Playtime", value=self._format_playtime(playtime), inline=True)
        embed.add_field(name="💀 Huge Pets", value=self._fmt(huge), inline=True)

        embed.add_field(name="🧪 Mastery", value=mastery_text[:1024], inline=False)

        embed.set_footer(text="PS99 Player Stats • MCWV Dashboard")

        await interaction.response.send_message(embed=embed, ephemeral=True)

    # ---------------- DEBUG BUTTON ----------------
    @discord.ui.button(label="🔧 Debug", style=discord.ButtonStyle.red)
    async def debug_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            import json

            url = f"{PS99_API}/v1/players/{self.roblox_id}?include=profile,inventory,extendedProfile"

            timeout = aiohttp.ClientTimeout(total=10)

            async with session.get(url, timeout=timeout) as r:
                data = await r.json()

            pretty = json.dumps(data, indent=2, ensure_ascii=False)
            chunks = self._split_text(pretty, 1800)

            await interaction.response.send_message(
                f"```json\n{chunks[0]}\n```",
                ephemeral=True
            )

            for chunk in chunks[1:3]:
                await interaction.followup.send(
                    f"```json\n{chunk}\n```",
                    ephemeral=True
                )

        except Exception as e:
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"❌ Debug failed: `{e}`",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"❌ Debug failed: `{e}`",
                    ephemeral=True
                )

    @discord.ui.button(label="🔄 Refresh", style=discord.ButtonStyle.secondary)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

        try:
            bundle = await get_profile_bundle(
                self.session,
                self.roblox_id,
                force=True
            )

            extended_data, profile_data, inventory_data, public_views = bundle

            self.extended = extended_data or {}
            self.profile = profile_data or {}
            self.inventory = inventory_data or {}
            self.public_views = public_views or {}

            await interaction.followup.send("🔄 Profile refreshed.", ephemeral=True)

        except Exception as e:
            print(f"[refresh_button error] {e}")
            await interaction.followup.send("❌ Failed to refresh profile.", ephemeral=True)

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
        linked = next((u for u in db_users if str(u[0]).strip() == roblox_id.strip()), None)

        discord_id = str(linked[1]) if linked and linked[1] else None
        linked_status = "Linked" if discord_id else "Not linked"

        discord_member = None
        if discord_id and interaction.guild:
            try:
                discord_member = await interaction.guild.fetch_member(int(discord_id))
            except Exception:
                discord_member = None

        discord_display = (
            discord_member.mention if discord_member else
            (f"<@{discord_id}>" if discord_id else "Not linked")
        )

        # ---------------- ROLE CHECK ----------------
        OWNER_ROLE_ID = 1501985344843813038
        OFFICER_ROLE_ID = 1501986357516701827
        MEMBER_ROLE_ID = 1501986780667314246

        clan_role = None

        if discord_member:
            role_ids = {r.id for r in discord_member.roles}

            if OWNER_ROLE_ID in role_ids:
                role = discord_member.guild.get_role(OWNER_ROLE_ID)
                clan_role = role.mention if role else "Owner"

            elif OFFICER_ROLE_ID in role_ids:
                role = discord_member.guild.get_role(OFFICER_ROLE_ID)
                clan_role = role.mention if role else "Officer"

            elif MEMBER_ROLE_ID in role_ids:
                role = discord_member.guild.get_role(MEMBER_ROLE_ID)
                clan_role = role.mention if role else "Member"

        # ---------------- SESSION ----------------
        global session
        if session is None or session.closed:
            session = aiohttp.ClientSession()

        timeout = aiohttp.ClientTimeout(total=10)

        # ---------------- WAR DATA ----------------
        battle = None
        points = 0
        rank = None
        war_count = 0

        try:
            async with session.get(CLAN_API, timeout=timeout) as clan_r:
                if clan_r.status == 200:
                    clan_data = await clan_r.json()
                    battles = (clan_data.get("data") or {}).get("Battles") or {}

                    now = datetime.now(timezone.utc).timestamp()

                    for b_id, b_data in battles.items():
                        start = b_data.get("StartTime", 0) or 0
                        end = b_data.get("FinishTime", 0) or 0
                        contributions = b_data.get("PointContributions", [])

                        if any(str(e.get("UserID")) == roblox_id for e in contributions):
                            war_count += 1

                        if start <= now <= end:
                            battle = b_data

                            contributions = sorted(
                                battle.get("PointContributions", []),
                                key=lambda x: x.get("Points", 0),
                                reverse=True
                            )

                            for i, entry in enumerate(contributions, start=1):
                                if str(entry.get("UserID")) == roblox_id:
                                    points = int(entry.get("Points", 0) or 0)
                                    rank = i
                                    break
                            break

        except Exception as e:
            print("[profile] war API error:", e)

        # ---------------- PS99 API ----------------
        extended_data, profile_data, inventory_data, public_views = await get_profile_bundle(session, roblox_id)

        view = ProfileView(
            extended_data,
            inventory_data,
            profile_data,
            roblox_name,
            public_views,
            roblox_id,
            session
        )
        
        # ---------------- AVATAR ----------------
        avatar_url = (
            discord_member.display_avatar.url
            if discord_member
            else interaction.user.display_avatar.url
        )

        # ---------------- EMBED (IDENTICAL STYLE) ----------------
        embed = discord.Embed(
            title=f"📇 Player Profile — {roblox_name}",
            color=discord.Color.blurple()
        )

        embed.set_thumbnail(url=avatar_url)

        embed.add_field(name="🎮 Username", value=roblox_name, inline=False)
        embed.add_field(name="💬 Discord", value=discord_display, inline=False)
        embed.add_field(name="🆔 Roblox ID", value=roblox_id, inline=False)

        embed.add_field(name="🔗 Account Status", value=linked_status, inline=False)

        embed.add_field(
            name="🏷️ Clan Role",
            value=clan_role or "None",
            inline=False
        )

        if battle:
            embed.add_field(
                name="⚔️ War Activity",
                value=(
                    f"Points: **{points:,}**\n"
                    f"Rank: **#{rank if rank else 'N/A'}**\n"
                    f"Wars: **{war_count}**"
                ),
                inline=False
            )
        else:
            embed.add_field(
                name="⚔️ War Activity",
                value="No active war participation",
                inline=False
            )

        embed.set_footer(
            text=f"MCWV Profile Dashboard • Requested by {interaction.user.display_name}"
        )

        await interaction.followup.send(embed=embed, view=view)

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

import re
import asyncio
from datetime import datetime, timezone
import aiohttp
import discord

ROBLOX_USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{3,20}$")


def _is_strict_username(text: str) -> bool:
    text = text.strip()
    return bool(ROBLOX_USERNAME_RE.fullmatch(text))


def _parse_alt_input(raw: str):
    raw = raw.strip()

    # exact "none" only
    if raw == "none":
        return []

    # reject anything that isn't comma-separated usernames only
    parts = [p.strip() for p in raw.split(",")]
    if not parts or any(not p for p in parts):
        return None

    # every item must be a strict username
    if any(not _is_strict_username(p) for p in parts):
        return None

    # de-duplicate while preserving order
    seen = set()
    cleaned = []
    for p in parts:
        key = p.lower()
        if key not in seen:
            seen.add(key)
            cleaned.append(p)

    return cleaned


@bot.tree.command(name="accept", description="Accept an applicant inside a Tickets v2 ticket", guild=guild_obj)
@require_role()
async def accept(interaction: discord.Interaction, member: discord.Member):
    await interaction.response.defer(ephemeral=True)

    channel = interaction.channel
    guild = interaction.guild
    ticket_creator = member

    # --- ask for Roblox username ---
    await channel.send(
        f"👋 {ticket_creator.mention} — you've been accepted into **MCWV**! 🎉\n"
        f"Please reply with your **Roblox username only**.\n"
        f"Do not add any extra words."
    )

    def from_creator(m):
        return m.author.id == ticket_creator.id and m.channel.id == channel.id and bool(m.content.strip())

    roblox_input = None
    for _ in range(3):
        try:
            username_msg = await bot.wait_for("message", check=from_creator, timeout=120)
        except asyncio.TimeoutError:
            return await interaction.followup.send(
                "❌ Timed out waiting for Roblox username. Run `/accept` again to retry.",
                ephemeral=True
            )

        candidate = username_msg.content.strip()

        if not _is_strict_username(candidate):
            await channel.send(
                f"❌ `{candidate}` is not a valid Roblox username.\n"
                f"Please send **one username only** with no extra words."
            )
            continue

        roblox_input = candidate
        break

    if not roblox_input:
        return await interaction.followup.send(
            "❌ Could not get a valid Roblox username. Run `/accept` again to retry.",
            ephemeral=True
        )

    # --- ask for alts ---
    await channel.send(
        "Got it! If you have any other accounts **IN THE CLAN**, reply with the alt usernames **comma-separated**.\n"
        "If you have none, reply with exactly `none`."
    )

    alts = []
    for _ in range(3):
        try:
            alts_msg = await bot.wait_for("message", check=from_creator, timeout=90)
        except asyncio.TimeoutError:
            alts = []
            break

        alts_raw = alts_msg.content.strip()

        parsed_alts = _parse_alt_input(alts_raw)
        if parsed_alts is None:
            await channel.send(
                "❌ Invalid input.\n"
                "Reply with comma-separated Roblox usernames only, or exactly `none`."
            )
            continue

        alts = parsed_alts
        break
    else:
        alts = []

    # --- validate Roblox username via API ---
    roblox_url = "https://users.roblox.com/v1/usernames/users"
    try:
        async with session.post(
            roblox_url,
            json={"usernames": [roblox_input], "excludeBannedUsers": False}
        ) as r:
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

            roblox_id = str(results[0]["id"])
            roblox_name = results[0]["name"]

    except Exception as e:
        print(f"[accept] Roblox API exception: {e}")
        return await interaction.followup.send(
            "❌ Roblox API error. Try again in a moment.",
            ephemeral=True
        )

    # --- validate alt usernames via API if any were provided ---
    valid_alts = []
    invalid_alts = []

    if alts:
        try:
            async with session.post(
                roblox_url,
                json={"usernames": alts, "excludeBannedUsers": False}
            ) as r:
                body = await r.json()
                results = body.get("data", [])

                found = {}
                for item in results:
                    try:
                        requested = str(item.get("requestedUsername", "")).strip().lower()
                        found[requested] = {
                            "id": str(item["id"]),
                            "name": item["name"]
                        }
                    except Exception:
                        continue

                for alt_name in alts:
                    key = alt_name.lower()
                    if key in found:
                        valid_alts.append(found[key]["name"])
                    else:
                        invalid_alts.append(alt_name)

        except Exception as e:
            print(f"[accept] Alt Roblox API exception: {e}")
            invalid_alts = alts[:]
            valid_alts = []

    if invalid_alts:
        await channel.send(
            "⚠️ Some alt usernames could not be found and were ignored:\n"
            + "\n".join(f"• `{x}`" for x in invalid_alts)
        )

    # --- link in bot DB ---
    db_add(roblox_id, ticket_creator.id, roblox_name)

    actions = []
    errors = []

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
    alts_str = ", ".join(valid_alts) if valid_alts else "none"
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
        + (f"\nAlts: **{alts_str}**" if valid_alts else "")
    )

    # --- log all actions ---
    log_ch = guild.get_channel(LOG_CHANNEL_ID)
    if log_ch:
        log_embed = discord.Embed(
            title="✅ Member Accepted",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc)
        )
        log_embed.add_field(name="Staff", value=interaction.user.mention, inline=True)
        log_embed.add_field(name="New Member", value=ticket_creator.mention, inline=True)
        log_embed.add_field(name="Roblox", value=roblox_name, inline=True)
        log_embed.add_field(name="Alts", value=alts_str, inline=True)
        log_embed.add_field(name="Ticket", value=f"<#{channel.id}>", inline=True)
        log_embed.add_field(name="Actions", value="\n".join(actions) or "—", inline=False)
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
    
# ---- CLEANUP COMMAND -------
import re
import discord
from discord import app_commands

STAFF_CHAT_CHANNEL_ID = 1501639281750442114
CLAN_MEMBER_ROLE_ID = 1501986780667314246


def clear_tracking_for_roblox_id(roblox_id: str):
    rid = str(roblox_id).strip()
    status_cache.pop(rid, None)
    status_cache_time.pop(rid, None)
    offline_since.pop(rid, None)


async def cleanup_autocomplete(interaction: discord.Interaction, current: str):
    users = db_get_all()
    results = []
    current_lower = current.lower().strip()

    for roblox_id, discord_id, username in users:
        rid = str(roblox_id).strip()
        did = str(discord_id).strip()
        uname = str(username).strip()

        if current_lower in uname.lower():
            results.append(
                app_commands.Choice(
                    name=f"{uname} (Roblox)",
                    value=uname
                )
            )

        if current and current_lower in did:
            results.append(
                app_commands.Choice(
                    name=f"{did} (Discord ID)",
                    value=did
                )
            )

        if current and current_lower in rid:
            results.append(
                app_commands.Choice(
                    name=f"{rid} (Roblox ID)",
                    value=rid
                )
            )

        if len(results) >= 25:
            break

    return results[:25]


async def resolve_cleanup_target(guild: discord.Guild, target: str, db_users: list):
    target = target.strip()

    member = None
    linked_row = None
    roblox_id = None
    roblox_name = None

    # Discord mention / ID
    discord_id = None
    m = re.fullmatch(r"<@!?(\d+)>", target)
    if m:
        discord_id = int(m.group(1))
    elif target.isdigit():
        discord_id = int(target)

    if discord_id is not None:
        member = guild.get_member(discord_id)
        if member is None:
            try:
                member = await guild.fetch_member(discord_id)
            except Exception:
                member = None

        linked_row = next((u for u in db_users if int(u[1]) == discord_id), None)
        if linked_row:
            roblox_id = str(linked_row[0]).strip()
            roblox_name = linked_row[2]

        return member, linked_row, roblox_id, roblox_name

    # Roblox username / Roblox ID
    lowered = target.lower()
    linked_row = next(
        (
            u for u in db_users
            if str(u[0]).strip() == target
            or str(u[2]).strip().lower() == lowered
        ),
        None
    )

    if linked_row:
        roblox_id = str(linked_row[0]).strip()
        roblox_name = linked_row[2]

        member = guild.get_member(int(linked_row[1]))
        if member is None:
            try:
                member = await guild.fetch_member(int(linked_row[1]))
            except Exception:
                member = None

    return member, linked_row, roblox_id, roblox_name


class CleanupConfirmView(discord.ui.View):

    def __init__(self, guild, target, reason, requestor):
        super().__init__(timeout=86400)
        self.guild = guild
        self.target = target
        self.reason = reason
        self.requestor = requestor

    async def run_cleanup(self, interaction: discord.Interaction):

        users = db_get_all_tracked()

        member, linked_row, roblox_id, roblox_name = await resolve_cleanup_target(
            self.guild, self.target, users
        )

        actions = []

        # ---------------- REMOVE ROLE ----------------
        if member:
            role = self.guild.get_role(CLAN_MEMBER_ROLE_ID)
            if role and role in member.roles:
                try:
                    await member.remove_roles(role, reason=self.reason)
                    actions.append("✅ Removed clan role")
                except Exception as e:
                    actions.append(f"⚠️ Role removal failed: {e}")

        # ---------------- REMOVE DB LINKS ----------------
        try:
            if member:
                db_remove_all_links_for_discord(member.id)
                actions.append("✅ Removed main + alts from DB")
            elif linked_row:
                db_remove_all_links_for_discord(int(linked_row[1]))
                actions.append("✅ Removed DB links (fallback)")
        except Exception as e:
            actions.append(f"⚠️ DB unlink failed: {e}")

        # ---------------- CACHE CLEANUP ----------------
        try:
            if member:
                discord_id = member.id
                alts = db_get_alts(discord_id)
                main = db_get_main_link(discord_id)

                all_ids = []

                if main:
                    all_ids.append(str(main[0]).strip())

                for rid, _ in alts:
                    all_ids.append(str(rid).strip())

                for rid in all_ids:
                    status_cache.pop(rid, None)
                    status_cache_time.pop(rid, None)
                    offline_since.pop(rid, None)

                actions.append("✅ Cleared all caches (main + alts)")

            elif roblox_id:
                clear_tracking_for_roblox_id(roblox_id)
                actions.append("✅ Cleared caches")

        except Exception as e:
            actions.append(f"⚠️ Cache cleanup failed: {e}")

        embed = discord.Embed(
            title="✅ Cleanup Completed",
            description=(
                f"**Target:** {self.target}\n"
                f"**Roblox:** {roblox_name or 'Unknown'}\n"
                f"**Requested by:** {self.requestor.mention}"
            ),
            color=discord.Color.green()
        )

        embed.add_field(
            name="Actions",
            value="\n".join(actions) if actions else "None",
            inline=False
        )

        await interaction.response.edit_message(embed=embed, view=None)

    # ---------------- BUTTONS (MUST BE OUTSIDE run_cleanup) ----------------

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.run_cleanup(interaction)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title="❌ Cleanup Cancelled",
            description=f"Target: {self.target}",
            color=discord.Color.red()
        )
        await interaction.response.edit_message(embed=embed, view=None)

@bot.tree.command(
    name="cleanup",
    description="Remove clan role, unlink user, and clear tracking",
    guild=guild_obj
)
@app_commands.describe(target="Discord mention/ID or Roblox username/ID", reason="Reason for cleanup")
@app_commands.autocomplete(target=cleanup_autocomplete)
@require_role()
async def cleanup(interaction: discord.Interaction, target: str, reason: str = "No reason provided"):
    await interaction.response.defer(ephemeral=True)

    users = db_get_all_tracked()
    member, linked_row, roblox_id, roblox_name = await resolve_cleanup_target(
        interaction.guild, target, users
    )

    if not member and not linked_row:
        return await interaction.followup.send(
            "❌ User not found.",
            ephemeral=True
        )

    preview = discord.Embed(
        title="⚠️ Confirm Cleanup",
        description=(
            f"**Target:** {target}\n"
            f"**Roblox:** {roblox_name or 'Unknown'}\n"
            f"**Discord:** {member.mention if member else 'Not in server'}\n"
            f"**Reason:** {reason}"
        ),
        color=discord.Color.orange()
    )

    staff_channel = interaction.guild.get_channel(STAFF_CHAT_CHANNEL_ID)
    if not staff_channel:
        try:
            staff_channel = await interaction.guild.fetch_channel(STAFF_CHAT_CHANNEL_ID)
        except Exception:
            return await interaction.followup.send(
                "❌ Staff channel not found.",
                ephemeral=True
            )

    view = CleanupConfirmView(
        guild=interaction.guild,
        target=target,
        reason=reason,
        requestor=interaction.user
    )

    await staff_channel.send(embed=preview, view=view)

    await interaction.followup.send(
        f"✅ Sent confirmation to {staff_channel.mention}",
        ephemeral=True
    )
    
@bot.tree.command(name="settings", description="Show current bot settings", guild=guild_obj)
@require_role()
async def settings(interaction: discord.Interaction):

    ping_state = "ON ✅" if offline_ping_enabled else "OFF ❌"

    # ---------------- SAFE CHANNEL DISPLAY ----------------
    if reminder_channel_id:
        if reminder_channel_id == CHANNEL_ID:
            channel_display = f"<#{CHANNEL_ID}> (default)"
        else:
            channel_display = f"<#{reminder_channel_id}>"
    else:
        channel_display = "Not set"

    war_state = "⚔️ Active" if bot_enabled else "💤 Inactive"

    embed = discord.Embed(
        title="⚙️ Bot Settings",
        color=discord.Color.blurple()
    )

    embed.add_field(name="⚔️ War Tracking", value=war_state, inline=True)
    embed.add_field(name="🔔 Offline Alerts", value=ping_state, inline=True)
    embed.add_field(name="⏱️ Reminder Interval", value=f"{reminder_interval} min", inline=True)
    embed.add_field(name="📢 Reminder Channel", value=channel_display, inline=True)
    embed.add_field(name="🚨 Alert Channel", value=f"<#{CHANNEL_ID}>", inline=True)

    embed.set_footer(text="/toggleoffline • /setreminderinterval • /setreminderchanel")

    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(
    name="testreminder",
    description="Force-send an offline reminder immediately",
    guild=guild_obj
)
@require_role()
async def testreminder(interaction: discord.Interaction):

    await interaction.response.defer(ephemeral=True)

    if not offline_ping_enabled:
        return await interaction.followup.send(
            "❌ Offline reminders are currently disabled.",
            ephemeral=True
        )

    if not offline_since:
        return await interaction.followup.send(
            "⚠️ No offline users found in memory.",
            ephemeral=True
        )

    try:
        channel = await bot.fetch_channel(reminder_channel_id)
    except Exception as e:
        return await interaction.followup.send(
            f"❌ Could not fetch reminder channel: {e}",
            ephemeral=True
        )

    users = db_get_all()
    if not users:
        return await interaction.followup.send(
            "❌ No tracked users found.",
            ephemeral=True
        )

    lines = []

    for rid, since in offline_since.items():

        current = status_cache.get(str(rid).strip(), 0)

        # only true offline users
        if current != 0:
            continue

        info = next((x for x in users if str(x[0]).strip() == str(rid).strip()), None)
        if not info:
            continue

        duration = format_duration(since)
        lines.append(
            f"⚫ <@{info[1]}> **({info[2]})** is offline - {duration}"
        )

    if not lines:
        return await interaction.followup.send(
            "⚠️ No valid offline reminders to send.",
            ephemeral=True
        )

    await channel.send("\n".join(lines))

    await interaction.followup.send(
        "✅ Test reminder sent successfully.",
        ephemeral=True
    )

from discord import app_commands

@bot.tree.command(
    name="addalt",
    description="Add an alt Roblox account to a member",
    guild=guild_obj
)
@require_role()
async def addalt(interaction: discord.Interaction, member: discord.Member, roblox_username: str):
    await interaction.response.defer(ephemeral=True)

    global session, conn

    if not db_enabled():
        return await interaction.followup.send(
            "❌ Database is not available.",
            ephemeral=True
        )

    try:
        # Clear any previous failed transaction first
        try:
            conn.rollback()
        except Exception:
            pass

        roblox_username = roblox_username.strip()

        if not re.fullmatch(r"^[A-Za-z0-9_]{3,20}$", roblox_username):
            return await interaction.followup.send(
                "❌ Invalid Roblox username. Use one username only, with no extra words.",
                ephemeral=True
            )

        if session is None or session.closed:
            session = aiohttp.ClientSession()

        # Resolve Roblox username
        async with session.post(
            "https://users.roblox.com/v1/usernames/users",
            json={"usernames": [roblox_username], "excludeBannedUsers": False}
        ) as r:
            if r.status != 200:
                try:
                    conn.rollback()
                except Exception:
                    pass
                return await interaction.followup.send(
                    f"❌ Roblox API error (HTTP {r.status}).",
                    ephemeral=True
                )

            data = await r.json()

        results = data.get("data", [])
        if not results:
            try:
                conn.rollback()
            except Exception:
                pass
            return await interaction.followup.send(
                f"❌ Roblox user `{roblox_username}` not found.",
                ephemeral=True
            )

        roblox_id = str(results[0]["id"]).strip()
        username = str(results[0]["name"]).strip()

        # Insert only into user_alts
        with conn.cursor() as cur:
            # Check if already linked as a main account
            cur.execute("""
                SELECT discord_id
                FROM users
                WHERE roblox_id = %s
            """, (roblox_id,))
            row = cur.fetchone()
            if row:
                if int(row[0]) == member.id:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    return await interaction.followup.send(
                        "❌ That Roblox account is already their main account.",
                        ephemeral=True
                    )
                try:
                    conn.rollback()
                except Exception:
                    pass
                return await interaction.followup.send(
                    f"❌ That Roblox account is already linked to <@{row[0]}>.",
                    ephemeral=True
                )

            # Check if already linked as an alt
            cur.execute("""
                SELECT discord_id
                FROM user_alts
                WHERE roblox_id = %s
            """, (roblox_id,))
            row = cur.fetchone()
            if row:
                if int(row[0]) == member.id:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    return await interaction.followup.send(
                        "❌ That Roblox account is already added as an alt for this member.",
                        ephemeral=True
                    )
                try:
                    conn.rollback()
                except Exception:
                    pass
                return await interaction.followup.send(
                    f"❌ That Roblox account is already linked as an alt to <@{row[0]}>.",
                    ephemeral=True
                )

            cur.execute("""
                INSERT INTO user_alts (discord_id, roblox_id, username)
                VALUES (%s, %s, %s)
            """, (int(member.id), roblox_id, username))

        conn.commit()

        # Clear caches for this alt in case it was already seen before
        status_cache.pop(roblox_id, None)
        status_cache_time.pop(roblox_id, None)
        offline_since.pop(roblox_id, None)

        await interaction.followup.send(
            f"✅ Added **{username}** as an alt for {member.mention}.",
            ephemeral=True
        )

    except Exception as e:
        print("[addalt] error:", repr(e))
        try:
            conn.rollback()
        except Exception:
            pass
        await interaction.followup.send(
            f"❌ Failed to add alt.\n```{type(e).__name__}: {e}```",
            ephemeral=True
        )

@bot.tree.command(
    name="listalts",
    description="List a member's linked Roblox accounts",
    guild=guild_obj
)
@require_role()
async def listalts(interaction: discord.Interaction, member: discord.Member):
    await interaction.response.defer(ephemeral=True)

    try:
        main = db_get_main_link(member.id)
        alts = db_get_alts(member.id)

        embed = discord.Embed(
            title=f"👥 Alts for {member.display_name}",
            color=discord.Color.blurple()
        )

        if main:
            main_rid, main_name = main
            main_status = status_cache.get(str(main_rid).strip(), 0)
            status_icons = {0: "⚫", 1: "🟢", 2: "🎮", 3: "🔧"}
            embed.add_field(
                name="Main",
                value=f"{status_icons.get(main_status, '❓')} **{main_name}**",
                inline=False
            )
        else:
            embed.add_field(
                name="Main",
                value="Not linked",
                inline=False
            )

        if alts:
            lines = []
            status_icons = {0: "⚫", 1: "🟢", 2: "🎮", 3: "🔧"}

            for rid, uname in alts:
                st = status_cache.get(str(rid).strip(), 0)
                lines.append(f"{status_icons.get(st, '❓')} **{uname}**")

            embed.add_field(
                name=f"Alts ({len(alts)})",
                value="\n".join(lines),
                inline=False
            )
        else:
            embed.add_field(
                name="Alts",
                value="None",
                inline=False
            )

        embed.set_footer(text=f"{member.id}")
        await interaction.followup.send(embed=embed, ephemeral=True)

    except Exception as e:
        print("listalts error:", e)
        await interaction.followup.send("❌ Failed to list alts.", ephemeral=True)

@bot.tree.command(
    name="removealt",
    description="Remove one alt Roblox account from a member",
    guild=guild_obj
)
@require_role()
async def removealt(interaction: discord.Interaction, member: discord.Member, alt: str):
    await interaction.response.defer(ephemeral=True)

    try:
        ok, roblox_id, msg = db_remove_alt(member.id, alt)
        if not ok:
            return await interaction.followup.send(f"❌ {msg}", ephemeral=True)

        if roblox_id:
            rid = str(roblox_id).strip()
            status_cache.pop(rid, None)
            status_cache_time.pop(rid, None)
            offline_since.pop(rid, None)

        await interaction.followup.send(
            f"✅ {msg} for {member.mention}.",
            ephemeral=True
        )

    except Exception as e:
        print("removealt error:", e)
        await interaction.followup.send("❌ Failed to remove alt.", ephemeral=True)

@bot.tree.command(
    name="memberedit",
    description="Fix a member's Roblox username or alts",
    guild=guild_obj
)
@require_role()
@app_commands.describe(
    member="Discord member to update",
    roblox_username="Correct Roblox username",
    alts="Comma-separated alt usernames or 'none'",
    channel="Ticket channel to store for this member"
)
async def memberedit(
    interaction: discord.Interaction,
    member: discord.Member,
    roblox_username: str,
    alts: str = "none",
    channel: discord.TextChannel = None
):
    await interaction.response.defer(ephemeral=True)

    global session

    if session is None or session.closed:
        session = aiohttp.ClientSession()

    try:
        # ---------------- VALIDATE MAIN ACCOUNT ----------------

        async with session.post(
            "https://users.roblox.com/v1/usernames/users",
            json={
                "usernames": [roblox_username.strip()],
                "excludeBannedUsers": False
            }
        ) as r:
            data = await r.json()

        results = data.get("data", [])

        if not results:
            return await interaction.followup.send(
                f"❌ Roblox user `{roblox_username}` not found.",
                ephemeral=True
            )

        roblox_id = str(results[0]["id"])
        roblox_name = results[0]["name"]

        # ---------------- UPDATE MAIN LINK ----------------

        db_remove_all_links_for_discord(member.id)
        db_add(roblox_id, member.id, roblox_name)

        # ---------------- VALIDATE ALTS ----------------

        validated_alts = []

        if alts.lower().strip() != "none":
            alt_names = [
                a.strip()
                for a in alts.split(",")
                if a.strip()
            ]

            async with session.post(
                "https://users.roblox.com/v1/usernames/users",
                json={
                    "usernames": alt_names,
                    "excludeBannedUsers": False
                }
            ) as r:
                alt_data = await r.json()

            found = {
                u["name"].lower(): u
                for u in alt_data.get("data", [])
            }

            missing = [
                a for a in alt_names
                if a.lower() not in found
            ]

            if missing:
                return await interaction.followup.send(
                    f"❌ Invalid alt usernames: `{', '.join(missing)}`",
                    ephemeral=True
                )

            for alt in alt_data.get("data", []):
                db_add_alt(
                    member.id,
                    str(alt["id"]),
                    alt["name"]
                )
                validated_alts.append(alt["name"])

        # ---------------- UPDATE MEMBERS CHANNEL RECORD ----------------

        members_channel = interaction.guild.get_channel(
            MEMBERS_CHANNEL_ID
        )

        if members_channel:
            async for msg in members_channel.history(limit=500):
                if f"<@{member.id}>" in msg.content:

                    alt_text = (
                        ", ".join(validated_alts)
                        if validated_alts else "none"
                    )

                    if channel:
                        channel_line = f"{channel.mention} {member.mention}"
                    else:
                        lines = msg.content.splitlines()
                        channel_line = (
                            lines[0]
                            if lines
                            else member.mention
                        )

                    new_content = (
                        f"{channel_line}\n"
                        f"user:{roblox_name}\n"
                        f"alt:{alt_text}"
                    )

                    await msg.edit(content=new_content)
                    break

        # ---------------- CLEAR CACHE ----------------

        clear_tracking_for_roblox_id(roblox_id)

        # ---------------- LOG ----------------

        log_channel = interaction.guild.get_channel(LOG_CHANNEL_ID)

        if log_channel:
            embed = discord.Embed(
                title="🛠️ Member Record Updated",
                color=discord.Color.blurple(),
                timestamp=datetime.now(timezone.utc)
            )

            embed.add_field(
                name="Staff",
                value=interaction.user.mention
            )

            embed.add_field(
                name="Member",
                value=member.mention
            )

            embed.add_field(
                name="Roblox",
                value=roblox_name
            )

            embed.add_field(
                name="Alts",
                value=", ".join(validated_alts) or "none",
                inline=False
            )

            if channel:
                embed.add_field(
                    name="Ticket Channel",
                    value=channel.mention,
                    inline=False
                )

            await log_channel.send(embed=embed)

        await interaction.followup.send(
            f"✅ Updated {member.mention}\n"
            f"**Main:** {roblox_name}\n"
            f"**Alts:** {', '.join(validated_alts) or 'none'}",
            ephemeral=True
        )

    except Exception as e:
        print("[memberedit]", repr(e))

        await interaction.followup.send(
            f"❌ Update failed.\n```{e}```",
            ephemeral=True
        )
        
# ---------------- STATUS COMMAND ----------------
@bot.tree.command(name="status", description="Check Roblox status", guild=guild_obj)
async def status(interaction: discord.Interaction, member: discord.Member):
    await interaction.response.defer(ephemeral=True)

    try:
        users = db_get_all()
        target = next((u for u in users if int(u[1]) == member.id), None)

        if not target:
            return await interaction.followup.send("❌ Not linked", ephemeral=True)

        roblox_id = int(target[0])
        roblox_name = target[2]

        async with session.post(
            "https://presence.roblox.com/v1/presence/users",
            json={"userIds": [roblox_id]}
        ) as r:

            if r.status != 200:
                return await interaction.followup.send("❌ Roblox API error", ephemeral=True)

            data = await r.json()
            pres = data.get("userPresences", [{}])[0]
            current = pres.get("userPresenceType", 0)

        status_map = {
            0: "⚫ Offline",
            1: "🟢 Website",
            2: "🎮 In Game",
            3: "🔧 Studio"
        }

        await interaction.followup.send(
            f"{status_map.get(current, '❓ Unknown')} **{roblox_name}**",
            ephemeral=True
        )

    except Exception as e:
        print("[status error]", e)
        await interaction.followup.send("❌ Error", ephemeral=True)
        import asyncio
from datetime import datetime, timezone
import discord
from discord.ext import tasks

# ---------------- LOOP SAFETY ----------------
api_semaphore = asyncio.Semaphore(3)
pending_clan_removals = {}

def _chunks(items, size=50):
    for i in range(0, len(items), size):
        yield items[i:i + size]

async def _get_channel(channel_id: int):
    ch = bot.get_channel(channel_id)
    if ch is None:
        try:
            ch = await bot.fetch_channel(channel_id)
        except Exception:
            ch = None
    return ch

async def _roblox_presence_request(user_ids: list[int]):
    async with api_semaphore:
        async with session.post(
            "https://presence.roblox.com/v1/presence/users",
            json={"userIds": user_ids}
        ) as r:
            if r.status != 200:
                return None
            return await r.json()

# ---------------- INITIAL PRESENCE SYNC ----------------
async def run_initial_presence_check():
    try:
        users = db_get_all_tracked()
        if not users:
            return

        now_dt = datetime.now(timezone.utc)

        for chunk in _chunks([int(u[0]) for u in users], 50):
            data = await _roblox_presence_request(chunk)
            if not data:
                continue

            for p in data.get("userPresences", []):
                rid = str(p.get("userId", "")).strip()
                if not rid:
                    continue

                status = int(p.get("userPresenceType", 0) or 0)

                status_cache[rid] = status
                status_cache_time[rid] = now_dt

                if status == 0:
                    offline_since[rid] = now_dt
                else:
                    offline_since.pop(rid, None)

            await asyncio.sleep(1)

    except Exception as e:
        print("Initial sync error:", e)

# ---------------- ROBLOX LOOP (every 2 min — detects transitions) ----------------
@tasks.loop(minutes=2)
async def check_loop():
    print("🔄 CHECK_LOOP HIT")

    if not bot_enabled:
        return

    users = db_get_all_tracked()
    if not users:
        return

    print("Loop running, users:", len(users))

    global session

    try:
        if session is None or session.closed:
            session = aiohttp.ClientSession()

        user_ids = [int(u[0]) for u in users]
        all_presences = []

        for chunk in _chunks(user_ids, 50):
            data = await _roblox_presence_request(chunk)
            if not data:
                print("Presence API returned non-200 or no data")
                continue

            presences = data.get("userPresences", [])
            if isinstance(presences, list):
                all_presences.extend(presences)

            await asyncio.sleep(1)

    except Exception as e:
        print("Loop error (API fetch):", e)
        return

    if not all_presences:
        print("❌ No presence data returned")
        return

    print("PRESENCE COUNT:", len(all_presences))

    now = datetime.now(timezone.utc)
    users_map = {str(u[0]).strip(): u for u in users}

    try:
        for u in all_presences:
            try:
                rid = str(u.get("userId", "")).strip()
                if not rid:
                    continue

                current = u.get("userPresenceType")
                if current is None:
                    continue

                current = int(current)
                if current not in (0, 1, 2, 3):
                    continue

                old = status_cache.get(rid)

                status_cache[rid] = current
                status_cache_time[rid] = now

                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO user_status (roblox_id, status, updated_at)
                            VALUES (%s, %s, NOW())
                            ON CONFLICT (roblox_id)
                            DO UPDATE SET status = EXCLUDED.status,
                                          updated_at = NOW()
                        """, (rid, current))
                except Exception as db_error:
                    print("Loop DB error:", db_error)

                if old == current:
                    continue

                info = users_map.get(rid)
                if not info:
                    continue

                if current == 2:
                    offline_since.pop(rid, None)
                    continue

                if rid not in offline_since:
                    offline_since[rid] = now

                try:
                    if str(db_get_setting("offline_tracking", "false")).lower() == "true":
                        channel = await _get_channel(CHANNEL_ID)
                        if channel:
                            await channel.send(
                                f"⚫ <@{info[1]}> **({info[2]})** is no longer in game — {discord.utils.format_dt(now, 'R')}",
                                allowed_mentions=discord.AllowedMentions(users=True)
                            )
                except Exception as e:
                    print("Ping error:", e)

            except Exception as inner:
                print("Loop user error:", inner)

        try:
            conn.commit()
        except Exception as e:
            print("DB commit error:", e)

    except Exception as e:
        print("Loop processing error:", e)

# ---------------- REMINDER LOOP ----------------
@tasks.loop(minutes=30)
async def reminder_loop():
    try:
        if not bot_enabled or not offline_ping_enabled:
            return

        if not offline_since:
            return

        channel = await _get_channel(reminder_channel_id)
        if not channel:
            print("Reminder loop: could not fetch channel")
            return

        users = db_get_all_tracked()
        if not users:
            return

        users_map = {str(u[0]).strip(): u for u in users}
        lines = []

        for rid, since in list(offline_since.items()):
            current = status_cache.get(str(rid).strip(), 0)

            if current != 0:
                continue

            info = users_map.get(str(rid).strip())
            if not info:
                continue

            duration = format_duration(since)
            lines.append(f"⚫ <@{info[1]}> **({info[2]})** is offline - {duration}")

        if not lines:
            return

        await channel.send("\n\n".join(lines))

    except Exception as e:
        print("Reminder loop error:", e)

# ---------------- PS99 WAR POLL ----------------
ps99_first_check = True
ps99_war_active = False

@tasks.loop(minutes=20)
async def war_poll_loop():
    global bot_enabled, ps99_war_active, ps99_first_check, session

    try:
        if session is None or session.closed:
            session = aiohttp.ClientSession()

        timeout = aiohttp.ClientTimeout(total=15)

        async with session.get(PS99_API, timeout=timeout) as r:
            api_ok = r.status == 200
            data = await r.json() if api_ok else {}

        config = data.get("data", {}).get("configData", {})
        start = config.get("StartTime")
        finish = config.get("FinishTime")
        now = datetime.now(timezone.utc).timestamp()

        currently_active = (
            api_ok and
            isinstance(start, (int, float)) and
            isinstance(finish, (int, float)) and
            start <= now <= finish
        )

        if ps99_first_check:
            ps99_first_check = False
            ps99_war_active = currently_active
            bot_enabled = currently_active
            print(f"[INIT] War state set to {currently_active}")
            return

        if ps99_war_active != currently_active:
            ps99_war_active = currently_active
            bot_enabled = currently_active

            channel = await _get_channel(CHANNEL_ID)
            if not channel:
                return

            if currently_active:
                await channel.send("⚠️ CLAN WAR STARTED!! LETS GO MCWV!!!!!")
                print("War started (state synced)")
                await run_initial_presence_check()
            else:
                offline_since.clear()
                status_cache.clear()
                await channel.send("🛑 CLAN WAR OVER. GG EVERYONE!!")
                print("War ended (state synced)")

    except Exception as e:
        print("War poll error:", e)

# ---------------- CLAN LEAVE DETECTION (STAFF PANEL) ----------------
@tasks.loop(minutes=10)
async def clan_leave_loop():
    try:
        users = db_get_all_tracked()
        if not users:
            return

        if session is None or session.closed:
            return

        timeout = aiohttp.ClientTimeout(total=15)

        async with session.get(CLAN_API, timeout=timeout) as r:
            if r.status != 200:
                return
            data = (await r.json()).get("data", {})

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

        staff_channel = await _get_channel(1501639281750442114)
        if not staff_channel:
            return

        for roblox_id, discord_id, roblox_name in users:
            try:
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

            except Exception as e:
                print("Clan leave row error:", e)

    except Exception as e:
        print("Clan leave loop error:", e)

# ---------------- CLAN REVIEW VIEW ----------------
class ClanReviewView(discord.ui.View):
    def __init__(self, roblox_id):
        super().__init__(timeout=86400)
        self.roblox_id = roblox_id

    @discord.ui.button(label="Approve Removal", style=discord.ButtonStyle.danger)
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if self.roblox_id not in pending_clan_removals:
                return await interaction.response.send_message("Already handled.", ephemeral=True)

            data = pending_clan_removals.pop(self.roblox_id)

            guild = interaction.guild
            member = guild.get_member(int(data["discord_id"])) if guild else None

            if member is None and guild:
                try:
                    member = await guild.fetch_member(int(data["discord_id"]))
                except Exception:
                    member = None

            role = guild.get_role(CLAN_MEMBER_ROLE_ID) if guild else None

            if member and role and role in member.roles:
                await member.remove_roles(role, reason="Staff approved clan removal")

            try:
                db_remove(data["discord_id"])
            except Exception as e:
                print("DB remove error:", e)

            await interaction.response.edit_message(
                content="✅ Member removed and processed.",
                embed=interaction.message.embeds[0] if interaction.message.embeds else None,
                view=None
            )

        except Exception as e:
            print("Approve button error:", e)
            try:
                await interaction.response.send_message(
                    "❌ Something went wrong while processing this.",
                    ephemeral=True
                )
            except Exception:
                pass

# ---------------- LOOP STARTER ----------------
def start_bot_loops():
    if not check_loop.is_running():
        check_loop.start()

    if not reminder_loop.is_running():
        reminder_loop.start()

    if not war_poll_loop.is_running():
        war_poll_loop.start()

    if not clan_leave_loop.is_running():
        clan_leave_loop.start()

# ---------------- READY ----------------
@bot.event
async def on_ready():
    global session

    print("🚀 ON_READY HIT")

    if session is None or session.closed:
        session = aiohttp.ClientSession()

    try:
        synced = await bot.tree.sync(guild=guild_obj)
        print(f"✅ Synced {len(synced)} commands")
    except Exception as e:
        print("❌ Sync error:", e)

    print(f"🤖 Logged in as {bot.user} ({bot.user.id})")

    try:
        start_bot_loops()
        print("✅ Bot loops started")
    except Exception as e:
        print(f"❌ Failed to start loops: {e}")

    print(f"👥 Tracking {len(db_get_all_tracked())} users")
    print("✅ ON_READY DONE")

# ---------------- CLEANUP ----------------
@bot.event
async def on_disconnect():
    global session

    if session and not session.closed:
        await session.close()


# ---------------- RUN ----------------
if __name__ == "__main__":
    bot.run(TOKEN)
