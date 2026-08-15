import os
import re
import asyncio
import sqlite3
import platform
import resource
import secrets
import psycopg2
from psycopg2.extras import RealDictCursor
from functools import wraps
import discord
import aiohttp
import traceback
from PIL import Image, ImageDraw, ImageFont, ImageFilter
from io import BytesIO
import math
import random
from datetime import datetime, timezone, timedelta
import time
import json

try:
    import psutil
except Exception:
    psutil = None

from discord import app_commands
from discord.ext import commands, tasks

from flask import Flask, jsonify, request
from threading import Thread

session = None
status_cooldown = {}

app = Flask(__name__)


@app.teardown_request
def _close_event_conn_after_request(exc=None):
    close_event_conn()


@app.route("/")
def home():
    return "Bot is alive"


# ---------------- ADMIN API ----------------
# The Hub talks to these routes server-to-server using X-Admin-API-Key.
# Keep every route registered before the Flask thread starts.
ADMIN_API_KEY = os.environ.get("BOT_ADMIN_API_KEY") or os.environ.get("ADMIN_API_KEY")
ADMIN_RESTART_ENABLED = os.environ.get("ALLOW_ADMIN_RESTART", "0") == "1"
HUB_BASE_URL = (os.environ.get("MCWV_HUB_URL") or os.environ.get("HUB_URL") or "https://mcwv-hub.vercel.app").rstrip("/")
WAR_COLLECT_SECRET = os.environ.get("WAR_COLLECT_SECRET", "")
WAR_COLLECT_INTERVAL_MINUTES = max(1, int(os.environ.get("WAR_COLLECT_INTERVAL_MINUTES", "1") or "1"))
OFFICER_GUIDE_ROLE_ID = int(os.environ.get("OFFICER_GUIDE_ROLE_ID", "1501986357516701827"))
STARTED_AT = time.time()
LAST_HEARTBEAT = datetime.now(timezone.utc).isoformat()
COMMANDS_EXECUTED = 0
ADMIN_LOGS = []


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def admin_log(event: str, message: str = "", level: str = "info", extra=None):
    entry = {
        "id": f"{int(time.time() * 1000)}-{len(ADMIN_LOGS)}",
        "level": level,
        "event": event,
        "message": message,
        "createdAt": _now_iso(),
        "extra": extra or {},
    }
    ADMIN_LOGS.insert(0, entry)
    del ADMIN_LOGS[200:]
    print(f"[admin-api] {level.upper()} {event}: {message}")
    return entry


def _admin_api_authorized():
    if not ADMIN_API_KEY:
        return False

    provided = request.headers.get("X-Admin-API-Key", "")
    if not provided:
        auth_header = request.headers.get("Authorization", "")
        if auth_header.lower().startswith("bearer "):
            provided = auth_header.split(" ", 1)[1].strip()

    return bool(provided) and secrets.compare_digest(str(provided), str(ADMIN_API_KEY))


def require_admin_api_key(handler):
    @wraps(handler)
    def wrapper(*args, **kwargs):
        if not _admin_api_authorized():
            return jsonify({"error": "Unauthorized"}), 401
        return handler(*args, **kwargs)

    return wrapper


def _row_to_dict(row):
    if row is None:
        return None
    if isinstance(row, sqlite3.Row):
        return {key: row[key] for key in row.keys()}
    if isinstance(row, dict):
        return row
    return dict(row) if hasattr(row, "keys") else row


def _safe_call(name, default=None, *args, **kwargs):
    func = globals().get(name)
    if not callable(func):
        return default
    try:
        return func(*args, **kwargs)
    except Exception as exc:
        print(f"[admin-api] {name} failed: {exc}")
        return default


def parse_hex_color(value, default=0x34D399):
    if value is None or value == "":
        return int(default)
    try:
        raw = str(value).strip().replace("#", "")
        if raw.lower().startswith("0x"):
            raw = raw[2:]
        if not re.fullmatch(r"[0-9a-fA-F]{6}", raw):
            return int(default)
        return int(raw, 16)
    except Exception:
        try:
            return int(default)
        except Exception:
            return 0x34D399


def _loop_status(name):
    loop_obj = globals().get(name)
    if loop_obj is None:
        return {"status": "Unknown"}
    try:
        return {
            "status": "Running" if loop_obj.is_running() else "Stopped",
            "seconds": getattr(loop_obj, "seconds", None),
            "minutes": getattr(loop_obj, "minutes", None),
        }
    except Exception:
        return {"status": "Unknown"}


def _process_metrics():
    if psutil is not None:
        proc = psutil.Process(os.getpid())
        return {
            "cpu": round(psutil.cpu_percent(interval=None), 2),
            "ramMb": round(proc.memory_info().rss / 1024 / 1024, 2),
            "platform": platform.platform(),
        }

    # Fallback without psutil. ru_maxrss is KiB on Linux.
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "cpu": None,
        "ramMb": round(float(usage.ru_maxrss) / 1024, 2),
        "platform": platform.platform(),
    }


def _bot_summary():
    bot_obj = globals().get("bot")
    if bot_obj is None:
        return {
            "ready": False,
            "pingMs": None,
            "guildCount": 0,
            "users": 0,
        }

    guilds = list(getattr(bot_obj, "guilds", []) or [])
    return {
        "ready": bool(getattr(bot_obj, "is_ready", lambda: False)()),
        "pingMs": round(float(getattr(bot_obj, "latency", 0) or 0) * 1000, 2),
        "guildCount": len(guilds),
        "users": sum(int(getattr(guild, "member_count", 0) or 0) for guild in guilds),
    }


def _db_connected():
    func = globals().get("db_enabled")
    if callable(func):
        try:
            return bool(func())
        except Exception:
            return False
    return False


def _tracked_players():
    rows = _safe_call("db_get_all_tracked", [], ) or []
    return len(rows)


def _active_invite_payload():
    event = _row_to_dict(_safe_call("get_active_event"))
    if not event:
        return None
    event["active"] = bool(int(event.get("active") or 0))
    return event


def _active_giveaway_payload():
    giveaway = _row_to_dict(_safe_call("get_giveaway_row"))
    if not giveaway:
        return None
    giveaway["active"] = bool(int(giveaway.get("active") or 0))
    return giveaway


def _run_on_bot_loop(coro):
    bot_obj = globals().get("bot")
    if bot_obj is None or getattr(bot_obj, "loop", None) is None:
        raise RuntimeError("Bot event loop is not ready")
    return asyncio.run_coroutine_threadsafe(coro, bot_obj.loop)


async def _restart_loop_from_admin(loop_name):
    loop_obj = globals().get(loop_name)
    if loop_obj is None:
        raise RuntimeError(f"Loop not found: {loop_name}")
    if loop_obj.is_running():
        loop_obj.restart()
    else:
        loop_obj.start()


async def _maybe_get_channel(channel_id):
    bot_obj = globals().get("bot")
    if bot_obj is None or not channel_id:
        return None
    channel = bot_obj.get_channel(int(channel_id))
    if channel:
        return channel
    try:
        return await bot_obj.fetch_channel(int(channel_id))
    except Exception:
        return None


async def _validate_admin_text_channel(channel_id, require_invite=False):
    if not channel_id:
        raise ValueError("A Discord channel ID is required.")

    try:
        channel_id = int(channel_id)
    except Exception:
        raise ValueError("A valid numeric Discord channel ID is required.")

    channel = await _maybe_get_channel(channel_id)
    if channel is None:
        raise ValueError("Channel not found. Check the channel ID and make sure the bot can see it.")

    if not isinstance(channel, discord.TextChannel):
        raise ValueError("The selected channel must be a server text channel.")

    guild = channel.guild
    bot_obj = globals().get("bot")
    me = guild.me

    if me is None and bot_obj and bot_obj.user:
        me = guild.get_member(bot_obj.user.id)

    if me is None:
        raise ValueError("Could not check bot permissions for that channel.")

    perms = channel.permissions_for(me)

    if not perms.view_channel:
        raise ValueError(f"Bot cannot view #{channel.name}.")
    if not perms.send_messages:
        raise ValueError(f"Bot cannot send messages in #{channel.name}.")
    if not perms.embed_links:
        raise ValueError(f"Bot cannot embed links in #{channel.name}.")
    if require_invite and not perms.create_instant_invite:
        raise ValueError(f"Bot cannot create invites in #{channel.name}.")

    return channel


async def _admin_channels_payload():
    bot_obj = globals().get("bot")
    if bot_obj is None or not getattr(bot_obj, "is_ready", lambda: False)():
        return []

    channels = []

    for guild in bot_obj.guilds:
        me = guild.me
        if me is None and bot_obj.user:
            me = guild.get_member(bot_obj.user.id)
        if me is None:
            continue

        for channel in guild.text_channels:
            perms = channel.permissions_for(me)
            can_send = bool(perms.view_channel and perms.send_messages and perms.embed_links)
            can_invite = bool(can_send and perms.create_instant_invite)
            parent_name = channel.category.name if channel.category else None
            label = f"{parent_name + ' / ' if parent_name else ''}#{channel.name}"

            channels.append({
                "id": str(channel.id),
                "name": channel.name,
                "label": label,
                "guildId": str(guild.id),
                "guildName": guild.name,
                "parentName": parent_name,
                "canSendMessages": can_send,
                "canCreateInvite": bool(perms.create_instant_invite),
                "usableForGiveaways": can_send,
                "usableForInvites": can_invite,
                "position": channel.position,
            })

    channels.sort(key=lambda item: (item.get("guildName") or "", item.get("parentName") or "", item.get("position") or 0, item.get("name") or ""))
    return channels


@app.route("/admin/channels")
@require_admin_api_key
def admin_channels():
    try:
        future = _run_on_bot_loop(_admin_channels_payload())
        channels = future.result(timeout=10)
        return jsonify({"success": True, "source": "bot", "channels": channels})
    except Exception as exc:
        return jsonify({"error": str(exc), "channels": []}), 500


async def _admin_roles_payload():
    bot_obj = globals().get("bot")
    if bot_obj is None or not getattr(bot_obj, "is_ready", lambda: False)():
        return []

    roles = []

    for guild in bot_obj.guilds:
        for role in sorted(guild.roles, key=lambda item: item.position, reverse=True):
            if role.is_default() or role.managed:
                continue
            roles.append({
                "id": str(role.id),
                "name": role.name,
                "guildId": str(guild.id),
                "guildName": guild.name,
                "position": role.position,
                "color": str(role.color),
                "memberCount": len(role.members),
            })

    return roles


@app.route("/admin/roles")
@require_admin_api_key
def admin_roles():
    try:
        future = _run_on_bot_loop(_admin_roles_payload())
        roles = future.result(timeout=10)
        return jsonify({"success": True, "source": "bot", "roles": roles})
    except Exception as exc:
        return jsonify({"error": str(exc), "roles": []}), 500


@app.route("/admin/signup/verify-dm", methods=["POST"])
@require_admin_api_key
def admin_signup_verify_dm():
    body = request.get_json(silent=True) or {}
    discord_id = body.get("discord_id") or body.get("discordId")
    username = str(body.get("username") or "MCWV member")[:80]
    code = str(body.get("code") or "").strip()

    if not discord_id or not code:
        return jsonify({"error": "discord_id and code are required"}), 400

    try:
        future = _run_on_bot_loop(_send_signup_verification_dm(discord_id, username, code))
        payload = future.result(timeout=15)
        return jsonify(payload)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


async def _send_signup_verification_dm(discord_id, username, code):
    try:
        user_id = int(discord_id)
    except Exception:
        raise ValueError("discord_id must be numeric")

    member = None
    guild = broadcast_primary_guild()
    if guild:
        member = guild.get_member(user_id)
        if member is None:
            try:
                member = await guild.fetch_member(user_id)
            except Exception:
                member = None

    target = member
    if target is None:
        try:
            target = await bot.fetch_user(user_id)
        except Exception:
            target = None

    if target is None:
        raise ValueError("Discord user could not be found")

    embed = discord.Embed(
        title="MCWV Hub verification",
        description=(
            f"Someone is creating an MCWV Hub account for **{username}**.\n\n"
            f"Your verification code is:\n\n"
            f"`{code}`\n\n"
            "Enter this code on the signup page to finish creating your account. "
            "If this was not you, ignore this message."
        ),
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_footer(text="This code expires in 10 minutes.")

    try:
        await target.send(embed=embed)
    except Exception:
        raise ValueError("Could not DM that Discord user. They may have DMs disabled.")

    return {"success": True, "sent": True, "discord_id": str(user_id)}




def _ticket_row_to_payload(row):
    if not row:
        return None
    return {
        "id": row[0],
        "ticketId": row[1],
        "channelId": str(row[2]) if row[2] else None,
        "guildId": str(row[3]) if row[3] else None,
        "openerDiscordId": str(row[4]) if row[4] else None,
        "robloxId": row[5],
        "robloxUsername": row[6],
        "status": row[7],
        "claimedBy": str(row[8]) if row[8] else None,
        "createdAt": row[9].isoformat() if row[9] else None,
        "updatedAt": row[10].isoformat() if row[10] else None,
        "acceptedAt": row[11].isoformat() if row[11] else None,
        "acceptedBy": str(row[12]) if row[12] else None,
        "rejectedAt": row[13].isoformat() if row[13] else None,
        "rejectedBy": str(row[14]) if row[14] else None,
        "rejectReason": row[15],
        "closedAt": row[16].isoformat() if row[16] else None,
        "closedBy": str(row[17]) if row[17] else None,
        "closeReason": row[18],
        "screenshotsUploaded": bool(row[19]) if len(row) > 19 else False,
    }


def _ticket_application_payload(row):
    if not row:
        return None
    return {
        "robloxUsername": row[0],
        "robloxId": row[1],
        "afk247": row[2],
        "activity": row[3],
        "liquidGems": row[4],
        "whyAccept": row[5],
        "submittedAt": row[6].isoformat() if row[6] else None,
    }


def _ticket_action_payload(row):
    return {
        "id": row[0],
        "ticketId": row[1],
        "actorDiscordId": str(row[2]) if row[2] else None,
        "action": row[3],
        "message": row[4],
        "metadata": row[5] or {},
        "createdAt": row[6].isoformat() if row[6] else None,
    }


def db_admin_list_mcwv_tickets():
    if not db_enabled():
        return []
    db_ensure_mcwv_ticket_tables()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, ticket_id, channel_id, guild_id, opener_discord_id, roblox_id, roblox_username,
                       status, claimed_by, created_at, updated_at, accepted_at, accepted_by,
                       rejected_at, rejected_by, reject_reason, closed_at, closed_by, close_reason,
                       EXISTS (
                         SELECT 1 FROM mcwv_ticket_actions a
                         WHERE a.ticket_id = mcwv_tickets.ticket_id
                           AND a.action = 'screenshots/uploaded'
                       ) AS screenshots_uploaded
                FROM mcwv_tickets
                ORDER BY updated_at DESC
                LIMIT 200
            """)
            rows = cur.fetchall()
        return [_ticket_row_to_payload(row) for row in rows]
    except Exception as e:
        print("db_admin_list_mcwv_tickets error:", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return []


def db_admin_get_mcwv_ticket(ticket_id):
    if not db_enabled():
        return None
    db_ensure_mcwv_ticket_tables()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, ticket_id, channel_id, guild_id, opener_discord_id, roblox_id, roblox_username,
                       status, claimed_by, created_at, updated_at, accepted_at, accepted_by,
                       rejected_at, rejected_by, reject_reason, closed_at, closed_by, close_reason,
                       EXISTS (
                         SELECT 1 FROM mcwv_ticket_actions a
                         WHERE a.ticket_id = mcwv_tickets.ticket_id
                           AND a.action = 'screenshots/uploaded'
                       ) AS screenshots_uploaded
                FROM mcwv_tickets
                WHERE ticket_id = %s OR channel_id::text = %s OR id::text = %s
                LIMIT 1
            """, (str(ticket_id), str(ticket_id), str(ticket_id)))
        ticket = cur.fetchone()
        if not ticket:
            return None
        canonical = ticket[1]
        cur.execute("""
            SELECT roblox_username, roblox_id, afk_247, activity, liquid_gems, why_accept, submitted_at
            FROM mcwv_ticket_applications
            WHERE ticket_id = %s
            LIMIT 1
        """, (canonical,))
        app_row = cur.fetchone()
        cur.execute("""
            SELECT id, ticket_id, actor_discord_id, action, message, metadata, created_at
            FROM mcwv_ticket_actions
            WHERE ticket_id = %s
            ORDER BY created_at DESC
            LIMIT 50
        """, (canonical,))
        actions = cur.fetchall()
        cur.execute("""
            SELECT transcript_text, created_at
            FROM mcwv_ticket_transcripts
            WHERE ticket_id = %s
            ORDER BY created_at DESC
            LIMIT 1
        """, (canonical,))
        transcript = cur.fetchone()
        payload = _ticket_row_to_payload(ticket)
        payload["application"] = _ticket_application_payload(app_row)
        payload["actions"] = [_ticket_action_payload(row) for row in actions]
        payload["screenshotsUploaded"] = bool(payload.get("screenshotsUploaded")) or any(action.get("action") == "screenshots/uploaded" for action in payload["actions"])
        payload["transcript"] = {"text": transcript[0], "createdAt": transcript[1].isoformat()} if transcript else None
        return payload
    except Exception as e:
        print("db_admin_get_mcwv_ticket error:", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return None


async def enrich_ticket_last_message(ticket):
    if not isinstance(ticket, dict) or not ticket.get("channelId"):
        return ticket
    try:
        channel = await _maybe_get_channel(int(ticket["channelId"]))
        last_at = None
        if isinstance(channel, discord.TextChannel):
            last_message_id = getattr(channel, "last_message_id", None)
            if last_message_id:
                try:
                    message = await channel.fetch_message(int(last_message_id))
                    last_at = message.created_at.astimezone(timezone.utc).isoformat()
                except Exception:
                    pass
            if not last_at:
                try:
                    async for message in channel.history(limit=1):
                        last_at = message.created_at.astimezone(timezone.utc).isoformat()
                        break
                except Exception:
                    pass
        if last_at:
            ticket["lastMessageAt"] = last_at
    except Exception as exc:
        print(f"[tickets api] last message lookup failed for {ticket.get('ticketId')}: {exc}")
    return ticket


async def enrich_tickets_last_message(tickets):
    semaphore = asyncio.Semaphore(8)

    async def limited(ticket):
        async with semaphore:
            return await enrich_ticket_last_message(ticket)

    return await asyncio.gather(*(limited(ticket) for ticket in tickets))


async def _admin_tickets_list_payload():
    tickets = db_admin_list_mcwv_tickets()
    tickets = await enrich_tickets_last_message(tickets)
    metrics = {
        "total": len(tickets),
        "open": sum(1 for ticket in tickets if ticket.get("status") in ("open", "pending")),
        "pending": sum(1 for ticket in tickets if ticket.get("status") == "pending"),
        "accepted": sum(1 for ticket in tickets if ticket.get("status") == "accepted"),
        "closed": sum(1 for ticket in tickets if ticket.get("status") == "closed"),
    }
    return {"success": True, "tickets": tickets, "metrics": metrics}


async def _admin_ticket_detail_payload(ticket_id):
    ticket = db_admin_get_mcwv_ticket(ticket_id)
    if not ticket:
        return None
    return await enrich_ticket_last_message(ticket)


async def _admin_ticket_accept(ticket_id, actor_id):
    ticket = db_admin_get_mcwv_ticket(ticket_id)
    if not ticket:
        raise ValueError("Ticket not found")
    app = ticket.get("application")
    if not app:
        raise ValueError("Ticket has no application")
    guild = bot.get_guild(int(ticket["guildId"])) if ticket.get("guildId") else broadcast_primary_guild()
    if not guild:
        raise ValueError("Guild not available")
    channel = guild.get_channel(int(ticket["channelId"])) if ticket.get("channelId") else None
    if channel is None and ticket.get("channelId"):
        channel = await guild.fetch_channel(int(ticket["channelId"]))
    applicant = guild.get_member(int(ticket["openerDiscordId"])) if ticket.get("openerDiscordId") else None
    if applicant is None and ticket.get("openerDiscordId"):
        applicant = await guild.fetch_member(int(ticket["openerDiscordId"]))
    if applicant is None:
        raise ValueError("Applicant not found in server")
    ok, db_msg = db_add(app["robloxId"], applicant.id, app["robloxUsername"])
    if not ok:
        raise ValueError(f"Could not link Roblox account: {db_msg}")
    if channel:
        db_set_ticket_channel(applicant.id, channel.id)
    role = guild.get_role(MCWV_TICKET_MEMBER_ROLE_ID)
    if role:
        await applicant.add_roles(role, reason=f"MCWV application accepted from Hub by {actor_id}")
    if channel:
        try:
            safe_name = normalize_ticket_key(app["robloxUsername"])[:24] or str(applicant.id)
            await channel.edit(name=f"⭐-ticket-{safe_name}", reason="MCWV application accepted from Hub")
        except Exception as exc:
            print(f"[ticket admin accept] rename failed for {ticket['ticketId']}: {exc}")
        try:
            accepted_category = get_available_category(guild)
            if accepted_category:
                await channel.edit(
                    category=accepted_category,
                    sync_permissions=False,
                    reason="MCWV application accepted from Hub — moved to member ticket category",
                )
            else:
                print(f"[ticket admin accept] no accepted category available for {ticket['ticketId']}")
        except Exception as exc:
            print(f"[ticket admin accept] category move failed for {ticket['ticketId']}: {exc}")
    db_update_ticket_status(ticket["ticketId"], "accepted", actor_id, accepted_at=datetime.now(timezone.utc), accepted_by=actor_id)
    embed = discord.Embed(
        title="Accepted into MCWV",
        description=f"Welcome to **MCWV**, {applicant.mention}!\nRoblox account linked as **{app['robloxUsername']}**. This ticket will stay open for next steps.",
        color=discord.Color.green(),
        timestamp=datetime.now(timezone.utc),
    )
    if channel:
        await channel.send(embed=embed)
    await log_ticket_event(guild, embed)
    await delete_ticket_control_message(guild, ticket_id=ticket["ticketId"])
    return db_admin_get_mcwv_ticket(ticket["ticketId"])


async def _admin_ticket_close(ticket_id, actor_id, reason):
    ticket = db_admin_get_mcwv_ticket(ticket_id)
    if not ticket:
        raise ValueError("Ticket not found")
    guild = bot.get_guild(int(ticket["guildId"])) if ticket.get("guildId") else broadcast_primary_guild()
    channel = guild.get_channel(int(ticket["channelId"])) if guild and ticket.get("channelId") else None
    if channel is None and guild and ticket.get("channelId"):
        channel = await guild.fetch_channel(int(ticket["channelId"]))
    transcript = await build_ticket_transcript(channel) if channel else "Channel unavailable"
    db_save_ticket_transcript(ticket["ticketId"], int(ticket["channelId"] or 0), transcript)
    db_update_ticket_status(ticket["ticketId"], "closed", actor_id, closed_at=datetime.now(timezone.utc), closed_by=actor_id, close_reason=reason)
    opened_at = datetime.fromisoformat(ticket["createdAt"]) if ticket.get("createdAt") else None
    await send_ticket_close_outputs(
        guild,
        channel,
        ticket["ticketId"],
        int(ticket["openerDiscordId"]) if ticket.get("openerDiscordId") else None,
        int(actor_id) if actor_id else None,
        opened_at,
        reason,
        transcript,
    )
    await delete_ticket_control_message(guild, ticket_id=ticket["ticketId"])
    if channel:
        await asyncio.sleep(MCWV_TICKET_DELETE_DELAY_SECONDS)
        deleted = await safe_delete_ticket_channel(channel, reason=f"MCWV ticket closed from Hub: {reason}")
        if not deleted:
            print(f"[ticket] admin close of {ticket['ticketId']}: channel auto-delete blocked by protection; channel left in place")
    return {"success": True}


async def _admin_ticket_clear(actor_id=0):
    if not db_enabled():
        raise ValueError("Database is not available")
    db_ensure_mcwv_ticket_tables()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM mcwv_tickets")
            before = int(cur.fetchone()[0] or 0)
            cur.execute("DELETE FROM mcwv_ticket_transcripts")
            cur.execute("DELETE FROM mcwv_ticket_actions")
            cur.execute("DELETE FROM mcwv_ticket_applications")
            cur.execute("DELETE FROM mcwv_tickets")
        conn.commit()
        db_log_admin_action(
            "warning",
            "Application Tickets Cleared",
            f"Cleared {before} application ticket record(s) from the dashboard.",
            "tickets/clear",
            str(actor_id),
            {"cleared": before, "actorId": str(actor_id)},
        )
        return {"success": True, "cleared": before}
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        raise ValueError(f"Failed to clear tickets: {exc}")


async def _ticket_blacklist_payload_row(guild, row):
    discord_id = int(row[0])
    user = guild.get_member(discord_id) if guild else None
    if user is None:
        try:
            user = await bot.fetch_user(discord_id)
        except Exception:
            user = None
    return {
        "discordId": str(discord_id),
        "reason": row[1] or "",
        "createdBy": str(row[2]) if row[2] else None,
        "createdAt": row[3].isoformat() if row[3] else None,
        "username": str(user) if user else None,
        "displayName": getattr(user, "display_name", None) if user else None,
        "avatarUrl": user.display_avatar.url if user else None,
    }


async def _admin_ticket_blacklist_list():
    guild = broadcast_primary_guild()
    rows = db_ticket_blacklist_list()
    items = []
    for row in rows:
        items.append(await _ticket_blacklist_payload_row(guild, row))
    return {"success": True, "blacklist": items}


async def _admin_ticket_blacklist_add(discord_id, reason, actor_id=0):
    guild = broadcast_primary_guild()
    if not str(discord_id or "").isdigit():
        raise ValueError("A valid Discord user ID is required")
    if not db_ticket_blacklist_add(discord_id, reason, actor_id):
        raise ValueError("Failed to save blacklist entry")
    member = None
    if guild:
        member = guild.get_member(int(discord_id))
        if member is None:
            try:
                member = await guild.fetch_member(int(discord_id))
            except Exception:
                member = None
    if member and guild:
        role = guild.get_role(MCWV_TICKET_BLACKLIST_ROLE_ID)
        if role:
            try:
                await member.add_roles(role, reason=f"Ticket blacklist from Hub by {actor_id}: {reason}")
            except Exception as exc:
                print(f"[ticket blacklist] role add failed for {discord_id}: {exc}")
    row = db_ticket_blacklist_get(discord_id)
    return {"success": True, "entry": await _ticket_blacklist_payload_row(guild, row)}


async def _admin_ticket_blacklist_remove(discord_id, actor_id=0):
    guild = broadcast_primary_guild()
    if not str(discord_id or "").isdigit():
        raise ValueError("A valid Discord user ID is required")
    db_ticket_blacklist_remove(discord_id)
    member = None
    if guild:
        member = guild.get_member(int(discord_id))
        if member is None:
            try:
                member = await guild.fetch_member(int(discord_id))
            except Exception:
                member = None
    if member and guild:
        role = guild.get_role(MCWV_TICKET_BLACKLIST_ROLE_ID)
        if role and role in member.roles:
            try:
                await member.remove_roles(role, reason=f"Ticket blacklist removed from Hub by {actor_id}")
            except Exception as exc:
                print(f"[ticket blacklist] role remove failed for {discord_id}: {exc}")
    return {"success": True, "removed": str(discord_id)}


@app.route("/admin/tickets/settings", methods=["GET", "POST"])
@require_admin_api_key
def admin_ticket_settings():
    if request.method == "GET":
        return jsonify({"success": True, "settings": get_mcwv_ticket_settings()})
    body = request.get_json(silent=True) or {}
    settings = save_mcwv_ticket_settings(body.get("settings") if isinstance(body.get("settings"), dict) else body)
    return jsonify({"success": True, "settings": settings})


@app.route("/admin/tickets/blacklist", methods=["GET", "POST"])
@require_admin_api_key
def admin_ticket_blacklist():
    body = request.get_json(silent=True) or {}
    try:
        if request.method == "GET":
            future = _run_on_bot_loop(_admin_ticket_blacklist_list())
        else:
            action = str(body.get("action") or "add").lower()
            discord_id = body.get("discord_id") or body.get("discordId") or body.get("user_id") or body.get("userId")
            if action in ("remove", "delete", "unblacklist"):
                future = _run_on_bot_loop(_admin_ticket_blacklist_remove(discord_id, body.get("actor_id") or body.get("actorId") or 0))
            else:
                future = _run_on_bot_loop(_admin_ticket_blacklist_add(discord_id, body.get("reason") or "No reason provided", body.get("actor_id") or body.get("actorId") or 0))
        payload = future.result(timeout=25)
        return jsonify(payload)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/admin/tickets", methods=["GET"])
@require_admin_api_key
def admin_tickets_list():
    try:
        future = _run_on_bot_loop(_admin_tickets_list_payload())
        payload = future.result(timeout=30)
        return jsonify(payload)
    except Exception as exc:
        return jsonify({"error": str(exc), "tickets": []}), 500


@app.route("/admin/tickets/<ticket_id>", methods=["GET"])
@require_admin_api_key
def admin_ticket_detail(ticket_id):
    try:
        future = _run_on_bot_loop(_admin_ticket_detail_payload(ticket_id))
        ticket = future.result(timeout=15)
        if not ticket:
            return jsonify({"error": "Ticket not found"}), 404
        return jsonify({"success": True, "ticket": ticket})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/admin/tickets/panel/send", methods=["POST"])
@require_admin_api_key
def admin_ticket_panel_send():
    body = request.get_json(silent=True) or {}
    try:
        channel_id = body.get("channel_id") or body.get("channelId") or MCWV_TICKET_PANEL_CHANNEL_ID
        title = str(body.get("title") or "MCWV Applications")[:256]
        description = str(body.get("description") or "Ready to apply for MCWV? Open a private application ticket below.")[:4000]
        button_label = str(body.get("button_label") or body.get("buttonLabel") or "Open Application")[:80]
        accent_color = body.get("accent_color") or body.get("accentColor") or body.get("hex_color") or body.get("hexColor")
        thumbnail_url = body.get("thumbnail_url") or body.get("thumbnailUrl") or body.get("thumbnail")
        future = _run_on_bot_loop(_admin_send_ticket_panel(channel_id, title, description, button_label, accent_color, thumbnail_url))
        payload = future.result(timeout=15)
        return jsonify(payload)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


async def _admin_send_ticket_panel(channel_id, title, description, button_label, accent_color=None, thumbnail_url=None):
    channel = bot.get_channel(int(channel_id))
    if channel is None:
        channel = await bot.fetch_channel(int(channel_id))
    if not isinstance(channel, discord.TextChannel):
        raise ValueError("Panel channel must be a text channel")
    settings = get_mcwv_ticket_settings()
    panel = settings.get("panel", {})
    title = title or panel.get("title") or "MCWV Applications"
    description = description or panel.get("description") or "Ready to apply for MCWV? Open a private application ticket below."
    button_label = button_label or panel.get("buttonLabel") or "Open Application"
    color_value = parse_hex_color(accent_color if accent_color is not None else panel.get("accentColor"), panel.get("accentColor", 0x34D399))
    thumbnail = str(thumbnail_url if thumbnail_url is not None else panel.get("thumbnailUrl", "") or "").strip()[:2048]
    thumbnail = thumbnail if thumbnail.startswith("https://") else ""
    embed = discord.Embed(
        title=title,
        description=description,
        color=discord.Color(color_value),
        timestamp=datetime.now(timezone.utc),
    )
    if thumbnail:
        embed.set_thumbnail(url=thumbnail)
    embed.set_footer(text="MCWV Applications")
    message = await channel.send(embed=embed, view=MCWVTicketPanelView(button_label))
    return {"success": True, "channel_id": str(channel.id), "message_id": str(message.id)}


@app.route("/admin/tickets/accept", methods=["POST"])
@require_admin_api_key
def admin_ticket_accept():
    body = request.get_json(silent=True) or {}
    try:
        future = _run_on_bot_loop(_admin_ticket_accept(body.get("ticket_id") or body.get("ticketId"), body.get("actor_id") or 0))
        ticket = future.result(timeout=25)
        return jsonify({"success": True, "ticket": ticket})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/admin/tickets/close", methods=["POST"])
@require_admin_api_key
def admin_ticket_close():
    body = request.get_json(silent=True) or {}
    try:
        future = _run_on_bot_loop(_admin_ticket_close(body.get("ticket_id") or body.get("ticketId"), body.get("actor_id") or 0, body.get("reason") or "Closed from Hub"))
        payload = future.result(timeout=35)
        return jsonify(payload)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/admin/tickets/clear", methods=["POST"])
@require_admin_api_key
def admin_ticket_clear():
    body = request.get_json(silent=True) or {}
    try:
        future = _run_on_bot_loop(_admin_ticket_clear(body.get("actor_id") or body.get("actorId") or 0))
        payload = future.result(timeout=20)
        return jsonify(payload)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/admin/broadcast/access", methods=["POST"])
@require_admin_api_key
def admin_broadcast_access():
    body = request.get_json(silent=True) or {}
    discord_id = body.get("discord_id") or body.get("discordId") or body.get("discord")

    try:
        future = _run_on_bot_loop(_admin_broadcast_access_from_body(discord_id))
        payload = future.result(timeout=10)
        return jsonify(payload)
    except ValueError as exc:
        return jsonify({"allowed": False, "error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"allowed": False, "error": str(exc)}), 500


@app.route("/admin/broadcast/allowed-users", methods=["GET", "POST"])
@require_admin_api_key
def admin_broadcast_allowed_users():
    if request.method == "GET":
        return jsonify({
            "success": True,
            "user_ids": [str(value) for value in sorted(get_broadcast_allowed_user_ids())],
            "env_user_ids": [str(value) for value in sorted(BROADCAST_DEFAULT_USER_IDS)],
        })

    body = request.get_json(silent=True) or {}
    raw = body.get("user_ids") or body.get("userIds") or body.get("ids") or ""
    if isinstance(raw, list):
        ids = _parse_id_set(",".join(str(value) for value in raw))
    else:
        ids = _parse_id_set(raw)
    saved = set_broadcast_allowed_user_ids(ids)
    return jsonify({"success": True, "user_ids": [str(value) for value in saved]})


@app.route("/admin/broadcast/preview", methods=["POST"])
@require_admin_api_key
def admin_broadcast_preview():
    body = request.get_json(silent=True) or {}
    try:
        future = _run_on_bot_loop(_admin_broadcast_preview_from_body(body))
        payload = future.result(timeout=25)
        return jsonify(payload)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/admin/broadcast/send", methods=["POST"])
@require_admin_api_key
def admin_broadcast_send():
    body = request.get_json(silent=True) or {}
    try:
        future = _run_on_bot_loop(_admin_broadcast_send_from_body(body))
        payload = future.result(timeout=120)
        return jsonify(payload)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/admin/status")
@require_admin_api_key
def admin_status():
    global LAST_HEARTBEAT
    LAST_HEARTBEAT = _now_iso()

    active_invite = _active_invite_payload()
    active_giveaway = _active_giveaway_payload()
    metrics = _process_metrics()
    summary = _bot_summary()
    uptime = int(time.time() - STARTED_AT)

    return jsonify({
        "success": True,
        "overview": {
            "botStatus": "Online" if summary["ready"] else "Starting",
            "uptimeSeconds": uptime,
            "lastHeartbeat": LAST_HEARTBEAT,
            "databaseStatus": "Connected" if _db_connected() else "Disconnected",
            "trackedPlayers": _tracked_players(),
            "activeGiveaway": bool(active_giveaway and active_giveaway.get("active")),
            "activeInviteEvent": bool(active_invite and active_invite.get("active")),
            "currentWar": globals().get("PS99_CURRENT_WAR_NAME") if globals().get("ps99_war_active") else None,
        },
        "bot": {
            **summary,
            **metrics,
            "uptimeSeconds": uptime,
            "lastHeartbeat": LAST_HEARTBEAT,
            "commandsExecuted": COMMANDS_EXECUTED,
            "queueLengths": {},
            "reminderInterval": globals().get("reminder_interval"),
            "reminderChannel": globals().get("reminder_channel_id"),
            "placementChannel": _safe_call("get_placement_channel_id", None),
            "placementAlertsEnabled": _safe_call("placement_alerts_enabled", False),
            "clanLogChannel": _safe_call("get_clan_log_channel_id", None),
            "clanLogsEnabled": _safe_call("clan_logs_enabled", False),
            "hourlyStatsChannel": _safe_call("get_hourly_stats_channel_id", None),
            "hourlyStatsEnabled": _safe_call("hourly_stats_enabled", False),
            "hourlyStatsAutoWarToggle": bool(globals().get("MCWV_HOURLY_STATS_AUTO_WAR_TOGGLE")),
            "hourlyStatsAutoDisabled": _safe_call("hourly_stats_auto_disabled", False),
            "hourlyStatsIntervalMinutes": globals().get("MCWV_HOURLY_STATS_INTERVAL_MINUTES"),
            "hourlyStatsPingEnabled": _safe_call("hourly_stats_ping_enabled", False),
            "hourlyStatsPingThreshold": _safe_call("get_hourly_stats_ping_threshold", MCWV_HOURLY_STATS_PING_THRESHOLD_DEFAULT),
            "hourlyStatsStartTime": _safe_call("get_hourly_stats_start_time", MCWV_HOURLY_STATS_START_TIME_DEFAULT),
            "hourlyStatsPingMessage": _safe_call("get_hourly_stats_ping_message", MCWV_HOURLY_STATS_PING_MESSAGE_DEFAULT),
            "hourlyStatsLastSentAt": _safe_call("db_get_setting", None, "mcwv_hourly_stats_last_sent_at"),
        },
        "loops": {
            "War Poll Loop": _loop_status("war_poll_loop"),
            "War Collector Loop": _loop_status("hub_war_collect_loop"),
            "Clan Log Loop": _loop_status("clan_log_loop"),
            "Hourly Stats Loop": _loop_status("hourly_stats_loop"),
            "Hourly Player Snapshot Loop": _loop_status("hourly_player_snapshot_loop"),
            "Ticket Screenshot Reminder Loop": _loop_status("ticket_screenshot_reminder_loop"),
            "Presence Loop": _loop_status("check_loop"),
            "Reminder Loop": _loop_status("reminder_loop"),
            "Invite Event Loop": _loop_status("check_invite_event"),
            "Giveaway Loop": _loop_status("check_giveaway_event"),
            "Clan Leave Loop": _loop_status("clan_leave_loop"),
            "Invite Cache": {"status": "Healthy" if globals().get("INVITE_SYSTEM_READY") else "Starting"},
            "Database": {"status": "Connected" if _db_connected() else "Disconnected"},
        },
        "invites": active_invite,
        "giveaways": active_giveaway,
        "logs": ADMIN_LOGS[:20],
    })


async def _admin_fetch_presence(user_ids):
    global session

    presence_by_id = {}

    if not user_ids:
        return presence_by_id

    if session is None or getattr(session, "closed", False):
        session = aiohttp.ClientSession()

    timeout = aiohttp.ClientTimeout(total=12)

    for index in range(0, len(user_ids), 100):
        chunk = user_ids[index:index + 100]

        try:
            async with session.post(
                "https://presence.roblox.com/v1/presence/users",
                json={"userIds": chunk},
                timeout=timeout,
            ) as response:
                if response.status != 200:
                    print(f"[admin players] Roblox presence HTTP {response.status}")
                    continue

                data = await response.json(content_type=None)
        except Exception as exc:
            print(f"[admin players] Roblox presence request failed: {exc}")
            continue

        for presence in data.get("userPresences", []) if isinstance(data, dict) else []:
            try:
                user_id = str(presence.get("userId", "")).strip()
                if user_id:
                    presence_by_id[user_id] = presence
            except Exception:
                continue

        await asyncio.sleep(0.2)

    return presence_by_id


async def _admin_players_payload():
    rows = _safe_call("db_get_all_tracked", []) or []
    roblox_ids = []

    for row in rows:
        try:
            roblox_ids.append(int(str(row[0]).strip()))
        except Exception:
            continue

    presence_by_id = await _admin_fetch_presence(roblox_ids)
    players = []

    for row in rows:
        roblox_id = str(row[0]).strip() if len(row) > 0 else ""
        discord_id = str(row[1]).strip() if len(row) > 1 else ""
        username = str(row[2]).strip() if len(row) > 2 else roblox_id
        presence = presence_by_id.get(roblox_id)

        if presence:
            status_value = presence.get("userPresenceType")
            try:
                status_value = int(status_value)
                _safe_call("db_set_user_status", None, roblox_id, status_value)
            except Exception:
                status_value = None

            status_label = _safe_call("status_text", "Unknown", status_value) if status_value is not None else "Unknown"
            current_world = presence.get("lastLocation") or "—"
            last_seen = presence.get("lastOnline") or None
        else:
            status_value = _safe_call("db_get_user_status", None, roblox_id)
            status_label = _safe_call("status_text", "Unknown", status_value) if status_value is not None else "Unknown"
            current_world = "—"
            last_seen = None

        players.append({
            "id": roblox_id,
            "robloxId": roblox_id,
            "roblox_id": roblox_id,
            "username": username,
            "discord": discord_id,
            "discord_id": discord_id,
            "status": status_label,
            "currentWorld": current_world,
            "current_world": current_world,
            "lastSeen": last_seen,
            "last_seen": last_seen,
            "clanRank": "—",
            "clan_rank": "—",
            "points": 0,
            "avatar": None,
        })

    alts = _safe_call("db_get_all_alts", []) or []
    links = [
        {
            "discord_id": str(row[0]) if len(row) > 0 else "",
            "roblox_id": str(row[1]) if len(row) > 1 else "",
            "username": str(row[2]) if len(row) > 2 else "Alt",
        }
        for row in alts
    ]

    return {"success": True, "source": "bot-live-presence", "players": players, "links": links}


@app.route("/admin/players")
@require_admin_api_key
def admin_players():
    try:
        future = _run_on_bot_loop(_admin_players_payload())
        payload = future.result(timeout=25)
        return jsonify(payload)
    except Exception as exc:
        print("[admin players] error:", exc)
        return jsonify({"error": str(exc), "players": [], "links": []}), 500


@app.route("/admin/invites")
@require_admin_api_key
def admin_invites():
    active = _active_invite_payload()
    leaderboard_rows = db_fetchall(
        "SELECT user_id, invites FROM invite_counts ORDER BY invites DESC, user_id ASC LIMIT 100"
    )
    invited_rows = db_fetchall(
        "SELECT member_id, inviter_id FROM invite_member_links ORDER BY member_id DESC LIMIT 200"
    )

    leaderboard = [
        {"user_id": str(row["user_id"]), "invites": int(row["invites"] or 0)}
        for row in leaderboard_rows
    ]
    invited_members = [
        {"member_id": str(row["member_id"]), "inviter_id": str(row["inviter_id"])}
        for row in invited_rows
    ]

    events = []
    if active:
        events.append({
            "id": active.get("id", 1),
            "name": "Invite Event",
            "status": "Active" if active.get("active") else "Ended",
            "active": active.get("active"),
            "start_time": active.get("start_time"),
            "end_time": active.get("end_time"),
            "invites": sum(item["invites"] for item in leaderboard),
            "reward": active.get("reward") or "Giveaway entries",
        })

    return jsonify({
        "success": True,
        "source": "bot",
        "active": events[0] if events else None,
        "events": events,
        "leaderboard": leaderboard,
        "invitedMembers": invited_members,
        "fakeInvitesRemoved": 0,
    })


@app.route("/admin/giveaways")
@require_admin_api_key
def admin_giveaways():
    active = _active_giveaway_payload()
    giveaways = []

    if active:
        invites_per_entry = max(1, int(active.get("invites_per_entry") or 2))
        counts = db_fetchall("SELECT user_id, invites FROM invite_counts ORDER BY invites DESC")
        entries = sum(max(0, int(row["invites"] or 0)) // invites_per_entry for row in counts)
        giveaways.append({
            "id": active.get("id", 1),
            "prize": active.get("prize") or "Unknown prize",
            "active": active.get("active"),
            "entries": entries,
            "end_time": active.get("end_time"),
            "winners": int(active.get("winners") or 1),
            "winnerCount": int(active.get("winners") or 1),
            "linkedInviteEvent": "Invite Event",
        })

    return jsonify({
        "success": True,
        "source": "bot",
        "active": giveaways[0] if giveaways else None,
        "giveaways": giveaways,
    })


@app.route("/admin/logs")
@require_admin_api_key
def admin_logs():
    return jsonify({"success": True, "source": "bot", "logs": ADMIN_LOGS[:200]})


@app.route("/admin/sync", methods=["POST"])
@require_admin_api_key
def admin_sync():
    body = request.get_json(silent=True) or {}
    target = str(body.get("target") or body.get("action") or "war").lower()

    if target == "profiles":
        globals().get("PROFILE_CACHE", {}).clear()
        admin_log("Profiles Refreshed", "Profile cache cleared from admin panel")
        return jsonify({"success": True, "message": "Profile cache cleared"})

    if target == "presence":
        try:
            _run_on_bot_loop(run_initial_presence_check())
            admin_log("Presence Sync", "Presence check queued from admin panel")
            return jsonify({"success": True, "message": "Presence check queued"})
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    if target == "war":
        try:
            _run_on_bot_loop(_restart_loop_from_admin("war_poll_loop"))
            admin_log("War Sync", "War poll loop restart queued from admin panel")
            return jsonify({"success": True, "message": "War sync queued"})
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    # Backwards-compatible admin actions through the already-existing /admin/sync
    # route. This avoids 404s on hosts that are still using the older route list.
    if target == "setup":
        try:
            future = _run_on_bot_loop(_admin_setup_system_from_body(body))
            payload = future.result(timeout=15)
            admin_log("Setup Updated", payload.get("message", "Bot system setup updated"))
            return jsonify(payload)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    if target in ("hourly_stats", "hourly_stats_send", "send_hourly_stats"):
        try:
            future = _run_on_bot_loop(_admin_send_hourly_stats_from_body(body))
            payload = future.result(timeout=45)
            admin_log("Hourly Stats Sent", payload.get("message", "Hourly stats sent"))
            return jsonify(payload)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    return jsonify({"error": f"Unknown sync target: {target}"}), 400


def _admin_bool(value, default=False):
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on", "enabled")


def _admin_int(value, default=0, minimum=0, maximum=None):
    try:
        parsed = int(float(str(value).strip()))
    except Exception:
        parsed = int(default)
    parsed = max(int(minimum), parsed)
    if maximum is not None:
        parsed = min(int(maximum), parsed)
    return parsed


def _hourly_stats_loop_obj_start():
    loop_obj = globals().get("hourly_stats_loop")
    if loop_obj is not None:
        loop_obj.change_interval(minutes=1)
        if not loop_obj.is_running():
            loop_obj.start()


async def _admin_setup_system_from_body(body):
    system = str(body.get("system") or body.get("target") or "").strip().lower().replace("-", "_")
    channel_id = body.get("channel_id") or body.get("channelId") or body.get("channel")
    enabled_raw = body.get("enabled", body.get("isEnabled", body.get("active", None)))

    if system in ("placement", "placement_alert", "placement_alerts"):
        # Enable/disable toggle from the Dashboard. Channel is not required.
        if enabled_raw is not None and not channel_id:
            enabled = _admin_bool(enabled_raw, True)
            db_set_setting("mcwv_placement_alerts_enabled", "1" if enabled else "0")
            if enabled and not get_placement_channel_id():
                raise ValueError("Pick a channel first before enabling placement alerts.")
            return {
                "success": True,
                "system": "placement_alerts",
                "enabled": enabled,
                "message": f"Placement alerts {'enabled' if enabled else 'disabled'}.",
            }

        channel = await _validate_admin_text_channel(channel_id, require_invite=False)
        set_placement_channel_id(channel.id)
        db_set_setting("mcwv_placement_alerts_enabled", "1")
        return {"success": True, "system": "placement_alerts", "channel_id": str(channel.id), "enabled": True, "message": f"Placement alerts configured for #{channel.name}."}

    if system in ("clan_log", "clan_logs", "logs"):
        if enabled_raw is not None and not channel_id:
            enabled = _admin_bool(enabled_raw, True)
            db_set_setting("mcwv_clan_logs_enabled", "1" if enabled else "0")
            if enabled and not get_clan_log_channel_id():
                raise ValueError("Pick a channel first before enabling clan logs.")
            return {
                "success": True,
                "system": "clan_logs",
                "enabled": enabled,
                "message": f"Clan logs {'enabled' if enabled else 'disabled'}.",
            }

        channel = await _validate_admin_text_channel(channel_id, require_invite=False)
        set_clan_log_channel_id(channel.id)
        db_set_setting("mcwv_clan_logs_enabled", "1")
        return {"success": True, "system": "clan_logs", "channel_id": str(channel.id), "enabled": True, "message": f"Clan logs configured for #{channel.name}."}

    if system in ("hourly", "hourly_stats", "hourly_statistics"):
        # Pure enable/disable toggle from the Dashboard button. Manual toggle
        # always wins over the auto war pause/resume.
        if enabled_raw is not None and not channel_id:
            enabled = _admin_bool(enabled_raw, True)
            if enabled and not get_hourly_stats_channel_id():
                raise ValueError("Pick a channel first before enabling hourly stats.")
            set_hourly_stats_enabled(enabled, auto_disabled=False)
            if enabled:
                _hourly_stats_loop_obj_start()
            return {
                "success": True,
                "system": "hourly_stats",
                "enabled": enabled,
                "message": f"Hourly stats {'enabled' if enabled else 'disabled'}.",
            }

        channel = await _validate_admin_text_channel(channel_id, require_invite=False)
        set_hourly_stats_channel_id(channel.id)
        # Configuring the channel explicitly also turns hourly stats on.
        set_hourly_stats_enabled(True, auto_disabled=False)

        if "ping_enabled" in body or "pingEnabled" in body:
            ping_enabled = _admin_bool(body.get("ping_enabled", body.get("pingEnabled")), False)
            db_set_setting("mcwv_hourly_stats_ping_enabled", "1" if ping_enabled else "0")

        if "ping_threshold" in body or "pingThreshold" in body:
            threshold = _admin_int(
                body.get("ping_threshold", body.get("pingThreshold")),
                MCWV_HOURLY_STATS_PING_THRESHOLD_DEFAULT,
                minimum=0,
                maximum=1_000_000_000,
            )
            db_set_setting("mcwv_hourly_stats_ping_threshold", threshold)

        if "start_time" in body or "startTime" in body:
            start_time = normalize_hourly_start_time(body.get("start_time", body.get("startTime")))
            db_set_setting("mcwv_hourly_stats_start_time", start_time)

        if "ping_message" in body or "pingMessage" in body:
            message = str(body.get("ping_message", body.get("pingMessage")) or "")[:1200]
            db_set_setting("mcwv_hourly_stats_ping_message", message)

        _hourly_stats_loop_obj_start()

        ping_text = ""
        if _admin_bool(body.get("ping_enabled", body.get("pingEnabled")), hourly_stats_ping_enabled()):
            ping_text = f" Pings enabled under {get_hourly_stats_ping_threshold()} PPH."

        return {"success": True, "system": "hourly_stats", "channel_id": str(channel.id), "enabled": True, "message": f"Hourly stats configured for #{channel.name}.{ping_text}"}

    raise ValueError("Unknown setup system. Use placement_alerts, clan_logs, or hourly_stats.")


@app.route("/admin/setup", methods=["POST"])
@require_admin_api_key
def admin_setup_system():
    body = request.get_json(silent=True) or {}
    try:
        future = _run_on_bot_loop(_admin_setup_system_from_body(body))
        payload = future.result(timeout=15)
        admin_log("Setup Updated", payload.get("message", "Bot system setup updated"))
        return jsonify(payload)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


async def _admin_send_hourly_stats_from_body(body):
    channel_id = body.get("channel_id") or body.get("channelId") or body.get("channel") or get_hourly_stats_channel_id()
    channel = await _validate_admin_text_channel(channel_id, require_invite=False)
    ping_enabled = body.get("ping_enabled", body.get("pingEnabled", None))
    ping_threshold = body.get("ping_threshold", body.get("pingThreshold", None))
    ping_message = body.get("ping_message", body.get("pingMessage", None))
    await send_hourly_stats_card(
        channel,
        ping_enabled=_admin_bool(ping_enabled, hourly_stats_ping_enabled()) if ping_enabled is not None else None,
        ping_threshold=_admin_int(ping_threshold, get_hourly_stats_ping_threshold(), minimum=0, maximum=1_000_000_000) if ping_threshold is not None else None,
        ping_message=str(ping_message)[:1200] if ping_message is not None else None,
    )
    return {"success": True, "channel_id": str(channel.id), "message": f"Hourly stats sent in #{channel.name}."}


@app.route("/admin/hourly-stats/send", methods=["POST"])
@require_admin_api_key
def admin_hourly_stats_send():
    body = request.get_json(silent=True) or {}
    try:
        future = _run_on_bot_loop(_admin_send_hourly_stats_from_body(body))
        payload = future.result(timeout=45)
        admin_log("Hourly Stats Sent", payload.get("message", "Hourly stats sent"))
        return jsonify(payload)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/admin/giveaway/end", methods=["POST"])
@require_admin_api_key
def admin_giveaway_end():
    try:
        _run_on_bot_loop(finish_giveaway("admin panel"))
        admin_log("Giveaway Ended", "Giveaway end queued from admin panel")
        return jsonify({"success": True, "message": "Giveaway end queued"})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/admin/giveaway/cancel", methods=["POST"])
@require_admin_api_key
def admin_giveaway_cancel():
    db_exec("UPDATE giveaway_events SET active = 0 WHERE id = 1")
    admin_log("Giveaway Cancelled", "Giveaway cancelled from admin panel", "warning")
    return jsonify({"success": True, "message": "Giveaway cancelled"})


@app.route("/admin/giveaway/reroll", methods=["POST"])
@require_admin_api_key
def admin_giveaway_reroll():
    return jsonify({"error": "Giveaway reroll is not implemented in the bot yet"}), 501


async def _create_giveaway_from_admin(body):
    prize = str(body.get("prize") or "").strip()
    if not prize:
        raise ValueError("Prize is required")

    winners = max(1, int(body.get("winners") or body.get("winner_count") or 1))
    invites_per_entry = max(1, int(body.get("invites_per_entry") or 2))
    duration_minutes = max(1, int(body.get("duration_minutes") or 60))
    now = int(time.time())
    end_time = int(body.get("end_time") or (now + duration_minutes * 60))
    channel_id = int(body.get("channel_id") or 0)
    channel = await _validate_admin_text_channel(channel_id, require_invite=False)
    thumbnail = str(body.get("thumbnail") or "")

    db_exec("""
        INSERT INTO giveaway_events (
            id, active, prize, winners, invites_per_entry,
            start_time, end_time, channel_id, message_id,
            thumbnail, created_by
        ) VALUES (1, 1, %s, %s, %s, %s, %s, %s, 0, %s, 0)
        ON CONFLICT(id) DO UPDATE SET
            active = excluded.active,
            prize = excluded.prize,
            winners = excluded.winners,
            invites_per_entry = excluded.invites_per_entry,
            start_time = excluded.start_time,
            end_time = excluded.end_time,
            channel_id = excluded.channel_id,
            thumbnail = excluded.thumbnail,
            created_by = excluded.created_by
    """, (prize, winners, invites_per_entry, now, end_time, channel_id, thumbnail))

    message_id = 0
    giveaway = get_active_giveaway()
    message = await channel.send(embed=build_giveaway_embed(giveaway), view=GiveawayView())
    message_id = int(message.id)
    db_exec("UPDATE giveaway_events SET message_id = %s WHERE id = 1", (message_id,))

    return {"message_id": message_id, "channel_id": str(channel.id), "end_time": end_time}


@app.route("/admin/giveaway/create", methods=["POST"])
@require_admin_api_key
def admin_giveaway_create():
    body = request.get_json(silent=True) or {}
    try:
        future = _run_on_bot_loop(_create_giveaway_from_admin(body))
        result = future.result(timeout=20)
        admin_log("Giveaway Created", str(body.get("prize") or "Giveaway created"))
        return jsonify({"success": True, "message": "Giveaway created", **result})
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


async def _start_invite_from_admin(body):
    duration_hours = max(1, int(body.get("duration_hours") or 24))
    now = int(time.time())
    end = now + duration_hours * 3600
    channel_id = int(body.get("channel_id") or 0)
    channel = await _validate_admin_text_channel(channel_id, require_invite=True)

    db_exec("DELETE FROM invite_counts")
    db_exec("DELETE FROM invite_used_users")
    db_exec("DELETE FROM invite_cache")
    db_exec("""
        INSERT INTO invite_events (id, active, start_time, end_time, channel_id)
        VALUES (1, 1, %s, %s, %s)
        ON CONFLICT(id) DO UPDATE SET
            active = excluded.active,
            start_time = excluded.start_time,
            end_time = excluded.end_time,
            channel_id = excluded.channel_id
    """, (now, end, channel_id))

    bot_obj = globals().get("bot")
    if bot_obj:
        for guild in bot_obj.guilds:
            await load_invite_snapshot(guild)

    embed = discord.Embed(
        title="📨 Invite Event Started",
        description="Click the buttons below to get your invite link or view your stats.",
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="Ends", value=f"<t:{end}:R>", inline=True)
    await channel.send(embed=embed, view=InviteView())

    return {"channel_id": str(channel.id), "end_time": end}


@app.route("/admin/invite/start", methods=["POST"])
@require_admin_api_key
def admin_invite_start():
    body = request.get_json(silent=True) or {}
    try:
        future = _run_on_bot_loop(_start_invite_from_admin(body))
        result = future.result(timeout=20)
        admin_log("Invite Event Created", "Invite event started from admin panel")
        return jsonify({"success": True, "message": "Invite event started", **result})
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/admin/invite/end", methods=["POST"])
@require_admin_api_key
def admin_invite_end():
    db_exec("UPDATE invite_events SET active = 0 WHERE id = 1")
    admin_log("Invite Event Ended", "Invite event ended from admin panel")
    return jsonify({"success": True, "message": "Invite event ended"})


@app.route("/admin/invite/pause", methods=["POST"])
@require_admin_api_key
def admin_invite_pause():
    db_exec("UPDATE invite_events SET active = 0 WHERE id = 1")
    admin_log("Invite Event Paused", "Invite event paused from admin panel", "warning")
    return jsonify({"success": True, "message": "Invite event paused"})


@app.route("/admin/invite/resume", methods=["POST"])
@require_admin_api_key
def admin_invite_resume():
    db_exec("UPDATE invite_events SET active = 1 WHERE id = 1")
    admin_log("Invite Event Resumed", "Invite event resumed from admin panel")
    return jsonify({"success": True, "message": "Invite event resumed"})


@app.route("/admin/invite/delete", methods=["POST"])
@require_admin_api_key
def admin_invite_delete():
    db_exec("UPDATE invite_events SET active = 0, start_time = 0, end_time = 0 WHERE id = 1")
    db_exec("DELETE FROM invite_counts")
    db_exec("DELETE FROM invite_used_users")
    db_exec("DELETE FROM invite_cache")
    admin_log("Invite Event Deleted", "Invite event reset from admin panel", "warning")
    return jsonify({"success": True, "message": "Invite event deleted"})


async def _resolve_roblox_username_for_admin(username):
    username = str(username or "").strip()

    if not re.fullmatch(r"[A-Za-z0-9_]{3,20}", username):
        raise ValueError("Enter a valid Roblox username.")

    payload = {"usernames": [username], "excludeBannedUsers": False}

    async def _post(client):
        async with client.post(
            "https://users.roblox.com/v1/usernames/users",
            json=payload,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as response:
            if response.status != 200:
                raise ValueError(f"Roblox username lookup failed with HTTP {response.status}.")
            data = await response.json(content_type=None)

        results = data.get("data", []) if isinstance(data, dict) else []
        if not results:
            raise ValueError("Roblox username not found.")

        return {
            "id": str(results[0]["id"]),
            "name": str(results[0]["name"]),
        }

    if session is not None and not getattr(session, "closed", True):
        return await _post(session)

    async with aiohttp.ClientSession() as temp_session:
        return await _post(temp_session)


async def _admin_add_alt_from_body(body):
    discord_id = body.get("discord_id") or body.get("discordId") or body.get("discord")
    roblox_username = body.get("roblox_username") or body.get("robloxUsername") or body.get("username")

    if not discord_id:
        raise ValueError("Discord ID is required.")

    resolved = await _resolve_roblox_username_for_admin(roblox_username)
    ok, message = db_add_alt(discord_id, resolved["id"], resolved["name"])

    if not ok:
        raise ValueError(message)

    return {
        "discord_id": str(discord_id),
        "roblox_id": resolved["id"],
        "username": resolved["name"],
        "message": f"Added {resolved['name']} as an alt.",
    }


@app.route("/admin/player/add-alt", methods=["POST"])
@require_admin_api_key
def admin_player_add_alt():
    body = request.get_json(silent=True) or {}
    try:
        future = _run_on_bot_loop(_admin_add_alt_from_body(body))
        result = future.result(timeout=20)
        admin_log("Alt Added", f"{result['username']} added as an alt for {result['discord_id']}")
        return jsonify({"success": True, **result})
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/admin/player/sync", methods=["POST"])
@require_admin_api_key
def admin_player_sync():
    body = request.get_json(silent=True) or {}
    roblox_id = body.get("roblox_id") or body.get("robloxId")
    if roblox_id:
        globals().get("PROFILE_CACHE", {}).pop(str(roblox_id), None)
        admin_log("Player Synced", f"Profile cache cleared for {roblox_id}")
        return jsonify({"success": True, "message": "Player sync queued"})
    return jsonify({"success": True, "message": "No Roblox ID supplied; nothing to sync"})


@app.route("/admin/player/remove", methods=["POST"])
@require_admin_api_key
def admin_player_remove():
    body = request.get_json(silent=True) or {}
    discord_id = body.get("discord_id") or body.get("discord")
    roblox_id = body.get("roblox_id") or body.get("robloxId")

    if discord_id:
        if _safe_call("db_is_owner_discord", False, discord_id):
            return jsonify({"error": "Owner accounts cannot be removed from the database or Roblox links."}), 400

        ok, message = _safe_call("db_remove_all_links_for_discord", (False, "Failed to remove player links."), discord_id)
        if not ok:
            return jsonify({"error": message}), 400
        admin_log("Player Removed", f"Removed links for Discord {discord_id}", "warning")
        return jsonify({"success": True, "message": message})

    if roblox_id:
        link = _safe_call("db_find_roblox_link", None, roblox_id)
        if not link or not link.get("discord_id"):
            return jsonify({"error": "Roblox link not found"}), 404

        if _safe_call("db_is_owner_discord", False, link.get("discord_id")):
            return jsonify({"error": "Owner accounts cannot be removed from the database or Roblox links."}), 400

        if link.get("kind") == "alt":
            ok, _removed_id, message = _safe_call(
                "db_remove_alt",
                (False, None, "Failed to remove alt."),
                link.get("discord_id"),
                roblox_id,
            )
            if not ok:
                return jsonify({"error": message}), 400
            admin_log("Alt Removed", f"Removed alt Roblox {roblox_id}", "warning")
            return jsonify({"success": True, "message": message})

        ok, message = _safe_call("db_remove_all_links_for_discord", (False, "Failed to remove player links."), link.get("discord_id"))
        if not ok:
            return jsonify({"error": message}), 400
        admin_log("Player Removed", f"Removed links for Roblox {roblox_id}", "warning")
        return jsonify({"success": True, "message": message})

    return jsonify({"error": "discord_id or roblox_id is required"}), 400


@app.route("/admin/restart", methods=["POST"])
@require_admin_api_key
def admin_restart():
    body = request.get_json(silent=True) or {}
    if not body.get("confirm"):
        return jsonify({"error": "Set confirm=true to restart"}), 400
    if not ADMIN_RESTART_ENABLED:
        return jsonify({"error": "Restart is disabled. Set ALLOW_ADMIN_RESTART=1 to enable it."}), 403

    admin_log("Bot Restart", "Restart requested from admin panel", "warning")

    def delayed_exit():
        time.sleep(1.5)
        os._exit(0)

    Thread(target=delayed_exit, daemon=True).start()
    return jsonify({"success": True, "message": "Bot restart scheduled"})

def run_web():
    port = int(os.environ.get("PORT", 10000))
    routes = sorted(str(rule) for rule in app.url_map.iter_rules())
    print("🌐 Flask routes registered:", ", ".join(routes))
    app.run(host="0.0.0.0", port=port)

Thread(target=run_web, daemon=True).start()
        
def get_available_category(guild):
    for cid in CLAN_MEMBER_CATEGORY_IDS:
        category = guild.get_channel(cid)

        if not category:
            continue

        if len(category.channels) < 50:
            return category

    return None

INVITE_SNAPSHOTS = {}
INVITE_SYSTEM_READY = False

DB_PATH = "bot.db"  # legacy name kept for reference only — event data lives in Postgres now

# ---- Event tables (invites/giveaways) live in Postgres (Supabase), NOT SQLite. ----
# Render's disk is ephemeral: the old bot.db was wiped on every deploy, so invite
# counts / giveaways / invite-cache kept vanishing. These helpers are thread-safe:
# the bot loop uses the single global `conn`; Flask request threads get their own
# per-thread connection so psycopg2 never crosses threads.
DATABASE_URL = os.environ.get("DATABASE_URL")
conn = None  # single persistent bot-loop connection (created lazily by ensure_db_connection)

import threading as _threading

_event_local = _threading.local()


def _event_conn():
    """Connection for event-table queries (invites/giveaways)."""
    if not DATABASE_URL:
        return None
    try:
        if _threading.current_thread() is _threading.main_thread():
            # Bot loop thread — reuse the global connection.
            ensure_db_connection()
            return conn if (conn is not None and conn.closed == 0) else None
        c = getattr(_event_local, "conn", None)
        if c is None or c.closed != 0:
            c = psycopg2.connect(
                DATABASE_URL,
                sslmode="require",
                connect_timeout=5,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=3,
            )
            c.autocommit = True
            _event_local.conn = c
        return c
    except Exception as exc:
        print("_event_conn error:", exc)
        return None


def close_event_conn():
    """Close the current thread's event-table connection (Flask request teardown)."""
    if _threading.current_thread() is _threading.main_thread():
        return  # never close the bot loop's global connection
    c = getattr(_event_local, "conn", None)
    if c is not None:
        try:
            c.close()
        except Exception:
            pass
        _event_local.conn = None


def db_exec(query, params=()):
    c = _event_conn()
    if c is None:
        return
    try:
        with c.cursor() as cur:
            cur.execute(query, params)
    except Exception as exc:
        print("db_exec error:", exc)
        try:
            c.rollback()
        except Exception:
            pass


def db_fetchone(query, params=()):
    c = _event_conn()
    if c is None:
        return None
    try:
        with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchone()
    except Exception as exc:
        print("db_fetchone error:", exc)
        try:
            c.rollback()
        except Exception:
            pass
        return None


def db_fetchall(query, params=()):
    c = _event_conn()
    if c is None:
        return []
    try:
        with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()
    except Exception as exc:
        print("db_fetchall error:", exc)
        try:
            c.rollback()
        except Exception:
            pass
        return []


def init_invite_tables():
    db_exec("""
    CREATE TABLE IF NOT EXISTS invite_events (
        id BIGINT PRIMARY KEY,
        active INTEGER DEFAULT 0,
        start_time BIGINT DEFAULT 0,
        end_time BIGINT DEFAULT 0,
        channel_id BIGINT
    )
    """)

    db_exec("""
    CREATE TABLE IF NOT EXISTS invite_counts (
        user_id BIGINT PRIMARY KEY,
        invites INTEGER DEFAULT 0
    )
    """)

    db_exec("""
    CREATE TABLE IF NOT EXISTS invite_used_users (
        user_id BIGINT PRIMARY KEY
    )
    """)

    db_exec("""
    CREATE TABLE IF NOT EXISTS invite_cache (
        invite_code TEXT PRIMARY KEY,
        inviter_id BIGINT
    )
    """)

    db_exec("""
    INSERT INTO invite_events (id, active, start_time, end_time, channel_id)
    VALUES (1, 0, 0, 0, 0)
    ON CONFLICT (id) DO NOTHING
    """)


def get_giveaway_row():
    return db_fetchone("SELECT * FROM giveaway_events WHERE id = 1")


def get_active_giveaway():
    giveaway = get_giveaway_row()
    if giveaway and int(giveaway["active"] or 0) == 1:
        return giveaway
    return None


def get_active_event():
    return db_fetchone("SELECT * FROM invite_events WHERE id = 1")


def increment_invite_count(user_id: int, amount: int = 1):
    db_exec("""
        INSERT INTO invite_counts (user_id, invites)
        VALUES (%s, %s)
        ON CONFLICT(user_id)
        DO UPDATE SET invites = invite_counts.invites + %s
    """, (user_id, amount, amount))


def get_invite_channel(guild, bot):
    if not bot.user:
        return None

    me = guild.get_member(bot.user.id)
    if not me:
        return None

    for ch in guild.text_channels:
        perms = ch.permissions_for(me)
        if perms.create_instant_invite and perms.send_messages:
            return ch

    return None


async def load_invite_snapshot(guild: discord.Guild):
    try:
        invites = await guild.invites()
    except Exception as e:
        print(f"[invite system] Failed to load invites for {guild.id}: {e}")
        INVITE_SNAPSHOTS[guild.id] = {}
        return

    INVITE_SNAPSHOTS[guild.id] = {
        inv.code: int(inv.uses or 0)
        for inv in invites
    }


def force_sync_giveaway_state():
    giveaway = get_giveaway_row()
    if not giveaway:
        return

    # only repair the row if needed, do NOT end it here
    if int(giveaway["active"] or 0) not in (0, 1):
        db_exec("UPDATE giveaway_events SET active = 0 WHERE id = 1")
        print("🛠️ Fixed invalid giveaway active state")

ROBLOX_USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{3,20}$")

def _is_strict_username(text: str) -> bool:
    text = text.strip()
    return bool(ROBLOX_USERNAME_RE.fullmatch(text))


def _parse_alt_input(raw: str):
    raw = raw.strip()

    if raw == "none":
        return []

    parts = [p.strip() for p in raw.split(",")]
    if not parts or any(not p for p in parts):
        return None

    if any(not ROBLOX_USERNAME_RE.fullmatch(p) for p in parts):
        return None

    seen = set()
    cleaned = []

    for p in parts:
        key = p.lower()
        if key not in seen:
            seen.add(key)
            cleaned.append(p)

    return cleaned

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
    empty_bundle = ({}, {}, {}, {})

    cached = PROFILE_CACHE.get(user_id)
    cached_data = cached[0] if cached else empty_bundle
    cached_expiry = cached[1] if cached else 0

    # ---------------- CACHE ----------------
    if not force and cached and now < cached_expiry:
        return cached_data

    # Safety: don't try to fetch if session isn't ready
    if session is None or getattr(session, "closed", False):
        print(f"[get_profile_bundle] session not ready for {user_id}")
        return cached_data if cached else empty_bundle

    timeout = aiohttp.ClientTimeout(total=10)
    url = f"{PS99_API}/v1/players/{user_id}?include=profile,inventory,extendedProfile"

    try:
        async with session.get(url, timeout=timeout) as r:
            if r.status != 200:
                print(f"[get_profile_bundle] bad status for {user_id}: {r.status}")
                return cached_data if cached else empty_bundle

            data = await r.json(content_type=None)

    except Exception as e:
        print(f"[get_profile_bundle] request failed for {user_id}: {e}")
        return cached_data if cached else empty_bundle

    # ---------------- SAFE ROOT HANDLING ----------------
    root = data.get("data", {}) if isinstance(data, dict) else {}

    if not isinstance(root, dict):
        print(f"[get_profile_bundle] bad root for {user_id}: {root}")
        return cached_data if cached else empty_bundle

    account = root.get("account", {})
    if not isinstance(account, dict):
        account = {}

    views = root.get("views", {})
    if not isinstance(views, dict):
        views = {}

    public_views = account.get("publicViews", {})
    if not isinstance(public_views, dict):
        public_views = {}

    def extract_view(view_name: str):
        view = views.get(view_name, {})
        if not isinstance(view, dict):
            print(f"[get_profile_bundle] {view_name} not a dict for {user_id}")
            return {}

        data_block = view.get("data", {})
        reason = view.get("reason")

        # If real data exists, use it
        if isinstance(data_block, dict) and data_block:
            return data_block

        # If the API says it is available, still allow an empty dict through safely
        if view.get("available") is True:
            return data_block if isinstance(data_block, dict) else {}

        # Log why it wasn't available
        if reason:
            print(f"[get_profile_bundle] {view_name} unavailable for {user_id}: {reason}")

        return {}

    profile_data = extract_view("profile")
    inventory_data = extract_view("inventory")
    extended_data = extract_view("extendedProfile")

    bundle = (extended_data, profile_data, inventory_data, public_views)

    # ---------------- CACHE ONLY VALID DATA ----------------
    if profile_data or inventory_data or extended_data:
        PROFILE_CACHE[user_id] = (bundle, now + CACHE_TTL)
        return bundle

    # If the API gives nothing useful, keep older cached data if we have it
    if cached:
        return cached_data

    return bundle
    
def _running_loop_present():
    """True when the current thread hosts a running asyncio event loop."""
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False


def ensure_db_connection():
    """Open/reopen the shared DB connection.

    NEVER blocks the Discord event loop: when called on the bot's loop thread
    with a dead connection, it schedules a background heal instead of doing a
    (potentially multi-second) synchronous connect. Blocking connects are only
    performed on worker threads (asyncio.to_thread / Flask request threads)."""
    global conn

    if conn is not None and conn.closed == 0:
        return conn

    if _running_loop_present():
        _schedule_db_heal()
        return None

    try:
        print("🔄 Reconnecting to database...")
        conn = psycopg2.connect(
            DATABASE_URL,
            sslmode="require",
            connect_timeout=5,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=3,
        )
        conn.autocommit = True
        print("✅ Database connected")
        return conn
    except Exception as e:
        print("DB reconnect failed:", repr(e))
        conn = None
        return None


async def ensure_db_connection_async():
    """Reconnect from an async context without blocking the event loop."""
    if conn is not None and conn.closed == 0:
        return conn
    return await asyncio.to_thread(ensure_db_connection)

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
                WHERE NOT EXISTS (
                    SELECT 1 FROM mcwv_loa_records l
                    WHERE l.active = TRUE
                      AND l.roblox_id = TRIM(t.roblox_id)
                )
                ORDER BY discord_id, username
            """)
            return cur.fetchall()
    except Exception as e:
        conn.rollback()
        if "mcwv_loa_records" in str(e):
            try:
                init_db_schema()
                return db_get_all_tracked()
            except Exception:
                pass
        print("db_get_all_tracked error:", e)
        return []


# ---------------- MCWV LOA RECORDS ----------------
def db_start_loa(roblox_id, discord_id, roblox_username, ticket_id, ticket_channel_id,
                 ticket_name_before, ticket_category_before, started_by):
    """Create an active LOA record. Returns (ok, record_id_or_error)."""
    if not db_enabled():
        return False, "Database is not available."
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO mcwv_loa_records
                    (roblox_id, discord_id, roblox_username, ticket_id, ticket_channel_id,
                     ticket_name_before, ticket_category_before, started_by, started_at, active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), TRUE)
                RETURNING id
            """, (str(roblox_id).strip(), int(discord_id) if discord_id else None,
                  str(roblox_username or ""), str(ticket_id) if ticket_id else None,
                  int(ticket_channel_id) if ticket_channel_id else None,
                  str(ticket_name_before) if ticket_name_before else None,
                  int(ticket_category_before) if ticket_category_before else None,
                  int(started_by) if started_by else None))
            rec_id = cur.fetchone()[0]
        conn.commit()
        return True, rec_id
    except Exception as e:
        conn.rollback()
        print("db_start_loa error:", e)
        return False, f"{type(e).__name__}: {e}"


def db_get_active_loa(roblox_id=None, channel_id=None, discord_id=None):
    """Fetch the active LOA record for a roblox id, ticket channel, or discord id."""
    if not db_enabled():
        return None
    try:
        with conn.cursor() as cur:
            if roblox_id:
                cur.execute("""
                    SELECT id, roblox_id, roblox_username, discord_id, ticket_id, ticket_channel_id,
                           ticket_name_before, ticket_category_before, started_by, started_at
                    FROM mcwv_loa_records
                    WHERE active = TRUE AND roblox_id = %s
                    ORDER BY id DESC LIMIT 1
                """, (str(roblox_id).strip(),))
            elif channel_id:
                cur.execute("""
                    SELECT id, roblox_id, roblox_username, discord_id, ticket_id, ticket_channel_id,
                           ticket_name_before, ticket_category_before, started_by, started_at
                    FROM mcwv_loa_records
                    WHERE active = TRUE AND ticket_channel_id = %s
                    ORDER BY id DESC LIMIT 1
                """, (int(channel_id),))
            elif discord_id:
                cur.execute("""
                    SELECT id, roblox_id, roblox_username, discord_id, ticket_id, ticket_channel_id,
                           ticket_name_before, ticket_category_before, started_by, started_at
                    FROM mcwv_loa_records
                    WHERE active = TRUE AND discord_id = %s
                    ORDER BY id DESC LIMIT 1
                """, (int(discord_id),))
            else:
                return None
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0], "roblox_id": row[1], "roblox_username": row[2],
                "discord_id": row[3], "ticket_id": row[4], "ticket_channel_id": row[5],
                "ticket_name_before": row[6], "ticket_category_before": row[7],
                "started_by": row[8], "started_at": row[9],
            }
    except Exception as e:
        conn.rollback()
        print("db_get_active_loa error:", e)
        return None


def db_end_loa(record_id, ended_by, end_notes=""):
    """Close an active LOA record. Returns (ok, message)."""
    if not db_enabled():
        return False, "Database is not available."
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE mcwv_loa_records
                SET active = FALSE, ended_by = %s, ended_at = NOW(), end_notes = %s
                WHERE id = %s
            """, (int(ended_by) if ended_by else None, str(end_notes or ""), int(record_id)))
        conn.commit()
        return True, "LOA record closed."
    except Exception as e:
        conn.rollback()
        print("db_end_loa error:", e)
        return False, f"{type(e).__name__}: {e}"


def db_list_active_loas():
    """All active LOAs, newest first."""
    if not db_enabled():
        return []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, roblox_id, roblox_username, discord_id, ticket_channel_id, started_by, started_at
                FROM mcwv_loa_records
                WHERE active = TRUE
                ORDER BY started_at DESC
            """)
            return cur.fetchall()
    except Exception as e:
        conn.rollback()
        print("db_list_active_loas error:", e)
        return []


async def perform_loa_revert(guild, record, actor, end_notes=""):
    """Shared End-LOA logic: restore roles + ticket channel, close the DB record,
    clear caches. Returns (ok, notes)."""
    notes = []
    discord_id = int(record["discord_id"]) if record.get("discord_id") else None

    member = guild.get_member(discord_id) if discord_id else None
    if member is None and guild and discord_id:
        try:
            member = await guild.fetch_member(discord_id)
        except Exception:
            member = None

    clan_role = guild.get_role(CLAN_MEMBER_ROLE_ID) if guild else None
    if member and clan_role:
        if clan_role not in member.roles:
            try:
                await member.add_roles(clan_role, reason="MCWV LOA ended — returning member")
                notes.append("Clan member role restored")
            except Exception as exc:
                notes.append(f"Could not re-add clan role: {exc}")
        else:
            notes.append("Clan member role already present")
    else:
        notes.append("Member not in server — role ops skipped")

    loa_role = guild.get_role(MCWV_LOA_ROLE_ID) if guild else None
    if member and loa_role and loa_role in member.roles:
        try:
            await member.remove_roles(loa_role, reason="MCWV LOA ended")
            notes.append("LOA role removed")
        except Exception as exc:
            notes.append(f"Could not remove LOA role: {exc}")

    channel = None
    if guild and record.get("ticket_channel_id"):
        channel = guild.get_channel(int(record["ticket_channel_id"]))
    if isinstance(channel, discord.TextChannel):
        try:
            target_cat = None
            if record.get("ticket_category_before"):
                target_cat = guild.get_channel(int(record["ticket_category_before"]))
            target_name = record.get("ticket_name_before") or channel.name
            await channel.edit(
                category=target_cat if isinstance(target_cat, discord.CategoryChannel) else None,
                name=target_name,
                reason=f"MCWV LOA ended — by {actor}",
            )
            notes.append("Ticket channel restored")
        except Exception as exc:
            notes.append(f"Channel restore failed: {exc}")
    else:
        notes.append("Ticket channel gone — skipped restore")

    ok, msg = db_end_loa(record["id"], actor.id, end_notes)
    notes.append("LOA record closed" if ok else f"DB close failed: {msg}")

    cleanup_memory_for_removed_user(str(record["roblox_id"]))
    return ok, notes


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

    if db_is_owner_discord(did):
        return False, None, "Owner accounts cannot be modified from Roblox Links."

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


def db_is_owner_discord(discord_id):
    if not db_enabled():
        return False

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT role FROM users WHERE discord_id = %s LIMIT 1",
                (int(discord_id),)
            )
            row = cur.fetchone()

        return bool(row and str(row[0] or "").lower() == "owner")
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print("db_is_owner_discord error:", e)
        return False


def db_remove_all_links_for_discord(discord_id):
    if not db_enabled():
        return False, "Database is not available."

    did = int(discord_id)

    try:
        with conn.cursor() as cur:
            if db_is_owner_discord(did):
                return False, "Owner accounts cannot be removed from Roblox Links. Restore or edit the owner manually in the database."

            # Get roblox_ids before deleting for memory cleanup
            cur.execute("SELECT roblox_id FROM user_alts WHERE discord_id = %s", (did,))
            alt_rids = [str(row[0]) for row in cur.fetchall()]
            cur.execute("SELECT roblox_id FROM users WHERE discord_id = %s", (did,))
            main_rids = [str(row[0]) for row in cur.fetchall()]
            cur.execute("DELETE FROM user_alts WHERE discord_id = %s", (did,))
            cur.execute("DELETE FROM users WHERE discord_id = %s", (did,))

        conn.commit()
        # Clean up in-memory caches
        for rid in alt_rids + main_rids:
            cleanup_memory_for_removed_user(rid)
        return True, "Player fully removed (Roblox links + Hub login)."
    except Exception as e:
        conn.rollback()
        print("db_remove_all_links_for_discord error:", e)
        return False, f"Failed to remove player links: {type(e).__name__}: {e}"

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
    def __init__(self, entries, battle_title, total_points, is_active, clan_rank=None, deltas=None, requester_id=None, avatar_url=None, refetch=None):
        super().__init__(timeout=300)
        self.entries = entries
        self.battle_title = battle_title
        self.total_points = total_points
        self.is_active = is_active
        self.clan_rank = clan_rank
        self.deltas = deltas or {}
        self.requester_id = requester_id
        self.avatar_url = avatar_url
        self.refetch = refetch
        self.page = 0
        self._sync_max()

    def _sync_max(self):
        self.max_points = max((e["points"] for e in self.entries), default=1) or 1

    def _total_pages(self):
        return max(1, (len(self.entries) + LEADERBOARD_PAGE_SIZE - 1) // LEADERBOARD_PAGE_SIZE)

    def _page_slice(self):
        start = self.page * LEADERBOARD_PAGE_SIZE
        end = start + LEADERBOARD_PAGE_SIZE
        return self.entries[start:end], start, end

    def _delta_txt(self, user_id):
        delta = self.deltas.get(user_id)
        if not delta:
            return ""
        arrow = "▲" if delta > 0 else "▼"
        return f" {arrow} {format_points(abs(delta))}"

    def _build_line(self, entry):
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
        share = (pts / self.total_points * 100) if self.total_points else 0

        discord_part = f" • <@{discord_id}>" if discord_id else ""
        you_part = "⭐ " if self.requester_id and uid == self.requester_id else ""

        return (
            f"{you_part}{medal} [{safe_name}]({profile_url}){discord_part}\n"
            f"`{bar}` **{format_points(pts)}** · {share:.1f}%{self._delta_txt(uid)}"
        )

    def build_embed(self):
        page_entries, start, end = self._page_slice()
        lines = [self._build_line(entry) for entry in page_entries]

        embed = discord.Embed(
            title=f"🏆 {CLAN_NAME} — {self.battle_title}",
            description="\n\n".join(lines) if lines else "No entries on this page.",
            color=discord.Color.red() if self.is_active else discord.Color(MCWV_BRAND_COLOR),
        )
        if self.avatar_url:
            embed.set_thumbnail(url=self.avatar_url)

        embed.add_field(name="🔢 Total Points", value=f"**{format_points(self.total_points)}**", inline=True)
        if self.clan_rank:
            embed.add_field(name=f"{mcwv_rank_flair(self.clan_rank)} Clan Rank", value=f"**#{self.clan_rank}**", inline=True)
        embed.add_field(name="👥 Contributors", value=f"**{len(self.entries)}**", inline=True)
        embed.add_field(name="📄 Page", value=f"**{self.page + 1}/{self._total_pages()}**", inline=True)

        if self.requester_id:
            you = next((e for e in self.entries if e["user_id"] == self.requester_id), None)
            if you:
                you_share = (you["points"] / self.total_points * 100) if self.total_points else 0
                embed.add_field(
                    name="⭐ You",
                    value=(
                        f"**#{you['rank']}** · **{format_points(you['points'])}** · {you_share:.1f}%"
                        f"{self._delta_txt(you['user_id'])}"
                    ),
                    inline=False,
                )

        embed.timestamp = datetime.now(timezone.utc)
        embed.set_footer(text=f"MCWV • PS99 live • Showing {start + 1}-{min(end, len(self.entries))} of {len(self.entries)}")
        return embed

    async def _move_page(self, interaction: discord.Interaction, delta: int):
        self.page = (self.page + delta) % self._total_pages()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(label="◀ Prev", style=discord.ButtonStyle.secondary)
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._move_page(interaction, -1)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.secondary)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._move_page(interaction, 1)

    @discord.ui.button(label="🔄 Refresh", style=discord.ButtonStyle.primary)
    async def refresh_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.refetch:
            return await interaction.response.edit_message(embed=self.build_embed(), view=self)
        await interaction.response.defer()
        try:
            state = await self.refetch()
        except Exception as exc:
            print(f"[leaderboard] refresh failed: {exc}")
            state = None
        if not state:
            return await interaction.followup.send("⚠️ Refresh failed — try again in a moment.", ephemeral=True)
        self.entries = state["entries"]
        self.battle_title = state["battle_title"]
        self.total_points = state["total_points"]
        self.is_active = state["is_active"]
        self.clan_rank = state.get("clan_rank")
        self.deltas = state.get("deltas") or {}
        self.avatar_url = state.get("avatar_url")
        self._sync_max()
        self.page = min(self.page, self._total_pages() - 1)
        await interaction.edit_original_response(embed=self.build_embed(), view=self)


# ---------------- CONFIG ----------------
TOKEN = os.environ.get("DISCORD_TOKEN")

GUILD_ID                  = 1501608673250640055
CHANNEL_ID                = 1514663069639245904
ALLOWED_ROLE_ID           = 1501986357516701827  # staff role (run commands)
CLAN_MEMBER_ROLE_ID       = 1501986780667314246  # given on accept
CLAN_MEMBER_CATEGORY_IDS = [
    1503109089931034785,  # main
    1520511998633185280   # backup
]
MCWV_LOA_CATEGORY_ID = int(os.environ.get("MCWV_LOA_CATEGORY_ID", "1509315307204771900") or "1509315307204771900")
MCWV_LOA_ROLE_ID = int(os.environ.get("MCWV_LOA_ROLE_ID", "1512865451900801085") or "1512865451900801085")
MEMBERS_CHANNEL_ID        = 1509276380674789617  # membership record posted here
LOG_CHANNEL_ID            = 1502001938705682622  # accept/action log
PS99_API                  = "https://ps99.biggamesapi.io"
ACTIVE_BATTLE_API         = f"{PS99_API}/api/activeClanBattle"
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

@bot.listen("on_app_command_completion")
async def track_admin_command_completion(interaction: discord.Interaction, command: app_commands.Command):
    global COMMANDS_EXECUTED
    COMMANDS_EXECUTED += 1

# Automatic cooldown: 1 command per 3 seconds per user
COOLDOWN_SECONDS = 3
_user_cooldowns = {}

def check_cooldown(interaction: discord.Interaction):
    """Returns retry_after seconds if on cooldown, 0 if OK."""
    uid = interaction.user.id
    now = time.time()
    last = _user_cooldowns.get(uid, 0)
    retry = COOLDOWN_SECONDS - (now - last)
    if retry > 0:
        return retry
    _user_cooldowns[uid] = now
    return 0

session = None
bot_enabled = True
offline_ping_enabled = True
reminder_interval = 30        # minutes between offline reminders
reminder_channel_id = CHANNEL_ID  # channel where reminders are sent
ps99_war_active = False       # tracks last known PS99 war state
ps99_first_check = True       # suppresses announcement on first poll (mid-war startup)

# DATABASE ------------------------------

import psycopg2
from psycopg2.extras import execute_values
import os

DATABASE_URL = os.environ.get("DATABASE_URL")

conn = None


def _schedule_db_heal():
    """Ask a background task to reconnect. Never blocks the event loop."""
    global _db_heal_scheduled
    if _db_heal_scheduled:
        return
    _db_heal_scheduled = True
    try:
        asyncio.get_running_loop().create_task(_db_heal_task())
    except RuntimeError:
        _db_heal_scheduled = False  # no running loop — a later call will retry


async def _db_heal_task():
    global conn, _db_heal_scheduled
    try:
        def _reconnect():
            ensure_db_connection()
            return conn is not None and conn.closed == 0
        ok = await asyncio.to_thread(_reconnect)
        if ok:
            print("✅ Database healed in background")
    except Exception as exc:
        print("DB heal failed:", exc)
    finally:
        _db_heal_scheduled = False


_db_heal_scheduled = False


def db_enabled():
    """True when the shared connection is usable. NEVER blocks the loop on a
    reconnect — if the connection is dead we schedule a background heal and
    return False so commands fail fast with a friendly message instead of
    freezing the whole bot for 10 seconds ("application did not respond")."""
    global conn
    if conn is not None:
        if conn.closed == 0:
            return True
        _schedule_db_heal()
        return False
    if DATABASE_URL:
        _schedule_db_heal()
    return False

# DB connection is opened on demand by db_enabled()/ensure_db_connection().
# We do NOT connect at import time — connect on demand instead.
# Table schema is initialized from init_db_schema() on bot ready instead.
if not DATABASE_URL:
    print("DATABASE_URL not set - running without DB")


def init_db_schema():
    """Create/alter tables once on bot ready. Safe to call multiple times."""
    if not db_enabled():
        return
    try:
        with conn.cursor() as cur:
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
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_status (
                    roblox_id TEXT PRIMARY KEY,
                    status INTEGER,
                    updated_at TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_alts (
                    discord_id BIGINT NOT NULL,
                    roblox_id TEXT NOT NULL,
                    username TEXT NOT NULL,
                    added_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (discord_id, roblox_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS player_presence_events (
                    id BIGSERIAL PRIMARY KEY,
                    roblox_id TEXT NOT NULL,
                    previous_status TEXT,
                    next_status TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS player_presence_events_roblox_created_idx ON player_presence_events (roblox_id, created_at DESC)")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS ticket_channel_id BIGINT")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mcwv_loa_records (
                    id BIGSERIAL PRIMARY KEY,
                    roblox_id TEXT NOT NULL,
                    roblox_username TEXT,
                    discord_id BIGINT,
                    ticket_id TEXT,
                    ticket_channel_id BIGINT,
                    ticket_name_before TEXT,
                    ticket_category_before BIGINT,
                    started_by BIGINT,
                    started_at TIMESTAMPTZ DEFAULT NOW(),
                    ended_by BIGINT,
                    ended_at TIMESTAMPTZ,
                    active BOOLEAN DEFAULT TRUE,
                    end_notes TEXT
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_loa_active_roblox ON mcwv_loa_records (active, roblox_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_loa_channel ON mcwv_loa_records (ticket_channel_id) WHERE ticket_channel_id IS NOT NULL")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS battles (
                    battle_id TEXT PRIMARY KEY,
                    battle_name TEXT,
                    start_time TIMESTAMPTZ,
                    end_time TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    manually_edited BOOLEAN DEFAULT FALSE,
                    edited_by BIGINT,
                    edited_at TIMESTAMPTZ
                )
            """)
            cur.execute("ALTER TABLE battles ADD COLUMN IF NOT EXISTS manually_edited BOOLEAN DEFAULT FALSE")
            cur.execute("ALTER TABLE battles ADD COLUMN IF NOT EXISTS edited_by BIGINT")
            cur.execute("ALTER TABLE battles ADD COLUMN IF NOT EXISTS edited_at TIMESTAMPTZ")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS admin_logs (
                    id BIGSERIAL PRIMARY KEY,
                    level TEXT NOT NULL DEFAULT 'info',
                    event TEXT NOT NULL,
                    message TEXT NOT NULL DEFAULT '',
                    action TEXT,
                    actor_user_id INTEGER,
                    actor_username TEXT,
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            # Ticket tables (created on startup so /reject etc. always work)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mcwv_tickets (
                    id BIGSERIAL PRIMARY KEY,
                    ticket_id TEXT UNIQUE NOT NULL,
                    channel_id BIGINT UNIQUE,
                    guild_id BIGINT,
                    opener_discord_id BIGINT NOT NULL,
                    roblox_id TEXT,
                    roblox_username TEXT,
                    status TEXT NOT NULL DEFAULT 'open',
                    claimed_by BIGINT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    accepted_at TIMESTAMPTZ,
                    accepted_by BIGINT,
                    rejected_at TIMESTAMPTZ,
                    rejected_by BIGINT,
                    reject_reason TEXT,
                    closed_at TIMESTAMPTZ,
                    closed_by BIGINT,
                    close_reason TEXT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mcwv_ticket_applications (
                    ticket_id TEXT PRIMARY KEY,
                    roblox_username TEXT,
                    roblox_id TEXT,
                    afk_247 TEXT,
                    activity TEXT,
                    liquid_gems TEXT,
                    why_accept TEXT,
                    submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mcwv_ticket_actions (
                    id BIGSERIAL PRIMARY KEY,
                    ticket_id TEXT,
                    actor_discord_id BIGINT,
                    action TEXT NOT NULL,
                    message TEXT,
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mcwv_ticket_transcripts (
                    id BIGSERIAL PRIMARY KEY,
                    ticket_id TEXT,
                    channel_id BIGINT,
                    transcript_text TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mcwv_ticket_blacklist (
                    discord_id BIGINT PRIMARY KEY,
                    reason TEXT NOT NULL DEFAULT '',
                    created_by BIGINT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
        # Event tables (invites / giveaways) now live in Postgres too —
        # the old SQLite bot.db was wiped on every Render deploy.
        try:
            init_invite_tables()
            init_giveaway_tables()
        except Exception as exc:
            print("Event table init failed:", exc)
        print("DB tables ready")
    except Exception as e:
        print("DB schema init failed:", e)


def close_db_connection():
    """No-op — connection stays alive (Supabase has no compute hour limit).
    Called at the end of each DB-heavy loop iteration."""
    global conn
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
        conn = None


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
            # Get roblox_ids before deleting so we can clean up memory
            cur.execute("SELECT roblox_id FROM users WHERE discord_id = %s", (int(did),))
            rids = [str(row[0]) for row in cur.fetchall()]
            cur.execute("""
                DELETE FROM users
                WHERE discord_id = %s
            """, (int(did),))
        conn.commit()
        # Clean up in-memory caches
        for rid in rids:
            cleanup_memory_for_removed_user(rid)
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


def deep_merge_dict(base, override):
    result = dict(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge_dict(result[key], value)
        else:
            result[key] = value
    return result


def sanitize_ticket_settings(raw):
    settings = deep_merge_dict(DEFAULT_MCWV_TICKET_SETTINGS, raw if isinstance(raw, dict) else {})

    panel = settings.get("panel", {})
    panel["title"] = str(panel.get("title") or DEFAULT_MCWV_TICKET_SETTINGS["panel"]["title"])[:256]
    panel["description"] = str(panel.get("description") or DEFAULT_MCWV_TICKET_SETTINGS["panel"]["description"])[:4000]
    panel["buttonLabel"] = str(panel.get("buttonLabel") or "Open Application")[:80]
    thumbnail_url = str(panel.get("thumbnailUrl") or panel.get("thumbnail") or "").strip()[:2048]
    panel["thumbnailUrl"] = thumbnail_url if thumbnail_url.startswith("https://") else ""
    try:
        panel["accentColor"] = int(str(panel.get("accentColor", 0x34D399)).replace("#", ""), 16) if isinstance(panel.get("accentColor"), str) else int(panel.get("accentColor", 0x34D399))
    except Exception:
        panel["accentColor"] = 0x34D399
    settings["panel"] = panel

    messages = settings.get("messages", {})
    messages["welcomeTitle"] = str(messages.get("welcomeTitle") or DEFAULT_MCWV_TICKET_SETTINGS["messages"]["welcomeTitle"])[:256]
    messages["welcomeDescription"] = str(messages.get("welcomeDescription") or DEFAULT_MCWV_TICKET_SETTINGS["messages"]["welcomeDescription"])[:4000]
    settings["messages"] = messages

    raw_colors = settings.get("embedColors", {}) if isinstance(settings.get("embedColors"), dict) else {}
    default_colors = DEFAULT_MCWV_TICKET_SETTINGS["embedColors"]
    embed_colors = {}
    for key, default_value in default_colors.items():
        embed_colors[key] = parse_hex_color(raw_colors.get(key, default_value), default_value)
    settings["embedColors"] = embed_colors

    questions = settings.get("questions") if isinstance(settings.get("questions"), list) else []
    defaults = DEFAULT_MCWV_TICKET_SETTINGS["questions"]
    cleaned = []
    for index in range(5):
        item = questions[index] if index < len(questions) and isinstance(questions[index], dict) else {}
        default = defaults[index]
        key = default["key"]
        cleaned.append({
            "key": key,
            "label": str(item.get("label") or default["label"])[:45],
            "placeholder": str(item.get("placeholder") or default.get("placeholder") or "")[:100],
            "style": "paragraph" if item.get("style", default["style"]) == "paragraph" else "short",
            "required": bool(item.get("required", default.get("required", True))),
            "maxLength": max(16, min(int(item.get("maxLength", default.get("maxLength", 500)) or 500), 1000)),
        })
    # Discord modal title/input flow needs field 1 to be Roblox username.
    cleaned[0]["key"] = "roblox_username"
    cleaned[0]["style"] = "short"
    cleaned[0]["required"] = True
    cleaned[0]["maxLength"] = min(cleaned[0]["maxLength"], 32)
    settings["questions"] = cleaned

    features = settings.get("features", {}) if isinstance(settings.get("features"), dict) else {}
    base_features = DEFAULT_MCWV_TICKET_SETTINGS["features"]
    settings["features"] = {**base_features, **features}
    return settings


def get_mcwv_ticket_settings():
    raw = _safe_call("db_get_setting", "", "mcwv_ticket_settings") or ""
    try:
        parsed = json.loads(raw) if raw else {}
    except Exception:
        parsed = {}
    return sanitize_ticket_settings(parsed)


def save_mcwv_ticket_settings(settings):
    cleaned = sanitize_ticket_settings(settings)
    _safe_call("db_set_setting", None, "mcwv_ticket_settings", json.dumps(cleaned))
    return cleaned


def get_ticket_embed_color(key, default=0x34D399):
    try:
        settings = get_mcwv_ticket_settings()
        colors = settings.get("embedColors", {}) if isinstance(settings.get("embedColors"), dict) else {}
        return parse_hex_color(colors.get(key, default), default)
    except Exception:
        return int(default)


_ticket_tables_ready = False


def db_ensure_mcwv_ticket_tables():
    """Create ticket tables if needed. Cached after the first success so ticket
    commands/buttons never pay for 6 DDL round-trips on every call."""
    global _ticket_tables_ready
    if _ticket_tables_ready:
        return True
    if not db_enabled():
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mcwv_tickets (
                    id BIGSERIAL PRIMARY KEY,
                    ticket_id TEXT UNIQUE NOT NULL,
                    channel_id BIGINT UNIQUE,
                    guild_id BIGINT,
                    opener_discord_id BIGINT NOT NULL,
                    roblox_id TEXT,
                    roblox_username TEXT,
                    status TEXT NOT NULL DEFAULT 'open',
                    claimed_by BIGINT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    accepted_at TIMESTAMPTZ,
                    accepted_by BIGINT,
                    rejected_at TIMESTAMPTZ,
                    rejected_by BIGINT,
                    reject_reason TEXT,
                    closed_at TIMESTAMPTZ,
                    closed_by BIGINT,
                    close_reason TEXT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mcwv_ticket_applications (
                    ticket_id TEXT PRIMARY KEY,
                    roblox_username TEXT,
                    roblox_id TEXT,
                    afk_247 TEXT,
                    activity TEXT,
                    liquid_gems TEXT,
                    why_accept TEXT,
                    submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mcwv_ticket_actions (
                    id BIGSERIAL PRIMARY KEY,
                    ticket_id TEXT,
                    actor_discord_id BIGINT,
                    action TEXT NOT NULL,
                    message TEXT,
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mcwv_ticket_transcripts (
                    id BIGSERIAL PRIMARY KEY,
                    ticket_id TEXT,
                    channel_id BIGINT,
                    transcript_text TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mcwv_ticket_blacklist (
                    discord_id BIGINT PRIMARY KEY,
                    reason TEXT NOT NULL DEFAULT '',
                    created_by BIGINT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
        conn.commit()
        _ticket_tables_ready = True
    except Exception as e:
        print("db_ensure_mcwv_ticket_tables error:", e)
        try:
            conn.rollback()
        except Exception:
            pass


def db_ticket_log(ticket_id, actor_id, action, message="", metadata=None):
    if not db_enabled():
        return
    try:
        import json
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO mcwv_ticket_actions (ticket_id, actor_discord_id, action, message, metadata)
                VALUES (%s, %s, %s, %s, %s::jsonb)
            """, (str(ticket_id), int(actor_id) if actor_id else None, str(action), str(message or ""), json.dumps(metadata or {})))
        conn.commit()
    except Exception as e:
        print("db_ticket_log error:", e)
        conn.rollback()


def db_create_mcwv_ticket(ticket_id, channel_id, guild_id, opener_id):
    if not db_enabled():
        return False
    db_ensure_mcwv_ticket_tables()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO mcwv_tickets (ticket_id, channel_id, guild_id, opener_discord_id, status, updated_at)
                VALUES (%s, %s, %s, %s, 'open', NOW())
                ON CONFLICT (ticket_id)
                DO UPDATE SET channel_id = EXCLUDED.channel_id, updated_at = NOW()
            """, (str(ticket_id), int(channel_id), int(guild_id), int(opener_id)))
        conn.commit()
        return True
    except Exception as e:
        print("db_create_mcwv_ticket error:", e)
        conn.rollback()
        return False


def db_get_ticket_by_channel(channel_id):
    if not db_enabled():
        print(f"[ticket] db_get_ticket_by_channel: db not enabled")
        return None
    db_ensure_mcwv_ticket_tables()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ticket_id, channel_id, guild_id, opener_discord_id, roblox_id, roblox_username, status, claimed_by
                FROM mcwv_tickets
                WHERE channel_id = %s
                LIMIT 1
            """, (int(channel_id),))
            row = cur.fetchone()
            if not row:
                print(f"[ticket] db_get_ticket_by_channel: no ticket found for channel_id={channel_id}")
            return row
    except Exception as e:
        print(f"db_get_ticket_by_channel error for channel_id={channel_id}: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return None


def db_get_ticket_by_ticket_id(ticket_id):
    if not db_enabled():
        return None
    db_ensure_mcwv_ticket_tables()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ticket_id, channel_id, guild_id, opener_discord_id, roblox_id, roblox_username, status, claimed_by
                FROM mcwv_tickets
                WHERE ticket_id = %s
                LIMIT 1
            """, (str(ticket_id),))
            return cur.fetchone()
    except Exception as e:
        print("db_get_ticket_by_ticket_id error:", e)
        return None


def db_get_ticket_by_opener(opener_discord_id, channel_id=None):
    """Look up a ticket by the opener's Discord ID.
    If channel_id is provided, prefer the ticket that matches both."""
    if not db_enabled():
        return None
    db_ensure_mcwv_ticket_tables()
    try:
        with conn.cursor() as cur:
            if channel_id:
                cur.execute("""
                    SELECT ticket_id, channel_id, guild_id, opener_discord_id, roblox_id, roblox_username, status, claimed_by
                    FROM mcwv_tickets
                    WHERE opener_discord_id = %s
                    ORDER BY (channel_id = %s) DESC, created_at DESC
                    LIMIT 1
                """, (int(opener_discord_id), int(channel_id)))
            else:
                cur.execute("""
                    SELECT ticket_id, channel_id, guild_id, opener_discord_id, roblox_id, roblox_username, status, claimed_by
                    FROM mcwv_tickets
                    WHERE opener_discord_id = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                """, (int(opener_discord_id),))
            return cur.fetchone()
    except Exception as e:
        print("db_get_ticket_by_opener error:", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return None


def db_save_ticket_application(ticket_id, roblox_username, roblox_id, afk_247, activity, liquid_gems, why_accept):
    if not db_enabled():
        return False
    db_ensure_mcwv_ticket_tables()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO mcwv_ticket_applications (ticket_id, roblox_username, roblox_id, afk_247, activity, liquid_gems, why_accept, submitted_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (ticket_id)
                DO UPDATE SET
                    roblox_username = EXCLUDED.roblox_username,
                    roblox_id = EXCLUDED.roblox_id,
                    afk_247 = EXCLUDED.afk_247,
                    activity = EXCLUDED.activity,
                    liquid_gems = EXCLUDED.liquid_gems,
                    why_accept = EXCLUDED.why_accept,
                    submitted_at = NOW()
            """, (str(ticket_id), roblox_username, roblox_id, afk_247, activity, liquid_gems, why_accept))
            cur.execute("""
                UPDATE mcwv_tickets
                SET roblox_username = %s, roblox_id = %s, status = 'pending', updated_at = NOW()
                WHERE ticket_id = %s
            """, (roblox_username, roblox_id, str(ticket_id)))
        conn.commit()
        return True
    except Exception as e:
        print("db_save_ticket_application error:", e)
        conn.rollback()
        return False


def db_get_ticket_application(ticket_id):
    if not db_enabled():
        return None
    db_ensure_mcwv_ticket_tables()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT roblox_username, roblox_id, afk_247, activity, liquid_gems, why_accept, submitted_at
                FROM mcwv_ticket_applications
                WHERE ticket_id = %s
                LIMIT 1
            """, (str(ticket_id),))
            return cur.fetchone()
    except Exception as e:
        print("db_get_ticket_application error:", e)
        return None


def db_update_ticket_status(ticket_id, status, actor_id=None, **fields):
    if not db_enabled():
        return False
    db_ensure_mcwv_ticket_tables()
    allowed = {
        'accepted_at', 'accepted_by', 'rejected_at', 'rejected_by', 'reject_reason',
        'closed_at', 'closed_by', 'close_reason', 'claimed_by'
    }
    assignments = ["status = %s", "updated_at = NOW()"]
    values = [str(status)]
    for key, value in fields.items():
        if key in allowed:
            assignments.append(f"{key} = %s")
            values.append(value)
    values.append(str(ticket_id))
    try:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE mcwv_tickets SET {', '.join(assignments)} WHERE ticket_id = %s", values)
        conn.commit()
        db_ticket_log(ticket_id, actor_id, f"ticket/{status}", status, fields)
        return True
    except Exception as e:
        print("db_update_ticket_status error:", e)
        conn.rollback()
        return False


def db_save_ticket_transcript(ticket_id, channel_id, transcript_text):
    if not db_enabled():
        return False
    db_ensure_mcwv_ticket_tables()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO mcwv_ticket_transcripts (ticket_id, channel_id, transcript_text)
                VALUES (%s, %s, %s)
            """, (str(ticket_id), int(channel_id), transcript_text))
        conn.commit()
        return True
    except Exception as e:
        print("db_save_ticket_transcript error:", e)
        conn.rollback()
        return False


def db_ticket_blacklist_add(discord_id, reason="", actor_id=None):
    if not db_enabled():
        return False
    db_ensure_mcwv_ticket_tables()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO mcwv_ticket_blacklist (discord_id, reason, created_by, created_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (discord_id)
                DO UPDATE SET reason = EXCLUDED.reason, created_by = EXCLUDED.created_by, created_at = NOW()
            """, (int(discord_id), str(reason or ""), int(actor_id) if actor_id else None))
        conn.commit()
        return True
    except Exception as exc:
        print("db_ticket_blacklist_add error:", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return False


def db_ticket_blacklist_remove(discord_id):
    if not db_enabled():
        return False
    db_ensure_mcwv_ticket_tables()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM mcwv_ticket_blacklist WHERE discord_id = %s", (int(discord_id),))
        conn.commit()
        return True
    except Exception as exc:
        print("db_ticket_blacklist_remove error:", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return False


def db_ticket_blacklist_list():
    if not db_enabled():
        return []
    db_ensure_mcwv_ticket_tables()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT discord_id, reason, created_by, created_at
                FROM mcwv_ticket_blacklist
                ORDER BY created_at DESC
                LIMIT 500
            """)
            return cur.fetchall()
    except Exception as exc:
        print("db_ticket_blacklist_list error:", exc)
        return []


def db_ticket_blacklist_get(discord_id):
    if not db_enabled():
        return None
    db_ensure_mcwv_ticket_tables()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT discord_id, reason, created_by, created_at
                FROM mcwv_ticket_blacklist
                WHERE discord_id = %s
                LIMIT 1
            """, (int(discord_id),))
            return cur.fetchone()
    except Exception as exc:
        print("db_ticket_blacklist_get error:", exc)
        return None


def db_set_user_status(rid, status):
    if not db_enabled():
        return

    rid = str(rid).strip()
    next_status = int(status)

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM user_status WHERE roblox_id = %s", (rid,))
            row = cur.fetchone()
            previous_status = int(row[0]) if row and row[0] is not None else None

            cur.execute("""
                INSERT INTO user_status (roblox_id, status, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (roblox_id)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    updated_at = NOW()
            """, (rid, next_status))

            if previous_status is not None and previous_status != next_status:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS player_presence_events (
                        id BIGSERIAL PRIMARY KEY,
                        roblox_id TEXT NOT NULL,
                        previous_status TEXT,
                        next_status TEXT NOT NULL,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    INSERT INTO player_presence_events (roblox_id, previous_status, next_status, created_at)
                    VALUES (%s, %s, %s, NOW())
                """, (rid, str(previous_status), str(next_status)))

        conn.commit()
    except Exception as e:
        print("db_set_user_status error:", e)
        conn.rollback()


def db_bulk_set_user_statuses(updates):
    """Bulk-write Roblox presence statuses off the Discord event loop.

    Opening a short-lived DB connection here avoids blocking the bot heartbeat with
    60+ sequential psycopg2 calls on the main asyncio loop.
    """
    cleaned = []
    seen = set()
    for rid, status in updates or []:
        try:
            rid = str(rid).strip()
            status = int(status)
            if not rid or status not in (0, 1, 2, 3):
                continue
            if rid in seen:
                continue
            seen.add(rid)
            cleaned.append((rid, status))
        except Exception:
            continue

    if not cleaned or not DATABASE_URL:
        return

    pass
    try:
        ensure_db_connection()
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_status (
                    roblox_id TEXT PRIMARY KEY,
                    status INTEGER,
                    updated_at TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS player_presence_events (
                    id BIGSERIAL PRIMARY KEY,
                    roblox_id TEXT NOT NULL,
                    previous_status TEXT,
                    next_status TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            ids = [rid for rid, _status in cleaned]
            cur.execute(
                "SELECT roblox_id, status FROM user_status WHERE roblox_id = ANY(%s)",
                (ids,),
            )
            previous = {str(row[0]): int(row[1]) for row in cur.fetchall() if row[1] is not None}

            cur.executemany("""
                INSERT INTO user_status (roblox_id, status, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (roblox_id)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    updated_at = NOW()
            """, cleaned)

            events = [
                (rid, str(previous[rid]), str(status))
                for rid, status in cleaned
                if rid in previous and previous[rid] != status
            ]
            if events:
                cur.executemany("""
                    INSERT INTO player_presence_events (roblox_id, previous_status, next_status, created_at)
                    VALUES (%s, %s, %s, NOW())
                """, events)

        conn.commit()
    except Exception as exc:
        print("db_bulk_set_user_statuses error:", exc)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        pass


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


def db_get_broadcast_users():
    if not db_enabled():
        return []

    try:
        ensure_db_connection()
        with conn.cursor() as cur:
            try:
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS ticket_channel_id BIGINT")
                conn.commit()
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass

            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'users'
            """)
            columns = {str(row[0]) for row in cur.fetchall()}

            if "roblox_id" not in columns or "discord_id" not in columns or "username" not in columns:
                return []

            role_expr = "COALESCE(role, 'member')" if "role" in columns else "'member'"
            ticket_expr = "ticket_channel_id" if "ticket_channel_id" in columns else "NULL"

            cur.execute(f"""
                SELECT roblox_id,
                       discord_id,
                       username,
                       {role_expr} AS role,
                       {ticket_expr} AS ticket_channel_id
                FROM users
                WHERE roblox_id IS NOT NULL
                  AND TRIM(CAST(roblox_id AS TEXT)) <> ''
                  AND discord_id IS NOT NULL
                ORDER BY username ASC
            """)
            rows = cur.fetchall()

            if rows:
                return rows

        # Fallback for older/odd schemas: use the same tracked source the admin players panel uses.
        tracked = db_get_all_tracked() or []
        return [
            (
                str(row[0]).strip(),
                int(row[1]),
                str(row[2]).strip() if len(row) > 2 else str(row[0]).strip(),
                "member",
                None,
            )
            for row in tracked
            if len(row) > 1 and row[0] is not None and str(row[0]).strip() and row[1] is not None
        ]
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print("db_get_broadcast_users error:", e)
        return []


def db_set_ticket_channel(discord_id, channel_id):
    if not db_enabled():
        return False

    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE users
                SET ticket_channel_id = %s
                WHERE discord_id = %s
            """, (int(channel_id), int(discord_id)))
        conn.commit()
        return True
    except Exception as e:
        print("db_set_ticket_channel error:", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return False


def db_get_ticket_channel_id(discord_id):
    """Read a member's stored ticket channel ID from the users table."""
    if not db_enabled():
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ticket_channel_id FROM users WHERE discord_id = %s LIMIT 1",
                (int(discord_id),)
            )
            row = cur.fetchone()
        if row and row[0] is not None:
            return int(row[0])
    except Exception as e:
        print("db_get_ticket_channel_id error:", e)
    return None


def db_log_admin_action(level, event, message, action=None, actor_username=None, metadata=None):
    if not db_enabled():
        return

    try:
        import json as _json
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO admin_logs (
                    level,
                    event,
                    message,
                    action,
                    actor_username,
                    metadata
                ) VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            """, (
                str(level or "info"),
                str(event or "Bot Event"),
                str(message or ""),
                action,
                actor_username,
                _json.dumps(metadata or {})
            ))
        conn.commit()
    except Exception as e:
        print("db_log_admin_action error:", e)
        try:
            conn.rollback()
        except Exception:
            pass
# ---------------- STATUS ----------------
status_cache = {}
offline_since = {}  # roblox_id -> datetime (UTC) when they went offline

def status_text(v):
    try:
        v = int(v)
    except Exception:
        return "UNKNOWN"

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
    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession()
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
        # The server owner can always run staff commands, even without the role.
        if interaction.guild and interaction.guild.owner_id == interaction.user.id:
            return True
        role_ids = [r.id for r in getattr(interaction.user, "roles", [])]
        return ALLOWED_ROLE_ID in role_ids
    return app_commands.check(predicate)

@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CheckFailure):
        try:
            if interaction.response.is_done():
                await interaction.followup.send("❌ You don't have permission to use this command.", ephemeral=True)
            else:
                await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        except Exception:
            pass
    elif isinstance(error, app_commands.CommandOnCooldown):
        retry = error.retry_after
        msg = f"\u23f3 Slow down! Try again in **{retry:.1f}s**."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except Exception:
            pass
    else:
        print(f"[command error] {error}")
        traceback.print_exc()
        try:
            err_msg = f"\u26a0\ufe0f Something went wrong. The error has been logged."
            if interaction.response.is_done():
                await interaction.followup.send(err_msg, ephemeral=True)
            else:
                await interaction.response.send_message(err_msg, ephemeral=True)
        except Exception:
            pass


@bot.event
async def on_error(event_method: str, *args, **kwargs):
    """Global safety net. If a button/modal callback crashes, Discord never gets
    its response and shows 'the application did not respond'. Answer the
    interaction here whenever we still can, then log the traceback."""
    try:
        traceback.print_exc()
    except Exception:
        pass

    interaction = next((a for a in args if isinstance(a, discord.Interaction)), None)
    if interaction is None or not isinstance(interaction, discord.Interaction):
        return

    try:
        if not interaction.response.is_done():
            await interaction.response.send_message(
                "⚠️ Something went wrong handling that. Please try again.",
                ephemeral=True,
            )
        else:
            await interaction.followup.send(
                "⚠️ Something went wrong handling that. Please try again.",
                ephemeral=True,
            )
    except Exception:
        try:
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True)
        except Exception:
            pass


# ---------------- SLASH COMMANDS ----------------
import random
import secrets
from typing import Optional, List, Tuple

GIVEAWAY_EDIT_ROLE_ID = 1501985889964789962
GIVEAWAY_LOG_CHANNEL_ID = 1502001938705682622


def init_giveaway_tables():
    db_exec("""
    CREATE TABLE IF NOT EXISTS giveaway_events (
        id BIGINT PRIMARY KEY,
        active INTEGER DEFAULT 0,
        prize TEXT,
        winners INTEGER DEFAULT 1,
        invites_per_entry INTEGER DEFAULT 2,
        start_time BIGINT DEFAULT 0,
        end_time BIGINT DEFAULT 0,
        channel_id BIGINT,
        message_id BIGINT,
        thumbnail TEXT,
        created_by BIGINT
    )
    """)

    db_exec("""
    CREATE TABLE IF NOT EXISTS invite_member_links (
        member_id BIGINT PRIMARY KEY,
        inviter_id BIGINT NOT NULL
    )
    """)

    db_exec("""
    INSERT INTO giveaway_events (
        id, active, prize, winners, invites_per_entry,
        start_time, end_time, channel_id, message_id,
        thumbnail, created_by
    ) VALUES (1, 0, '', 1, 2, 0, 0, 0, 0, '', 0)
    ON CONFLICT (id) DO NOTHING
    """)


def has_edit_role(member: discord.Member) -> bool:
    return any(role.id == GIVEAWAY_EDIT_ROLE_ID for role in member.roles)


def get_active_giveaway():
    return db_fetchone("""
        SELECT * FROM giveaway_events
        WHERE id = 1
    """)


def get_valid_invites(user_id: int) -> int:
    row = db_fetchone(
        "SELECT invites FROM invite_counts WHERE user_id = %s",
        (user_id,)
    )
    return int(row["invites"]) if row else 0


def get_user_entries(user_id: int, invites_per_entry: int) -> int:
    invites_per_entry = max(1, int(invites_per_entry))
    return get_valid_invites(user_id) // invites_per_entry


def build_giveaway_embed(giveaway_row):
    prize = giveaway_row["prize"] or "Unknown prize"
    winners = int(giveaway_row["winners"] or 1)
    invites_per_entry = max(1, int(giveaway_row["invites_per_entry"] or 2))
    end_time = int(giveaway_row["end_time"] or 0)

    embed = discord.Embed(
        title="🎁 Giveaway Event",
        description=(
            "This giveaway is running during the active invite event.\n\n"
            f"🏆 **Prize:** {prize}\n"
            f"👑 **Winners:** {winners}\n"
            f"🔁 **Entry rate:** {invites_per_entry} invites = 1 entry\n"
            f"⏳ **Ends:** <t:{end_time}:R>\n"
        ),
        color=discord.Color.blurple()
    )

    thumb = giveaway_row["thumbnail"]
    if thumb:
        embed.set_thumbnail(url=thumb)

    embed.set_footer(text="Entries update live from valid invites only")
    embed.timestamp = discord.utils.utcnow()
    return embed


def weighted_unique_winners(candidates: List[Tuple[int, int]], winner_count: int) -> List[int]:
    rng = secrets.SystemRandom()
    pool = [(uid, weight) for uid, weight in candidates if weight > 0]
    winners: List[int] = []

    winner_count = min(max(1, int(winner_count)), len(pool))
    for _ in range(winner_count):
        total_weight = sum(weight for _, weight in pool)
        if total_weight <= 0:
            break

        pick = rng.randrange(total_weight)
        running = 0
        chosen_index = None

        for idx, (uid, weight) in enumerate(pool):
            running += weight
            if pick < running:
                chosen_index = idx
                break

        if chosen_index is None:
            break

        chosen_uid = pool.pop(chosen_index)[0]
        winners.append(chosen_uid)

    return winners


async def log_giveaway_edit(action: str, before: Optional[dict], after: Optional[dict], edited_by: discord.Member):
    channel = bot.get_channel(GIVEAWAY_LOG_CHANNEL_ID)
    if channel is None:
        return

    embed = discord.Embed(
        title=f"📝 Giveaway {action}",
        color=discord.Color.orange(),
        timestamp=discord.utils.utcnow()
    )
    embed.add_field(name="Edited by", value=f"{edited_by} (`{edited_by.id}`)", inline=False)

    if before is not None:
        embed.add_field(
            name="Before",
            value=(
                f"Prize: {before['prize']}\n"
                f"Winners: {before['winners']}\n"
                f"Invites/Entry: {before['invites_per_entry']}\n"
                f"Thumbnail: {before['thumbnail'] or 'None'}"
            ),
            inline=False
        )

    if after is not None:
        embed.add_field(
            name="After",
            value=(
                f"Prize: {after['prize']}\n"
                f"Winners: {after['winners']}\n"
                f"Invites/Entry: {after['invites_per_entry']}\n"
                f"Thumbnail: {after['thumbnail'] or 'None'}"
            ),
            inline=False
        )

    await channel.send(embed=embed)


async def finish_giveaway(reason: str = "ended"):
    print("🔥 finish_giveaway TRIGGERED:", reason)

    giveaway = get_active_giveaway()

    if not giveaway or int(giveaway["active"]) != 1:
        print("⚠️ No active giveaway found")
        return

    channel = None
    channel_id = giveaway["channel_id"]

    # ---------------- CHANNEL SAFETY ----------------
    try:
        if channel_id:
            channel = bot.get_channel(int(channel_id))
            if channel is None:
                channel = await bot.fetch_channel(int(channel_id))
    except Exception as e:
        print("❌ Channel fetch failed:", repr(e))
        return

    if channel is None:
        print("❌ Giveaway channel is None (cannot send)")
        return

    print("📡 Channel resolved:", channel.id)

    # ---------------- ENTRY SETTINGS ----------------
    invites_per_entry = max(1, int(giveaway["invites_per_entry"] or 2))
    winner_count = max(1, int(giveaway["winners"] or 1))

    # ---------------- FETCH ENTRIES ----------------
    rows = db_fetchall("""
        SELECT user_id, invites
        FROM invite_counts
        ORDER BY invites DESC
    """)

    candidates = []

    for row in rows:
        invites = int(row["invites"] or 0)
        entries = invites // invites_per_entry

        if entries > 0:
            candidates.append((int(row["user_id"]), entries))

    print(f"📊 Candidates found: {len(candidates)}")

    chosen = weighted_unique_winners(candidates, winner_count)

    # ---------------- NO WINNERS ----------------
    if not chosen:
        try:
            await channel.send("🏁 Giveaway ended — no valid entries.")
        except Exception as e:
            print("❌ Failed to send no-winner message:", repr(e))

        db_exec("UPDATE giveaway_events SET active = 0 WHERE id = 1")
        print("🏁 Giveaway closed (no winners)")
        return

    # ---------------- WINNER MESSAGE ----------------
    mentions = "\n".join(f"<@{uid}>" for uid in chosen)

    embed = discord.Embed(
        title="🏁 Giveaway Ended",
        description=(
            f"**Prize:** {giveaway['prize'] if giveaway['prize'] else 'Unknown'}\n\n"
            f"**Winners:**\n{mentions}"
        ),
        color=discord.Color.gold(),
        timestamp=discord.utils.utcnow()
    )

    if giveaway["thumbnail"]:
        embed.set_thumbnail(url=giveaway["thumbnail"])

    # ---------------- SEND RESULT ----------------
    try:
        msg = await channel.send(embed=embed)
        print("✅ Giveaway sent:", msg.id)
    except Exception as e:
        print("❌ Failed to send giveaway embed:", repr(e))
        return

    # ---------------- CLOSE GIVEAWAY ----------------
    db_exec("UPDATE giveaway_events SET active = 0 WHERE id = 1")
    print("🏁 Giveaway marked inactive")

    # ---------------- LOG ----------------
    try:
        log_channel = bot.get_channel(GIVEAWAY_LOG_CHANNEL_ID)
        if log_channel:
            await log_channel.send(
                f"🎁 Giveaway ended ({reason}). Winners: {', '.join(map(str, chosen))}"
            )
    except Exception as e:
        print("❌ Log send failed:", repr(e))


class GiveawayView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="My Entries",
        style=discord.ButtonStyle.green,
        custom_id="giveaway_my_entries"
    )
    async def my_entries(self, interaction: discord.Interaction, button: discord.ui.Button):
        giveaway = get_active_giveaway()
        if not giveaway or not giveaway["active"]:
            return await interaction.response.send_message(
                "❌ No active giveaway right now.",
                ephemeral=True
            )

        invites_per_entry = max(1, int(giveaway["invites_per_entry"] or 2))
        invites = get_valid_invites(interaction.user.id)
        entries = invites // invites_per_entry

        embed = discord.Embed(
            title="📊 Your Giveaway Stats",
            color=discord.Color.blurple()
        )
        embed.add_field(name="Valid invites", value=str(invites), inline=True)
        embed.add_field(name="Entries", value=str(entries), inline=True)
        embed.add_field(name="Rate", value=f"{invites_per_entry} invites = 1 entry", inline=False)

        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(
        label="Leaderboard",
        style=discord.ButtonStyle.blurple,
        custom_id="giveaway_leaderboard"
    )
    async def leaderboard(self, interaction: discord.Interaction, button: discord.ui.Button):
        giveaway = get_active_giveaway()
        if not giveaway or not giveaway["active"]:
            return await interaction.response.send_message(
                "❌ No active giveaway right now.",
                ephemeral=True
            )

        invites_per_entry = max(1, int(giveaway["invites_per_entry"] or 2))
        rows = db_fetchall("""
            SELECT user_id, invites
            FROM invite_counts
            ORDER BY invites DESC, user_id ASC
        """)

        lines = []
        rank = 1
        for row in rows:
            invites = int(row["invites"] or 0)
            entries = invites // invites_per_entry
            if entries <= 0:
                continue
            lines.append(f"{rank}. <@{row['user_id']}> — {entries} entries ({invites} invites)")
            rank += 1
            if rank > 10:
                break

        if not lines:
            return await interaction.response.send_message(
                "No entries yet.",
                ephemeral=True
            )

        embed = discord.Embed(
            title="🏆 Giveaway Leaderboard",
            description="\n".join(lines),
            color=discord.Color.blurple()
        )

        await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="giveaway_start", description="Start the MCWV invite giveaway", guild=guild_obj)
@require_role()
@app_commands.describe(
    prize="What is being given away",
    winners="How many winners",
    invites_per_entry="How many valid invites are needed for 1 entry",
    thumbnail_url="Optional image link for the embed"
)
async def giveaway_start(
    interaction: discord.Interaction,
    prize: str,
    winners: int,
    invites_per_entry: int,
    thumbnail_url: str = None
):
    await interaction.response.defer(ephemeral=True)

    try:
        if not interaction.guild or not interaction.channel:
            return await interaction.followup.send(
                "❌ This command only works in a server channel.",
                ephemeral=True
            )

        invite_event = get_active_event()
        if not invite_event or not invite_event["active"]:
            return await interaction.followup.send(
                "❌ There is no active invite event.",
                ephemeral=True
            )

        if int(time.time()) >= int(invite_event["end_time"]):
            return await interaction.followup.send(
                "❌ The invite event has already ended.",
                ephemeral=True
            )

        existing = get_active_giveaway()
        if existing and existing["active"]:
            return await interaction.followup.send(
                "❌ There is already an active giveaway.",
                ephemeral=True
            )

        winners = max(1, int(winners))
        invites_per_entry = max(1, int(invites_per_entry))
        end_time = int(invite_event["end_time"])

        db_exec("""
            INSERT INTO giveaway_events (
                id, active, prize, winners, invites_per_entry,
                start_time, end_time, channel_id, message_id, thumbnail, created_by
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(id) DO UPDATE SET
                active = excluded.active,
                prize = excluded.prize,
                winners = excluded.winners,
                invites_per_entry = excluded.invites_per_entry,
                start_time = excluded.start_time,
                end_time = excluded.end_time,
                channel_id = excluded.channel_id,
                message_id = excluded.message_id,
                thumbnail = excluded.thumbnail,
                created_by = excluded.created_by
        """, (
            1, 1, prize, winners, invites_per_entry,
            int(time.time()), end_time, interaction.channel_id, None,
            thumbnail_url if thumbnail_url and thumbnail_url.startswith("http") else None,
            interaction.user.id
        ))

        giveaway = get_active_giveaway()
        embed = build_giveaway_embed(giveaway)
        message = await interaction.channel.send(embed=embed, view=GiveawayView())

        db_exec(
            "UPDATE giveaway_events SET message_id = %s WHERE id = 1",
            (message.id,)
        )

        await interaction.followup.send(
            "✅ Giveaway started publicly in the channel.",
            ephemeral=True
        )

    except Exception as e:
        import traceback
        print("[giveaway_start error]")
        print(traceback.format_exc())
        await interaction.followup.send(
            f"❌ {type(e).__name__}: {e}",
            ephemeral=True
        )


@bot.tree.command(name="giveaway_edit", description="Edit the active invite giveaway settings", guild=guild_obj)
async def giveaway_edit(
    interaction: discord.Interaction,
    prize: str = None,
    winners: int = None,
    invites_per_entry: int = None,
    thumbnail_url: str = None
):
    await interaction.response.defer(ephemeral=True)

    try:
        if not interaction.guild:
            return await interaction.followup.send("❌ Guild only.", ephemeral=True)

        if not isinstance(interaction.user, discord.Member) or not has_edit_role(interaction.user):
            return await interaction.followup.send(
                "❌ You do not have permission to edit giveaways.",
                ephemeral=True
            )

        giveaway = get_active_giveaway()
        if not giveaway or not giveaway["active"]:
            return await interaction.followup.send(
                "❌ No active giveaway to edit.",
                ephemeral=True
            )

        before = {
            "prize": giveaway["prize"],
            "winners": int(giveaway["winners"] or 1),
            "invites_per_entry": int(giveaway["invites_per_entry"] or 2),
            "thumbnail": giveaway["thumbnail"]
        }

        new_prize = prize if prize is not None else giveaway["prize"]
        new_winners = max(1, int(winners)) if winners is not None else int(giveaway["winners"] or 1)
        new_invites_per_entry = max(1, int(invites_per_entry)) if invites_per_entry is not None else int(giveaway["invites_per_entry"] or 2)
        new_thumbnail = thumbnail_url if thumbnail_url is not None else giveaway["thumbnail"]
        if new_thumbnail and not str(new_thumbnail).startswith("http"):
            new_thumbnail = None

        db_exec("""
            UPDATE giveaway_events
            SET prize = %s, winners = %s, invites_per_entry = %s, thumbnail = %s
            WHERE id = 1
        """, (new_prize, new_winners, new_invites_per_entry, new_thumbnail))

        after = {
            "prize": new_prize,
            "winners": new_winners,
            "invites_per_entry": new_invites_per_entry,
            "thumbnail": new_thumbnail
        }

        await log_giveaway_edit("edited", before, after, interaction.user)

        await interaction.followup.send("✅ Giveaway updated.", ephemeral=True)

    except Exception as e:
        import traceback
        print("[giveaway_edit error]")
        print(traceback.format_exc())
        await interaction.followup.send(
            f"❌ {type(e).__name__}: {e}",
            ephemeral=True
        )


@bot.tree.command(name="giveaway_end", description="End the invite giveaway and pick winners", guild=guild_obj)
@require_role()
async def giveaway_end(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    try:
        giveaway = get_active_giveaway()
        if not giveaway or not giveaway["active"]:
            return await interaction.followup.send(
                "❌ No active giveaway.",
                ephemeral=True
            )

        await finish_giveaway(reason="manual end")
        await interaction.followup.send("✅ Giveaway ended and winners picked.", ephemeral=True)

    except Exception as e:
        import traceback
        print("[giveaway_end error]")
        print(traceback.format_exc())
        await interaction.followup.send(
            f"❌ {type(e).__name__}: {e}",
            ephemeral=True
        )

@tasks.loop(seconds=30)
async def check_giveaway_event():
    giveaway = get_giveaway_row()
    if not giveaway:
        return

    if int(giveaway["active"] or 0) != 1:
        return

    if int(time.time()) >= int(giveaway["end_time"] or 0):
        await finish_giveaway("auto end")
        return

    invite_event = get_active_event()
    if not invite_event or not invite_event["active"]:
        await finish_giveaway("invite event ended")
        return
        
class InviteView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Get Invite Link",
        style=discord.ButtonStyle.blurple,
        custom_id="invite_event_button"
    )
    async def invite_button(self, interaction: discord.Interaction, button: discord.ui.Button):

        event = get_active_event()
        if not event or not event["active"]:
            return await interaction.response.send_message(
                "❌ No active invite event.",
                ephemeral=True
            )

        guild = interaction.guild
        if guild is None:
            return await interaction.response.send_message(
                "❌ This can only be used in a server.",
                ephemeral=True
            )

        channel = get_invite_channel(guild, interaction.client)

        if not channel:
            return await interaction.response.send_message(
                "❌ No valid channel for invites.",
                ephemeral=True
            )

        try:
            invite = await channel.create_invite(
                unique=True,
                max_uses=0
            )
        except Exception as e:
            print(f"[invite system] create invite error: {e}")
            return await interaction.response.send_message(
                "❌ Failed to create invite.",
                ephemeral=True
            )

        db_exec(
            "INSERT INTO invite_cache (invite_code, inviter_id) VALUES (%s, %s) "
            "ON CONFLICT (invite_code) DO UPDATE SET inviter_id = EXCLUDED.inviter_id",
            (invite.code, interaction.user.id)
        )

        await interaction.response.send_message(
            f"Your invite link:\n{invite.url}",
            ephemeral=True
        )

    @discord.ui.button(
        label="My Invites",
        style=discord.ButtonStyle.green,
        custom_id="invite_event_my_invites"
    )
    async def my_invites_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.response.is_done():
            try:
                await interaction.response.defer(ephemeral=True)
            except Exception:
                pass

        row = db_fetchone(
            "SELECT invites FROM invite_counts WHERE user_id = %s",
            (interaction.user.id,)
        )

        invites = row["invites"] if row else 0

        embed = discord.Embed(
            title="📊 Invite Stats",
            color=discord.Color.blurple()
        )
        embed.add_field(
            name="Your Invites",
            value=str(invites),
            inline=False
        )

        await interaction.followup.send(embed=embed, ephemeral=True)

# ---------------- INVITE DEBUG TOOLKIT ----------------

@bot.tree.command(name="invite_debug", description="Staff diagnostic: show invite tracking state", guild=guild_obj)
@require_role()
async def invite_debug(interaction: discord.Interaction):
    event = get_active_event()
    guild = interaction.guild

    snap = INVITE_SNAPSHOTS.get(guild.id, {}) if guild else {}

    await interaction.response.send_message(
        f"🧪 Invite System Debug\n\n"
        f"Active Event: {bool(event and event['active'])}\n"
        f"End Time: {event['end_time'] if event else 'None'}\n"
        f"Channel ID: {event['channel_id'] if event else 'None'}\n\n"
        f"Snapshot size: {len(snap)} invites tracked",
        ephemeral=True
    )

@bot.tree.command(name="invite_snapshot_refresh", description="Refresh the invite snapshot cache", guild=guild_obj)
@require_role()
async def invite_snapshot_refresh(interaction: discord.Interaction):
    guild = interaction.guild

    if not guild:
        return await interaction.response.send_message("No guild found.", ephemeral=True)

    try:
        await load_invite_snapshot(guild)

        await interaction.response.send_message(
            "🔄 Invite snapshot refreshed successfully.",
            ephemeral=True
        )

    except Exception as e:
        import traceback

        print("FULL SNAPSHOT ERROR:")
        print(traceback.format_exc())

        await interaction.response.send_message(
            f"❌ Failed to refresh snapshot:\n`{type(e).__name__}: {e}`",
            ephemeral=True
        )

@bot.tree.command(name="host_invite_event", description="Host a timed invite event with a prize", guild=guild_obj)
@require_role()
@app_commands.describe(duration_hours="Event duration")
async def host_invite_event(interaction: discord.Interaction, duration_hours: int):
    await interaction.response.defer(ephemeral=True)

    try:
        if duration_hours <= 0:
            return await interaction.followup.send(
                "❌ Must be at least 1 hour.",
                ephemeral=True
            )

        start = int(time.time())
        end = start + (duration_hours * 3600)

        db_exec("DELETE FROM invite_counts")
        db_exec("DELETE FROM invite_used_users")
        db_exec("DELETE FROM invite_cache")

        db_exec("""
            INSERT INTO invite_events
            (id, active, start_time, end_time, channel_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT(id)
            DO UPDATE SET
                active = excluded.active,
                start_time = excluded.start_time,
                end_time = excluded.end_time,
                channel_id = excluded.channel_id
        """, (1, 1, start, end, interaction.channel_id))

        if interaction.guild:
            await load_invite_snapshot(interaction.guild)

        embed = discord.Embed(
            title="🎉 Invite Event Started!",
            description=(
                f"**Duration:** {duration_hours} hour(s)\n"
                f"**Ends:** <t:{end}:R>\n\n"
                "Click the buttons below to get your invite link or view your stats.\n"
                "Only first-time joins are counted."
            ),
            color=discord.Color.green()
        )

        # 🔥 PUBLIC MESSAGE (IMPORTANT)
        await interaction.channel.send(embed=embed, view=InviteView())

        # private confirmation
        await interaction.followup.send("✅ Invite event started.", ephemeral=True)

    except Exception as e:
        import traceback
        print(traceback.format_exc())

        await interaction.followup.send(
            f"❌ {type(e).__name__}: {e}",
            ephemeral=True
        )
        
@bot.tree.command(name="end_invite_event", description="End the current invite event and announce winners", guild=guild_obj)
@require_role()
async def end_invite_event(interaction: discord.Interaction):

    try:
        event = get_active_event()

        if not event or not event["active"]:
            return await interaction.response.send_message(
                "❌ No active event.",
                ephemeral=True
            )

        db_exec("UPDATE invite_events SET active = 0 WHERE id = 1")

        await interaction.response.send_message(
            "🏁 Event ended.",
            ephemeral=True
        )

    except Exception as e:
        print(f"[end_invite_event error] {e}")
        await interaction.response.send_message(
            "❌ Something went wrong ending the event.",
            ephemeral=True
        )

@bot.event
async def on_member_join(member: discord.Member):
    if member.bot:
        return

    event = get_active_event()
    if not event or not event["active"]:
        return

    if int(time.time()) >= int(event["end_time"]):
        return

    # first join only
    if db_fetchone("SELECT 1 FROM invite_used_users WHERE user_id = %s", (member.id,)):
        return

    db_exec("INSERT INTO invite_used_users (user_id) VALUES (%s) ON CONFLICT (user_id) DO NOTHING", (member.id,))

    try:
        after = await member.guild.invites()
    except Exception as e:
        print(f"[invite system] join fetch error: {e}")
        return

    before = INVITE_SNAPSHOTS.get(member.guild.id)
    if not before:
        await load_snapshot(member.guild)
        before = INVITE_SNAPSHOTS.get(member.guild.id, {})

    used = None
    for inv in after:
        if int(inv.uses or 0) > int(before.get(inv.code, 0)):
            used = inv.code
            break

    INVITE_SNAPSHOTS[member.guild.id] = {
        i.code: int(i.uses or 0) for i in after
    }

    if not used:
        return

    row = db_fetchone(
        "SELECT inviter_id FROM invite_cache WHERE invite_code = %s",
        (used,)
    )

    if not row:
        return

    inviter_id = int(row["inviter_id"])

    increment_invite(inviter_id)

    db_exec(
        "INSERT INTO invite_member_links (member_id, inviter_id) VALUES (%s, %s) "
        "ON CONFLICT (member_id) DO UPDATE SET inviter_id = EXCLUDED.inviter_id",
        (member.id, inviter_id)
    )

@bot.event
async def on_member_remove(member: discord.Member):
    if member.bot:
        return

    row = db_fetchone(
        "SELECT inviter_id FROM invite_member_links WHERE member_id = %s",
        (member.id,)
    )

    if not row:
        return

    inviter_id = int(row["inviter_id"])

    # remove mapping
    db_exec(
        "DELETE FROM invite_member_links WHERE member_id = %s",
        (member.id,)
    )

    # decrease invite count safely
    db_exec("""
        UPDATE invite_counts
        SET invites = CASE WHEN invites > 0 THEN invites - 1 ELSE 0 END
        WHERE user_id = %s
    """, (inviter_id,))

@bot.tree.command(name="inviteleaderboard", description="Show the MCWV invite leaderboard", guild=guild_obj)
async def inviteleaderboard(interaction: discord.Interaction):
    rows = db_fetchall("""
        SELECT user_id, invites
        FROM invite_counts
        ORDER BY invites DESC, user_id ASC
        LIMIT 10
    """)

    if not rows:
        return await interaction.response.send_message("No invite joins yet.", ephemeral=True)

    text = "\n".join(
        f"{i+1}. <@{r['user_id']}> — {r['invites']} joins"
        for i, r in enumerate(rows)
    )

    embed = discord.Embed(
        title="🏆 Invite Joins Leaderboard",
        description=text,
        color=discord.Color.blurple()
    )

    await interaction.response.send_message(embed=embed)


@tasks.loop(seconds=30)
async def check_invite_event():
    event = get_active_event()
    if not event or not event["active"]:
        return

    if int(time.time()) < event["end_time"]:
        return

    db_exec("UPDATE invite_events SET active = 0 WHERE id = 1")
    print("Invite event auto-ended")


async def setup_invite_system():
    global INVITE_SYSTEM_READY

    if INVITE_SYSTEM_READY:
        return

    INVITE_SYSTEM_READY = True

    if not check_invite_event.is_running():
        check_invite_event.start()

    for g in bot.guilds:
        await load_invite_snapshot(g)

    bot.add_view(InviteView())

# ---------------- BROADCAST SYSTEM ----------------
BROADCAST_DEFAULT_ROLE_IDS = {
    1225521918984061041,
    1226507841301516329,
    1194908177171480667,
}


def _parse_id_set(raw):
    ids = set()
    for part in str(raw or "").replace(";", ",").split(","):
        part = part.strip()
        if part.isdigit():
            ids.add(int(part))
    return ids


BROADCAST_ALLOWED_ROLE_IDS = _parse_id_set(os.environ.get("BROADCAST_ROLE_IDS")) or BROADCAST_DEFAULT_ROLE_IDS
BROADCAST_DEFAULT_USER_IDS = _parse_id_set(os.environ.get("BROADCAST_USER_IDS"))
BROADCAST_RECENT = {}
TICKET_STAFF_ROLE_ID = int(os.environ.get("TICKET_STAFF_ROLE_ID", "1501986357516701827"))
TICKET_IGNORE_ROLE_IDS = (
    _parse_id_set(os.environ.get("TICKET_IGNORE_ROLE_IDS"))
    or {TICKET_STAFF_ROLE_ID, 1502339420207059066}
)
TICKET_IGNORE_ROLE_IDS.add(TICKET_STAFF_ROLE_ID)
TICKET_IGNORE_ROLE_IDS.add(1502339420207059066)

MCWV_TICKET_CATEGORY_ID = int(os.environ.get("MCWV_TICKET_CATEGORY_ID", "1503106486392328333"))
MCWV_TICKET_PANEL_CHANNEL_ID = int(os.environ.get("MCWV_TICKET_PANEL_CHANNEL_ID", "1501613434364760174"))
MCWV_TICKET_LOG_CHANNEL_ID = int(os.environ.get("MCWV_TICKET_LOG_CHANNEL_ID", "1501997396610125876"))
MCWV_TICKET_REVIEW_CHANNEL_ID = int(os.environ.get("MCWV_TICKET_REVIEW_CHANNEL_ID", "1533390721145372784"))
MCWV_TICKET_MEMBER_ROLE_ID = int(os.environ.get("MCWV_TICKET_MEMBER_ROLE_ID", str(CLAN_MEMBER_ROLE_ID)))
MCWV_TICKET_BLACKLIST_ROLE_ID = int(os.environ.get("MCWV_TICKET_BLACKLIST_ROLE_ID", "1516151211735257259"))
MCWV_TICKET_STAFF_ROLE_IDS = _parse_id_set(os.environ.get("MCWV_TICKET_STAFF_ROLE_IDS")) or {ALLOWED_ROLE_ID, 1502339420207059066}
MCWV_TICKET_STAFF_ROLE_IDS.add(ALLOWED_ROLE_ID)
MCWV_TICKET_STAFF_ROLE_IDS.add(1502339420207059066)
MCWV_TICKET_DELETE_DELAY_SECONDS = max(5, int(os.environ.get("MCWV_TICKET_DELETE_DELAY_SECONDS", "20") or "20"))
MCWV_TICKET_MIN_SCREENSHOT_ATTACHMENTS = max(1, int(os.environ.get("MCWV_TICKET_MIN_SCREENSHOT_ATTACHMENTS", "1") or "1"))
MCWV_HUB_LINKS_ENABLED = os.environ.get("MCWV_HUB_LINKS_ENABLED", "0") == "1"
MCWV_TICKET_BANNER_PATH = os.environ.get(
    "MCWV_TICKET_BANNER_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets", "clan_application_banner.png"),
)
MCWV_PLACEMENT_CHANNEL_ID = int(os.environ.get("MCWV_PLACEMENT_CHANNEL_ID", "0") or "0")
MCWV_PLACEMENT_ALERTS_ENABLED_DEFAULT = os.environ.get("MCWV_PLACEMENT_ALERTS_ENABLED", "1") != "0"
MCWV_PLACEMENT_MIN_SECONDS = max(10, int(os.environ.get("MCWV_PLACEMENT_MIN_SECONDS", "45") or "45"))
MCWV_PLACEMENT_BG_PATH = os.environ.get(
    "MCWV_PLACEMENT_BG_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets", "placement_card_bg.webp"),
)
MCWV_LOG_CHANNEL_ID = int(os.environ.get("MCWV_LOG_CHANNEL_ID", "0") or "0")
MCWV_CLAN_LOGS_ENABLED_DEFAULT = os.environ.get("MCWV_CLAN_LOGS_ENABLED", "1") != "0"
MCWV_CLAN_LOG_INTERVAL_SECONDS = max(30, int(os.environ.get("MCWV_CLAN_LOG_INTERVAL_SECONDS", "60") or "60"))
MCWV_CLAN_LOG_MAX_DIAMOND_ALERTS = max(1, min(int(os.environ.get("MCWV_CLAN_LOG_MAX_DIAMOND_ALERTS", "8") or "8"), 25))
MCWV_HOURLY_STATS_CHANNEL_ID = int(os.environ.get("MCWV_HOURLY_STATS_CHANNEL_ID", "0") or "0")
MCWV_HOURLY_STATS_ENABLED_DEFAULT = os.environ.get("MCWV_HOURLY_STATS_ENABLED", "1") != "0"
MCWV_HOURLY_STATS_INTERVAL_MINUTES = max(5, int(os.environ.get("MCWV_HOURLY_STATS_INTERVAL_MINUTES", "60") or "60"))
MCWV_HOURLY_STATS_PING_ENABLED_DEFAULT = os.environ.get("MCWV_HOURLY_STATS_PING_ENABLED", "0") == "1"
MCWV_HOURLY_STATS_PING_THRESHOLD_DEFAULT = max(0, int(os.environ.get("MCWV_HOURLY_STATS_PING_THRESHOLD", "100") or "100"))
MCWV_HOURLY_STATS_START_TIME_DEFAULT = os.environ.get("MCWV_HOURLY_STATS_START_TIME", "").strip()
MCWV_HOURLY_STATS_PING_MESSAGE_DEFAULT = os.environ.get(
    "MCWV_HOURLY_STATS_PING_MESSAGE",
    "Reconnect/lock in and start gaining points now.\n\nEvery time you slack in points per hour, it’s recorded. Slack too much = get kicked.",
)
MCWV_HOURLY_STATS_BG_PATH = os.environ.get(
    "MCWV_HOURLY_STATS_BG_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets", "hourly_stats_bg.webp"),
)

# Automatically pause hourly stats when no clan war is active and resume them
# when the next war starts. Set MCWV_HOURLY_STATS_AUTO_WAR_TOGGLE=0 to keep them
# fully manual (only the /toggle_automation command and Dashboard button control them).
MCWV_HOURLY_STATS_AUTO_WAR_TOGGLE = os.environ.get("MCWV_HOURLY_STATS_AUTO_WAR_TOGGLE", "1") != "0"
HOURLY_STATS_AUTO_DISABLE_MISSES_REQUIRED = max(1, int(os.environ.get("MCWV_HOURLY_STATS_AUTO_DISABLE_MISSES", "2") or "2"))
PS99_GAMEPASS_UNIVERSE_ID = int(os.environ.get("PS99_GAMEPASS_UNIVERSE_ID", "3317771874"))
# Full official PS99 store gamepass list (id -> clean label). The runtime map
# is refreshed daily from Roblox's store page by _get_ps99_gamepass_map() and
# falls back to this baked copy when Roblox refuses to play nice.
PS99_STORE_PLACE_ID = 8737899170
PS99_GAMEPASSES = {
    205379487: "Lucky",
    257803774: "Ultra Lucky",
    257811346: "VIP",
    258567677: "Magic Eggs",
    259437976: "+15 Pets",
    264808140: "Huge Hunter",
    265320491: "Auto Farm",
    265324265: "Auto Tap",
    651611000: "Daycare Slots",
    655859720: "+15 Eggs",
    690997523: "Super Drops",
    720275150: "Double Stars",
    975558264: "Super Shiny Hunter",
}
_GAMEPASS_LIST_CACHE = {"at": 0.0, "passes": dict(PS99_GAMEPASSES)}
_GAMEPASS_LIST_TTL = 24 * 60 * 60
KNOWN_PS99_BATTLE_IDS = [
    "GummyBattle2026",
    "LunarBattle2026",
    "SoccerBattle2026",
    "Backrooms2026",
    "AngelBattle2026",
    "StarryBattle",
    "Spring2026",
    "BlockPartyBattle",
    "StrengthBattle",
    "TowerBattle",
    "BasketballBattle",
    "BalloonCorgiBattle",
    "PoisonTurtleBattle",
    "AthenaBattle",
]
PLAYER_WAR_HISTORY_CACHE = {}
PLAYER_WAR_HISTORY_TTL_SECONDS = 30 * 60
TOP_CLAN_HISTORY_CACHE = {}
TOP_CLAN_HISTORY_BUILD_TASK = None
TOP_CLAN_HISTORY_LIMIT = max(10, min(int(os.environ.get("PS99_TOP_CLAN_HISTORY_LIMIT", "100") or "100"), 100))
TOP_CLAN_HISTORY_TTL_SECONDS = max(10 * 60, int(os.environ.get("PS99_TOP_CLAN_HISTORY_TTL_SECONDS", str(60 * 60)) or str(60 * 60)))
TOP_CLAN_HISTORY_CONCURRENCY = max(2, min(int(os.environ.get("PS99_TOP_CLAN_HISTORY_CONCURRENCY", "10") or "10"), 20))

DEFAULT_MCWV_TICKET_SETTINGS = {
    "panel": {
        "title": "MCWV Applications",
        "description": "Ready to apply for MCWV? Open a private application ticket below. Inside the ticket, you’ll submit your Roblox details for staff review.",
        "buttonLabel": "Open Application",
        "accentColor": 0x34D399,
        "thumbnailUrl": "",
    },
    "messages": {
        "welcomeTitle": "Thank you for applying for MCWV!",
        "welcomeDescription": (
            "Please send the following screenshots of your:\n\n"
            "• Pets\n"
            "• Rank\n"
            "• Masteries\n"
            "• Enchants\n"
            "• Game-passes\n"
            "• Player profile *(found in trading plaza, double tap on avatar)*\n\n"
            "**Make sure the screenshots are NON-CROPPED!**"
        ),
    },
    "embedColors": {
        "banner": 0x34D399,
        "ticketInstructions": 0x34D399,
        "review": 0x34D399,
        "staffInfo": 0x60A5FA,
        "accepted": 0x22C55E,
        "closed": 0x22C55E,
        "reminder": 0xF59E0B,
    },
    "questions": [
        {"key": "roblox_username", "label": "Roblox username", "placeholder": "Your Roblox username", "style": "short", "required": True, "maxLength": 32},
        {"key": "afk_247", "label": "Can you AFK 24/7 on Windows?", "placeholder": "Yes/No + details", "style": "paragraph", "required": True, "maxLength": 500},
        {"key": "activity", "label": "Discord + in-game active hours", "placeholder": "Example: 6h Discord, 12h in-game", "style": "paragraph", "required": True, "maxLength": 500},
        {"key": "liquid_gems", "label": "Liquid gems you can spend per war", "placeholder": "Example: 5b liquid gems", "style": "paragraph", "required": True, "maxLength": 500},
        {"key": "why_accept", "label": "Why should we accept you?", "placeholder": "Tell us why you fit MCWV", "style": "paragraph", "required": True, "maxLength": 900},
    ],
    "features": {
        "openLimit": 1,
        "acceptButton": True,
        "closeButton": True,
        "staffInfoButton": True,
        "transcripts": True,
        "deleteAfterClose": True,
        "supportHours": False,
    },
}



def get_broadcast_allowed_user_ids():
    saved = _safe_call("db_get_setting", "", "broadcast_allowed_user_ids") or ""
    return set(BROADCAST_DEFAULT_USER_IDS) | _parse_id_set(saved)


def set_broadcast_allowed_user_ids(ids):
    cleaned = sorted({int(value) for value in ids if str(value).isdigit()})
    _safe_call("db_set_setting", None, "broadcast_allowed_user_ids", ",".join(str(value) for value in cleaned))
    return cleaned


def has_broadcast_permission(member):
    if not isinstance(member, discord.Member):
        return False

    if member.guild and member.guild.owner_id == member.id:
        return True

    if member.id in get_broadcast_allowed_user_ids():
        return True

    return any(role.id in BROADCAST_ALLOWED_ROLE_IDS for role in member.roles)


def normalize_ticket_key(value):
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def broadcast_actor_name(interaction):
    row = _safe_call("db_get_main_link", None, interaction.user.id)
    if row and len(row) > 1 and row[1]:
        return str(row[1])
    return getattr(interaction.user, "display_name", None) or interaction.user.name


async def fetch_broadcast_points_map():
    global session

    points = {}

    try:
        if session is None or session.closed:
            session = aiohttp.ClientSession()

        timeout = aiohttp.ClientTimeout(total=15)

        async with session.get(ACTIVE_BATTLE_API, timeout=timeout) as war_r:
            if war_r.status != 200:
                return points
            war_data = await war_r.json(content_type=None)

        async with session.get(CLAN_API, timeout=timeout) as clan_r:
            if clan_r.status != 200:
                return points
            clan_data = await clan_r.json(content_type=None)

        _battle_id, battle = get_current_war(war_data, clan_data)
        if not isinstance(battle, dict):
            return points

        contributions = (
            battle.get("PointContributions")
            or battle.get("pointContributions")
            or battle.get("Contributions")
            or []
        )

        for entry in contributions:
            if not isinstance(entry, dict):
                continue
            user_id = entry.get("UserID") or entry.get("userId") or entry.get("user_id")
            raw_points = entry.get("Points") or entry.get("points") or 0
            try:
                points[str(user_id).strip()] = int(float(raw_points or 0))
            except Exception:
                points[str(user_id).strip()] = 0

    except Exception as exc:
        print("fetch_broadcast_points_map error:", exc)

    return points



def fetch_broadcast_metrics_map():
    metrics = {}
    if not db_enabled():
        return metrics

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT to_regclass('public.player_leaderboard_history') IS NOT NULL AS exists
            """)
            exists = cur.fetchone()
            if not exists or not exists[0]:
                return metrics

            cur.execute("""
                WITH latest AS (
                    SELECT DISTINCT ON (roblox_id)
                        roblox_id::text AS roblox_id,
                        battle_id,
                        points::bigint AS points,
                        captured_at
                    FROM player_leaderboard_history
                    WHERE points IS NOT NULL
                    ORDER BY roblox_id, captured_at DESC
                )
                SELECT
                    l.roblox_id,
                    GREATEST(0, l.points - COALESCE(h.points, l.points)) AS pph,
                    GREATEST(0, l.points - COALESCE(f.points, l.points)) AS change_5m
                FROM latest l
                LEFT JOIN LATERAL (
                    SELECT points::bigint AS points, captured_at
                    FROM player_leaderboard_history p
                    WHERE p.roblox_id::text = l.roblox_id
                      AND p.points IS NOT NULL
                      AND p.battle_id IS NOT DISTINCT FROM l.battle_id
                      AND p.captured_at <= l.captured_at - INTERVAL '1 hour'
                    ORDER BY p.captured_at DESC
                    LIMIT 1
                ) h ON TRUE
                LEFT JOIN LATERAL (
                    SELECT points::bigint AS points, captured_at
                    FROM player_leaderboard_history p
                    WHERE p.roblox_id::text = l.roblox_id
                      AND p.points IS NOT NULL
                      AND p.battle_id IS NOT DISTINCT FROM l.battle_id
                      AND p.captured_at <= l.captured_at - INTERVAL '5 minutes'
                    ORDER BY p.captured_at DESC
                    LIMIT 1
                ) f ON TRUE
            """)
            for roblox_id, pph, change_5m in cur.fetchall():
                metrics[str(roblox_id)] = {
                    "pph": int(pph or 0),
                    "change5m": int(change_5m or 0),
                }
    except Exception as exc:
        print("fetch_broadcast_metrics_map error:", exc)

    return metrics


def broadcast_user_from_row(row, points_map, metrics_map):
    roblox_id = str(row[0]).strip() if len(row) > 0 and row[0] is not None else ""
    discord_id = int(row[1]) if len(row) > 1 and row[1] is not None else 0
    username = str(row[2]).strip() if len(row) > 2 and row[2] is not None else roblox_id
    role = str(row[3] or "member").strip().lower() if len(row) > 3 else "member"
    ticket_channel_id = int(row[4]) if len(row) > 4 and row[4] is not None else None
    points = int(points_map.get(roblox_id, 0))
    metrics = metrics_map.get(roblox_id, {}) if isinstance(metrics_map, dict) else {}

    # If the user has zero current war points, any old DB metric is stale for
    # the active battle. War points do not decrease, so last-hour gain must be 0.
    pph = int(metrics.get("pph", 0) or 0) if points > 0 else 0
    change5m = int(metrics.get("change5m", 0) or 0) if points > 0 else 0

    return {
        "roblox_id": roblox_id,
        "discord_id": discord_id,
        "username": username,
        "role": role,
        "ticket_channel_id": ticket_channel_id,
        "points": points,
        "pph": pph,
        "change5m": change5m,
        "rank": None,
    }


def dedupe_recipients(users):
    seen = set()
    deduped = []
    for user in users:
        discord_id = user.get("discord_id")
        if not discord_id or discord_id in seen:
            continue
        seen.add(discord_id)
        deduped.append(user)
    return deduped


async def resolve_broadcast_recipients(interaction, audience, value=None, role=None, user=None):
    rows = _safe_call("db_get_broadcast_users", []) or []
    points_map = await fetch_broadcast_points_map()
    metrics_map = fetch_broadcast_metrics_map()
    users = [broadcast_user_from_row(row, points_map, metrics_map) for row in rows]

    users.sort(key=lambda item: item["points"], reverse=True)
    for index, item in enumerate(users, start=1):
        item["rank"] = index

    audience = str(audience or "everyone")
    value = str(value or "").strip()

    if audience == "everyone":
        selected = users
    elif audience == "below_points":
        threshold = int(value or 0)
        selected = [item for item in users if item["points"] < threshold]
    elif audience == "above_points":
        threshold = int(value or 0)
        selected = [item for item in users if item["points"] > threshold]
    elif audience == "zero_points":
        selected = [item for item in users if item["points"] == 0]
    elif audience == "bottom_n":
        amount = max(1, int(value or 15))
        selected = sorted(users, key=lambda item: item["points"])[:amount]
    elif audience == "top_n":
        amount = max(1, int(value or 15))
        selected = sorted(users, key=lambda item: item["points"], reverse=True)[:amount]
    elif audience == "members":
        selected = [item for item in users if item["role"] == "member"]
    elif audience == "officers":
        selected = [item for item in users if item["role"] in ("officer", "owner")]
    elif audience == "discord_role":
        if not role:
            raise ValueError("Choose a Discord role for the Discord role audience filter.")
        selected = []
        guild = interaction.guild
        for item in users:
            member = guild.get_member(item["discord_id"]) if guild else None
            if member is None and guild:
                try:
                    member = await guild.fetch_member(item["discord_id"])
                except Exception:
                    member = None
            if member and any(r.id == role.id for r in member.roles):
                selected.append(item)
    elif audience == "custom_user":
        ids = set()
        if user:
            ids.add(int(user.id))
        for token in re.findall(r"\d{15,25}", value):
            ids.add(int(token))
        selected = [item for item in users if item["discord_id"] in ids]
    else:
        raise ValueError("Unknown broadcast audience filter.")

    # Enrich recipients with competitive + war context for template variables.
    next_by_id = {}
    for index, item in enumerate(users):
        above = users[index - 1] if index > 0 else None
        next_by_id[item["discord_id"]] = (
            above["username"] if above else "—",
            max(0, (above["points"] - item["points"])) if above else 0,
        )

    war_time_left = "—"
    clan_rank = None
    try:
        context = await get_broadcast_war_context()
        finish = context.get("finish")
        if finish:
            remaining = int(finish - time.time())
            war_time_left = (fmt_hm(remaining) + " left") if remaining > 0 else "ended"
        clan_rank = context.get("clan_rank")
    except Exception as exc:
        print(f"[broadcast] context vars failed: {exc}")

    deduped = dedupe_recipients(selected)
    for item in deduped:
        above_name, gap = next_by_id.get(item["discord_id"], ("—", 0))
        item["next_player"] = above_name
        item["next_rank_gap"] = gap
        item["war_time_left"] = war_time_left
        item["clan_rank"] = clan_rank

    return deduped


def render_broadcast_message(template, recipient):
    discord_id = recipient.get("discord_id") or ""
    ping = f"<@{discord_id}>" if discord_id else ""
    ticket_channel_id = recipient.get("ticket_channel_id")
    ticket = f"<#{ticket_channel_id}>" if ticket_channel_id else "—"
    return str(template or "").replace("{username}", str(recipient.get("username") or "")) \
        .replace("{points}", str(recipient.get("points", 0))) \
        .replace("{rank}", str(recipient.get("rank") or "—")) \
        .replace("{ping}", ping) \
        .replace("{mention}", ping) \
        .replace("{discord_id}", str(discord_id or "")) \
        .replace("{roblox_id}", str(recipient.get("roblox_id") or "")) \
        .replace("{role}", str(recipient.get("role") or "member")) \
        .replace("{ticket}", ticket) \
        .replace("{pph}", str(recipient.get("pph", 0))) \
        .replace("{change5m}", str(recipient.get("change5m", 0))) \
        .replace("{next_player}", str(recipient.get("next_player") or "—")) \
        .replace("{next_rank_gap}", str(recipient.get("next_rank_gap", 0))) \
        .replace("{war_time_left}", str(recipient.get("war_time_left") or "—")) \
        .replace("{clan_rank}", (f"#{recipient.get('clan_rank')}" if recipient.get("clan_rank") else "—"))


_BROADCAST_IMAGE_RE = re.compile(r"^https?://", re.IGNORECASE)


def clean_broadcast_image_url(value, max_len=1000):
    """Direct http(s) image link for broadcast artwork — '' when absent/invalid."""
    text = str(value or "").strip()[:max_len]
    if not text or not _BROADCAST_IMAGE_RE.match(text):
        return ""
    return text


def broadcast_embed_for(message, recipient, image_url=""):
    embed = discord.Embed(
        title="📢 MCWV Broadcast",
        description=render_broadcast_message(message, recipient),
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_footer(text="MCWV Staff Broadcast")
    if image_url:
        embed.set_image(url=image_url)
    return embed


async def _admin_broadcast_access_from_body(discord_id):
    if not discord_id:
        raise ValueError("discord_id is required")

    guild = broadcast_primary_guild()
    if not guild:
        raise ValueError("Broadcast guild is not available yet.")

    try:
        discord_id_int = int(discord_id)
    except Exception:
        raise ValueError("discord_id must be numeric")

    member = guild.get_member(discord_id_int)
    if member is None:
        try:
            member = await guild.fetch_member(discord_id_int)
        except Exception:
            member = None

    if member is None:
        return {"success": True, "allowed": False, "reason": "Member not found in Discord server"}

    return {
        "success": True,
        "allowed": has_broadcast_permission(member),
        "discord_id": str(discord_id_int),
        "hardcoded_user_ids": [str(value) for value in sorted(get_broadcast_allowed_user_ids())],
        "roles": [str(role.id) for role in member.roles],
    }


async def send_broadcast_to_recipient(guild, recipient, delivery, style, message, image_url=""):
    image_url = clean_broadcast_image_url(image_url)
    rendered = render_broadcast_message(message, recipient)
    embed = broadcast_embed_for(message, recipient, image_url) if style == "embed" else None
    content = None if embed else f"📢 **MCWV Broadcast**\n{rendered}"
    # Plain style has no embed — a bare image link still gets Discord's big
    # image preview, so artwork reaches DMs/tickets either way.
    if not embed and image_url:
        content = f"{content}\n{image_url}"

    async def _send_once():
        if delivery == "dm":
            member = guild.get_member(recipient["discord_id"]) if guild else None
            if member is None and guild:
                member = await guild.fetch_member(recipient["discord_id"])
            if member is None:
                raise RuntimeError("Member not found")
            await member.send(content=content, embed=embed)
            return "dm"

        if delivery == "ticket":
            channel_id = recipient.get("ticket_channel_id")
            if not channel_id:
                raise RuntimeError("No ticket channel saved")
            channel = guild.get_channel(channel_id) if guild else None
            if channel is None and guild:
                channel = await guild.fetch_channel(channel_id)
            if channel is None:
                raise RuntimeError("Ticket channel not found")
            await channel.send(content=content, embed=embed)
            return "ticket"

        raise RuntimeError("Unknown delivery method")

    try:
        return True, await _send_once(), None
    except Exception as first_error:
        await asyncio.sleep(2)
        try:
            return True, await _send_once(), None
        except Exception as second_error:
            return False, None, f"{type(second_error).__name__}: {second_error or first_error}"


def broadcast_primary_guild():
    guild = bot.get_guild(GUILD_ID)
    if guild:
        return guild
    return bot.guilds[0] if bot.guilds else None


def broadcast_payload_value(body, *keys, default=""):
    for key in keys:
        value = body.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return default


async def resolve_broadcast_recipients_from_body(body):
    guild = broadcast_primary_guild()
    if not guild:
        raise ValueError("Broadcast guild is not available yet.")

    audience = broadcast_payload_value(body, "audience", default="everyone")
    value = broadcast_payload_value(body, "value", "custom", "custom_user_ids", default="")
    role_id = broadcast_payload_value(body, "role_id", "roleId", default="")
    user_id = broadcast_payload_value(body, "user_id", "userId", default="")

    role = None
    if role_id:
        try:
            role = guild.get_role(int(role_id))
        except Exception:
            role = None
        if audience == "discord_role" and role is None:
            raise ValueError("Selected Discord role was not found.")

    if user_id:
        value = f"{value} {user_id}".strip()

    context = type("BroadcastContext", (), {"guild": guild})()
    return await resolve_broadcast_recipients(context, audience, value=value, role=role, user=None)


def broadcast_preview_payload(recipients, delivery):
    missing_tickets = [item for item in recipients if delivery == "ticket" and not item.get("ticket_channel_id")]
    deliverable_count = len(recipients) - len(missing_tickets)

    return {
        "recipientCount": len(recipients),
        "deliverableCount": deliverable_count,
        "missingTicketCount": len(missing_tickets),
        "sampleRecipients": recipients[:10],
        "missingTicketRecipients": missing_tickets[:25],
    }


async def _admin_broadcast_preview_from_body(body):
    delivery = broadcast_payload_value(body, "delivery", default="dm")
    recipients = await resolve_broadcast_recipients_from_body(body)
    return {
        "success": True,
        **broadcast_preview_payload(recipients, delivery),
    }


async def _admin_broadcast_send_from_body(body):
    guild = broadcast_primary_guild()
    if not guild:
        raise ValueError("Broadcast guild is not available yet.")

    audience = broadcast_payload_value(body, "audience", default="everyone")
    value = broadcast_payload_value(body, "value", "custom", "custom_user_ids", default="")
    delivery = broadcast_payload_value(body, "delivery", default="dm")
    style = broadcast_payload_value(body, "style", default="plain")
    message = broadcast_payload_value(body, "message", default="")
    image_url = clean_broadcast_image_url(
        broadcast_payload_value(body, "image_url", "imageUrl", "image", default="")
    )
    actor_name = broadcast_payload_value(body, "requested_by", "sender", default="Hub Admin")

    if delivery not in ("dm", "ticket"):
        raise ValueError("Unknown delivery method.")
    if style not in ("plain", "embed"):
        raise ValueError("Unknown broadcast style.")
    if not message:
        raise ValueError("Broadcast message is required.")

    recipients = await resolve_broadcast_recipients_from_body(body)
    if not recipients:
        raise ValueError("No recipients matched that broadcast filter.")

    fingerprint = f"web:{actor_name}:{audience}:{value}:{delivery}:{style}:{message}:{image_url}:{','.join(str(r['discord_id']) for r in recipients)}"
    now = time.time()
    for key, created in list(BROADCAST_RECENT.items()):
        if now - created > 300:
            BROADCAST_RECENT.pop(key, None)
    if fingerprint in BROADCAST_RECENT:
        raise ValueError("Duplicate broadcast blocked. Wait a few minutes before sending the same broadcast again.")
    BROADCAST_RECENT[fingerprint] = now

    sent = 0
    failed = []
    results = []

    for recipient in recipients:
        ok, _where, error = await send_broadcast_to_recipient(guild, recipient, delivery, style, message, image_url)
        results.append({
            "roblox_id": recipient.get("roblox_id"),
            "discord_id": recipient.get("discord_id"),
            "username": recipient.get("username"),
            "points_at_send": recipient.get("points", 0),
            "delivered": bool(ok),
            "error": error,
        })
        if ok:
            sent += 1
        else:
            failed.append((recipient, error))
        await asyncio.sleep(0.8)

    try:
        _ctx = await get_broadcast_war_context()
        db_record_broadcast_send(
            source="hub",
            actor=actor_name,
            actor_discord_id=None,
            audience=audience,
            value=value,
            delivery=delivery,
            style=style,
            message=message,
            image_url=image_url,
            results=results,
            battle_key=_ctx.get("battle_key") or "",
        )
    except Exception as exc:
        print(f"[broadcast] hub send record failed: {exc}")

    metadata = {
        "sender": actor_name,
        "audience": audience,
        "value": value,
        "delivery": delivery,
        "style": style,
        "message": message,
        "image_url": image_url,
        "recipientCount": len(recipients),
        "sent": sent,
        "failed": len(failed),
        "failedRecipients": [
            {
                "discord_id": str(item[0].get("discord_id")),
                "username": item[0].get("username"),
                "error": item[1],
            }
            for item in failed[:50]
        ],
    }

    db_log_admin_action(
        "info" if not failed else "warning",
        "Broadcast Sent",
        f"{actor_name} sent broadcast to {sent}/{len(recipients)} recipients via {delivery}.",
        "broadcast/send",
        actor_name,
        metadata,
    )

    return {
        "success": True,
        "message": f"Broadcast complete: {sent} sent, {len(failed)} failed.",
        "sent": sent,
        "failed": len(failed),
        "recipientCount": len(recipients),
        "failedRecipients": metadata["failedRecipients"],
    }


# ---------------- BROADCAST FEATURE TABLES ----------------

_BROADCAST_TABLES_READY = False


def ensure_broadcast_feature_tables():
    """Create the broadcast template/log/schedule tables (idempotent)."""
    global _BROADCAST_TABLES_READY
    if _BROADCAST_TABLES_READY or not db_enabled():
        return

    try:
        ensure_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS broadcast_templates (
                    id BIGSERIAL PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    audience TEXT NOT NULL DEFAULT 'everyone',
                    value TEXT NOT NULL DEFAULT '',
                    delivery TEXT NOT NULL DEFAULT 'dm',
                    style TEXT NOT NULL DEFAULT 'plain',
                    message TEXT NOT NULL,
                    created_by TEXT,
                    updated_by TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS broadcast_sends (
                    id BIGSERIAL PRIMARY KEY,
                    actor TEXT,
                    actor_discord_id TEXT,
                    source TEXT NOT NULL DEFAULT 'discord',
                    template_id BIGINT,
                    audience TEXT,
                    value TEXT,
                    delivery TEXT,
                    style TEXT,
                    message TEXT NOT NULL,
                    battle_key TEXT,
                    matched_count INTEGER NOT NULL DEFAULT 0,
                    sent_count INTEGER NOT NULL DEFAULT 0,
                    failed_count INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'done',
                    sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    conversion_checked_at TIMESTAMPTZ,
                    conversion_zero_at_send INTEGER,
                    conversion_scorers INTEGER,
                    conversion_points BIGINT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS broadcast_recipients (
                    id BIGSERIAL PRIMARY KEY,
                    send_id BIGINT NOT NULL REFERENCES broadcast_sends(id) ON DELETE CASCADE,
                    roblox_id TEXT,
                    discord_id TEXT,
                    username TEXT,
                    points_at_send BIGINT NOT NULL DEFAULT 0,
                    delivered BOOLEAN NOT NULL DEFAULT FALSE,
                    error TEXT
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS broadcast_recipients_send_idx
                ON broadcast_recipients (send_id)
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS broadcast_schedules (
                    id BIGSERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    audience TEXT NOT NULL DEFAULT 'everyone',
                    value TEXT NOT NULL DEFAULT '',
                    delivery TEXT NOT NULL DEFAULT 'dm',
                    style TEXT NOT NULL DEFAULT 'plain',
                    message TEXT NOT NULL DEFAULT '',
                    top_n INTEGER,
                    hours_before_end NUMERIC,
                    run_at TIMESTAMPTZ,
                    enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    created_by TEXT,
                    last_fired_at TIMESTAMPTZ,
                    last_fired_battle TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

            # Broadcast artwork columns (older DBs predate them). The hub runs
            # the same idempotent ALTERs, so either deploy order is safe.
            cur.execute("ALTER TABLE broadcast_templates ADD COLUMN IF NOT EXISTS image_url TEXT NOT NULL DEFAULT ''")
            cur.execute("ALTER TABLE broadcast_sends ADD COLUMN IF NOT EXISTS image_url TEXT NOT NULL DEFAULT ''")

            # Seed sensible defaults on first run only (all editable in the hub).
            cur.execute("SELECT COUNT(*) FROM broadcast_schedules")
            if int(cur.fetchone()[0] or 0) == 0:
                cur.executemany("""
                    INSERT INTO broadcast_schedules
                        (name, kind, audience, value, delivery, style, message, top_n, hours_before_end, enabled, created_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, [
                    (
                        "🏆 Auto congrats — war end",
                        "war_end_congrats",
                        "everyone",
                        "",
                        "dm",
                        "embed",
                        "🏆 War's over {username}! You finished **#{rank}** with **{points}** points. Absolute legend — rest up, next war soon 💜",
                        10,
                        None,
                        True,
                        "system",
                    ),
                    (
                        "⚠️ Final 24h lock-in (zero pointers)",
                        "war_final_hours",
                        "zero_points",
                        "",
                        "dm",
                        "plain",
                        "⚠️ {username}, war ends in {war_time_left} and you're still on 0 points. Get ANY score on the board or it's a warning! ⏳",
                        None,
                        24,
                        False,
                        "system",
                    ),
                    (
                        "⚔️ Mid-war push (low scorers)",
                        "war_midpoint",
                        "zero_points",
                        "",
                        "dm",
                        "plain",
                        "⚔️ {username}, we're halfway through the war and you're on {points} points — jump in, every point counts. Clan rank: {clan_rank}! 🔥",
                        None,
                        None,
                        False,
                        "system",
                    ),
                ])
        conn.commit()
        _BROADCAST_TABLES_READY = True
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[broadcast] table setup failed: {exc}")


# ---------------- BROADCAST TEMPLATE CRUD ----------------

def db_list_broadcast_templates(limit=100):
    if not db_enabled():
        return []
    try:
        ensure_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, audience, value, delivery, style, message, image_url, created_by, updated_by, updated_at
                FROM broadcast_templates
                ORDER BY name ASC
                LIMIT %s
            """, (int(limit),))
            rows = cur.fetchall()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[broadcast] list templates failed: {exc}")
        return []

    return [
        {
            "id": int(row[0]),
            "name": str(row[1] or ""),
            "audience": str(row[2] or "everyone"),
            "value": str(row[3] or ""),
            "delivery": str(row[4] or "dm"),
            "style": str(row[5] or "plain"),
            "message": str(row[6] or ""),
            "image_url": str(row[7] or ""),
            "created_by": str(row[8]) if row[8] else None,
            "updated_by": str(row[9]) if row[9] else None,
        }
        for row in rows
    ]


def db_get_broadcast_template(ref):
    """Look up a template by numeric id or exact (case-insensitive) name."""
    if not db_enabled() or ref is None:
        return None
    ref_text = str(ref).strip()
    if not ref_text:
        return None
    try:
        ensure_db_connection()
        with conn.cursor() as cur:
            if ref_text.isdigit():
                cur.execute("""
                    SELECT id, name, audience, value, delivery, style, message, image_url
                    FROM broadcast_templates WHERE id = %s
                """, (int(ref_text),))
            else:
                cur.execute("""
                    SELECT id, name, audience, value, delivery, style, message, image_url
                    FROM broadcast_templates WHERE LOWER(name) = LOWER(%s)
                """, (ref_text,))
            row = cur.fetchone()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[broadcast] get template failed: {exc}")
        return None

    if not row:
        return None
    return {
        "id": int(row[0]),
        "name": str(row[1] or ""),
        "audience": str(row[2] or "everyone"),
        "value": str(row[3] or ""),
        "delivery": str(row[4] or "dm"),
        "style": str(row[5] or "plain"),
        "message": str(row[6] or ""),
        "image_url": str(row[7] or ""),
    }


def db_create_broadcast_template(name, audience, delivery, style, message, value, actor, image_url=""):
    ensure_broadcast_feature_tables()
    if not db_enabled():
        raise ValueError("Database is not available.")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO broadcast_templates (name, audience, value, delivery, style, message, image_url, created_by, updated_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (name, audience, value or "", delivery, style, message, clean_broadcast_image_url(image_url), actor, actor))
            new_id = cur.fetchone()[0]
        conn.commit()
        return int(new_id)
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[broadcast] create template failed: {e}")
        raise


def db_delete_broadcast_template(ref):
    if not db_enabled():
        return False
    try:
        ensure_db_connection()
        with conn.cursor() as cur:
            if str(ref).strip().isdigit():
                cur.execute("DELETE FROM broadcast_templates WHERE id = %s", (int(ref),))
            else:
                cur.execute("DELETE FROM broadcast_templates WHERE LOWER(name) = LOWER(%s)", (str(ref).strip(),))
            deleted = cur.rowcount
        conn.commit()
        return deleted > 0
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[broadcast] delete template failed: {exc}")
        return False


# ---------------- BROADCAST SEND LOG + RECIPIENTS ----------------

def db_record_broadcast_send(*, source, actor, actor_discord_id, audience, value, delivery, style,
                             message, results, battle_key, template_id=None, image_url=None):
    """Persist a broadcast send + per-recipient rows for history & conversion."""
    ensure_broadcast_feature_tables()
    if not db_enabled():
        return None

    results = list(results or [])
    sent = sum(1 for item in results if item.get("delivered"))
    failed = len(results) - sent
    status = "done" if failed == 0 else ("partial" if sent > 0 else "failed")
    zero_at_send = sum(1 for item in results if int(item.get("points_at_send") or 0) <= 0)

    try:
        ensure_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO broadcast_sends
                    (actor, actor_discord_id, source, template_id, audience, value, delivery, style,
                     message, image_url, battle_key, matched_count, sent_count, failed_count, status, conversion_zero_at_send)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                actor,
                str(actor_discord_id) if actor_discord_id else None,
                source,
                template_id,
                audience,
                value or "",
                delivery,
                style,
                message,
                clean_broadcast_image_url(image_url),
                battle_key or None,
                len(results),
                sent,
                failed,
                status,
                zero_at_send,
            ))
            send_id = int(cur.fetchone()[0])

            if results:
                cur.executemany("""
                    INSERT INTO broadcast_recipients
                        (send_id, roblox_id, discord_id, username, points_at_send, delivered, error)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, [
                    (
                        send_id,
                        str(item.get("roblox_id") or ""),
                        str(item.get("discord_id") or ""),
                        str(item.get("username") or ""),
                        int(item.get("points_at_send") or 0),
                        bool(item.get("delivered")),
                        str(item.get("error"))[:300] if item.get("error") else None,
                    )
                    for item in results
                ])
        conn.commit()
        return send_id
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[broadcast] send log failed: {exc}")
        return None


# ---------------- BROADCAST WAR CONTEXT (cached) ----------------

_BROADCAST_WAR_CACHE = {"ts": 0.0, "data": {}}


async def get_broadcast_war_context(force=False):
    """Cached active-war info for broadcast vars: battle id/key, start/finish, clan rank."""
    now = time.time()
    if not force and _BROADCAST_WAR_CACHE["data"] and now - _BROADCAST_WAR_CACHE["ts"] < 300:
        return _BROADCAST_WAR_CACHE["data"]

    context = {"battle_id": None, "battle_key": "", "start": None, "finish": None, "clan_rank": None}
    try:
        battle_id = await get_active_battle_id_for_placement()
        if battle_id:
            context["battle_id"] = battle_id
            context["battle_key"] = normalize_hourly_battle_key(battle_id)

        active_payload = await fetch_json_for_placement(ACTIVE_BATTLE_API)
        active_data = active_payload.get("data", {}) if isinstance(active_payload, dict) else {}
        config = active_data.get("configData", {}) if isinstance(active_data, dict) else {}
        start = pick_first_int(config, ("StartTime", "startTime", "start_time")) or pick_first_int(active_data, ("startTime",))
        finish = pick_first_int(config, ("FinishTime", "finishTime", "finish_time")) or pick_first_int(active_data, ("finishTime",))
        if start and start > 10_000_000_000:
            start //= 1000
        if finish and finish > 10_000_000_000:
            finish //= 1000
        context["start"] = start
        context["finish"] = finish

        snapshot = await get_mcwv_placement_snapshot()
        if isinstance(snapshot, dict):
            context["clan_rank"] = snapshot.get("rank")
    except Exception as exc:
        print(f"[broadcast] war context failed: {exc}")

    _BROADCAST_WAR_CACHE["ts"] = now
    _BROADCAST_WAR_CACHE["data"] = context
    return context


# ---------------- BROADCAST CONVERSION CHECKS ----------------

def db_run_broadcast_conversion_checks(limit=5):
    """24h after a send, measure: zero-at-send recipients who scored, and points gained."""
    ensure_broadcast_feature_tables()
    if not db_enabled():
        return

    try:
        ensure_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, battle_key
                FROM broadcast_sends
                WHERE conversion_checked_at IS NULL
                  AND sent_at <= NOW() - INTERVAL '24 hours'
                ORDER BY sent_at ASC
                LIMIT %s
            """, (int(limit),))
            pending = cur.fetchall()

            for send_id, battle_key in pending:
                if not battle_key:
                    cur.execute("""
                        UPDATE broadcast_sends
                        SET conversion_checked_at = NOW()
                        WHERE id = %s
                    """, (send_id,))
                    continue

                cur.execute("""
                    WITH recip AS (
                        SELECT roblox_id, points_at_send
                        FROM broadcast_recipients
                        WHERE send_id = %s AND delivered
                    ),
                    latest AS (
                        SELECT DISTINCT ON (roblox_id)
                            roblox_id, points
                        FROM player_leaderboard_history
                        WHERE regexp_replace(lower(battle_id), '[^a-z0-9]+', '', 'g') = %s
                          AND points IS NOT NULL
                        ORDER BY roblox_id, captured_at DESC
                    )
                    SELECT
                        COUNT(*) FILTER (
                            WHERE r.points_at_send <= 0 AND COALESCE(l.points, 0) > 0
                        ) AS zero_starters,
                        COALESCE(SUM(GREATEST(0, COALESCE(l.points, r.points_at_send) - r.points_at_send)), 0) AS gained
                    FROM recip r
                    LEFT JOIN latest l ON l.roblox_id = r.roblox_id
                """, (send_id, battle_key))
                row = cur.fetchone() or (0, 0)
                cur.execute("""
                    UPDATE broadcast_sends
                    SET conversion_checked_at = NOW(),
                        conversion_scorers = %s,
                        conversion_points = %s
                    WHERE id = %s
                """, (send_id, int(row[0] or 0), int(row[1] or 0)))
        conn.commit()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[broadcast] conversion check failed: {exc}")


# ---------------- BROADCAST SCHEDULER ----------------

def db_get_enabled_broadcast_schedules():
    if not db_enabled():
        return []
    try:
        ensure_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, kind, audience, value, delivery, style, message,
                       top_n, hours_before_end, run_at, last_fired_at, last_fired_battle
                FROM broadcast_schedules
                WHERE enabled
                ORDER BY id ASC
            """)
            rows = cur.fetchall()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[broadcast] schedule list failed: {exc}")
        return []

    schedules = []
    for row in rows:
        schedules.append({
            "id": int(row[0]),
            "name": str(row[1] or "Broadcast"),
            "kind": str(row[2] or "one_time"),
            "audience": str(row[3] or "everyone"),
            "value": str(row[4] or ""),
            "delivery": str(row[5] or "dm"),
            "style": str(row[6] or "plain"),
            "message": str(row[7] or ""),
            "top_n": int(row[8]) if row[8] is not None else None,
            "hours_before_end": float(row[9]) if row[9] is not None else None,
            "run_at": row[10],
            "last_fired_at": row[11],
            "last_fired_battle": str(row[12]) if row[12] else "",
        })
    return schedules


def db_mark_schedule_fired(schedule_id, battle_key=None, disable=False):
    if not db_enabled():
        return
    try:
        ensure_db_connection()
        with conn.cursor() as cur:
            if disable:
                cur.execute("""
                    UPDATE broadcast_schedules
                    SET last_fired_at = NOW(), last_fired_battle = COALESCE(%s, last_fired_battle), enabled = FALSE
                    WHERE id = %s
                """, (battle_key or None, int(schedule_id)))
            else:
                cur.execute("""
                    UPDATE broadcast_schedules
                    SET last_fired_at = NOW(), last_fired_battle = COALESCE(%s, last_fired_battle)
                    WHERE id = %s
                """, (battle_key or None, int(schedule_id)))
        conn.commit()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[broadcast] schedule mark failed: {exc}")


async def build_war_end_congrats_recipients(top_n):
    """Top N scorers of the most recently ENDED war, matched to linked Discord users."""
    clan_payload = await fetch_json_for_placement(CLAN_API) if CLAN_API else None
    data = clan_payload.get("data", {}) if isinstance(clan_payload, dict) else {}
    battles = (data.get("Battles") or {}) if isinstance(data, dict) else {}
    if not isinstance(battles, dict) or not battles:
        return None, []

    now = time.time()
    ended = []
    for key, battle in battles.items():
        if not isinstance(battle, dict):
            continue
        finish = pick_first_int(battle, ("FinishTime", "finishTime", "finish_time", "EndTime", "endTime"))
        if finish and finish > 10_000_000_000:
            finish //= 1000
        ended.append((finish or 0, key, battle))

    ended.sort(key=lambda item: item[0], reverse=True)
    if not ended:
        return None, []

    _finish, battle_id, battle = ended[0]
    contributions = battle.get("PointContributions") or battle.get("pointContributions") or []
    scorers = []
    for entry in contributions if isinstance(contributions, list) else []:
        try:
            rid = str(int(entry.get("UserID") or entry.get("userId") or 0))
            pts = int(entry.get("Points") or entry.get("points") or 0)
            if rid and pts > 0:
                scorers.append((rid, pts))
        except Exception:
            continue
    scorers.sort(key=lambda item: item[1], reverse=True)
    scorers = scorers[:max(1, int(top_n or 10))]

    rows = _safe_call("db_get_broadcast_users", []) or []
    by_roblox = {str(row[0]).strip(): row for row in rows if len(row) > 0 and row[0] is not None}

    recipients = []
    for index, (rid, pts) in enumerate(scorers, start=1):
        row = by_roblox.get(rid)
        if not row:
            continue
        user = broadcast_user_from_row(row, {}, {})
        user["points"] = pts
        user["rank"] = index
        user["war_time_left"] = "ended"
        user["next_player"] = "—"
        user["next_rank_gap"] = 0
        user["clan_rank"] = None
        recipients.append(user)

    battle_key = normalize_hourly_battle_key(battle_id)
    return (battle_key, dedupe_recipients(recipients)) if battle_key else (None, [])


async def fire_broadcast_schedule(row, context):
    """Execute one schedule row: resolve recipients, rate-limited send, log it."""
    guild = broadcast_primary_guild()
    if not guild:
        raise ValueError("Broadcast guild unavailable")

    kind = row["kind"]
    battle_key = context.get("battle_key") or ""

    if kind == "war_end_congrats":
        battle_key, recipients = await build_war_end_congrats_recipients(row.get("top_n"))
        if not battle_key:
            raise ValueError("No ended battle found for congrats broadcast")
    else:
        fake_context = type("BroadcastContext", (), {"guild": guild})()
        recipients = await resolve_broadcast_recipients(
            fake_context,
            row.get("audience") or "everyone",
            value=row.get("value") or "",
        )

    if not recipients:
        raise ValueError("Schedule matched zero recipients")

    results = []
    sent = 0
    for recipient in recipients:
        ok, _where, error = await send_broadcast_to_recipient(
            guild, recipient, row.get("delivery") or "dm", row.get("style") or "plain", row["message"]
        )
        results.append({
            "roblox_id": recipient.get("roblox_id"),
            "discord_id": recipient.get("discord_id"),
            "username": recipient.get("username"),
            "points_at_send": recipient.get("points", 0),
            "delivered": bool(ok),
            "error": error,
        })
        if ok:
            sent += 1
        await asyncio.sleep(0.8)

    db_record_broadcast_send(
        source="auto_congrats" if kind == "war_end_congrats" else "scheduler",
        actor=row.get("name") or "Broadcast Scheduler",
        actor_discord_id=None,
        audience="top_n" if kind == "war_end_congrats" else row.get("audience"),
        value=str(row.get("top_n") or "") if kind == "war_end_congrats" else (row.get("value") or ""),
        delivery=row.get("delivery") or "dm",
        style=row.get("style") or "plain",
        message=row["message"],
        results=results,
        battle_key=battle_key,
    )

    db_log_admin_action(
        "info",
        "Scheduled Broadcast Sent",
        f"'{row.get('name')}' fired: {sent}/{len(recipients)} delivered ({kind}).",
        "broadcast/schedule",
        row.get("name") or "Scheduler",
        {"kind": kind, "sent": sent, "matched": len(recipients)},
    )
    return sent, len(recipients)


_CONGRATS_BATTLE_CACHE = {"ts": 0.0, "key": ""}


async def get_ended_battle_key_for_congrats():
    """Most recent ENDED battle's normalized key (cached 5 min)."""
    now = time.time()
    if _CONGRATS_BATTLE_CACHE["key"] and now - _CONGRATS_BATTLE_CACHE["ts"] < 300:
        return _CONGRATS_BATTLE_CACHE["key"]

    key = ""
    try:
        clan_payload = await fetch_json_for_placement(CLAN_API) if CLAN_API else None
        data = clan_payload.get("data", {}) if isinstance(clan_payload, dict) else {}
        battles = (data.get("Battles") or {}) if isinstance(data, dict) else {}
        ended = []
        for battle_id, battle in battles.items() if isinstance(battles, dict) else []:
            if not isinstance(battle, dict):
                continue
            finish = pick_first_int(battle, ("FinishTime", "finishTime", "finish_time", "EndTime", "endTime"))
            if finish and finish > 10_000_000_000:
                finish //= 1000
            ended.append((finish or 0, battle_id))
        ended.sort(key=lambda item: item[0], reverse=True)
        if ended:
            key = normalize_hourly_battle_key(ended[0][1])
    except Exception as exc:
        print(f"[broadcast] congrats battle lookup failed: {exc}")

    _CONGRATS_BATTLE_CACHE["ts"] = now
    _CONGRATS_BATTLE_CACHE["key"] = key
    return key


@tasks.loop(seconds=60)
async def broadcast_scheduler_loop():
    if not db_enabled():
        return

    ensure_broadcast_feature_tables()

    # Conversion checks run every tick (cheap, age-gated per send).
    db_run_broadcast_conversion_checks()

    rows = db_get_enabled_broadcast_schedules()
    if not rows:
        return

    context = await get_broadcast_war_context()
    now = time.time()
    now_dt = datetime.now(timezone.utc)

    for row in rows:
        try:
            kind = row["kind"]

            if kind == "one_time":
                run_at = row.get("run_at")
                if not run_at or row.get("last_fired_at"):
                    continue
                run_dt = run_at if getattr(run_at, "tzinfo", None) else run_at.replace(tzinfo=timezone.utc)
                if run_dt > now_dt:
                    continue
                db_mark_schedule_fired(row["id"], context.get("battle_key") or None, disable=True)
                await fire_broadcast_schedule(row, context)
                continue

            battle_key = context.get("battle_key") or ""
            start = context.get("start")
            finish = context.get("finish")

            if kind == "war_midpoint":
                if not battle_key or not start or not finish:
                    continue
                if row["last_fired_battle"] == battle_key:
                    continue
                if not (start <= now <= finish):
                    continue
                midpoint = start + (finish - start) / 2
                if now < midpoint:
                    continue
                db_mark_schedule_fired(row["id"], battle_key)
                await fire_broadcast_schedule(row, context)
                continue

            if kind == "war_final_hours":
                if not battle_key or not finish:
                    continue
                if row["last_fired_battle"] == battle_key:
                    continue
                hours = float(row.get("hours_before_end") or 24)
                if not (now >= finish - hours * 3600 and now <= finish):
                    continue
                db_mark_schedule_fired(row["id"], battle_key)
                await fire_broadcast_schedule(row, context)
                continue

            if kind == "war_end_congrats":
                ended_key = await get_ended_battle_key_for_congrats()
                if not ended_key:
                    continue
                if row["last_fired_battle"] == ended_key:
                    continue
                # Only fire once the ACTIVE battle has moved past this battle too.
                if battle_key and battle_key == ended_key and finish and now <= finish:
                    continue
                db_mark_schedule_fired(row["id"], ended_key)
                await fire_broadcast_schedule(row, context)
                continue

        except Exception as exc:
            print(f"[broadcast] schedule {row.get('id')} failed: {exc}")


@broadcast_scheduler_loop.before_loop
async def before_broadcast_scheduler_loop():
    await bot.wait_until_ready()



class BroadcastConfirmView(discord.ui.View):
    def __init__(self, *, sender_id, actor_name, recipients, audience, value, delivery, style, message, template_id=None, image_url=""):
        super().__init__(timeout=300)
        self.sender_id = sender_id
        self.actor_name = actor_name
        self.recipients = recipients
        self.audience = audience
        self.value = value
        self.delivery = delivery
        self.style = style
        self.message = message
        self.template_id = template_id
        self.image_url = clean_broadcast_image_url(image_url)
        self.done = False

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.sender_id:
            await interaction.response.send_message("Only the broadcast creator can confirm this.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Send Broadcast", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.done:
            return await interaction.response.send_message("This broadcast has already been handled.", ephemeral=True)

        fingerprint = f"{self.sender_id}:{self.audience}:{self.value}:{self.delivery}:{self.style}:{self.message}:{self.image_url}:{','.join(str(r['discord_id']) for r in self.recipients)}"
        now = time.time()
        for key, created in list(BROADCAST_RECENT.items()):
            if now - created > 300:
                BROADCAST_RECENT.pop(key, None)
        if fingerprint in BROADCAST_RECENT:
            return await interaction.response.send_message("Duplicate broadcast blocked. Wait a few minutes before sending the same broadcast again.", ephemeral=True)
        BROADCAST_RECENT[fingerprint] = now

        self.done = True
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(content="Sending broadcast...", view=self)

        sent = 0
        failed = []
        results = []

        for recipient in self.recipients:
            ok, _where, error = await send_broadcast_to_recipient(
                interaction.guild,
                recipient,
                self.delivery,
                self.style,
                self.message,
                self.image_url,
            )
            results.append({
                "roblox_id": recipient.get("roblox_id"),
                "discord_id": recipient.get("discord_id"),
                "username": recipient.get("username"),
                "points_at_send": recipient.get("points", 0),
                "delivered": bool(ok),
                "error": error,
            })
            if ok:
                sent += 1
            else:
                failed.append((recipient, error))
            await asyncio.sleep(0.8)

        try:
            _ctx = await get_broadcast_war_context()
            db_record_broadcast_send(
                source="discord",
                actor=self.actor_name,
                actor_discord_id=interaction.user.id,
                audience=self.audience,
                value=self.value,
                delivery=self.delivery,
                style=self.style,
                message=self.message,
                image_url=self.image_url,
                results=results,
                battle_key=_ctx.get("battle_key") or "",
                template_id=self.template_id,
            )
        except Exception as exc:
            print(f"[broadcast] send record failed: {exc}")

        metadata = {
            "senderDiscordId": str(interaction.user.id),
            "sender": self.actor_name,
            "audience": self.audience,
            "value": self.value,
            "delivery": self.delivery,
            "style": self.style,
            "message": self.message,
            "recipientCount": len(self.recipients),
            "sent": sent,
            "failed": len(failed),
            "failedRecipients": [
                {
                    "discord_id": str(item[0].get("discord_id")),
                    "username": item[0].get("username"),
                    "error": item[1],
                }
                for item in failed[:25]
            ],
        }

        db_log_admin_action(
            "info" if not failed else "warning",
            "Broadcast Sent",
            f"{self.actor_name} sent broadcast to {sent}/{len(self.recipients)} recipients via {self.delivery}.",
            "broadcast/send",
            self.actor_name,
            metadata,
        )

        embed = discord.Embed(
            title="Broadcast complete",
            description=f"✅ Sent: **{sent}**\n❌ Failed: **{len(failed)}**",
            color=discord.Color.green() if not failed else discord.Color.orange(),
        )
        if failed:
            missing_ticket = [item for item in failed if "No ticket channel saved" in str(item[1])]
            other_failed = [item for item in failed if item not in missing_ticket]

            if missing_ticket:
                preview = "\n".join(
                    f"• {recipient['username']} — no saved ticket"
                    for recipient, _error in missing_ticket[:15]
                )
                embed.add_field(name="Not sent: missing ticket", value=preview[:1024], inline=False)

            if other_failed:
                preview = "\n".join(f"• {r['username']} — {err}" for r, err in other_failed[:10])
                embed.add_field(name="Other failures", value=preview[:1024], inline=False)

        await interaction.followup.send(embed=embed, ephemeral=True)

    @discord.ui.button(label="🧪 Test send to me", style=discord.ButtonStyle.primary)
    async def testme(self, interaction: discord.Interaction, button: discord.ui.Button):
        sample = next(
            (r for r in self.recipients if int(r.get("discord_id") or 0) == interaction.user.id),
            self.recipients[0],
        )
        test_message = "🧪 **TEST BROADCAST — only you see this.**\n\n" + self.message
        ok, where, error = await send_broadcast_to_recipient(
            interaction.guild, sample, self.delivery, self.style, test_message, self.image_url
        )
        if ok:
            spot = "your DMs" if where == "dm" else "your ticket channel"
            await interaction.response.send_message(
                f"🧪 Test broadcast sent to **{spot}** — check it renders how you want, then hit Send Broadcast.",
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(f"❌ Test broadcast failed: {error}", ephemeral=True)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.done = True
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(content="Broadcast cancelled.", view=self)


@bot.tree.command(name="broadcast", description="Send a staff broadcast to selected MCWV members", guild=guild_obj)
@app_commands.describe(
    template="Pick a saved template (auto-fills audience/delivery/style/message)",
    audience="Who should receive the broadcast",
    delivery="Where to deliver the broadcast",
    style="Send as plain text or embed",
    message="Message to send. Vars: {username} {points} {rank} {ping} {pph} {next_player} {next_rank_gap} {war_time_left} {clan_rank} {ticket}",
    value="Threshold, N, or custom Discord IDs depending on audience",
    role="Discord role for the discord_role audience",
    user="Specific user for custom_user audience",
    image_url="Optional direct image link (https://…) shown with the broadcast",
    save_as_template="Save this exact broadcast setup as a new template with this name",
)
@app_commands.choices(
    audience=[
        app_commands.Choice(name="Everyone", value="everyone"),
        app_commands.Choice(name="Below X points", value="below_points"),
        app_commands.Choice(name="Above X points", value="above_points"),
        app_commands.Choice(name="Exactly 0 points", value="zero_points"),
        app_commands.Choice(name="Bottom N players", value="bottom_n"),
        app_commands.Choice(name="Top N players", value="top_n"),
        app_commands.Choice(name="Members", value="members"),
        app_commands.Choice(name="Officers", value="officers"),
        app_commands.Choice(name="Discord role", value="discord_role"),
        app_commands.Choice(name="Custom user(s)", value="custom_user"),
    ],
    delivery=[
        app_commands.Choice(name="DM", value="dm"),
        app_commands.Choice(name="Ticket", value="ticket"),
    ],
    style=[
        app_commands.Choice(name="Plain text", value="plain"),
        app_commands.Choice(name="Embed", value="embed"),
    ],
)
async def broadcast_command(
    interaction: discord.Interaction,
    template: str = "",
    audience: app_commands.Choice[str] = None,
    delivery: app_commands.Choice[str] = None,
    style: app_commands.Choice[str] = None,
    message: str = "",
    value: str = "",
    role: discord.Role = None,
    user: discord.Member = None,
    image_url: str = "",
    save_as_template: str = "",
):
    if not has_broadcast_permission(interaction.user):
        return await interaction.response.send_message("❌ You do not have permission to use broadcasts.", ephemeral=True)

    ensure_broadcast_feature_tables()

    tpl = None
    if str(template or "").strip():
        tpl = db_get_broadcast_template(str(template).strip())
        if not tpl:
            return await interaction.response.send_message(
                f"❌ No broadcast template found for `{template}`.", ephemeral=True
            )

    audience_value = audience.value if audience else (tpl["audience"] if tpl else "everyone")
    audience_label = audience.name if audience else (f"template: {tpl['audience']}" if tpl else "everyone")
    delivery_value = delivery.value if delivery else (tpl["delivery"] if tpl else "dm")
    delivery_label = delivery.name if delivery else (tpl["delivery"].upper() if tpl else "DM")
    style_value = style.value if style else (tpl["style"] if tpl else "plain")
    style_label = style.name if style else (tpl["style"].capitalize() if tpl else "Plain")
    message = str(message or "").strip() or (tpl["message"] if tpl else "")
    value = str(value or "").strip() or (tpl["value"] if tpl else "")

    # Explicit option wins; otherwise inherit the template's saved artwork.
    raw_image = str(image_url or "").strip()
    if raw_image and not clean_broadcast_image_url(raw_image):
        return await interaction.response.send_message(
            "❌ Image URL must start with http:// or https:// (or be left empty).", ephemeral=True
        )
    image_url = clean_broadcast_image_url(raw_image) or clean_broadcast_image_url(
        (tpl or {}).get("image_url")
    )

    if not message:
        return await interaction.response.send_message(
            "❌ A message is required — type one, or pick a template.",
            ephemeral=True,
        )

    await interaction.response.defer(ephemeral=True)

    try:
        recipients = await resolve_broadcast_recipients(
            interaction,
            audience_value,
            value=value,
            role=role,
            user=user,
        )
    except Exception as exc:
        return await interaction.followup.send(f"❌ Broadcast filter failed: {exc}", ephemeral=True)

    if not recipients:
        return await interaction.followup.send("No recipients matched that broadcast filter.", ephemeral=True)

    missing_ticket_recipients = []
    deliverable_count = len(recipients)

    if delivery_value == "ticket":
        missing_ticket_recipients = [item for item in recipients if not item.get("ticket_channel_id")]
        deliverable_count = len(recipients) - len(missing_ticket_recipients)

    actor_name = broadcast_actor_name(interaction)

    saved_note = ""
    if str(save_as_template or "").strip():
        tpl_name = str(save_as_template).strip()
        try:
            db_create_broadcast_template(
                tpl_name, audience_value, delivery_value, style_value, message, value, actor_name,
                image_url=image_url,
            )
            saved_note = f"💾 Saved as template **{tpl_name}**\n"
        except Exception:
            saved_note = f"⚠️ Could not save template **{tpl_name}** (name may already exist)\n"

    sample = recipients[:10]
    sample_text = "\n".join(
        f"• {item['username']} — {item['points']} pts — <@{item['discord_id']}>"
        for item in sample
    )

    embed = discord.Embed(
        title="Broadcast Preview",
        description=(
            f"{saved_note}"
            f"**Template:** {tpl['name'] if tpl else '—'}\n"
            f"**Audience:** {audience_label}\n"
            f"**Value:** {value or '—'}\n"
            f"**Delivery:** {delivery_label}\n"
            f"**Style:** {style_label}\n"
            f"**Image:** {'shown below 🖼️' if image_url else '—'}\n"
            f"**Recipients matched:** {len(recipients)}\n"
            f"**Will attempt:** {deliverable_count if delivery_value == 'ticket' else len(recipients)}\n"
            f"**Will fail / no ticket:** {len(missing_ticket_recipients) if delivery_value == 'ticket' else 0}\n\n"
            f"**Message:**\n{message[:1200]}"
        ),
        color=discord.Color.orange(),
    )
    if image_url:
        embed.set_image(url=image_url)
    embed.add_field(name="Sample recipients", value=sample_text[:1024] or "—", inline=False)

    if missing_ticket_recipients:
        missing_preview = "\n".join(
            f"• {item['username']} — no saved ticket"
            for item in missing_ticket_recipients[:10]
        )
        embed.add_field(
            name="Will not send to these ticket recipients",
            value=missing_preview[:1024],
            inline=False,
        )

    if len(recipients) > 25:
        embed.set_footer(text="Large broadcast: confirmation required and sending will be rate-limited.")

    view = BroadcastConfirmView(
        sender_id=interaction.user.id,
        actor_name=actor_name,
        recipients=recipients,
        audience=audience_value,
        value=value,
        delivery=delivery_value,
        style=style_value,
        message=message,
        template_id=tpl["id"] if tpl else None,
        image_url=image_url,
    )

    await interaction.followup.send(embed=embed, view=view, ephemeral=True)


@broadcast_command.autocomplete("template")
async def broadcast_template_autocomplete(interaction: discord.Interaction, current: str):
    if not has_broadcast_permission(interaction.user):
        return []
    try:
        templates = db_list_broadcast_templates(limit=25)
        needle = str(current or "").strip().lower()
        matches = [
            app_commands.Choice(name=f"{t['name']} ({t['audience']})", value=str(t["id"]))
            for t in templates
            if not needle or needle in t["name"].lower()
        ]
        return matches[:25]
    except Exception:
        return []


@bot.tree.command(name="broadcast_templates", description="Manage saved broadcast templates", guild=guild_obj)
@app_commands.describe(
    action="List, create or delete templates",
    name="Template name (create/delete)",
    message="Template message text (create)",
    audience="Default audience (create)",
    delivery="Default delivery (create)",
    style="Default style (create)",
    value="Default value/threshold (create)",
    image_url="Optional image link saved on the template (create)",
)
@app_commands.choices(
    action=[
        app_commands.Choice(name="List", value="list"),
        app_commands.Choice(name="Create", value="create"),
        app_commands.Choice(name="Delete", value="delete"),
    ],
    audience=[
        app_commands.Choice(name="Everyone", value="everyone"),
        app_commands.Choice(name="Below X points", value="below_points"),
        app_commands.Choice(name="Above X points", value="above_points"),
        app_commands.Choice(name="Exactly 0 points", value="zero_points"),
        app_commands.Choice(name="Bottom N players", value="bottom_n"),
        app_commands.Choice(name="Top N players", value="top_n"),
        app_commands.Choice(name="Members", value="members"),
        app_commands.Choice(name="Officers", value="officers"),
    ],
    delivery=[
        app_commands.Choice(name="DM", value="dm"),
        app_commands.Choice(name="Ticket", value="ticket"),
    ],
    style=[
        app_commands.Choice(name="Plain text", value="plain"),
        app_commands.Choice(name="Embed", value="embed"),
    ],
)
async def broadcast_templates_command(
    interaction: discord.Interaction,
    action: app_commands.Choice[str],
    name: str = "",
    message: str = "",
    audience: app_commands.Choice[str] = None,
    delivery: app_commands.Choice[str] = None,
    style: app_commands.Choice[str] = None,
    value: str = "",
    image_url: str = "",
):
    if not has_broadcast_permission(interaction.user):
        return await interaction.response.send_message("❌ You do not have permission to manage broadcast templates.", ephemeral=True)

    ensure_broadcast_feature_tables()
    actor_name = broadcast_actor_name(interaction)

    if action.value == "list":
        templates = db_list_broadcast_templates(limit=25)
        if not templates:
            return await interaction.response.send_message(
                "No broadcast templates yet — create one with `/broadcast_templates create`.",
                ephemeral=True,
            )
        lines = []
        for tpl in templates:
            short = tpl["message"][:60] + ("…" if len(tpl["message"]) > 60 else "")
            art = " · 🖼️" if tpl.get("image_url") else ""
            lines.append(f"**#{tpl['id']} {tpl['name']}**\n   {tpl['audience']} · {tpl['delivery']} · {tpl['style']}{art}\n   _{short}_")
        embed = discord.Embed(
            title=f"📋 Broadcast Templates ({len(templates)})",
            description="\n".join(lines)[:4000],
            color=MCWV_BRAND_COLOR if "MCWV_BRAND_COLOR" in globals() else discord.Color.blurple(),
        )
        return await interaction.response.send_message(embed=embed, ephemeral=True)

    if action.value == "create":
        if not name.strip():
            return await interaction.response.send_message("❌ Give the template a name.", ephemeral=True)
        if not message.strip():
            return await interaction.response.send_message("❌ Give the template a message.", ephemeral=True)
        clean_image = clean_broadcast_image_url(image_url)
        if str(image_url or "").strip() and not clean_image:
            return await interaction.response.send_message(
                "❌ Image URL must start with http:// or https:// (or be left empty).", ephemeral=True
            )
        try:
            new_id = db_create_broadcast_template(
                name.strip(),
                audience.value if audience else "everyone",
                delivery.value if delivery else "dm",
                style.value if style else "plain",
                message.strip(),
                value.strip(),
                actor_name,
                image_url=clean_image,
            )
            art_note = " with 🖼️ artwork" if clean_image else ""
            return await interaction.response.send_message(
                f"✅ Template **{name.strip()}** created (#{new_id}){art_note}. Use it via `/broadcast template:` — variables like {{username}} {{points}} {{rank}} work.",
                ephemeral=True,
            )
        except Exception as exc:
            return await interaction.response.send_message(
                f"❌ Could not create template: {exc}", ephemeral=True
            )

    if action.value == "delete":
        if not name.strip():
            return await interaction.response.send_message("❌ Give the template name or id to delete.", ephemeral=True)
        deleted = db_delete_broadcast_template(name.strip())
        if deleted:
            return await interaction.response.send_message(f"🗑️ Template `{name.strip()}` deleted.", ephemeral=True)
        return await interaction.response.send_message(f"❌ No template found for `{name.strip()}`.", ephemeral=True)

    return await interaction.response.send_message("Unknown action.", ephemeral=True)


@broadcast_templates_command.autocomplete("name")
async def broadcast_templates_name_autocomplete(interaction: discord.Interaction, current: str):
    if not has_broadcast_permission(interaction.user):
        return []
    try:
        templates = db_list_broadcast_templates(limit=25)
        needle = str(current or "").strip().lower()
        return [
            app_commands.Choice(name=t["name"], value=t["name"])
            for t in templates
            if not needle or needle in t["name"].lower()
        ][:25]
    except Exception:
        return []


def likely_ticket_channel(channel):
    category_name = getattr(getattr(channel, "category", None), "name", "")
    combined = normalize_ticket_key(f"{getattr(channel, 'name', '')} {getattr(channel, 'topic', '') or ''} {category_name}")
    return "ticket" in combined or "support" in combined or "application" in combined


def ticket_sync_channels(guild, category=None, scan_all=False):
    channels = []
    seen = set()

    def add_channel(channel):
        if isinstance(channel, discord.TextChannel) and channel.id not in seen:
            seen.add(channel.id)
            channels.append(channel)

    if category:
        for channel in getattr(category, "channels", []):
            add_channel(channel)
        return channels

    for category_id in CLAN_MEMBER_CATEGORY_IDS:
        cat = guild.get_channel(category_id)
        for channel in getattr(cat, "channels", []) if cat else []:
            add_channel(channel)

    for cat in guild.categories:
        cat_key = normalize_ticket_key(cat.name)
        if "ticket" in cat_key or "support" in cat_key or "application" in cat_key:
            for channel in cat.channels:
                add_channel(channel)

    for channel in guild.text_channels:
        if scan_all or likely_ticket_channel(channel):
            add_channel(channel)

    return channels


async def build_ticket_sync_candidates(guild, users):
    candidates = []

    for row in users:
        roblox_id = str(row[0]).strip() if len(row) > 0 else ""
        discord_id = int(row[1]) if len(row) > 1 and row[1] else 0
        username = str(row[2]).strip() if len(row) > 2 else roblox_id
        keys = {normalize_ticket_key(username), normalize_ticket_key(str(discord_id)), normalize_ticket_key(roblox_id)}

        # Keep ticket sync fast: do not fetch every linked member from Discord here.
        # Fetching hundreds of members can make /broadcast_ticket_sync run for several minutes.
        # Cached members still improve name matching, and visible ticket members are checked directly per channel below.
        member = guild.get_member(discord_id) if discord_id else None

        if member:
            keys.add(normalize_ticket_key(member.name))
            keys.add(normalize_ticket_key(member.display_name))
            keys.add(normalize_ticket_key(getattr(member, "global_name", None)))
            keys.add(normalize_ticket_key(getattr(member, "nick", None)))

        keys = {key for key in keys if len(key) >= 3}
        candidates.append({
            "discord_id": discord_id,
            "username": username,
            "keys": keys,
            "matched": False,
        })

    return candidates



def candidate_by_discord_id(candidates, discord_id):
    try:
        target = int(discord_id)
    except Exception:
        return None
    return next((candidate for candidate in candidates if int(candidate.get("discord_id") or 0) == target), None)


def visible_non_staff_ticket_members(channel):
    members = []

    # Do NOT use channel.members here. On large servers it can scan every guild member
    # and make /broadcast_ticket_sync take several minutes. Ticket bots normally add
    # a member-specific permission overwrite for the ticket owner, so read overwrites.
    overwrites = getattr(channel, "overwrites", {}) or {}
    for target in overwrites.keys():
        if not isinstance(target, discord.Member):
            continue
        if getattr(target, "bot", False):
            continue
        member_role_ids = {getattr(role, "id", 0) for role in getattr(target, "roles", [])}
        if member_role_ids.intersection(TICKET_IGNORE_ROLE_IDS):
            continue
        members.append(target)

    return members



class TicketLinkUserSelect(discord.ui.UserSelect):
    def __init__(self, channel_id):
        super().__init__(
            placeholder="Select the clan member this ticket belongs to",
            min_values=1,
            max_values=1,
        )
        self.channel_id = int(channel_id)

    async def callback(self, interaction: discord.Interaction):
        if not has_broadcast_permission(interaction.user):
            return await interaction.response.send_message("❌ You do not have permission to save ticket links.", ephemeral=True)

        member = self.values[0]
        if db_set_ticket_channel(member.id, self.channel_id):
            await interaction.response.send_message(
                f"✅ Saved this ticket for {member.mention}.",
                ephemeral=True,
            )
            try:
                self.view.stop()
                for child in self.view.children:
                    child.disabled = True
                await interaction.message.edit(view=self.view)
            except Exception:
                pass
        else:
            await interaction.response.send_message(
                f"⚠️ {member.mention} is not linked in the bot database yet. Link/accept them first, then try again.",
                ephemeral=True,
            )


class TicketLinkResolveView(discord.ui.View):
    def __init__(self, channel_id):
        super().__init__(timeout=7 * 24 * 60 * 60)
        self.add_item(TicketLinkUserSelect(channel_id))


async def send_ticket_resolve_menu(channel):
    try:
        await channel.send(
            "I couldn't automatically tell which clan member this ticket belongs to. "
            "Please select/ping the member below and I'll save this ticket for broadcasts.",
            view=TicketLinkResolveView(channel.id),
            allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
        )
        return True
    except Exception as exc:
        print(f"[ticket sync] failed to send resolver in #{getattr(channel, 'name', channel.id)}: {exc}")
        return False



def has_officer_guide_permission(member):
    if not isinstance(member, discord.Member):
        return False
    if member.guild and member.guild.owner_id == member.id:
        return True
    return any(getattr(role, "id", 0) == OFFICER_GUIDE_ROLE_ID for role in getattr(member, "roles", []))


def officer_guide_embed(section="overview"):
    section = str(section or "overview")
    embed = discord.Embed(
        title="MCWV Officer Guide",
        color=discord.Color.from_rgb(52, 211, 153),
        timestamp=datetime.now(timezone.utc),
    )

    if section == "overview":
        embed.description = (
            "A quick officer tutorial for MCWV-BOT. This guide only covers tools officers can use — "
            "owner-only website actions are intentionally left out."
        )
        embed.add_field(
            name="How to use this guide",
            value=(
                "Use the dropdown below to jump between command groups.\n"
                "Most commands are slash commands. Some are public info commands, but the management tools require officer permissions."
            ),
            inline=False,
        )
        embed.add_field(
            name="Safe officer workflow",
            value=(
                "1. Check info first.\n"
                "2. Preview broadcasts before sending.\n"
                "3. Use ticket delivery for individual nudges.\n"
                "4. Be careful with cleanup/unlink-style tools."
            ),
            inline=False,
        )

    elif section == "war":
        embed.description = "War, leaderboard, profile, and status commands."
        embed.add_field(
            name="War commands",
            value=(
                "`/warinfo` — shows current PS99 war details.\n"
                "`/leaderboard` — posts the current MCWV contribution leaderboard.\n"
                "`/mystats <roblox_username>` — shows one Roblox user's war contribution stats.\n"
                "`/profile <roblox_username>` — opens a linked user's profile dashboard.\n"
                "`/clanstats` — shows MCWV clan overview, level, gems, members, and battle history.\n"
                "`/compare <member1> <member2>` — compares two linked members in the current war."
            ),
            inline=False,
        )
        embed.add_field(
            name="Presence/status commands",
            value=(
                "`/status <member>` — checks a linked member's Roblox presence.\n"
                "`/offlinelist` — lists tracked users currently offline and how long.\n"
                "`/toggleoffline` — turns offline ping alerts on/off.\n"
                "`/testreminder` — sends an offline reminder immediately for testing."
            ),
            inline=False,
        )

    elif section == "members":
        embed.description = "Commands for Roblox links, alts, profiles, and ticket acceptance."
        embed.add_field(
            name="Linking and roster commands",
            value=(
                "`/add <member> <roblox_username>` — links a Discord member to a Roblox account.\n"
                "`/list` — shows all tracked users.\n"
                "`/refreshprofile <roblox_id>` — clears/refreshes cached profile data for a Roblox user.\n"
                "`/accept <member>` — accepts an applicant in a ticket and saves their ticket channel.\n"
                "`/memberedit <member> <roblox_username> [alts] [channel]` — fixes main Roblox username, alts, and optional ticket channel."
            ),
            inline=False,
        )
        embed.add_field(
            name="Alt commands",
            value=(
                "`/addalt <member> <roblox_username>` — adds an alt Roblox account to a member.\n"
                "`/listalts <member>` — lists a member's main and alt Roblox accounts.\n"
                "`/removealt <member> <alt>` — removes one alt from a member."
            ),
            inline=False,
        )
        embed.add_field(
            name="Cleanup command",
            value="`/cleanup <target> [reason]` — removes clan role/unlinks tracking for a target. Use carefully and only when you're sure.",
            inline=False,
        )

    elif section == "broadcast":
        embed.description = "Broadcast commands and ticket-delivery tools."
        embed.add_field(
            name="Broadcast commands",
            value=(
                "`/broadcast` — sends a staff broadcast by DM or saved ticket. Supports filters like everyone, below/above points, zero points, top/bottom N, role, custom users.\n"
                "`/broadcast_ticket_sync [category] [scan_all] [send_menus] [name_fallback]` — scans tickets and saves ticket channels for linked members.\n"
                "`/broadcast_ticket_link <member> [channel]` — manually saves one member's ticket channel."
            ),
            inline=False,
        )
        embed.add_field(
            name="Broadcast variables",
            value=(
                "`{ping}` / `{mention}` — mention the user\n"
                "`{username}` — Roblox username\n"
                "`{points}` — current war points\n"
                "`{pph}` — points gained in the last hour\n"
                "`{change5m}` — points gained in the last 5 minutes\n"
                "`{rank}` — current broadcast rank\n"
                "`{ticket}` — saved ticket channel mention\n"
                "`{roblox_id}`, `{discord_id}`, `{role}` — IDs/role metadata"
            ),
            inline=False,
        )
        embed.add_field(
            name="Recommended process",
            value="Preview first, check matched/missing recipients, then send. For ticket delivery, run `/broadcast_ticket_sync` first.",
            inline=False,
        )

    elif section == "events":
        embed.description = "Giveaway and invite-event commands."
        embed.add_field(
            name="Giveaway commands",
            value=(
                "`/giveaway_start` — starts a giveaway with prize, winners, invite requirement, and optional image.\n"
                "`/giveaway_edit` — edits the active giveaway settings.\n"
                "`/giveaway_end` — ends the active giveaway and picks winners."
            ),
            inline=False,
        )
        embed.add_field(
            name="Invite event commands",
            value=(
                "`/host_invite_event <duration_hours>` — starts an invite competition.\n"
                "`/end_invite_event` — ends the invite event.\n"
                "`/inviteleaderboard` — shows the invite leaderboard.\n"
                "`/invite_snapshot_refresh` — refreshes invite snapshots.\n"
                "`/invite_debug` — shows invite system debug info.\n"
                "`/invite_simulate <amount>` — simulates invite progress for testing.\n"
                "`/invite_full_test` — runs a full invite-system test."
            ),
            inline=False,
        )

    elif section == "settings":
        embed.description = "Settings, reminders, and diagnostics commands."
        embed.add_field(
            name="Settings/reminder commands",
            value=(
                "`/settings` — shows current bot settings.\n"
                "`/setreminderinterval <minutes>` — changes offline reminder interval.\n"
                "`/setreminderchannel <channel>` — sets where offline reminders are sent.\n"
                "`/clanwar` — toggles clan war tracking on/off."
            ),
            inline=False,
        )
        embed.add_field(
            name="Diagnostics commands",
            value=(
                "`/ping` — quick bot response test.\n"
                "`/dbtest` — checks DB/user tracking health.\n"
                "`/statstest` — shows a sample tracked user row.\n"
                "`/guide` — opens this officer guide."
            ),
            inline=False,
        )

    elif section == "safety":
        embed.description = "Officer safety notes."
        embed.add_field(
            name="Do",
            value=(
                "• Preview broadcasts before sending.\n"
                "• Prefer ticket delivery for individual reminders.\n"
                "• Double-check member links before editing.\n"
                "• Use `/broadcast_ticket_link` for one-off ticket fixes.\n"
                "• Ask another officer if you're unsure."
            ),
            inline=False,
        )
        embed.add_field(
            name="Avoid",
            value=(
                "• Spamming broad audiences.\n"
                "• Running cleanup unless you are certain.\n"
                "• Sharing bot/API secrets.\n"
                "• Guessing ticket owners when the resolver menu can confirm them."
            ),
            inline=False,
        )

    else:
        embed.description = "Unknown guide section. Pick a topic from the menu below."

    embed.set_footer(text="MCWV-BOT Officer Guide • Use /guide anytime")
    return embed


class OfficerGuideSelect(discord.ui.Select):
    def __init__(self):
        super().__init__(
            placeholder="Choose a guide topic",
            min_values=1,
            max_values=1,
            options=[
                discord.SelectOption(label="Overview", value="overview", emoji="🏠", description="How to use the officer guide"),
                discord.SelectOption(label="War + Stats", value="war", emoji="⚔️", description="War info, leaderboard, profiles, comparisons"),
                discord.SelectOption(label="Members + Links", value="members", emoji="👥", description="Roblox links, alts, accept, cleanup"),
                discord.SelectOption(label="Broadcasts + Tickets", value="broadcast", emoji="📢", description="Broadcasts, variables, ticket sync/link"),
                discord.SelectOption(label="Giveaways + Invites", value="events", emoji="🎉", description="Giveaway and invite event commands"),
                discord.SelectOption(label="Settings + Tests", value="settings", emoji="⚙️", description="Settings, reminders, diagnostics"),
                discord.SelectOption(label="Safety", value="safety", emoji="🛡️", description="Officer safety rules"),
            ],
        )

    async def callback(self, interaction: discord.Interaction):
        if not has_officer_guide_permission(interaction.user):
            return await interaction.response.send_message("❌ You do not have permission to use the officer guide.", ephemeral=True)
        await interaction.response.edit_message(embed=officer_guide_embed(self.values[0]), view=self.view)


class OfficerGuideView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=15 * 60)
        self.add_item(OfficerGuideSelect())



def has_mcwv_ticket_staff_permission(member):
    if not isinstance(member, discord.Member):
        return False
    if member.guild and member.guild.owner_id == member.id:
        return True
    role_ids = {getattr(role, "id", 0) for role in getattr(member, "roles", [])}
    return bool(role_ids.intersection(MCWV_TICKET_STAFF_ROLE_IDS)) or has_officer_guide_permission(member)


async def resolve_roblox_username_basic(username):
    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession()
    async with session.post(
        "https://users.roblox.com/v1/usernames/users",
        json={"usernames": [username], "excludeBannedUsers": False},
        timeout=aiohttp.ClientTimeout(total=15),
    ) as res:
        if res.status != 200:
            return None
        payload = await res.json(content_type=None)
    data = payload.get("data", []) if isinstance(payload, dict) else []
    if not data:
        return None
    return {"id": str(data[0]["id"]), "name": str(data[0]["name"])}


async def get_roblox_headshot_url(roblox_id):
    global session
    try:
        user_id = int(str(roblox_id).strip())
    except Exception:
        return None

    if session is None or session.closed:
        session = aiohttp.ClientSession()

    url = (
        "https://thumbnails.roblox.com/v1/users/avatar-headshot"
        f"?userIds={user_id}&size=150x150&format=Png&isCircular=true"
    )
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as res:
            if res.status != 200:
                return None
            payload = await res.json(content_type=None)
        data = payload.get("data", []) if isinstance(payload, dict) else []
        if data and data[0].get("imageUrl"):
            return str(data[0]["imageUrl"])
    except Exception as exc:
        print(f"[ticket] roblox headshot lookup failed for {roblox_id}: {exc}")
    return None


def build_application_review_embed(ticket_id, applicant, roblox_name, roblox_id, afk_247, activity, liquid_gems, why_accept, claimed_by=None, avatar_url=None):
    embed = discord.Embed(
        title="MCWV Application Ready for Review",
        description=(
            f"{applicant.mention} submitted an application. Staff can use **Staff Info** for answers, checks, and gamepass verification."
        ),
        color=discord.Color(get_ticket_embed_color("review", 0x34D399)),
        timestamp=datetime.now(timezone.utc),
    )
    if avatar_url:
        embed.set_thumbnail(url=avatar_url)
    embed.add_field(name="Applicant", value=f"{applicant.mention}\n`{applicant.id}`", inline=True)
    embed.add_field(name="Roblox", value=f"**{roblox_name}**\n`{roblox_id}`", inline=True)
    embed.add_field(name="Ticket", value=f"`{ticket_id}`", inline=True)
    embed.set_footer(text=f"Claimed by {claimed_by}" if claimed_by else "Pending staff review")
    return embed


async def send_application_review_card(guild, channel, ticket_id, applicant, app_row):
    """Post the 'Application Ready for Review' staff card into the ticket channel.

    The card lives inside the applicant's ticket only — there is no review-channel
    fallback. Returns True when the card was posted successfully, False otherwise
    (e.g. missing application data or a Discord send failure); the caller is
    expected to warn the applicant on False.
    """
    if app_row is None:
        return False
    try:
        roblox_username = app_row[0]
        roblox_id = app_row[1]
        avatar_url = await get_roblox_headshot_url(roblox_id)
        embed = build_application_review_embed(
            ticket_id,
            applicant,
            roblox_username or "Unknown",
            roblox_id or "0",
            app_row[2],
            app_row[3],
            app_row[4],
            app_row[5],
            avatar_url=avatar_url,
        )
        await channel.send(embed=embed, view=ApplicationReviewView(ticket_id))
        return True
    except Exception as exc:
        print(f"[ticket] in-ticket review card send failed for {ticket_id}: {exc}")
        return False


async def _fetch_ps99_store_gamepasses():
    """Crawl Roblox's store page for the COMPLETE PS99 gamepass list."""
    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession()

    from html import unescape

    found = {}
    timeout = aiohttp.ClientTimeout(total=15)
    for start in range(0, 300, 50):
        url = (
            "https://www.roblox.com/games/getgamepassesinnerpartial"
            f"?startIndex={start}&maxRows=50&placeId={PS99_STORE_PLACE_ID}"
        )
        try:
            async with session.get(url, timeout=timeout) as res:
                if res.status != 200:
                    break
                text = await res.text()
        except Exception:
            break
        ids = re.findall(r'href="/game-pass/(\d+)/[^"]*"', text)
        names = re.findall(r'class="text-overflow store-card-name"[^>]*>\s*([^<]+?)\s*<', text)
        if not ids:
            break
        for pid, name in zip(ids, names):
            label = unescape(name)
            label = re.sub(r"[^\w+\- ]+", " ", label)  # kills "!" + emoji soup
            label = " ".join(label.split()).strip()
            try:
                found[int(pid)] = label or name.strip()
            except ValueError:
                continue
        if len(ids) < 50:
            break
        await asyncio.sleep(0.3)
    return found


async def _get_ps99_gamepass_map():
    """Live store list if Roblox cooperates, baked copy otherwise (24h cache)."""
    if _GAMEPASS_LIST_CACHE["passes"] and time.time() - _GAMEPASS_LIST_CACHE["at"] < _GAMEPASS_LIST_TTL:
        return dict(_GAMEPASS_LIST_CACHE["passes"])
    live = await _fetch_ps99_store_gamepasses()
    if live:
        _GAMEPASS_LIST_CACHE["at"] = time.time()
        _GAMEPASS_LIST_CACHE["passes"] = dict(live)
    elif not _GAMEPASS_LIST_CACHE["passes"]:
        _GAMEPASS_LIST_CACHE["passes"] = dict(PS99_GAMEPASSES)
    return dict(_GAMEPASS_LIST_CACHE["passes"])


async def check_ps99_gamepasses(roblox_id):
    """Ownership of EVERY PS99 store gamepass — owned first, then missing/unknown."""
    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession()

    passes = await _get_ps99_gamepass_map()
    timeout = aiohttp.ClientTimeout(total=15)
    sem = asyncio.Semaphore(5)

    async def probe(pass_id, label):
        url = f"https://inventory.roblox.com/v1/users/{roblox_id}/items/GamePass/{pass_id}/is-owned"
        for _ in range(2):
            try:
                async with sem:
                    async with session.get(url, timeout=timeout) as res:
                        if res.status == 200:
                            owned = await res.json(content_type=None)
                            return {"id": pass_id, "label": label, "owned": owned is True, "unknown": False}
                        if res.status == 429:
                            await asyncio.sleep(1.5)
                            continue
                        break
            except Exception:
                await asyncio.sleep(0.5)
        return {"id": pass_id, "label": label, "owned": False, "unknown": True}

    results = await asyncio.gather(*(probe(pid, label) for pid, label in passes.items()))
    order = {pid: i for i, pid in enumerate(passes)}
    return sorted(results, key=lambda r: (not r["owned"], order.get(r["id"], 999)))


def format_gamepass_results(results):
    if not results:
        return "No gamepasses configured."
    owned = [r for r in results if r.get("owned")]
    missing = [r["label"] for r in results if not r.get("owned") and not r.get("unknown")]
    unknown = [r["label"] for r in results if r.get("unknown")]
    lines = [f"**Owns {len(owned)}/{len(results)} PS99 passes** 🎫"]
    lines += [f"✅ **{r['label']}**" for r in owned]
    if missing:
        lines.append("")
        lines.append(f"❌ Missing ({len(missing)}): {', '.join(missing)}")
    if unknown:
        lines.append(f"⚠️ Couldn't verify: {', '.join(unknown)}")
    text = "\n".join(lines)
    return text[:1021] + "…" if len(text) > 1024 else text


def _safe_int(value, default=0):
    try:
        return int(value or 0)
    except Exception:
        return default


def _friendly_battle_name(battle_id):
    text = str(battle_id or "Unknown Battle")
    return re.sub(r'(\d+)', r' \1', re.sub(r'([a-z])([A-Z])', r'\1 \2', text)).strip()


def _battle_sort_key(item):
    return int(item.get("startTime") or 0), int(item.get("battleOrder") or 0), str(item.get("battleId") or "")


async def _ps99_json(url, timeout_seconds=15):
    global session
    if session is None or getattr(session, "closed", False):
        session = aiohttp.ClientSession()
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout_seconds)) as res:
            if res.status != 200:
                return None
            return await res.json(content_type=None)
    except Exception as exc:
        print(f"[ticket war history] fetch failed {url}: {exc}")
        return None


def _extract_clan_battle_ids(clan_payload):
    data = clan_payload.get("data", {}) if isinstance(clan_payload, dict) else {}
    battles = data.get("Battles") or data.get("battles") or {}
    if isinstance(battles, dict):
        return list(battles.keys())
    return []


async def fetch_known_battle_ids_for_history():
    ids = list(KNOWN_PS99_BATTLE_IDS)
    clan_payload = await _ps99_json(CLAN_API)
    ids.extend(_extract_clan_battle_ids(clan_payload or {}))
    seen = set()
    cleaned = []
    for battle_id in ids:
        battle_id = str(battle_id or "").strip()
        if not battle_id or not re.fullmatch(r"[A-Za-z0-9_]+", battle_id):
            continue
        key = battle_id.lower()
        if key not in seen:
            seen.add(key)
            cleaned.append(battle_id)
    return cleaned[:30], clan_payload


async def fetch_top_clan_names_for_history(limit=None):
    limit = int(limit or TOP_CLAN_HISTORY_LIMIT)
    payload = await _ps99_json(f"{PS99_API}/api/clans?page=1&pageSize={limit}&sort=Points&sortOrder=desc", timeout_seconds=20)
    rows = payload.get("data", []) if isinstance(payload, dict) else []
    names = []
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            name = str(row.get("Name") or row.get("name") or "").strip().upper()
            if name and re.fullmatch(r"[A-Z0-9_]{1,8}", name):
                names.append(name)
    if CLAN_NAME.upper() not in names:
        names.append(CLAN_NAME.upper())
    seen = set()
    cleaned = []
    for name in names:
        if name not in seen:
            seen.add(name)
            cleaned.append(name)
    return cleaned[:limit] if CLAN_NAME.upper() in cleaned[:limit] else cleaned[:limit] + [CLAN_NAME.upper()]


async def fetch_legacy_clan_payload(clan_name):
    clan_name = str(clan_name or "").strip().upper()
    if not re.fullmatch(r"[A-Z0-9_]{1,8}", clan_name):
        return clan_name, None
    return clan_name, await _ps99_json(f"{PS99_API}/api/clan/{clan_name}", timeout_seconds=20)


def _legacy_clan_battles(payload):
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    battles = data.get("Battles") or data.get("battles") or {}
    return battles if isinstance(battles, dict) else {}


async def build_top_clan_history_index():
    started = time.time()
    clan_names = await fetch_top_clan_names_for_history(TOP_CLAN_HISTORY_LIMIT)
    semaphore = asyncio.Semaphore(TOP_CLAN_HISTORY_CONCURRENCY)

    async def limited_fetch(name):
        async with semaphore:
            return await fetch_legacy_clan_payload(name)

    fetched = await asyncio.gather(*(limited_fetch(name) for name in clan_names), return_exceptions=True)
    battle_entries = {}
    scanned = []
    failed = []

    for fetch_index, result in enumerate(fetched):
        if isinstance(result, Exception):
            failed.append(str(result))
            continue
        clan_name, payload = result
        battles = _legacy_clan_battles(payload or {})
        if not battles:
            failed.append(clan_name)
            continue
        scanned.append(clan_name)
        for battle_index, (battle_id, battle) in enumerate(battles.items()):
            if not isinstance(battle, dict):
                continue
            battle_id = str(battle.get("BattleID") or battle.get("battleId") or battle_id or "").strip()
            if not battle_id:
                continue
            contributions = battle.get("PointContributions") or battle.get("pointContributions") or []
            if not isinstance(contributions, list):
                continue
            bucket = battle_entries.setdefault(battle_id, {})
            clan_place = battle.get("Place") or battle.get("place")
            earned_medal = bool(battle.get("EarnedMedal") or battle.get("earnedMedal"))
            battle_order = (fetch_index * 10000) + battle_index
            for item in contributions:
                if not isinstance(item, dict):
                    continue
                user_id = str(item.get("UserID") or item.get("userId") or item.get("UserId") or "").strip()
                points = _safe_int(item.get("Points") or item.get("points"))
                if not user_id or points <= 0:
                    continue
                previous = bucket.get(user_id)
                if previous and _safe_int(previous.get("points")) >= points:
                    continue
                bucket[user_id] = {
                    "userId": user_id,
                    "battleId": battle_id,
                    "title": _friendly_battle_name(battle_id),
                    "clan": clan_name,
                    "points": points,
                    "clanPlace": clan_place,
                    "earnedMedal": earned_medal,
                    "startTime": _safe_int(battle.get("StartTime") or battle.get("startTime")),
                    "battleOrder": battle_order,
                    "source": f"Top {TOP_CLAN_HISTORY_LIMIT} clan scan",
                }

    players = {}
    battle_meta = {}
    for battle_id, by_user in battle_entries.items():
        ranked = sorted(by_user.values(), key=lambda row: _safe_int(row.get("points")), reverse=True)
        total = len(ranked)
        battle_meta[battle_id] = {"totalContributors": total}
        for rank, row in enumerate(ranked, start=1):
            user_id = str(row.get("userId") or "").strip()
            if not user_id:
                continue
            better = max(0.0, ((total - rank) / total) * 100) if total else 0.0
            player_row = dict(row)
            player_row.update({"rank": rank, "total": total, "betterThan": better})
            players.setdefault(user_id, []).append(player_row)

    for rows in players.values():
        rows.sort(key=_battle_sort_key, reverse=True)

    return {
        "builtAt": datetime.now(timezone.utc).isoformat(),
        "scanSeconds": round(time.time() - started, 2),
        "limit": TOP_CLAN_HISTORY_LIMIT,
        "clansScanned": scanned,
        "failedClans": failed[:25],
        "battleCount": len(battle_entries),
        "playerCount": len(players),
        "players": players,
        "battleMeta": battle_meta,
    }


async def get_top_clan_history_index(force=False):
    global TOP_CLAN_HISTORY_BUILD_TASK
    now_ts = time.time()
    cached = TOP_CLAN_HISTORY_CACHE.get("index")
    if not force and cached and cached.get("expires", 0) > now_ts:
        return cached.get("data") or {}

    if TOP_CLAN_HISTORY_BUILD_TASK and not TOP_CLAN_HISTORY_BUILD_TASK.done():
        return await TOP_CLAN_HISTORY_BUILD_TASK

    TOP_CLAN_HISTORY_BUILD_TASK = asyncio.create_task(build_top_clan_history_index())
    data = await TOP_CLAN_HISTORY_BUILD_TASK
    TOP_CLAN_HISTORY_CACHE["index"] = {"expires": now_ts + TOP_CLAN_HISTORY_TTL_SECONDS, "data": data}
    return data


def _mcwv_history_from_clan_payload(roblox_id, clan_payload):
    data = clan_payload.get("data", {}) if isinstance(clan_payload, dict) else {}
    battles = data.get("Battles") or data.get("battles") or {}
    rows = []
    if not isinstance(battles, dict):
        return rows

    target = str(roblox_id).strip()
    for battle_id, battle in battles.items():
        if not isinstance(battle, dict):
            continue
        contributions = battle.get("PointContributions") or battle.get("pointContributions") or []
        if not isinstance(contributions, list):
            continue
        ranked = sorted(contributions, key=lambda item: _safe_int(item.get("Points") if isinstance(item, dict) else 0), reverse=True)
        for index, item in enumerate(ranked, start=1):
            if not isinstance(item, dict):
                continue
            user_id = str(item.get("UserID") or item.get("userId") or item.get("UserId") or "").strip()
            if user_id != target:
                continue
            total = max(1, len(ranked))
            better = max(0.0, ((total - index) / total) * 100) if total else 0.0
            rows.append({
                "battleId": str(battle_id),
                "title": _friendly_battle_name(battle_id),
                "clan": "MCWV",
                "points": _safe_int(item.get("Points")),
                "rank": index,
                "total": total,
                "betterThan": better,
                "clanPlace": battle.get("Place") or battle.get("place"),
                "earnedMedal": bool(battle.get("EarnedMedal") or battle.get("earnedMedal")),
                "startTime": _safe_int(battle.get("StartTime") or battle.get("startTime")),
                "source": "MCWV exact",
            })
            break
    return rows


async def _battle_history_lookup(battle_id, roblox_id):
    payload = await _ps99_json(f"{PS99_API}/v1/clans/battles/{battle_id}")
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    if not isinstance(data, dict):
        return None
    players = data.get("topPlayers") or []
    if not isinstance(players, list):
        return None
    target = str(roblox_id).strip()
    for player in players:
        if not isinstance(player, dict):
            continue
        if str(player.get("userId") or player.get("UserID") or "").strip() != target:
            continue
        stats = data.get("stats") if isinstance(data.get("stats"), dict) else {}
        meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
        total = _safe_int(stats.get("totalContributors"), len(players)) or len(players)
        rank = _safe_int(player.get("rank"), 0)
        better = max(0.0, ((total - rank) / total) * 100) if total and rank else 0.0
        clan = player.get("clan") if isinstance(player.get("clan"), dict) else {}
        return {
            "battleId": str(battle_id),
            "title": str(meta.get("title") or _friendly_battle_name(battle_id)),
            "clan": str(clan.get("name") or "—"),
            "points": _safe_int(player.get("points")),
            "rank": rank,
            "total": total,
            "betterThan": better,
            "clanPlace": clan.get("place"),
            "startTime": _safe_int(meta.get("startTime")),
            "source": "Top-player sample",
        }
    return None


async def fetch_ps99_player_war_history(roblox_id):
    cache_key = str(roblox_id).strip()
    now_ts = time.time()
    cached = PLAYER_WAR_HISTORY_CACHE.get(cache_key)
    if cached and cached.get("expires", 0) > now_ts:
        return cached.get("data") or {}

    summary_payload, top_index = await asyncio.gather(
        _ps99_json(f"{PS99_API}/v1/clans/players/{cache_key}"),
        get_top_clan_history_index(),
        return_exceptions=True,
    )
    if isinstance(summary_payload, Exception):
        summary_payload = None
    if isinstance(top_index, Exception):
        print(f"[ticket war history] top clan index failed: {top_index}")
        top_index = {}

    summary_data = summary_payload.get("data", {}) if isinstance(summary_payload, dict) else {}
    summary = summary_data.get("player") if isinstance(summary_data, dict) else None

    player_rows = []
    players_map = top_index.get("players", {}) if isinstance(top_index, dict) else {}
    if isinstance(players_map, dict):
        player_rows = [dict(row) for row in players_map.get(cache_key, []) if isinstance(row, dict)]

    # Fallback to the public v1 battle top-player sample for any manually known IDs
    # not already covered by the top-clan legacy scan.
    existing_ids = {str(row.get("battleId")).lower() for row in player_rows}
    if len(player_rows) < 3:
        battle_ids, clan_payload = await fetch_known_battle_ids_for_history()
        for row in _mcwv_history_from_clan_payload(cache_key, clan_payload or {}):
            if str(row.get("battleId")).lower() not in existing_ids:
                player_rows.append(row)
                existing_ids.add(str(row.get("battleId")).lower())

        semaphore = asyncio.Semaphore(4)
        async def limited_lookup(battle_id):
            async with semaphore:
                return await _battle_history_lookup(battle_id, cache_key)
        lookups = await asyncio.gather(*(limited_lookup(battle_id) for battle_id in battle_ids if str(battle_id).lower() not in existing_ids), return_exceptions=True)
        for item in lookups:
            if isinstance(item, dict) and item and str(item.get("battleId")).lower() not in existing_ids:
                player_rows.append(item)
                existing_ids.add(str(item.get("battleId")).lower())

    player_rows.sort(key=_battle_sort_key, reverse=True)
    result = {
        "summary": summary if isinstance(summary, dict) else None,
        "activeBattleId": summary_data.get("activeBattleId") if isinstance(summary_data, dict) else None,
        "sampledClans": summary_data.get("sampledClans") if isinstance(summary_data, dict) else None,
        "battles": player_rows[:20],
        "scan": {
            "mode": f"Top {top_index.get('limit', TOP_CLAN_HISTORY_LIMIT)} legacy clan scan" if isinstance(top_index, dict) else "Top clan scan",
            "clansScanned": len(top_index.get("clansScanned", [])) if isinstance(top_index, dict) else 0,
            "battleCount": top_index.get("battleCount") if isinstance(top_index, dict) else None,
            "playerCount": top_index.get("playerCount") if isinstance(top_index, dict) else None,
            "scanSeconds": top_index.get("scanSeconds") if isinstance(top_index, dict) else None,
            "builtAt": top_index.get("builtAt") if isinstance(top_index, dict) else None,
        },
        "coverageNote": "Ranks are calculated from every contribution found in the scanned top-100 legacy clan records. Missing battles can still happen if a clan is outside the scan or API data is absent.",
    }
    PLAYER_WAR_HISTORY_CACHE[cache_key] = {"expires": now_ts + PLAYER_WAR_HISTORY_TTL_SECONDS, "data": result}
    return result


def format_player_war_history(history):
    summary = history.get("summary") if isinstance(history, dict) else None
    rows = history.get("battles", []) if isinstance(history, dict) else []
    rows = rows if isinstance(rows, list) else []
    scan = history.get("scan", {}) if isinstance(history, dict) and isinstance(history.get("scan"), dict) else {}

    aggregate_total = _safe_int(summary.get("TotalBattles")) if isinstance(summary, dict) else 0
    aggregate_medals = _safe_int(summary.get("EarnedMedals")) if isinstance(summary, dict) else 0
    row_total = len(rows)
    row_medals = sum(1 for row in rows if isinstance(row, dict) and row.get("earnedMedal"))
    known_total = max(aggregate_total, row_total)
    known_medals = max(aggregate_medals, row_medals)

    numeric_places = []
    better_values = []
    clans_seen = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        clan_name = str(row.get("clan") or "").strip()
        if clan_name and clan_name not in clans_seen:
            clans_seen.append(clan_name)
        try:
            place = row.get("clanPlace")
            if place not in (None, "", 0):
                numeric_places.append(float(place))
        except Exception:
            pass
        better = row.get("betterThan")
        if isinstance(better, (int, float)):
            better_values.append(float(better))

    summary_lines = [f"Known battle rows found: **{known_total}**"]
    summary_lines.append(f"Earned medals shown: **{known_medals}**")
    if scan.get("clansScanned"):
        summary_lines.append(f"Scan coverage: **{scan.get('clansScanned')} clans** • **{scan.get('battleCount') or 0} battles**")
    if clans_seen:
        summary_lines.append(f"Clans seen: **{', '.join(clans_seen[:8])}**")
    if better_values:
        summary_lines.append(f"Best percentile: **{max(better_values):.1f}%** • Avg: **{(sum(better_values) / len(better_values)):.1f}%**")

    if isinstance(summary, dict):
        clan = summary.get("Clan") if isinstance(summary.get("Clan"), dict) else {}
        avg_place = summary.get("AvgPlace")
        if avg_place is not None:
            try:
                summary_lines.append(f"Aggregate avg clan place: **{float(avg_place):.1f}**")
            except Exception:
                pass
        elif numeric_places:
            summary_lines.append(f"Avg clan place from shown rows: **{sum(numeric_places) / len(numeric_places):.1f}**")
        summary_lines.append(f"Active battle points: **{format_points(_safe_int(summary.get('ActiveBattlePoints')))}**")
        if clan.get("Name"):
            summary_lines.append(f"Aggregate sampled clan: **{clan.get('Name')}**")
        if aggregate_total == 0 and row_total > 0:
            summary_lines.append("_Aggregate says 0, but the top-clan history scan found rows below._")
    else:
        if numeric_places:
            summary_lines.append(f"Avg clan place from shown rows: **{sum(numeric_places) / len(numeric_places):.1f}**")
        summary_lines.append("No aggregate player summary found in the sampled PS99 clan data.")

    if rows:
        battle_lines = []
        for row in rows[:8]:
            if not isinstance(row, dict):
                continue
            points = format_points(_safe_int(row.get("points")))
            rank = _safe_int(row.get("rank"))
            total = _safe_int(row.get("total"))
            rank_text = f"#{rank}/{total}" if rank and total else "rank unknown"
            better = row.get("betterThan")
            better_text = f" • better than **{float(better):.1f}%**" if isinstance(better, (int, float)) else ""
            clan_place = row.get("clanPlace")
            place_text = f" • clan place **#{clan_place}**" if clan_place not in (None, "", 0) else ""
            medal_text = " • medal" if row.get("earnedMedal") else ""
            source = str(row.get("source") or "scan")
            source_text = "top100" if source.startswith("Top") else ("exact" if source == "MCWV exact" else "sampled")
            battle_lines.append(
                f"• **{_friendly_battle_name(row.get('battleId'))}** — {points} pts — **{row.get('clan') or '—'}** — {rank_text}{better_text}{place_text}{medal_text} _({source_text})_"
            )
        history_text = "\n".join(battle_lines)
    else:
        history_text = "No per-battle rows found from the top-clan history scan or PS99 sampled battle data."

    return "\n".join(summary_lines)[:1024], history_text[:1024]


async def build_staff_info_embed(ticket_row):
    ticket_id = str(ticket_row[0])
    opener_id = int(ticket_row[3])
    app = db_get_ticket_application(ticket_id)
    embed = discord.Embed(
        title="MCWV Application Staff Info",
        color=discord.Color(get_ticket_embed_color("staffInfo", 0x60A5FA)),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="Applicant", value=f"<@{opener_id}>\n`{opener_id}`", inline=True)
    embed.add_field(name="Ticket", value=f"`{ticket_id}`", inline=True)

    if not app:
        embed.description = "No submitted application answers found yet."
        return embed

    roblox_username, roblox_id, afk_247, activity, liquid_gems, why_accept, submitted_at = app
    embed.add_field(name="Roblox", value=f"**{roblox_username}**\n`{roblox_id}`", inline=True)
    embed.add_field(name="AFK 24/7 on Windows?", value=str(afk_247 or "—")[:1024], inline=False)
    embed.add_field(name="Discord + in-game active hours", value=str(activity or "—")[:1024], inline=False)
    embed.add_field(name="Liquid gems per war", value=str(liquid_gems or "—")[:1024], inline=False)
    embed.add_field(name="Why should we accept you?", value=str(why_accept or "—")[:1024], inline=False)

    gamepasses = await check_ps99_gamepasses(str(roblox_id))
    embed.add_field(
        name="Gamepass check",
        value=format_gamepass_results(gamepasses),
        inline=False,
    )

    embed.set_footer(text="Only staff can see this panel.")
    return embed


async def log_ticket_event(guild, embed):
    channel = guild.get_channel(MCWV_TICKET_LOG_CHANNEL_ID) if guild else None
    if channel:
        try:
            await channel.send(embed=embed)
        except Exception as exc:
            print("ticket log send error:", exc)


async def send_ticket_application_banner(channel, mention=None):
    try:
        if MCWV_TICKET_BANNER_PATH and os.path.exists(MCWV_TICKET_BANNER_PATH):
            filename = "clan_application_banner.png"
            embed = discord.Embed(color=discord.Color(get_ticket_embed_color("banner", 0x34D399)))
            embed.set_image(url=f"attachment://{filename}")
            await channel.send(
                content=mention or None,
                embed=embed,
                file=discord.File(MCWV_TICKET_BANNER_PATH, filename=filename),
                allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
            )
            return True
        print(f"[ticket] banner file missing: {MCWV_TICKET_BANNER_PATH}")
    except Exception as exc:
        print(f"[ticket] banner send failed in {getattr(channel, 'id', 'unknown')}: {exc}")
    return False


# ---------------- TICKET CHANNEL DELETE PROTECTION ----------------
# The ticket system must NEVER delete its own infrastructure channels. (The
# ticket-control channel was deleted once when a Close click fell back to the
# channel the button was pressed in.) These guards make that class of bug
# impossible: deletes only happen for provably-real ticket channels.
TICKET_PROTECTED_CHANNEL_IDS = {
    cid
    for cid in (
        int(MCWV_TICKET_PANEL_CHANNEL_ID or 0),
        int(MCWV_TICKET_REVIEW_CHANNEL_ID or 0),
        int(MCWV_TICKET_LOG_CHANNEL_ID or 0),
    )
    if cid
}


def is_protected_guild_channel(channel):
    cid = getattr(channel, "id", 0) or 0
    return cid in TICKET_PROTECTED_CHANNEL_IDS


def looks_like_ticket_channel(channel):
    """True only for channels that match the real ticket naming pattern."""
    if not isinstance(channel, discord.TextChannel):
        return False
    if is_protected_guild_channel(channel):
        return False
    name = (getattr(channel, "name", "") or "").lower()
    if name == "ticket-control":
        return False
    return name.startswith("ticket-") or name.startswith("⭐")


async def safe_delete_ticket_channel(channel, reason):
    """Delete a ticket channel ONLY when it provably belongs to the ticket system.

    Protected (panel/review/log) channels can never be deleted, and every other
    channel requires the ticket name pattern or a DB ticket row pointing at it.
    When in doubt: refuse. Failing safe beats deleting the wrong channel ever again.
    """
    if channel is None:
        return False
    if is_protected_guild_channel(channel):
        print(f"[ticket] REFUSED to delete protected channel {getattr(channel, 'id', '?')} ({getattr(channel, 'name', '?')})")
        return False
    if not looks_like_ticket_channel(channel):
        row = None
        try:
            row = db_get_ticket_by_channel(channel.id)
        except Exception:
            row = None
        if not row:
            print(f"[ticket] REFUSED to delete unverified channel {getattr(channel, 'id', '?')} ({getattr(channel, 'name', '?')})")
            return False
    try:
        await channel.delete(reason=reason)
        return True
    except Exception as exc:
        print(f"[ticket] channel delete failed for {getattr(channel, 'id', '?')}: {exc}")
        return False


async def resolve_ticket_channel_for_close(guild, row, interaction_channel):
    """Resolve which channel a Close should delete — or None ('touch nothing').

    The old code blindly fell back to the channel the Close button was clicked
    in; when the ticket record couldn't be loaded that channel was the staff
    review channel, and the bot deleted it. Never again.
    """
    if guild is not None and row and row[1]:
        try:
            resolved = guild.get_channel(int(row[1]))
            if resolved is None:
                resolved = await guild.fetch_channel(int(row[1]))
            if resolved is not None and not is_protected_guild_channel(resolved):
                return resolved
        except Exception:
            return None
        return None

    # No usable DB record: only allow the channel the button was pressed in when
    # it is a VERIFIED ticket channel (name pattern AND a DB row pointing at it).
    if looks_like_ticket_channel(interaction_channel):
        try:
            if db_get_ticket_by_channel(interaction_channel.id):
                return interaction_channel
        except Exception:
            return None
    return None


async def restore_application_review_messages(guild):
    """Re-send any missing 'MCWV Application Ready for Review' cards.

    Review cards live INSIDE each application ticket. For every pending ticket,
    checks the ticket channel history for its card and re-posts it (with working
    buttons) when absent. Falls back to the review channel only if the ticket
    channel is gone. Returns the number of cards re-sent.
    """
    restored = 0
    try:
        if guild is None or not db_enabled():
            return 0

        db_ensure_mcwv_ticket_tables()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT t.ticket_id, t.channel_id, t.opener_discord_id, t.claimed_by,
                       a.roblox_username, a.roblox_id, a.afk_247, a.activity, a.liquid_gems, a.why_accept
                FROM mcwv_tickets t
                JOIN mcwv_ticket_applications a ON a.ticket_id = t.ticket_id
                WHERE t.status = 'pending'
                  AND EXISTS (
                    SELECT 1 FROM mcwv_ticket_actions act
                    WHERE act.ticket_id = t.ticket_id
                      AND act.action = 'screenshots/uploaded'
                  )
                ORDER BY t.updated_at DESC
                LIMIT 25
                """
            )
            rows = cur.fetchall()

        for row in rows or []:
            ticket_id = str(row[0])
            try:
                applicant = guild.get_member(int(row[2]))
                if applicant is None:
                    try:
                        applicant = await guild.fetch_member(int(row[2]))
                    except Exception:
                        applicant = None
                if applicant is None:
                    print(f"[ticket] review restore skipped {ticket_id}: applicant no longer in server")
                    continue

                avatar_url = await get_roblox_headshot_url(row[5])
                embed = build_application_review_embed(
                    ticket_id,
                    applicant,
                    row[4] or "Unknown",
                    row[5] or "0",
                    row[6],
                    row[7],
                    row[8],
                    row[9],
                    claimed_by=row[3] or None,
                    avatar_url=avatar_url,
                )
                ticket_channel = guild.get_channel(int(row[1])) if row[1] else None
                # Review cards live inside the ticket channel only. If the ticket
                # channel is gone, there is nowhere to restore the card.
                if not isinstance(ticket_channel, discord.TextChannel):
                    continue
                # Skip when the card is already posted inside the ticket (never double-post).
                already_posted = False
                try:
                    async for message in ticket_channel.history(limit=50):
                        for existing_embed in message.embeds or []:
                            if (existing_embed.title or "") != "MCWV Application Ready for Review":
                                continue
                            for field in existing_embed.fields:
                                if str(field.name).lower() == "ticket" and str(field.value).replace("`", "").strip() == ticket_id:
                                    already_posted = True
                                    break
                        if already_posted:
                            break
                except Exception:
                    already_posted = True  # cannot verify -> safer to skip than duplicate
                if already_posted:
                    continue
                await ticket_channel.send(embed=embed, view=ApplicationReviewView(ticket_id))
                restored += 1
                await asyncio.sleep(1.0)
            except Exception as exc:
                print(f"[ticket] review restore failed for {ticket_id}: {exc}")
    except Exception as exc:
        print(f"[ticket] review restore error: {exc}")

    if restored:
        print(f"[ticket] restored {restored} application review card(s) in ticket channels")
    return restored


async def delete_ticket_control_message(guild, ticket_id=None, channel_id=None, message_id=None):
    """Delete the staff review/control message once an application is finished."""
    if not guild:
        return False

    # Fast path: delete the exact message that contained the buttons.
    if channel_id and message_id:
        try:
            channel = guild.get_channel(int(channel_id)) or await guild.fetch_channel(int(channel_id))
            if isinstance(channel, discord.TextChannel):
                message = await channel.fetch_message(int(message_id))
                await message.delete()
                return True
        except discord.NotFound:
            return True
        except Exception as exc:
            print(f"ticket control exact delete failed: {exc}")

    # Fallback: search the configured review channel by Ticket field. This also
    # lets Hub/admin accept-close actions clean up old control messages.
    if ticket_id:
        try:
            review_channel = guild.get_channel(MCWV_TICKET_REVIEW_CHANNEL_ID) or await guild.fetch_channel(MCWV_TICKET_REVIEW_CHANNEL_ID)
            if not isinstance(review_channel, discord.TextChannel):
                return False
            wanted = str(ticket_id).strip()
            async for message in review_channel.history(limit=200):
                for embed in message.embeds:
                    if (embed.title or "") != "MCWV Application Ready for Review":
                        continue
                    for field in embed.fields:
                        if str(field.name).lower() == "ticket" and str(field.value).replace("`", "").strip() == wanted:
                            await message.delete()
                            return True
        except discord.NotFound:
            return True
        except Exception as exc:
            print(f"ticket control fallback delete failed for {ticket_id}: {exc}")

    return False


async def build_ticket_transcript(channel, limit=250):
    lines = []
    try:
        async for msg in channel.history(limit=limit, oldest_first=True):
            stamp = msg.created_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            content = msg.content or ""
            if msg.embeds:
                content += " " + " ".join(f"[embed: {e.title or 'no title'}]" for e in msg.embeds)
            if msg.attachments:
                content += " " + " ".join(a.url for a in msg.attachments)
            lines.append(f"[{stamp}] {msg.author} ({msg.author.id}): {content}".strip())
    except Exception as exc:
        lines.append(f"Transcript failed: {exc}")
    return "\n".join(lines)[-150000:]


async def accept_application_ticket(interaction, ticket_row):
    # Defer FIRST. This function does several seconds of DB/Discord API work
    # (link Roblox, add role, rename + move channel, send embeds, DM). Without
    # deferring, the final interaction.response.send_message blows past Discord's
    # interaction window and raises 404 Unknown interaction (10062). Guarded so a
    # caller that already deferred (or responded) does not double-acknowledge.
    if not interaction.response.is_done():
        try:
            await interaction.response.defer(ephemeral=True)
        except Exception:
            pass

    guild = interaction.guild
    channel = None
    if guild and ticket_row and ticket_row[1]:
        channel = guild.get_channel(int(ticket_row[1]))
        if channel is None:
            try:
                channel = await guild.fetch_channel(int(ticket_row[1]))
            except Exception:
                channel = None
    applicant_id = int(ticket_row[3])
    applicant = guild.get_member(applicant_id) if guild else None
    if applicant is None and guild:
        try:
            applicant = await guild.fetch_member(applicant_id)
        except Exception:
            applicant = None
    if applicant is None:
        if interaction.response.is_done():
            await interaction.followup.send("❌ Applicant is no longer in the server.", ephemeral=True)
        else:
            await interaction.response.send_message("❌ Applicant is no longer in the server.", ephemeral=True)
        return False

    app = db_get_ticket_application(ticket_row[0])
    if not app:
        if interaction.response.is_done():
            await interaction.followup.send("❌ No submitted application found yet.", ephemeral=True)
        else:
            await interaction.response.send_message("❌ No submitted application found yet.", ephemeral=True)
        return False
    roblox_name, roblox_id = str(app[0]), str(app[1])

    actions = []
    errors = []

    ok, db_msg = db_add(roblox_id, applicant.id, roblox_name)
    if ok:
        actions.append("Roblox account linked")
    else:
        errors.append(f"Could not link Roblox account: {db_msg}")

    if ok and channel:
        if db_set_ticket_channel(applicant.id, channel.id):
            actions.append("Ticket saved for broadcasts")
        else:
            errors.append("Could not save ticket channel for broadcasts")

    role = guild.get_role(MCWV_TICKET_MEMBER_ROLE_ID) if guild else None
    if role and ok:
        try:
            await applicant.add_roles(role, reason=f"MCWV application accepted by {interaction.user}")
            actions.append(f"Member role added: {role.name}")
        except Exception as exc:
            errors.append(f"Could not assign member role: {exc}")
    elif ok:
        errors.append("Member role not found")

    if ok and channel:
        try:
            safe_name = normalize_ticket_key(roblox_name)[:24] or str(applicant.id)
            await channel.edit(name=f"⭐-ticket-{safe_name}", reason="MCWV application accepted")
            actions.append("Ticket renamed as accepted")
        except Exception as exc:
            errors.append(f"Could not rename ticket: {exc}")

        try:
            accepted_category = get_available_category(guild)
            if accepted_category:
                await channel.edit(
                    category=accepted_category,
                    sync_permissions=False,
                    reason="MCWV application accepted — moved to member ticket category",
                )
                actions.append(f"Ticket moved to {accepted_category.name}")
            else:
                errors.append("Accepted member ticket categories are full or unavailable")
        except Exception as exc:
            errors.append(f"Could not move ticket to accepted category: {exc}")

    if ok:
        db_update_ticket_status(
            ticket_row[0],
            "accepted",
            interaction.user.id,
            accepted_at=datetime.now(timezone.utc),
            accepted_by=interaction.user.id,
        )
        actions.append("Ticket marked accepted")
        if MCWV_HUB_LINKS_ENABLED:
            actions.append("Website signup is ready")

    status_embed = discord.Embed(
        title="Application Accepted" if ok else "Application Accept Failed",
        description=(
            f"Applicant: {applicant.mention}\n"
            f"Roblox: **{roblox_name}** (`{roblox_id}`)\n"
            f"Accepted by: {interaction.user.mention}"
        ),
        color=discord.Color(get_ticket_embed_color("accepted", 0x22C55E)) if ok else discord.Color.red(),
        timestamp=datetime.now(timezone.utc),
    )
    status_embed.add_field(
        name="Completed",
        value="\n".join(f"✅ {item}" for item in actions) or "—",
        inline=False,
    )
    if errors:
        status_embed.add_field(
            name="Needs attention",
            value="\n".join(f"⚠️ {item}" for item in errors)[:1024],
            inline=False,
        )
    if MCWV_HUB_LINKS_ENABLED:
        status_embed.add_field(
            name="Hub Profile",
            value=f"https://mcwv-hub.vercel.app/profile/{roblox_id}",
            inline=False,
        )
        status_embed.add_field(
            name="Hub Signup",
            value="https://mcwv-hub.vercel.app/signup",
            inline=False,
        )
    status_embed.set_footer(text="Ticket stays open for next steps")

    if channel:
        await channel.send(embed=status_embed)
    await log_ticket_event(guild, status_embed)

    if ok:
        try:
            if MCWV_HUB_LINKS_ENABLED:
                dm_description = (
                    f"You have been accepted into **MCWV**, {applicant.mention}!\n\n"
                    f"Your Roblox account has been linked as **{roblox_name}**.\n"
                    "You can now create your MCWV Hub login using the link below."
                )
            else:
                dm_description = (
                    f"You have been accepted into **MCWV**, {applicant.mention}!\n\n"
                    f"Your Roblox account has been linked as **{roblox_name}**.\n"
                    "Staff will continue next steps with you in your ticket."
                )

            dm_embed = discord.Embed(
                title="Welcome to MCWV!",
                description=dm_description,
                color=discord.Color.green(),
                timestamp=datetime.now(timezone.utc),
            )
            if MCWV_HUB_LINKS_ENABLED:
                dm_embed.add_field(name="Create your Hub account", value="https://mcwv-hub.vercel.app/signup", inline=False)
            dm_embed.set_footer(text="Your ticket will stay open for next steps.")
            await applicant.send(embed=dm_embed)
            actions.append("Applicant DM sent")
        except Exception:
            if channel:
                await channel.send("⚠️ I could not DM the applicant. They may have DMs disabled.")

    if interaction.response.is_done():
        await interaction.followup.send(
            "✅ Applicant accepted and saved." if ok else "❌ Accept failed. Check the ticket for details.",
            ephemeral=True,
        )
    else:
        await interaction.response.send_message(
            "✅ Applicant accepted and saved." if ok else "❌ Accept failed. Check the ticket for details.",
            ephemeral=True,
        )

    return bool(ok)


SCREENSHOT_IMAGE_EXTENSIONS = (".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp", ".heic", ".heif")


def is_image_attachment(attachment):
    content_type = str(getattr(attachment, "content_type", "") or "").lower()
    filename = str(getattr(attachment, "filename", "") or "").lower()
    return content_type.startswith("image/") or filename.endswith(SCREENSHOT_IMAGE_EXTENSIONS)


async def count_ticket_screenshot_attachments(channel, applicant_id, limit=250):
    if channel is None or not hasattr(channel, "history"):
        return 0

    count = 0
    try:
        async for message in channel.history(limit=limit, oldest_first=False):
            if getattr(message.author, "id", None) != int(applicant_id):
                continue
            for attachment in getattr(message, "attachments", []) or []:
                if is_image_attachment(attachment):
                    count += 1
    except Exception as exc:
        print(f"[ticket screenshots] history scan failed in {getattr(channel, 'id', 'unknown')}: {exc}")
        return 0

    return count




def find_ticket_in_channel(channel):
    """Look up a ticket by channel_id, with fallback to opener from channel topic.
    The topic format is: mcwv-ticket-owner:DISCORD_ID"""
    row = db_get_ticket_by_channel(channel.id)
    if row:
        return row

    if not channel.topic:
        return None

    topic = channel.topic

    # Try app-XXXXX in topic (older format)
    ticket_match = re.search(r'app-\d+', topic)
    if ticket_match:
        row = db_get_ticket_by_ticket_id(ticket_match.group(0))
        if row:
            return row

    # Try mcwv-ticket-owner:DISCORD_ID (current format)
    owner_match = re.search(r'mcwv-ticket-owner:(\d+)', topic)
    if owner_match:
        opener_id = int(owner_match.group(1))
        row = db_get_ticket_by_opener(opener_id, channel.id)
        if row:
            return row

    # Last resort: any large number in topic (Discord snowflake ID)
    id_matches = re.findall(r'(\d{15,20})', topic)
    for id_str in id_matches:
        try:
            opener_id = int(id_str)
            row = db_get_ticket_by_opener(opener_id)
            if row:
                return row
        except Exception:
            continue

    return None

class ScreenshotUploadedView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Screenshots uploaded", style=discord.ButtonStyle.primary, custom_id="mcwv_ticket_screenshots_uploaded")
    async def uploaded_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Defer FIRST: attachment counting + DB work can exceed Discord's 3s
        # interaction window, which caused "application did not respond".
        await interaction.response.defer(ephemeral=True)
        try:
            row = find_ticket_in_channel(interaction.channel)
            if not row:
                return await interaction.followup.send("❌ Ticket record not found.", ephemeral=True)
            if interaction.user.id != int(row[3]):
                return await interaction.followup.send("Only the applicant can confirm screenshots for this ticket.", ephemeral=True)

            screenshot_count = await count_ticket_screenshot_attachments(interaction.channel, interaction.user.id)
            if screenshot_count < MCWV_TICKET_MIN_SCREENSHOT_ATTACHMENTS:
                db_ticket_log(
                    row[0],
                    interaction.user.id,
                    "screenshots/missing",
                    f"Applicant tried to confirm screenshots before uploading enough image attachments ({screenshot_count}/{MCWV_TICKET_MIN_SCREENSHOT_ATTACHMENTS})",
                )
                return await interaction.followup.send(
                    "❌ Please upload your screenshot images in this ticket before pressing this button. "
                    f"I found **{screenshot_count}** image attachment(s); required: **{MCWV_TICKET_MIN_SCREENSHOT_ATTACHMENTS}**.",
                    ephemeral=True,
                )

            staff_mentions = " ".join(
                f"<@&{role_id}>"
                for role_id in sorted(MCWV_TICKET_STAFF_ROLE_IDS)
                if role_id != 1502339420207059066
            )
            db_ticket_log(row[0], interaction.user.id, "screenshots/uploaded", "Applicant confirmed screenshots were uploaded")

            for child in self.children:
                child.disabled = True

            await interaction.edit_original_response(content="✅ Screenshots confirmed. Staff have been notified.", view=self)
            await interaction.channel.send(
                f"✅ Thanks {interaction.user.mention}! {staff_mentions} will review your application soon.",
                allowed_mentions=discord.AllowedMentions(users=True, roles=True, everyone=False),
            )

            # Screenshots are confirmed, so the application is now ready for staff
            # review — post the staff review card (Accept / Staff Info / Close) into
            # the ticket channel. It lives here only (no review-channel fallback).
            try:
                app_row = await asyncio.to_thread(db_get_ticket_application, row[0])
                applicant = interaction.guild.get_member(interaction.user.id) if interaction.guild else None
                if applicant is None:
                    applicant = interaction.user
                posted = await send_application_review_card(
                    interaction.guild,
                    interaction.channel,
                    row[0],
                    applicant,
                    app_row,
                )
                if not posted:
                    await interaction.channel.send(
                        "⚠️ Could not post the staff review card here. Staff can still review this application via the Hub dashboard."
                    )
            except Exception as review_error:
                print(f"[ticket] review card post after screenshots failed for {row[0]}: {review_error}")
        except Exception as exc:
            print(f"[ticket] uploaded_btn error: {exc}")
            traceback.print_exc()
            try:
                await interaction.followup.send("⚠️ Something went wrong confirming your screenshots. Please try again.", ephemeral=True)
            except Exception:
                pass


class ApplicationModal(discord.ui.Modal):
    def __init__(self, opener):
        super().__init__(title="MCWV Application")
        self.opener = opener
        self.settings = get_mcwv_ticket_settings()
        self.inputs_by_key = {}

        for question in self.settings.get("questions", DEFAULT_MCWV_TICKET_SETTINGS["questions"]):
            style = discord.TextStyle.paragraph if question.get("style") == "paragraph" else discord.TextStyle.short
            text_input = discord.ui.TextInput(
                label=str(question.get("label") or "Question")[:45],
                placeholder=str(question.get("placeholder") or "")[:100],
                style=style,
                required=bool(question.get("required", True)),
                max_length=int(question.get("maxLength") or 500),
            )
            self.inputs_by_key[str(question.get("key"))] = text_input
            self.add_item(text_input)

    def answer(self, key, default=""):
        item = self.inputs_by_key.get(key)
        return str(getattr(item, "value", default) or default)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            await self._do_submit(interaction)
        except Exception as exc:
            print(f"[ticket] application submit error: {exc}")
            traceback.print_exc()
            try:
                await interaction.followup.send(
                    "❌ Something went wrong while creating your ticket. Please try again — if it keeps happening, ping staff.",
                    ephemeral=True,
                )
            except Exception:
                pass

    async def _do_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        if not guild:
            return await interaction.followup.send("❌ This must be used in the server.", ephemeral=True)

        blacklist_entry = await asyncio.to_thread(db_ticket_blacklist_get, interaction.user.id)
        if blacklist_entry:
            reason = str(blacklist_entry[1] or "No reason provided")[:500]
            return await interaction.followup.send(
                f"❌ You are currently blocked from opening MCWV application tickets. Reason: {reason}",
                ephemeral=True,
            )

        existing = discord.utils.get(guild.text_channels, topic=f"mcwv-ticket-owner:{interaction.user.id}")
        if existing:
            return await interaction.followup.send(f"You already have an open application: {existing.mention}", ephemeral=True)

        roblox_input = self.answer("roblox_username").strip()
        resolved = await resolve_roblox_username_basic(roblox_input)
        if not resolved:
            return await interaction.followup.send("❌ Roblox username not found. Please check spelling and try again.", ephemeral=True)

        category = guild.get_channel(MCWV_TICKET_CATEGORY_ID)
        if not isinstance(category, discord.CategoryChannel):
            return await interaction.followup.send("❌ Ticket category is not configured correctly. Please contact staff.", ephemeral=True)

        bot_member = guild.me or (guild.get_member(bot.user.id) if bot.user else None)
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            interaction.user: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, attach_files=True, embed_links=True),
        }
        if bot_member:
            overwrites[bot_member] = discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True, read_message_history=True, attach_files=True, embed_links=True)
        for role_id in MCWV_TICKET_STAFF_ROLE_IDS:
            role = guild.get_role(role_id)
            if role:
                overwrites[role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, attach_files=True, embed_links=True, manage_messages=True)

        safe_name = normalize_ticket_key(resolved["name"] or interaction.user.display_name or interaction.user.name)[:24] or str(interaction.user.id)
        channel = await guild.create_text_channel(
            name=f"ticket-{safe_name}",
            category=category,
            topic=f"mcwv-ticket-owner:{interaction.user.id}",
            overwrites=overwrites,
            reason=f"MCWV application opened by {interaction.user}",
        )
        if bot_member:
            try:
                await channel.set_permissions(
                    bot_member,
                    view_channel=True,
                    send_messages=True,
                    manage_channels=True,
                    read_message_history=True,
                    attach_files=True,
                    embed_links=True,
                )
            except Exception as perm_error:
                print(f"[ticket] could not reinforce bot permissions: {perm_error}")

        ticket_id = f"app-{channel.id}"
        db_create_mcwv_ticket(ticket_id, channel.id, guild.id, interaction.user.id)

        afk_247 = self.answer("afk_247")
        activity = self.answer("activity")
        liquid_gems = self.answer("liquid_gems")
        why_accept = self.answer("why_accept")

        saved = db_save_ticket_application(
            ticket_id,
            resolved["name"],
            resolved["id"],
            afk_247,
            activity,
            liquid_gems,
            why_accept,
        )
        if not saved:
            return await interaction.followup.send("❌ Ticket created, but I could not save the application. Please contact staff.", ephemeral=True)

        db_ticket_log(ticket_id, interaction.user.id, "ticket/opened", "Application ticket opened", {"robloxId": resolved["id"]})
        db_ticket_log(ticket_id, interaction.user.id, "application/submitted", f"Application submitted for {resolved['name']}", {"robloxId": resolved["id"]})

        messages = self.settings.get("messages", DEFAULT_MCWV_TICKET_SETTINGS["messages"])
        welcome_description = str(messages.get("welcomeDescription") or DEFAULT_MCWV_TICKET_SETTINGS["messages"]["welcomeDescription"])
        # The upload confirmation prompt/button is sent separately below, so remove
        # any old saved copy of this line from the screenshot instructions embed.
        welcome_description = re.sub(
            r"\n*After uploading them in this ticket,\s*press\s*\*\*Screenshots uploaded\*\*\s*below\.?\s*",
            "",
            welcome_description,
            flags=re.IGNORECASE,
        ).strip()

        screenshot_embed = discord.Embed(
            title=str(messages.get("welcomeTitle") or DEFAULT_MCWV_TICKET_SETTINGS["messages"]["welcomeTitle"]),
            description=welcome_description,
            color=discord.Color(get_ticket_embed_color("ticketInstructions", 0x34D399)),
            timestamp=datetime.now(timezone.utc),
        )
        screenshot_embed.set_footer(text="MCWV Applications")

        # Banner is best-effort only. If Discord/file upload blocks or fails,
        # do not let it stop the actual ticket instructions from sending.
        banner_sent = False
        try:
            banner_sent = await asyncio.wait_for(
                send_ticket_application_banner(channel, interaction.user.mention),
                timeout=8,
            )
        except Exception as banner_error:
            print(f"[ticket] banner skipped in {channel.id}: {banner_error}")

        try:
            await channel.send(
                content=None if banner_sent else interaction.user.mention,
                embed=screenshot_embed,
                allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
            )
        except Exception as send_error:
            print(f"[ticket] screenshot embed send failed in {channel.id}: {send_error}")
            try:
                await channel.send(
                    f"{interaction.user.mention} Thank you for applying for MCWV! Please upload your non-cropped screenshots listed in the application instructions."
                )
            except Exception as fallback_error:
                print(f"[ticket] fallback send failed in {channel.id}: {fallback_error}")
                return await interaction.followup.send(
                    f"⚠️ Ticket was created ({channel.mention}) but I cannot send messages there. Please check my channel permissions.",
                    ephemeral=True,
                )

        try:
            await channel.send(
                "Once every required screenshot is uploaded, press the button below so staff know your application is ready for review.",
                view=ScreenshotUploadedView(),
            )
        except Exception as button_error:
            print(f"[ticket] screenshot confirmation button failed in {channel.id}: {button_error}")
            await channel.send("When your screenshots are uploaded, please tell staff: `Screenshots uploaded`.")

        # The staff review card ("Application Ready for Review") is NOT posted at
        # ticket open. It is posted once the applicant confirms their screenshots
        # are uploaded (ScreenshotUploadedView), because that is when the
        # application is actually ready for staff review.

        await interaction.followup.send(f"✅ Application ticket created: {channel.mention}", ephemeral=True)


class TicketWelcomeView(discord.ui.View):
    def __init__(self, ticket_id):
        super().__init__(timeout=None)
        self.ticket_id = ticket_id

    @discord.ui.button(label="Submit Application", style=discord.ButtonStyle.success, custom_id="mcwv_ticket_submit_application")
    async def submit_application(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            row = find_ticket_in_channel(interaction.channel)
            if not row:
                return await interaction.response.send_message("❌ Ticket record not found.", ephemeral=True)
            if interaction.user.id != int(row[3]):
                return await interaction.response.send_message("Only the ticket opener can submit this application.", ephemeral=True)
            await interaction.response.send_modal(ApplicationModal(interaction.user))
        except discord.HTTPException as http_exc:
            print(f"[ticket] submit_application modal failed: {http_exc}")
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message("❌ Could not open the application form. Please try again.", ephemeral=True)
            except Exception:
                pass
        except Exception as exc:
            print(f"[ticket] submit_application error: {exc}")
            traceback.print_exc()
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message("❌ Something went wrong. Please try again.", ephemeral=True)
                else:
                    await interaction.followup.send("❌ Something went wrong. Please try again.", ephemeral=True)
            except Exception:
                pass


def transcript_file(transcript_text, ticket_id):
    data = BytesIO(str(transcript_text or "No transcript available.").encode("utf-8", errors="replace"))
    safe_id = normalize_ticket_key(ticket_id) or "ticket"
    return discord.File(data, filename=f"mcwv-ticket-{safe_id}-transcript.txt")


def ticket_closed_embed(ticket_id, opener_id, closer_id, opened_at, reason, for_user=False):
    embed = discord.Embed(
        title="Ticket Closed",
        color=discord.Color(get_ticket_embed_color("closed", 0x22C55E)) if not for_user else discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="🎫 Ticket ID", value=str(ticket_id), inline=True)
    embed.add_field(name="✅ Opened By", value=f"<@{opener_id}>" if opener_id else "—", inline=True)
    embed.add_field(name="🔒 Closed By", value=f"<@{closer_id}>" if closer_id else "—", inline=True)
    embed.add_field(name="🕒 Open Time", value=opened_at.strftime("%d %b %Y at %H:%M") if hasattr(opened_at, "strftime") else "—", inline=True)
    embed.add_field(name="❔ Reason", value=str(reason or "No reason specified")[:1024], inline=False)
    embed.set_footer(text="Transcript attached" if for_user else "MCWV Ticket Logs • Transcript attached")
    return embed


async def send_ticket_close_outputs(guild, channel, ticket_id, opener_id, closer_id, opened_at, reason, transcript):
    log_channel = guild.get_channel(MCWV_TICKET_LOG_CHANNEL_ID) if guild else None
    if log_channel:
        try:
            await log_channel.send(
                embed=ticket_closed_embed(ticket_id, opener_id, closer_id, opened_at, reason, for_user=False),
                file=transcript_file(transcript, ticket_id),
                allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
            )
        except Exception as exc:
            print("ticket transcript log send error:", exc)

    if opener_id:
        try:
            user = guild.get_member(int(opener_id)) if guild else None
            if user is None:
                user = await bot.fetch_user(int(opener_id))
            await user.send(
                embed=ticket_closed_embed(ticket_id, opener_id, closer_id, opened_at, reason, for_user=True),
                file=transcript_file(transcript, ticket_id),
                allowed_mentions=discord.AllowedMentions.none(),
            )
        except Exception as exc:
            print("ticket transcript DM send error:", exc)

    if channel:
        try:
            await channel.send(embed=ticket_closed_embed(ticket_id, opener_id, closer_id, opened_at, reason, for_user=False))
        except Exception:
            pass


async def prepare_ticket_close(guild, ticket_id, actor_id, reason, interaction_channel, final_status="closed", extra_fields=None, control_channel_id=None, control_message_id=None):
    """Shared close pipeline: resolve channel (safety-guarded) -> transcript -> DB status
    -> log-channel/DM/in-channel outputs -> control-card cleanup.

    Returns the resolved ticket channel, or None when the safety lock cannot prove
    which channel belongs to the ticket (callers must NOT delete anything then).
    """
    row = db_get_ticket_by_ticket_id(ticket_id)
    if not row and interaction_channel is not None and looks_like_ticket_channel(interaction_channel):
        row = db_get_ticket_by_channel(interaction_channel.id)
    opener_id = int(row[3]) if row else None
    opened_at = row[8] if row and len(row) > 8 else None

    # SAFETY: resolve the channel to delete. If we cannot prove which
    # channel belongs to this ticket, we touch nothing (this guard exists
    # because the old fallback once deleted the ticket-control channel).
    ticket_channel = await resolve_ticket_channel_for_close(guild, row, interaction_channel)

    transcript = await build_ticket_transcript(ticket_channel) if ticket_channel else "Channel unavailable."
    db_save_ticket_transcript(ticket_id, ticket_channel.id if ticket_channel else 0, transcript)
    fields = {"closed_at": datetime.now(timezone.utc), "closed_by": actor_id, "close_reason": str(reason)}
    if extra_fields:
        fields.update(extra_fields)
    db_update_ticket_status(ticket_id, final_status, actor_id, **fields)
    await send_ticket_close_outputs(guild, ticket_channel, ticket_id, opener_id, actor_id, opened_at, str(reason), transcript)
    await delete_ticket_control_message(
        guild,
        ticket_id=ticket_id,
        channel_id=control_channel_id,
        message_id=control_message_id,
    )
    return ticket_channel


async def finalize_ticket_close(ticket_channel, actor_label, reason, ticket_id):
    """Delete the ticket channel after the configured delay (Close modal and /reject share this)."""
    await asyncio.sleep(MCWV_TICKET_DELETE_DELAY_SECONDS)
    deleted = await safe_delete_ticket_channel(ticket_channel, reason=f"MCWV ticket closed by {actor_label}: {reason}")
    if not deleted:
        print(f"[ticket] close of {ticket_id}: channel auto-delete blocked by protection; channel left in place")


class CloseTicketModal(discord.ui.Modal, title="Close Ticket"):
    reason = discord.ui.TextInput(label="Close reason", style=discord.TextStyle.paragraph, max_length=900)
    def __init__(self, ticket_id, control_channel_id=None, control_message_id=None):
        super().__init__()
        self.ticket_id = ticket_id
        self.control_channel_id = control_channel_id
        self.control_message_id = control_message_id
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        ticket_channel = await prepare_ticket_close(
            interaction.guild,
            self.ticket_id,
            interaction.user.id,
            str(self.reason.value),
            interaction.channel,
            final_status="closed",
            control_channel_id=self.control_channel_id,
            control_message_id=self.control_message_id,
        )

        if ticket_channel is None:
            await interaction.followup.send(
                "⚠️ Ticket marked as closed and transcript saved, but I could not verify the ticket's channel, "
                "so **no channel was deleted** (safety lock). Please delete the channel manually if it still exists.",
                ephemeral=True,
            )
            return

        await interaction.followup.send("✅ Transcript saved and sent, control message removed. Deleting the ticket channel shortly.", ephemeral=True)
        await finalize_ticket_close(ticket_channel, str(interaction.user), str(self.reason.value), self.ticket_id)


class AcceptConfirmView(discord.ui.View):
    def __init__(self, ticket_id, requester_id, control_channel_id=None, control_message_id=None):
        super().__init__(timeout=60)
        self.ticket_id = ticket_id
        self.requester_id = int(requester_id)
        self.control_channel_id = control_channel_id
        self.control_message_id = control_message_id

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("Only the officer who clicked Accept can confirm this.", ephemeral=True)
            return False
        if not has_mcwv_ticket_staff_permission(interaction.user):
            await interaction.response.send_message("❌ Staff only.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Yes, accept applicant", style=discord.ButtonStyle.success)
    async def confirm_accept(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Defer first: DB lookups + full accept pipeline can exceed 3 seconds.
        if not interaction.response.is_done():
            try:
                await interaction.response.defer(ephemeral=True)
            except Exception:
                pass
        try:
            row = db_get_ticket_by_ticket_id(self.ticket_id) or find_ticket_in_channel(interaction.channel)
            if not row:
                return await interaction.followup.send("❌ Ticket record not found.", ephemeral=True)
            if str(row[0]) != str(self.ticket_id):
                return await interaction.followup.send("❌ This confirmation does not match this ticket.", ephemeral=True)

            for child in self.children:
                child.disabled = True
            try:
                await interaction.message.edit(view=self)
            except Exception:
                pass

            accepted = await accept_application_ticket(interaction, row)
            if accepted:
                await delete_ticket_control_message(
                    interaction.guild,
                    ticket_id=self.ticket_id,
                    channel_id=self.control_channel_id,
                    message_id=self.control_message_id,
                )
            self.stop()
        except Exception as exc:
            print(f"[ticket] confirm_accept error: {exc}")
            traceback.print_exc()
            try:
                await interaction.followup.send("⚠️ Something went wrong while accepting. Please try again.", ephemeral=True)
            except Exception:
                pass

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_accept(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(content="Accept cancelled.", view=self)
        self.stop()


class StaffInfoView(discord.ui.View):
    def __init__(self, ticket_id, applicant_id):
        super().__init__(timeout=10 * 60)
        self.ticket_id = str(ticket_id)
        self.applicant_id = int(applicant_id)

    async def interaction_check(self, interaction: discord.Interaction):
        if not has_mcwv_ticket_staff_permission(interaction.user):
            await interaction.response.send_message("❌ Staff only.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Blacklist applicant", style=discord.ButtonStyle.danger)
    async def blacklist_applicant(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if not guild:
            return await interaction.response.send_message("❌ This must be used in the server.", ephemeral=True)

        role = guild.get_role(MCWV_TICKET_BLACKLIST_ROLE_ID)
        if not role:
            return await interaction.response.send_message("❌ Blacklist role not found. Check MCWV_TICKET_BLACKLIST_ROLE_ID.", ephemeral=True)

        member = guild.get_member(self.applicant_id)
        if member is None:
            try:
                member = await guild.fetch_member(self.applicant_id)
            except Exception:
                member = None

        if member is None:
            return await interaction.response.send_message("❌ Applicant is no longer in the server.", ephemeral=True)

        try:
            await member.add_roles(role, reason=f"Ticket blacklist by {interaction.user}")
            db_ticket_blacklist_add(member.id, f"Blacklisted from Staff Info by {interaction.user}", interaction.user.id)
            db_ticket_log(self.ticket_id, interaction.user.id, "ticket/blacklist", f"Blacklisted {member} from opening application tickets", {"roleId": str(role.id)})
            await interaction.response.send_message(f"✅ {member.mention} has been given **{role.name}** and cannot open application tickets.", ephemeral=True)
            try:
                await interaction.channel.send(f"🚫 {member.mention} has been blacklisted from opening MCWV application tickets by {interaction.user.mention}.")
            except Exception:
                pass
        except Exception as exc:
            await interaction.response.send_message(f"❌ Failed to add blacklist role: `{exc}`", ephemeral=True)


class CloseConfirmView(discord.ui.View):
    def __init__(self, ticket_id, requester_id, control_channel_id=None, control_message_id=None):
        super().__init__(timeout=60)
        self.ticket_id = str(ticket_id)
        self.requester_id = int(requester_id)
        self.control_channel_id = control_channel_id
        self.control_message_id = control_message_id

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("Only the officer who clicked Close can confirm this.", ephemeral=True)
            return False
        if not has_mcwv_ticket_staff_permission(interaction.user):
            await interaction.response.send_message("❌ Staff only.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Continue to close", style=discord.ButtonStyle.danger)
    async def confirm_close(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            row = db_get_ticket_by_ticket_id(self.ticket_id) or find_ticket_in_channel(interaction.channel)
            if not row:
                return await interaction.response.send_message("❌ Ticket record not found.", ephemeral=True)
            if str(row[0]) != self.ticket_id:
                return await interaction.response.send_message("❌ This confirmation does not match this ticket.", ephemeral=True)
            await interaction.response.send_modal(CloseTicketModal(self.ticket_id, self.control_channel_id, self.control_message_id))
            self.stop()
        except discord.HTTPException as http_exc:
            print(f"[ticket] confirm_close modal failed: {http_exc}")
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message("❌ Could not open the close form. Please try again.", ephemeral=True)
            except Exception:
                pass
        except Exception as exc:
            print(f"[ticket] confirm_close error: {exc}")
            traceback.print_exc()
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message("❌ Something went wrong. Please try again.", ephemeral=True)
                else:
                    await interaction.followup.send("❌ Something went wrong. Please try again.", ephemeral=True)
            except Exception:
                pass

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_close(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(content="Close cancelled.", embed=None, view=self)
        self.stop()


class ApplicationReviewView(discord.ui.View):
    def __init__(self, ticket_id):
        super().__init__(timeout=None)
        self.ticket_id = ticket_id

    def staff_ok(self, interaction):
        return has_mcwv_ticket_staff_permission(interaction.user)

    def resolved_ticket_id(self, interaction):
        if self.ticket_id and self.ticket_id != "persistent":
            return self.ticket_id
        try:
            for field in interaction.message.embeds[0].fields:
                if str(field.name).lower() == "ticket":
                    return str(field.value).replace("`", "").strip()
        except Exception:
            pass
        # Try channel topic
        try:
            topic = interaction.channel.topic or ""
            import re as _re
            m = _re.search(r'app-\d+', topic)
            if m:
                return m.group(0)
        except Exception:
            pass
        return self.ticket_id

    @discord.ui.button(label="Accept", style=discord.ButtonStyle.success, custom_id="mcwv_ticket_accept")
    async def accept_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.staff_ok(interaction):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        ticket_id = self.resolved_ticket_id(interaction)
        row = db_get_ticket_by_ticket_id(ticket_id) or find_ticket_in_channel(interaction.channel)
        if not row:
            return await interaction.response.send_message("❌ Ticket record not found.", ephemeral=True)
        if str(row[6] or "").lower() == "accepted":
            return await interaction.response.send_message("This application is already accepted.", ephemeral=True)

        await interaction.response.send_message(
            (
                "⚠️ **Are you sure you want to accept this applicant?**\n\n"
                "Before confirming, make sure you have checked:\n"
                "• Their full non-cropped screenshots\n"
                "• Their application answers via **Staff Info**\n"
                "• Their Roblox profile/history and requirements\n\n"
                "Confirming will link their Roblox account, save this ticket for broadcasts, and give the member role."
            ),
            view=AcceptConfirmView(row[0], interaction.user.id, interaction.channel.id, interaction.message.id if interaction.message else None),
            ephemeral=True,
        )

    @discord.ui.button(label="Staff Info", style=discord.ButtonStyle.primary, custom_id="mcwv_ticket_staff_info")
    async def staff_info_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.staff_ok(interaction):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        ticket_id = self.resolved_ticket_id(interaction)
        row = db_get_ticket_by_ticket_id(ticket_id) or find_ticket_in_channel(interaction.channel)
        if not row:
            return await interaction.response.send_message("❌ Ticket record not found.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)
        embed = await build_staff_info_embed(row)
        await interaction.followup.send(embed=embed, view=StaffInfoView(row[0], row[3]), ephemeral=True)

    @discord.ui.button(label="Close", style=discord.ButtonStyle.danger, custom_id="mcwv_ticket_close")
    async def close_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.staff_ok(interaction):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        embed = discord.Embed(
            title="Close this application ticket?",
            description=(
                "This will generate and save a transcript, mark the ticket as closed, "
                f"and delete the channel after **{MCWV_TICKET_DELETE_DELAY_SECONDS}s**.\n\n"
                "Only continue if the application is finished or no longer needed."
            ),
            color=discord.Color.red(),
            timestamp=datetime.now(timezone.utc),
        )
        ticket_id = self.resolved_ticket_id(interaction)
        await interaction.response.send_message(
            embed=embed,
            view=CloseConfirmView(ticket_id, interaction.user.id, interaction.channel.id, interaction.message.id if interaction.message else None),
            ephemeral=True,
        )


class MCWVTicketPanelView(discord.ui.View):
    def __init__(self, button_label=None):
        super().__init__(timeout=None)
        if button_label is None:
            button_label = get_mcwv_ticket_settings().get("panel", {}).get("buttonLabel", "Open Application")
        for child in self.children:
            if getattr(child, "custom_id", None) == "mcwv_open_application_ticket":
                child.label = str(button_label or "Open Application")[:80]

    @discord.ui.button(label="Open Application", style=discord.ButtonStyle.success, custom_id="mcwv_open_application_ticket")
    async def open_application(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Everything here must finish within Discord's 3-second interaction
        # window — no DB calls, and any failure still answers the interaction.
        try:
            guild = interaction.guild
            if not guild:
                return await interaction.response.send_message("This must be used in the server.", ephemeral=True)
            blacklist_role = guild.get_role(MCWV_TICKET_BLACKLIST_ROLE_ID)
            if blacklist_role and isinstance(interaction.user, discord.Member) and blacklist_role in interaction.user.roles:
                return await interaction.response.send_message("❌ You are currently blocked from opening MCWV application tickets.", ephemeral=True)
            # Keep this interaction fast: Discord requires modal responses within a
            # few seconds. The database-backed blacklist is checked after modal submit
            # (where we can defer), while the role blacklist is checked immediately.
            existing = discord.utils.get(guild.text_channels, topic=f"mcwv-ticket-owner:{interaction.user.id}")
            if existing:
                return await interaction.response.send_message(f"You already have an open application: {existing.mention}", ephemeral=True)
            await interaction.response.send_modal(ApplicationModal(interaction.user))
        except discord.HTTPException as http_exc:
            print(f"[ticket] open_application modal failed: {http_exc}")
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message("❌ Could not open the application form. Please try again.", ephemeral=True)
            except Exception:
                pass
        except Exception as exc:
            print(f"[ticket] open_application error: {exc}")
            traceback.print_exc()
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message("❌ Something went wrong. Please try again.", ephemeral=True)
                else:
                    await interaction.followup.send("❌ Something went wrong. Please try again.", ephemeral=True)
            except Exception:
                pass


@bot.tree.command(name="setup", description="Set up MCWV bot systems in a channel", guild=guild_obj)
@app_commands.describe(
    system="Which system to set up",
    channel="Channel to use for that system"
)
@app_commands.choices(system=[
    app_commands.Choice(name="Placement alerts", value="placement_alerts"),
    app_commands.Choice(name="Clan logs", value="clan_logs"),
    app_commands.Choice(name="Hourly stats", value="hourly_stats"),
])
async def setup(interaction: discord.Interaction, system: app_commands.Choice[str], channel: discord.TextChannel):
    if not has_mcwv_ticket_staff_permission(interaction.user):
        return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    if system.value == "placement_alerts":
        set_placement_channel_id(channel.id)
        db_set_setting("mcwv_placement_alerts_enabled", "1")
        embed = discord.Embed(
            title="Placement alerts configured",
            description=(
                f"MCWV placement cards will be posted in {channel.mention} during active wars only.\n\n"
                "The bot saves the current placement first, then alerts only when the placement changes."
            ),
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc),
        )
        await interaction.followup.send(embed=embed, ephemeral=True)
        return

    if system.value == "clan_logs":
        set_clan_log_channel_id(channel.id)
        db_set_setting("mcwv_clan_logs_enabled", "1")
        embed = discord.Embed(
            title="Clan logs configured",
            description=(
                f"MCWV join, leave, and diamond donation logs will be posted in {channel.mention}.\n\n"
                "The bot saves the current clan state first, then logs only new changes."
            ),
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc),
        )
        await interaction.followup.send(embed=embed, ephemeral=True)
        return

    if system.value == "hourly_stats":
        set_hourly_stats_channel_id(channel.id)
        db_set_setting("mcwv_hourly_stats_enabled", "1")
        if not hourly_stats_loop.is_running():
            hourly_stats_loop.change_interval(minutes=MCWV_HOURLY_STATS_INTERVAL_MINUTES)
            hourly_stats_loop.start()
        embed = discord.Embed(
            title="Hourly stats configured",
            description=(
                f"MCWV hourly stats cards will be posted in {channel.mention} every "
                f"**{MCWV_HOURLY_STATS_INTERVAL_MINUTES} minutes**.\n\n"
                "Use `/hourly_stats` anytime to send one manually."
            ),
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc),
        )
        await interaction.followup.send(embed=embed, ephemeral=True)
        return

    await interaction.followup.send("❌ Unknown setup option.", ephemeral=True)


@bot.tree.command(name="hourly_stats", description="Send the MCWV hourly points statistics card", guild=guild_obj)
@app_commands.describe(channel="Optional channel to send the card in. Defaults to this channel.")
async def hourly_stats(interaction: discord.Interaction, channel: discord.TextChannel = None):
    if not has_mcwv_ticket_staff_permission(interaction.user):
        return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    target = channel or interaction.channel
    if not isinstance(target, discord.TextChannel):
        return await interaction.followup.send("❌ Pick a text channel.", ephemeral=True)
    try:
        await send_hourly_stats_card(target)
        await interaction.followup.send(f"✅ Sent hourly stats in {target.mention}.", ephemeral=True)
    except Exception as exc:
        await interaction.followup.send(f"❌ Hourly stats failed: `{type(exc).__name__}: {exc}`", ephemeral=True)


@bot.tree.command(name="toggle_automation", description="Enable or disable an MCWV bot automation", guild=guild_obj)
@app_commands.describe(
    system="Which automation to toggle",
    enabled="True = on, False = off"
)
@app_commands.choices(system=[
    app_commands.Choice(name="Hourly stats", value="hourly_stats"),
    app_commands.Choice(name="Hourly PPH pings", value="hourly_pings"),
    app_commands.Choice(name="Placement alerts", value="placement_alerts"),
    app_commands.Choice(name="Clan logs", value="clan_logs"),
])
async def toggle_automation(interaction: discord.Interaction, system: app_commands.Choice[str], enabled: bool):
    if not has_mcwv_ticket_staff_permission(interaction.user):
        return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    value = system.value
    notes = []

    if value == "hourly_stats":
        # Manual toggle always wins over the auto war toggle.
        set_hourly_stats_enabled(bool(enabled), auto_disabled=False)
        label = "Hourly stats"
        if enabled:
            loop_obj = globals().get("hourly_stats_loop")
            if loop_obj is not None and not loop_obj.is_running():
                loop_obj.change_interval(minutes=1)
                loop_obj.start()
            if not get_hourly_stats_channel_id():
                notes.append("No hourly channel set yet — run `/setup Hourly stats #channel`.")
        elif MCWV_HOURLY_STATS_AUTO_WAR_TOGGLE:
            notes.append("Auto pause/resume on clan war start/end is still on, but this manual toggle wins.")
    elif value == "hourly_pings":
        db_set_setting("mcwv_hourly_stats_ping_enabled", "1" if enabled else "0")
        label = "Hourly PPH pings"
        if enabled:
            notes.append(f"Members under {get_hourly_stats_ping_threshold()} PPH will be pinged after each hourly card.")
    elif value == "placement_alerts":
        db_set_setting("mcwv_placement_alerts_enabled", "1" if enabled else "0")
        label = "Placement alerts"
        if enabled and not get_placement_channel_id():
            notes.append("No placement channel set yet — run `/setup Placement alerts #channel`.")
    elif value == "clan_logs":
        db_set_setting("mcwv_clan_logs_enabled", "1" if enabled else "0")
        label = "Clan logs"
        if enabled and not get_clan_log_channel_id():
            notes.append("No clan log channel set yet — run `/setup Clan logs #channel`.")
        return await interaction.followup.send("❌ Unknown automation.", ephemeral=True)

    description = f"**{label}** are now **{'enabled' if enabled else 'disabled'}**."
    if notes:
        description += "\n\n" + "\n".join(f"• {note}" for note in notes)

    embed = discord.Embed(
        title=f"{'✅' if enabled else '⏸️'} Automation toggled",
        description=description,
        color=discord.Color.green() if enabled else discord.Color.orange(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_footer(text=f"Changed by {interaction.user}")
    await interaction.followup.send(embed=embed, ephemeral=True)


# ---------------- CROSS-CLAN SPY ----------------
# Premium rival intel — projections, threat ratings, pace analysis.

def _normalize_clan_name(value):
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


async def _fetch_clan_data(clan_name):
    """Fetch any clan's data from PS99 API. Returns the 'data' dict or None."""
    name = str(clan_name or "").strip()
    if not name:
        return None
    payload = await fetch_json_for_placement(f"{PS99_API}/api/clan/{name}")
    if not isinstance(payload, dict):
        return None
    data = payload.get("data", {})
    return data if isinstance(data, dict) and data else None


async def _get_clan_pph_from_history(clan_name, battle_id):
    """Compute a rival clan's PPH from clan_history (last 60 min of snapshots).
    Returns None if we don't have enough data."""
    if not battle_id or not db_enabled():
        return None
    try:
        battle_key = normalize_hourly_battle_key(battle_id)
        battle_variants = list(dict.fromkeys([str(battle_id), battle_key]))
        with conn.cursor() as cur:
            cur.execute(
                """SELECT points, captured_at
                   FROM clan_history
                   WHERE battle_id = ANY(%s)
                     AND LOWER(clan_name) = LOWER(%s)
                     AND captured_at >= NOW() - INTERVAL '3 hours'
                   ORDER BY captured_at ASC""",
                (battle_variants, clan_name),
            )
            rows = cur.fetchall()
        if not rows or len(rows) < 2:
            return None

        latest = rows[-1]
        latest_pts = int(latest[0] or 0)
        latest_ms = latest[1].timestamp() * 1000 if hasattr(latest[1], "timestamp") else 0

        cutoff_ms = latest_ms - 60 * 60 * 1000
        baseline = None
        for row in rows:
            row_ms = row[1].timestamp() * 1000 if hasattr(row[1], "timestamp") else 0
            if row_ms <= cutoff_ms:
                baseline = int(row[0] or 0)
        if baseline is None:
            return None
        return max(0, latest_pts - baseline)
    except Exception as exc:
        print(f"[spy] clan PPH lookup failed for {clan_name}: {exc}")
        return None


async def _get_clan_rank_from_standings(clan_name):
    """Get a clan's rank from the PS99 top-100 leaderboard."""
    payload = await fetch_json_for_placement(
        f"{PS99_API}/api/clans?page=1&pageSize=100&sort=Points&sortOrder=desc"
    )
    if not isinstance(payload, dict):
        return None, None
    rows = payload.get("data", [])
    if not isinstance(rows, list):
        return None, None
    target = _normalize_clan_name(clan_name)
    for i, row in enumerate(rows):
        name = str(row.get("Name") or row.get("name") or "")
        if _normalize_clan_name(name) == target:
            pts = int(row.get("Points") or row.get("points") or 0)
            return i + 1, pts
    return None, None


async def _get_war_timing():
    """Get the active war's start/finish timestamps for projections.
    Returns (start_ts, finish_ts, hours_left) or (None, None, None)."""
    payload = await fetch_json_for_placement(ACTIVE_BATTLE_API)
    if not isinstance(payload, dict):
        return None, None, None
    config = payload.get("data", {}).get("configData", {}) if isinstance(payload.get("data"), dict) else {}
    start = pick_first_int(config, ("StartTime", "startTime"))
    finish = pick_first_int(config, ("FinishTime", "finishTime"))
    if not start or not finish:
        return None, None, None
    now = datetime.now(timezone.utc).timestamp()
    hours_left = max(0, (finish - now) / 3600) if finish > now else 0
    return start, finish, hours_left


def _threat_level(mcwv_pph, rival_pph, gap, above):
    """Rate the threat level of a rival clan.
    Returns (label, emoji, color_hint)."""
    if not mcwv_pph or not rival_pph:
        if above and gap is not None and gap < 5_000_000:
            return ("LOW THREAT", "\U0001f7e1", "close but no pace data")
        return ("UNKNOWN", "\u26aa", "insufficient pace data")
    if above:
        net = mcwv_pph - rival_pph
        if net <= 0:
            return ("EXTREME", "\U0001f534", "they're out-pacing us - gap grows")
        if gap is not None and net > 0:
            eta = gap / net
            if eta < 6:
                return ("HIGH", "\U0001f7e0", f"we overtake in ~{math.ceil(eta)}h")
            if eta < 24:
                return ("MEDIUM", "\U0001f7e1", f"we overtake in ~{math.ceil(eta)}h")
            return ("LOW", "\U0001f7e2", f"~{math.ceil(eta)}h to catch - safe for now")
    else:
        net = rival_pph - mcwv_pph
        if net <= 0:
            return ("SAFE", "\U0001f7e2", "we're pulling away")
        if gap is not None and net > 0:
            eta = abs(gap) / net
            if eta < 6:
                return ("CRITICAL", "\U0001f534", f"they overtake us in ~{math.ceil(eta)}h!")
            if eta < 24:
                return ("HIGH", "\U0001f7e0", f"they overtake us in ~{math.ceil(eta)}h")
            return ("MEDIUM", "\U0001f7e1", f"~{math.ceil(eta)}h until they catch us")
    return ("UNKNOWN", "\u26aa", "insufficient data")


def _pace_bar(mcwv_pph, rival_pph, bar_len=20):
    """Visual bar comparing pace share. Returns a string."""
    if not mcwv_pph or not rival_pph:
        return ""
    if mcwv_pph == 0 and rival_pph == 0:
        return ""
    total = mcwv_pph + rival_pph
    mcwv_share = mcwv_pph / total if total else 0.5
    mcwv_filled = max(1, int(round(mcwv_share * bar_len)))
    rival_filled = bar_len - mcwv_filled
    return f"`{'\u2588' * mcwv_filled}{'\u2591' * rival_filled}`"


@bot.tree.command(name="spy", description="Spy on a rival clan - full intel with projections and threat assessment", guild=guild_obj)
@app_commands.describe(clan_name="Clan tag to spy on (e.g. ABCD)")
@require_role()
async def spy(interaction: discord.Interaction, clan_name: str):
    await interaction.response.defer()

    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession()

    clan_name = clan_name.strip().upper()
    if not re.fullmatch(r"[A-Z0-9_]{1,8}", clan_name):
        return await interaction.followup.send(f"Invalid clan tag `{clan_name}`. Use 1-8 letters/numbers.")

    try:
        data = await _fetch_clan_data(clan_name)
        if not data:
            return await interaction.followup.send(f"Could not find clan `{clan_name}` on PS99.")

        name = data.get("Name", clan_name)
        members = data.get("Members", [])
        if not isinstance(members, list):
            members = []
        member_count = len(members)
        member_capacity = int(data.get("MemberCapacity") or 0)

        level = pick_first_int(data, ("Level", "ClanLevel", "level", "Lvl"))
        gem_candidates = [data.get("Diamonds"), data.get("Bank"), data.get("ClanBank"), data.get("TotalDiamonds")]
        gems = next((int(g) for g in gem_candidates if isinstance(g, (int, float)) and int(g) > 0), None)

        battle_id = await get_active_battle_id_for_placement()
        battles = data.get("Battles", {})
        if not isinstance(battles, dict):
            battles = {}

        battle = None
        if battle_id:
            battle = battles.get(battle_id) or battles.get(str(battle_id))
            if not battle:
                norm = normalize_hourly_battle_key(battle_id)
                battle = next((v for k, v in battles.items() if normalize_hourly_battle_key(k) == norm), None)

        rival_points = None
        contributions = []
        if isinstance(battle, dict):
            rival_points = pick_first_int(battle, ("Points", "points", "BattlePoints"))
            contributions = sorted(
                battle.get("PointContributions") or battle.get("pointContributions") or [],
                key=lambda x: int(x.get("Points", 0) or 0),
                reverse=True,
            )

        mcwv_snapshot, (rival_rank, rival_standings_pts), rival_pph, war_timing = await asyncio.gather(
            get_mcwv_placement_snapshot(),
            _get_clan_rank_from_standings(clan_name),
            _get_clan_pph_from_history(clan_name, battle_id) if battle_id else asyncio.sleep(0, result=None),
            _get_war_timing() if battle_id else asyncio.sleep(0, result=(None, None, None)),
        )

        if rival_points is None:
            rival_points = rival_standings_pts

        mcwv_points = mcwv_snapshot.get("points") if mcwv_snapshot else None
        mcwv_rank = mcwv_snapshot.get("rank") if mcwv_snapshot else None
        mcwv_pph = await _get_clan_pph_from_history(CLAN_NAME, battle_id) if battle_id else None
        hours_left = war_timing[2] if war_timing else None

        rival_projected = None
        mcwv_projected = None
        if hours_left and rival_pph and rival_points:
            rival_projected = rival_points + (rival_pph * hours_left)
        if hours_left and mcwv_pph and mcwv_points:
            mcwv_projected = mcwv_points + (mcwv_pph * hours_left)

        gap = (rival_points - mcwv_points) if (rival_points and mcwv_points) else None
        above = gap is not None and gap > 0
        threat_label, threat_emoji, threat_detail = _threat_level(mcwv_pph, rival_pph, gap, above)

        icon = data.get("Icon")
        icon_id = extract_asset_id(icon) if icon else None

        # ===== BUILD EMBED =====
        embed_color = discord.Color.red() if battle_id else discord.Color(MCWV_BRAND_COLOR)

        embed = discord.Embed(
            title=f"\U0001f50d Spy Report: {name}",
            color=embed_color,
            timestamp=datetime.now(timezone.utc),
        )
        if icon_id:
            embed.set_thumbnail(url=f"{PS99_API}/image/{icon_id}")

        # Description block: threat + war status
        desc_lines = [f"**{threat_emoji} {threat_label}** \u2014 {threat_detail}"]
        if battle_id:
            friendly = _friendly_battle_name(battle_id)
            war_line = f"\u2694\ufe0f **{friendly}**"
            if hours_left:
                war_line += f" \u00b7 {hours_left:.1f}h remaining"
            desc_lines.append(war_line)
        else:
            desc_lines.append("\U0001f634 No active war")
        embed.description = "\n".join(desc_lines)

        # Row 1: Core stats
        stats = []
        stats.append(f"\U0001f465 **{member_count}**/{member_capacity or '?'} members")
        if level:
            stats.append(f"\U0001f3c6 Level **{level}**")
        if rival_rank:
            stats.append(f"\U0001f3c6 Rank **#{rival_rank}**")
        if rival_points is not None:
            stats.append(f"\u2694\ufe0f **{format_points(rival_points)}** pts")
        if gems:
            stats.append(f"\U0001f48e **{format_points(gems)}** gems")
        if rival_points and member_count:
            stats.append(f"\U0001f4ca **{format_points(int(rival_points / member_count))}** avg/member")
        embed.add_field(name="\U0001f4cb Overview", value="\n".join(stats), inline=True)

        # Row 2: Pace
        pace_lines = []
        pace_lines.append(f"**{name}**: {format_points(rival_pph) + '/h' if rival_pph else 'no data'}")
        pace_lines.append(f"**MCWV**: {format_points(mcwv_pph) + '/h' if mcwv_pph else 'no data'}")
        if mcwv_pph and rival_pph:
            pace_lines.append("")
            pace_lines.append(_pace_bar(mcwv_pph, rival_pph))
            net = mcwv_pph - rival_pph
            if net > 0:
                pace_lines.append(f"\U0001f539 We're **{format_points(net)}/h** faster")
            elif net < 0:
                pace_lines.append(f"\U0001f53b They're **{format_points(-net)}/h** faster")
            else:
                pace_lines.append("\u2696 Same pace")
        embed.add_field(name="\U0001f4c8 Pace (PPH)", value="\n".join(pace_lines), inline=True)

        # Gap analysis (full width)
        if gap is not None:
            if above:
                gap_txt = f"\U0001f53b **{format_points(gap)} pts** behind {name}"
            elif gap < 0:
                gap_txt = f"\U0001f539 **{format_points(-gap)} pts** ahead of {name}"
            else:
                gap_txt = "\u2696 **Level** \u2014 same points!"

            if mcwv_pph and rival_pph:
                net = mcwv_pph - rival_pph if above else rival_pph - mcwv_pph
                if above:
                    if net > 0:
                        eta = gap / net
                        gap_txt += f"\n\U0001f539 Closing at **{format_points(net)}/h** net"
                        if hours_left and eta < hours_left:
                            gap_txt += f"\n\u27a1 Overtake in **~{math.ceil(eta)}h** \u2014 **WE CAN CATCH THEM**"
                        elif hours_left:
                            gap_txt += f"\n\u26a0\ufe0f Need {math.ceil(eta)}h, only {hours_left:.0f}h left \u2014 **not enough time**"
                    else:
                        gap_txt += f"\n\U0001f53b They're pulling away at **{format_points(-net)}/h**"
                else:
                    if net > 0:
                        eta = (-gap) / net
                        gap_txt += f"\n\U0001f53b They're closing at **{format_points(net)}/h** net"
                        if hours_left and eta < hours_left:
                            gap_txt += f"\n\u26a0\ufe0f They overtake us in **~{math.ceil(eta)}h** \u2014 **DANGER**"
                        elif hours_left:
                            gap_txt += f"\n\u2705 Need {math.ceil(eta)}h, only {hours_left:.0f}h left \u2014 **safe for now**"
                    else:
                        gap_txt += f"\n\U0001f539 MCWV pulling away at **{format_points(-net)}/h**"

            embed.add_field(name="\U0001f4ca Gap vs MCWV", value=gap_txt, inline=False)

        # Projected finish
        if hours_left and (rival_projected or mcwv_projected):
            proj_lines = []
            if rival_projected:
                proj_lines.append(f"**{name}**: {format_points(int(rival_projected))} pts")
            if mcwv_projected:
                proj_lines.append(f"**MCWV**: {format_points(int(mcwv_projected))} pts")
            if rival_projected and mcwv_projected:
                final_gap = rival_projected - mcwv_projected
                if final_gap > 0:
                    proj_lines.append(f"\u27a1 {name} finishes **{format_points(int(final_gap))}** ahead")
                elif final_gap < 0:
                    proj_lines.append(f"\u27a1 MCWV finishes **{format_points(int(-final_gap))}** ahead!")
                else:
                    proj_lines.append("\u27a1 Dead heat!")
            embed.add_field(name="\U0001f52e Projected Finish", value="\n".join(proj_lines), inline=False)

        # Top 3 contributors
        if contributions:
            linked_by_roblox = {}
            try:
                for u in db_get_all() or []:
                    linked_by_roblox[int(u[0])] = u[2]
            except Exception:
                pass

            top_lines = []
            medals = ["\U0001f947", "\U0001f948", "\U0001f949"]
            for i, entry in enumerate(contributions[:3]):
                rid = int(entry.get("UserID", 0) or 0)
                pts = int(entry.get("Points", 0) or 0)
                linked_name = linked_by_roblox.get(rid)
                display_name = linked_name or str(rid)
                pct = (pts / rival_points * 100) if rival_points else 0
                top_lines.append(f"{medals[i]} **{display_name}** \u2014 {format_points(pts)} pts ({pct:.1f}%)")
            embed.add_field(name="\U0001f3c6 Top Contributors", value="\n".join(top_lines), inline=False)

        embed.set_footer(text=f"PS99 live \u00b7 Spied by {interaction.user}")
        await interaction.followup.send(embed=embed)

    except Exception as e:
        print(f"[spy] error: {e}")
        traceback.print_exc()
        await interaction.followup.send(f"Spy report failed: `{type(e).__name__}`")


@bot.tree.command(name="spycompare", description="Full head-to-head: MCWV vs rival with projections, pace, and threat level", guild=guild_obj)
@app_commands.describe(clan_name="Rival clan tag (e.g. ABCD)")
@require_role()
async def spycompare(interaction: discord.Interaction, clan_name: str):
    await interaction.response.defer()

    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession()

    clan_name = clan_name.strip().upper()
    if not re.fullmatch(r"[A-Z0-9_]{1,8}", clan_name):
        return await interaction.followup.send(f"Invalid clan tag `{clan_name}`.")

    try:
        battle_id = await get_active_battle_id_for_placement()

        mcwv_data, rival_data, war_timing = await asyncio.gather(
            _fetch_clan_data(CLAN_NAME),
            _fetch_clan_data(clan_name),
            _get_war_timing() if battle_id else asyncio.sleep(0, result=(None, None, None)),
        )

        if not rival_data:
            return await interaction.followup.send(f"Could not find clan `{clan_name}`.")

        rival_name = rival_data.get("Name", clan_name)
        hours_left = war_timing[2] if war_timing else None

        mcwv_members = len(mcwv_data.get("Members", []) or []) if mcwv_data else 0
        mcwv_battles = (mcwv_data or {}).get("Battles", {})
        mcwv_battle = None
        if battle_id and isinstance(mcwv_battles, dict):
            mcwv_battle = mcwv_battles.get(battle_id) or mcwv_battles.get(str(battle_id))
        mcwv_points = pick_first_int(mcwv_battle, ("Points", "points")) if isinstance(mcwv_battle, dict) else None

        rival_members = len(rival_data.get("Members", []) or [])
        rival_battles = rival_data.get("Battles", {})
        rival_battle = None
        if battle_id and isinstance(rival_battles, dict):
            rival_battle = rival_battles.get(battle_id) or rival_battles.get(str(battle_id))
        rival_points = pick_first_int(rival_battle, ("Points", "points")) if isinstance(rival_battle, dict) else None

        (mcwv_rank, mcwv_sp), (rival_rank, rival_sp), mcwv_pph, rival_pph = await asyncio.gather(
            _get_clan_rank_from_standings(CLAN_NAME),
            _get_clan_rank_from_standings(clan_name),
            _get_clan_pph_from_history(CLAN_NAME, battle_id) if battle_id else asyncio.sleep(0, result=None),
            _get_clan_pph_from_history(clan_name, battle_id) if battle_id else asyncio.sleep(0, result=None),
        )
        if mcwv_points is None:
            mcwv_points = mcwv_sp
        if rival_points is None:
            rival_points = rival_sp

        gap = (rival_points - mcwv_points) if (rival_points and mcwv_points) else None
        above = gap is not None and gap > 0
        threat_label, threat_emoji, threat_detail = _threat_level(mcwv_pph, rival_pph, gap, above)

        mcwv_proj = (mcwv_points + mcwv_pph * hours_left) if (hours_left and mcwv_pph and mcwv_points) else None
        rival_proj = (rival_points + rival_pph * hours_left) if (hours_left and rival_pph and rival_points) else None

        embed_color = discord.Color.red() if battle_id else discord.Color(MCWV_BRAND_COLOR)

        embed = discord.Embed(
            title=f"\u2694\ufe0f MCWV vs {rival_name}",
            description=f"**{threat_emoji} {threat_label}** \u2014 {threat_detail}",
            color=embed_color,
            timestamp=datetime.now(timezone.utc),
        )
        if hours_left:
            embed.description += f"\n\u23f1\ufe0f **{hours_left:.1f}h** remaining"

        # Comparison table
        comp = []
        comp.append(f"**Rank**     MCWV #{mcwv_rank or '?'}  vs  {rival_name} #{rival_rank or '?'}")
        comp.append(f"**Points**   MCWV {format_points(mcwv_points or 0)}  vs  {rival_name} {format_points(rival_points or 0)}")
        comp.append(f"**Members**  MCWV {mcwv_members}  vs  {rival_name} {rival_members}")
        pph_m = format_points(mcwv_pph) + "/h" if mcwv_pph else "no data"
        pph_r = format_points(rival_pph) + "/h" if rival_pph else "no data"
        comp.append(f"**PPH**      MCWV {pph_m}  vs  {rival_name} {pph_r}")
        if mcwv_points and mcwv_members and rival_points and rival_members:
            mcwv_avg = int(mcwv_points / mcwv_members)
            rival_avg = int(rival_points / rival_members)
            efficiency = (rival_avg / mcwv_avg) if mcwv_avg else 0
            eff_label = "highly efficient" if efficiency > 1.2 else "comparable" if efficiency > 0.8 else "less efficient"
            comp.append(f"**Avg/Member** MCWV {format_points(mcwv_avg)}  vs  {rival_name} {format_points(rival_avg)} ({efficiency:.2f}x - {eff_label})")
        embed.add_field(name="\U0001f4ca Head to Head", value=f"```\n{chr(10).join(comp)}\n```", inline=False)

        # Pace bar
        if mcwv_pph and rival_pph:
            bar = _pace_bar(mcwv_pph, rival_pph)
            total_pace = mcwv_pph + rival_pph
            mcwv_pct = (mcwv_pph / total_pace * 100) if total_pace else 50
            embed.add_field(
                name="\U0001f4c8 Pace Share",
                value=f"```\nMCWV {bar} {rival_name}\n     {mcwv_pct:.0f}% vs {100 - mcwv_pct:.0f}%\n```",
                inline=False,
            )

        # Gap analysis
        if gap is not None:
            if above:
                gap_txt = f"\U0001f53b **{format_points(gap)} pts** behind {rival_name}"
            elif gap < 0:
                gap_txt = f"\U0001f539 **{format_points(-gap)} pts** ahead of {rival_name}"
            else:
                gap_txt = "\u2696 **Level** \u2014 same points!"

            if mcwv_pph and rival_pph:
                net = mcwv_pph - rival_pph if above else rival_pph - mcwv_pph
                if above:
                    if net > 0:
                        eta = gap / net
                        gap_txt += f"\n\U0001f539 Closing at **{format_points(net)}/h** net"
                        if hours_left and eta < hours_left:
                            gap_txt += f"\n\u27a1 Overtake in **~{math.ceil(eta)}h** \u2014 **WE CAN CATCH THEM**"
                        elif hours_left:
                            gap_txt += f"\n\u26a0\ufe0f Need {math.ceil(eta)}h, only {hours_left:.0f}h left \u2014 **not enough time**"
                    else:
                        gap_txt += f"\n\U0001f53b They're pulling away at **{format_points(-net)}/h**"
                else:
                    if net > 0:
                        eta = (-gap) / net
                        gap_txt += f"\n\U0001f53b They're closing at **{format_points(net)}/h** net"
                        if hours_left and eta < hours_left:
                            gap_txt += f"\n\u26a0\ufe0f They overtake us in **~{math.ceil(eta)}h** \u2014 **DANGER**"
                        elif hours_left:
                            gap_txt += f"\n\u2705 Need {math.ceil(eta)}h, only {hours_left:.0f}h left \u2014 **safe for now**"
                    else:
                        gap_txt += f"\n\U0001f539 MCWV pulling away at **{format_points(-net)}/h**"

            embed.add_field(name="\U0001f4ca Gap Analysis", value=gap_txt, inline=False)

        # Projected finish
        if hours_left and (mcwv_proj or rival_proj):
            proj_lines = []
            if mcwv_proj:
                proj_lines.append(f"**MCWV**: {format_points(int(mcwv_proj))} pts")
            if rival_proj:
                proj_lines.append(f"**{rival_name}**: {format_points(int(rival_proj))} pts")
            if mcwv_proj and rival_proj:
                final_gap = rival_proj - mcwv_proj
                if final_gap > 0:
                    proj_lines.append(f"\u27a1 {rival_name} finishes **{format_points(int(final_gap))}** ahead")
                elif final_gap < 0:
                    proj_lines.append(f"\u27a1 MCWV finishes **{format_points(int(-final_gap))}** ahead!")
                else:
                    proj_lines.append("\u27a1 Dead heat!")
            embed.add_field(name="\U0001f52e Projected Final", value="\n".join(proj_lines), inline=False)

        embed.set_footer(text=f"PS99 live + clan_history \u00b7 {interaction.user}")
        await interaction.followup.send(embed=embed)

    except Exception as e:
        print(f"[spycompare] error: {e}")
        traceback.print_exc()
        await interaction.followup.send(f"Comparison failed: `{type(e).__name__}`")


@bot.tree.command(name="threatboard", description="Show clans ranked around MCWV with pace analysis and threat ratings", guild=guild_obj)
@require_role()
async def threatboard(interaction: discord.Interaction):
    await interaction.response.defer()

    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession()

    try:
        battle_id = await get_active_battle_id_for_placement()
        if not battle_id:
            return await interaction.followup.send("No active war \u2014 threat board is only useful during a war.")

        mcwv_snapshot, war_timing = await asyncio.gather(
            get_mcwv_placement_snapshot(),
            _get_war_timing(),
        )
        if not mcwv_snapshot:
            return await interaction.followup.send("Can't determine MCWV's current placement.")

        mcwv_rank = mcwv_snapshot["rank"]
        mcwv_points = mcwv_snapshot["points"]
        hours_left = war_timing[2] if war_timing else None

        mcwv_pph = await _get_clan_pph_from_history(CLAN_NAME, battle_id)

        # Get top-100 standings
        payload = await fetch_json_for_placement(
            f"{PS99_API}/api/clans?page=1&pageSize=100&sort=Points&sortOrder=desc"
        )
        rows = payload.get("data", []) if isinstance(payload, dict) else []
        if not isinstance(rows, list) or not rows:
            return await interaction.followup.send("Can't load the clan standings right now.")

        # Build the ordered list: 3 clans above MCWV, MCWV, 3 clans below
        # (like a leaderboard window: #20, #21, #22, [MCWV #23], #24, #25, #26)
        window_above = 3
        window_below = 3

        # Find MCWV in the standings
        mcwv_idx = -1
        all_clans = []
        for i, row in enumerate(rows):
            clan_name = str(row.get("Name") or row.get("name") or "")
            pts = int(row.get("Points") or row.get("points") or 0)
            is_us = _normalize_clan_name(clan_name) == _normalize_clan_name(CLAN_NAME)
            all_clans.append({"name": clan_name, "rank": i + 1, "points": pts, "is_us": is_us})
            if is_us:
                mcwv_idx = i

        if mcwv_idx < 0:
            return await interaction.followup.send("MCWV not found in the top-100 standings.")

        # Window: 3 above + MCWV + 3 below
        start_idx = max(0, mcwv_idx - window_above)
        end_idx = min(len(all_clans), mcwv_idx + window_below + 1)
        window = all_clans[start_idx:end_idx]

        # Fetch PPH for all window clans in parallel
        rival_clans = [c for c in window if not c["is_us"]]
        pph_results = await asyncio.gather(
            *[_get_clan_pph_from_history(c["name"], battle_id) for c in rival_clans],
            return_exceptions=True,
        )
        pph_map = {}
        for i, c in enumerate(rival_clans):
            pph_map[c["name"]] = pph_results[i] if not isinstance(pph_results[i], Exception) else None

        # Build embed
        embed = discord.Embed(
            title="\U0001f3af Threat Board",
            description=(
                f"Clans ranked around MCWV \u2014 **#{mcwv_rank}** \u00b7 {format_points(mcwv_points)} pts"
                + (f" \u00b7 {format_points(mcwv_pph)}/h" if mcwv_pph else "")
                + (f" \u00b7 {hours_left:.1f}h left" if hours_left else "")
            ),
            color=discord.Color.red(),
            timestamp=datetime.now(timezone.utc),
        )

        lines = []
        for c in window:
            rank = c["rank"]
            clan_name = c["name"]
            pts = c["points"]

            if c["is_us"]:
                # Highlight MCWV
                lines.append(f"\n\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
                lines.append(f"\U0001f451 **#{rank} {clan_name}** (US) \u2014 {format_points(pts)} pts")
                if mcwv_pph:
                    lines.append(f"   \U0001f4c8 {format_points(mcwv_pph)}/h")
                lines.append(f"\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n")
                continue

            rival_pph = pph_map.get(clan_name)
            gap = pts - mcwv_points
            above = gap > 0

            threat_label, threat_emoji, threat_detail = _threat_level(mcwv_pph, rival_pph, gap, above)

            # Rank + name + threat emoji
            line = f"**#{rank}** {threat_emoji} **{clan_name}**"

            # Points + gap
            gap_dir = "\u2b06\ufe0f" if above else "\u2b07\ufe0f"
            gap_txt = f"{format_points(abs(gap))} pts {'above' if above else 'below'}"
            line += f" \u2014 {format_points(pts)} pts ({gap_dir} {gap_txt})"

            # PPH
            if rival_pph and rival_pph > 0:
                line += f" \u00b7 \U0001f4c8 {format_points(rival_pph)}/h"

            # Threat detail
            line += f"\n   {threat_emoji} {threat_detail}"

            # Projection
            if hours_left and rival_pph and mcwv_pph:
                rival_proj = pts + rival_pph * hours_left
                mcwv_proj = mcwv_points + mcwv_pph * hours_left
                final_gap = rival_proj - mcwv_proj
                if above and final_gap < 0:
                    line += f"\n   \u27a1 \U0001f539 MCWV overtakes by **{format_points(int(-final_gap))}** at war end!"
                elif not above and final_gap > 0:
                    line += f"\n   \u27a1 \U0001f53b They overtake by **{format_points(int(final_gap))}** at war end!"
                elif abs(final_gap) < 2_000_000:
                    line += f"\n   \u27a1 \u2696 Dead heat at war end!"

            lines.append(line + "\n")

        embed.add_field(name=f"\U0001f4cb Standings Window (#{window[0]['rank']}\u2013#{window[-1]['rank']})", value="\n".join(lines), inline=False)

        # Summary
        safe = 0
        danger = 0
        medium = 0
        for c in rival_clans:
            r_pph = pph_map.get(c["name"])
            gap = c["points"] - mcwv_points
            label, _, _ = _threat_level(mcwv_pph, r_pph, gap, gap > 0)
            if label in ("SAFE", "LOW"):
                safe += 1
            elif label in ("CRITICAL", "EXTREME", "HIGH"):
                danger += 1
            else:
                medium += 1

        embed.add_field(name="\U0001f4ca Summary", value=f"\U0001f7e2 **{safe}** safe \u00b7 \U0001f7e1 **{medium}** unknown/medium \u00b7 \U0001f534 **{danger}** dangerous", inline=True)

        embed.set_footer(text=f"PS99 live + clan_history \u00b7 {interaction.user}")
        await interaction.followup.send(embed=embed)

    except Exception as e:
        print(f"[threatboard] error: {e}")
        traceback.print_exc()
        await interaction.followup.send(f"Threat board failed: `{type(e).__name__}`")



# ---------------- CROSS-CLAN PLAYER CHECK ----------------
# Look up any Roblox player's war history across all clans they've been in.
# Uses the existing top-100 clan scan index + PS99 API fallbacks.


# ---------------- CHECKPLAYER: DATA GATHERING HELPER ----------------

async def gather_player_data(roblox_id, roblox_name):
    """Gather all player data from cache + 1-2 API calls. Returns a dict.
    Cache-first: no 100-clan scan, ~1 second instead of 10+."""
    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession()

    # 1. Cache (instant)
    cached_rows = get_cached_player_history(roblox_id)
    print(f"[checkplayer] {roblox_name} (ID:{roblox_id}) — cache: {len(cached_rows)} rows")

    # 2. V1 summary (1 API call) for current clan + active points
    summary_payload = await _ps99_json(f"{PS99_API}/v1/clans/players/{roblox_id}")
    summary = {}
    if isinstance(summary_payload, dict) and summary_payload.get("status") == "ok":
        player = summary_payload.get("data", {}).get("player", {})
        if isinstance(player, dict):
            summary = player

    # 3. Current clan from summary
    current_clan = summary.get("Clan", {}) if isinstance(summary.get("Clan"), dict) else {}
    current_clan_name = current_clan.get("Name") or "Unknown"
    active_points = _safe_int(summary.get("ActiveBattlePoints"))

    # 4. Check MCWV membership (1 API call)
    mcwv_clan_data = await _fetch_clan_data(CLAN_NAME)
    in_mcwv_now = False
    if mcwv_clan_data:
        mcwv_members = mcwv_clan_data.get("Members", [])
        if isinstance(mcwv_members, list):
            in_mcwv_now = any(str(m.get("UserID")) == str(roblox_id) for m in mcwv_members if isinstance(m, dict))
            if in_mcwv_now:
                current_clan_name = CLAN_NAME

    # 5. Update active battle points from summary
    rows = []
    for row in cached_rows:
        r = dict(row)
        # If this is the active battle and summary has higher points, update
        if active_points and active_points > _safe_int(r.get("points")):
            # The summary's ActiveBattlePoints is for the current battle
            # We can't map it to a specific battle_id, so just leave cache as-is
            pass
        rows.append(r)
    rows.sort(key=_battle_sort_key, reverse=True)

    # 6. Scraped clan memberships
    scraped_clans = get_cached_clan_memberships(roblox_id)
    clans_from_scrape_only = []
    clans_in_cache = set()
    for row in rows:
        if isinstance(row, dict):
            clans_in_cache.add(str(row.get("clan") or "").upper())
    for cn in scraped_clans:
        if cn.upper() not in clans_in_cache and cn not in clans_from_scrape_only:
            clans_from_scrape_only.append(cn)

    # 6c. Build clan_stats early (needed by AwardUserIDs scan below)
    clan_stats = {}
    for row in rows:
        cn = str(row.get("clan") or "Unknown").strip()
        if cn not in clan_stats:
            clan_stats[cn] = {"battles": 0, "total_points": 0, "best_points": 0, "medals": 0}
        clan_stats[cn]["battles"] += 1
        pts = _safe_int(row.get("points"))
        clan_stats[cn]["total_points"] += pts
        if pts > clan_stats[cn]["best_points"]:
            clan_stats[cn]["best_points"] = pts
        if row.get("earnedMedal"):
            clan_stats[cn]["medals"] += 1

    # 6b. Scan AwardUserIDs for clans the player was in but has no battle data for.
    # The API returns AwardUserIDs (full 75-member roster) even for past battles
    # where PointContributions only has top scorers. This captures battles where
    # the player participated but wasn't a top scorer.
    existing_battle_ids = {str(r.get("battleId") or "").lower() for r in rows}
    award_battles = []
    clans_to_scan = set()
    # Add clans from scrape that have no battle data
    for cn in clans_from_scrape_only:
        clans_to_scan.add(cn)
    # Also add rival clans from cache that have 0 battles
    for cn, stats in clan_stats.items():
        if stats["battles"] == 0:
            clans_to_scan.add(cn)

    for clan_name in list(clans_to_scan)[:5]:  # cap at 5 to limit API calls
        try:
            clan_data = await _fetch_clan_data(clan_name)
            if not clan_data or not isinstance(clan_data, dict):
                continue
            battles = clan_data.get("Battles") or {}
            if not isinstance(battles, dict):
                continue
            for bid, battle in battles.items():
                if not isinstance(battle, dict):
                    continue
                bid_lower = str(bid).lower()
                if bid_lower in existing_battle_ids:
                    continue
                award_ids = battle.get("AwardUserIDs") or []
                if not isinstance(award_ids, list):
                    continue
                if int(roblox_id) in award_ids:
                    # Player participated but points unknown
                    award_battles.append({
                        "battleId": str(bid),
                        "title": _friendly_battle_name(str(bid)),
                        "clan": str(clan_name),
                        "points": 0,
                        "rank": None,
                        "total": None,
                        "clanPlace": battle.get("Place") or battle.get("place"),
                        "earnedMedal": bool(battle.get("EarnedMedal") or battle.get("earnedMedal")),
                        "startTime": _safe_int(battle.get("StartTime") or battle.get("startTime")),
                        "participated_only": True,
                    })
                    existing_battle_ids.add(bid_lower)
        except Exception as exc:
            print(f"[checkplayer] AwardUserIDs scan failed for {clan_name}: {exc}")

    if award_battles:
        print(f"[checkplayer] {roblox_name} — found {len(award_battles)} participated-only battles via AwardUserIDs")
        rows.extend(award_battles)
        rows.sort(key=_battle_sort_key, reverse=True)

    # 7. Analyze
    clans_seen = []
    for row in rows:
        cn = str(row.get("clan") or "").strip()
        if cn and cn not in clans_seen:
            clans_seen.append(cn)
    for cn in clans_from_scrape_only:
        if cn not in clans_seen:
            clans_seen.append(cn)

    been_in_mcwv = any(_normalize_clan_name(c) == _normalize_clan_name(CLAN_NAME) for c in clans_seen)
    rival_clans = [c for c in clans_seen if _normalize_clan_name(c) != _normalize_clan_name(CLAN_NAME)]
    current_clan_norm = _normalize_clan_name(current_clan_name)
    is_currently_mcwv = current_clan_norm == _normalize_clan_name(CLAN_NAME)
    is_currently_rival = current_clan_name != "Unknown" and current_clan_norm != _normalize_clan_name(CLAN_NAME)

    # Rank analysis
    rank_values = []
    for r in rows:
        rk = _safe_int(r.get("rank"))
        total = _safe_int(r.get("total")) or 1
        if rk > 0 and total > 1:
            pct = float(r.get("betterThan") or ((total - rk) / total * 100))
            rank_values.append({"rank": rk, "total": total, "pct": pct, "clan": r.get("clan")})

    total_battles_agg = max(len(rows), len(clans_seen))
    earned_medals_agg = sum(1 for r in rows if r.get("earnedMedal"))

    return {
        "roblox_id": roblox_id,
        "roblox_name": roblox_name,
        "rows": rows,
        "current_clan_name": current_clan_name,
        "active_points": active_points,
        "in_mcwv_now": in_mcwv_now,
        "is_currently_mcwv": is_currently_mcwv,
        "is_currently_rival": is_currently_rival,
        "been_in_mcwv": been_in_mcwv,
        "rival_clans": rival_clans,
        "clans_seen": clans_seen,
        "clans_from_scrape_only": clans_from_scrape_only,
        "clan_stats": clan_stats,
        "rank_values": rank_values,
        "total_battles_agg": total_battles_agg,
        "earned_medals_agg": earned_medals_agg,
        "cached_rows": cached_rows,
        "scraped_clans": scraped_clans,
    }


def build_checkplayer_embed(data, page=0, per_page=7):
    """Build a Discord embed for checkplayer. Supports pagination."""
    rows = data["rows"]
    roblox_name = data["roblox_name"]
    roblox_id = data["roblox_id"]
    is_currently_mcwv = data["is_currently_mcwv"]
    is_currently_rival = data["is_currently_rival"]
    current_clan_name = data["current_clan_name"]
    active_points = data["active_points"]
    earned_medals_agg = data["earned_medals_agg"]
    total_battles_agg = data["total_battles_agg"]
    clans_seen = data["clans_seen"]
    clans_from_scrape_only = data["clans_from_scrape_only"]
    been_in_mcwv = data["been_in_mcwv"]
    rival_clans = data["rival_clans"]
    rank_values = data["rank_values"]
    clan_stats = data["clan_stats"]

    # Color
    if is_currently_mcwv:
        embed_color = discord.Color.green()
    elif is_currently_rival:
        embed_color = discord.Color.red()
    else:
        embed_color = discord.Color(MCWV_BRAND_COLOR)

    embed = discord.Embed(
        title=f"\U0001f50d {roblox_name}",
        color=embed_color,
        timestamp=datetime.now(timezone.utc),
    )

    # ===== STATUS BAR =====
    status_parts = []
    if is_currently_mcwv:
        status_parts.append("\U0001f451 MCWV Member")
    elif is_currently_rival:
        status_parts.append(f"\U0001f575 {current_clan_name}")
    else:
        status_parts.append("\u2754 Clan unknown")
    if active_points and active_points > 0:
        status_parts.append(f"\u2694\ufe0f {format_points(active_points)} pts")
    if total_battles_agg:
        status_parts.append(f"\U0001f4ca {total_battles_agg} battles")
    embed.description = f"ID: `{roblox_id}`\n{' | '.join(status_parts)}"

    # ===== TRANSFER DETECTION (page 0 only) =====
    if page == 0:
        if been_in_mcwv and is_currently_rival:
            embed.add_field(
                name="\u26a0\ufe0f Left MCWV",
                value=f"Was in **MCWV**, now in **{current_clan_name}**.",
                inline=False,
            )
        elif is_currently_mcwv and rival_clans:
            embed.add_field(
                name="\U0001f575 Recruit from Rivals",
                value=f"Now in **MCWV**, previously in: **{', '.join(rival_clans[:5])}**",
                inline=False,
            )
        elif been_in_mcwv and not is_currently_mcwv:
            embed.add_field(
                name="\U0001f451 Former MCWV",
                value=f"Was in MCWV, now in **{current_clan_name}**",
                inline=False,
            )

        # Clan History
        if clans_seen:
            clan_line = " \u2192 ".join(f"**{c}**" for c in clans_seen[:10])
            if len(clans_seen) > 10:
                clan_line += f" +{len(clans_seen) - 10}"
            embed.add_field(name="\U0001f4cb Clan History", value=clan_line, inline=False)

    # ===== WAR HISTORY TABLE (paginated) =====
    if rows:
        start = page * per_page
        end = start + per_page
        page_rows = rows[start:end]
        table_lines = []
        MAX_FIELD_LEN = 1020

        for row in page_rows:
            title = _friendly_battle_name(row.get("battleId") or row.get("title") or "?")
            clan = str(row.get("clan") or "?")
            pts = _safe_int(row.get("points"))
            rank = _safe_int(row.get("rank"))
            total = _safe_int(row.get("total")) or 0
            place = row.get("clanPlace")
            clan_display = f"**{clan}**" if clan.upper() == CLAN_NAME.upper() else clan
            participated = row.get("participated_only")

            if participated:
                # Participated but no score data (from AwardUserIDs)
                place_str = f" \u00b7 clan #{int(place)}" if place and place not in (None, "", 0) else ""
                line1 = f"`{title}` \u2014 {clan_display}"
                line2 = f"\u2713 participated (no score data){place_str}"
            elif rank > 0 and total > 1:
                pct = float(row.get("betterThan") or ((total - rank) / total * 100))
                rank_txt = f"#{rank:,}/{total:,}"
                # Percentile with visual indicator
                if pct >= 95:
                    pct_icon = "\U0001f451"
                    better_txt = f"{pct_icon} **{pct:.1f}%**"
                elif pct >= 80:
                    better_txt = f"\U0001f7e2 **{pct:.1f}%**"
                elif pct >= 50:
                    better_txt = f"\U0001f7e1 {pct:.1f}%"
                else:
                    better_txt = f"\U0001f534 _{pct:.1f}%_"
                place_str = f" \u00b7 clan #{int(place)}" if place and place not in (None, "", 0) else ""
                line1 = f"`{title}` \u2014 {clan_display}"
                stats_parts = [f"**{format_points(pts)}**", rank_txt, f"{better_txt} better"]
                if place_str:
                    stats_parts.append(f"clan #{int(place)}")
                line2 = " \u00b7 ".join(stats_parts)
            else:
                place_str = f" \u00b7 clan #{int(place)}" if place and place not in (None, "", 0) else ""
                line1 = f"`{title}` \u2014 {clan_display}"
                line2 = f"{format_points(pts)} pts{place_str}"

            projected = len("\n".join(table_lines + [line1, line2]))
            if projected > MAX_FIELD_LEN:
                break
            table_lines.append(line1)
            table_lines.append(line2)

        total_pages = max(1, (len(rows) + per_page - 1) // per_page)
        shown = len(table_lines) // 2
        embed.add_field(
            name=f"\U0001f4ca War History \u00b7 {len(rows)} battles \u00b7 Page {page+1}/{total_pages}",
            value="\n".join(table_lines) or "No battle data.",
            inline=False,
        )

    # ===== PERFORMANCE ANALYSIS (last page only) =====
    total_pages = max(1, (len(rows) + per_page - 1) // per_page)
    if page >= total_pages - 1 and rank_values:
        sorted_by_perf = sorted(rank_values, key=lambda r: r["pct"], reverse=True)
        best = sorted_by_perf[0]
        worst = sorted_by_perf[-1]
        avg_pct = sum(r["pct"] for r in rank_values) / len(rank_values)

        embed.add_field(
            name="\U0001f3c6 Best Finish",
            value=f"**{best['pct']:.1f}%** better\n#{best['rank']:,}/{best['total']:,}\n_{best['clan']}_",
            inline=True,
        )
        embed.add_field(
            name="\U0001f4ca Average",
            value=f"**{avg_pct:.1f}%** better\nthan global\n{len(rank_values)} ranked battles",
            inline=True,
        )
        embed.add_field(
            name="\U0001f53b Worst Finish",
            value=f"**{worst['pct']:.1f}%** better\n#{worst['rank']:,}/{worst['total']:,}",
            inline=True,
        )

        # Trend + Activity
        trend_txt = ""
        if len(rank_values) >= 3:
            recent = [r["rank"] for r in rank_values[:3]]
            old = [r["rank"] for r in rank_values[-3:]]
            recent_avg = sum(recent) / len(recent)
            old_avg = sum(old) / len(old)
            if recent_avg < old_avg * 0.7:
                trend_txt = "\U0001f539 Improving"
            elif recent_avg > old_avg * 1.3:
                trend_txt = "\U0001f53b Declining"
            else:
                trend_txt = "\u2192 Stable"

        activity_txt = ""
        if rows:
            latest_start = _safe_int(rows[0].get("startTime"))
            if latest_start > 0:
                days_ago = (time.time() - latest_start) / 86400
                if days_ago < 14:
                    activity_txt = "\U0001f525 Active"
                elif days_ago < 60:
                    activity_txt = "\u2705 Recent"
                elif days_ago < 180:
                    activity_txt = "\U0001f575 Semi-active"
                else:
                    activity_txt = f"\U0001f4a8 Inactive ({days_ago/30:.0f}mo)"

        if trend_txt or activity_txt:
            parts = [p for p in [trend_txt, activity_txt] if p]
            embed.add_field(name="\U0001f4c9 Trend & Activity", value=" \u00b7 ".join(parts), inline=False)

    # ===== FOOTER =====
    data_sources = []
    if data.get("cached_rows"):
        data_sources.append("global cache")
    if data.get("scraped_clans"):
        data_sources.append("scrape")
    source_txt = " + ".join(data_sources) if data_sources else "PS99"
    total_pages = max(1, (len(rows) + per_page - 1) // per_page) if rows else 1

    # Accuracy note on page 1 only
    has_participated = any(r.get("participated_only") for r in rows)
    footer_parts = [source_txt, f"{len(rows)} battles", f"pg {page+1}/{total_pages}"]
    if page == 0:
        footer_parts.append("pre-backfill wars may be incomplete")
    embed.set_footer(text=" \u00b7 ".join(footer_parts))

    return embed


class CheckPlayerView(discord.ui.View):
    """Pagination view for /checkplayer — regenerates image per page."""
    def __init__(self, data, avatar_url=None, per_page=7):
        super().__init__(timeout=300)
        self.data = data
        self.avatar_url = avatar_url
        self.page = 0
        self.per_page = per_page

    def total_pages(self):
        rows = self.data["rows"]
        return max(1, (len(rows) + self.per_page - 1) // self.per_page)

    async def _move(self, interaction, delta):
        # Defer FIRST so Discord doesn't timeout while we generate the image
        await interaction.response.defer()
        self.page = (self.page + delta) % self.total_pages()
        embed = build_checkplayer_embed(self.data, page=self.page, per_page=self.per_page)
        if self.avatar_url:
            embed.set_thumbnail(url=self.avatar_url)
        try:
            image = await generate_checkplayer_card(self.data, self.avatar_url, page=self.page, per_page=self.per_page)
            file = discord.File(image, filename="checkplayer-card.png")
            embed.set_image(url="attachment://checkplayer-card.png")
            await interaction.edit_original_response(embed=embed, attachments=[file], view=self)
        except Exception as exc:
            print(f"[checkplayer] page {self.page} image failed: {exc}")
            traceback.print_exc()
            try:
                await interaction.edit_original_response(embed=embed, view=self)
            except Exception:
                pass

    @discord.ui.button(label="\u25c0", style=discord.ButtonStyle.secondary)
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._move(interaction, -1)

    @discord.ui.button(label="\u25b6", style=discord.ButtonStyle.secondary)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._move(interaction, 1)


async def generate_checkplayer_card(data, avatar_url=None, page=0, per_page=7):
    """Generate a polished dashboard image for /checkplayer.
    Clean card UI: galaxy bg, avatar, header section, battle rows, summary."""
    S = 2
    rows = data["rows"]
    page_rows = rows[page * per_page : page * per_page + per_page]

    def sc(v):
        return int(round(v * S))

    # ---- Layout ----
    W = 1200 * S
    MARGIN = sc(60)
    RIGHT = W - sc(60)
    CONTENT_W = RIGHT - MARGIN

    # Column positions (aligned grid)
    C_NAME = MARGIN + sc(12)       # battle name
    C_CLAN = MARGIN + sc(12)       # clan tag (below name)
    C_PTS = MARGIN + sc(430)       # points column
    C_RANK = MARGIN + sc(560)      # rank column
    C_BAR = MARGIN + sc(770)       # percentile bar
    BAR_W = sc(260)
    C_PCT = C_BAR + BAR_W + sc(10) # percentile text

    # Row heights
    RH_NORMAL = sc(50)
    RH_COMPACT = sc(34)
    HEADER_H = sc(280)
    FOOTER_H = sc(35)
    SUMMARY_H = sc(95)

    # Calculate total height
    list_h = 0
    for row in page_rows:
        list_h += RH_COMPACT if row.get("participated_only") else RH_NORMAL
    total_pages = max(1, (len(rows) + per_page - 1) // per_page)
    has_summary = page >= total_pages - 1 and data.get("rank_values")
    H = HEADER_H + max(list_h, sc(40)) + (SUMMARY_H if has_summary else 0) + FOOTER_H + sc(20)

    # ---- Fonts ----
    def font(size, bold=True):
        path = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
        try:
            return ImageFont.truetype(path, sc(size))
        except Exception:
            return ImageFont.load_default()

    F = {
        "name": font(28, True),
        "sub": font(15, False),
        "badge": font(16, True),
        "section": font(18, True),
        "battle": font(16, True),
        "stats": font(13, False),
        "pct": font(14, True),
        "sum_lbl": font(12, False),
        "sum_val": font(22, True),
        "sum_sub": font(11, False),
        "footer": font(12, False),
    }

    # ---- Accent ----
    if data["is_currently_mcwv"]:
        accent = (74, 222, 128)
    elif data["is_currently_rival"]:
        accent = (255, 84, 96)
    else:
        accent = (130, 100, 255)

    # ---- Background ----
    img = cover_image(MCWV_HOURLY_STATS_BG_PATH, (W, H))
    img.alpha_composite(Image.new("RGBA", (W, H), (3, 5, 16, 130)))

    # Faint grid
    grid = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    gd = ImageDraw.Draw(grid)
    for x in range(0, W, sc(50)):
        gd.line((x, 0, x, H), fill=(120, 140, 200, 10), width=sc(1))
    for y in range(0, H, sc(50)):
        gd.line((0, y, W, y), fill=(120, 140, 200, 7), width=sc(1))
    img.alpha_composite(grid)
    await asyncio.sleep(0)

    # ---- Helpers ----
    def panel(box, radius, fill, outline=None, width=1, blur=0):
        layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        ld = ImageDraw.Draw(layer)
        ld.rounded_rectangle(box, radius=sc(radius), fill=fill,
                              outline=outline, width=sc(width) if outline else 1)
        if blur:
            layer = layer.filter(ImageFilter.GaussianBlur(sc(blur)))
        img.alpha_composite(layer)

    def txt(xy, text, f, fill, shadow=True):
        d = ImageDraw.Draw(img)
        if shadow:
            d.text((xy[0] + sc(1), xy[1] + sc(1)), text, font=f, fill=(0, 0, 0, 110))
        d.text(xy, text, font=f, fill=fill)

    # ---- Card panel ----
    card = (sc(20), sc(20), W - sc(20), H - sc(20))
    panel((card[0]+sc(4), card[1]+sc(6), card[2]+sc(4), card[3]+sc(6)), 22, (0,0,0,130), blur=6)
    panel(card, 22, (12, 15, 28, 218), outline=(*accent, 100), width=2)
    panel((card[0]+sc(2), card[1]+sc(2), card[2]-sc(2), card[3]-sc(2)), 20, (255,255,255,0), outline=(255,255,255,18), width=1)
    await asyncio.sleep(0)

    # ---- Avatar ----
    AV_SIZE = sc(90)
    AV_X, AV_Y = MARGIN, sc(55)
    if avatar_url:
        try:
            global session
            if session is None or session.closed:
                session = aiohttp.ClientSession()
            async with session.get(avatar_url, timeout=aiohttp.ClientTimeout(total=8)) as res:
                if res.status == 200:
                    ab = await res.read()
                    av = Image.open(BytesIO(ab)).convert("RGBA").resize((AV_SIZE, AV_SIZE), Image.Resampling.LANCZOS)
                    mask = Image.new("L", (AV_SIZE, AV_SIZE), 0)
                    ImageDraw.Draw(mask).ellipse((0, 0, AV_SIZE-1, AV_SIZE-1), fill=255)
                    glow = Image.new("RGBA", (W, H), (0,0,0,0))
                    rg = ImageDraw.Draw(glow)
                    rg.ellipse((AV_X-sc(6), AV_Y-sc(6), AV_X+AV_SIZE+sc(6), AV_Y+AV_SIZE+sc(6)), fill=(*accent, 35))
                    img.alpha_composite(glow.filter(ImageFilter.GaussianBlur(sc(6))))
                    img.paste(av, (AV_X, AV_Y), mask)
                    d = ImageDraw.Draw(img)
                    d.ellipse((AV_X-sc(2), AV_Y-sc(2), AV_X+AV_SIZE+sc(2), AV_Y+AV_SIZE+sc(2)), outline=(*accent, 170), width=sc(2))
        except Exception:
            pass
    await asyncio.sleep(0)

    # ---- Name + ID ----
    NAME_X = AV_X + AV_SIZE + sc(20)
    txt((NAME_X, sc(55)), data["roblox_name"], F["name"], (255, 255, 255, 255))
    txt((NAME_X, sc(90)), f"ID: {data['roblox_id']}", F["sub"], (150, 160, 180, 255), shadow=False)

    # ---- Status badges ----
    bx = NAME_X
    by = sc(115)
    badges = []
    if data["is_currently_mcwv"]:
        badges.append("MCWV")
    elif data["is_currently_rival"]:
        badges.append(data["current_clan_name"])
    if data.get("total_battles_agg"):
        badges.append(f"{data['total_battles_agg']} battles")

    for btext in badges[:4]:
        d = ImageDraw.Draw(img)
        tw = d.textbbox((0, 0), btext, font=F["badge"])
        bw = tw[2] - tw[0]
        panel((bx, by, bx + bw + sc(18), by + sc(26)), 13, (20, 24, 38, 220), outline=(*accent, 80), width=1)
        d.text((bx + sc(9), by + sc(4)), btext, font=F["badge"], fill=(*accent, 255))
        bx += bw + sc(18) + sc(7)
    await asyncio.sleep(0)

    # ---- Clan history ----
    clans = data.get("clans_seen", [])[:8]
    if clans:
        d = ImageDraw.Draw(img)
        clan_text = "  \u2192  ".join(clans)
        clan_text = fit_text(d, clan_text, F["sub"], CONTENT_W)
        txt((MARGIN, sc(165)), "Clan History", F["section"], (160, 170, 195, 190), shadow=False)
        txt((MARGIN, sc(190)), clan_text, F["sub"], (210, 220, 245, 255))

    # ---- Separator ----
    d = ImageDraw.Draw(img)
    d.line((MARGIN, sc(230), RIGHT, sc(230)), fill=(70, 80, 110, 60), width=sc(1))

    # ---- War History header ----
    txt((MARGIN, sc(248)), f"War History  \u00b7  Page {page+1}/{total_pages}", F["section"], (195, 205, 235, 230))

    # ---- Battle rows ----
    y = sc(278)
    for row in page_rows:
        title = _friendly_battle_name(row.get("battleId") or row.get("title") or "?")
        clan = str(row.get("clan") or "?")
        pts = _safe_int(row.get("points"))
        rank = _safe_int(row.get("rank"))
        total_c = _safe_int(row.get("total")) or 0
        place = row.get("clanPlace")
        participated = row.get("participated_only")
        rh = RH_COMPACT if participated else RH_NORMAL

        # Row card
        panel((MARGIN, y, RIGHT, y + rh - sc(4)), 8, (18, 22, 36, 165))
        d = ImageDraw.Draw(img)

        if participated:
            t_short = fit_text(d, title, F["battle"], sc(390))
            d.text((C_NAME, y + sc(3)), t_short, font=F["battle"], fill=(170, 180, 205, 255))
            cc = (*accent, 255) if clan.upper() == CLAN_NAME.upper() else (130, 140, 165, 255)
            d.text((C_PTS, y + sc(5)), f"[{clan}]", font=F["stats"], fill=cc)
            d.text((C_RANK, y + sc(5)), "participated \u2713", font=F["stats"], fill=(110, 120, 140, 255))
        else:
            pct = float(row.get("betterThan") or ((total_c - rank) / total_c * 100 if rank > 0 and total_c > 1 else 0))
            if pct >= 80:
                bc = (74, 222, 128)
            elif pct >= 50:
                bc = (250, 200, 60)
            else:
                bc = (255, 100, 100)

            # Battle name
            t_short = fit_text(d, title, F["battle"], sc(400))
            d.text((C_NAME, y + sc(2)), t_short, font=F["battle"], fill=(235, 240, 255, 255))

            # Clan tag
            cc = (*accent, 255) if clan.upper() == CLAN_NAME.upper() else (130, 140, 165, 255)
            clan_str = f"[{clan}]"
            d.text((C_NAME, y + sc(22)), clan_str, font=F["stats"], fill=cc)

            # Points (aligned)
            d.text((C_PTS, y + sc(2)), format_points(pts), font=F["pct"], fill=(200, 210, 230, 255))

            # Rank (aligned)
            if rank > 0 and total_c > 1:
                d.text((C_RANK, y + sc(2)), f"#{rank:,}/{total_c:,}", font=F["stats"], fill=(170, 180, 205, 255))
            if place and place not in (None, "", 0):
                d.text((C_RANK, y + sc(22)), f"clan #{int(place)}", font=F["stats"], fill=(115, 125, 145, 255))

            # Percentile bar
            bar_y = y + sc(14)
            bar_h = sc(8)
            d.rounded_rectangle((C_BAR, bar_y, C_BAR + BAR_W, bar_y + bar_h), radius=sc(3), fill=(38, 42, 56, 255))
            fw = int(BAR_W * (pct / 100))
            if fw > 0:
                fi = Image.new("RGBA", (fw, bar_h), (0,0,0,0))
                fd2 = ImageDraw.Draw(fi)
                for bx2 in range(fw):
                    t2 = bx2 / max(fw - 1, 1)
                    s2 = 0.80 + 0.20 * t2
                    g2 = tuple(min(255, int(bc[i] * s2 + 12 * (1 - t2))) for i in range(3))
                    fd2.line((bx2, 0, bx2, bar_h), fill=(*g2, 255))
                fm = Image.new("L", fi.size, 0)
                ImageDraw.Draw(fm).rounded_rectangle((0, 0, fi.size[0]-1, fi.size[1]-1), radius=sc(3), fill=255)
                img.paste(fi, (C_BAR, bar_y), fm)
                d = ImageDraw.Draw(img)

            # Percentile text
            d.text((C_PCT, y + sc(11)), f"{pct:.1f}%", font=F["pct"], fill=(*bc, 255))

        y += rh
        await asyncio.sleep(0)

    # ---- Summary (last page only) ----
    if has_summary:
        rv = data["rank_values"]
        sp = sorted(rv, key=lambda r: r["pct"], reverse=True)
        best, worst = sp[0], sp[-1]
        avg = sum(r["pct"] for r in rv) / len(rv)

        d = ImageDraw.Draw(img)
        d.line((MARGIN, y, RIGHT, y), fill=(70, 80, 110, 60), width=sc(1))
        y += sc(10)

        col_w = CONTENT_W // 3
        items = [
            ("BEST", f"{best['pct']:.1f}%", f"#{best['rank']:,}/{best['total']:,}", (74, 222, 128)),
            ("AVERAGE", f"{avg:.1f}%", "better than global", (100, 180, 255)),
            ("WORST", f"{worst['pct']:.1f}%", f"#{worst['rank']:,}/{worst['total']:,}", (255, 100, 100)),
        ]
        for i, (lbl, val, sub, col) in enumerate(items):
            x = MARGIN + i * col_w
            bw = col_w - sc(8)
            panel((x, y, x + bw, y + sc(72)), 10, (20, 24, 38, 195), outline=(*col, 60), width=1)
            d = ImageDraw.Draw(img)
            d.text((x + sc(12), y + sc(6)), lbl, font=F["sum_lbl"], fill=(125, 135, 155, 255))
            txt((x + sc(12), y + sc(22)), val, F["sum_val"], (*col, 255))
            d.text((x + sc(12), y + sc(52)), sub, font=F["sum_sub"], fill=(145, 155, 175, 255))

    # ---- Footer ----
    d = ImageDraw.Draw(img)
    sources = []
    if data.get("cached_rows"):
        sources.append("cache")
    if data.get("scraped_clans"):
        sources.append("scrape")
    src = " + ".join(sources) if sources else "PS99"
    ft = f"{src}  \u00b7  {len(rows)} battles"
    if page == 0:
        ft += "  \u00b7  older wars may be partial"
    d.text((MARGIN, H - sc(25)), ft, font=F["footer"], fill=(135, 145, 170, 210))

    await asyncio.sleep(0)

    # ---- Output ----
    out_w = W // S
    out_h = H // S
    img = img.resize((out_w, out_h), Image.Resampling.LANCZOS)
    out = BytesIO()
    img.save(out, format="PNG")
    out.seek(0)
    return out



# ---------------- CHECKPLAYER COMMAND ----------------

# Recent username searches for autocomplete
RECENT_PLAYER_SEARCHES = []
MAX_RECENT_SEARCHES = 25


async def checkplayer_autocomplete(interaction: discord.Interaction, current: str):
    """Autocomplete for /checkplayer — shows recent searches + tracked members."""
    choices = []
    current_lower = current.lower().strip()

    # Recent searches first
    for name in RECENT_PLAYER_SEARCHES:
        if not current_lower or current_lower in name.lower():
            choices.append(app_commands.Choice(name=name, value=name))

    # Tracked members from DB
    try:
        users = db_get_all_tracked()
        for row in users:
            username = str(row[2]) if len(row) > 2 and row[2] else str(row[0])
            if not current_lower or current_lower in username.lower():
                if not any(c.value == username for c in choices):
                    choices.append(app_commands.Choice(name=username, value=username))
    except Exception:
        pass

    return choices[:25]


@bot.tree.command(name="checkplayer", description="Look up any player's complete cross-clan war history and performance", guild=guild_obj)
@app_commands.describe(roblox_username="Roblox username to investigate")
@app_commands.autocomplete(roblox_username=checkplayer_autocomplete)
@require_role()
async def checkplayer(interaction: discord.Interaction, roblox_username: str):
    await interaction.response.defer()

    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession()

    roblox_username = roblox_username.strip()
    if not re.fullmatch(r"[A-Za-z0-9_]{3,20}", roblox_username):
        return await interaction.followup.send(f"Invalid Roblox username `{roblox_username}`.")

    # Track recent search
    if roblox_username not in RECENT_PLAYER_SEARCHES:
        RECENT_PLAYER_SEARCHES.insert(0, roblox_username)
        del RECENT_PLAYER_SEARCHES[MAX_RECENT_SEARCHES:]

    try:
        resolved = await resolve_roblox_username(roblox_username)
        if not resolved:
            return await interaction.followup.send(f"Roblox user `{roblox_username}` not found.")

        roblox_id = resolved["id"]
        roblox_name = resolved["name"]

        # ===== GATHER DATA (instant — cache + 1-2 API calls) =====
        data = await gather_player_data(roblox_id, roblox_name)

        if not data["rows"] and not data["scraped_clans"]:
            return await interaction.followup.send(f"No war data found for `{roblox_name}`.")

        # ===== AVATAR =====
        avatar_url = None
        try:
            avatar_url = await get_roblox_headshot_url(roblox_id)
        except Exception:
            pass

        # ===== GENERATE IMAGE CARD (page 0) =====
        per_page = 7
        try:
            image = await generate_checkplayer_card(data, avatar_url, page=0, per_page=per_page)
            file = discord.File(image, filename="checkplayer-card.png")
        except Exception as exc:
            print(f"[checkplayer] image card failed: {exc}")
            file = None

        # ===== BUILD EMBED (page 0) =====
        embed = build_checkplayer_embed(data, page=0, per_page=per_page)
        if avatar_url:
            embed.set_thumbnail(url=avatar_url)
        if file:
            embed.set_image(url="attachment://checkplayer-card.png")

        # ===== SEND WITH PAGINATION =====
        rows = data["rows"]
        total_pages = max(1, (len(rows) + per_page - 1) // per_page)
        if total_pages > 1:
            view = CheckPlayerView(data, avatar_url=avatar_url, per_page=per_page)
            if file:
                await interaction.followup.send(embed=embed, file=file, view=view)
            else:
                await interaction.followup.send(embed=embed, view=view)
        else:
            if file:
                await interaction.followup.send(embed=embed, file=file)
            else:
                await interaction.followup.send(embed=embed)

    except Exception as e:
        print(f"[checkplayer] error: {e}")
        traceback.print_exc()
        await interaction.followup.send(f"Lookup failed: `{type(e).__name__}`")


# ---------------- COMPARE PLAYER COMMAND ----------------

@bot.tree.command(name="compareplayer", description="Compare two players' war history side-by-side", guild=guild_obj)
@app_commands.describe(player1="First Roblox username", player2="Second Roblox username")
@app_commands.autocomplete(player1=checkplayer_autocomplete, player2=checkplayer_autocomplete)
@require_role()
async def compareplayer(interaction: discord.Interaction, player1: str, player2: str):
    await interaction.response.defer()

    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession()

    p1 = player1.strip()
    p2 = player2.strip()
    if not re.fullmatch(r"[A-Za-z0-9_]{3,20}", p1) or not re.fullmatch(r"[A-Za-z0-9_]{3,20}", p2):
        return await interaction.followup.send("Invalid username(s). Use valid Roblox usernames (3-20 chars, letters/numbers/underscores).")

    try:
        # Resolve both
        r1, r2 = await asyncio.gather(
            resolve_roblox_username(p1),
            resolve_roblox_username(p2),
        )
        if not r1:
            return await interaction.followup.send(f"Roblox user `{p1}` not found.")
        if not r2:
            return await interaction.followup.send(f"Roblox user `{p2}` not found.")

        # Gather both (concurrent)
        d1, d2 = await asyncio.gather(
            gather_player_data(r1["id"], r1["name"]),
            gather_player_data(r2["id"], r2["name"]),
        )

        if not d1["rows"] and not d2["rows"]:
            return await interaction.followup.send(f"No war data found for either player.")

        # Build comparison embed
        embed = discord.Embed(
            title=f"\u2694\ufe0f {d1['roblox_name']} vs {d2['roblox_name']}",
            color=discord.Color(MCWV_BRAND_COLOR),
            timestamp=datetime.now(timezone.utc),
        )

        # Side-by-side stats
        def player_summary(d):
            parts = []
            if d["is_currently_mcwv"]:
                parts.append("\U0001f451 MCWV")
            elif d["is_currently_rival"]:
                parts.append(f"\U0001f575 {d['current_clan_name']}")
            if d["earned_medals_agg"]:
                parts.append(f"\U0001f4ca {d['total_battles_agg']} battles")
            return " | ".join(parts)

        embed.description = f"**{d1['roblox_name']}**\n{player_summary(d1)}\n\n**{d2['roblox_name']}**\n{player_summary(d2)}"

        # Battles comparison
        r1_battles = len(d1["rows"])
        r2_battles = len(d2["rows"])
        embed.add_field(name="\U0001f4ca Battles", value=f"**{d1['roblox_name']}**: {r1_battles}\n**{d2['roblox_name']}**: {r2_battles}", inline=True)

        # Best percentile
        def best_pct(d):
            if d["rank_values"]:
                return max(r["pct"] for r in d["rank_values"])
            return 0
        b1, b2 = best_pct(d1), best_pct(d2)
        winner = d1["roblox_name"] if b1 > b2 else (d2["roblox_name"] if b2 > b1 else "Tie")
        embed.add_field(name="\U0001f3c6 Best %", value=f"**{d1['roblox_name']}**: {b1:.1f}%\n**{d2['roblox_name']}**: {b2:.1f}%\nWinner: **{winner}**", inline=True)

        # Avg percentile
        def avg_pct(d):
            if d["rank_values"]:
                return sum(r["pct"] for r in d["rank_values"]) / len(d["rank_values"])
            return 0
        a1, a2 = avg_pct(d1), avg_pct(d2)
        embed.add_field(name="\U0001f4ca Avg %", value=f"**{d1['roblox_name']}**: {a1:.1f}%\n**{d2['roblox_name']}**: {a2:.1f}%", inline=True)

        # Total points
        def total_pts(d):
            return sum(_safe_int(r.get("points")) for r in d["rows"])
        t1, t2 = total_pts(d1), total_pts(d2)
        embed.add_field(name="\u2694\ufe0f Total Points", value=f"**{d1['roblox_name']}**: {format_points(t1)}\n**{d2['roblox_name']}**: {format_points(t2)}", inline=True)

        # Clan history comparison
        def clan_history(d):
            clans = d["clans_seen"][:6]
            return " \u2192 ".join(clans) if clans else "Unknown"
        embed.add_field(name="\U0001f4cb Clans", value=f"**{d1['roblox_name']}**: {clan_history(d1)}\n**{d2['roblox_name']}**: {clan_history(d2)}", inline=False)

        # Shared battles
        b1_ids = {str(r.get("battleId") or "").lower() for r in d1["rows"]}
        b2_ids = {str(r.get("battleId") or "").lower() for r in d2["rows"]}
        shared = b1_ids & b2_ids
        if shared:
            shared_lines = []
            for bid in sorted(shared, key=lambda b: -max(
                _safe_int(next((r.get("startTime") for r in d1["rows"] if str(r.get("battleId") or "").lower() == b), 0)),
                _safe_int(next((r.get("startTime") for r in d2["rows"] if str(r.get("battleId") or "").lower() == b), 0)),
            ))[:5]:
                r1_row = next((r for r in d1["rows"] if str(r.get("battleId") or "").lower() == bid), None)
                r2_row = next((r for r in d2["rows"] if str(r.get("battleId") or "").lower() == bid), None)
                if r1_row and r2_row:
                    title = _friendly_battle_name(bid)
                    p1_pts = format_points(_safe_int(r1_row.get("points")))
                    p2_pts = format_points(_safe_int(r2_row.get("points")))
                    p1_rank = f"#{_safe_int(r1_row.get('rank')):,}" if r1_row.get("rank") else "?"
                    p2_rank = f"#{_safe_int(r2_row.get('rank')):,}" if r2_row.get("rank") else "?"
                    winner_icon = "\u2705" if _safe_int(r1_row.get("points")) > _safe_int(r2_row.get("points")) else ("\U0001f534" if _safe_int(r2_row.get("points")) > _safe_int(r1_row.get("points")) else "\u2696")
                    shared_lines.append(f"**{title}**: {p1_pts} ({p1_rank}) vs {p2_pts} ({p2_rank}) {winner_icon}")
            if shared_lines:
                embed.add_field(name=f"\U0001f501 Shared Battles ({len(shared)})", value="\n".join(shared_lines), inline=False)

        embed.set_footer(text=f"Global cache \u00b7 {interaction.user}")
        await interaction.followup.send(embed=embed)

    except Exception as e:
        print(f"[compareplayer] error: {e}")
        traceback.print_exc()
        await interaction.followup.send(f"Comparison failed: `{type(e).__name__}`")



# ---------------- CROSS-CLAN HISTORY CACHE ----------------
# Permanently store every player's war contributions across all clans.
# Backfilled from PS99 public API + auto-cached at the end of each war.

CROSS_CLAN_CACHE_VERSION = 1


def ensure_cross_clan_members_table():
    """Create the all-time clan members table if it doesn't exist."""
    if not db_enabled():
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cross_clan_members (
                    id BIGSERIAL PRIMARY KEY,
                    roblox_id TEXT NOT NULL,
                    clan_name TEXT NOT NULL,
                    first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (roblox_id, clan_name)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS cross_clan_members_roblox_idx ON cross_clan_members (roblox_id)")
        conn.commit()
    except Exception as e:
        print("[cross-clan cache] members table creation failed:", e)
        conn.rollback()


def get_cached_clan_memberships(roblox_id):
    """Get all clans a player has been a member of (from website scrape).
    Uses a local connection to avoid race conditions."""
    if not DATABASE_URL:
        return []
    pass
    try:
        ensure_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT clan_name FROM cross_clan_members
                WHERE roblox_id = %s
                ORDER BY first_seen DESC
            """, (str(roblox_id),))
            return [str(row[0]) for row in cur.fetchall()]
    except Exception:
        return []
    finally:
        pass


def ensure_cross_clan_history_table():
    """Create the permanent cache table if it doesn't exist."""
    if not db_enabled():
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cross_clan_player_history (
                    id BIGSERIAL PRIMARY KEY,
                    roblox_id TEXT NOT NULL,
                    battle_id TEXT NOT NULL,
                    battle_name TEXT,
                    clan_name TEXT NOT NULL,
                    points BIGINT NOT NULL DEFAULT 0,
                    rank INTEGER,
                    total_contributors INTEGER,
                    clan_place INTEGER,
                    earned_medal BOOLEAN DEFAULT FALSE,
                    start_time BIGINT,
                    cached_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (roblox_id, battle_id, clan_name)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS cross_clan_history_roblox_idx
                ON cross_clan_player_history (roblox_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS cross_clan_history_battle_idx
                ON cross_clan_player_history (battle_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS cross_clan_history_clan_idx
                ON cross_clan_player_history (clan_name, battle_id)
            """)
        conn.commit()
    except Exception as e:
        print("[cross-clan cache] table creation failed:", e)
        conn.rollback()


async def scrape_clan_member_ids(clan_name):
    """Scrape db.biggames.io for a clan's FULL member list (all-time).
    Returns a set of Roblox user IDs. The public PS99 API only returns
    top 2-6 contributors per battle, but the website shows ALL members.
    This fetches the clan page and extracts user IDs from the RSC data."""
    if not clan_name:
        return set()
    try:
        global session
        if session is None or session.closed:
            session = aiohttp.ClientSession()
        url = f"https://db.biggames.io/clans/{clan_name}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15), headers={"User-Agent": "Mozilla/5.0"}) as res:
            if res.status != 200:
                return set()
            html = await res.text()
        # The RSC flight data contains user IDs as escaped quoted strings: \"12345678\"
        import re as _re
        ids = set()
        for match in _re.findall(r'\\"(\d{7,12})\\"', html):
            uid = int(match)
            if uid > 1000000:  # Filter out asset IDs and small numbers
                ids.add(str(uid))
        # Also try unescaped quotes (in case the page format differs)
        for match in _re.findall(r'"(\d{7,12})"', html):
            uid = int(match)
            if uid > 1000000:
                ids.add(str(uid))
        if ids:
            print(f"[cross-clan scrape] {clan_name}: found {len(ids)} member IDs from website")
        return ids
    except Exception as exc:
        print(f"[cross-clan scrape] failed for {clan_name}: {exc}")
        return set()


# Cache of scraped clan members so we don't re-fetch per battle
SCRAPED_CLAN_MEMBERS = {}


async def get_clan_member_ids(clan_name):
    """Get a clan's full member list, using cache to avoid re-fetching."""
    if clan_name in SCRAPED_CLAN_MEMBERS:
        return SCRAPED_CLAN_MEMBERS[clan_name]
    ids = await scrape_clan_member_ids(clan_name)
    SCRAPED_CLAN_MEMBERS[clan_name] = ids
    return ids


async def cache_battle_contributors(battle_id):
    """Fetch and cache ALL contributor data for a single battle.
    Uses its OWN DB connection so other loops closing the global conn
    can't interrupt the cache mid-write.
    Returns the number of rows cached."""
    if not battle_id or not DATABASE_URL:
        return 0

    # Open a dedicated connection for this battle cache operation.
    pass
    try:
        ensure_db_connection()
        conn.autocommit = True

        # Ensure the table exists on this connection
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cross_clan_player_history (
                    id BIGSERIAL PRIMARY KEY,
                    roblox_id TEXT NOT NULL,
                    battle_id TEXT NOT NULL,
                    battle_name TEXT,
                    clan_name TEXT NOT NULL,
                    points BIGINT NOT NULL DEFAULT 0,
                    rank INTEGER,
                    total_contributors INTEGER,
                    clan_place INTEGER,
                    earned_medal BOOLEAN DEFAULT FALSE,
                    start_time BIGINT,
                    cached_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (roblox_id, battle_id, clan_name)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS cross_clan_history_roblox_idx ON cross_clan_player_history (roblox_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS cross_clan_history_battle_idx ON cross_clan_player_history (battle_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS cross_clan_history_clan_idx ON cross_clan_player_history (clan_name, battle_id)")

        battle_name = _friendly_battle_name(battle_id)
        cached_count = 0

        global session
        if session is None or session.closed:
            session = aiohttp.ClientSession()

        # Source 1: v1/clans/battles/{battle_id} — top 200 players globally
        # Retry up to 3 times with a delay (PS99 API can rate-limit)
        v1_payload = None
        for attempt in range(3):
            v1_payload = await _ps99_json(f"{PS99_API}/v1/clans/battles/{battle_id}")
            if v1_payload is not None:
                break
            print(f"[cross-clan cache] {battle_id}: v1 fetch attempt {attempt+1} failed, retrying...")
            await asyncio.sleep(2)
        if v1_payload is None:
            print(f"[cross-clan cache] {battle_id}: v1 API returned no data after 3 tries — using clan scan only")
        v1_data = v1_payload.get("data", {}) if isinstance(v1_payload, dict) else {}
        v1_meta = v1_data.get("meta", {}) if isinstance(v1_data, dict) else {}
        v1_start = _safe_int(v1_meta.get("startTime"))
        v1_players = v1_data.get("topPlayers", []) if isinstance(v1_data, dict) else []
        v1_stats = v1_data.get("stats", {}) if isinstance(v1_data, dict) else {}
        total_contributors = _safe_int(v1_stats.get("totalContributors"))

        rows_to_cache = []

        for player in v1_players if isinstance(v1_players, list) else []:
            if not isinstance(player, dict):
                continue
            uid = str(player.get("userId") or player.get("UserID") or "").strip()
            if not uid:
                continue
            clan = player.get("clan", {}) if isinstance(player.get("clan"), dict) else {}
            clan_name = str(clan.get("name") or "Unknown").strip()
            pts = _safe_int(player.get("points"))
            rank = _safe_int(player.get("rank"))
            place = clan.get("place")
            place_int = int(place) if isinstance(place, (int, float)) and place else None
            medal = bool(player.get("earnedMedal") or player.get("medal"))
            rows_to_cache.append({
                "roblox_id": uid,
                "battle_id": str(battle_id),
                "battle_name": str(v1_meta.get("title") or battle_name),
                "clan_name": clan_name,
                "points": pts,
                "rank": rank if rank > 0 else None,
                "total_contributors": total_contributors if total_contributors > 0 else None,
                "clan_place": place_int,
                "earned_medal": medal,
                "start_time": v1_start if v1_start > 0 else None,
            })

        # Source 2: each top-100 clan's legacy PointContributions for this battle.
        # Done SEQUENTIALLY (one clan at a time) to avoid timeouts and rate limits.
        clan_names = await fetch_top_clan_names_for_history()
        clans_scanned = 0
        clans_failed = 0

        for clan_name in clan_names:
            try:
                payload = await fetch_legacy_clan_payload(clan_name)
                if not payload:
                    clans_failed += 1
                    continue
                battles = _legacy_clan_battles(payload)
                battle = battles.get(battle_id) or battles.get(str(battle_id))
                if not isinstance(battle, dict):
                    norm = normalize_hourly_battle_key(battle_id)
                    for bid, b in battles.items():
                        if normalize_hourly_battle_key(bid) == norm:
                            battle = b
                            break
                if not isinstance(battle, dict):
                    clans_scanned += 1
                    continue
                contribs = battle.get("PointContributions") or battle.get("pointContributions") or []
                if not isinstance(contribs, list):
                    clans_scanned += 1
                    continue
                clan_place = battle.get("Place") or battle.get("place")
                clan_place_int = int(clan_place) if isinstance(clan_place, (int, float)) and clan_place else None
                earned_medal = bool(battle.get("EarnedMedal") or battle.get("earnedMedal"))
                start_time = _safe_int(battle.get("StartTime") or battle.get("startTime"))
                ranked = sorted(contribs, key=lambda c: _safe_int(c.get("Points")), reverse=True)
                total = len(ranked)
                for idx, c in enumerate(ranked, start=1):
                    uid = str(c.get("UserID") or c.get("userId") or c.get("UserId") or "").strip()
                    if not uid:
                        continue
                    rows_to_cache.append({
                        "roblox_id": uid,
                        "battle_id": str(battle_id),
                        "battle_name": str(battle_name),
                        "clan_name": str(clan_name),
                        "points": _safe_int(c.get("Points")),
                        "rank": idx,
                        "total_contributors": total,
                        "clan_place": clan_place_int,
                        "earned_medal": earned_medal,
                        "start_time": start_time if start_time > 0 else None,
                    })
                clans_scanned += 1
            except Exception as exc:
                clans_failed += 1

        print(f"[cross-clan cache] {battle_id}: scanned {clans_scanned} clans ({clans_failed} failed)")

        # Source 3: Scrape db.biggames.io for FULL clan member lists.
        # Store as all-time clan membership (not per-battle, since the page
        # shows all members ever, not per-battle rosters).
        # This goes into a separate table so /checkplayer can show
        # "was in V1LN" even without per-battle data.
        try:
            ensure_cross_clan_members_table()
            scrape_added = 0
            for clan_name in clan_names:
                try:
                    member_ids = await get_clan_member_ids(clan_name)
                    if not member_ids:
                        continue
                    with conn.cursor() as cur:
                        for uid in member_ids:
                            cur.execute("""
                                INSERT INTO cross_clan_members (roblox_id, clan_name, first_seen)
                                VALUES (%s, %s, NOW())
                                ON CONFLICT (roblox_id, clan_name) DO NOTHING
                            """, (uid, clan_name))
                            scrape_added += cur.rowcount
                    conn.commit()
                    await asyncio.sleep(0)
                except Exception:
                    pass
            if scrape_added:
                print(f"[cross-clan cache] {battle_id}: +{scrape_added} all-time clan members from website scrape")
        except Exception as exc:
            print(f"[cross-clan cache] member scrape failed: {exc}")

        # Yield to event loop between battles
        await asyncio.sleep(0)

        # Deduplicate: keep the row with the highest points per (roblox_id, battle_id, clan_name)
        best_rows = {}
        for row in rows_to_cache:
            key = (row["roblox_id"], row["battle_id"], row["clan_name"])
            if key not in best_rows or row["points"] > best_rows[key]["points"]:
                best_rows[key] = row

        # Insert into DB (upsert — update if exists with higher points)
        if best_rows:
            with conn.cursor() as cur:
                for row in best_rows.values():
                    cur.execute("""
                        INSERT INTO cross_clan_player_history
                            (roblox_id, battle_id, battle_name, clan_name, points,
                             rank, total_contributors, clan_place, earned_medal, start_time)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (roblox_id, battle_id, clan_name)
                        DO UPDATE SET
                            points = GREATEST(cross_clan_player_history.points, EXCLUDED.points),
                            rank = CASE WHEN EXCLUDED.rank IS NOT NULL AND EXCLUDED.points >= cross_clan_player_history.points
                                        THEN EXCLUDED.rank ELSE cross_clan_player_history.rank END,
                            total_contributors = COALESCE(EXCLUDED.total_contributors, cross_clan_player_history.total_contributors),
                            clan_place = COALESCE(EXCLUDED.clan_place, cross_clan_player_history.clan_place),
                            earned_medal = cross_clan_player_history.earned_medal OR EXCLUDED.earned_medal,
                            battle_name = COALESCE(EXCLUDED.battle_name, cross_clan_player_history.battle_name),
                            start_time = COALESCE(EXCLUDED.start_time, cross_clan_player_history.start_time),
                            cached_at = NOW()
                    """, (
                        row["roblox_id"], row["battle_id"], row["battle_name"],
                        row["clan_name"], row["points"], row["rank"],
                        row["total_contributors"], row["clan_place"],
                        row["earned_medal"], row["start_time"],
                    ))
            conn.commit()
            cached_count = len(best_rows)

        print(f"[cross-clan cache] {battle_id}: cached {cached_count} contributor rows")
        return cached_count

    except Exception as e:
        print(f"[cross-clan cache] failed for {battle_id}: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        pass


async def get_all_battle_ids():
    """Collect all unique battle IDs from the top 100 clans' legacy data.
    Falls back to MCWV's own battles if the scan fails.
    Returns a list of battle IDs sorted by start time (oldest first)."""
    battle_ids = {}

    # Always start with MCWV's own battles (reliable fallback)
    try:
        mcwv_payload = await fetch_legacy_clan_payload(CLAN_NAME)
        if mcwv_payload:
            mcwv_battles = _legacy_clan_battles(mcwv_payload)
            for bid, b in mcwv_battles.items():
                if not isinstance(b, dict):
                    continue
                battle_id = str(b.get("BattleID") or b.get("battleId") or bid or "").strip()
                if battle_id:
                    start = _safe_int(b.get("StartTime") or b.get("startTime"))
                    battle_ids[battle_id] = start
            print(f"[cross-clan cache] MCWV fallback: found {len(battle_ids)} battles")
    except Exception as e:
        print(f"[cross-clan cache] MCWV fallback failed: {e}")

    # Then try the full top-100 scan for more battles
    try:
        clan_names = await fetch_top_clan_names_for_history()
        print(f"[cross-clan cache] scanning {len(clan_names)} clans for battle IDs")

        semaphore = asyncio.Semaphore(10)
        scanned_count = 0
        failed_count = 0

        async def fetch_battles(clan_name):
            nonlocal scanned_count, failed_count
            async with semaphore:
                payload = await fetch_legacy_clan_payload(clan_name)
                if not payload:
                    failed_count += 1
                    return {}
                scanned_count += 1
                battles = _legacy_clan_battles(payload)
                result = {}
                for bid, b in battles.items():
                    if not isinstance(b, dict):
                        continue
                    battle_id = str(b.get("BattleID") or b.get("battleId") or bid or "").strip()
                    if not battle_id:
                        continue
                    start = _safe_int(b.get("StartTime") or b.get("startTime"))
                    result[battle_id] = start
                return result

        results = await asyncio.gather(
            *(fetch_battles(cn) for cn in clan_names),
            return_exceptions=True,
        )

        for result in results:
            if isinstance(result, dict):
                for bid, start in result.items():
                    if bid not in battle_ids or start > battle_ids[bid]:
                        battle_ids[bid] = start

        print(f"[cross-clan cache] scan done: {scanned_count} clans scanned, {failed_count} failed, {len(battle_ids)} unique battles")
    except Exception as e:
        print(f"[cross-clan cache] top-100 scan failed (using MCWV fallback only): {e}")

    # Sort by start time (oldest first for backfill)
    sorted_ids = sorted(battle_ids.keys(), key=lambda bid: battle_ids.get(bid, 0))
    return sorted_ids


# Known battle IDs that exist in PS99 (hardcoded fallback so backfill
# doesn't depend on the top-100 scan working). Updated 2026-08-13.
KNOWN_BATTLE_IDS = [
    "NinjaBattle2026", "GummyBattle2026", "LunarBattle2026", "SoccerBattle2026",
    "Backrooms2026", "AngelBattle2026", "StarryBattle", "Spring2026",
    "LuckyChestBattle", "Christmas2025", "Turkey2025", "TrickOrTreat",
    "BlockPartyBattle", "StrengthBattle", "TowerBattle", "BasketballBattle",
    "BalloonCorgiBattle", "PoisonTurtleBattle", "PixelChickBattle", "AthenaBattle",
    "TieDyeBattle", "LuckyBattle", "CardBattle", "ValBattle", "CannonBattle",
    "SquidBattle", "NewYear2024", "YearEnd2024", "Christmas2024", "SantaBattle",
    "LineBattle", "HalloweenBattle", "CatchingBattle", "CrabBattle", "RngBattle",
    "MillionaireRunBattle", "GoodEvilBattle", "HackerBattle", "PrisonBattle",
    "GlitchBattle", "GoalBattleTwo", "GoalBattleOne", "AchBattle",
    "IndexBattle", "RaidBattle", "Christmas2023", "DecemberActiveHugePets",
]


async def backfill_cross_clan_history(interaction=None, battle_filter=None):
    """Backfill all historical battle data into the permanent cache.
    Uses a hardcoded list of known battle IDs + MCWV's own battles as the
    source (no dependency on the top-100 scan). Caches each battle one at a
    time with progress updates. Returns (battles_cached, total_rows)."""
    if not db_enabled():
        if interaction:
            await interaction.followup.send("Database is not available.", ephemeral=True)
        return 0, 0

    ensure_cross_clan_history_table()

    if battle_filter:
        count = await cache_battle_contributors(battle_filter)
        pass  # keep connection alive
        if interaction:
            await interaction.followup.send(f"Done! Cached **{count:,}** rows for {battle_filter}.", ephemeral=True)
        return 1, count

    # Use the hardcoded known battle IDs (reliable, no API dependency)
    battle_ids = list(KNOWN_BATTLE_IDS)

    # Also try to add any battles from MCWV's own data
    try:
        mcwv_payload = await fetch_legacy_clan_payload(CLAN_NAME)
        if mcwv_payload:
            mcwv_battles = _legacy_clan_battles(mcwv_payload)
            for bid, b in mcwv_battles.items():
                if isinstance(b, dict):
                    battle_id = str(b.get("BattleID") or b.get("battleId") or bid or "").strip()
                    if battle_id and battle_id not in battle_ids:
                        battle_ids.append(battle_id)
    except Exception:
        pass

    # Check which battles are already cached
    already_cached = set()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT battle_id FROM cross_clan_player_history")
            for row in cur.fetchall():
                already_cached.add(str(row[0]))
    except Exception:
        pass

    to_cache = [bid for bid in battle_ids if bid not in already_cached]
    if not to_cache:
        if interaction:
            await interaction.followup.send(f"All {len(battle_ids)} battles are already cached!", ephemeral=True)
        pass  # keep connection alive
        return 0, 0

    if interaction:
        await interaction.followup.send(
            f"Backfilling **{len(to_cache)}** battles (skipping {len(already_cached)} already cached)...\n"
            f"Each battle takes ~30-60s. I will update you after each one.",
            ephemeral=True,
        )

    total_rows = 0
    battles_cached = 0
    failed = []

    for i, bid in enumerate(to_cache, 1):
        try:
            rows = await cache_battle_contributors(bid)
            total_rows += rows
            battles_cached += 1

            if interaction:
                try:
                    await interaction.followup.send(
                        f"[{i}/{len(to_cache)}] **{bid}** — {rows:,} rows cached",
                        ephemeral=True,
                    )
                except Exception:
                    pass
        except Exception as exc:
            failed.append(bid)
            if interaction:
                try:
                    await interaction.followup.send(
                        f"[{i}/{len(to_cache)}] **{bid}** — failed: {exc}",
                        ephemeral=True,
                    )
                except Exception:
                    pass

        # No need to close DB between battles (Supabase)
        pass  # keep connection alive
        await asyncio.sleep(1)  # Small delay to avoid hammering the API

    if interaction:
        summary = f"\nBackfill complete! **{battles_cached}/{len(to_cache)}** battles cached, **{total_rows:,}** total rows."
        if failed:
            summary += f"\nFailed: {', '.join(failed)}"
        await interaction.followup.send(summary, ephemeral=True)

    print(f"[cross-clan cache] backfill done: {battles_cached} battles, {total_rows} rows, {len(failed)} failed")
    pass  # keep connection alive (Supabase has no compute hour limit)
    return battles_cached, total_rows


async def auto_cache_war_end(battle_id):
    """Auto-cache a war's data when it ends. Called from war_poll_loop.
    
    Triggers a full 50k clan scan via the sitemap so we capture ALL
    contributor data while it's still available (the API only returns
    full PointContributions for active/recently-ended battles).
    Runs in the background so the war-end announcement isn't delayed."""
    if not battle_id:
        return
    if not DATABASE_URL:
        return
    if GLOBAL_BACKFILL_RUNNING:
        print(f"[cross-clan cache] auto-cache skipped for {battle_id}: global backfill already running")
        return
    # Run the full scan in the background
    asyncio.create_task(_auto_cache_full_scan(battle_id))
    print(f"[cross-clan cache] auto-cache full scan queued for {battle_id}")


async def _auto_cache_full_scan(battle_id):
    """Background task: scan all 50k clans for one specific battle."""
    global GLOBAL_BACKFILL_RUNNING

    battle_id = str(battle_id)
    GLOBAL_BACKFILL_RUNNING = True
    scan_session = aiohttp.ClientSession()
    pass
    started = time.time()

    try:
        clan_names = await fetch_all_clan_names_from_sitemap(scan_session)
        if not clan_names:
            print(f"[auto-cache] no clans from sitemap for {battle_id}")
            return

        ensure_db_connection()
        conn.autocommit = False

        # Ensure table exists
        def _ensure():
            try:
                with conn.cursor() as cur:
                    cur.execute("""CREATE TABLE IF NOT EXISTS cross_clan_player_history (
                        id BIGSERIAL PRIMARY KEY, roblox_id TEXT NOT NULL, battle_id TEXT NOT NULL,
                        battle_name TEXT, clan_name TEXT NOT NULL, points BIGINT NOT NULL DEFAULT 0,
                        rank INTEGER, total_contributors INTEGER, clan_place INTEGER,
                        earned_medal BOOLEAN DEFAULT FALSE, start_time BIGINT,
                        cached_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        UNIQUE (roblox_id, battle_id, clan_name))""")
                    cur.execute("CREATE INDEX IF NOT EXISTS cross_clan_history_roblox_idx ON cross_clan_player_history (roblox_id)")
                    cur.execute("CREATE INDEX IF NOT EXISTS cross_clan_history_battle_idx ON cross_clan_player_history (battle_id)")
                conn.commit()
            except Exception:
                conn.rollback()
        await asyncio.to_thread(_ensure)

        CONCURRENCY = 8
        clans_with_data = 0
        total_contribs = 0
        pending_rows = []

        print(f"[auto-cache] scanning {len(clan_names)} clans for {battle_id}")

        for batch_start in range(0, len(clan_names), CONCURRENCY):
            batch = clan_names[batch_start:batch_start + CONCURRENCY]
            results = await asyncio.gather(
                *(_fetch_clan_contributions(scan_session, name) for name in batch),
                return_exceptions=True,
            )

            for rows in results:
                if isinstance(rows, Exception) or not rows:
                    continue
                # Filter to only this battle's contributions
                battle_rows = [r for r in rows if r[1] == battle_id]
                if battle_rows:
                    clans_with_data += 1
                    total_contribs += len(battle_rows)
                    pending_rows.extend(battle_rows)

            if len(pending_rows) >= 5000:
                await asyncio.to_thread(_insert_raw_contributions, conn, pending_rows)
                pending_rows.clear()

            if (batch_start // CONCURRENCY + 1) % 100 == 0:
                elapsed = time.time() - started
                print(f"[auto-cache] {batch_start+CONCURRENCY}/{len(clan_names)} clans, {total_contribs:,} contribs, {elapsed:.0f}s")
                await asyncio.sleep(0)

        if pending_rows:
            await asyncio.to_thread(_insert_raw_contributions, conn, pending_rows)
            pending_rows.clear()

        # Recompute ranks for this battle only
        def _rank_battle():
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM cross_clan_player_history WHERE battle_id = %s", (battle_id,))
                    actual_total = int(cur.fetchone()[0] or 0)
                    if actual_total == 0:
                        return 0
                    cur.execute("""
                        WITH ranked AS (
                            SELECT id, ROW_NUMBER() OVER (ORDER BY points DESC, roblox_id ASC) AS gr, %s AS gt
                            FROM cross_clan_player_history WHERE battle_id = %s
                        )
                        UPDATE cross_clan_player_history h SET rank = r.gr, total_contributors = r.gt
                        FROM ranked r WHERE h.id = r.id
                    """, (actual_total, battle_id))
                    updated = cur.rowcount
                conn.commit()
                return updated
            except Exception as exc:
                conn.rollback()
                print(f"[auto-cache] rank failed: {exc}")
                return 0

        ranked = await asyncio.to_thread(_rank_battle)
        elapsed = time.time() - started
        print(f"[auto-cache] {battle_id}: {clans_with_data} clans, {total_contribs:,} contribs, {ranked:,} ranked, {elapsed:.0f}s")

    except Exception as e:
        print(f"[auto-cache] FATAL: {e}")
        traceback.print_exc()
    finally:
        GLOBAL_BACKFILL_RUNNING = False
        await scan_session.close()
        pass


def get_cached_player_history(roblox_id):
    """Read a player's full cross-clan history from the permanent cache.
    Returns a list of dicts sorted by start time (most recent first).
    Uses a local connection to avoid race conditions with other loops
    that close the global conn."""
    if not DATABASE_URL:
        return []
    pass
    try:
        ensure_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT battle_id, battle_name, clan_name, points, rank,
                       total_contributors, clan_place, earned_medal, start_time
                FROM cross_clan_player_history
                WHERE roblox_id = %s
                ORDER BY start_time DESC NULLS LAST, battle_id DESC
            """, (str(roblox_id),))
            rows = cur.fetchall()
        return [
            {
                "battleId": r[0],
                "title": r[1] or _friendly_battle_name(r[0]),
                "clan": r[2],
                "points": int(r[3] or 0),
                "rank": int(r[4]) if r[4] else None,
                "total": int(r[5]) if r[5] else None,
                "clanPlace": r[6],
                "earnedMedal": bool(r[7]),
                "startTime": int(r[8]) if r[8] else 0,
            }
            for r in rows
        ]
    except Exception as e:
        print(f"[cross-clan cache] player history read failed: {e}")
        return []
    finally:
        pass


def get_cached_battle_stats(battle_id):
    """Get stats for a cached battle — how many clans and players we have."""
    if not db_enabled():
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(DISTINCT clan_name) as clans,
                       COUNT(*) as players,
                       COUNT(DISTINCT roblox_id) as unique_players
                FROM cross_clan_player_history
                WHERE battle_id = %s
            """, (str(battle_id),))
            row = cur.fetchone()
        if row:
            return {"clans": int(row[0] or 0), "rows": int(row[1] or 0), "uniquePlayers": int(row[2] or 0)}
    except Exception:
        pass
    return None


# Hardcoded top-100 clan names as fallback (updated 2026-08-13).
# Used when the PS99 API is unreachable from Render.
HARDCODED_TOP_100_CLANS = [
    "UN0", "SOPU", "C0LD", "RFIL", "H8M3", "GSTG", "0RBI", "XB0X",
    "_MGW", "BOSS", "ERX", "POPS", "V1LN", "DGSL", "KOHV", "Y0R",
    "OLDD", "H8ER", "M2NY", "MCWV", "VLP", "BEZE", "R2FF", "D0S",
    "GANG", "R0W", "S7PY", "ER2X", "FMLY", "KRR", "LXCC", "TPGD",
    "SULS", "YLW", "NUUP", "CITC", "GDSQ", "ANG_", "UNTY", "AWZY",
    "HSTL", "EDS1", "L1GO", "1PNK", "M4RB", "FNXX", "BOSK", "JJ07",
    "212", "WMSY", "GCEM", "H8R2", "SVLW", "LSQ", "KR7X", "L2BR",
    "AGZY", "J200", "7476", "IDNZ", "AX0C", "K0I7", "OT3R", "R4YO",
    "ILDK", "DV_", "AX0K", "VNO1", "0FCL", "BYRD", "LX2C", "IFTM",
    "WRCK", "TSMU", "BETP", "GST2", "NDSS",
]


@bot.tree.command(name="scrape_members", description="Scrape db.biggames.io for ALL member lists across top 100 clans", guild=guild_obj)
@require_role()
async def scrape_members_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    if not DATABASE_URL:
        return await interaction.followup.send("Database is not available.", ephemeral=True)

    # Everything runs in a background task so the bot NEVER blocks.
    # Progress is sent via followup at safe intervals.
    progress_msg = await interaction.followup.send(
        "Scraping **77** clan pages from db.biggames.io...\n"
        "This runs in the background - the bot stays fully responsive.\n"
        "I will update you with progress.",
        ephemeral=True,
        wait=True,
    )

    async def _run_scrape():
        """Background scrape task — fully isolated from the bot's event loop."""
        # Fresh aiohttp session (don't touch the global one)
        scrape_session = aiohttp.ClientSession()
        pass
        total_members = 0
        clans_done = 0
        clans_failed = 0
        failed_clans = []

        try:
            ensure_db_connection()
            conn.autocommit = True

            # Ensure table exists + migrate
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS cross_clan_members (
                        id BIGSERIAL PRIMARY KEY,
                        roblox_id TEXT NOT NULL,
                        clan_name TEXT NOT NULL,
                        first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        last_checked TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        UNIQUE (roblox_id, clan_name)
                    )
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS cross_clan_members_roblox_idx ON cross_clan_members (roblox_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS cross_clan_members_clan_idx ON cross_clan_members (clan_name)")
                cur.execute("ALTER TABLE cross_clan_members ADD COLUMN IF NOT EXISTS last_checked TIMESTAMPTZ NOT NULL DEFAULT NOW()")

            clan_names = list(HARDCODED_TOP_100_CLANS)
            print(f"[scrape_members] Starting scrape of {len(clan_names)} clans")

            for i, clan_name in enumerate(clan_names, 1):
                try:
                    # Fetch the clan page
                    url = f"https://db.biggames.io/clans/{clan_name}"
                    async with scrape_session.get(
                        url,
                        timeout=aiohttp.ClientTimeout(total=20),
                        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"},
                    ) as res:
                        if res.status != 200:
                            clans_failed += 1
                            failed_clans.append(clan_name)
                            print(f"[scrape_members] {clan_name}: HTTP {res.status}")
                            continue
                        html = await res.text()

                    # Extract user IDs from the RSC data
                    ids = set()
                    for match in re.findall(r'\\"(\d{7,12})\"', html):
                        try:
                            uid = int(match)
                            if uid > 1000000:
                                ids.add(str(match))
                        except Exception:
                            pass
                    for match in re.findall(r'"(\d{7,12})"', html):
                        try:
                            uid = int(match)
                            if uid > 1000000:
                                ids.add(str(match))
                        except Exception:
                            pass

                    if ids:
                        # Batch insert (500 at a time) with event loop yields
                        id_list = list(ids)
                        for batch_start in range(0, len(id_list), 500):
                            batch = id_list[batch_start:batch_start + 500]
                            # Use parameterized values to avoid SQL injection
                            args = [(uid, clan_name) for uid in batch]
                            # Build VALUES clause safely
                            values_clause = ",".join(f"(%s,%s,NOW(),NOW())" for _ in batch)
                            flat_args = []
                            for uid, cn in args:
                                flat_args.extend([uid, cn])
                            with conn.cursor() as cur:
                                cur.execute(
                                    f"""INSERT INTO cross_clan_members (roblox_id, clan_name, first_seen, last_checked)
                                    VALUES {values_clause}
                                    ON CONFLICT (roblox_id, clan_name)
                                    DO UPDATE SET last_checked = NOW()""",
                                    flat_args,
                                )
                            conn.commit()
                            await asyncio.sleep(0)  # CRITICAL: yield to event loop

                        total_members += len(ids)
                        clans_done += 1
                        print(f"[scrape_members] {clan_name}: {len(ids)} members")
                    else:
                        clans_failed += 1
                        failed_clans.append(clan_name)
                        print(f"[scrape_members] {clan_name}: no IDs found")

                    # Progress update every 10 clans
                    if i % 10 == 0 or i == len(clan_names):
                        try:
                            await progress_msg.edit(
                                content=f"Scraping clan pages... **[{i}/{len(clan_names)}]**\n"
                                f"Done: **{clans_done}** | Failed: **{clans_failed}** | IDs: **{total_members:,}**"
                            )
                        except Exception:
                            pass

                    # Delay between clans (avoid rate limiting)
                    await asyncio.sleep(0.5)

                except asyncio.TimeoutError:
                    clans_failed += 1
                    failed_clans.append(clan_name)
                    print(f"[scrape_members] {clan_name}: timeout")
                except Exception as exc:
                    clans_failed += 1
                    failed_clans.append(clan_name)
                    print(f"[scrape_members] {clan_name}: {type(exc).__name__}: {exc}")

            # Final update
            summary = (
                f"Scrape complete!\n"
                f"Clans: **{clans_done}/{len(clan_names)}** ({clans_failed} failed)\n"
                f"Total member IDs: **{total_members:,}**\n"
            )
            if failed_clans:
                summary += f"Failed: {', '.join(failed_clans[:10])}"
            summary += f"\nUse `/checkplayer <username>` to see full cross-clan history."

            try:
                await progress_msg.edit(content=summary)
            except Exception:
                pass

            print(f"[scrape_members] DONE: {clans_done} clans, {total_members} members, {clans_failed} failed")

        except Exception as e:
            print(f"[scrape_members] FATAL: {e}")
            traceback.print_exc()
            try:
                await progress_msg.edit(content=f"Scrape failed: `{type(e).__name__}`")
            except Exception:
                pass
        finally:
            await scrape_session.close()
            pass

    # Run as a background task so the bot stays fully responsive
    asyncio.create_task(_run_scrape())


# ---------------- GLOBAL BACKFILL (all clans from sitemap) ----------------
# Scans every clan on db.biggames.io (50k+) via the legacy PS99 API, extracts
# PointContributions for every battle, and rebuilds the cross_clan_player_history
# table with TRUE global ranks + totalContributors — same data CW Bot has.
#
# Two-pass design:
#   Pass 1: Fetch all clans concurrently, INSERT raw contributions (rank=NULL)
#   Pass 2: SQL window function computes global rank + total per battle

GLOBAL_BACKFILL_RUNNING = False


async def fetch_all_clan_names_from_sitemap(scan_session=None):
    """Fetch every clan name from the db.biggames.io sitemap.
    Uses the provided scan_session (dedicated to the backfill) to avoid
    race conditions with the global session that other loops close."""
    own_session = False
    if scan_session is None or getattr(scan_session, "closed", True):
        scan_session = aiohttp.ClientSession()
        own_session = True
    try:
        async with scan_session.get(
            "https://db.biggames.io/sitemap.xml?sub=clans",
            headers={"User-Agent": "MCWV-Bot/1.0"},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as res:
            if res.status != 200:
                print(f"[global backfill] sitemap HTTP {res.status}")
                return []
            text = await res.text()
        names = re.findall(r"/clans/([^<]+)", text)
        cleaned = []
        seen = set()
        for name in names:
            name = name.strip()
            if name and name not in seen:
                seen.add(name)
                cleaned.append(name)
        print(f"[global backfill] sitemap: {len(cleaned)} clans")
        return cleaned
    except Exception as exc:
        print(f"[global backfill] sitemap fetch failed: {exc}")
        traceback.print_exc()
        return []
    finally:
        if own_session:
            await scan_session.close()


def _insert_raw_contributions(conn, rows):
    """Batch-insert raw contribution rows using execute_values (one INSERT, not N).
    Called via asyncio.to_thread() so it never blocks the event loop."""
    if not rows:
        return 0
    try:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO cross_clan_player_history
                    (roblox_id, battle_id, battle_name, clan_name, points,
                     clan_place, earned_medal, start_time)
                VALUES %s
                ON CONFLICT (roblox_id, battle_id, clan_name)
                DO UPDATE SET
                    points = GREATEST(cross_clan_player_history.points, EXCLUDED.points),
                    clan_place = COALESCE(EXCLUDED.clan_place, cross_clan_player_history.clan_place),
                    earned_medal = cross_clan_player_history.earned_medal OR EXCLUDED.earned_medal,
                    start_time = COALESCE(EXCLUDED.start_time, cross_clan_player_history.start_time),
                    cached_at = NOW()
            """, rows, page_size=1000)
        conn.commit()
        return len(rows)
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[global backfill] insert failed: {exc}")
        return 0


def _compute_global_ranks_sql(conn):
    """Pass 2: compute true global rank + total per battle.
    Done per-battle in a loop to avoid statement timeouts on large tables."""
    try:
        with conn.cursor() as cur:
            # Get all battle IDs and their actual row counts
            cur.execute("""
                SELECT battle_id, COUNT(*) as actual_total
                FROM cross_clan_player_history
                GROUP BY battle_id
            """)
            battles = cur.fetchall()

        print(f"[global backfill] computing ranks for {len(battles)} battles...")
        total_updated = 0

        for battle_id, actual_total in battles:
            try:
                with conn.cursor() as cur:
                    # Window function per battle — small, fast, no timeout
                    cur.execute("""
                        WITH ranked AS (
                            SELECT id,
                                   ROW_NUMBER() OVER (
                                       ORDER BY points DESC, roblox_id ASC
                                   ) AS global_rank,
                                   %s AS global_total
                            FROM cross_clan_player_history
                            WHERE battle_id = %s
                        )
                        UPDATE cross_clan_player_history h
                        SET rank = r.global_rank,
                            total_contributors = r.global_total
                        FROM ranked r
                        WHERE h.id = r.id
                    """, (actual_total, battle_id))
                    updated = cur.rowcount
                conn.commit()
                total_updated += updated if updated > 0 else 0
            except Exception as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                print(f"[global backfill] rank failed for {battle_id}: {exc}")

        print(f"[global backfill] rank computation complete: {total_updated:,} rows updated across {len(battles)} battles")
        return total_updated
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[global backfill] rank computation FAILED: {exc}")
        traceback.print_exc()
        return 0


@bot.tree.command(name="backfill_global", description="Scan ALL clans (50k+) for true global battle ranks — takes ~30min, runs in background", guild=guild_obj)
@require_role()
async def backfill_global_cmd(interaction: discord.Interaction):
    global GLOBAL_BACKFILL_RUNNING
    await interaction.response.defer(ephemeral=True)

    if GLOBAL_BACKFILL_RUNNING:
        return await interaction.followup.send("Global backfill is already running. Check logs for progress.", ephemeral=True)

    if not DATABASE_URL:
        return await interaction.followup.send("Database is not available.", ephemeral=True)

    GLOBAL_BACKFILL_RUNNING = True
    await interaction.followup.send(
        "Starting global backfill... This scans all 50k+ clans from the sitemap and may take ~30 minutes.\n"
        "The bot stays fully responsive. Progress is logged to console.\n"
        "When done, `/checkplayer` will show true global ranks.",
        ephemeral=True,
    )
    task = asyncio.create_task(_run_global_backfill(interaction.channel_id))
    def _log_backfill_exception(t):
        if t.cancelled():
            return
        exc = t.exception()
        if exc:
            print(f"[global backfill] TASK CRASHED: {exc}")
            traceback.print_exc()
    task.add_done_callback(_log_backfill_exception)


@bot.tree.command(name="backfill_status", description="Check if the global backfill is running and see cache stats", guild=guild_obj)
@require_role()
async def backfill_status_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    running = GLOBAL_BACKFILL_RUNNING
    if not db_enabled():
        return await interaction.followup.send(f"Backfill running: **{'yes' if running else 'no'}**\nDatabase not available for stats.", ephemeral=True)

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(DISTINCT battle_id), COUNT(DISTINCT roblox_id), COUNT(*) FROM cross_clan_player_history")
            battles, players, rows = cur.fetchone()
            cur.execute("SELECT battle_id, total_contributors FROM cross_clan_player_history ORDER BY total_contributors DESC NULLS LAST LIMIT 5")
            top = cur.fetchall()
        pass  # keep connection alive

        lines = [f"Backfill running: **{'yes' if running else 'no'}**"]
        lines.append(f"Battles cached: **{battles}**")
        lines.append(f"Unique players: **{players:,}**")
        lines.append(f"Total rows: **{rows:,}**")
        if top:
            lines.append("\n**Top battles by contributor count:**")
            for bid, total in top:
                lines.append(f"  {bid}: {total:,} contributors" if total else f"  {bid}: (no rank data)")
        await interaction.followup.send("\n".join(lines), ephemeral=True)
    except Exception as e:
        pass  # keep connection alive
        await interaction.followup.send(f"Backfill running: **{'yes' if running else 'no'}**\nStats error: `{e}`", ephemeral=True)


@bot.tree.command(name="recompute_ranks", description="Recompute global ranks from cached data (no re-scan needed)", guild=guild_obj)
@require_role()
async def recompute_ranks_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    if not DATABASE_URL:
        return await interaction.followup.send("Database is not available.", ephemeral=True)

    await interaction.followup.send("Recomputing global ranks... This takes a few seconds.", ephemeral=True)

    def _do_recompute():
        ensure_db_connection()
        conn.autocommit = False
        try:
            ranked = _compute_global_ranks_sql(conn)
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(DISTINCT battle_id) FROM cross_clan_player_history WHERE rank IS NOT NULL")
                battles_with_ranks = int(cur.fetchone()[0] or 0)
                cur.execute("SELECT battle_id, total_contributors FROM cross_clan_player_history ORDER BY total_contributors DESC NULLS LAST LIMIT 5")
                top = cur.fetchall()
            conn.commit()
            return ranked, battles_with_ranks, top
        finally:
            try:
                pass
            except Exception:
                pass

    try:
        ranked, battles_with_ranks, top = await asyncio.to_thread(_do_recompute)
        lines = [f"Rank computation complete! **{ranked:,}** rows updated."]
        lines.append(f"Battles with rank data: **{battles_with_ranks}**")
        if top:
            lines.append("\n**Top battles by contributor count:**")
            for bid, total in top:
                lines.append(f"  {bid}: {total:,} contributors" if total else f"  {bid}: (no rank data)")
        lines.append("\n`/checkplayer` should now show correct global ranks.")
        try:
            ch = bot.get_channel(interaction.channel_id)
            if ch is None:
                ch = await bot.fetch_channel(interaction.channel_id)
            if ch:
                await ch.send("\n".join(lines))
        except Exception:
            pass
        await interaction.followup.send("\n".join(lines), ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"Rank computation failed: `{type(e).__name__}: {e}`", ephemeral=True)


@bot.tree.command(name="fix_battle_times", description="Fetch start times for all cached battles from the v1 API", guild=guild_obj)
@require_role()
async def fix_battle_times_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    if not DATABASE_URL:
        return await interaction.followup.send("Database is not available.", ephemeral=True)

    await interaction.followup.send("Fetching battle start times from v1 API...", ephemeral=True)

    scan_session = aiohttp.ClientSession()
    try:
        # Get all distinct battle IDs from the cache
        ensure_db_connection()
        conn.autocommit = False

        def _get_battle_ids():
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT battle_id FROM cross_clan_player_history")
                return [str(row[0]) for row in cur.fetchall()]

        battle_ids = await asyncio.to_thread(_get_battle_ids)
        print(f"[fix_battle_times] {len(battle_ids)} battles to update")

        def _get_manual_battles():
            with conn.cursor() as cur:
                cur.execute("SELECT battle_id FROM battles WHERE manually_edited = TRUE")
                return {str(r[0]) for r in cur.fetchall()}

        manual_battles = await asyncio.to_thread(_get_manual_battles)

        updated = 0
        failed = 0
        skipped = 0
        for bid in battle_ids:
            if bid in manual_battles:
                skipped += 1
                continue
            try:
                async with scan_session.get(
                    f"{PS99_API}/v1/clans/battles/{bid}",
                    headers={"User-Agent": "MCWV-Bot/1.0"},
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as res:
                    if res.status != 200:
                        failed += 1
                        continue
                    payload = await res.json(content_type=None)

                meta = payload.get("data", {}).get("meta", {}) if isinstance(payload, dict) else {}
                start_time = _safe_int(meta.get("startTime"))
                title = str(meta.get("title") or bid)

                if start_time and start_time > 0:
                    def _update_battle():
                        try:
                            with conn.cursor() as cur:
                                cur.execute("""
                                    UPDATE cross_clan_player_history
                                    SET start_time = %s, battle_name = %s
                                    WHERE battle_id = %s AND start_time IS NULL
                                """, (start_time, title, bid))
                                return cur.rowcount
                            conn.commit()
                        except Exception:
                            conn.rollback()
                            return 0
                    rows_updated = await asyncio.to_thread(_update_battle)
                    updated += rows_updated
                else:
                    failed += 1

                await asyncio.sleep(0.3)
            except Exception as exc:
                failed += 1

        summary = f"Battle times updated! **{updated:,}** rows updated, {failed} battles failed (no v1 data), {skipped} skipped (manual override)."
        print(f"[fix_battle_times] {summary}")

        try:
            ch = bot.get_channel(interaction.channel_id)
            if ch is None:
                ch = await bot.fetch_channel(interaction.channel_id)
            if ch:
                await ch.send(summary)
        except Exception:
            pass
        await interaction.followup.send(summary, ephemeral=True)

        pass
    except Exception as e:
        await interaction.followup.send(f"Failed: `{type(e).__name__}: {e}`", ephemeral=True)
    finally:
        await scan_session.close()


@bot.tree.command(name="backfill_history", description="Backfill all historical war data into the permanent cross-clan cache", guild=guild_obj)
@app_commands.describe(battle_name="Optional: only backfill a specific battle (e.g. NinjaBattle2026)")
@require_role()
async def backfill_history(interaction: discord.Interaction, battle_name: str = None):
    await interaction.response.defer(ephemeral=True)

    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession()

    if not db_enabled():
        return await interaction.followup.send("Database is not available.", ephemeral=True)

    battle_filter = battle_name.strip() if battle_name else None
    await interaction.followup.send(
        f"Starting backfill{' for ' + battle_filter if battle_filter else ' of all historical battles'}... This may take a few minutes.",
        ephemeral=True,
    )

    battles, rows = await backfill_cross_clan_history(interaction=interaction, battle_filter=battle_filter)
    pass  # keep connection alive (Supabase has no compute hour limit)


@bot.tree.command(name="cachewar", description="Cache a specific war's data into the permanent cross-clan cache", guild=guild_obj)
@app_commands.describe(battle_id="Battle ID to cache (e.g. NinjaBattle2026)")
@require_role()
async def cachewar(interaction: discord.Interaction, battle_id: str):
    await interaction.response.defer(ephemeral=True)

    if not db_enabled():
        return await interaction.followup.send("Database is not available.", ephemeral=True)

    battle_id = battle_id.strip()
    count = await cache_battle_contributors(battle_id)
    pass  # keep connection alive (Supabase has no compute hour limit)

    if count > 0:
        stats = get_cached_battle_stats(battle_id)
        stats_txt = ""
        if stats:
            stats_txt = f"\n{stats['clans']} clans | {stats['uniquePlayers']} unique players | {stats['rows']} total rows"
        await interaction.followup.send(f"Cached **{battle_id}** — {count:,} contributor rows.{stats_txt}", ephemeral=True)
    else:
        await interaction.followup.send(f"No data found for `{battle_id}`. Check the battle ID.", ephemeral=True)


@bot.tree.command(name="cachedbstats", description="Show stats for the cross-clan history cache", guild=guild_obj)
@require_role()
async def cachedbstats(interaction: discord.Interaction):
    if not db_enabled():
        return await interaction.response.send_message("Database is not available.", ephemeral=True)

    ensure_cross_clan_history_table()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM cross_clan_player_history")
            total_rows = int(cur.fetchone()[0] or 0)
            cur.execute("SELECT COUNT(DISTINCT roblox_id) FROM cross_clan_player_history")
            unique_players = int(cur.fetchone()[0] or 0)
            cur.execute("SELECT COUNT(DISTINCT battle_id) FROM cross_clan_player_history")
            unique_battles = int(cur.fetchone()[0] or 0)
            cur.execute("SELECT COUNT(DISTINCT clan_name) FROM cross_clan_player_history")
            unique_clans = int(cur.fetchone()[0] or 0)
            cur.execute("""
                SELECT battle_id, battle_name, COUNT(*) as rows, COUNT(DISTINCT clan_name) as clans
                FROM cross_clan_player_history
                GROUP BY battle_id, battle_name
                ORDER BY MAX(start_time) DESC NULLS LAST
                LIMIT 10
            """)
            recent = cur.fetchall()

        embed = discord.Embed(
            title="Cross-Clan Cache Stats",
            color=discord.Color(MCWV_BRAND_COLOR),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="Total Rows", value=f"**{total_rows:,}**", inline=True)
        embed.add_field(name="Unique Players", value=f"**{unique_players:,}**", inline=True)
        embed.add_field(name="Battles", value=f"**{unique_battles}**", inline=True)
        embed.add_field(name="Clans", value=f"**{unique_clans}**", inline=True)

        if recent:
            lines = []
            for r in recent:
                lines.append(f"**{r[1] or r[0]}** — {r[2]} rows, {r[3]} clans")
            embed.add_field(name="Recent Cached Battles", value="\n".join(lines), inline=False)

        embed.set_footer(text="Use /backfill_history to cache more battles")
        await interaction.response.send_message(embed=embed, ephemeral=True)
        pass  # keep connection alive

    except Exception as e:
        await interaction.response.send_message(f"Error: `{e}`", ephemeral=True)



@bot.tree.command(name="ticket_panel_send", description="Send the MCWV application ticket panel", guild=guild_obj)
@app_commands.describe(
    channel="Channel to send the panel in. Defaults to configured panel channel.",
    title="Panel title",
    description="Panel description",
    button_label="Text on the application button",
    hex_color="Embed colour as a hex value, for example #34D399",
    thumbnail_url="Optional HTTPS thumbnail image URL"
)
async def ticket_panel_send(
    interaction: discord.Interaction,
    channel: discord.TextChannel = None,
    title: str = "MCWV Applications",
    description: str = "Ready to apply for MCWV? Open a private application ticket below.",
    button_label: str = "Open Application",
    hex_color: str = None,
    thumbnail_url: str = None,
):
    if not has_mcwv_ticket_staff_permission(interaction.user):
        return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
    target_channel = channel or interaction.guild.get_channel(MCWV_TICKET_PANEL_CHANNEL_ID)
    if not isinstance(target_channel, discord.TextChannel):
        return await interaction.response.send_message("❌ Ticket panel channel is not configured correctly.", ephemeral=True)
    settings_panel = get_mcwv_ticket_settings().get("panel", {})
    embed = discord.Embed(
        title=str(title or "MCWV Applications")[:256],
        description=str(description or "Ready to apply for MCWV? Open a private application ticket below.")[:4000],
        color=discord.Color(parse_hex_color(hex_color, settings_panel.get("accentColor", 0x34D399))),
        timestamp=datetime.now(timezone.utc),
    )
    thumbnail = str(thumbnail_url or settings_panel.get("thumbnailUrl") or "").strip()[:2048]
    if thumbnail.startswith("https://"):
        embed.set_thumbnail(url=thumbnail)
    embed.set_footer(text="MCWV Applications")
    await target_channel.send(embed=embed, view=MCWVTicketPanelView(button_label))
    await interaction.response.send_message(f"✅ Ticket panel sent in {target_channel.mention}.", ephemeral=True)


@bot.tree.command(name="ticket_review_restore", description="Re-send any missing application review cards into their ticket channels", guild=guild_obj)
async def ticket_review_restore(interaction: discord.Interaction):
    if not has_mcwv_ticket_staff_permission(interaction.user):
        return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    restored = await restore_application_review_messages(interaction.guild)
    await interaction.followup.send(f"✅ Done — {restored} review card(s) re-sent into ticket channels.", ephemeral=True)


@bot.tree.command(name="reject", description="Reject this application and close the ticket", guild=guild_obj)
@app_commands.describe(reason="Why the application is being rejected (optional — defaults to 'Rejected')")
async def reject_application(interaction: discord.Interaction, reason: str = None):
    if not interaction.guild:
        return await interaction.response.send_message("❌ This can only be used in the server.", ephemeral=True)
    if not has_mcwv_ticket_staff_permission(interaction.user):
        return await interaction.response.send_message("❌ Staff only.", ephemeral=True)

    channel = interaction.channel
    channel_id = channel.id
    topic = channel.topic or ""
    
    print(f"[reject] channel_id={channel_id}, topic={topic!r}, channel_name={channel.name}")

    # Try channel_id first
    row = db_get_ticket_by_channel(channel_id)
    if row:
        print(f"[reject] found by channel_id: ticket_id={row[0]}")
    
    # Fall back to ticket_id from channel topic
    if not row and topic:
        ticket_match = re.search(r'app-\d+', topic)
        if ticket_match:
            found_id = ticket_match.group(0)
            print(f"[reject] trying ticket_id from topic: {found_id}")
            row = db_get_ticket_by_ticket_id(found_id)
            if row:
                print(f"[reject] found by ticket_id: {found_id}")
        else:
            # Try mcwv-ticket-owner pattern
            owner_match = re.search(r'mcwv-ticket-owner:(\d+)', topic)
            if owner_match:
                opener_id = int(owner_match.group(1))
                print(f"[reject] trying opener_id from topic: {opener_id}")
                row = db_get_ticket_by_opener(opener_id, channel_id)
                if row:
                    print(f"[reject] found by opener_id: {opener_id}")

    # Last resort: any large number in topic
    if not row and topic:
        id_matches = re.findall(r'(\d{15,20})', topic)
        for id_str in id_matches:
            try:
                opener_id = int(id_str)
                row = db_get_ticket_by_opener(opener_id)
                if row:
                    break
            except Exception:
                continue

    if not row:
        print(f"[reject] no ticket found for channel_id={channel_id} or in topic={topic!r}")
        return await interaction.response.send_message("❌ Run this command inside an application ticket channel.", ephemeral=True)
    status = str(row[6] or "").lower()
    if status == "accepted":
        return await interaction.response.send_message("❌ This application is already accepted.", ephemeral=True)
    if status in ("rejected", "closed"):
        return await interaction.response.send_message(f"❌ This application is already {status}.", ephemeral=True)

    final_reason = (reason or "").strip() or "Rejected"
    ticket_id = str(row[0])
    opener_id = int(row[3]) if row[3] else None
    await interaction.response.defer(ephemeral=True)

    now = datetime.now(timezone.utc)
    reject_embed = discord.Embed(
        title="❌ Application Rejected",
        description=f"**Reason:** {final_reason}",
        color=discord.Color.red(),
        timestamp=now,
    )
    reject_embed.add_field(name="Rejected by", value=interaction.user.mention, inline=True)
    reject_embed.set_footer(text=f"This ticket closes automatically in {MCWV_TICKET_DELETE_DELAY_SECONDS}s")
    try:
        await interaction.channel.send(
            content=f"<@{opener_id}>" if opener_id else None,
            embed=reject_embed,
            allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
        )
    except Exception as exc:
        print(f"[ticket] /reject in-channel notice failed for {ticket_id}: {exc}")
    await log_ticket_event(interaction.guild, reject_embed)

    ticket_channel = await prepare_ticket_close(
        interaction.guild,
        ticket_id,
        interaction.user.id,
        f"Rejected: {final_reason}",
        interaction.channel,
        final_status="rejected",
        extra_fields={"rejected_at": now, "rejected_by": interaction.user.id, "reject_reason": final_reason},
    )
    if ticket_channel is None:
        await interaction.followup.send(
            "⚠️ Application marked as **rejected** and transcript saved, but I could not verify the ticket's channel, "
            "so **no channel was deleted** (safety lock). Please delete it manually if needed.",
            ephemeral=True,
        )
        return
    await interaction.followup.send(
        f"✅ Application rejected (**{final_reason}**). Transcript saved, applicant DM sent, channel deletes in {MCWV_TICKET_DELETE_DELAY_SECONDS}s.",
        ephemeral=True,
    )
    await finalize_ticket_close(ticket_channel, str(interaction.user), f"Rejected: {final_reason}", ticket_id)


@bot.tree.command(name="guide", description="Officer tutorial for MCWV-BOT and Hub tools", guild=guild_obj)
async def guide(interaction: discord.Interaction):
    if not has_officer_guide_permission(interaction.user):
        return await interaction.response.send_message("❌ This guide is for officers only.", ephemeral=True)

    await interaction.response.send_message(
        embed=officer_guide_embed("overview"),
        view=OfficerGuideView(),
        ephemeral=True,
    )


@bot.tree.command(name="broadcast_ticket_link", description="Save a member's broadcast ticket channel", guild=guild_obj)
@app_commands.describe(
    member="The clan member this ticket belongs to.",
    channel="Ticket channel to save. Leave empty to use the current channel."
)
async def broadcast_ticket_link(
    interaction: discord.Interaction,
    member: discord.Member,
    channel: discord.TextChannel = None,
):
    if not has_broadcast_permission(interaction.user):
        return await interaction.response.send_message("❌ You do not have permission to link broadcast tickets.", ephemeral=True)

    target_channel = channel or interaction.channel
    if not isinstance(target_channel, discord.TextChannel):
        return await interaction.response.send_message("❌ Please run this in a ticket text channel or choose a text channel.", ephemeral=True)

    if getattr(member, "bot", False):
        return await interaction.response.send_message("❌ Pick the clan member, not a bot.", ephemeral=True)

    if db_set_ticket_channel(member.id, target_channel.id):
        actor_name = broadcast_actor_name(interaction)
        db_log_admin_action(
            "info",
            "Broadcast Ticket Linked",
            f"{actor_name} linked {member} to #{target_channel.name}.",
            "broadcast/ticket-link",
            actor_name,
            {
                "memberId": str(member.id),
                "channelId": str(target_channel.id),
                "channelName": target_channel.name,
            },
        )
        return await interaction.response.send_message(
            f"✅ Saved {target_channel.mention} as the broadcast ticket for {member.mention}.",
            ephemeral=True,
        )

    return await interaction.response.send_message(
        f"⚠️ {member.mention} is not linked in the bot database yet. Accept/link them first, then try again.",
        ephemeral=True,
    )


@bot.tree.command(name="broadcast_ticket_sync", description="Scan ticket categories and save member ticket channel IDs", guild=guild_obj)
@app_commands.describe(
    category="Optional category to scan. Leave empty to auto-detect ticket channels.",
    scan_all="Scan every text channel if auto-detect misses tickets. Slower but more complete.",
    send_menus="Send a resolver menu into tickets that still cannot be matched.",
    name_fallback="Also try slower channel-name matching if overwrite detection fails."
)
async def broadcast_ticket_sync(
    interaction: discord.Interaction,
    category: discord.CategoryChannel = None,
    scan_all: bool = False,
    send_menus: bool = True,
    name_fallback: bool = False,
):
    if not has_broadcast_permission(interaction.user):
        return await interaction.response.send_message("❌ You do not have permission to sync broadcast tickets.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    guild = interaction.guild
    if not guild:
        return await interaction.followup.send("This command must be used in the server.", ephemeral=True)

    users = _safe_call("db_get_broadcast_users", []) or []
    if not users:
        return await interaction.followup.send("No linked users found to match tickets against.", ephemeral=True)

    user_candidates = await build_ticket_sync_candidates(guild, users)
    channels = ticket_sync_channels(guild, category=category, scan_all=scan_all)

    progress_message = await interaction.followup.send(
        f"🔎 Starting ticket sync... **0/{len(channels)}** channel(s) checked.",
        ephemeral=True,
        wait=True,
    )
    last_progress_edit = 0.0

    matched = []
    matched_channel_ids = set()

    ambiguous_channels = []

    for channel_index, channel in enumerate(channels, start=1):
        now_ts = time.time()
        if channel_index == 1 or channel_index == len(channels) or now_ts - last_progress_edit >= 2.0:
            last_progress_edit = now_ts
            try:
                await progress_message.edit(
                    content=(
                        f"🔎 Ticket sync running... **{channel_index}/{len(channels)}** checked.\n"
                        f"Currently checking: {channel.mention} (`#{channel.name}`)\n"
                        f"Matched so far: **{len(matched)}**"
                    )
                )
            except Exception:
                pass

        # First try the reliable method: identify who has a member-specific ticket overwrite, excluding staff.
        visible_members = visible_non_staff_ticket_members(channel)
        linked_visible = []
        for member in visible_members:
            candidate = candidate_by_discord_id(user_candidates, member.id)
            if candidate and not candidate["matched"]:
                linked_visible.append((member, candidate))

        if len(linked_visible) == 1:
            member, candidate = linked_visible[0]
            if db_set_ticket_channel(candidate["discord_id"], channel.id):
                candidate["matched"] = True
                matched_channel_ids.add(channel.id)
                matched.append((candidate["username"], channel.name))
                continue
        elif len(linked_visible) > 1:
            ambiguous_channels.append(channel)
            continue

        if name_fallback:
            channel_key = normalize_ticket_key(f"{channel.name} {channel.topic or ''}")
            if channel_key:
                for candidate in user_candidates:
                    if candidate["matched"]:
                        continue
                    if any(key and (key in channel_key or channel_key in key) for key in candidate["keys"]):
                        if db_set_ticket_channel(candidate["discord_id"], channel.id):
                            candidate["matched"] = True
                            matched_channel_ids.add(channel.id)
                            matched.append((candidate["username"], channel.name))
                        break

        if channel.id not in matched_channel_ids:
            ambiguous_channels.append(channel)

        # Yield back to Discord's event loop so progress edits and interactions stay responsive.
        await asyncio.sleep(0)

    unmatched = [candidate["username"] for candidate in user_candidates if not candidate["matched"]]
    resolver_sent = 0
    if send_menus:
        unique_ambiguous = []
        seen_ambiguous = set()
        for channel in ambiguous_channels:
            if channel.id in seen_ambiguous or channel.id in matched_channel_ids:
                continue
            seen_ambiguous.add(channel.id)
            unique_ambiguous.append(channel)

        for menu_index, channel in enumerate(unique_ambiguous[:25], start=1):
            try:
                await progress_message.edit(
                    content=(
                        f"🧩 Auto-scan finished. Sending resolver menus... **{menu_index}/{min(len(unique_ambiguous), 25)}**\n"
                        f"Current ticket: {channel.mention} (`#{channel.name}`)\n"
                        f"Matched: **{len(matched)}**"
                    )
                )
            except Exception:
                pass
            if await send_ticket_resolve_menu(channel):
                resolver_sent += 1
            await asyncio.sleep(0.4)

    actor_name = broadcast_actor_name(interaction)
    db_log_admin_action(
        "info",
        "Broadcast Tickets Synced",
        f"{actor_name} synced {len(matched)} ticket channels.",
        "broadcast/ticket-sync",
        actor_name,
        {
            "matched": len(matched),
            "unmatched": len(unmatched),
            "scannedChannels": len(channels),
            "categoryId": str(category.id) if category else None,
            "scanAll": scan_all,
            "sendMenus": send_menus,
            "nameFallback": name_fallback,
            "resolverMenusSent": resolver_sent,
            "categoryIds": CLAN_MEMBER_CATEGORY_IDS,
        },
    )

    matched_preview = "\n".join(f"• {name} → #{channel}" for name, channel in matched[:15])
    unmatched_preview = "\n".join(f"• {name}" for name in unmatched[:15])
    message = (
        f"✅ Ticket sync complete.\n"
        f"Scanned **{len(channels)}** channel(s).\n"
        f"Matched **{len(matched)}** ticket channel(s).\n"
        f"Unmatched **{len(unmatched)}** linked user(s).\n\n"
        f"**Matches:**\n{matched_preview or 'No matches found.'}"
    )

    if unmatched_preview:
        message += f"\n\n**First unmatched:**\n{unmatched_preview}"

    try:
        await progress_message.edit(content=message[:1900])
    except Exception:
        await interaction.followup.send(message[:1900], ephemeral=True)


@bot.tree.command(name="refreshprofile", description="Refresh a member's cached Roblox profile data", guild=guild_obj)
@require_role()
@app_commands.describe(roblox_id="Roblox user ID to refresh")
async def refreshprofile(interaction: discord.Interaction, roblox_id: str):
    await interaction.response.defer(ephemeral=True)

    try:
        # basic anti-spam cooldown
        now = time.time()
        last = status_cooldown.get(f"refresh_{roblox_id}", 0)
        if now - last < 15:
            return await interaction.followup.send(
                "⏳ Please wait a few seconds before refreshing again.",
                ephemeral=True
            )
        status_cooldown[f"refresh_{roblox_id}"] = now

        # clear cache keys
        try:
            rid_int = int(roblox_id)
            PROFILE_CACHE.pop(rid_int, None)
        except Exception:
            pass

        PROFILE_CACHE.pop(roblox_id, None)

        # force fresh fetch
        bundle = await get_profile_bundle(session, roblox_id, force=True)
        extended_data, profile_data, inventory_data, public_views = bundle

        # optional: count what came back
        loaded_profile = bool(profile_data)
        loaded_inventory = bool(inventory_data)
        loaded_extended = bool(extended_data)

        await interaction.followup.send(
            "🔄 Refresh complete.\n"
            f"Profile: {'loaded' if loaded_profile else 'empty'}\n"
            f"Inventory: {'loaded' if loaded_inventory else 'empty'}\n"
            f"Extended: {'loaded' if loaded_extended else 'empty'}",
            ephemeral=True
        )

    except Exception as e:
        print(f"[refreshprofile] error for {roblox_id}: {e}")
        traceback.print_exc()
        await interaction.followup.send(
            f"❌ Refresh failed: `{type(e).__name__}`",
            ephemeral=True
        )
        
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

ACTIVE_BATTLE_API = f"{PS99_API}/api/activeClanBattle"

# ---------------- MEMBER COMMAND DESIGN SYSTEM ----------------
MCWV_BRAND_COLOR = 0x8B5CF6


def mcwv_bar(pct, length=16):
    pct = max(0.0, min(1.0, float(pct or 0)))
    filled = int(round(pct * length))
    return "▰" * filled + "▱" * (length - filled)


def mcwv_rank_flair(rank):
    try:
        rank = int(rank)
    except Exception:
        return "⚔️"
    if rank == 1:
        return "🥇"
    if rank == 2:
        return "🥈"
    if rank == 3:
        return "🥉"
    if rank <= 10:
        return "🔥"
    if rank <= 25:
        return "🏅"
    return "⚔️"


def mcwv_footer(embed, source="PS99 live"):
    embed.timestamp = datetime.now(timezone.utc)
    embed.set_footer(text=f"MCWV • {source}")
    return embed


def fmt_hm(seconds):
    seconds = max(0, int(seconds or 0))
    h, rem = divmod(seconds, 3600)
    m = rem // 60
    if h and m:
        return f"{h}h {m}m"
    if h:
        return f"{h}h"
    return f"{m}m"


def _as_utc(dt):
    if not hasattr(dt, "replace"):
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def db_get_clan_war_overview(recent_limit=3):
    """Recent + best completed wars from the shared Hub DB (war_snapshots + battles).
    Returns None when those tables don't exist or the DB is unavailable —
    callers should fall back to API data or hide the section."""
    if not db_enabled():
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.war_snapshots') IS NOT NULL AS e")
            row = cur.fetchone()
            if not row or not row[0]:
                return None
            cur.execute("""
                SELECT ws.battle_id, ws.rank, ws.battle_points, ws.captured_at, b.battle_name, b.end_time
                FROM war_snapshots ws
                LEFT JOIN battles b ON b.battle_id = ws.battle_id
                WHERE LOWER(ws.clan_name) = LOWER(%s)
                ORDER BY ws.captured_at DESC
                LIMIT 400
            """, (CLAN_NAME,))
            rows = cur.fetchall()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[war overview] query failed: {exc}")
        return None

    best_by_battle = {}
    for battle_id, rank, points, captured_at, battle_name, end_time in rows:
        key = str(battle_id)
        existing = best_by_battle.get(key)
        if existing is None:
            best_by_battle[key] = {
                "battleId": key,
                "name": battle_name or key,
                "rank": int(rank) if rank is not None else None,
                "points": int(points or 0),
                "when": end_time or captured_at,
            }
        elif points and int(points) > int(existing["points"] or 0):
            existing["points"] = int(points)

    wars = list(best_by_battle.values())

    def sort_key(war):
        when = war.get("when")
        return when.timestamp() if hasattr(when, "timestamp") else 0

    wars.sort(key=sort_key, reverse=True)
    ranked = [w for w in wars if w.get("rank")]
    best = min(ranked, key=lambda w: w["rank"]) if ranked else None
    return {"recent": wars[:recent_limit], "best": best, "count": len(wars)}


def db_get_points_24h_ago(battle_id):
    """{roblox_id: points} as of ~24h ago in this battle, from hourly snapshots.
    Used for ▲/▼ day-deltas on member-facing war commands. Best-effort."""
    if not db_enabled() or not battle_id:
        return {}
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.player_leaderboard_history') IS NOT NULL AS e")
            row = cur.fetchone()
            if not row or not row[0]:
                return {}
            cur.execute("""
                SELECT DISTINCT ON (roblox_id) roblox_id::text, points::bigint
                FROM player_leaderboard_history
                WHERE battle_id = %s
                  AND points IS NOT NULL
                  AND captured_at <= NOW() - INTERVAL '24 hours'
                ORDER BY roblox_id, captured_at DESC
            """, (str(battle_id),))
            return {str(r[0]): int(r[1]) for r in cur.fetchall()}
    except Exception as exc:
        print(f"[war commands] 24h delta query failed: {exc}")
        return {}


def mcwv_presence_line(roblox_id, presence):
    """Human presence string from a Roblox presence payload (+ offline timer)."""
    presence = presence or {}
    try:
        ptype = int(presence.get("userPresenceType", 0) or 0)
    except Exception:
        ptype = 0
    last_location = str(presence.get("lastLocation") or "").strip()
    if ptype == 1:
        return "🟢 Online — Website"
    if ptype == 2:
        return f"🎮 In Game{f' — {last_location}' if last_location else ''}"
    if ptype == 3:
        return "🔧 In Studio"
    since = offline_since.get(str(roblox_id)) or offline_since.get(int(roblox_id))
    if since:
        try:
            if not getattr(since, "tzinfo", None):
                since = since.replace(tzinfo=timezone.utc)
            elapsed = datetime.now(timezone.utc) - since
            return f"⚫ Offline for {fmt_hm(elapsed.total_seconds())}"
        except Exception:
            pass
    return "⚫ Offline"


def db_get_member_war_career(roblox_id):
    """(wars_played, career_points) from the hourly snapshot archive.
    None when the archive is unavailable — callers fall back to API counts."""
    if not db_enabled():
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.player_leaderboard_history') IS NOT NULL AS e")
            row = cur.fetchone()
            if not row or not row[0]:
                return None
            cur.execute("""
                SELECT COUNT(DISTINCT battle_id), COALESCE(SUM(points), 0)::bigint
                FROM (
                    SELECT DISTINCT ON (battle_id) battle_id, points::bigint AS points
                    FROM player_leaderboard_history
                    WHERE roblox_id = %s AND points IS NOT NULL
                    ORDER BY battle_id, captured_at DESC
                ) s
            """, (str(roblox_id),))
            row = cur.fetchone()
            if not row or not int(row[0] or 0):
                return None
            return int(row[0]), int(row[1] or 0)
    except Exception as exc:
        print(f"[profile] career query failed: {exc}")
        return None




@bot.tree.command(name="warinfo", description="Show current war status, MCWV rank, pace, and top contributors", guild=guild_obj)
async def warinfo(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=False)

    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession()

    try:
        async with session.get(ACTIVE_BATTLE_API) as r:
            if r.status != 200:
                return await interaction.followup.send("❌ Could not reach the PS99 war API right now.", ephemeral=True)
            if "application/json" not in r.headers.get("Content-Type", ""):
                print("[warinfo] war api returned non-JSON:", (await r.text())[:200])
                return await interaction.followup.send("❌ War API returned invalid data.", ephemeral=True)
            war_data = await r.json()

        async with session.get(CLAN_API) as r:
            if r.status != 200:
                return await interaction.followup.send("❌ Could not reach the clan API right now.", ephemeral=True)
            if "application/json" not in r.headers.get("Content-Type", ""):
                print("[warinfo] clan api returned non-JSON:", (await r.text())[:200])
                return await interaction.followup.send("❌ Clan API returned invalid data.", ephemeral=True)
            clan_data = await r.json()
    except Exception as e:
        print("[warinfo error]", repr(e))
        return await interaction.followup.send("❌ API request failed.", ephemeral=True)

    battle_id, battle = get_current_war(war_data, clan_data)
    if not battle:
        return await interaction.followup.send("❌ Could not determine current war.", ephemeral=True)

    war_config = war_data.get("data", {}).get("configData", {})
    start_ts = battle.get("StartTime") or war_config.get("StartTime")
    finish_ts = battle.get("FinishTime") or war_config.get("FinishTime")
    if not start_ts or not finish_ts:
        return await interaction.followup.send("❌ War timing data missing.", ephemeral=True)

    now = datetime.now(timezone.utc).timestamp()
    total_duration = max(finish_ts - start_ts, 1)
    elapsed = max(0, now - start_ts)
    progress = max(0.0, min(1.0, elapsed / total_duration))

    start_dt = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    finish_dt = datetime.fromtimestamp(finish_ts, tz=timezone.utc)
    friendly_name = _friendly_battle_name(battle_id)

    contributions = sorted(battle.get("PointContributions", []), key=lambda x: int(x.get("Points", 0) or 0), reverse=True)
    total_points = int(battle.get("Points") or sum(int(c.get("Points", 0) or 0) for c in contributions) or 0)

    # Live rank: same source the placement alert cards use (Big Games index + API fallback).
    placement = None
    try:
        placement = await get_mcwv_placement_snapshot()
    except Exception as placement_exc:
        print(f"[warinfo] placement snapshot failed: {placement_exc}")
    live_rank = (placement or {}).get("rank")
    total_points = int((placement or {}).get("points") or total_points)

    # Resolve contributor Roblox IDs to linked usernames/Discords for the top 5.
    linked_by_roblox = {}
    try:
        for u in db_get_all() or []:
            linked_by_roblox[int(u[0])] = (u[2], u[1])
    except Exception as link_exc:
        print(f"[warinfo] link map failed: {link_exc}")

    top_lines = []
    medals = ["🥇", "🥈", "🥉", "4.", "5."]
    for index, entry in enumerate(contributions[:5], start=1):
        rid = int(entry.get("UserID", 0) or 0)
        pts = int(entry.get("Points", 0) or 0)
        share = (pts / total_points * 100) if total_points else 0
        linked = linked_by_roblox.get(rid)
        name = (linked[0] if linked else None) or str(rid)
        mention = f" <@{linked[1]}>" if linked and linked[1] else ""
        top_lines.append(f"{medals[index - 1]} **{name}**{mention} — {format_points(pts)} · {share:.1f}%")
    top_block = "\n".join(top_lines) if top_lines else "No contributions tracked yet."

    elapsed_hours = max(elapsed / 3600, 0.25)
    pace = (total_points / elapsed_hours) if total_points else 0
    projected = int(pace * (total_duration / 3600)) if pace else 0

    member_count = len((clan_data.get("data", {}) or {}).get("Members", []) or [])

    if now < start_ts:
        status_line = "⏳ UPCOMING — war hasn't started"
        color = discord.Color.gold()
        time_field = f"Starts {discord.utils.format_dt(start_dt, 'R')}"
    elif now > finish_ts:
        status_line = "🏁 WAR ENDED"
        color = discord.Color.dark_gray()
        progress = 1.0
        time_field = f"Ended {discord.utils.format_dt(finish_dt, 'R')}"
    else:
        status_line = "⚔️ IN PROGRESS"
        color = discord.Color.red()
        time_field = f"Ends {discord.utils.format_dt(finish_dt, 'R')} · **{fmt_hm(finish_ts - now)} left**"

    embed = discord.Embed(title=f"⚔️ {friendly_name}", description=f"**{status_line}**", color=color)
    embed.add_field(name="Progress", value=f"`{mcwv_bar(progress)}` **{int(progress * 100)}%**", inline=False)

    if live_rank:
        embed.add_field(name=f"{mcwv_rank_flair(live_rank)} Clan Rank", value=f"**#{live_rank}**", inline=True)
    embed.add_field(name="🔢 Points", value=f"**{format_points(total_points)}**", inline=True)
    if start_ts <= now <= finish_ts and pace:
        embed.add_field(name="📈 Pace", value=f"**{format_points(int(pace))}**/hr\n→ ~**{format_points(projected)}** at end", inline=True)

    embed.add_field(name="🕐 Start", value=discord.utils.format_dt(start_dt, "f"), inline=True)
    embed.add_field(name="🏁 End", value=discord.utils.format_dt(finish_dt, "f"), inline=True)
    embed.add_field(name="⏱ Time", value=time_field, inline=True)
    if member_count:
        embed.add_field(name="👥 Contributors", value=f"**{len(contributions)}**/{member_count}", inline=True)

    embed.add_field(name="🏆 Top Contributors", value=top_block, inline=False)

    embed.timestamp = datetime.now(timezone.utc)
    mcwv_footer(embed, "PS99 live")
    await interaction.followup.send(embed=embed)


@bot.tree.command(
    name="leaderboard",
    description="Show MCWV clan war contribution leaderboard",
    guild=guild_obj
)
async def leaderboard(interaction: discord.Interaction):
    await interaction.response.defer()

    async def build_state():
        global session
        if session is None or session.closed:
            session = aiohttp.ClientSession()

        timeout = aiohttp.ClientTimeout(total=15)

        async with session.get(ACTIVE_BATTLE_API, timeout=timeout) as war_r:
            if war_r.status != 200:
                raise RuntimeError(f"war api {war_r.status}")
            if "application/json" not in war_r.headers.get("Content-Type", ""):
                raise RuntimeError("war api non-json")
            war_data = await war_r.json()

        async with session.get(CLAN_API, timeout=timeout) as clan_r:
            if clan_r.status != 200:
                raise RuntimeError(f"clan api {clan_r.status}")
            if "application/json" not in clan_r.headers.get("Content-Type", ""):
                raise RuntimeError("clan api non-json")
            clan_data = await clan_r.json()

        war_config = war_data.get("data", {}).get("configData", {})
        battle_id, battle = get_current_war(war_data, clan_data)
        if not battle:
            raise RuntimeError("no battle data")

        contributions = sorted(
            battle.get("PointContributions", []),
            key=lambda x: int(x.get("Points", 0) or 0),
            reverse=True,
        )
        if not contributions:
            raise RuntimeError("no contributions yet")

        total_points = int(battle.get("Points") or sum(int(c.get("Points", 0) or 0) for c in contributions) or 0)

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

        # Roblox usernames for every contributor (best-effort).
        id_to_name = {}
        try:
            for chunk in chunk_list(user_ids, 100):
                async with session.post(
                    ROBLOX_USERS_API,
                    json={"userIds": chunk, "excludeBannedUsers": False},
                    timeout=timeout,
                ) as r:
                    if r.status != 200:
                        continue
                    data = await r.json()
                    for u in data.get("data", []):
                        try:
                            uid = int(u["id"])
                            id_to_name[uid] = u.get("name", f"Unknown ({uid})")
                        except Exception:
                            continue
        except Exception as name_exc:
            print("[leaderboard] roblox name lookup error:", repr(name_exc))

        roblox_to_discord = {}
        discord_to_roblox = {}
        try:
            for row in db_get_all_tracked() or []:
                try:
                    roblox_to_discord[int(row[0])] = int(row[1])
                    discord_to_roblox[int(row[1])] = int(row[0])
                except Exception:
                    continue
        except Exception as map_exc:
            print("[leaderboard] tracked map error:", repr(map_exc))

        battle_name = _friendly_battle_name(battle_id)

        now = datetime.now(timezone.utc).timestamp()
        finish_ts = war_config.get("FinishTime")
        start_ts = war_config.get("StartTime", 0)
        is_active = bool(finish_ts and start_ts <= now <= finish_ts)

        entries = []
        for rank, entry in enumerate(contributions, start=1):
            uid = entry.get("UserID")
            if uid is None:
                continue
            try:
                uid_int = int(uid)
            except Exception:
                continue
            entries.append({
                "rank": rank,
                "user_id": uid_int,
                "name": id_to_name.get(uid_int, f"Unknown ({uid_int})"),
                "points": int(entry.get("Points", 0) or 0),
                "discord_id": roblox_to_discord.get(uid_int),
            })

        if not entries:
            raise RuntimeError("no valid leaderboard entries")

        # Live clan rank — same source as the placement cards (best-effort).
        clan_rank = None
        try:
            clan_rank = (await get_mcwv_placement_snapshot() or {}).get("rank")
        except Exception as rank_exc:
            print(f"[leaderboard] clan rank failed: {rank_exc}")

        # 24h point deltas from the hourly snapshots (best-effort).
        deltas = {}
        try:
            old_points = db_get_points_24h_ago(battle_id)
            for entry in entries:
                old = old_points.get(str(entry["user_id"]))
                if old is not None:
                    deltas[entry["user_id"]] = entry["points"] - int(old)
        except Exception as delta_exc:
            print(f"[leaderboard] delta lookup failed: {delta_exc}")

        # Top player's avatar as the embed thumbnail (best-effort).
        avatar_url = None
        try:
            avatar_url = await get_roblox_headshot_url(entries[0]["user_id"])
        except Exception:
            avatar_url = None

        return {
            "entries": entries,
            "battle_title": battle_name,
            "total_points": total_points,
            "is_active": is_active,
            "clan_rank": clan_rank,
            "deltas": deltas,
            "avatar_url": avatar_url,
            "requester_id": discord_to_roblox.get(interaction.user.id),
        }

    try:
        state = await build_state()
    except Exception as e:
        import traceback
        print("[LEADERBOARD ERROR]")
        print(traceback.format_exc())
        return await interaction.followup.send(
            "❌ Could not load the leaderboard right now. Try again in a moment.",
            ephemeral=True,
        )

    view = LeaderboardView(
        entries=state["entries"],
        battle_title=state["battle_title"],
        total_points=state["total_points"],
        is_active=state["is_active"],
        clan_rank=state.get("clan_rank"),
        deltas=state.get("deltas"),
        requester_id=state.get("requester_id"),
        avatar_url=state.get("avatar_url"),
        refetch=build_state,
    )

    await interaction.followup.send(embed=view.build_embed(), view=view)


@bot.tree.command(
    name="mystats",
    description="Check a Roblox user's clan war contribution stats",
    guild=guild_obj
)
async def mystats(interaction: discord.Interaction, roblox_username: str):
    await interaction.response.defer()

    global session

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

        # ---------------- SESSION SAFETY ----------------
        if session is None or session.closed:
            session = aiohttp.ClientSession()

        timeout = aiohttp.ClientTimeout(total=15)

        # ---------------- API CALLS ----------------
        async with session.get(ACTIVE_BATTLE_API, timeout=timeout) as war_r:
            if war_r.status != 200:
                return await interaction.followup.send("❌ Could not reach the PS99 war API.", ephemeral=True)
            if "application/json" not in war_r.headers.get("Content-Type", ""):
                print("[mystats] war api non-json")
                return await interaction.followup.send("❌ PS99 war API returned invalid data.", ephemeral=True)
            war_data = await war_r.json()

        async with session.get(CLAN_API, timeout=timeout) as clan_r:
            if clan_r.status != 200:
                return await interaction.followup.send("❌ Could not reach the PS99 clan API.", ephemeral=True)
            if "application/json" not in clan_r.headers.get("Content-Type", ""):
                print("[mystats] clan api non-json")
                return await interaction.followup.send("❌ PS99 clan API returned invalid data.", ephemeral=True)
            clan_data = await clan_r.json()

        # ---------------- CURRENT WAR ----------------
        battle_id, battle = get_current_war(war_data, clan_data)
        if not battle:
            return await interaction.followup.send("❌ Could not determine current battle.", ephemeral=True)

        contributions = sorted(
            battle.get("PointContributions", []),
            key=lambda x: int(x.get("Points", 0) or 0),
            reverse=True,
        )
        total_points = int(battle.get("Points") or sum(int(c.get("Points", 0) or 0) for c in contributions) or 0)
        total_players = len(contributions)
        average = (total_points / total_players) if total_players else 0

        war_config = war_data.get("data", {}).get("configData", {})
        start_ts = battle.get("StartTime") or war_config.get("StartTime")
        finish_ts = battle.get("FinishTime") or war_config.get("FinishTime")
        now = datetime.now(timezone.utc).timestamp()
        is_active = bool(start_ts and finish_ts and start_ts <= now <= finish_ts)
        time_left = fmt_hm(finish_ts - now) if is_active else None
        friendly = _friendly_battle_name(battle_id)

        # Avatar (best-effort).
        avatar_url = None
        try:
            avatar_url = await get_roblox_headshot_url(roblox_id)
        except Exception:
            avatar_url = None

        user_rank = next(
            (i + 1 for i, e in enumerate(contributions) if int(e.get("UserID", 0) or 0) == roblox_id),
            None,
        )
        user_entry = contributions[user_rank - 1] if user_rank else None

        # ---------------- NO CONTRIBUTION CARD ----------------
        if not user_entry:
            embed = discord.Embed(title=f"📊 {roblox_name} — {friendly}", color=discord.Color.dark_gray())
            if avatar_url:
                embed.set_thumbnail(url=avatar_url)
            embed.description = "😴 No contributions recorded for this war yet."
            embed.add_field(name="Discord", value=discord_display, inline=True)
            if is_active and finish_ts:
                finish_dt = datetime.fromtimestamp(finish_ts, tz=timezone.utc)
                embed.add_field(name="🏁 War Ends", value=discord.utils.format_dt(finish_dt, "R"), inline=True)
            embed.add_field(name="👥 Contributors So Far", value=f"**{total_players}**", inline=True)
            mcwv_footer(embed, "PS99 live")
            return await interaction.followup.send(embed=embed)

        # ---------------- MAIN CARD ----------------
        pts = int(user_entry.get("Points", 0) or 0)
        share = (pts / total_points * 100) if total_points else 0
        top_percent = (1 - ((user_rank - 1) / total_players)) * 100 if user_rank and total_players else 0

        # 24h delta (best-effort).
        delta_txt = ""
        try:
            old_points = db_get_points_24h_ago(battle_id)
            old = old_points.get(str(roblox_id))
            if old is not None:
                delta = pts - int(old)
                if delta != 0:
                    delta_txt = f"\n{'▲' if delta > 0 else '▼'} **{format_points(abs(delta))}** in 24h"
        except Exception as delta_exc:
            print(f"[mystats] delta lookup failed: {delta_exc}")

        # Vs clan average (full bar = 2x average).
        if average > 0:
            ratio = pts / average
            filled = max(0, min(20, int(round(ratio * 10))))
            if ratio >= 1:
                avg_txt = "🔥 **+%.0f%%** vs clan average" % ((ratio - 1) * 100)
            else:
                avg_txt = "🌙 **%.0f%%** below clan average" % ((1 - ratio) * 100)
        else:
            filled = 0
            avg_txt = "—"
        avg_bar = "█" * filled + "░" * (20 - filled)

        if user_rank and user_rank <= 3:
            color = discord.Color.gold()
        elif is_active:
            color = discord.Color.red()
        else:
            color = discord.Color(MCWV_BRAND_COLOR)

        embed = discord.Embed(title=f"📊 {roblox_name} — {friendly}", color=color)
        if avatar_url:
            embed.set_thumbnail(url=avatar_url)

        medals = {1: "🥇", 2: "🥈", 3: "🥉"}
        rank_display = medals.get(user_rank, f"#{user_rank}" if user_rank else "—")

        embed.add_field(name="Discord", value=discord_display, inline=True)
        embed.add_field(
            name=f"{mcwv_rank_flair(user_rank) if user_rank else '🏅'} Rank",
            value=f"**{rank_display}**/{total_players}",
            inline=True,
        )
        embed.add_field(name="⚔️ Points", value=f"**{format_points(pts)}**{delta_txt}", inline=True)
        embed.add_field(name="📈 Share", value=f"**{share:.1f}%** of clan total", inline=True)
        embed.add_field(name="🏅 Position", value=f"Outranks **{top_percent:.0f}%** of the clan", inline=True)
        if is_active and time_left:
            embed.add_field(name="⏱ Time Left", value=f"**{time_left}**", inline=True)

        embed.add_field(
            name="Vs Clan Average",
            value=f"`{avg_bar}`\n{avg_txt} · avg **{format_points(int(average))}**",
            inline=False,
        )
        embed.add_field(name="🔢 Clan Total", value=f"**{format_points(total_points)}**", inline=True)

        mcwv_footer(embed, "PS99 live")
        await interaction.followup.send(embed=embed)

    except Exception as e:
        import traceback
        print("[mystats error]")
        print(traceback.format_exc())
        await interaction.followup.send("❌ Something went wrong while fetching stats.", ephemeral=True)


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
        if isinstance(mastery, dict):
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

    @discord.ui.button(label="🔧 Debug", style=discord.ButtonStyle.red)
    async def debug_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            import json

            url = f"{PS99_API}/v1/players/{self.roblox_id}?include=profile,inventory,extendedProfile"
            timeout = aiohttp.ClientTimeout(total=10)

            async with self.session.get(url, timeout=timeout) as r:
                data = await r.json(content_type=None)

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
            PROFILE_CACHE.pop(self.roblox_id, None)

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

        roblox_id = int(resolved["id"])
        roblox_name = resolved["name"]

        # ---------------- DB LOOKUP ----------------
        db_users = db_get_all()
        linked = next((u for u in db_users if int(u[0]) == roblox_id), None)

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
                clan_role = "Owner"
            elif OFFICER_ROLE_ID in role_ids:
                clan_role = "Officer"
            elif MEMBER_ROLE_ID in role_ids:
                clan_role = "Member"

        # ---------------- SESSION SAFETY ----------------
        global session
        if session is None or session.closed:
            session = aiohttp.ClientSession()

        timeout = aiohttp.ClientTimeout(total=10)

        # ---------------- PRESENCE (best-effort) ----------------
        presence = None
        try:
            async with session.post(
                "https://presence.roblox.com/v1/presence/users",
                json={"userIds": [roblox_id]},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as pres_r:
                if pres_r.status == 200:
                    pres_payload = await pres_r.json(content_type=None)
                    presence = (pres_payload.get("userPresences") or [{}])[0]
        except Exception as pres_exc:
            print(f"[profile] presence failed: {pres_exc}")

        # ---------------- WAR DATA ----------------
        battle = None
        points = 0
        rank = None
        share = None
        war_count = 0

        try:
            async with session.get(CLAN_API, timeout=timeout) as clan_r:
                if clan_r.status == 200:
                    clan_data = await clan_r.json()
                    battles = (clan_data.get("data") or {}).get("Battles") or {}

                    now_ts = datetime.now(timezone.utc).timestamp()

                    for b_id, b_data in battles.items():
                        if not isinstance(b_data, dict):
                            continue
                        start = b_data.get("StartTime", 0) or 0
                        end = b_data.get("FinishTime", 0) or 0
                        contributions = b_data.get("PointContributions", [])

                        if any(int(e.get("UserID", 0) or 0) == roblox_id for e in contributions):
                            war_count += 1

                        if start <= now_ts <= end:
                            battle = b_data
                            contributions = sorted(
                                battle.get("PointContributions", []),
                                key=lambda x: int(x.get("Points", 0) or 0),
                                reverse=True,
                            )
                            total_clan = int(battle.get("Points") or sum(int(c.get("Points", 0) or 0) for c in contributions) or 0)
                            for i, entry in enumerate(contributions, start=1):
                                if int(entry.get("UserID", 0) or 0) == roblox_id:
                                    points = int(entry.get("Points", 0) or 0)
                                    share = (points / total_clan * 100) if total_clan else None
                                    rank = i
                                    break
                            break

        except Exception as e:
            print("[profile] war API error:", e)

        # ---------------- CAREER + ALTS (best-effort, MCWV archive) ----------------
        career = None
        try:
            career = db_get_member_war_career(roblox_id)
        except Exception as career_exc:
            print(f"[profile] career failed: {career_exc}")

        alt_names = []
        if discord_id:
            try:
                for row in db_get_alts(discord_id) or []:
                    alt_name = row[2] if len(row) > 2 else None
                    if alt_name and str(alt_name).lower() != roblox_name.lower():
                        alt_names.append(str(alt_name))
            except Exception as alt_exc:
                print(f"[profile] alts failed: {alt_exc}")

        # ---------------- PROFILE BUNDLE (for the tab buttons) ----------------
        extended_data, profile_data, inventory_data, public_views = await get_profile_bundle(
            session,
            roblox_id,
            force=True
        )

        # ---------------- AVATAR ----------------
        avatar_url = None
        try:
            avatar_url = await get_roblox_headshot_url(roblox_id)
        except Exception:
            avatar_url = None
        if not avatar_url:
            avatar_url = (
                discord_member.display_avatar.url
                if discord_member
                else interaction.user.display_avatar.url
            )

        # ---------------- EMBED ----------------
        role_colors = {
            "Owner": discord.Color.gold(),
            "Officer": discord.Color.from_rgb(56, 189, 248),
            "Member": discord.Color(MCWV_BRAND_COLOR),
        }
        color = role_colors.get(clan_role, discord.Color(MCWV_BRAND_COLOR))

        embed = discord.Embed(
            title=f"📇 {roblox_name}",
            description=f"[Roblox Profile ↗](https://www.roblox.com/users/{roblox_id}/profile)",
            color=color,
        )
        embed.set_thumbnail(url=avatar_url)

        embed.add_field(name="🆔 Roblox ID", value=f"`{roblox_id}`", inline=True)
        embed.add_field(name="💬 Discord", value=discord_display, inline=True)
        embed.add_field(name="🏷️ Clan Role", value=clan_role or "—", inline=True)

        embed.add_field(name="🔗 Account Status", value=linked_status, inline=True)
        embed.add_field(name="🟢 Presence", value=mcwv_presence_line(roblox_id, presence), inline=True)
        embed.add_field(name="\u200b", value="\u200b", inline=True)

        if battle and rank:
            war_value = f"**{format_points(points)}** · rank **#{rank}**"
            if share is not None:
                war_value += f" · **{share:.1f}%**"
            embed.add_field(name="⚔️ This War", value=war_value, inline=True)

        if career:
            career_wars, career_points = career
            embed.add_field(
                name="🗡️ War Career",
                value=f"**{format_points(career_points)}** pts across **{career_wars}** wars",
                inline=True,
            )
        elif war_count:
            embed.add_field(name="🗡️ War Career", value=f"**{war_count}** wars on record", inline=True)

        if alt_names:
            embed.add_field(name="🧩 Alts", value=", ".join(f"`{a}`" for a in alt_names[:6]), inline=False)

        mcwv_footer(embed, "PS99 live + MCWV archive")

        # ---------------- VIEW (unchanged tab buttons) ----------------
        view = ProfileView(
            extended_data,
            inventory_data,
            profile_data,
            roblox_name,
            public_views,
            roblox_id,
            session
        )

        await interaction.followup.send(embed=embed, view=view)

    except Exception as e:
        print("[profile] error:", repr(e))
        await interaction.followup.send(
            f"❌ Profile failed: `{e}`",
            ephemeral=True
        )

@bot.tree.command(
    name="clanstats",
    description="Show MCWV clan overview — level, members, gems, and battle history",
    guild=guild_obj
)
async def clanstats(interaction: discord.Interaction):
    await interaction.response.defer()

    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession()

    try:
        async with session.get(CLAN_API) as r:
            if r.status != 200:
                print(f"[clanstats] CLAN_API bad status: {r.status}")
                return await interaction.followup.send(
                    f"❌ Could not reach the PS99 API. (status {r.status})",
                    ephemeral=True
                )
            raw = await r.json(content_type=None)
            if not isinstance(raw, dict) or not isinstance(raw.get("data"), dict):
                print(f"[clanstats] Invalid API payload: {str(raw)[:200]}")
                return await interaction.followup.send("❌ Invalid API response data.", ephemeral=True)
            data = raw["data"]
    except Exception as e:
        print(f"[clanstats] API request failed: {e}")
        traceback.print_exc()
        return await interaction.followup.send(
            f"❌ API request failed: `{type(e).__name__}`",
            ephemeral=True
        )

    name = data.get("Name", CLAN_NAME)

    members = data.get("Members", [])
    if not isinstance(members, list):
        members = []

    level = pick_first_int(data, ("Level", "ClanLevel", "level", "Lvl", "lvl"))

    # Gems: only when the API actually reports a value (never show a fake 0).
    gem_candidates = [
        data.get("Diamonds"), data.get("diamonds"), data.get("Bank"),
        data.get("ClanBank"), data.get("TotalDiamonds"),
    ]
    if isinstance(data.get("Stats"), dict):
        gem_candidates.append(data["Stats"].get("Diamonds"))
    if isinstance(data.get("Economy"), dict):
        gem_candidates.append(data["Economy"].get("Diamonds"))
    gems = next((int(g) for g in gem_candidates if isinstance(g, (int, float)) and int(g) > 0), None)

    # War record: prefer the shared Hub DB (real captured ranks/points per war),
    # fall back to the PS99 battles dict when the DB has nothing.
    overview = None
    try:
        overview = db_get_clan_war_overview()
    except Exception as overview_exc:
        print(f"[clanstats] war overview failed: {overview_exc}")
        overview = None

    battles = data.get("Battles", {})
    if not isinstance(battles, dict):
        battles = {}

    api_total = len(battles)
    api_best_placement = None
    api_best_battle = None
    for bid, b in battles.items():
        if not isinstance(b, dict):
            continue
        placement = pick_first_int(
            b,
            ("Placement", "placement", "Rank", "rank", "Position", "position",
             "ClanPlacement", "LeaderboardPosition", "Place", "place"),
        )
        if not placement:
            continue
        if api_best_placement is None or placement < api_best_placement:
            api_best_placement = placement
            api_best_battle = bid

    total_wars = overview["count"] if overview else api_total

    best = overview["best"] if overview else None
    if not best and api_best_placement:
        best = {"name": _friendly_battle_name(api_best_battle), "rank": api_best_placement, "points": None}

    embed = discord.Embed(title=f"🏰 {name} — Clan Overview", color=discord.Color(MCWV_BRAND_COLOR))

    # Clan icon when the API exposes an asset id.
    icon = data.get("Icon")
    icon_id = None
    try:
        if icon is not None and str(icon).isdigit():
            icon_id = int(str(icon))
    except Exception:
        icon_id = None
    if icon_id:
        embed.set_thumbnail(url=f"https://www.roblox.com/asset-thumbnail/image?assetId={icon_id}&width=150&height=150&format=png")

    embed.add_field(name="👥 Members", value=f"**{len(members)}**", inline=True)
    if level:
        embed.add_field(name="🏅 Clan Level", value=f"**{level}**", inline=True)
    if gems:
        embed.add_field(name="💎 Gems", value=f"**{format_points(gems)}**", inline=True)
    if total_wars:
        embed.add_field(name="⚔️ Wars Fought", value=f"**{total_wars}**", inline=True)
    if best and best.get("rank"):
        best_line = f"**#{best['rank']}** · {_friendly_battle_name(best.get('name'))}"
        if best.get("points"):
            best_line += f"\n{format_points(best['points'])} pts"
        embed.add_field(name=f"{mcwv_rank_flair(best['rank'])} Best War", value=best_line, inline=True)

    # Live war chip — light check, details belong to /warinfo.
    if session is None or session.closed:
        session = aiohttp.ClientSession()
    try:
        async with session.get(ACTIVE_BATTLE_API) as r:
            if r.status == 200 and "application/json" in r.headers.get("Content-Type", ""):
                war_data = await r.json()
                live_battle_id, live_battle = get_current_war(war_data, {"data": data})
                if live_battle and live_battle_id:
                    embed.description = f"⚔️ **War in progress: {_friendly_battle_name(live_battle_id)}** — full details with `/warinfo`"
    except Exception as live_exc:
        print(f"[clanstats] live war check failed: {live_exc}")

    recent = overview["recent"] if overview else []
    if recent:
        lines = []
        for war in recent:
            war_name = _friendly_battle_name(war["name"])
            flair = mcwv_rank_flair(war["rank"]) if war.get("rank") else "⚔️"
            rank_txt = f"**#{war['rank']}**" if war.get("rank") else "—"
            pts_txt = f" · {format_points(war['points'])} pts" if war.get("points") else ""
            when = _as_utc(war.get("when"))
            date_txt = f" · {discord.utils.format_dt(when, 'd')}" if when else ""
            lines.append(f"{flair} **{war_name}** → {rank_txt}{pts_txt}{date_txt}")
        embed.add_field(name="🗓 Recent Wars", value="\n".join(lines), inline=False)

    mcwv_footer(embed, "PS99 live + MCWV war archive")
    await interaction.followup.send(embed=embed)

@bot.tree.command(name="compare", description="Compare two linked clan members head-to-head in the current war", guild=guild_obj)
async def compare(interaction: discord.Interaction, member1: discord.Member, member2: discord.Member):
    await interaction.response.defer()

    db_users = db_get_all()

    def get_linked(m):
        return next((u for u in db_users if int(u[1]) == m.id), None)

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
        timeout = aiohttp.ClientTimeout(total=15)

        async with session.get(ACTIVE_BATTLE_API, timeout=timeout) as war_r:
            if war_r.status != 200:
                return await interaction.followup.send("❌ Could not reach the PS99 war API.", ephemeral=True)
            if "application/json" not in war_r.headers.get("Content-Type", ""):
                print("[COMPARE] War API non-JSON")
                return await interaction.followup.send("❌ PS99 war API returned invalid data.", ephemeral=True)
            war_data = await war_r.json()

        async with session.get(CLAN_API, timeout=timeout) as clan_r:
            if clan_r.status != 200:
                return await interaction.followup.send("❌ Could not reach the PS99 clan API.", ephemeral=True)
            if "application/json" not in clan_r.headers.get("Content-Type", ""):
                print("[COMPARE] Clan API non-JSON")
                return await interaction.followup.send("❌ PS99 clan API returned invalid data.", ephemeral=True)
            clan_data = await clan_r.json()

    except Exception as e:
        print("[COMPARE ERROR]", repr(e))
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
        key=lambda x: int(x.get("Points", 0) or 0),
        reverse=True,
    )
    clan_total = int(battle.get("Points") or sum(int(c.get("Points", 0) or 0) for c in contributions) or 0)

    def get_entry(rid):
        entry = next((e for e in contributions if int(e.get("UserID", 0) or 0) == rid), None)
        rank = next((i + 1 for i, e in enumerate(contributions) if int(e.get("UserID", 0) or 0) == rid), None)
        pts = int(entry.get("Points", 0) or 0) if entry else 0
        return pts, rank

    pts1, rank1 = get_entry(rid1)
    pts2, rank2 = get_entry(rid2)
    share1 = (pts1 / clan_total * 100) if clan_total else 0
    share2 = (pts2 / clan_total * 100) if clan_total else 0

    # 24h deltas (best-effort).
    delta1 = delta2 = None
    try:
        old_points = db_get_points_24h_ago(battle_id)
        old1, old2 = old_points.get(str(rid1)), old_points.get(str(rid2))
        delta1 = (pts1 - int(old1)) if old1 is not None else None
        delta2 = (pts2 - int(old2)) if old2 is not None else None
    except Exception as delta_exc:
        print(f"[compare] delta lookup failed: {delta_exc}")

    friendly = _friendly_battle_name(battle_id)
    now = datetime.now(timezone.utc).timestamp()
    finish_ts = war_config.get("FinishTime")
    is_active = finish_ts and war_config.get("StartTime", 0) <= now <= finish_ts

    total = pts1 + pts2
    if total > 0:
        share1_bar = int((pts1 / total) * 20)
        share2_bar = 20 - share1_bar
    else:
        share1_bar = share2_bar = 10
    hth_bar = f"{'█' * share1_bar}{'░' * share2_bar}"
    pct1 = (pts1 / total * 100) if total else 50.0
    pct2 = 100 - pct1

    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    rank1_display = medals.get(rank1, f"#{rank1}") if rank1 else "—"
    rank2_display = medals.get(rank2, f"#{rank2}") if rank2 else "—"

    def crown(v1, v2, higher_better=True):
        """Return (suffix1, suffix2) crowning the better value."""
        if v1 is None or v2 is None or v1 == v2:
            return "", ""
        first_wins = (v1 > v2) if higher_better else (v1 < v2)
        return (" 🏆", "") if first_wins else ("", " 🏆")

    pts_t1, pts_t2 = crown(pts1, pts2)
    rank_t1, rank_t2 = crown(rank1, rank2, higher_better=False) if (rank1 and rank2) else ("", "")
    shr_t1, shr_t2 = crown(share1, share2)
    dlt_t1, dlt_t2 = crown(delta1, delta2)

    def delta_txt(d):
        if d is None:
            return "—"
        return f"{'▲' if d >= 0 else '▼'} {format_points(abs(d))}"

    col1 = (
        f"**{format_points(pts1)}**{pts_t1}\n"
        f"**{rank1_display}**{rank_t1}\n"
        f"**{share1:.1f}%**{shr_t1}\n"
        f"**{delta_txt(delta1)}**{dlt_t1}"
    )
    col2 = (
        f"**{format_points(pts2)}**{pts_t2}\n"
        f"**{rank2_display}**{rank_t2}\n"
        f"**{share2:.1f}%**{shr_t2}\n"
        f"**{delta_txt(delta2)}**{dlt_t2}"
    )
    categories = "⚔️ Points\n🏅 Clan Rank\n📈 Share\n▲ 24h"

    if pts1 > pts2:
        diff = pts1 - pts2
        verdict = f"🏆 **{name1}** leads by **{format_points(diff)}**"
        winner_id = rid1
    elif pts2 > pts1:
        diff = pts2 - pts1
        verdict = f"🏆 **{name2}** leads by **{format_points(diff)}**"
        winner_id = rid2
    else:
        verdict = "🤝 Dead even!"
        winner_id = rid1

    # Winner's avatar as the thumbnail (best-effort).
    avatar_url = None
    try:
        avatar_url = await get_roblox_headshot_url(winner_id)
    except Exception:
        avatar_url = None

    color = discord.Color.red() if is_active else discord.Color(MCWV_BRAND_COLOR)

    embed = discord.Embed(
        title=f"⚔️ {name1} vs {name2}",
        description=f"**{friendly}**\n{verdict}",
        color=color,
    )
    if avatar_url:
        embed.set_thumbnail(url=avatar_url)

    embed.add_field(name="\u200b", value=categories, inline=True)
    embed.add_field(name=f"📊 {name1}", value=col1, inline=True)
    embed.add_field(name=f"📊 {name2}", value=col2, inline=True)

    embed.add_field(
        name="Head-to-Head",
        value=f"`{hth_bar}`\n{name1} **{pct1:.0f}%** — **{pct2:.0f}%** {name2}",
        inline=False,
    )

    embed.timestamp = datetime.now(timezone.utc)
    embed.set_footer(text=f"MCWV • PS99 live • {'⚔️ War active' if is_active else '🏁 War ended'}")
    await interaction.followup.send(embed=embed)

@bot.tree.command(name="accept", description="Accept an applicant inside a Tickets v2 ticket", guild=guild_obj)
@require_role()
async def accept(interaction: discord.Interaction, member: discord.Member):
    await interaction.response.defer(ephemeral=True)

    channel = interaction.channel
    guild = interaction.guild
    ticket_creator = member

    actions = []
    errors = []
    summary_text = None

    try:
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
                summary_text = "❌ Timed out waiting for Roblox username. Run `/accept` again to retry."
                return

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
            summary_text = "❌ Could not get a valid Roblox username. Run `/accept` again to retry."
            return

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
        async with session.post(
            roblox_url,
            json={"usernames": [roblox_input], "excludeBannedUsers": False}
        ) as r:
            body = await r.json()
            print(f"[accept] Roblox lookup for '{roblox_input}': HTTP {r.status} → {body}")

            if r.status != 200:
                summary_text = f"❌ Roblox API returned an error (HTTP {r.status}). Try again in a moment."
                return

            results = body.get("data", [])
            if not results:
                summary_text = f"❌ Roblox user `{roblox_input}` not found. Please check the spelling and try again."
                return

            roblox_id = str(results[0]["id"])
            roblox_name = results[0]["name"]

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

        # --- link in bot DB and save this ticket channel for broadcasts ---
        ok, db_msg = db_add(roblox_id, ticket_creator.id, roblox_name)
        if not ok:
            summary_text = f"❌ Could not link Roblox account: {db_msg}"
            return

        actions.append("✅ Linked Roblox account in database")

        if db_set_ticket_channel(ticket_creator.id, channel.id):
            actions.append(f"✅ Saved ticket channel <#{channel.id}> for broadcasts")
        else:
            errors.append("⚠️ Could not save ticket channel ID for broadcasts")

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
        category = get_available_category(guild)

        if category:
            try:
                await channel.edit(
                    category=category,
                    sync_permissions=True,
                    reason="Member accepted"
                )
                actions.append(f"✅ Moved ticket to **{category.name}**")
            except Exception as e:
                errors.append(f"❌ Could not move ticket: {e}")
        else:
            errors.append("⚠️ All member categories are full — ticket not moved")

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
            except Exception as e:
                errors.append(f"❌ Could not send log embed: {e}")

        summary = "\n".join(actions) if actions else "—"
        if errors:
            summary += "\n\n⚠️ **Some steps failed:**\n" + "\n".join(errors)

        summary_text = f"**Accept complete!**\n{summary}"

    except Exception as e:
        print("[accept error]", repr(e))
        summary_text = f"❌ Accept failed.\n```{type(e).__name__}: {e}```"

    if summary_text:
        try:
            await interaction.followup.send(summary_text, ephemeral=True)
        except Exception as e:
            print("[accept followup error]", repr(e))
    
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

        await interaction.edit_original_response(embed=embed, view=None)

    # ---------------- BUTTONS (MUST BE OUTSIDE run_cleanup) ----------------

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Defer first — the cleanup does heavy DB/Discord work before responding.
        if not interaction.response.is_done():
            try:
                await interaction.response.defer()
            except Exception:
                pass
        try:
            await self.run_cleanup(interaction)
        except Exception as exc:
            print(f"[cleanup] confirm error: {exc}")
            traceback.print_exc()
            try:
                await interaction.followup.send("⚠️ Something went wrong during cleanup. Please try again.", ephemeral=True)
            except Exception:
                pass

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
@bot.tree.command(name="status", description="Check a member's Roblox status and war info", guild=guild_obj)
async def status(interaction: discord.Interaction, member: discord.Member):
    await interaction.response.defer(ephemeral=True)

    global session

    try:
        users = db_get_all()
        target = next((u for u in users if int(u[1]) == member.id), None)

        if not target:
            return await interaction.followup.send(f"❌ **{member.display_name}** is not linked to a Roblox account.", ephemeral=True)

        roblox_id = int(target[0])
        roblox_name = target[2]

        if session is None or session.closed:
            session = aiohttp.ClientSession()

        # Get presence
        pres = {}
        try:
            async with session.post(
                "https://presence.roblox.com/v1/presence/users",
                json={"userIds": [roblox_id]},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    pres = (data.get("userPresences") or [{}])[0]
        except Exception:
            pass

        ptype = int(pres.get("userPresenceType", 0) or 0)
        line = mcwv_presence_line(roblox_id, pres)

        # Color based on status
        if ptype == 2:
            color = discord.Color.green()
            status_emoji = "🎮"
        elif ptype == 1:
            color = discord.Color(0x3498DB)
            status_emoji = "🟢"
        elif ptype == 3:
            color = discord.Color(0xE67E22)
            status_emoji = "🔧"
        else:
            color = discord.Color.dark_gray()
            status_emoji = "⚫"

        embed = discord.Embed(
            title=f"{status_emoji} {roblox_name}",
            description=f"{line}\n\n**Discord:** {member.mention}\n**Roblox ID:** `{roblox_id}`\n[Profile](https://www.roblox.com/users/{roblox_id}/profile)",
            color=color,
            timestamp=datetime.now(timezone.utc),
        )

        # Avatar
        try:
            avatar_url = await get_roblox_headshot_url(roblox_id)
            if avatar_url:
                embed.set_thumbnail(url=avatar_url)
        except Exception:
            pass

        # War stats from cache
        try:
            cached = get_cached_player_history(str(roblox_id))
            if cached:
                battle_count = len(cached)
                ranked = [r for r in cached if r.get("rank") and r.get("total")]
                best_pct = 0
                if ranked:
                    best_pct = max(float(r.get("betterThan") or ((int(r.get("total")) - int(r.get("rank"))) / int(r.get("total")) * 100)) for r in ranked)
                clans = list(dict.fromkeys(str(r.get("clan") or "") for r in cached if r.get("clan")))
                embed.add_field(
                    name="⚔️ War History",
                    value=f"{battle_count} battles · best {best_pct:.0f}% better\n{' → '.join(clans[:4])}" if clans else f"{battle_count} battles",
                    inline=False,
                )
        except Exception:
            pass

        embed.set_footer(text=f"Requested by {interaction.user}")
        await interaction.followup.send(embed=embed, ephemeral=True)

    except Exception as e:
        print("[status error]", e)
        traceback.print_exc()
        await interaction.followup.send("❌ Error looking up status.", ephemeral=True)


# ---------------- /whois COMMAND ----------------

@bot.tree.command(name="whois", description="Look up a Discord member's linked Roblox account and war stats", guild=guild_obj)
@app_commands.describe(member="Discord member to look up")
@require_role()
async def whois(interaction: discord.Interaction, member: discord.Member):
    await interaction.response.defer(ephemeral=True)
    try:
        discord_id = member.id
        main_link = db_get_main_link(discord_id)
        alts = db_get_alts(discord_id)

        if not main_link and not alts:
            return await interaction.followup.send(f"No Roblox account linked for **{member.display_name}**.", ephemeral=True)

        embed = discord.Embed(title=f"\U0001f464 {member.display_name}", description=f"<@{discord_id}>", color=discord.Color(MCWV_BRAND_COLOR), timestamp=datetime.now(timezone.utc))

        loa = db_get_active_loa(discord_id=discord_id)
        if loa:
            started = f"<t:{int(loa['started_at'].timestamp())}:R>" if loa.get("started_at") else "unknown"
            embed.add_field(
                name="\U0001f3dd\ufe0f Leave of Absence",
                value=f"**Active** since {started}\nExcused from wars & tracking",
                inline=False,
            )

        if main_link:
            roblox_id, roblox_name = main_link
            embed.add_field(name="\U0001f3ae Main Roblox", value=f"**{roblox_name}**\n`ID: {roblox_id}`\n[Profile](https://www.roblox.com/users/{roblox_id}/profile)", inline=True)
            try:
                status_val = status_cache.get(str(roblox_id).strip())
                status_label = status_text(status_val) if status_val is not None else "Unknown"
                embed.add_field(name="\U0001f7e2 Status", value=status_label, inline=True)
            except Exception:
                embed.add_field(name="\U0001f7e2 Status", value="Unknown", inline=True)
            try:
                avatar_url = await get_roblox_headshot_url(roblox_id)
                if avatar_url:
                    embed.set_thumbnail(url=avatar_url)
            except Exception:
                pass
            try:
                cached = get_cached_player_history(roblox_id)
                if cached:
                    bc = len(cached)
                    medals = sum(1 for r in cached if r.get("earnedMedal"))
                    clans = list(dict.fromkeys(str(r.get("clan") or "") for r in cached if r.get("clan")))
                    best_pct = 0
                    ranked = [r for r in cached if r.get("rank") and r.get("total")]
                    if ranked:
                        best_pct = max(float(r.get("betterThan") or ((int(r.get("total")) - int(r.get("rank"))) / int(r.get("total")) * 100)) for r in ranked)
                    embed.add_field(name="\u2694\ufe0f War History", value=f"{bc} battles \u00b7 {medals} medals \u00b7 best {best_pct:.0f}%", inline=True)
                    if clans:
                        embed.add_field(name="\U0001f4cb Clans", value=" \u2192 ".join(clans[:6]), inline=False)
            except Exception:
                pass

        if alts:
            alt_lines = [f"**{name}** (`{rid}`)" for rid, name in alts[:5]]
            embed.add_field(name=f"\U0001f510 Alt Accounts ({len(alts)})", value="\n".join(alt_lines), inline=False)

        created = member.created_at
        joined = member.joined_at if hasattr(member, "joined_at") else None
        embed.add_field(name="\U0001f4c5 Discord", value=f"Account: {discord.utils.format_dt(created, 'R')}\nJoined: {discord.utils.format_dt(joined, 'R') if joined else 'Unknown'}", inline=True)
        role_names = [r.name for r in member.roles if r.name != "@everyone"][:8]
        if role_names:
            embed.add_field(name="\U0001f3c5 Roles", value=", ".join(role_names), inline=True)

        embed.set_footer(text=f"Lookup by {interaction.user}")
        await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"[whois] error: {e}")
        traceback.print_exc()
        await interaction.followup.send(f"Lookup failed: `{type(e).__name__}`", ephemeral=True)


# ---------------- /botstats COMMAND ----------------

@bot.tree.command(name="botstats", description="Show bot health, uptime, loop status, and stats", guild=guild_obj)
@require_role()
async def botstats(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    try:
        uptime = int(time.time() - STARTED_AT)
        hours, rem = divmod(uptime, 3600)
        minutes, seconds = divmod(rem, 60)
        days, hours = divmod(hours, 24)
        uptime_str = f"{days}d {hours}h {minutes}m"

        ping = round(bot.latency * 1000) if bot.latency else 0
        ready = bot.is_ready()

        embed = discord.Embed(title="\U0001f9fe Bot Status", color=discord.Color.green() if ready else discord.Color.orange(), timestamp=datetime.now(timezone.utc))
        embed.description = f"{'\U0001f7e2 Online' if ready else '\U0001f7e1 Starting'} \u00b7 Ping: **{ping}ms**"

        embed.add_field(name="\u23f1 Uptime", value=uptime_str, inline=True)
        embed.add_field(name="\U0001f4e6 Commands Run", value=str(COMMANDS_EXECUTED), inline=True)
        embed.add_field(name="\U0001f3d8 Servers", value=str(len(bot.guilds)), inline=True)

        db_ok = _db_connected()
        tracked = _tracked_players()
        embed.add_field(name="\U0001f4be Database", value=f"{'\u2705 Connected' if db_ok else '\u274c Disconnected'}\n{tracked} tracked", inline=True)
        embed.add_field(name="\u2694\ufe0f War", value=f"{'Active' if ps99_war_active else 'Peacetime'}\n{PS99_CURRENT_WAR_NAME or 'None'}", inline=True)

        loop_names = [("Presence", "check_loop"), ("War Poll", "war_poll_loop"), ("Reminder", "reminder_loop"), ("Clan Leave", "clan_leave_loop"), ("Placement", "placement_alert_loop"), ("Clan Logs", "clan_log_loop"), ("Hourly Stats", "hourly_stats_loop"), ("Hourly Snapshot", "hourly_player_snapshot_loop"), ("Hub Collector", "hub_war_collect_loop"), ("Screenshot", "ticket_screenshot_reminder_loop"), ("Broadcast", "broadcast_scheduler_loop"), ("Giveaway", "check_giveaway_event")]
        loop_lines = []
        for label, ln in loop_names:
            lo = globals().get(ln)
            if lo:
                running = lo.is_running()
                icon = "\u2705" if running else "\u26ab"
                interval = ""
                if hasattr(lo, "seconds") and lo.seconds:
                    interval = f" ({lo.seconds}s)"
                elif hasattr(lo, "minutes") and lo.minutes:
                    interval = f" ({lo.minutes}m)"
                loop_lines.append(f"{icon} {label}{interval}")
            else:
                loop_lines.append(f"\u2753 {label}")
        embed.add_field(name="\U0001f501 Loops", value="\n".join(loop_lines), inline=False)

        try:
            if psutil:
                proc = psutil.Process(os.getpid())
                mem_mb = round(proc.memory_info().rss / 1024 / 1024, 1)
                cpu = round(psutil.cpu_percent(interval=None), 1)
                embed.add_field(name="\U0001f4be RAM", value=f"{mem_mb} MB", inline=True)
                embed.add_field(name="\U0001f9ee CPU", value=f"{cpu}%", inline=True)
        except Exception:
            pass

        embed.set_footer(text=f"Bot ID: {bot.user.id if bot.user else '?'}")
        await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"[botstats] error: {e}")
        traceback.print_exc()
        await interaction.followup.send(f"Stats failed: `{type(e).__name__}`", ephemeral=True)


# ---------------- AUTO-RECONNECT ----------------

@bot.event
async def on_resumed():
    print("\u2705 Session resumed (gateway reconnected)")
    try:
        await update_bot_presence()
    except Exception:
        pass


@bot.event
async def on_connect():
    print("\u2705 Connected to Discord gateway")
    global session
    if session is None or (hasattr(session, "closed") and session.closed):
        session = aiohttp.ClientSession()


        import asyncio
from datetime import datetime, timezone, timedelta
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

        db_updates = []

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
                db_updates.append((rid, status))

                if status == 0:
                    offline_since[rid] = now_dt
                else:
                    offline_since.pop(rid, None)

            await asyncio.sleep(1)

        if db_updates:
            await asyncio.to_thread(db_bulk_set_user_statuses, db_updates)

    except Exception as e:
        print("Initial sync error:", e)

# ---------------- ROBLOX LOOP (every 2 min — detects transitions) ----------------
@tasks.loop(minutes=2)
async def check_loop():
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
        db_updates = []
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

                db_updates.append((rid, current))

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

        if db_updates:
            await asyncio.to_thread(db_bulk_set_user_statuses, db_updates)

    except Exception as e:
        print("Loop processing error:", e)
    pass  # keep connection alive (Supabase has no compute hour limit)

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
    pass  # keep connection alive (Supabase has no compute hour limit)

# ---------------- PS99 WAR POLL ----------------
ps99_first_check = True
ps99_war_active = False
PS99_CURRENT_WAR_NAME = None

ACTIVE_BATTLE_API = f"{PS99_API}/api/activeClanBattle"


async def update_bot_presence():
    """Update the bot's Discord presence based on current state."""
    try:
        if ps99_war_active and PS99_CURRENT_WAR_NAME:
            war_name = _friendly_battle_name(PS99_CURRENT_WAR_NAME)
            await bot.change_presence(
                status=discord.Status.online,
                activity=discord.Activity(
                    type=discord.ActivityType.watching,
                    name=f"⚔️ {war_name}",
                ),
            )
        else:
            try:
                count = len(db_get_all_tracked())
            except Exception:
                count = 0
            await bot.change_presence(
                status=discord.Status.online,
                activity=discord.Activity(
                    type=discord.ActivityType.watching,
                    name=f"🛡️ MCWV · {count} members",
                ),
            )
    except Exception:
        pass


@tasks.loop(minutes=10)
async def war_poll_loop():
    global bot_enabled, ps99_war_active, ps99_first_check, PS99_CURRENT_WAR_NAME, session

    try:
        if session is None or session.closed:
            session = aiohttp.ClientSession()

        timeout = aiohttp.ClientTimeout(total=15)

        async with session.get(ACTIVE_BATTLE_API, timeout=timeout) as r:
            if r.status != 200:
                print(f"❌ War API returned {r.status}")
                return

            content_type = r.headers.get("Content-Type", "")
            if "application/json" not in content_type:
                print(f"❌ War API returned non-JSON: {content_type}")
                print(await r.text())
                return

            data = await r.json()

        print("📦 War API response:", data)

        config = data.get("data", {}).get("configData", {})
        PS99_CURRENT_WAR_NAME = config.get("Title") or config.get("configName") or data.get("data", {}).get("configName")
        start = config.get("StartTime")
        finish = config.get("FinishTime")
        now = datetime.now(timezone.utc).timestamp()

        currently_active = (
            isinstance(start, (int, float)) and
            isinstance(finish, (int, float)) and
            start <= now <= finish
        )

        if ps99_first_check:
            ps99_first_check = False
            ps99_war_active = currently_active
            bot_enabled = currently_active
            print(f"[INIT] War state set to {currently_active}")
            await update_bot_presence()
            update_check_loop_interval()
            return

        if ps99_war_active != currently_active:
            ps99_war_active = currently_active
            bot_enabled = currently_active
            await update_bot_presence()
            update_check_loop_interval()

            channel = await _get_channel(CHANNEL_ID)
            if not channel:
                return

            if currently_active:
                await channel.send("⚠️ CLAN WAR STARTED!! LETS GO MCWV!!!!!")
                print("War started (state synced)")
                await run_initial_presence_check()
                # Fire instant push to all Hub subscribers.
                await trigger_hub_push(
                    "war_start",
                    title="⚔️ WAR DECLARED",
                    body=f"{PS99_CURRENT_WAR_NAME or 'New battle'} is live — MCWV, to arms!",
                    url="/war-info",
                    tag=f"war-start-{PS99_CURRENT_WAR_NAME or 'battle'}".lower()[:48],
                )
            else:
                offline_since.clear()
                status_cache.clear()
                await channel.send("🛑 CLAN WAR OVER. GG EVERYONE!!")
                print("War ended (state synced)")
                # Fire instant push + sweep any pending broadcasts.
                await trigger_hub_push(
                    "war_end",
                    title="🛑 WAR OVER",
                    body="GG MCWV — war's done. Check the recap on the Hub.",
                    url="/war-info",
                    tag="war-end",
                )
                await trigger_hub_push("sweep")
                # Auto-cache the just-ended war's cross-clan contributor data.
                if PS99_CURRENT_WAR_NAME:
                    asyncio.create_task(auto_cache_war_end(PS99_CURRENT_WAR_NAME))
                    print(f"[cross-clan cache] auto-cache queued for {PS99_CURRENT_WAR_NAME}")

    except Exception as e:
        print("War poll error:", e)
    pass  # keep connection alive (Supabase has no compute hour limit)
        
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

        # The in-game Owner is not always listed in Members. Include them so
        # the owner never triggers a false "Clan Leave Detected" alert.
        owner_id = get_clan_owner_id_from_data(data)
        if owner_id:
            try:
                clan_member_ids.add(int(owner_id))
            except Exception:
                pass

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
                embed.add_field(name="Action Required", value="Approve removal, grant LOA, or ignore.", inline=False)

                await staff_channel.send(
                    embed=embed,
                    view=ClanReviewView(roblox_id)
                )

            except Exception as e:
                print("Clan leave row error:", e)

    except Exception as e:
        print("Clan leave loop error:", e)
    pass  # keep connection alive (Supabase has no compute hour limit)

# ---------------- LOA TICKET VIEW (End LOA / Info) ----------------
class LoaTicketView(discord.ui.View):
    def __init__(self, roblox_id, roblox_name, discord_id):
        super().__init__(timeout=None)
        self.roblox_id = str(roblox_id).strip()
        self.roblox_name = roblox_name
        self.discord_id = discord_id

    @discord.ui.button(label="End LOA", style=discord.ButtonStyle.success, emoji="✅")
    async def end_loa(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not has_mcwv_ticket_staff_permission(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        await interaction.response.defer()

        record = db_get_active_loa(roblox_id=self.roblox_id) or db_get_active_loa(channel_id=interaction.channel_id)
        if not record:
            await interaction.followup.send(
                "⚠️ No active LOA record found. Use `/endloa` or `/loas` to manage it manually.",
                ephemeral=True,
            )
            return

        notes = []
        guild = interaction.guild
        discord_id = int(record["discord_id"]) if record.get("discord_id") else None
        ok, notes = await perform_loa_revert(guild, record, interaction.user, "LOA ended from ticket button")

        # 5) Replace the LOA card with a completed state.
        done_embed = discord.Embed(
            title="✅ LOA Ended",
            description=f"**{record.get('roblox_username') or self.roblox_name}** is back on active duty.",
            color=discord.Color.from_rgb(52, 211, 153),
            timestamp=datetime.now(timezone.utc),
        )
        done_embed.add_field(name="Ended by", value=interaction.user.mention, inline=True)
        done_embed.add_field(name="Changes", value="\n".join(f"• {n}" for n in notes), inline=False)
        try:
            await interaction.message.edit(embed=done_embed, view=None)
        except Exception:
            pass
        if discord_id:
            await interaction.followup.send(f"🏝️ **LOA ended** — welcome back <@{discord_id}>!")
        else:
            await interaction.followup.send("🏝️ **LOA ended.**")
        self.stop()

    @discord.ui.button(label="Info", style=discord.ButtonStyle.secondary, emoji="ℹ️")
    async def info(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "🏝️ **Leave of Absence**\n\n"
            "This member is excused from wars and point tracking.\n"
            "• Their Roblox link and Hub login stay intact.\n"
            "• They won't appear in member lists or receive war pings while on LOA.\n"
            "• Press **✅ End LOA** (staff only) when they return to restore their role, ticket channel and tracking.\n"
            "• If the buttons ever stop working after a bot restart, use `/loas` and `/endloa` instead.",
            ephemeral=True,
        )


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

            # Defer FIRST so Discord doesn't timeout during DB operations
            await interaction.response.defer()

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

            await interaction.edit_original_response(
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

    @discord.ui.button(label="LOA", style=discord.ButtonStyle.secondary, emoji="🏝️")
    async def loa(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if self.roblox_id not in pending_clan_removals:
                return await interaction.response.send_message("Already handled.", ephemeral=True)

            await interaction.response.defer(ephemeral=True)
            data = pending_clan_removals.pop(self.roblox_id)
            guild = interaction.guild
            discord_id = int(data["discord_id"])
            roblox_name = str(data["roblox_name"] or "unknown")
            notes = []

            # 1) Find the member's ticket channel (by topic or DB-stored channel ID).
            ticket_channel = None
            if guild:
                ticket_channel = discord.utils.get(
                    guild.text_channels,
                    topic=f"mcwv-ticket-owner:{discord_id}"
                )
                if ticket_channel is None:
                    # Fall back to the DB-stored ticket_channel_id column.
                    try:
                        saved_id = db_get_ticket_channel_id(discord_id)
                        if saved_id:
                            ticket_channel = guild.get_channel(saved_id)
                    except Exception:
                        pass

            # 2) Move + rename the ticket channel.
            if isinstance(ticket_channel, discord.TextChannel):
                safe_name = normalize_ticket_key(roblox_name)[:24] or str(discord_id)
                loa_category = guild.get_channel(MCWV_LOA_CATEGORY_ID) if guild else None
                if isinstance(loa_category, discord.CategoryChannel):
                    try:
                        await ticket_channel.edit(
                            category=loa_category,
                            name=f"\U0001f3dd\ufe0f-ticket-{safe_name}",
                            reason=f"MCWV LOA - moved by {interaction.user}",
                        )
                        notes.append(f"Ticket moved to LOA category and renamed")
                    except Exception as exc:
                        print(f"[LOA] ticket move/rename failed: {exc}")
                        notes.append(f"Could not move ticket: {exc}")
                else:
                    notes.append("LOA category not found - ticket not moved")
            else:
                notes.append("No ticket channel found for this member - skipped move")

            # 3) Remove the clan member role and add the LOA role.
            member = guild.get_member(discord_id) if guild else None
            if member is None and guild:
                try:
                    member = await guild.fetch_member(discord_id)
                except Exception:
                    member = None

            clan_role = guild.get_role(CLAN_MEMBER_ROLE_ID) if guild else None
            if member and clan_role and clan_role in member.roles:
                try:
                    await member.remove_roles(clan_role, reason=f"MCWV LOA - by {interaction.user}")
                    notes.append("Clan member role removed")
                except Exception as exc:
                    notes.append(f"Could not remove clan role: {exc}")

            loa_role = guild.get_role(MCWV_LOA_ROLE_ID) if guild else None
            if member and loa_role:
                try:
                    if loa_role not in member.roles:
                        await member.add_roles(loa_role, reason=f"MCWV LOA - by {interaction.user}")
                    notes.append(f"LOA role added ({loa_role.name})")
                except Exception as exc:
                    notes.append(f"Could not add LOA role: {exc}")
            elif not loa_role:
                notes.append("LOA role not found in server")

            # 4) Record the LOA in the database. KEEP the Roblox link + Hub login intact!
            ticket_row = find_ticket_in_channel(ticket_channel) if isinstance(ticket_channel, discord.TextChannel) else None
            ticket_id = str(ticket_row[0]) if ticket_row else None
            channel_id = int(ticket_channel.id) if isinstance(ticket_channel, discord.TextChannel) else None
            name_before = ticket_channel.name if isinstance(ticket_channel, discord.TextChannel) else None
            cat_before = ticket_channel.category_id if isinstance(ticket_channel, discord.TextChannel) else None

            loa_ok, loa_info = db_start_loa(
                roblox_id=self.roblox_id,
                discord_id=discord_id,
                roblox_username=roblox_name,
                ticket_id=ticket_id,
                ticket_channel_id=channel_id,
                ticket_name_before=name_before,
                ticket_category_before=cat_before,
                started_by=interaction.user.id,
            )
            if not loa_ok:
                notes.append(f"⚠️ LOA record failed: {loa_info}")
            else:
                notes.append("LOA recorded — links + Hub login preserved")

            # 5) Post the LOA card in the member's ticket (End LOA button lives there).
            if isinstance(ticket_channel, discord.TextChannel):
                try:
                    loa_embed = discord.Embed(
                        title="🏝️ Leave of Absence — Active",
                        description=(
                            f"**{roblox_name}** has been placed on **Leave of Absence** by "
                            f"{interaction.user.mention}.\n\n"
                            "They are excused from wars and point tracking until staff press **✅ End LOA** below."
                        ),
                        color=discord.Color.from_rgb(96, 165, 250),
                        timestamp=datetime.now(timezone.utc),
                    )
                    loa_embed.add_field(name="Roblox", value=f"`{roblox_name}`", inline=True)
                    loa_embed.add_field(name="Started", value=f"<t:{int(time.time())}:F>", inline=True)
                    loa_embed.add_field(
                        name="Changes",
                        value="\n".join(f"• {n}" for n in notes) or "• none",
                        inline=False,
                    )
                    loa_embed.set_footer(text="End LOA restores roles, the ticket channel and tracking automatically.")
                    await ticket_channel.send(
                        embed=loa_embed,
                        view=LoaTicketView(self.roblox_id, roblox_name, discord_id),
                    )
                    notes.append("LOA card posted in ticket")
                except Exception as exc:
                    print(f"[LOA] ticket card failed: {exc}")
                    notes.append(f"LOA card failed: {exc}")
            else:
                notes.append("No ticket channel — LOA card skipped")

            # 6) Edit the original staff embed to show LOA was applied.
            await interaction.edit_original_response(
                content=f"\U0001f3dd\ufe0f **LOA applied by {interaction.user.mention}**\n" + "\n".join(notes),
                embed=interaction.message.embeds[0] if interaction.message.embeds else None,
                view=None
            )
            self.stop()

        except Exception as e:
            print("LOA button error:", e)
            try:
                await interaction.followup.send(
                    "Something went wrong while applying LOA.",
                    ephemeral=True
                )
            except Exception:
                pass

# ---------------- TICKET SCREENSHOT REMINDER LOOP ----------------
def db_tickets_needing_screenshot_reminder():
    if not db_enabled():
        return []
    db_ensure_mcwv_ticket_tables()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT t.ticket_id, t.channel_id, t.opener_discord_id
                FROM mcwv_tickets t
                WHERE t.status IN ('open', 'pending')
                  AND t.created_at <= NOW() - INTERVAL '6 hours'
                  AND NOT EXISTS (
                    SELECT 1 FROM mcwv_ticket_actions a
                    WHERE a.ticket_id = t.ticket_id
                      AND a.action = 'screenshots/uploaded'
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM mcwv_ticket_actions a
                    WHERE a.ticket_id = t.ticket_id
                      AND a.action = 'screenshots/reminder_sent'
                      AND a.created_at >= NOW() - INTERVAL '6 hours'
                  )
                ORDER BY t.created_at ASC
                LIMIT 25
            """)
            return cur.fetchall()
    except Exception as exc:
        print("db_tickets_needing_screenshot_reminder error:", exc)
        return []


@tasks.loop(minutes=10)
async def ticket_screenshot_reminder_loop():
    await bot.wait_until_ready()
    rows = db_tickets_needing_screenshot_reminder()
    if not rows:
        return

    for ticket_id, channel_id, opener_id in rows:
        try:
            channel = bot.get_channel(int(channel_id))
            if channel is None:
                channel = await bot.fetch_channel(int(channel_id))
            if not isinstance(channel, discord.TextChannel):
                # Channel deleted — mark as reminded so we stop retrying
                db_ticket_log(ticket_id, None, "screenshots/reminder_sent", "Channel no longer exists — reminder skipped")
                continue

            embed = discord.Embed(
                title="Screenshot reminder",
                description=(
                    "Please upload your **non-cropped** screenshots for your MCWV application, then press "
                    "**Screenshots uploaded** so staff know your application is ready."
                ),
                color=discord.Color(get_ticket_embed_color("reminder", 0xF59E0B)),
                timestamp=datetime.now(timezone.utc),
            )
            embed.add_field(
                name="Needed screenshots",
                value="• Pets\n• Rank\n• Masteries\n• Enchants\n• Game-passes\n• Player profile",
                inline=False,
            )
            await channel.send(
                content=f"<@{int(opener_id)}>",
                embed=embed,
                view=ScreenshotUploadedView(),
                allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
            )
            db_ticket_log(ticket_id, None, "screenshots/reminder_sent", "Screenshot reminder sent after 6 hours")
            await asyncio.sleep(0.5)
        except discord.NotFound:
            # Channel deleted — mark as reminded so we stop retrying every 30 min
            db_ticket_log(ticket_id, None, "screenshots/reminder_sent", "Channel deleted — reminder skipped")
        except Exception as exc:
            print(f"ticket_screenshot_reminder_loop error for {ticket_id}: {exc}")


# ---------------- PLACEMENT ALERTS ----------------
def placement_setting_key(battle_id):
    return f"mcwv_placement_state:{battle_id}"


def placement_alerts_enabled():
    raw = db_get_setting("mcwv_placement_alerts_enabled", "1" if MCWV_PLACEMENT_ALERTS_ENABLED_DEFAULT else "0")
    return str(raw).lower() not in ("0", "false", "off", "no")


def get_placement_channel_id():
    saved = db_get_setting("mcwv_placement_channel_id", None)
    try:
        return int(saved or MCWV_PLACEMENT_CHANNEL_ID or 0)
    except Exception:
        return int(MCWV_PLACEMENT_CHANNEL_ID or 0)


def set_placement_channel_id(channel_id):
    db_set_setting("mcwv_placement_channel_id", int(channel_id))


def load_placement_state(battle_id):
    raw = db_get_setting(placement_setting_key(battle_id), "")
    try:
        return json.loads(raw) if raw else None
    except Exception:
        return None


def save_placement_state(battle_id, rank, points, announced=False):
    state = {
        "battleId": str(battle_id),
        "rank": int(rank),
        "points": int(points or 0),
        "updatedAt": datetime.now(timezone.utc).isoformat(),
        "lastAnnouncedAt": time.time() if announced else (load_placement_state(battle_id) or {}).get("lastAnnouncedAt"),
    }
    db_set_setting(placement_setting_key(battle_id), json.dumps(state))
    return state


def format_compact_points(value):
    try:
        value = float(value or 0)
    except Exception:
        value = 0
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}b"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}m"
    if value >= 1_000:
        return f"{value / 1_000:.2f}k"
    return str(int(value))


def extract_asset_id(value):
    match = re.search(r"(\d+)", str(value or ""))
    return match.group(1) if match else None


async def fetch_image_bytes(url):
    global session
    if not url:
        return None
    if session is None or session.closed:
        session = aiohttp.ClientSession()
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as res:
            if res.status == 200:
                return await res.read()
    except Exception as exc:
        print(f"[placement] image fetch failed: {exc}")
    return None


def placement_int(value):
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def pick_first_int(source, keys):
    if not isinstance(source, dict):
        return None
    for key in keys:
        value = placement_int(source.get(key))
        if value is not None:
            return value
    return None


def pick_active_battle_id(payload):
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    config = data.get("configData", {}) if isinstance(data, dict) else {}

    candidates = [
        data.get("activeBattleConfigName"),
        data.get("activeBattleId"),
        data.get("battleId"),
        data.get("configName"),
        payload.get("activeBattleConfigName") if isinstance(payload, dict) else None,
        payload.get("activeBattleId") if isinstance(payload, dict) else None,
        payload.get("battleId") if isinstance(payload, dict) else None,
        config.get("Title") if isinstance(config, dict) else None,
        config.get("configName") if isinstance(config, dict) else None,
        config.get("_id") if isinstance(config, dict) else None,
    ]

    for candidate in candidates:
        if candidate:
            return str(candidate)
    return None


def battle_is_live(payload):
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    config = data.get("configData", {}) if isinstance(data, dict) else {}
    start = config.get("StartTime") or data.get("startTime") or data.get("startTimeSeconds")
    finish = config.get("FinishTime") or data.get("finishTime") or data.get("finishTimeSeconds")

    # If the endpoint does not include times, do not block the alert.
    if not start or not finish:
        return True

    try:
        now = datetime.now(timezone.utc).timestamp()
        return float(start) <= now <= float(finish)
    except Exception:
        return True


async def fetch_json_for_placement(url, timeout_seconds=15):
    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession()
    try:
        async with session.get(
            url,
            headers={"User-Agent": "MCWV-Bot/1.0", "Accept": "application/json"},
            timeout=aiohttp.ClientTimeout(total=timeout_seconds),
        ) as res:
            if res.status != 200:
                print(f"[placement] HTTP {res.status} for {url}")
                return None
            return await res.json(content_type=None)
    except Exception as exc:
        print(f"[placement] JSON fetch failed for {url}: {exc}")
        return None


async def get_big_games_index_clan_overview():
    # This copies the same position source used by the Hub /api/war route.
    # The db.biggames.io overview page contains MCWV's live battle points and Place.
    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession()

    url = f"https://db.biggames.io/clans/leaderboard?sort=Points&item={CLAN_NAME}&tab=overview"

    try:
        async with session.get(
            url,
            headers={"User-Agent": "MCWV-Bot/1.0", "Accept": "text/html"},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as res:
            if res.status != 200:
                print(f"[placement] Big Games index HTTP {res.status}")
                return None
            html = await res.text()

        escaped = re.search(r'\\"BattleID\\",\\"Points\\",(\d+),\\"PointContributions\\",[\s\S]*?\\"Place\\",(\d+)', html)
        plain = re.search(r'"BattleID","Points",(\d+),"PointContributions",[\s\S]*?"Place",(\d+)', html)
        match = escaped or plain

        if not match:
            print("[placement] Big Games index did not expose Points/Place")
            return None

        return {
            "points": int(match.group(1)),
            "rank": int(match.group(2)),
        }
    except Exception as exc:
        print(f"[placement] Big Games index fetch failed: {exc}")
        return None


async def get_active_battle_id_for_placement():
    # Prefer the v1 endpoint used by the Hub, then fall back to the legacy active battle endpoint.
    for url in (f"{PS99_API}/v1/clans/players", ACTIVE_BATTLE_API):
        payload = await fetch_json_for_placement(url)
        if not payload:
            continue

        battle_id = pick_active_battle_id(payload)
        if not battle_id:
            continue

        if not battle_is_live(payload):
            return None

        return battle_id

    return None


async def get_mcwv_placement_snapshot():
    battle_id = await get_active_battle_id_for_placement()
    if not battle_id:
        return None

    # Same primary source as the website: db.biggames.io overview.
    index_overview = await get_big_games_index_clan_overview()

    clan_payload = await fetch_json_for_placement(CLAN_API) if CLAN_API else None
    data = clan_payload.get("data", {}) if isinstance(clan_payload, dict) else {}
    battles = (data.get("Battles") or {}) if isinstance(data, dict) else {}
    battle = (battles.get(battle_id) or battles.get(str(battle_id))) if isinstance(battles, dict) else None

    if not battle and isinstance(battles, dict) and battles:
        norm = str(battle_id).lower()
        battle = next((value for key, value in battles.items() if str(key).lower() == norm), None)

    fallback_rank = pick_first_int(
        battle,
        (
            "Place",
            "place",
            "Placement",
            "placement",
            "Rank",
            "rank",
            "Position",
            "position",
            "ClanPlacement",
            "LeaderboardPosition",
            "reportedPlace",
        ),
    )
    fallback_points = pick_first_int(
        battle,
        ("Points", "points", "BattlePoints", "battlePoints", "Score", "score", "Value", "value"),
    )

    rank = (index_overview or {}).get("rank") or fallback_rank
    points = (index_overview or {}).get("points") or fallback_points or 0

    if not rank:
        print(f"[placement] no rank found for {battle_id} (index={index_overview}, battle_keys={list(battle.keys()) if isinstance(battle, dict) else None})")
        return None

    return {
        "battleId": battle_id,
        "rank": int(rank),
        "points": int(points or 0),
        "icon": data.get("Icon") if isinstance(data, dict) else None,
        "name": data.get("Name") if isinstance(data, dict) else CLAN_NAME,
    }


def _load_card_fonts():
    def font(size, bold=True):
        paths = [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
        ]
        for path in paths:
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                continue
        return ImageFont.load_default()
    return {
        "title": font(74),
        "pill": font(38),
        "rank": font(106),
        "label": font(52, False),
        "points": font(58),
        "logo": font(34),
    }


def draw_text_shadow(draw, xy, text, font, fill, shadow=(0, 0, 0, 150), offset=(4, 5)):
    x, y = xy
    draw.text((x + offset[0], y + offset[1]), text, font=font, fill=shadow)
    draw.text((x, y), text, font=font, fill=fill)


def rounded_mask(size, radius):
    mask = Image.new("L", size, 0)
    ImageDraw.Draw(mask).rounded_rectangle((0, 0, size[0]-1, size[1]-1), radius=radius, fill=255)
    return mask



# ---------------- CLAN LOGS ----------------
CLAN_LOG_STATE_KEY = "mcwv_clan_log_state"


def clan_logs_enabled():
    raw = db_get_setting("mcwv_clan_logs_enabled", "1" if MCWV_CLAN_LOGS_ENABLED_DEFAULT else "0")
    return str(raw).lower() not in ("0", "false", "off", "no")


def get_clan_log_channel_id():
    saved = db_get_setting("mcwv_clan_log_channel_id", None)
    try:
        return int(saved or MCWV_LOG_CHANNEL_ID or 0)
    except Exception:
        return int(MCWV_LOG_CHANNEL_ID or 0)


def set_clan_log_channel_id(channel_id):
    db_set_setting("mcwv_clan_log_channel_id", int(channel_id))


def load_clan_log_state():
    raw = db_get_setting(CLAN_LOG_STATE_KEY, "")
    try:
        return json.loads(raw) if raw else None
    except Exception:
        return None


def save_clan_log_state(state):
    db_set_setting(CLAN_LOG_STATE_KEY, json.dumps(state))


def format_log_number(value):
    try:
        value = float(value or 0)
    except Exception:
        value = 0
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}b"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}m"
    if value >= 1_000:
        return f"{value / 1_000:.2f}k"
    return str(int(value))


def get_clan_owner_id_from_data(data):
    if not isinstance(data, dict):
        return None
    owner_id = data.get("Owner") or data.get("owner") or data.get("OwnerUserID") or data.get("ownerUserId")
    try:
        return str(int(owner_id)) if owner_id is not None and str(owner_id).strip() else None
    except Exception:
        return None


def extract_clan_members(data):
    members = {}
    for member in data.get("Members", []) if isinstance(data, dict) else []:
        try:
            user_id = str(int(member.get("UserID")))
            members[user_id] = {
                "joinTime": int(member.get("JoinTime") or 0),
                "permissionLevel": int(member.get("PermissionLevel") or 0),
            }
        except Exception:
            continue

    # BIG Games legacy clan data can list Members without counting the owner.
    # Include Owner so cards/report counts show the true in-game clan size, e.g. 75/75.
    owner_id = get_clan_owner_id_from_data(data)
    if owner_id and owner_id not in members:
        members[owner_id] = {
            "joinTime": int(data.get("Created") or 0) if isinstance(data, dict) else 0,
            "permissionLevel": 100,
            "isOwner": True,
        }

    return members


def extract_clan_diamonds(data):
    contributions = {}
    root = data.get("DiamondContributions", {}) if isinstance(data, dict) else {}
    all_time = root.get("AllTime", {}) if isinstance(root, dict) else {}

    for entry in all_time.get("Data", []) if isinstance(all_time, dict) else []:
        try:
            user_id = str(int(entry.get("UserID")))
            diamonds = int(entry.get("Diamonds") or 0)
            contributions[user_id] = diamonds
        except Exception:
            continue

    total = int(all_time.get("Sum") or data.get("DepositedDiamonds") or sum(contributions.values()) or 0)
    return contributions, total


def build_clan_log_state(data):
    members = extract_clan_members(data)
    diamonds, total_diamonds = extract_clan_diamonds(data)
    return {
        "members": members,
        "diamonds": diamonds,
        "totalDiamonds": total_diamonds,
        "memberCapacity": int(data.get("MemberCapacity") or 0) if isinstance(data, dict) else 0,
        "updatedAt": datetime.now(timezone.utc).isoformat(),
    }


def display_user_label(user_info, fallback_id=None):
    name = str((user_info or {}).get("name") or fallback_id or "Unknown")
    display = str((user_info or {}).get("displayName") or name)
    return f"{display} ({name})"


def fit_text(draw, text, font, max_width):
    text = str(text)
    if draw.textbbox((0, 0), text, font=font)[2] <= max_width:
        return text
    ellipsis = "…"
    while text and draw.textbbox((0, 0), text + ellipsis, font=font)[2] > max_width:
        text = text[:-1]
    return text + ellipsis if text else ellipsis


async def fetch_roblox_users_for_logs(user_ids):
    ids = []
    for value in user_ids:
        try:
            uid = int(value)
            if uid not in ids:
                ids.append(uid)
        except Exception:
            continue

    user_map = {}

    # Prefer local DB names when available so logs still work during Roblox hiccups.
    try:
        for row in db_get_all_tracked() or []:
            try:
                rid = int(row[0])
                username = str(row[2]) if len(row) > 2 and row[2] else str(rid)
                if rid in ids:
                    user_map[rid] = {"name": username, "displayName": username}
            except Exception:
                continue
    except Exception:
        pass

    missing = [uid for uid in ids if uid not in user_map]
    if not missing:
        return user_map

    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession()

    for index in range(0, len(missing), 100):
        chunk = missing[index:index + 100]
        try:
            async with session.post(
                ROBLOX_USERS_API,
                json={"userIds": chunk, "excludeBannedUsers": False},
                headers={"User-Agent": "MCWV-Bot/1.0", "Accept": "application/json"},
                timeout=aiohttp.ClientTimeout(total=12),
            ) as res:
                if res.status != 200:
                    print(f"[clan logs] Roblox users HTTP {res.status}")
                    continue
                payload = await res.json(content_type=None)

            for user in payload.get("data", []) if isinstance(payload, dict) else []:
                try:
                    uid = int(user.get("id"))
                    user_map[uid] = {
                        "name": str(user.get("name") or uid),
                        "displayName": str(user.get("displayName") or user.get("name") or uid),
                    }
                except Exception:
                    continue
        except Exception as exc:
            print(f"[clan logs] Roblox user lookup failed: {exc}")

    return user_map


async def fetch_roblox_headshot_for_logs(user_id, size=320):
    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession()

    try:
        url = (
            "https://thumbnails.roblox.com/v1/users/avatar-headshot"
            f"?userIds={int(user_id)}&size=720x720&format=Png&isCircular=false"
        )
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as res:
            if res.status != 200:
                return None
            payload = await res.json(content_type=None)

        image_url = ((payload.get("data") or [{}])[0] or {}).get("imageUrl") if isinstance(payload, dict) else None
        if not image_url:
            return None

        async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=12)) as res:
            if res.status != 200:
                return None
            image_bytes = await res.read()

        avatar = Image.open(BytesIO(image_bytes)).convert("RGBA")
        return avatar.resize((size, size), Image.Resampling.LANCZOS)
    except Exception as exc:
        print(f"[clan logs] avatar fetch failed for {user_id}: {exc}")
        return None


def draw_member_icon(draw, box, accent, S):
    x1, y1, x2, y2 = box
    # Use fully opaque blended colours here. ImageDraw does not alpha-composite
    # semi-transparent fills; it replaces pixels, which made the icon blocks look
    # like bright empty squares in Discord.
    bg = (
        max(24, int(accent[0] * 0.22)),
        max(22, int(accent[1] * 0.22)),
        max(42, int(accent[2] * 0.30)),
        255,
    )
    draw.rounded_rectangle(box, radius=int(14 * S), fill=bg, outline=(*accent, 255), width=int(1.5 * S))
    cx = (x1 + x2) // 2
    head_r = int(9 * S)
    head_y = y1 + int(20 * S)
    stroke = int(4.5 * S)
    draw.ellipse((cx - head_r, head_y - head_r, cx + head_r, head_y + head_r), outline=(*accent, 255), width=stroke)
    draw.arc((cx - int(19 * S), y1 + int(31 * S), cx + int(19 * S), y1 + int(67 * S)), 200, 340, fill=(*accent, 255), width=stroke)


def draw_arrow_icon(draw, box, accent, S):
    x1, y1, x2, y2 = box
    bg = (
        max(24, int(accent[0] * 0.22)),
        max(22, int(accent[1] * 0.22)),
        max(42, int(accent[2] * 0.30)),
        255,
    )
    draw.rounded_rectangle(box, radius=int(14 * S), fill=bg, outline=(*accent, 255), width=int(1.5 * S))
    cx = (x1 + x2) // 2
    cy = (y1 + y2) // 2
    stroke = int(4.5 * S)
    draw.line((cx - int(13 * S), cy, cx + int(13 * S), cy), fill=(*accent, 255), width=stroke)
    draw.line((cx + int(2 * S), cy - int(11 * S), cx + int(14 * S), cy, cx + int(2 * S), cy + int(11 * S)), fill=(*accent, 255), width=stroke, joint="curve")


async def generate_clan_member_log_card(kind, user_id, user_info, member_count, member_capacity):
    # Match the cleaner CW-Bot style: simple dark card, big title, subtle icons,
    # large avatar ring. No watermark/panels/chips.
    S = 2
    W, H = 1250 * S, 420 * S
    joined = kind == "joined"

    accent = (74, 222, 128) if joined else (255, 84, 96)
    ring = (190, 70, 255) if joined else (255, 118, 138)
    label_color = (190, 78, 255) if joined else (255, 110, 135)
    small_label = "NEW MEMBER" if joined else "MEMBER LEFT"
    title = "Player Joined" if joined else "Player Left"
    verb = "joined" if joined else "left"

    def sc(value):
        return int(round(value * S))

    # Keep default/system fonts as requested.
    def font(size, bold=True):
        path = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
        try:
            return ImageFont.truetype(path, sc(size))
        except Exception:
            return ImageFont.load_default()

    fonts = {
        "eyebrow": font(24, True),
        "title": font(82, True),
        "meta": font(37, True),
        "meta_regular": font(34, False),
        "body_bold": font(29, True),
        "body": font(29, False),
    }

    img = Image.new("RGBA", (W, H), (0, 0, 0, 0))

    # Shadow.
    shadow = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    sd = ImageDraw.Draw(shadow)
    card_box = (sc(8), sc(8), W - sc(8), H - sc(12))
    sd.rounded_rectangle(card_box, radius=sc(36), fill=(0, 0, 0, 150))
    shadow = shadow.filter(ImageFilter.GaussianBlur(sc(7)))
    img.alpha_composite(shadow)

    await asyncio.sleep(0)

    # Clean dark background similar to the reference card.
    card = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    cd = ImageDraw.Draw(card)
    for x in range(W):
        t = x / max(W - 1, 1)
        r = int(11 + 6 * (1 - t))
        g = int(14 + 7 * (1 - t))
        b = int(36 + 14 * (1 - t))
        cd.line((x, 0, x, H), fill=(r, g, b, 255))

    # Soft glows only; no heavy panels/diagonal shapes.
    fx = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    fd = ImageDraw.Draw(fx)
    fd.ellipse((sc(-165), sc(155), sc(420), sc(615)), fill=(102, 45, 165, 48))
    fd.ellipse((sc(760), sc(-135), sc(1335), sc(520)), fill=(*ring, 38))
    fx = fx.filter(ImageFilter.GaussianBlur(sc(42)))
    card.alpha_composite(fx)

    mask = rounded_mask((W - sc(16), H - sc(20)), sc(36))
    shaped = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    shaped.paste(card.crop((sc(8), sc(8), W - sc(8), H - sc(12))), (sc(8), sc(8)), mask)
    img.alpha_composite(shaped)

    d = ImageDraw.Draw(img)
    d.rounded_rectangle(card_box, radius=sc(36), outline=(225, 230, 255, 185), width=sc(2))
    d.rounded_rectangle((sc(11), sc(11), W - sc(11), H - sc(15)), radius=sc(33), outline=(255, 255, 255, 28), width=sc(1))

    # Yield after the heaviest compositing (per-pixel gradient + sc(42) blur)
    # so the event loop can ack Discord heartbeats. Critical here because this
    # generator runs in a loop (one card per joined/left member).
    await asyncio.sleep(0)

    # Text positions copied from the clean reference layout.
    draw_text_shadow(d, (sc(50), sc(42)), small_label, fonts["eyebrow"], (*label_color, 255), shadow=(0, 0, 0, 115), offset=(sc(2), sc(2)))
    draw_text_shadow(d, (sc(50), sc(100)), title, fonts["title"], (*accent, 255), shadow=(0, 0, 0, 145), offset=(sc(4), sc(5)))

    # Subtle small icon tiles. Use alpha compositing so they blend instead of becoming bright squares.
    def icon_tile(box, draw_symbol):
        layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        ld = ImageDraw.Draw(layer)
        ld.rounded_rectangle(box, radius=sc(14), fill=(*label_color, 22))
        img.alpha_composite(layer)
        dd = ImageDraw.Draw(img)
        draw_symbol(dd, box)
        return dd

    def member_symbol(dd, box):
        x1, y1, x2, y2 = box
        cx = (x1 + x2) // 2
        head_r = sc(10)
        head_y = y1 + sc(20)
        dd.ellipse((cx - head_r, head_y - head_r, cx + head_r, head_y + head_r), outline=(*label_color, 255), width=sc(4))
        dd.arc((cx - sc(22), y1 + sc(30), cx + sc(22), y1 + sc(70)), 200, 340, fill=(*label_color, 255), width=sc(4))

    def arrow_symbol(dd, box):
        x1, y1, x2, y2 = box
        cx = (x1 + x2) // 2
        cy = (y1 + y2) // 2
        dd.line((cx - sc(12), cy, cx + sc(13), cy), fill=(*label_color, 255), width=sc(4))
        dd.line((cx + sc(2), cy - sc(10), cx + sc(13), cy, cx + sc(2), cy + sc(10)), fill=(*label_color, 255), width=sc(4), joint="curve")

    icon_tile((sc(50), sc(210), sc(108), sc(268)), member_symbol)
    d = ImageDraw.Draw(img)
    count_text = f"{int(member_count or 0)}/{int(member_capacity or 0) if member_capacity else '?'}"
    count_x = sc(112)
    count_y = sc(218)
    draw_text_shadow(d, (count_x, count_y), count_text, fonts["meta"], (252, 252, 255, 255), shadow=(0, 0, 0, 130), offset=(sc(2), sc(2)))
    count_right = d.textbbox((count_x, count_y), count_text, font=fonts["meta"])[2]
    d.text((count_right + sc(22), sc(224)), "Members", font=fonts["meta_regular"], fill=(190, 188, 205, 255))

    icon_tile((sc(50), sc(315), sc(108), sc(373)), arrow_symbol)
    d = ImageDraw.Draw(img)
    label = fit_text(d, display_user_label(user_info, user_id), fonts["body_bold"], sc(510))
    x = sc(112)
    y = sc(326)
    draw_text_shadow(d, (x, y), label, fonts["body_bold"], (255, 255, 255, 255), shadow=(0, 0, 0, 130), offset=(sc(2), sc(2)))
    label_w = d.textbbox((x, y), label, font=fonts["body_bold"])[2] - x
    verb_text = f" {verb} "
    d.text((x + label_w + sc(8), y), verb_text, font=fonts["body"], fill=(188, 186, 203, 255))
    verb_w = d.textbbox((0, 0), verb_text, font=fonts["body"])[2]
    d.text((x + label_w + sc(8) + verb_w, y), f"[{CLAN_NAME}].", font=fonts["body"], fill=(155, 155, 255, 255))

    # Avatar ring like the reference.
    center = (sc(1045), sc(210))
    outer_r = sc(151)
    inner_r = sc(126)

    ring_glow = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    rg = ImageDraw.Draw(ring_glow)
    rg.ellipse((center[0] - outer_r, center[1] - outer_r, center[0] + outer_r, center[1] + outer_r), fill=(*ring, 54))
    ring_glow = ring_glow.filter(ImageFilter.GaussianBlur(sc(8)))
    img.alpha_composite(ring_glow)

    d = ImageDraw.Draw(img)
    d.ellipse((center[0] - sc(145), center[1] - sc(145), center[0] + sc(145), center[1] + sc(145)), outline=(*ring, 255), width=sc(8))
    d.ellipse((center[0] - inner_r, center[1] - inner_r, center[0] + inner_r, center[1] + inner_r), fill=(34, 35, 46, 255))

    avatar = await fetch_roblox_headshot_for_logs(user_id, size=sc(244))
    if avatar is None:
        avatar = Image.new("RGBA", (sc(244), sc(244)), (180, 180, 190, 255))
        ad = ImageDraw.Draw(avatar)
        ad.ellipse((sc(82), sc(70), sc(108), sc(96)), fill=(0, 0, 0, 255))
        ad.ellipse((sc(136), sc(70), sc(162), sc(96)), fill=(0, 0, 0, 255))
        ad.arc((sc(78), sc(95), sc(168), sc(180)), 20, 160, fill=(0, 0, 0, 255), width=sc(6))

    avatar_mask = Image.new("L", avatar.size, 0)
    ImageDraw.Draw(avatar_mask).ellipse((0, 0, avatar.size[0] - 1, avatar.size[1] - 1), fill=255)
    img.paste(avatar, (center[0] - avatar.size[0] // 2, center[1] - avatar.size[1] // 2), avatar_mask)

    await asyncio.sleep(0)

    img = img.resize((1250, 420), Image.Resampling.LANCZOS)
    out = BytesIO()
    img.save(out, format="PNG")
    out.seek(0)
    return out

async def send_clan_member_log(channel, kind, user_id, user_info, member_count, member_capacity):
    image = await generate_clan_member_log_card(kind, user_id, user_info, member_count, member_capacity)
    filename = f"mcwv-player-{kind}.png"
    file = discord.File(image, filename=filename)
    embed = discord.Embed(
        color=discord.Color.green() if kind == "joined" else discord.Color.red(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_image(url=f"attachment://{filename}")
    # Discord renders descriptions above embed images. Footer is the clean way to
    # keep the user text below the generated card.
    embed.set_footer(text=f"User: {display_user_label(user_info, user_id)}")
    await channel.send(embed=embed, file=file)


async def send_diamond_log(channel, data, user_id, user_info, donated, new_total, clan_total):
    clan_name = str(data.get("Name") or CLAN_NAME).lower()
    icon_asset = extract_asset_id(data.get("Icon"))
    embed = discord.Embed(
        title=f"Diamond Update • {clan_name}",
        description=(
            f"{display_user_label(user_info, user_id)} donated\n"
            f"**{format_log_number(donated)} 💎** (Total: {format_log_number(new_total)})\n"
            f"Clan Diamonds: **{format_log_number(clan_total)} 💎**"
        ),
        color=discord.Color.gold(),
        timestamp=datetime.now(timezone.utc),
    )
    if icon_asset:
        embed.set_thumbnail(url=f"{PS99_API}/image/{icon_asset}")
    await channel.send(embed=embed)


# ---------------- HOURLY STATS CARD ----------------
def normalize_hourly_battle_key(value):
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def format_hourly_points(value):
    try:
        value = float(value or 0)
    except Exception:
        value = 0
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{value / 1_000:.2f}K"
    return str(int(value))


def hourly_colour(index, total, zero=False):
    if zero:
        return (242, 78, 94)
    if total <= 1:
        return (74, 222, 128)
    t = max(0.0, min(1.0, index / max(total - 1, 1)))
    stops = [
        (0.0, (74, 222, 128)),
        (0.42, (224, 189, 46)),
        (0.72, (246, 133, 37)),
        (1.0, (242, 78, 94)),
    ]
    for idx in range(len(stops) - 1):
        left_t, left = stops[idx]
        right_t, right = stops[idx + 1]
        if left_t <= t <= right_t:
            span = max(right_t - left_t, 0.0001)
            local = (t - left_t) / span
            return tuple(int(left[i] + (right[i] - left[i]) * local) for i in range(3))
    return stops[-1][1]


def cover_image(path, size):
    target_w, target_h = size
    try:
        img = Image.open(path).convert("RGBA")
        scale = max(target_w / img.width, target_h / img.height)
        resized = img.resize((int(img.width * scale), int(img.height * scale)), Image.Resampling.LANCZOS)
        left = (resized.width - target_w) // 2
        top = (resized.height - target_h) // 2
        return resized.crop((left, top, left + target_w, top + target_h))
    except Exception as exc:
        print(f"[hourly stats] background load failed: {exc}")
        return Image.new("RGBA", size, (8, 10, 26, 255))


def to_ms_for_hourly(value):
    try:
        if isinstance(value, datetime):
            dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)
    except Exception:
        return 0


def points_at_time_for_hourly(rows, target_ms):
    if not rows:
        return None
    rows = sorted(rows, key=lambda item: item["time"])
    if target_ms < rows[0]["time"]:
        # No full 60-minute baseline yet. Return None instead of faking a rate.
        return None
    for idx, current in enumerate(rows):
        current_ms = current["time"]
        if current_ms == target_ms:
            return current["points"]
        nxt = rows[idx + 1] if idx + 1 < len(rows) else None
        if not nxt:
            return current["points"]
        if current_ms <= target_ms <= nxt["time"]:
            span = max(nxt["time"] - current_ms, 1)
            ratio = (target_ms - current_ms) / span
            return current["points"] + (nxt["points"] - current["points"]) * ratio
    return rows[-1]["points"]


def fetch_hourly_points_from_history(battle_id, user_ids, current_points=None, battle_start_ts=None):
    """Return PPH exactly like the Hub leaderboard profile cards.
    Uses a local DB connection to avoid race conditions with other loops."""
    ids = [str(value) for value in user_ids if str(value).strip()]
    if not ids or not DATABASE_URL:
        return {}

    result = {rid: {"pph": 0, "ready": False} for rid in ids}
    pass
    try:
        ensure_db_connection()
        battle_keys = sorted({str(battle_id), normalize_hourly_battle_key(battle_id)})
        grouped = {rid: [] for rid in ids}

        with conn.cursor() as cur:
            cur.execute("""
                SELECT to_regclass('public.player_leaderboard_history') IS NOT NULL AS exists
            """)
            if not bool(cur.fetchone()[0]):
                return result

            cur.execute("""
                SELECT roblox_id::text, points::bigint, captured_at
                FROM player_leaderboard_history
                WHERE battle_id = ANY(%s)
                  AND roblox_id::text = ANY(%s)
                  AND points IS NOT NULL
                ORDER BY roblox_id::text ASC, captured_at ASC
            """, (battle_keys, ids))

            for roblox_id, points, captured_at in cur.fetchall():
                grouped.setdefault(str(roblox_id), []).append({
                    "points": int(points or 0),
                    "time": to_ms_for_hourly(captured_at),
                })

        for rid, rows in grouped.items():
            rows = [row for row in rows if row.get("time")]
            if len(rows) < 2:
                result[rid] = {"pph": 0, "ready": False}
                continue

            rows.sort(key=lambda item: item["time"])
            latest = rows[-1]
            latest_points = int(latest["points"] or 0)
            hourly_cutoff = int(latest["time"] or 0) - 60 * 60 * 1000

            baseline = points_at_time_for_hourly(rows, hourly_cutoff)
            if baseline is None:
                result[rid] = {"pph": 0, "ready": False}
                continue

            result[rid] = {
                "pph": max(0, int(round(latest_points - baseline))),
                "ready": True,
            }

        return result
    except Exception as exc:
        print(f"[hourly stats] history lookup failed: {exc}")
        return result
    finally:
        pass


def save_hourly_player_snapshot(payload):
    if not DATABASE_URL or not isinstance(payload, dict):
        return

    entries = payload.get("entries") or []
    if not entries:
        return

    pass
    try:
        ensure_db_connection()
        battle_key = normalize_hourly_battle_key(payload.get("battleId"))
        if not battle_key:
            return

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS player_leaderboard_history (
                    id BIGSERIAL PRIMARY KEY,
                    battle_id TEXT,
                    roblox_id TEXT NOT NULL,
                    username TEXT,
                    rank INTEGER,
                    points BIGINT,
                    pph NUMERIC,
                    change_5m BIGINT,
                    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                SELECT MAX(captured_at)
                FROM player_leaderboard_history
                WHERE battle_id = %s
            """, (battle_key,))
            last = cur.fetchone()[0]
            if last:
                last_dt = last if getattr(last, "tzinfo", None) else last.replace(tzinfo=timezone.utc)
                if (datetime.now(timezone.utc) - last_dt).total_seconds() < 60:
                    return

            values = []
            for _hourly_rank, entry in enumerate(entries, start=1):
                values.append((
                    battle_key,
                    str(entry.get("robloxId") or ""),
                    str(entry.get("name") or entry.get("robloxId") or "Unknown"),
                    int(entry.get("currentRank") or 0),
                    int(entry.get("points") or 0),
                    int(entry.get("pph") or 0),
                    0,
                ))

            if values:
                cur.executemany("""
                    INSERT INTO player_leaderboard_history
                        (battle_id, roblox_id, username, rank, points, pph, change_5m, captured_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                """, values)
        conn.commit()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[hourly stats] snapshot save failed: {exc}")
    finally:
        pass


def draw_gradient_text_on_image(img, xy, text, font, left_color, right_color, shadow=True):
    x, y = xy
    dummy = ImageDraw.Draw(img)
    bbox = dummy.textbbox((0, 0), text, font=font)
    text_w = bbox[2] - bbox[0]
    text_h = bbox[3] - bbox[1]
    if text_w <= 0 or text_h <= 0:
        return
    if shadow:
        dummy.text((x + 3, y + 4), text, font=font, fill=(0, 0, 0, 150))
    mask = Image.new("L", (text_w + 4, text_h + 4), 0)
    md = ImageDraw.Draw(mask)
    md.text((2 - bbox[0], 2 - bbox[1]), text, font=font, fill=255)
    gradient = Image.new("RGBA", mask.size, (0, 0, 0, 0))
    gd = ImageDraw.Draw(gradient)
    for px in range(mask.size[0]):
        t = px / max(mask.size[0] - 1, 1)
        color = tuple(int(left_color[i] + (right_color[i] - left_color[i]) * t) for i in range(3))
        gd.line((px, 0, px, mask.size[1]), fill=(*color, 255))
    img.paste(gradient, (x + bbox[0] - 2, y + bbox[1] - 2), mask)


def hourly_exact_slot_iso(value):
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).replace(second=0, microsecond=0).isoformat()


def ensure_hourly_exact_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hourly_stats_player_snapshots (
            id BIGSERIAL PRIMARY KEY,
            battle_id TEXT NOT NULL,
            roblox_id TEXT NOT NULL,
            username TEXT,
            rank INTEGER,
            points BIGINT NOT NULL DEFAULT 0,
            scheduled_at TIMESTAMPTZ NOT NULL,
            captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS hourly_stats_player_snapshots_unique_idx
        ON hourly_stats_player_snapshots (battle_id, roblox_id, scheduled_at)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS hourly_stats_player_snapshots_battle_time_idx
        ON hourly_stats_player_snapshots (battle_id, scheduled_at DESC)
    """)


def save_hourly_exact_snapshot(battle_id, scheduled_at, entries):
    if not DATABASE_URL or not entries:
        return False
    pass
    try:
        ensure_db_connection()
        battle_key = normalize_hourly_battle_key(battle_id)
        scheduled_dt = scheduled_at if isinstance(scheduled_at, datetime) else datetime.fromisoformat(str(scheduled_at).replace("Z", "+00:00"))
        scheduled_dt = scheduled_dt.astimezone(timezone.utc).replace(second=0, microsecond=0)
        with conn.cursor() as cur:
            ensure_hourly_exact_table(cur)
            values = []
            for entry in entries:
                values.append((
                    battle_key,
                    str(entry.get("robloxId") or ""),
                    str(entry.get("name") or entry.get("robloxId") or "Unknown"),
                    int(entry.get("currentRank") or 0),
                    int(entry.get("points") or 0),
                    scheduled_dt,
                ))
            cur.executemany("""
                INSERT INTO hourly_stats_player_snapshots
                    (battle_id, roblox_id, username, rank, points, scheduled_at, captured_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (battle_id, roblox_id, scheduled_at)
                DO UPDATE SET
                    username = EXCLUDED.username,
                    rank = EXCLUDED.rank,
                    points = EXCLUDED.points,
                    captured_at = NOW()
            """, values)
        conn.commit()
        return True
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[hourly stats] exact snapshot save failed: {exc}")
        return False
    finally:
        pass


def load_hourly_exact_entries(battle_id, scheduled_at):
    if not DATABASE_URL:
        return None
    pass
    try:
        ensure_db_connection()
        battle_key = normalize_hourly_battle_key(battle_id)
        scheduled_dt = scheduled_at if isinstance(scheduled_at, datetime) else datetime.fromisoformat(str(scheduled_at).replace("Z", "+00:00"))
        scheduled_dt = scheduled_dt.astimezone(timezone.utc).replace(second=0, microsecond=0)
        previous_dt = scheduled_dt - timedelta(minutes=max(1, int(MCWV_HOURLY_STATS_INTERVAL_MINUTES or 60)))
        with conn.cursor() as cur:
            ensure_hourly_exact_table(cur)
            cur.execute("""
                SELECT
                    c.roblox_id,
                    c.username,
                    c.rank,
                    c.points,
                    p.points AS previous_points
                FROM hourly_stats_player_snapshots c
                LEFT JOIN hourly_stats_player_snapshots p
                  ON p.battle_id = c.battle_id
                 AND p.roblox_id = c.roblox_id
                 AND p.scheduled_at = %s
                WHERE c.battle_id = %s
                  AND c.scheduled_at = %s
                ORDER BY COALESCE(c.rank, 999999), LOWER(c.username), c.roblox_id
            """, (previous_dt, battle_key, scheduled_dt))
            rows = cur.fetchall()
        if not rows:
            return None
        entries = []
        for roblox_id, username, rank, points, previous_points in rows:
            ready = previous_points is not None
            pph = max(0, int(points or 0) - int(previous_points or 0)) if ready else 0
            entries.append({
                "robloxId": str(roblox_id),
                "name": str(username or roblox_id),
                "displayName": str(username or roblox_id),
                "points": int(points or 0),
                "currentRank": int(rank or 0) if rank else None,
                "pph": pph,
                "pphReady": ready,
            })
        return entries
    except Exception as exc:
        print(f"[hourly stats] exact snapshot load failed: {exc}")
        return None
    finally:
        pass


def get_latest_hourly_exact_slot(battle_id=None):
    if not DATABASE_URL:
        return None
    pass
    try:
        ensure_db_connection()
        with conn.cursor() as cur:
            ensure_hourly_exact_table(cur)
            if battle_id:
                cur.execute("""
                    SELECT MAX(scheduled_at)
                    FROM hourly_stats_player_snapshots
                    WHERE battle_id = %s
                """, (normalize_hourly_battle_key(battle_id),))
            else:
                cur.execute("SELECT MAX(scheduled_at) FROM hourly_stats_player_snapshots")
            row = cur.fetchone()
        return row[0] if row and row[0] else None
    except Exception:
        return None
    finally:
        pass
        return None


async def build_hourly_stats_payload(scheduled_at=None, use_latest_exact=False):
    battle_id = await get_active_battle_id_for_placement()
    if not battle_id:
        return None

    scheduled_dt = None
    if use_latest_exact:
        scheduled_dt = get_latest_hourly_exact_slot(battle_id)
    if scheduled_dt is None:
        scheduled_dt = scheduled_at or datetime.now(timezone.utc).replace(second=0, microsecond=0)
    if not isinstance(scheduled_dt, datetime):
        scheduled_dt = datetime.fromisoformat(str(scheduled_dt).replace("Z", "+00:00"))
    scheduled_dt = scheduled_dt.astimezone(timezone.utc).replace(second=0, microsecond=0)

    active_payload = await fetch_json_for_placement(ACTIVE_BATTLE_API)
    active_data = active_payload.get("data", {}) if isinstance(active_payload, dict) else {}
    active_config = active_data.get("configData", {}) if isinstance(active_data, dict) else {}
    battle_start_ts = placement_int(active_config.get("StartTime") or active_data.get("startTime"))
    if battle_start_ts and battle_start_ts > 10_000_000_000:
        battle_start_ts = battle_start_ts // 1000

    clan_payload = await fetch_json_for_placement(CLAN_API) if CLAN_API else None
    data = clan_payload.get("data", {}) if isinstance(clan_payload, dict) else {}
    if not isinstance(data, dict) or not data:
        return None

    battles = data.get("Battles") or {}
    battle = battles.get(battle_id) or battles.get(str(battle_id)) if isinstance(battles, dict) else None
    if not battle and isinstance(battles, dict):
        norm = normalize_hourly_battle_key(battle_id)
        battle = next((value for key, value in battles.items() if normalize_hourly_battle_key(key) == norm), None)
    if not isinstance(battle, dict):
        return None

    contributions = battle.get("PointContributions") or battle.get("pointContributions") or []
    current_points = {}
    for entry in contributions if isinstance(contributions, list) else []:
        try:
            rid = str(int(entry.get("UserID") or entry.get("userId") or 0))
            current_points[rid] = int(entry.get("Points") or entry.get("points") or 0)
        except Exception:
            continue

    members = extract_clan_members(data)
    roster_ids = set(members.keys()) or set(current_points.keys())
    user_ids = sorted(roster_ids, key=lambda item: int(item) if str(item).isdigit() else 0)
    if not user_ids:
        return None

    users = await fetch_roblox_users_for_logs(user_ids)

    base_entries = []
    for rid in user_ids:
        info = users.get(int(rid), {}) if str(rid).isdigit() else {}
        name = str(info.get("name") or rid)
        base_entries.append({
            "robloxId": rid,
            "name": name,
            "displayName": str(info.get("displayName") or name),
            "points": int(current_points.get(rid, 0) or 0),
            "pph": 0,
        })

    war_rank_by_id = {
        entry["robloxId"]: index + 1
        for index, entry in enumerate(sorted(base_entries, key=lambda item: (-int(item.get("points") or 0), item["name"].lower())))
    }
    for entry in base_entries:
        entry["currentRank"] = war_rank_by_id.get(entry["robloxId"])

    # For the exact scheduled hourly card, save one snapshot for the scheduled slot,
    # then compare only against the previous scheduled slot. No interpolation, no
    # current rolling window: exactly one interval boundary to the next.
    save_hourly_exact_snapshot(battle_id, scheduled_dt, base_entries)
    save_hourly_player_snapshot({"battleId": battle_id, "entries": base_entries})

    entries = load_hourly_exact_entries(battle_id, scheduled_dt) or base_entries
    for entry in entries:
        entry.setdefault("pphReady", False)
        entry.setdefault("pph", 0)

    entries.sort(key=lambda item: (not bool(item.get("pphReady")), -int(item.get("pph") or 0), item.get("name", "").lower()))

    overview = await get_big_games_index_clan_overview()
    clan_rank = (overview or {}).get("rank") or pick_first_int(battle, ("Place", "place", "Rank", "rank", "Position", "position"))
    total_hourly = sum(int(entry.get("pph") or 0) for entry in entries if entry.get("pphReady"))

    return {
        "battleId": battle_id,
        "scheduledAt": scheduled_dt.isoformat(),
        "clanName": str(data.get("Name") or CLAN_NAME),
        "icon": data.get("Icon"),
        "rank": clan_rank,
        "players": len(entries),
        "active": sum(1 for entry in entries if entry.get("pphReady") and int(entry.get("pph") or 0) > 0),
        "zero": sum(1 for entry in entries if entry.get("pphReady") and int(entry.get("pph") or 0) <= 0),
        "warmingUp": sum(1 for entry in entries if not entry.get("pphReady")),
        "hourlyPoints": total_hourly,
        "entries": entries,
    }


async def collect_hourly_player_snapshot():
    """Save current player leaderboard points without sending a card.

    This runs every minute so the 60-minute PPH window has real history even if
    nobody opens the website leaderboard.
    """
    battle_id = await get_active_battle_id_for_placement()
    if not battle_id:
        return False

    clan_payload = await fetch_json_for_placement(CLAN_API) if CLAN_API else None
    data = clan_payload.get("data", {}) if isinstance(clan_payload, dict) else {}
    if not isinstance(data, dict) or not data:
        return False

    battles = data.get("Battles") or {}
    battle = battles.get(battle_id) or battles.get(str(battle_id)) if isinstance(battles, dict) else None
    if not battle and isinstance(battles, dict):
        norm = normalize_hourly_battle_key(battle_id)
        battle = next((value for key, value in battles.items() if normalize_hourly_battle_key(key) == norm), None)
    if not isinstance(battle, dict):
        return False

    contributions = battle.get("PointContributions") or battle.get("pointContributions") or []
    current_points = {}
    for entry in contributions if isinstance(contributions, list) else []:
        try:
            rid = str(int(entry.get("UserID") or entry.get("userId") or 0))
            current_points[rid] = int(entry.get("Points") or entry.get("points") or 0)
        except Exception:
            continue

    members = extract_clan_members(data)
    roster_ids = set(members.keys()) or set(current_points.keys())
    if not roster_ids:
        return False

    names = {}
    try:
        for row in db_get_all_tracked() or []:
            try:
                rid = str(row[0]).strip()
                if rid:
                    names[rid] = str(row[2]).strip() if len(row) > 2 and row[2] else rid
            except Exception:
                continue
    except Exception:
        pass

    base_entries = []
    for rid in sorted(roster_ids, key=lambda item: int(item) if str(item).isdigit() else 0):
        base_entries.append({
            "robloxId": rid,
            "name": names.get(rid, rid),
            "points": int(current_points.get(rid, 0) or 0),
            "pph": 0,
        })

    war_rank_by_id = {
        entry["robloxId"]: index + 1
        for index, entry in enumerate(sorted(base_entries, key=lambda item: (-int(item.get("points") or 0), str(item.get("name") or "").lower())))
    }
    for entry in base_entries:
        entry["currentRank"] = war_rank_by_id.get(entry["robloxId"])

    save_hourly_player_snapshot({"battleId": battle_id, "entries": base_entries})
    return True


async def generate_hourly_stats_card(payload):
    # Polished dashboard-style hourly stats card. Coordinates are authored at
    # 1355x804 and rendered at 2x for crisp Discord output.
    S = 2
    W, H = 1355 * S, 804 * S

    def sc(value):
        return int(round(value * S))

    def font(size, bold=True):
        path = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
        try:
            return ImageFont.truetype(path, sc(size))
        except Exception:
            return ImageFont.load_default()

    fonts = {
        "tag": font(34, True),
        "badge_label": font(10, False),
        "badge_value": font(28, True),
        "stat_label": font(12, False),
        "stat_value": font(40, True),
        "row": font(17, True),
        "row_small": font(15, True),
        "tiny": font(10, False),
    }

    def alpha_round(box, radius, fill, outline=None, width=1, blur=0):
        layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        ld = ImageDraw.Draw(layer)
        ld.rounded_rectangle(
            box,
            radius=sc(radius),
            fill=fill,
            outline=outline,
            width=sc(width) if outline else 1,
        )
        if blur:
            layer = layer.filter(ImageFilter.GaussianBlur(sc(blur)))
        img.alpha_composite(layer)

    def alpha_rect(box, fill, blur=0):
        layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        ld = ImageDraw.Draw(layer)
        ld.rectangle(box, fill=fill)
        if blur:
            layer = layer.filter(ImageFilter.GaussianBlur(sc(blur)))
        img.alpha_composite(layer)

    def centered_text(draw, box, text, font_obj, fill):
        text = str(text)
        bbox = draw.textbbox((0, 0), text, font=font_obj)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        x = box[0] + ((box[2] - box[0]) - tw) // 2 - bbox[0]
        y = box[1] + ((box[3] - box[1]) - th) // 2 - bbox[1]
        draw.text((x, y), text, font=font_obj, fill=fill)

    def left_center_text(draw, box, text, font_obj, fill):
        text = str(text)
        bbox = draw.textbbox((0, 0), text, font=font_obj)
        th = bbox[3] - bbox[1]
        x = box[0] - bbox[0]
        y = box[1] + ((box[3] - box[1]) - th) // 2 - bbox[1]
        draw.text((x, y), text, font=font_obj, fill=fill)

    def right_text(draw, right_x, y, text, font_obj, fill):
        text = str(text)
        bbox = draw.textbbox((0, 0), text, font=font_obj)
        draw.text((right_x - (bbox[2] - bbox[0]), y), text, font=font_obj, fill=fill)

    # Background: user's galaxy asset, darkened but still visible.
    img = cover_image(MCWV_HOURLY_STATS_BG_PATH, (W, H))
    img.alpha_composite(Image.new("RGBA", (W, H), (3, 5, 16, 104)))

    # Faint grid with depth. Use alpha compositing so it never becomes harsh.
    grid = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    gd = ImageDraw.Draw(grid)
    for x in range(0, W, sc(54)):
        gd.line((x, 0, x, H), fill=(125, 145, 205, 16), width=sc(1))
    for y in range(0, H, sc(54)):
        gd.line((0, y, W, y), fill=(125, 145, 205, 12), width=sc(1))
    img.alpha_composite(grid)

    # Let the event loop ack Discord heartbeats between heavy render sections.
    await asyncio.sleep(0)

    # Main panel.
    card = (sc(31), sc(28), sc(1324), sc(776))
    alpha_round((card[0] + sc(4), card[1] + sc(7), card[2] + sc(4), card[3] + sc(7)), 25, (0, 0, 0, 145), blur=8)
    alpha_round(card, 25, (14, 17, 30, 218), outline=(76, 88, 120, 190), width=2)
    alpha_round((card[0] + sc(2), card[1] + sc(2), card[2] - sc(2), card[3] - sc(2)), 23, (255, 255, 255, 0), outline=(255, 255, 255, 26), width=1)

    d = ImageDraw.Draw(img)

    # Top rainbow strip with soft glow.
    strip_x1, strip_y = sc(52), sc(50)
    strip_x2 = sc(1303)
    colours = [(93, 111, 255), (72, 214, 177), (232, 205, 54), (248, 135, 43), (250, 72, 82)]
    strip = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    sd = ImageDraw.Draw(strip)
    for x in range(strip_x1, strip_x2):
        t = (x - strip_x1) / max(strip_x2 - strip_x1 - 1, 1)
        idx = min(int(t * (len(colours) - 1)), len(colours) - 2)
        local = (t - idx / (len(colours) - 1)) * (len(colours) - 1)
        c1, c2 = colours[idx], colours[idx + 1]
        color = tuple(int(c1[i] + (c2[i] - c1[i]) * local) for i in range(3))
        sd.line((x, strip_y, x, strip_y + sc(5)), fill=(*color, 255), width=sc(1))
    img.alpha_composite(strip.filter(ImageFilter.GaussianBlur(sc(1.2))))
    img.alpha_composite(strip)
    d = ImageDraw.Draw(img)

    await asyncio.sleep(0)

    # Logo: clean full ring. Keep it symmetrical and avoid the broken-looking
    # segmented gaps that appeared around the previous version.
    logo_center = (sc(112), sc(132))
    ring_r = sc(56)
    alpha_round((logo_center[0] - sc(64), logo_center[1] - sc(64), logo_center[0] + sc(64), logo_center[1] + sc(64)), 66, (0, 0, 0, 96), blur=6)

    ring_layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    rd = ImageDraw.Draw(ring_layer)
    ring_box = (logo_center[0] - ring_r, logo_center[1] - ring_r, logo_center[0] + ring_r, logo_center[1] + ring_r)
    ring_colours = [
        (93, 111, 255),
        (72, 214, 177),
        (232, 205, 54),
        (248, 135, 43),
        (201, 91, 255),
        (93, 111, 255),
    ]
    steps = 180
    for step in range(steps):
        t = step / max(steps - 1, 1)
        colour_pos = t * (len(ring_colours) - 1)
        idx = min(int(colour_pos), len(ring_colours) - 2)
        local = colour_pos - idx
        c1, c2 = ring_colours[idx], ring_colours[idx + 1]
        color = tuple(int(c1[i] + (c2[i] - c1[i]) * local) for i in range(3))
        a0 = -90 + 360 * t
        a1 = -90 + 360 * min(1, t + 1 / steps)
        rd.arc(ring_box, a0, a1, fill=(*color, 255), width=sc(5))

    # Subtle glow/edge for a more finished circular badge.
    glow_ring = ring_layer.filter(ImageFilter.GaussianBlur(sc(1.2)))
    img.alpha_composite(glow_ring)
    img.alpha_composite(ring_layer)
    d = ImageDraw.Draw(img)
    d.ellipse((logo_center[0] - sc(60), logo_center[1] - sc(60), logo_center[0] + sc(60), logo_center[1] + sc(60)), outline=(255, 255, 255, 28), width=sc(1))
    d.ellipse((logo_center[0] - sc(47), logo_center[1] - sc(47), logo_center[0] + sc(47), logo_center[1] + sc(47)), fill=(11, 12, 29, 238), outline=(190, 195, 255, 88), width=sc(1))

    await asyncio.sleep(0)

    asset_id = extract_asset_id(payload.get("icon"))
    icon_bytes = await fetch_image_bytes(f"{PS99_API}/image/{asset_id}") if asset_id else None
    if icon_bytes:
        try:
            icon = Image.open(BytesIO(icon_bytes)).convert("RGBA").resize((sc(82), sc(82)), Image.Resampling.LANCZOS)
            mask = Image.new("L", icon.size, 0)
            ImageDraw.Draw(mask).ellipse((0, 0, icon.size[0] - 1, icon.size[1] - 1), fill=255)
            img.paste(icon, (logo_center[0] - icon.size[0] // 2, logo_center[1] - icon.size[1] // 2), mask)
        except Exception:
            d.text((sc(78), sc(116)), CLAN_NAME, font=font(18, True), fill=(207, 98, 255, 255))
    else:
        d.text((sc(78), sc(116)), CLAN_NAME, font=font(18, True), fill=(207, 98, 255, 255))

    # Header tag.
    clan_text = f"[{payload.get('clanName') or CLAN_NAME}]"
    clan_x, clan_y = sc(181), sc(111)
    draw_gradient_text_on_image(img, (clan_x, clan_y), clan_text, fonts["tag"], (45, 225, 215), (249, 91, 51))
    d = ImageDraw.Draw(img)
    clan_bbox = d.textbbox((0, 0), clan_text, font=fonts["tag"])
    clan_text_w = clan_bbox[2] - clan_bbox[0]

    def small_badge(x, label, value, accent, width=148):
        box = (x, sc(104), x + sc(width), sc(166))
        alpha_round((box[0] + sc(1), box[1] + sc(2), box[2] + sc(1), box[3] + sc(2)), 12, (0, 0, 0, 80), blur=3)
        alpha_round(box, 12, (22, 25, 43, 228), outline=(*accent, 145), width=1)
        dd = ImageDraw.Draw(img)
        centered_text(dd, (box[0], box[1] + sc(7), box[2], box[1] + sc(22)), label.upper(), fonts["badge_label"], (164, 172, 196, 255))
        centered_text(dd, (box[0], box[1] + sc(22), box[2], box[3] - sc(2)), value, fonts["badge_value"], (*accent, 255))

    badge1_x = max(sc(366), clan_x + clan_text_w + sc(40))
    badge2_x = badge1_x + sc(158)
    small_badge(badge1_x, "Clan Rank", f"#{payload.get('rank') or '—'}", (239, 198, 58), width=132)
    small_badge(badge2_x, "Hourly Points", format_hourly_points(payload.get("hourlyPoints", 0)), (82, 200, 240), width=152)

    # Top-right stat cards.
    def stat_box(x, label, value, accent):
        box = (sc(x), sc(94), sc(x + 135), sc(172))
        alpha_round((box[0] + sc(1), box[1] + sc(2), box[2] + sc(1), box[3] + sc(2)), 13, (0, 0, 0, 75), blur=3)
        alpha_round(box, 13, (22, 25, 43, 224), outline=(*accent, 112), width=1)
        dd = ImageDraw.Draw(img)
        text_left = box[0] + sc(14)
        text_right = box[2] - sc(35)
        left_center_text(dd, (text_left, box[1] + sc(12), text_right, box[1] + sc(29)), label, fonts["stat_label"], (162, 170, 194, 255))
        left_center_text(dd, (text_left, box[1] + sc(28), text_right, box[3] - sc(7)), str(value), fonts["stat_value"], (247, 248, 255, 255))
        dd.rounded_rectangle((box[2] - sc(24), box[1] + sc(22), box[2] - sc(12), box[3] - sc(20)), radius=sc(6), fill=(*accent, 255))

    stat_box(858, "Players", payload.get("players", 0), (93, 111, 255))
    stat_box(1012, "Active", payload.get("active", 0), (75, 205, 120))
    stat_box(1167, "Zero", payload.get("zero", 0), (250, 72, 82))

    entries = list(payload.get("entries") or [])
    total_hourly = int(payload.get("hourlyPoints") or 0)
    all_zero = total_hourly <= 0
    max_pph = max([int(entry.get("pph") or 0) for entry in entries] + [1])
    columns = [entries[0:25], entries[25:50], entries[50:75]]
    panel_y, panel_h = sc(208), sc(545)
    panel_w = sc(412)
    panel_xs = [sc(46), sc(472), sc(898)]

    for col_idx, col_entries in enumerate(columns):
        px = panel_xs[col_idx]
        alpha_round((px, panel_y, px + panel_w, panel_y + panel_h), 15, (9, 12, 24, 204), outline=(61, 70, 96, 185), width=2)
        alpha_round((px + sc(2), panel_y + sc(2), px + panel_w - sc(2), panel_y + sc(26)), 13, (255, 255, 255, 12))
        dd = ImageDraw.Draw(img)

        for row_idx, entry in enumerate(col_entries):
            global_idx = col_idx * 25 + row_idx
            y = sc(226) + sc(row_idx * 20.4)
            pph_ready = bool(entry.get("pphReady"))
            pph = int(entry.get("pph") or 0)
            zero = pph_ready and pph <= 0
            color = (112, 122, 148) if (all_zero or not pph_ready) else hourly_colour(global_idx, max(len(entries), 1), zero=zero)

            row_box = (px + sc(10), y - sc(1), px + panel_w - sc(10), y + sc(18))
            if row_idx % 2 == 0:
                alpha_round(row_box, 5, (255, 255, 255, 17))
            if global_idx < 3 and pph_ready and not zero:
                alpha_round(row_box, 5, (*color, 20))
            dd = ImageDraw.Draw(img)

            rank_text = f"{global_idx + 1:02d}"
            name = fit_text(dd, str(entry.get("name") or entry.get("robloxId") or "Unknown"), fonts["row"], sc(150))
            rank_fill = color if pph_ready and not zero else (145, 154, 178)
            name_fill = (246, 247, 252) if pph_ready and not zero else ((176, 184, 205) if (all_zero or not pph_ready) else (226, 96, 108))
            score_fill = color if pph_ready and not zero else ((152, 160, 184) if (all_zero or not pph_ready) else (145, 151, 168))

            dd.text((px + sc(18), y), rank_text, font=fonts["row_small"], fill=(*rank_fill, 255))
            dd.text((px + sc(68), y), name, font=fonts["row"], fill=(*name_fill, 255))

            bar_x = px + sc(224)
            bar_y = y + sc(7)
            bar_w = sc(66)
            bar_h = sc(7)
            track = (43, 49, 63)
            dd.rounded_rectangle((bar_x, bar_y, bar_x + bar_w, bar_y + bar_h), radius=sc(4), fill=(*track, 255))
            dd.rounded_rectangle((bar_x, bar_y, bar_x + bar_w, bar_y + bar_h), radius=sc(4), outline=(88, 98, 122, 95), width=sc(1))
            fill_w = int(bar_w * (pph / max_pph)) if max_pph > 0 else 0
            if fill_w > 0:
                fill_w = max(sc(4), fill_w)
                # Smooth tiny gradient instead of the old harsh white stripe.
                fill_img = Image.new("RGBA", (fill_w, bar_h + 1), (0, 0, 0, 0))
                fd = ImageDraw.Draw(fill_img)
                for bx in range(fill_w):
                    t = bx / max(fill_w - 1, 1)
                    shade = 0.82 + 0.18 * t
                    grad = tuple(min(255, int(color[i] * shade + 18 * (1 - t))) for i in range(3))
                    fd.line((bx, 0, bx, bar_h + 1), fill=(*grad, 255))
                fd.rounded_rectangle((1, 1, max(1, fill_w - 2), max(1, sc(2))), radius=sc(2), fill=(255, 255, 255, 34))
                fill_mask = Image.new("L", fill_img.size, 0)
                ImageDraw.Draw(fill_mask).rounded_rectangle((0, 0, fill_img.size[0] - 1, fill_img.size[1] - 1), radius=sc(4), fill=255)
                img.paste(fill_img, (bar_x, bar_y), fill_mask)
                dd = ImageDraw.Draw(img)
                dd.rounded_rectangle((bar_x, bar_y, bar_x + fill_w, bar_y + bar_h), radius=sc(4), outline=(255, 255, 255, 42), width=sc(1))
            elif zero:
                marker = (92, 104, 132) if all_zero else (197, 55, 74)
                dd.rounded_rectangle((bar_x, bar_y, bar_x + sc(3), bar_y + bar_h), radius=sc(2), fill=(*marker, 220))

            score = str(pph) if pph_ready else "—"
            right_text(dd, px + panel_w - sc(18), y, score, fonts["row_small"], (*score_fill, 255))

        # Yield after each column so heartbeats are acked during the heaviest
        # per-row gradient work (3 columns x 25 rows).
        await asyncio.sleep(0)

    # Tiny timestamp in the bottom-right.
    d = ImageDraw.Draw(img)
    try:
        stamp_dt = datetime.fromisoformat(str(payload.get("scheduledAt") or "").replace("Z", "+00:00"))
    except Exception:
        stamp_dt = datetime.now(timezone.utc)
    updated = stamp_dt.astimezone(timezone.utc).strftime("Slot %H:%M UTC")
    right_text(d, sc(1300), sc(754), updated, fonts["tiny"], (120, 129, 155, 190))

    await asyncio.sleep(0)

    out = BytesIO()
    img = img.resize((1355, 804), Image.Resampling.LANCZOS)
    img.save(out, format="PNG")
    out.seek(0)
    return out

def fetch_hourly_ping_targets(entries, threshold):
    if not DATABASE_URL or threshold <= 0:
        return []

    low_entries = [
        entry for entry in entries
        if entry.get("pphReady") and int(entry.get("pph") or 0) < int(threshold)
    ]
    if not low_entries:
        return []

    ids = [str(entry.get("robloxId") or "").strip() for entry in low_entries if str(entry.get("robloxId") or "").strip()]
    if not ids:
        return []

    linked = {}
    pass
    try:
        ensure_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT roblox_id::text, discord_id::text, username
                FROM users
                WHERE roblox_id::text = ANY(%s)
                  AND discord_id IS NOT NULL
            """, (ids,))
            for roblox_id, discord_id, username in cur.fetchall():
                linked[str(roblox_id)] = {"discordId": str(discord_id), "username": str(username or roblox_id)}

            try:
                cur.execute("""
                    SELECT roblox_id::text, discord_id::text, username
                    FROM user_alts
                    WHERE roblox_id::text = ANY(%s)
                """, (ids,))
                for roblox_id, discord_id, username in cur.fetchall():
                    linked.setdefault(str(roblox_id), {"discordId": str(discord_id), "username": str(username or roblox_id)})
            except Exception:
                pass
    except Exception as exc:
        print(f"[hourly stats] ping target lookup failed: {exc}")
        return []
    finally:
        pass

    targets = []
    seen_discord = set()
    for entry in sorted(low_entries, key=lambda item: (int(item.get("pph") or 0), str(item.get("name") or "").lower())):
        rid = str(entry.get("robloxId") or "")
        link = linked.get(rid)
        if not link or not link.get("discordId"):
            continue
        discord_id = str(link["discordId"])
        if discord_id in seen_discord:
            continue
        seen_discord.add(discord_id)
        targets.append({
            "discordId": discord_id,
            "mention": f"<@{discord_id}>",
            "username": str(entry.get("name") or link.get("username") or rid),
            "pph": int(entry.get("pph") or 0),
        })

    return targets


def record_hourly_ping_targets(targets, threshold):
    if not DATABASE_URL or not targets:
        return
    pass
    try:
        ensure_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS hourly_stats_ping_records (
                    id BIGSERIAL PRIMARY KEY,
                    discord_id TEXT,
                    username TEXT,
                    pph INTEGER,
                    threshold INTEGER,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.executemany("""
                INSERT INTO hourly_stats_ping_records (discord_id, username, pph, threshold, created_at)
                VALUES (%s, %s, %s, %s, NOW())
            """, [
                (
                    str(target.get("discordId") or ""),
                    str(target.get("username") or "Unknown"),
                    int(target.get("pph") or 0),
                    int(threshold),
                )
                for target in targets
            ])
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        pass


async def send_hourly_ping_followup(channel, entries, threshold, message=None):
    targets = fetch_hourly_ping_targets(entries, threshold)
    if not targets:
        return 0

    warning = render_hourly_ping_message(message, threshold, len(targets))
    header = f"⚠️ **Hourly PPH check:** {len(targets)} linked member(s) under **{int(threshold)} PPH**.\n"
    footer = f"\n\n{warning}" if warning else ""
    max_len = 1900
    available = max_len - len(header) - len(footer) - 20

    mentions = []
    used = 0
    for target in targets:
        mention = f"{target['mention']} "
        if used + len(mention) > available:
            break
        mentions.append(mention)
        used += len(mention)

    omitted = max(0, len(targets) - len(mentions))
    omitted_text = f"\n…and **{omitted}** more linked member(s)." if omitted else ""
    content = f"{header}{''.join(mentions).rstrip()}{omitted_text}{footer}".strip()

    await channel.send(
        content=content[:2000],
        allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
    )

    try:
        record_hourly_ping_targets(targets, threshold)
    except Exception as exc:
        print(f"[hourly stats] ping record failed: {exc}")

    return len(targets)


async def send_hourly_stats_card(channel, ping_enabled=None, ping_threshold=None, ping_message=None, scheduled_at=None, use_latest_exact=False):
    payload = await build_hourly_stats_payload(scheduled_at=scheduled_at, use_latest_exact=use_latest_exact)
    if not payload:
        raise ValueError("Could not load active war hourly stats.")
    image = await generate_hourly_stats_card(payload)
    file = discord.File(image, filename="mcwv-hourly-stats.png")
    # Send as a plain image attachment, not an embed.
    await channel.send(file=file)
    save_hourly_player_snapshot(payload)
    db_set_setting("mcwv_hourly_stats_last_sent_at", _now_iso())

    should_ping = hourly_stats_ping_enabled() if ping_enabled is None else bool(ping_enabled)
    threshold = get_hourly_stats_ping_threshold() if ping_threshold is None else max(0, int(ping_threshold))
    if should_ping and threshold > 0:
        await send_hourly_ping_followup(channel, payload.get("entries") or [], threshold, message=ping_message)


def hourly_stats_enabled():
    raw = db_get_setting("mcwv_hourly_stats_enabled", "1" if MCWV_HOURLY_STATS_ENABLED_DEFAULT else "0")
    return str(raw).lower() not in ("0", "false", "off", "no")


def set_hourly_stats_enabled(enabled, auto_disabled=False):
    db_set_setting("mcwv_hourly_stats_enabled", "1" if enabled else "0")
    db_set_setting("mcwv_hourly_stats_auto_disabled", "1" if auto_disabled else "0")


def hourly_stats_auto_disabled():
    raw = db_get_setting("mcwv_hourly_stats_auto_disabled", "0")
    return str(raw).lower() in ("1", "true", "yes", "on")


# Shared short cache so both hourly loops do not double-poll the war endpoints.
hourly_stats_war_state = {"checked_at": 0.0, "active": None}
hourly_stats_war_misses = 0


async def hourly_stats_war_is_active():
    now = time.time()
    if now - float(hourly_stats_war_state.get("checked_at") or 0) < 45:
        return hourly_stats_war_state.get("active")

    battle_id = await get_active_battle_id_for_placement()
    hourly_stats_war_state["checked_at"] = now
    hourly_stats_war_state["active"] = bool(battle_id)
    return hourly_stats_war_state["active"]


async def sync_hourly_stats_with_war_state():
    """Auto pause hourly stats when no clan war is active and auto resume them.

    Returns True when hourly stats should run this tick, False when they should
    stay paused. Manual toggles always win: the bot only re-enables automatically
    if the auto war toggle was the one that disabled it.
    """
    if not MCWV_HOURLY_STATS_AUTO_WAR_TOGGLE:
        return hourly_stats_enabled()

    global hourly_stats_war_misses

    if not hourly_stats_enabled():
        if hourly_stats_auto_disabled():
            if await hourly_stats_war_is_active():
                set_hourly_stats_enabled(True, auto_disabled=False)
                hourly_stats_war_misses = 0
                admin_log("Hourly Stats Auto-Enabled", "A clan war is active again; hourly stats resumed automatically.")
                return True
        return False

    if await hourly_stats_war_is_active():
        hourly_stats_war_misses = 0
        return True

    # Debounce: one API hiccup must not pause the hourly cards mid-war.
    hourly_stats_war_misses += 1
    if hourly_stats_war_misses < HOURLY_STATS_AUTO_DISABLE_MISSES_REQUIRED:
        print(f"[hourly stats] active war check failed/ended ({hourly_stats_war_misses}/{HOURLY_STATS_AUTO_DISABLE_MISSES_REQUIRED})")
        return True

    hourly_stats_war_misses = 0
    set_hourly_stats_enabled(False, auto_disabled=True)
    admin_log("Hourly Stats Auto-Disabled", "No active clan war detected; hourly stats paused until the next war.", "warning")
    return False


def hourly_stats_ping_enabled():
    raw = db_get_setting("mcwv_hourly_stats_ping_enabled", "1" if MCWV_HOURLY_STATS_PING_ENABLED_DEFAULT else "0")
    return str(raw).lower() not in ("0", "false", "off", "no")


def get_hourly_stats_ping_threshold():
    try:
        return max(0, int(float(db_get_setting("mcwv_hourly_stats_ping_threshold", MCWV_HOURLY_STATS_PING_THRESHOLD_DEFAULT))))
    except Exception:
        return int(MCWV_HOURLY_STATS_PING_THRESHOLD_DEFAULT)


def normalize_hourly_start_time(value):
    text = str(value or "").strip()
    if not text:
        return ""
    match = re.fullmatch(r"([01]?\d|2[0-3]):([0-5]\d)", text)
    if not match:
        return ""
    return f"{int(match.group(1)):02d}:{int(match.group(2)):02d}"


def get_hourly_stats_start_time():
    return normalize_hourly_start_time(db_get_setting("mcwv_hourly_stats_start_time", MCWV_HOURLY_STATS_START_TIME_DEFAULT))


def get_hourly_stats_ping_message():
    value = db_get_setting("mcwv_hourly_stats_ping_message", None)
    if value is None:
        return MCWV_HOURLY_STATS_PING_MESSAGE_DEFAULT
    return str(value)


def render_hourly_ping_message(message, threshold, count):
    template = get_hourly_stats_ping_message() if message is None else str(message)
    return (
        template
        .replace("{threshold}", str(int(threshold)))
        .replace("{count}", str(int(count)))
    ).strip()


def parse_iso_ms(value):
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)
    except Exception:
        return None


def get_hourly_stats_due_slot(now=None):
    interval = max(1, int(MCWV_HOURLY_STATS_INTERVAL_MINUTES or 60))
    now = now or datetime.now(timezone.utc)
    now = now.astimezone(timezone.utc).replace(second=0, microsecond=0)
    start_time = get_hourly_stats_start_time()

    if start_time:
        hour, minute = [int(part) for part in start_time.split(":", 1)]
        anchor = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        while anchor > now:
            anchor -= timedelta(days=1)
    else:
        # No custom start: exact top-of-hour schedule.
        anchor = now.replace(minute=0, second=0, microsecond=0)

    elapsed_minutes = int((now - anchor).total_seconds() // 60)
    slot = anchor + timedelta(minutes=(elapsed_minutes // interval) * interval)

    # Only collect in the first ~90 seconds of the scheduled slot. If Render is
    # late by a minute, it still sends; if the service was asleep/deploying for
    # longer, skip the missed slot instead of collecting inaccurate late data.
    delay_seconds = (datetime.now(timezone.utc) - slot).total_seconds()
    if delay_seconds < -5 or delay_seconds > 90:
        return None
    return slot


def hourly_stats_due_now():
    return get_hourly_stats_due_slot() is not None


def hourly_stats_last_auto_sent_ms():
    value = db_get_setting("mcwv_hourly_stats_last_auto_sent_at", None)
    return parse_iso_ms(value) if value else None


def hourly_stats_auto_cooldown_remaining_seconds():
    last_ms = hourly_stats_last_auto_sent_ms()
    if last_ms is None:
        return 0
    interval_ms = max(1, int(MCWV_HOURLY_STATS_INTERVAL_MINUTES or 60)) * 60 * 1000
    elapsed_ms = int(datetime.now(timezone.utc).timestamp() * 1000) - last_ms
    remaining_ms = interval_ms - elapsed_ms
    return max(0, math.ceil(remaining_ms / 1000))


def reserve_hourly_stats_auto_slot():
    """Atomically reserve the next automated hourly send slot.

    Returns the scheduled slot datetime if this process should collect/send, or
    None if the slot is not due/already reserved. Manual /hourly_stats does not
    use this.
    """
    slot = get_hourly_stats_due_slot()
    if slot is None:
        return None

    slot_iso = slot.isoformat()

    if not db_enabled():
        return slot

    lock_key = 294_729_601
    locked = False
    try:
        ensure_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_key,))
            locked = bool(cur.fetchone()[0])

        if not locked:
            return None

        # Re-check inside the lock to prevent overlap during deploys/restarts.
        current_slot = get_hourly_stats_due_slot()
        if current_slot is None or current_slot.isoformat() != slot_iso:
            return None

        last_slot = str(db_get_setting("mcwv_hourly_stats_last_auto_slot", "") or "")
        if last_slot == slot_iso:
            return None

        db_set_setting("mcwv_hourly_stats_last_auto_slot", slot_iso)
        db_set_setting("mcwv_hourly_stats_last_auto_sent_at", _now_iso())
        return slot
    except Exception as exc:
        print(f"[hourly stats] auto slot reserve failed: {exc}")
        return None
    finally:
        if locked:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))
            except Exception:
                pass


def format_hourly_cooldown(seconds):
    seconds = max(0, int(seconds or 0))
    minutes, sec = divmod(seconds, 60)
    if minutes > 0:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


def get_hourly_stats_channel_id():
    saved = db_get_setting("mcwv_hourly_stats_channel_id", None)
    try:
        return int(saved or MCWV_HOURLY_STATS_CHANNEL_ID or 0)
    except Exception:
        return int(MCWV_HOURLY_STATS_CHANNEL_ID or 0)


def set_hourly_stats_channel_id(channel_id):
    db_set_setting("mcwv_hourly_stats_channel_id", int(channel_id))


@tasks.loop(minutes=1)
async def hourly_stats_loop():
    await bot.wait_until_ready()
    try:
        if not await sync_hourly_stats_with_war_state():
            return
    except Exception as exc:
        print(f"[hourly stats] war-state sync failed: {exc}")
        if not hourly_stats_enabled():
            return

    channel_id = get_hourly_stats_channel_id()
    if not channel_id:
        return

    channel = await _maybe_get_channel(channel_id)
    if not isinstance(channel, discord.TextChannel):
        print(f"[hourly stats] channel not found/not text: {channel_id}")
        return

    scheduled_slot = reserve_hourly_stats_auto_slot()
    if scheduled_slot is None:
        return

    try:
        await send_hourly_stats_card(channel, scheduled_at=scheduled_slot)
        print(f"[hourly stats] card sent to {channel_id} for slot {scheduled_slot.isoformat()}")
    except Exception as exc:
        print(f"[hourly stats] auto-send failed: {exc}")
    pass  # keep connection alive (Supabase has no compute hour limit)


@hourly_stats_loop.before_loop
async def before_hourly_stats_loop():
    await bot.wait_until_ready()


@tasks.loop(minutes=1)
async def hourly_player_snapshot_loop():
    await bot.wait_until_ready()
    try:
        if not await sync_hourly_stats_with_war_state():
            return
    except Exception as exc:
        print(f"[hourly stats] player snapshot war-state sync failed: {exc}")
        if not hourly_stats_enabled():
            return
    try:
        if await collect_hourly_player_snapshot():
            print("[hourly stats] player snapshot saved/refreshed")
    except Exception as exc:
        print(f"[hourly stats] player snapshot loop failed: {exc}")
    pass  # keep connection alive (Supabase has no compute hour limit)


@hourly_player_snapshot_loop.before_loop
async def before_hourly_player_snapshot_loop():
    await bot.wait_until_ready()


async def process_clan_logs():
    channel_id = get_clan_log_channel_id()
    if not channel_id:
        return

    channel = await _maybe_get_channel(channel_id)
    if not isinstance(channel, discord.TextChannel):
        print(f"[clan logs] channel not found/not text: {channel_id}")
        return

    payload = await fetch_json_for_placement(CLAN_API) if CLAN_API else None
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    if not isinstance(data, dict) or not data:
        return

    current_state = build_clan_log_state(data)
    previous = load_clan_log_state()

    if not previous:
        save_clan_log_state(current_state)
        print(f"[clan logs] initial state saved: members={len(current_state['members'])} diamonds={current_state['totalDiamonds']}")
        return

    old_members = previous.get("members", {}) if isinstance(previous.get("members"), dict) else {}
    new_members = current_state.get("members", {})
    old_diamonds = previous.get("diamonds", {}) if isinstance(previous.get("diamonds"), dict) else {}
    new_diamonds = current_state.get("diamonds", {})

    joined_ids = sorted(set(new_members) - set(old_members), key=lambda rid: new_members.get(rid, {}).get("joinTime", 0))
    left_ids = sorted(set(old_members) - set(new_members))

    # Migration safety: after adding the owner into member counts, do not send a
    # fake "owner joined" card just because older saved state did not include Owner.
    owner_id = get_clan_owner_id_from_data(data)
    if owner_id and owner_id in joined_ids and owner_id not in old_members:
        joined_ids = [rid for rid in joined_ids if rid != owner_id]

    diamond_events = []
    for rid, total in new_diamonds.items():
        try:
            previous_total = int(old_diamonds.get(rid, total))
            total = int(total)
            delta = total - previous_total
            if delta > 0:
                diamond_events.append((rid, delta, total))
        except Exception:
            continue

    diamond_events.sort(key=lambda item: item[1], reverse=True)
    diamond_events = diamond_events[:MCWV_CLAN_LOG_MAX_DIAMOND_ALERTS]

    lookup_ids = set(joined_ids) | set(left_ids) | {rid for rid, _, _ in diamond_events}
    users = await fetch_roblox_users_for_logs(lookup_ids)
    member_count = len(new_members)
    member_capacity = current_state.get("memberCapacity") or data.get("MemberCapacity") or 0

    for rid in joined_ids:
        try:
            await send_clan_member_log(channel, "joined", int(rid), users.get(int(rid), {}), member_count, member_capacity)
            await asyncio.sleep(0.8)
        except Exception as exc:
            print(f"[clan logs] joined log failed for {rid}: {exc}")

    for rid in left_ids:
        try:
            await send_clan_member_log(channel, "left", int(rid), users.get(int(rid), {}), member_count, member_capacity)
            await asyncio.sleep(0.8)
        except Exception as exc:
            print(f"[clan logs] left log failed for {rid}: {exc}")

    for rid, delta, total in diamond_events:
        try:
            await send_diamond_log(channel, data, int(rid), users.get(int(rid), {}), delta, total, current_state.get("totalDiamonds") or 0)
            await asyncio.sleep(0.8)
        except Exception as exc:
            print(f"[clan logs] diamond log failed for {rid}: {exc}")

    save_clan_log_state(current_state)


@tasks.loop(seconds=60)
async def clan_log_loop():
    await bot.wait_until_ready()
    if not clan_logs_enabled():
        return
    try:
        await process_clan_logs()
    except Exception as exc:
        print(f"[clan logs] loop failed: {exc}")
    pass  # keep connection alive (Supabase has no compute hour limit)


@clan_log_loop.before_loop
async def before_clan_log_loop():
    await bot.wait_until_ready()


async def generate_placement_card(old_rank, new_rank, points, icon_value=None):
    # Modern placement card using the supplied galaxy background as the main art.
    # Transparent Discord-ready PNG, rendered at 2x then downsampled for smooth edges.
    S = 2
    W, H = 1080 * S, 560 * S
    improved = int(new_rank) < int(old_rank)
    diff = abs(int(old_rank) - int(new_rank))
    accent = (119, 255, 180) if improved else (255, 111, 122)
    accent_2 = (88, 211, 255) if improved else (255, 143, 96)
    glass = (11, 18, 54)

    def sc(value):
        return int(round(value * S))

    def font(size, bold=True):
        path = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
        return ImageFont.truetype(path, sc(size))

    fonts = {
        "title": font(68, True),
        "small": font(28, True),
        "pill": font(29, True),
        "rank": font(102, True),
        "label": font(39, False),
        "points": font(54, True),
        "logo": font(28, True),
    }

    img = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    card_xy = (sc(32), sc(26), sc(1048), sc(534))
    cw, ch = card_xy[2] - card_xy[0], card_xy[3] - card_xy[1]
    ox, oy = card_xy[0], card_xy[1]

    # Background: only the supplied galaxy image, cover-cropped into the card.
    if MCWV_PLACEMENT_BG_PATH and os.path.exists(MCWV_PLACEMENT_BG_PATH):
        try:
            bg = Image.open(MCWV_PLACEMENT_BG_PATH).convert("RGBA")
            bw, bh = bg.size
            scale = max(cw / bw, ch / bh)
            bg = bg.resize((int(bw * scale), int(bh * scale)), Image.Resampling.LANCZOS)
            left = (bg.size[0] - cw) // 2
            top = (bg.size[1] - ch) // 2
            card = bg.crop((left, top, left + cw, top + ch))
        except Exception as exc:
            print(f"[placement] background load failed: {exc}")
            card = Image.new("RGBA", (cw, ch), (8, 10, 32, 255))
    else:
        card = Image.new("RGBA", (cw, ch), (8, 10, 32, 255))

    # Professional readability overlays, but keep the galaxy visible.
    overlay = Image.new("RGBA", (cw, ch), (0, 0, 0, 0))
    od = ImageDraw.Draw(overlay)
    od.rounded_rectangle((0, 0, cw, ch), radius=sc(54), fill=(4, 8, 28, 92))
    od.ellipse((sc(-220), sc(250), sc(340), sc(760)), fill=(136, 70, 255, 38))
    od.ellipse((sc(610), sc(-180), sc(1260), sc(430)), fill=(77, 111, 255, 42))
    overlay = overlay.filter(ImageFilter.GaussianBlur(sc(1)))
    card.alpha_composite(overlay)

    await asyncio.sleep(0)

    # Rounded card mask + outer glow.
    mask = rounded_mask((cw, ch), sc(54))
    shaped = Image.new("RGBA", (cw, ch), (0, 0, 0, 0))
    shaped.paste(card, (0, 0), mask)

    glow = Image.new("RGBA", (cw, ch), (0, 0, 0, 0))
    gd = ImageDraw.Draw(glow)
    gd.rounded_rectangle((0, 0, cw - 1, ch - 1), radius=sc(54), fill=(104, 118, 255, 145))
    glow = glow.filter(ImageFilter.GaussianBlur(sc(14)))
    img.alpha_composite(glow, (ox, oy))
    img.alpha_composite(shaped, (ox, oy))

    d = ImageDraw.Draw(img)
    d.rounded_rectangle(card_xy, radius=sc(54), outline=(132, 142, 255, 230), width=sc(3))
    d.rounded_rectangle((ox + sc(7), oy + sc(7), ox + cw - sc(7), oy + ch - sc(7)), radius=sc(48), outline=(255, 255, 255, 42), width=sc(1))

    # Glass helper.
    def glass_panel(box, radius=26, fill_alpha=112, outline_alpha=82):
        layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        ld = ImageDraw.Draw(layer)
        ld.rounded_rectangle(box, radius=sc(radius), fill=(*glass, fill_alpha), outline=(180, 190, 255, outline_alpha), width=sc(1.5))
        ld.rounded_rectangle((box[0]+sc(2), box[1]+sc(2), box[2]-sc(2), box[1]+sc(32)), radius=sc(radius-2), fill=(255, 255, 255, 16))
        img.alpha_composite(layer)

    def aligned_xy(box, text, font, align="center"):
        bbox = d.textbbox((0, 0), text, font=font)
        tw = bbox[2] - bbox[0]
        th = bbox[3] - bbox[1]
        if align == "left":
            x = box[0]
        elif align == "right":
            x = box[2] - tw
        else:
            x = box[0] + ((box[2] - box[0]) - tw) / 2
        y = box[1] + ((box[3] - box[1]) - th) / 2
        return int(x - bbox[0]), int(y - bbox[1])

    def draw_aligned(text, font, box, fill, align="center", shadow=(0, 0, 0, 125), offset=None):
        xy = aligned_xy(box, text, font, align=align)
        off = offset or (sc(3), sc(4))
        draw_text_shadow(d, xy, text, font, fill, shadow=shadow, offset=off)

    # Top logo orb.
    logo = (ox + sc(36), oy + sc(31), ox + sc(151), oy + sc(146))
    d.ellipse(logo, fill=(7, 11, 35, 150), outline=(119, 145, 255, 230), width=sc(3))
    d.ellipse((logo[0]+sc(10), logo[1]+sc(10), logo[2]-sc(10), logo[3]-sc(10)), outline=(255, 255, 255, 38), width=sc(1))
    asset_id = extract_asset_id(icon_value)
    icon_bytes = await fetch_image_bytes(f"{PS99_API}/image/{asset_id}") if asset_id else None
    if icon_bytes:
        try:
            icon = Image.open(BytesIO(icon_bytes)).convert("RGBA").resize((sc(88), sc(88)), Image.Resampling.LANCZOS)
            imask = Image.new("L", icon.size, 0)
            ImageDraw.Draw(imask).ellipse((0, 0, icon.size[0]-1, icon.size[1]-1), fill=255)
            img.paste(icon, (ox + sc(49), oy + sc(44)), imask)
        except Exception:
            draw_text_shadow(d, (ox + sc(56), oy + sc(74)), "MCWV", fonts["logo"], (188, 86, 255, 255), offset=(sc(2), sc(2)))
    else:
        draw_text_shadow(d, (ox + sc(56), oy + sc(74)), "MCWV", fonts["logo"], (188, 86, 255, 255), offset=(sc(2), sc(2)))

    # Header title and status pill. Text is aligned in boxes so it stays clean at every size.
    draw_aligned(
        f"[{CLAN_NAME}]",
        fonts["title"],
        (ox + sc(202), oy + sc(43), ox + sc(545), oy + sc(150)),
        (250, 251, 255, 255),
        align="left",
        shadow=(0, 0, 0, 118),
        offset=(sc(3), sc(4)),
    )
    pill_text = f"Position {'Increased' if improved else 'Decreased'} by {diff}"
    pill = (ox + sc(560), oy + sc(38), ox + cw - sc(38), oy + sc(100))
    glass_panel(pill, radius=20, fill_alpha=118, outline_alpha=90)
    d.rounded_rectangle(pill, radius=sc(20), outline=(*accent, 185), width=sc(2))
    draw_aligned(
        pill_text,
        fonts["pill"],
        (pill[0] + sc(18), pill[1] + sc(2), pill[2] - sc(18), pill[3] - sc(2)),
        (*accent, 255),
        shadow=(0, 0, 0, 132),
        offset=(sc(2), sc(2)),
    )

    # Main glass rank transition panel.
    bar = (ox + sc(38), oy + sc(182), ox + cw - sc(38), oy + sc(368))
    glass_panel(bar, radius=34, fill_alpha=118, outline_alpha=82)

    # Rank transition bar: smooth continuous gradient with solid chevrons.
    bar_w, bar_h = bar[2] - bar[0], bar[3] - bar[1]
    bar_art = Image.new("RGBA", (bar_w, bar_h), (0, 0, 0, 0))
    bd = ImageDraw.Draw(bar_art)

    left_rgb = (30, 42, 120)
    mid_rgb = (38, 70, 145)
    right_rgb = tuple(min(255, int(v * 0.88)) for v in accent)
    far_rgb = tuple(min(255, int(v * 0.70 + 35)) for v in accent)

    def smoothstep(v):
        v = max(0.0, min(1.0, v))
        return v * v * (3 - 2 * v)

    for x in range(bar_w):
        t = x / max(bar_w - 1, 1)
        # One long soft gradient: blue holds on the left, then rolls smoothly into the accent.
        blend_one = smoothstep((t - 0.30) / 0.34)
        blend_two = smoothstep((t - 0.58) / 0.38)
        blue_to_mid = tuple(int((1 - blend_one) * left_rgb[i] + blend_one * mid_rgb[i]) for i in range(3))
        mid_to_accent = tuple(int((1 - blend_two) * blue_to_mid[i] + blend_two * right_rgb[i]) for i in range(3))
        final_blend = smoothstep((t - 0.78) / 0.22)
        rgb = tuple(int((1 - final_blend) * mid_to_accent[i] + final_blend * far_rgb[i]) for i in range(3))
        alpha = int(162 + 34 * smoothstep((t - 0.45) / 0.55))
        bd.line((x, 0, x, bar_h), fill=(*rgb, alpha))

    # Soft gloss pass composited over the gradient. Important: use a separate
    # overlay so it blends with the bar instead of replacing pixels with white/black.
    gloss = Image.new("RGBA", (bar_w, bar_h), (0, 0, 0, 0))
    gl = ImageDraw.Draw(gloss)
    for y in range(bar_h):
        top = max(0.0, 1 - y / max(bar_h * 0.34, 1))
        bottom = max(0.0, (y - bar_h * 0.64) / max(bar_h * 0.36, 1))
        if top:
            gl.line((0, y, bar_w, y), fill=(255, 255, 255, int(18 * top)))
        if bottom:
            gl.line((0, y, bar_w, y), fill=(0, 0, 0, int(18 * bottom)))
    bar_art.alpha_composite(gloss)

    img.paste(bar_art, (bar[0], bar[1]), rounded_mask((bar_w, bar_h), sc(34)))
    d = ImageDraw.Draw(img)
    d.rounded_rectangle(bar, radius=sc(34), outline=(165, 181, 255, 132), width=sc(2))
    d.rounded_rectangle((bar[0]+sc(3), bar[1]+sc(3), bar[2]-sc(3), bar[3]-sc(3)), radius=sc(31), outline=(255, 255, 255, 30), width=sc(1))

    await asyncio.sleep(0)

    # Solid colour chevrons with a crisp shadow and a tiny highlight edge.
    # Slightly left of the previous version so the full double-chevron sits more centrally
    # between the old and new rank blocks instead of leaning into the new-rank side.
    chevron_shift = sc(38)
    arrow = [(ox+sc(414)-chevron_shift, bar[1]), (ox+sc(552)-chevron_shift, oy+sc(275)), (ox+sc(414)-chevron_shift, bar[3]), (ox+sc(506)-chevron_shift, bar[3]), (ox+sc(644)-chevron_shift, oy+sc(275)), (ox+sc(506)-chevron_shift, bar[1])]
    arrow2 = [(ox+sc(494)-chevron_shift, bar[1]), (ox+sc(632)-chevron_shift, oy+sc(275)), (ox+sc(494)-chevron_shift, bar[3]), (ox+sc(580)-chevron_shift, bar[3]), (ox+sc(718)-chevron_shift, oy+sc(275)), (ox+sc(580)-chevron_shift, bar[1])]
    # Solid two-tone chevrons — no shadows/highlights/outline so there are no dark seams.
    chevron_main = tuple(min(255, int(v * 0.98)) for v in accent)
    chevron_second = tuple(min(255, int(v * 0.72 + 35)) for v in accent)
    d.polygon(arrow2, fill=(*chevron_second, 255))
    d.polygon(arrow, fill=(*chevron_main, 255))

    # Rank numbers centered vertically in each half of the bar.
    draw_aligned(
        f"#{old_rank}",
        fonts["rank"],
        (bar[0] + sc(44), bar[1] + sc(8), bar[0] + sc(330), bar[3] - sc(4)),
        (248, 249, 255, 255),
        shadow=(0, 0, 0, 128),
        offset=(sc(4), sc(5)),
    )
    draw_aligned(
        f"#{new_rank}",
        fonts["rank"],
        (bar[2] - sc(330), bar[1] + sc(8), bar[2] - sc(44), bar[3] - sc(4)),
        (*accent, 255),
        shadow=(0, 0, 0, 130),
        offset=(sc(4), sc(5)),
    )

    # Bottom contribution glass chip.
    footer = (ox + sc(230), oy + sc(398), ox + cw - sc(230), oy + sc(478))
    glass_panel(footer, radius=28, fill_alpha=54, outline_alpha=24)
    label = "Contributions"
    pts = format_compact_points(points)
    label_bbox = d.textbbox((0, 0), label, font=fonts["label"])
    pts_bbox = d.textbbox((0, 0), pts, font=fonts["points"])
    label_w = label_bbox[2] - label_bbox[0]
    pts_w = pts_bbox[2] - pts_bbox[0]
    group_w = label_w + sc(28) + pts_w
    group_x = ox + (cw - group_w) // 2
    label_box = (group_x, footer[1] + sc(6), group_x + label_w, footer[3] - sc(6))
    pts_box = (group_x + label_w + sc(28), footer[1] + sc(2), group_x + group_w, footer[3] - sc(6))
    draw_aligned(label, fonts["label"], label_box, (233, 236, 250, 240), align="left", shadow=(0, 0, 0, 105), offset=(sc(2), sc(3)))
    draw_aligned(pts, fonts["points"], pts_box, (255, 220, 94, 255), align="left", shadow=(0, 0, 0, 138), offset=(sc(3), sc(3)))

    await asyncio.sleep(0)

    img = img.resize((1080, 560), Image.Resampling.LANCZOS)
    out = BytesIO()
    img.save(out, format="PNG")
    out.seek(0)
    return out

def build_placement_embed(old_rank, new_rank, points):
    improved = int(new_rank) < int(old_rank)
    color = discord.Color.green() if improved else discord.Color.red()
    embed = discord.Embed(color=color)
    embed.set_image(url="attachment://mcwv-placement.png")
    return embed


async def trigger_hub_push(event, title=None, body=None, url=None, tag=None, image=None):
    """Fire a push notification to all Hub subscribers instantly via the
    bot-to-hub server endpoint. Best-effort: failures are logged but never
    block the calling flow (war detection, placement alerts, etc.).
    """
    if not HUB_BASE_URL:
        return
    api_key = os.environ.get("BOT_ADMIN_API_KEY") or os.environ.get("ADMIN_API_KEY")
    if not api_key:
        return
    try:
        global session
        if session is None or session.closed:
            session = aiohttp.ClientSession()
        payload = {"event": event}
        if title: payload["title"] = str(title)[:200]
        if body: payload["body"] = str(body)[:2000]
        if url: payload["url"] = url
        if tag: payload["tag"] = str(tag)[:48]
        if image: payload["image"] = image
        endpoint = f"{HUB_BASE_URL}/api/push/trigger"
        async with session.post(
            endpoint,
            json=payload,
            headers={"X-Admin-API-Key": api_key, "Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as res:
            if res.status == 200:
                data = await res.json(content_type=None)
                print(f"[hub push] {event} sent: {data.get('sent', '?')} delivered")
            else:
                text = await res.text()
                print(f"[hub push] {event} HTTP {res.status}: {text[:200]}")
    except Exception as exc:
        print(f"[hub push] {event} failed: {exc}")


async def send_placement_alert(snapshot, old_rank):
    channel_id = get_placement_channel_id()
    if not channel_id:
        return False
    channel = await _maybe_get_channel(channel_id)
    if not isinstance(channel, discord.TextChannel):
        print(f"[placement] channel not found/not text: {channel_id}")
        return False
    image = await generate_placement_card(old_rank, snapshot["rank"], snapshot["points"], snapshot.get("icon"))
    file = discord.File(image, filename="mcwv-placement.png")
    embed = build_placement_embed(old_rank, snapshot["rank"], snapshot["points"])
    await channel.send(embed=embed, file=file)
    return True


@tasks.loop(seconds=30)
async def placement_alert_loop():
    await bot.wait_until_ready()
    if not placement_alerts_enabled():
        return
    snapshot = await get_mcwv_placement_snapshot()
    if not snapshot:
        return
    battle_id = snapshot["battleId"]
    rank = int(snapshot["rank"])
    points = int(snapshot.get("points") or 0)
    previous = load_placement_state(battle_id)
    if not previous:
        save_placement_state(battle_id, rank, points, announced=False)
        print(f"[placement] initial state saved {battle_id}: rank={rank} points={points}")
        return
    old_rank = int(previous.get("rank") or rank)
    last_announced = float(previous.get("lastAnnouncedAt") or 0)
    if rank == old_rank:
        save_placement_state(battle_id, rank, points, announced=False)
        return
    if time.time() - last_announced < MCWV_PLACEMENT_MIN_SECONDS:
        save_placement_state(battle_id, rank, points, announced=False)
        return
    try:
        if await send_placement_alert(snapshot, old_rank):
            save_placement_state(battle_id, rank, points, announced=True)
            print(f"[placement] alert sent {battle_id}: {old_rank}->{rank} points={points}")
            # Fire instant push for placement changes.
            improved = rank < old_rank
            await trigger_hub_push(
                "placement",
                title=f"{'📈' if improved else '📉'} MCWV #{rank}",
                body=f"Clan placement {'up' if improved else 'down'} from #{old_rank} to #{rank} - {format_compact_points(points)} pts",
                url="/war-info",
                tag=f"placement-{battle_id}".lower()[:48],
            )
        else:
            save_placement_state(battle_id, rank, points, announced=False)
    except Exception as exc:
        print(f"[placement] alert failed: {exc}")


@placement_alert_loop.before_loop
async def before_placement_alert_loop():
    await bot.wait_until_ready()


# ---------------- HUB WAR COLLECTOR LOOP ----------------
@tasks.loop(minutes=WAR_COLLECT_INTERVAL_MINUTES)
async def hub_war_collect_loop():
    global session

    if not HUB_BASE_URL:
        return

    try:
        if session is None or session.closed:
            session = aiohttp.ClientSession()

        url = f"{HUB_BASE_URL}/api/war-collector"
        if WAR_COLLECT_SECRET:
            url = f"{url}?secret={WAR_COLLECT_SECRET}"

        timeout = aiohttp.ClientTimeout(total=25)
        async with session.get(url, timeout=timeout) as response:
            text = await response.text()
            if response.status != 200:
                print(f"[hub war collector] HTTP {response.status}: {text[:300]}")
                return

            try:
                data = await response.json(content_type=None)
            except Exception:
                data = {}

            if data.get("success"):
                if data.get("active"):
                    clan = data.get("clan") or {}
                    print(f"[hub war collector] saved {data.get('battleId')} rank={clan.get('rank')} points={clan.get('points')}")
                else:
                    print("[hub war collector] no active battle")
            else:
                print(f"[hub war collector] failed: {data or text[:300]}")
    except Exception as exc:
        print("[hub war collector] error:", exc)
    pass  # keep connection alive (Supabase has no compute hour limit)


@hub_war_collect_loop.before_loop
async def before_hub_war_collect_loop():
    await bot.wait_until_ready()


# ---------------- LOOP STARTER ----------------
def start_bot_loops():
    if not check_loop.is_running():
        check_loop.start()

    if not reminder_loop.is_running():
        reminder_loop.start()

    if not war_poll_loop.is_running():
        war_poll_loop.start()

    if not ticket_screenshot_reminder_loop.is_running():
        ticket_screenshot_reminder_loop.start()

    if not placement_alert_loop.is_running():
        placement_alert_loop.start()

    if not clan_log_loop.is_running():
        clan_log_loop.change_interval(seconds=MCWV_CLAN_LOG_INTERVAL_SECONDS)
        clan_log_loop.start()

    if not hourly_stats_loop.is_running():
        hourly_stats_loop.change_interval(minutes=1)
        hourly_stats_loop.start()

    if not hourly_player_snapshot_loop.is_running():
        hourly_player_snapshot_loop.start()

    if not broadcast_scheduler_loop.is_running():
        broadcast_scheduler_loop.start()

    if HUB_BASE_URL and not hub_war_collect_loop.is_running():
        hub_war_collect_loop.change_interval(minutes=WAR_COLLECT_INTERVAL_MINUTES)
        hub_war_collect_loop.start()
        print(f"✅ Hub war collector loop started ({WAR_COLLECT_INTERVAL_MINUTES}m) -> {HUB_BASE_URL}")
    elif not HUB_BASE_URL:
        print("⚠️ Hub war collector loop not started: HUB_BASE_URL is empty")

    if not clan_leave_loop.is_running():
        clan_leave_loop.start()

    # ---------------- GIVEAWAY LOOP ----------------
    if not check_giveaway_event.is_running():
        check_giveaway_event.start()

    # ---------------- HEALTH MONITOR LOOP ----------------
    if not health_monitor_loop.is_running():
        health_monitor_loop.start()

    # ---------------- DB KEEPER LOOP ----------------
    if not db_keeper_loop.is_running():
        db_keeper_loop.start()


# ---------------- LOOP HEALTH MONITOR + DB HEALTH ----------------


@tasks.loop(minutes=1)
async def db_keeper_loop():
    """Ping the shared DB connection every minute. If it's dead, schedule a
    threaded heal instead of blocking the event loop on a reconnect."""
    try:
        if not DATABASE_URL:
            return
        if conn is None or conn.closed != 0:
            _schedule_db_heal()
            return
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
    except Exception:
        try:
            if conn is not None and conn.closed == 0:
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception:
            pass
        _schedule_db_heal()


@db_keeper_loop.before_loop
async def before_db_keeper_loop():
    await bot.wait_until_ready()


ALL_LOOPS = [
    ("Presence", "check_loop"),
    ("War Poll", "war_poll_loop"),
    ("Reminder", "reminder_loop"),
    ("Clan Leave", "clan_leave_loop"),
    ("Placement", "placement_alert_loop"),
    ("Clan Logs", "clan_log_loop"),
    ("Hourly Stats", "hourly_stats_loop"),
    ("Hourly Snapshot", "hourly_player_snapshot_loop"),
    ("Hub Collector", "hub_war_collect_loop"),
    ("Screenshot", "ticket_screenshot_reminder_loop"),
    ("Broadcast", "broadcast_scheduler_loop"),
    ("Giveaway", "check_giveaway_event"),
    ("DB Keeper", "db_keeper_loop"),
]


@tasks.loop(minutes=5)
async def health_monitor_loop():
    """Check all loops are running, restart dead ones. Also check DB connection."""
    await bot.wait_until_ready()

    # 1. Loop health check
    restarted = []
    for label, loop_name in ALL_LOOPS:
        loop_obj = globals().get(loop_name)
        if loop_obj is None:
            continue
        try:
            if not loop_obj.is_running():
                print(f"[health] ⚠️ {label} loop stopped — restarting")
                loop_obj.start()
                restarted.append(label)
        except Exception as exc:
            print(f"[health] ❌ Failed to restart {label}: {exc}")

    if restarted:
        print(f"[health] Restarted {len(restarted)} loops: {', '.join(restarted)}")

    # 2. DB connection health check
    if DATABASE_URL:
        try:
            if conn is None or conn.closed:
                print("[health] ⚠️ DB connection dead — reconnecting")
                ensure_db_connection()
            else:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
        except Exception as exc:
            print(f"[health] ⚠️ DB health check failed: {exc} — reconnecting")
            try:
                ensure_db_connection()
            except Exception:
                print("[health] ❌ DB reconnect failed")


@health_monitor_loop.before_loop
async def before_health_monitor_loop():
    await bot.wait_until_ready()


# ---------------- WAR-AWARE PRESENCE INTERVAL ----------------

def update_check_loop_interval():
    """Adjust check_loop interval based on war state.
    War: every 2 min. Peacetime: every 10 min."""
    try:
        if ps99_war_active:
            check_loop.change_interval(minutes=2)
        else:
            check_loop.change_interval(minutes=10)
    except Exception:
        pass


# ---------------- MEMORY CLEANUP ----------------

def cleanup_memory_for_removed_user(roblox_id):
    """Clean up in-memory caches when a user is removed from tracking."""
    rid = str(roblox_id).strip()
    status_cache.pop(rid, None)
    status_cache_time.pop(rid, None)
    offline_since.pop(rid, None)
    PROFILE_CACHE.pop(rid, None)
    try:
        PROFILE_CACHE.pop(int(rid), None)
    except Exception:
        pass


# ---------------- /cleanup_tickets COMMAND ----------------

@bot.tree.command(name="cleanup_tickets", description="Mark tickets as closed if their Discord channel no longer exists", guild=guild_obj)
@require_role()
async def cleanup_tickets(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    if not db_enabled():
        return await interaction.followup.send("Database is not available.", ephemeral=True)

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ticket_id, channel_id, status
                FROM mcwv_tickets
                WHERE status IN ('open', 'pending')
                ORDER BY created_at DESC
                LIMIT 100
            """)
            rows = cur.fetchall()

        if not rows:
            return await interaction.followup.send("No open/pending tickets to check.", ephemeral=True)

        closed_count = 0
        checked = 0
        for ticket_id, channel_id, status in rows:
            if not channel_id:
                continue
            checked += 1
            try:
                channel = bot.get_channel(int(channel_id))
                if channel is None:
                    channel = await bot.fetch_channel(int(channel_id))
            except discord.NotFound:
                try:
                    db_update_ticket_status(ticket_id, "closed", interaction.user.id,
                                            closed_at=datetime.now(timezone.utc),
                                            closed_by=interaction.user.id,
                                            close_reason="Channel deleted — auto-cleanup")
                    closed_count += 1
                except Exception:
                    pass
            except Exception:
                pass

        await interaction.followup.send(
            f"Checked **{checked}** open tickets.\n"
            f"Closed **{closed_count}** tickets with deleted channels.",
            ephemeral=True,
        )
    except Exception as e:
        print(f"[cleanup_tickets] error: {e}")
        traceback.print_exc()
        await interaction.followup.send(f"Cleanup failed: `{type(e).__name__}`", ephemeral=True)


@bot.tree.command(name="loas", description="List all active Leaves of Absence", guild=guild_obj)
@require_role()
async def loas_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    rows = db_list_active_loas()
    if not rows:
        return await interaction.followup.send("🏝️ No active LOAs.", ephemeral=True)

    embed = discord.Embed(
        title="🏝️ Active Leaves of Absence",
        color=discord.Color.from_rgb(96, 165, 250),
        timestamp=datetime.now(timezone.utc),
    )
    lines = []
    for rec_id, rid, uname, did, chid, started_by, started_at in rows:
        who = f"<@{did}>" if did else "`unknown discord`"
        start = f"<t:{int(started_at.timestamp())}:R>" if started_at else "`?`"
        ch = f"<#{chid}>" if chid else "no channel"
        lines.append(f"• **{uname or rid}** — {who}\n　· since {start} · {ch}")
    embed.description = "\n".join(lines)
    embed.set_footer(text="End an LOA with /endloa or the ✅ End LOA button in their ticket")
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="endloa", description="End a member's Leave of Absence (roles, channel and tracking restored)", guild=guild_obj)
@require_role()
@app_commands.describe(member="Discord member to end LOA for", roblox_id="Or: Roblox user ID of the member")
async def endloa_cmd(interaction: discord.Interaction, member: discord.Member = None, roblox_id: str = None):
    await interaction.response.defer(ephemeral=True)

    record = None
    if member:
        record = db_get_active_loa(discord_id=member.id)
    elif roblox_id:
        record = db_get_active_loa(roblox_id=str(roblox_id).strip())
    if not record:
        return await interaction.followup.send(
            "❌ No active LOA found for that member. Use `/loas` to see active LOAs.",
            ephemeral=True,
        )

    ok, notes = await perform_loa_revert(interaction.guild, record, interaction.user, "LOA ended from /endloa command")

    channel = None
    if record.get("ticket_channel_id"):
        channel = interaction.guild.get_channel(int(record["ticket_channel_id"]))
    if isinstance(channel, discord.TextChannel):
        try:
            await channel.send(
                f"✅ **LOA ended** by {interaction.user.mention} — welcome back!"
            )
        except Exception:
            pass

    summary = "\n".join(f"• {n}" for n in notes)
    await interaction.followup.send(
        f"🏝️ **LOA ended** for **{record.get('roblox_username') or 'unknown'}**\n{summary}",
        ephemeral=True,
    )


@bot.tree.command(name="setwartime", description="Manually set a battle's start/end times (overrides the API)", guild=guild_obj)
@require_role()
@app_commands.describe(
    battle_id="Battle ID (e.g. NinjaBattle2026)",
    start_time="Start time, UTC (e.g. 2026-08-01 17:00)",
    end_time="End time, UTC (e.g. 2026-08-15 15:00)",
)
async def setwartime_cmd(interaction: discord.Interaction, battle_id: str, start_time: str = None, end_time: str = None):
    await interaction.response.defer(ephemeral=True)
    if not db_enabled():
        return await interaction.followup.send("Database is not available.", ephemeral=True)

    def _parse(value):
        if not value:
            return None
        value = str(value).strip()
        if re.fullmatch(r"\d{9,11}", value):
            return datetime.fromtimestamp(int(value), tz=timezone.utc)
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
            try:
                return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        raise ValueError(f"cannot parse {value!r}")

    try:
        start = _parse(start_time)
        end = _parse(end_time)
    except ValueError as exc:
        return await interaction.followup.send(f"❌ {exc}. Use `YYYY-MM-DD HH:MM` (UTC).", ephemeral=True)
    if not start and not end:
        return await interaction.followup.send("❌ Provide at least one of start_time or end_time.", ephemeral=True)
    if start and end and start >= end:
        return await interaction.followup.send("❌ Start must be before end.", ephemeral=True)

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO battles (battle_id, battle_name, start_time, end_time, manually_edited, edited_by, edited_at)
                VALUES (%s, %s, %s, %s, TRUE, %s, NOW())
                ON CONFLICT (battle_id) DO UPDATE SET
                    start_time = COALESCE(EXCLUDED.start_time, battles.start_time),
                    end_time   = COALESCE(EXCLUDED.end_time, battles.end_time),
                    manually_edited = TRUE,
                    edited_by = EXCLUDED.edited_by,
                    edited_at = NOW()
            """, (battle_id.strip(), battle_id.strip(), start, end, interaction.user.id))
        conn.commit()
    except Exception as e:
        conn.rollback()
        return await interaction.followup.send(f"❌ DB error: `{type(e).__name__}: {e}`", ephemeral=True)

    await interaction.followup.send(
        f"✅ **{battle_id}** times set (manual override):\n"
        f"• Start: {f'<t:{int(start.timestamp())}:F>' if start else '— (unchanged)'}\n"
        f"• End: {f'<t:{int(end.timestamp())}:F>' if end else '— (unchanged)'}\n\n"
        f"ℹ️ The API will no longer overwrite these. Reset from the Hub admin or edit again.",
        ephemeral=True,
    )


# ---------------- READY ----------------
@bot.event
async def on_ready():
    global session, reminder_interval, reminder_channel_id

    print("🚀 ON_READY HIT")

    # ---------------- SESSION SAFETY ----------------
    if session is None:
        session = aiohttp.ClientSession()
        print("✅ aiohttp session created")
    elif session.closed:
        session = aiohttp.ClientSession()
        print("🔄 aiohttp session re-created (was closed)")

    # ---------------- PREVENT DOUBLE START ----------------
    if getattr(bot, "_ready_done", False):
        print("⚠️ on_ready already initialised, skipping setup")
        return

    bot._ready_done = True

    # ---------------- DB SCHEMA INIT ----------------
    try:
        if DATABASE_URL:
            await ensure_db_connection_async()  # connect in a worker thread, never blocks the loop
        init_db_schema()
        print("✅ DB schema initialized")
    except Exception as e:
        print(f"❌ DB schema init error: {e}")

    # ---------------- LOAD SAVED SETTINGS ----------------
    try:
        saved_interval = db_get_setting("reminder_interval")
        if saved_interval is not None:
            reminder_interval = int(saved_interval)
            reminder_loop.change_interval(minutes=reminder_interval)
            print(f"Loaded reminder interval: {reminder_interval}m")
        saved_channel = db_get_setting("reminder_channel_id")
        if saved_channel is not None:
            reminder_channel_id = int(saved_channel)
            print(f"Loaded reminder channel: {reminder_channel_id}")
    except Exception as e:
        print(f"⚠️ Failed to load reminder settings: {e}")

    try:
        bot.add_view(MCWVTicketPanelView())
        bot.add_view(ScreenshotUploadedView())
        bot.add_view(TicketWelcomeView("persistent"))
        bot.add_view(ApplicationReviewView("persistent"))
        print("✅ MCWV ticket views registered")
    except Exception as e:
        print(f"❌ Failed to register ticket views: {e}")

    # ---------------- SYNC COMMANDS ----------------
    try:
        synced = await bot.tree.sync(guild=guild_obj)
        print(f"✅ Synced {len(synced)} commands")
    except Exception as e:
        print(f"❌ Sync error: {e}")

    print(f"🤖 Logged in as {bot.user} ({bot.user.id})")

    # ---------------- START LOOPS ----------------
    try:
        start_bot_loops()
        print("✅ Bot loops started")
    except Exception as e:
        print(f"❌ Failed to start loops: {e}")

    # ---------------- DB CHECK ----------------
    try:
        tracked = db_get_all_tracked()
        print(f"👥 Tracking {len(tracked)} users")
    except Exception as e:
        print(f"❌ DB tracking error: {e}")

    # ---------------- INVITE SYSTEM INIT ----------------
    try:
        await setup_invite_system()
        print("🎟 Invite system started")
    except Exception as e:
        print(f"❌ Invite system failed: {e}")

    # ---------------- STARTUP SAFETY DELAY ----------------
    try:
        await asyncio.sleep(1)
    except Exception:
        pass

    # ---------------- GIVEAWAY SELF-HEAL ----------------
    try:
        force_sync_giveaway_state()
        print("🎁 Giveaway state synced")
    except Exception as e:
        print(f"❌ Giveaway sync error: {e}")

    # ---------------- TICKET REVIEW SELF-HEAL ----------------
    # If the staff review channel was deleted/recreated, re-post any missing
    # 'MCWV Application Ready for Review' messages for pending applications.
    try:
        guild = bot.get_guild(GUILD_ID) or (bot.guilds[0] if bot.guilds else None)
        restored = await restore_application_review_messages(guild)
        print(f"🎫 Ticket review restore done ({restored} re-posted)")
    except Exception as e:
        print(f"❌ Ticket review restore error: {e}")

    # ---------------- UPDATE PRESENCE ----------------
    try:
        await update_bot_presence()
    except Exception:
        pass

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
