import os
import re
import asyncio
import sqlite3
import platform
import resource
import secrets
from functools import wraps
import discord
import aiohttp
import traceback
from PIL import Image, ImageDraw, ImageFont, ImageFilter
from io import BytesIO
import math
import random
from datetime import datetime, timezone
import time

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

@app.route("/")
def home():
    return "Bot is alive"


# ---------------- ADMIN API ----------------
# The Hub talks to these routes server-to-server using X-Admin-API-Key.
# Keep every route registered before the Flask thread starts.
ADMIN_API_KEY = os.environ.get("BOT_ADMIN_API_KEY") or os.environ.get("ADMIN_API_KEY")
ADMIN_RESTART_ENABLED = os.environ.get("ALLOW_ADMIN_RESTART", "0") == "1"
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
            "currentWar": globals().get("CLAN_NAME", "MCWV"),
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
        },
        "loops": {
            "War Poll Loop": _loop_status("war_poll_loop"),
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

    return jsonify({"error": f"Unknown sync target: {target}"}), 400


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
        ) VALUES (1, 1, ?, ?, ?, ?, ?, ?, 0, ?, 0)
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
    db_exec("UPDATE giveaway_events SET message_id = ? WHERE id = 1", (message_id,))

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
        VALUES (1, 1, ?, ?, ?)
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

DB_PATH = "bot.db"


def sqlite_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def db_exec(query, params=()):
    with sqlite_connect() as conn:
        conn.execute(query, params)
        conn.commit()

def db_fetchone(query, params=()):
    with sqlite_connect() as conn:
        cur = conn.execute(query, params)
        return cur.fetchone()

def db_fetchall(query, params=()):
    with sqlite_connect() as conn:
        cur = conn.execute(query, params)
        return cur.fetchall()


def init_invite_tables():
    db_exec("""
    CREATE TABLE IF NOT EXISTS invite_events (
        id INTEGER PRIMARY KEY DEFAULT 1,
        active INTEGER DEFAULT 0,
        start_time INTEGER DEFAULT 0,
        end_time INTEGER DEFAULT 0,
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
    INSERT OR IGNORE INTO invite_events (id, active, start_time, end_time, channel_id)
    VALUES (1, 0, 0, 0, 0)
    """)


init_invite_tables()


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
        VALUES (?, ?)
        ON CONFLICT(user_id)
        DO UPDATE SET invites = invites + ?
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
    
def ensure_db_connection():
    global conn

    try:
        if conn is None or conn.closed != 0:
            print("🔄 Reconnecting to Neon DB...")

            conn = psycopg2.connect(
                DATABASE_URL,
                sslmode="require",
                connect_timeout=10
            )

            conn.autocommit = True

    except Exception as e:
        print("DB reconnect failed:", repr(e))
        conn = None

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
                return False, "Owner accounts cannot be removed from Roblox Links. Restore or edit the owner manually in Neon."

            # Important: do not DELETE from users. The Hub auth account lives in this table too.
            # Only remove Roblox tracking/link data.
            cur.execute("DELETE FROM user_alts WHERE discord_id = %s", (did,))
            cur.execute("UPDATE users SET roblox_id = NULL WHERE discord_id = %s", (did,))

        conn.commit()
        return True, "Player Roblox links removed. The Hub account was kept."
    except Exception as e:
        conn.rollback()
        print("db_remove_all_links_for_discord error:", e)
        return False, "Failed to remove player links."

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
CLAN_MEMBER_CATEGORY_IDS = [
    1503109089931034785,  # main
    1520511998633185280   # backup
]
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

# DATABASE ------------------------------

import psycopg2
import os

DATABASE_URL = os.environ.get("DATABASE_URL")

conn = None

def db_enabled():
    return conn is not None

if DATABASE_URL:
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = True  # 🔥 important fix

        print("Database connected")

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

            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS ticket_channel_id BIGINT")

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

        print("DB tables ready")

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


def db_get_broadcast_users():
    if not db_enabled():
        return []

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT roblox_id,
                       discord_id,
                       username,
                       COALESCE(role, 'member') AS role,
                       ticket_channel_id
                FROM users
                WHERE roblox_id IS NOT NULL
                  AND TRIM(CAST(roblox_id AS TEXT)) <> ''
                  AND discord_id IS NOT NULL
                ORDER BY username ASC
            """)
            return cur.fetchall()
    except Exception as e:
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
import random
import secrets
from typing import Optional, List, Tuple

GIVEAWAY_EDIT_ROLE_ID = 1501985889964789962
GIVEAWAY_LOG_CHANNEL_ID = 1502001938705682622


def init_giveaway_tables():
    db_exec("""
    CREATE TABLE IF NOT EXISTS giveaway_events (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        active INTEGER DEFAULT 0,
        prize TEXT,
        winners INTEGER DEFAULT 1,
        invites_per_entry INTEGER DEFAULT 2,
        start_time INTEGER DEFAULT 0,
        end_time INTEGER DEFAULT 0,
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
    INSERT OR IGNORE INTO giveaway_events (
        id, active, prize, winners, invites_per_entry,
        start_time, end_time, channel_id, message_id,
        thumbnail, created_by
    ) VALUES (1, 0, '', 1, 2, 0, 0, 0, 0, '', 0)
    """)


init_giveaway_tables()


def has_edit_role(member: discord.Member) -> bool:
    return any(role.id == GIVEAWAY_EDIT_ROLE_ID for role in member.roles)


def get_active_giveaway():
    return db_fetchone("""
        SELECT * FROM giveaway_events
        WHERE id = 1
    """)


def get_valid_invites(user_id: int) -> int:
    row = db_fetchone(
        "SELECT invites FROM invite_counts WHERE user_id = ?",
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


@bot.tree.command(name="giveaway_start", guild=guild_obj)
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            "UPDATE giveaway_events SET message_id = ? WHERE id = 1",
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


@bot.tree.command(name="giveaway_edit", guild=guild_obj)
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
            SET prize = ?, winners = ?, invites_per_entry = ?, thumbnail = ?
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


@bot.tree.command(name="giveaway_end", guild=guild_obj)
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
            "INSERT OR REPLACE INTO invite_cache (invite_code, inviter_id) VALUES (?, ?)",
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

        row = db_fetchone(
            "SELECT invites FROM invite_counts WHERE user_id = ?",
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

        await interaction.response.send_message(embed=embed, ephemeral=True)

# ---------------- INVITE DEBUG TOOLKIT ----------------

@bot.tree.command(name="invite_debug", guild=guild_obj)
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

@bot.tree.command(name="invite_snapshot_refresh", guild=guild_obj)
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

@bot.tree.command(name="invite_simulate", guild=guild_obj)
@require_role()
async def invite_simulate(interaction: discord.Interaction, amount: int = 1):

    await interaction.response.defer(ephemeral=True)

    try:
        if amount <= 0:
            return await interaction.followup.send(
                "❌ Amount must be at least 1.",
                ephemeral=True
            )

        increment_invite_count(interaction.user.id, amount)

        await interaction.followup.send(
            f"🧪 Simulated +{amount} invite(s) for you.",
            ephemeral=True
        )

    except Exception as e:
        import traceback
        print("SIMULATE ERROR:")
        print(traceback.format_exc())

        await interaction.followup.send(
            f"❌ Error: `{type(e).__name__}: {e}`",
            ephemeral=True
        )


@bot.tree.command(name="invite_full_test", guild=guild_obj)
@require_role()
async def invite_full_test(interaction: discord.Interaction):
    try:
        test_user = interaction.user.id

        increment_invite_count(test_user, 1)

        rows = db_fetchall(
            "SELECT user_id, invites FROM invite_counts ORDER BY invites DESC LIMIT 5"
        )

        leaderboard = "\n".join(
            f"{i+1}. <@{r['user_id']}> — {r['invites']}"
            for i, r in enumerate(rows)
        ) or "No data"

        await interaction.response.send_message(
            "🧪 Full Invite System Test Complete\n\n"
            f"Leaderboard:\n{leaderboard}",
            ephemeral=True
        )

    except Exception as e:
        print(f"[invite_full_test error] {e}")
        await interaction.response.send_message(
            "❌ Test failed (check console).",
            ephemeral=True
        )

@bot.tree.command(name="host_invite_event", guild=guild_obj)
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
            VALUES (?, ?, ?, ?, ?)
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
        
@bot.tree.command(name="end_invite_event", guild=guild_obj)
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
    if db_fetchone("SELECT 1 FROM invite_used_users WHERE user_id = ?", (member.id,)):
        return

    db_exec("INSERT INTO invite_used_users (user_id) VALUES (?)", (member.id,))

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
        "SELECT inviter_id FROM invite_cache WHERE invite_code = ?",
        (used,)
    )

    if not row:
        return

    inviter_id = int(row["inviter_id"])

    increment_invite(inviter_id)

    db_exec(
        "INSERT OR REPLACE INTO invite_member_links (member_id, inviter_id) VALUES (?, ?)",
        (member.id, inviter_id)
    )

@bot.event
async def on_member_remove(member: discord.Member):
    if member.bot:
        return

    row = db_fetchone(
        "SELECT inviter_id FROM invite_member_links WHERE member_id = ?",
        (member.id,)
    )

    if not row:
        return

    inviter_id = int(row["inviter_id"])

    # remove mapping
    db_exec(
        "DELETE FROM invite_member_links WHERE member_id = ?",
        (member.id,)
    )

    # decrease invite count safely
    db_exec("""
        UPDATE invite_counts
        SET invites = CASE WHEN invites > 0 THEN invites - 1 ELSE 0 END
        WHERE user_id = ?
    """, (inviter_id,))

@bot.tree.command(name="inviteleaderboard", guild=guild_obj)
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
BROADCAST_RECENT = {}


def has_broadcast_permission(member):
    if not isinstance(member, discord.Member):
        return False

    if member.guild and member.guild.owner_id == member.id:
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


def broadcast_user_from_row(row, points_map):
    roblox_id = str(row[0]).strip() if len(row) > 0 and row[0] is not None else ""
    discord_id = int(row[1]) if len(row) > 1 and row[1] is not None else 0
    username = str(row[2]).strip() if len(row) > 2 and row[2] is not None else roblox_id
    role = str(row[3] or "member").strip().lower() if len(row) > 3 else "member"
    ticket_channel_id = int(row[4]) if len(row) > 4 and row[4] is not None else None
    points = int(points_map.get(roblox_id, 0))

    return {
        "roblox_id": roblox_id,
        "discord_id": discord_id,
        "username": username,
        "role": role,
        "ticket_channel_id": ticket_channel_id,
        "points": points,
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
    users = [broadcast_user_from_row(row, points_map) for row in rows]

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

    return dedupe_recipients(selected)


def render_broadcast_message(template, recipient):
    return str(template or "").replace("{username}", str(recipient.get("username") or "")) \
        .replace("{points}", str(recipient.get("points", 0))) \
        .replace("{rank}", str(recipient.get("rank") or "—"))


def broadcast_embed_for(message, recipient):
    embed = discord.Embed(
        title="📢 MCWV Broadcast",
        description=render_broadcast_message(message, recipient),
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_footer(text="MCWV Staff Broadcast")
    return embed


async def send_broadcast_to_recipient(guild, recipient, delivery, style, message):
    rendered = render_broadcast_message(message, recipient)
    embed = broadcast_embed_for(message, recipient) if style == "embed" else None
    content = None if embed else f"📢 **MCWV Broadcast**\n{rendered}"

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


class BroadcastConfirmView(discord.ui.View):
    def __init__(self, *, sender_id, actor_name, recipients, audience, value, delivery, style, message):
        super().__init__(timeout=120)
        self.sender_id = sender_id
        self.actor_name = actor_name
        self.recipients = recipients
        self.audience = audience
        self.value = value
        self.delivery = delivery
        self.style = style
        self.message = message
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

        fingerprint = f"{self.sender_id}:{self.audience}:{self.value}:{self.delivery}:{self.style}:{self.message}:{','.join(str(r['discord_id']) for r in self.recipients)}"
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

        for recipient in self.recipients:
            ok, _where, error = await send_broadcast_to_recipient(
                interaction.guild,
                recipient,
                self.delivery,
                self.style,
                self.message,
            )
            if ok:
                sent += 1
            else:
                failed.append((recipient, error))
            await asyncio.sleep(0.8)

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

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.done = True
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(content="Broadcast cancelled.", view=self)


@bot.tree.command(name="broadcast", description="Send a staff broadcast to selected MCWV members", guild=guild_obj)
@app_commands.describe(
    audience="Who should receive the broadcast",
    delivery="Where to deliver the broadcast",
    style="Send as plain text or embed",
    message="Message to send. Supports {username}, {points}, {rank}",
    value="Threshold, N, or custom Discord IDs depending on audience",
    role="Discord role for the discord_role audience",
    user="Specific user for custom_user audience",
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
    audience: app_commands.Choice[str],
    delivery: app_commands.Choice[str],
    style: app_commands.Choice[str],
    message: str,
    value: str = "",
    role: discord.Role = None,
    user: discord.Member = None,
):
    if not has_broadcast_permission(interaction.user):
        return await interaction.response.send_message("❌ You do not have permission to use broadcasts.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        recipients = await resolve_broadcast_recipients(
            interaction,
            audience.value,
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

    if delivery.value == "ticket":
        missing_ticket_recipients = [item for item in recipients if not item.get("ticket_channel_id")]
        deliverable_count = len(recipients) - len(missing_ticket_recipients)

    actor_name = broadcast_actor_name(interaction)
    sample = recipients[:10]
    sample_text = "\n".join(
        f"• {item['username']} — {item['points']} pts — <@{item['discord_id']}>"
        for item in sample
    )

    embed = discord.Embed(
        title="Broadcast Preview",
        description=(
            f"**Audience:** {audience.name}\n"
            f"**Value:** {value or '—'}\n"
            f"**Delivery:** {delivery.name}\n"
            f"**Style:** {style.name}\n"
            f"**Recipients matched:** {len(recipients)}\n"
            f"**Will attempt:** {deliverable_count if delivery.value == 'ticket' else len(recipients)}\n"
            f"**Will fail / no ticket:** {len(missing_ticket_recipients) if delivery.value == 'ticket' else 0}\n\n"
            f"**Message:**\n{message[:1200]}"
        ),
        color=discord.Color.orange(),
    )
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
        audience=audience.value,
        value=value,
        delivery=delivery.value,
        style=style.value,
        message=message,
    )

    await interaction.followup.send(embed=embed, view=view, ephemeral=True)


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

        member = guild.get_member(discord_id)
        if member is None and discord_id:
            try:
                member = await guild.fetch_member(discord_id)
            except Exception:
                member = None

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


@bot.tree.command(name="broadcast_ticket_sync", description="Scan ticket categories and save member ticket channel IDs", guild=guild_obj)
@app_commands.describe(
    category="Optional category to scan. Leave empty to auto-detect ticket channels.",
    scan_all="Scan every text channel if auto-detect misses tickets. Slower but more complete."
)
async def broadcast_ticket_sync(
    interaction: discord.Interaction,
    category: discord.CategoryChannel = None,
    scan_all: bool = False,
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
    matched = []
    matched_channel_ids = set()

    for channel in channels:
        channel_key = normalize_ticket_key(f"{channel.name} {channel.topic or ''}")
        if not channel_key:
            continue

        for candidate in user_candidates:
            if candidate["matched"]:
                continue
            if any(key and (key in channel_key or channel_key in key) for key in candidate["keys"]):
                if db_set_ticket_channel(candidate["discord_id"], channel.id):
                    candidate["matched"] = True
                    matched_channel_ids.add(channel.id)
                    matched.append((candidate["username"], channel.name))
                break

    unmatched = [candidate["username"] for candidate in user_candidates if not candidate["matched"]]

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

    await interaction.followup.send(message[:1900], ephemeral=True)


@bot.tree.command(name="refreshprofile", guild=guild_obj)
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
        users = db_get_all() or []

        valid = 0
        missing_cache = 0

        for row in users:
            try:
                roblox_id = int(row[0])
            except Exception:
                print(f"[dbtest] bad DB row: {row}")
                continue

            bundle = PROFILE_CACHE.get(roblox_id) or PROFILE_CACHE.get(str(roblox_id))
            if not bundle:
                missing_cache += 1
                continue

            extended, profile, inventory, public_views = bundle[0]

            if profile or inventory or extended:
                valid += 1
            else:
                print(f"[dbtest] cached but empty for {roblox_id}")

        await interaction.response.send_message(
            f"DB OK: {len(users)} users\nValid profiles: {valid}\nMissing cache: {missing_cache}",
            ephemeral=True
        )

    except Exception as e:
        print(f"[dbtest] error: {e}")
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

ACTIVE_BATTLE_API = f"{PS99_API}/api/activeClanBattle"

@bot.tree.command(name="warinfo", description="Show current PS99 clan war details", guild=guild_obj)
async def warinfo(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=False)

    try:
        async with session.get(ACTIVE_BATTLE_API) as r:
            if r.status != 200:
                return await interaction.followup.send(
                    "❌ Could not reach the PS99 war API right now.",
                    ephemeral=True
                )

            if "application/json" not in r.headers.get("Content-Type", ""):
                text = await r.text()
                print("[warinfo] war api returned non-JSON:", text[:200])
                return await interaction.followup.send(
                    "❌ War API returned invalid data.",
                    ephemeral=True
                )

            war_data = await r.json()

        async with session.get(CLAN_API) as r:
            if r.status != 200:
                return await interaction.followup.send(
                    "❌ Could not reach the clan API right now.",
                    ephemeral=True
                )

            if "application/json" not in r.headers.get("Content-Type", ""):
                text = await r.text()
                print("[warinfo] clan api returned non-JSON:", text[:200])
                return await interaction.followup.send(
                    "❌ Clan API returned invalid data.",
                    ephemeral=True
                )

            clan_data = await r.json()

    except Exception as e:
        print("[warinfo error]", repr(e))
        return await interaction.followup.send(
            "❌ API request failed.",
            ephemeral=True
        )

    battle_id, battle = get_current_war(war_data, clan_data)

    if not battle:
        return await interaction.followup.send(
            "❌ Could not determine current war.",
            ephemeral=True
        )

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

    friendly_name = re.sub(
        r'(\d+)',
        r' \1',
        re.sub(r'([A-Z])', r' \1', str(battle_id))
    ).strip()

    contributions = sorted(
        battle.get("PointContributions", []),
        key=lambda x: x.get("Points", 0),
        reverse=True
    )

    total_points = battle.get("Points", 0)
    contributor_count = len(contributions)

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

    embed = discord.Embed(
        title=f"🎮 {friendly_name}",
        description=f"**{status_line}**",
        color=color
    )

    embed.add_field(name="Progress", value=bar, inline=False)
    embed.add_field(name="🕐 Start", value=discord.utils.format_dt(start_dt, 'F'), inline=True)
    embed.add_field(name="🏁 End", value=discord.utils.format_dt(finish_dt, 'F'), inline=True)
    embed.add_field(name="⏱ Time", value=time_field, inline=False)
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

    global session

    try:
        # ---------------- SESSION ----------------
        if session is None or session.closed:
            session = aiohttp.ClientSession()

        timeout = aiohttp.ClientTimeout(total=15)

        # ---------------- WAR API ----------------
        async with session.get(ACTIVE_BATTLE_API, timeout=timeout) as war_r:
            if war_r.status != 200:
                return await interaction.followup.send(
                    "❌ Could not reach the PS99 war API right now.",
                    ephemeral=True
                )

            if "application/json" not in war_r.headers.get("Content-Type", ""):
                text = await war_r.text()
                print("[LEADERBOARD] Non-JSON war API response:", text[:200])
                return await interaction.followup.send(
                    "❌ PS99 war API returned invalid data.",
                    ephemeral=True
                )

            war_data = await war_r.json()

        # ---------------- CLAN API ----------------
        async with session.get(CLAN_API, timeout=timeout) as clan_r:
            if clan_r.status != 200:
                return await interaction.followup.send(
                    "❌ Could not reach the PS99 clan API right now.",
                    ephemeral=True
                )

            if "application/json" not in clan_r.headers.get("Content-Type", ""):
                text = await clan_r.text()
                print("[LEADERBOARD] Non-JSON clan API response:", text[:200])
                return await interaction.followup.send(
                    "❌ PS99 clan API returned invalid data.",
                    ephemeral=True
                )

            clan_data = await clan_r.json()

        # ---------------- WAR DATA ----------------
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

        if not contributions:
            return await interaction.followup.send(
                "❌ No contribution data yet for this war.",
                ephemeral=True
            )

        total_points = battle.get("Points", 0)

        # ---------------- USER IDS ----------------
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

        # ---------------- ROBLOX USERNAMES ----------------
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

                    data = await r.json()

                    for u in data.get("data", []):
                        try:
                            uid = int(u["id"])
                            id_to_name[uid] = u.get("name", f"Unknown ({uid})")
                        except Exception:
                            continue

        except Exception as e:
            print("[LEADERBOARD ROBLOX NAME ERROR]", repr(e))

        # ---------------- DISCORD MAP ----------------
        tracked_rows = db_get_all_tracked()
        roblox_to_discord = {}

        for row in tracked_rows:
            try:
                roblox_to_discord[int(row[0])] = int(row[1])
            except Exception:
                continue

        # ---------------- BATTLE NAME ----------------
        battle_name = re.sub(
            r'(\d+)',
            r' \1',
            re.sub(r'([A-Z])', r' \1', str(battle_id))
        ).strip()

        # ---------------- WAR STATE ----------------
        now = datetime.now(timezone.utc).timestamp()
        finish_ts = war_config.get("FinishTime")
        start_ts = war_config.get("StartTime", 0)

        is_active = bool(finish_ts and start_ts <= now <= finish_ts)

        # ---------------- BUILD ENTRIES ----------------
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
                "discord_id": discord_id,
                "avatar": None
            })

        # ---------------- VALIDATION ----------------
        if not entries:
            return await interaction.followup.send(
                "❌ No valid leaderboard entries found.",
                ephemeral=True
            )

        # ---------------- VIEW ----------------
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
        import traceback
        print("[LEADERBOARD ERROR]")
        print(traceback.format_exc())

        await interaction.followup.send(
            f"❌ {type(e).__name__}: {e}",
            ephemeral=True
        )
        
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

        # ---------------- API CALL ----------------
        async with session.get(ACTIVE_BATTLE_API, timeout=timeout) as war_r:
            if war_r.status != 200:
                text = await war_r.text()
                print("[mystats] war api bad status:", war_r.status, text[:200])
                return await interaction.followup.send(
                    "❌ Could not reach the PS99 war API.",
                    ephemeral=True
                )

            if "application/json" not in war_r.headers.get("Content-Type", ""):
                text = await war_r.text()
                print("[mystats] war api non-json:", text[:200])
                return await interaction.followup.send(
                    "❌ PS99 war API returned invalid data.",
                    ephemeral=True
                )

            war_data = await war_r.json()

        async with session.get(CLAN_API, timeout=timeout) as clan_r:
            if clan_r.status != 200:
                text = await clan_r.text()
                print("[mystats] clan api bad status:", clan_r.status, text[:200])
                return await interaction.followup.send(
                    "❌ Could not reach the PS99 clan API.",
                    ephemeral=True
                )

            if "application/json" not in clan_r.headers.get("Content-Type", ""):
                text = await clan_r.text()
                print("[mystats] clan api non-json:", text[:200])
                return await interaction.followup.send(
                    "❌ PS99 clan API returned invalid data.",
                    ephemeral=True
                )

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

        pts = int(user_entry.get("Points", 0) or 0)
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

        top_pts = max(int(contributions[0].get("Points", 1) or 1), 1)
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
        import traceback
        print("[mystats error]")
        print(traceback.format_exc())
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

                        if any(int(e.get("UserID", 0)) == roblox_id for e in contributions):
                            war_count += 1

                        if start <= now <= end:
                            battle = b_data

                            contributions = sorted(
                                battle.get("PointContributions", []),
                                key=lambda x: x.get("Points", 0),
                                reverse=True
                            )

                            for i, entry in enumerate(contributions, start=1):
                                if int(entry.get("UserID", 0)) == roblox_id:
                                    points = int(entry.get("Points", 0) or 0)
                                    rank = i
                                    break
                            break

        except Exception as e:
            print("[profile] war API error:", e)

        # ---------------- PROFILE BUNDLE (FIX: FORCE REFRESH + TYPE SAFETY) ----------------
        extended_data, profile_data, inventory_data, public_views = await get_profile_bundle(
            session,
            roblox_id,
            force=True
        )

        # ---------------- AVATAR ----------------
        avatar_url = (
            discord_member.display_avatar.url
            if discord_member
            else interaction.user.display_avatar.url
        )

        # ---------------- EMBED ----------------
        embed = discord.Embed(
            title=f"📇 Player Profile — {roblox_name}",
            color=discord.Color.blurple()
        )

        embed.set_thumbnail(url=avatar_url)

        embed.add_field(name="🎮 Username", value=roblox_name, inline=False)
        embed.add_field(name="💬 Discord", value=discord_display, inline=False)
        embed.add_field(name="🆔 Roblox ID", value=str(roblox_id), inline=False)
        embed.add_field(name="🔗 Account Status", value=linked_status, inline=False)
        embed.add_field(name="🏷️ Clan Role", value=clan_role or "None", inline=False)

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

        # ---------------- VIEW ----------------
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

    try:
        async with session.get(CLAN_API) as r:
            if r.status != 200:
                print(f"[clanstats] CLAN_API bad status: {r.status}")
                return await interaction.followup.send(
                    f"❌ Could not reach the PS99 API. (status {r.status})",
                    ephemeral=True
                )

            raw = await r.json(content_type=None)
            if not isinstance(raw, dict):
                print(f"[clanstats] Invalid JSON root type: {type(raw)}")
                return await interaction.followup.send(
                    "❌ Invalid API response.",
                    ephemeral=True
                )

            data = raw.get("data", {})
            if not isinstance(data, dict):
                print(f"[clanstats] Missing/invalid data block: {raw}")
                return await interaction.followup.send(
                    "❌ Invalid API response data.",
                    ephemeral=True
                )

    except Exception as e:
        print(f"[clanstats] API request failed: {e}")
        traceback.print_exc()
        return await interaction.followup.send(
            f"❌ API request failed: `{type(e).__name__}`",
            ephemeral=True
        )

    name = data.get("Name", CLAN_NAME)
    level = data.get("Level", "?")

    members = data.get("Members", [])
    if not isinstance(members, list):
        print(f"[clanstats] Members not a list: {type(members)}")
        members = []

    battles = data.get("Battles", {})
    if not isinstance(battles, dict):
        print(f"[clanstats] Battles not a dict: {type(battles)}")
        battles = {}

    # Robust gem / clan bank detection
    gems = (
        data.get("Diamonds")
        or data.get("diamonds")
        or data.get("Bank")
        or data.get("ClanBank")
        or data.get("TotalDiamonds")
        or data.get("Stats", {}).get("Diamonds")
        or data.get("Economy", {}).get("Diamonds")
        or 0
    )

    if gems == 0:
        print(f"[clanstats] Gems resolved to 0. Top-level keys: {list(data.keys())}")
        if isinstance(data.get("Stats"), dict):
            print(f"[clanstats] Stats keys: {list(data['Stats'].keys())}")
        if isinstance(data.get("Economy"), dict):
            print(f"[clanstats] Economy keys: {list(data['Economy'].keys())}")

    # Best placement tracking
    total_battles = len(battles)
    best_battle = ""
    best_placement = None

    for bid, b in battles.items():
        if not isinstance(b, dict):
            print(f"[clanstats] Battle entry not a dict for {bid}: {type(b)}")
            continue

        placement = (
            b.get("Placement")
            or b.get("Rank")
            or b.get("Position")
            or b.get("ClanPlacement")
            or b.get("LeaderboardPosition")
        )

        if placement is None:
            continue

        try:
            placement = int(placement)
        except Exception:
            print(f"[clanstats] Bad placement value for {bid}: {placement}")
            continue

        # Lower placement is better, e.g. Top 38 is better than Top 120
        if best_placement is None or placement < best_placement:
            best_placement = placement
            best_battle = bid

    def friendly_battle(bid):
        if not bid:
            return "—"
        return re.sub(r'(\d+)', r' \1', re.sub(r'([A-Z])', r' \1', str(bid))).strip()

    try:
        embed = discord.Embed(
            title=f"🏰 {name} — Clan Overview",
            color=discord.Color.blurple()
        )

        embed.add_field(name="👥 Members", value=f"**{len(members)}**", inline=True)
        embed.add_field(name="💎 Gems", value=f"**{format_points(gems)}**", inline=True)
        embed.add_field(name="🏅 Clan Level", value=f"**{level}**", inline=True)

        embed.add_field(
            name="\u200b",
            value="─────────────────────── **Battle History** ───────────────────────",
            inline=False
        )

        embed.add_field(name="⚔️ Battles", value=f"**{total_battles}**", inline=True)

        embed.add_field(
            name="🔥 Best War",
            value=(
                f"**{friendly_battle(best_battle)}**\n"
                f"🥇 Top {best_placement if best_placement is not None else '—'}"
            ),
            inline=True
        )

        if best_placement is None:
            print("[clanstats] No placement found in any battle entry.")

        embed.set_footer(text="ps99.biggamesapi.io")
        await interaction.followup.send(embed=embed)

    except Exception as e:
        print(f"[clanstats] Embed build/send failed: {e}")
        traceback.print_exc()
        await interaction.followup.send(
            f"❌ Failed to build clan stats: `{type(e).__name__}`",
            ephemeral=True
        )

ACTIVE_BATTLE_API = f"{PS99_API}/api/activeClanBattle"

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
                return await interaction.followup.send(
                    "❌ Could not reach the PS99 war API.",
                    ephemeral=True
                )

            if "application/json" not in war_r.headers.get("Content-Type", ""):
                text = await war_r.text()
                print("[COMPARE] War API non-JSON:", text[:200])
                return await interaction.followup.send(
                    "❌ PS99 war API returned invalid data.",
                    ephemeral=True
                )

            war_data = await war_r.json()

        async with session.get(CLAN_API, timeout=timeout) as clan_r:
            if clan_r.status != 200:
                return await interaction.followup.send(
                    "❌ Could not reach the PS99 clan API.",
                    ephemeral=True
                )

            if "application/json" not in clan_r.headers.get("Content-Type", ""):
                text = await clan_r.text()
                print("[COMPARE] Clan API non-JSON:", text[:200])
                return await interaction.followup.send(
                    "❌ PS99 clan API returned invalid data.",
                    ephemeral=True
                )

            clan_data = await clan_r.json()

    except Exception as e:
        print("[COMPARE ERROR]", repr(e))
        return await interaction.followup.send(
            "❌ API request failed.",
            ephemeral=True
        )

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
        entry = next((e for e in contributions if int(e.get("UserID", 0)) == rid), None)
        rank = next((i + 1 for i, e in enumerate(contributions) if int(e.get("UserID", 0)) == rid), None)
        pts = entry["Points"] if entry else 0
        return pts, rank

    pts1, rank1 = get_entry(rid1)
    pts2, rank2 = get_entry(rid2)

    friendly = re.sub(r'(\d+)', r' \1', re.sub(r'([A-Z])', r' \1', battle_id)).strip()
    now = datetime.now(timezone.utc).timestamp()
    finish_ts = war_config.get("FinishTime")
    is_active = finish_ts and war_config.get("StartTime", 0) <= now <= finish_ts
    color = discord.Color.red() if is_active else discord.Color.dark_gold()

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
                _safe_call("db_set_user_status", None, rid, status)

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

ACTIVE_BATTLE_API = f"{PS99_API}/api/activeClanBattle"

@tasks.loop(minutes=20)
async def war_poll_loop():
    global bot_enabled, ps99_war_active, ps99_first_check, session

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

    # ---------------- GIVEAWAY LOOP ----------------
    if not check_giveaway_event.is_running():
        check_giveaway_event.start()


# ---------------- READY ----------------
@bot.event
async def on_ready():
    global session

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
