"""CW-style war collection / global cache.

Imported by main.py so this file can be edited/pasted on its own.
Looks up live bot state (conn, bot, settings) from __main__ on each entry.
"""
from __future__ import annotations

import asyncio
import os
import random
import re
import sys
import time
import traceback
from datetime import datetime, timezone

import aiohttp
import discord
from discord.ext import tasks
import psycopg2
from psycopg2.extras import execute_values

_OWN_NAMES = None


def _sync_from_main():
    """Copy names from main.py so we always see the current `conn` / bot."""
    global _OWN_NAMES
    main = sys.modules.get("__main__")
    if main is None:
        return
    g = globals()
    if _OWN_NAMES is None:
        _OWN_NAMES = set(g)
    for k, v in main.__dict__.items():
        if k in _OWN_NAMES or k.startswith("__"):
            continue
        g[k] = v


# ---------------- WAR-END CAPTURE (CW-BOT PARITY) ----------------
# CW Bot wins because it snapshots every clan's full contributor list WHILE the
# battle is live (all 75 members), before the API prunes contributors after the
# war ends. This pipeline does the same:
#   1. PRE-SCAN: a full 50k-clan scan starts WAR_CACHE_PRE_SCAN_HOURS before the
#      real finish time (user's War Schedule wins) — captures everyone while the
#      API still lists all 75 per clan.
#   2. END-SCAN: another full scan when the war ends (auto_cache_war_end) —
#      GREATEST upserts keep the best of both passes.
#   3. PRIORITY RE-SCANS: 1h and 10min before the end, MCWV + all clans of
#      tracked users get re-scanned on a dedicated connection so their final
#      pushes are captured exactly.
#   4. PARTICIPANTS: AwardUserIDs (participated, incl. 0-pointers) are stored in
#      cross_clan_participants for top clans + MCWV + tracked users' clans.
WAR_CACHE_PRE_SCAN_HOURS = max(1.0, float(os.environ.get("MCWV_WAR_PRE_SCAN_HOURS", "4") or "4"))
WAR_CACHE_SCAN_CONCURRENCY = max(4, int(os.environ.get("MCWV_WAR_SCAN_CONCURRENCY", "40") or "40"))
WAR_CACHE_PARTICIPANT_MAX_PLACE = max(0, int(os.environ.get("MCWV_WAR_PARTICIPANT_MAX_PLACE", "2500") or "2500"))
WAR_CACHE_ROLLING_INTERVAL_SECONDS = max(15 * 60, int(os.environ.get("MCWV_WAR_SCAN_ROLLING_SECONDS", "2700") or "2700"))
WAR_CACHE_SCAN_MAX_MINUTES = max(15, int(os.environ.get("MCWV_WAR_SCAN_MAX_MINUTES", "45") or "45"))
_scan_lock = None
_scan_started_at = None
_scan_label = None


def _get_scan_lock():
    global _scan_lock
    if _scan_lock is None:
        _scan_lock = asyncio.Lock()
    return _scan_lock


def _scan_status_line():
    if _scan_started_at is None:
        return "idle"
    ago = int((datetime.now(timezone.utc) - _scan_started_at).total_seconds())
    mins = ago // 60
    return f"{_scan_label or 'scan'} running · {mins}m"


def _open_scan_connection_sync():
    _sync_from_main()
    c = psycopg2.connect(
        DATABASE_URL,
        sslmode="require",
        connect_timeout=10,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=3,
        options="-c statement_timeout=60000 -c lock_timeout=5000 -c idle_in_transaction_session_timeout=30000",
    )
    c.autocommit = False
    return c


async def _open_scan_connection():
    return await asyncio.to_thread(_open_scan_connection_sync)


def _close_scan_connection_sync(c):
    if c is None:
        return
    try:
        c.close()
    except Exception:
        pass


async def run_scan_exclusive(coro, *, label, max_minutes=None):
    _sync_from_main()
    """One full sitemap scan at a time. Skip if already running. Never holds forever."""
    global GLOBAL_BACKFILL_RUNNING, _scan_started_at, _scan_label
    if max_minutes is None:
        max_minutes = WAR_CACHE_SCAN_MAX_MINUTES
    lock = _get_scan_lock()
    # timeout=0 on wait_for races the timer and skips even when idle.
    if lock.locked():
        print(f"[scan] {label} skipped — {_scan_status_line()}")
        return False
    await lock.acquire()
    _scan_started_at = datetime.now(timezone.utc)
    _scan_label = str(label)
    GLOBAL_BACKFILL_RUNNING = True
    try:
        await asyncio.wait_for(coro, timeout=max(60, int(max_minutes) * 60))
        return True
    except asyncio.TimeoutError:
        print(f"[scan] {label} timed out after {max_minutes}m")
        ops_log_soon(
            "cache",
            title="Scan timed out",
            description=f"**{label}** exceeded {max_minutes}m — cancelled.",
            level="error",
        )
        return False
    except Exception as exc:
        print(f"[scan] {label} crashed: {exc}")
        traceback.print_exc()
        return False
    finally:
        GLOBAL_BACKFILL_RUNNING = False
        _scan_started_at = None
        _scan_label = None
        try:
            lock.release()
        except Exception:
            pass


def queue_full_scan(battle_id, *, include_participants=True, label=None):
    _sync_from_main()
    """Fire-and-forget exclusive full scan. Never blocks the caller."""
    battle_id = str(battle_id)
    tag = label or f"full:{battle_id}"
    if _get_scan_lock().locked():
        print(f"[scan] {tag} not queued — {_scan_status_line()}")
        return

    async def _run():
        ok = await run_scan_exclusive(
            _auto_cache_full_scan(battle_id, include_participants=include_participants),
            label=tag,
        )
        if ok:
            key = normalize_hourly_battle_key(battle_id)
            db_set_setting(f"mcwv_war_cache_last_full_{key}", str(int(time.time())))

    asyncio.create_task(_run())


def freeze_mcwv_war_from_last_snapshot(battle_id):
    _sync_from_main()
    """Lock MCWV's last leaderboard snapshot into the durable war tables.

    Kicks after war-end must not shrink the report — this copies whoever was
    on the last hourly/history snapshot, even if the live clan is now 60.
    """
    if not battle_id or not db_enabled():
        return 0
    key = normalize_hourly_battle_key(str(battle_id))
    try:
        ensure_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cross_clan_player_history (
                    id BIGSERIAL PRIMARY KEY, roblox_id TEXT NOT NULL, battle_id TEXT NOT NULL,
                    battle_name TEXT, clan_name TEXT NOT NULL, points BIGINT NOT NULL DEFAULT 0,
                    rank INTEGER, total_contributors INTEGER, clan_place INTEGER,
                    earned_medal BOOLEAN DEFAULT FALSE, start_time BIGINT,
                    cached_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (roblox_id, battle_id, clan_name))
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cross_clan_participants (
                    battle_id TEXT NOT NULL, clan_name TEXT NOT NULL, roblox_id TEXT NOT NULL,
                    place INTEGER, captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (battle_id, clan_name, roblox_id))
            """)
            cur.execute("""
                SELECT end_time FROM battles
                WHERE regexp_replace(lower(battle_id), '[^a-z0-9]+', '', 'g') = %s
                   OR regexp_replace(lower(COALESCE(battle_name,'')), '[^a-z0-9]+', '', 'g') = %s
                ORDER BY end_time DESC NULLS LAST
                LIMIT 1
            """, (key, key))
            end_row = cur.fetchone()
            end_time = end_row[0] if end_row else None
            cur.execute("""
                SELECT roblox_id::text, username, points, rank
                FROM hourly_stats_player_snapshots
                WHERE battle_id = %s AND scheduled_at = (
                    SELECT MAX(scheduled_at) FROM hourly_stats_player_snapshots
                    WHERE battle_id = %s
                      AND (%s::timestamptz IS NULL OR scheduled_at <= %s)
                )
            """, (key, key, end_time, end_time))
            rows = cur.fetchall()
            if not rows:
                cur.execute("""
                    WITH last_ts AS (
                        SELECT MAX(captured_at) AS ts
                        FROM player_leaderboard_history
                        WHERE battle_id = %s AND points IS NOT NULL
                          AND (%s::timestamptz IS NULL OR captured_at <= %s)
                    )
                    SELECT DISTINCT ON (roblox_id)
                        roblox_id::text, username, points, rank
                    FROM player_leaderboard_history
                    WHERE battle_id = %s AND points IS NOT NULL
                      AND captured_at >= (SELECT ts FROM last_ts) - INTERVAL '3 minutes'
                      AND captured_at <= COALESCE(%s::timestamptz, (SELECT ts FROM last_ts))
                    ORDER BY roblox_id, captured_at DESC
                """, (key, end_time, end_time, key, end_time))
                rows = cur.fetchall()
            cur.execute("""
                ALTER TABLE cross_clan_player_history
                    ADD COLUMN IF NOT EXISTS clan_member_rank INTEGER
            """)
            cur.execute("""
                ALTER TABLE cross_clan_player_history
                    ADD COLUMN IF NOT EXISTS clan_member_count INTEGER
            """)
            n = 0
            clan_size = len(rows or [])
            for rid, uname, pts, rank in rows or []:
                rid = str(rid or "").strip()
                if not rid:
                    continue
                # NEVER write clan-local rank into `rank` — that column is global CW rank.
                # Snapshot position lives in clan_member_rank / clan_member_count (67/75).
                cur.execute("""
                    INSERT INTO cross_clan_player_history
                        (roblox_id, battle_id, battle_name, clan_name, points, rank,
                         total_contributors, clan_place, earned_medal, start_time,
                         clan_member_rank, clan_member_count)
                    VALUES (%s, %s, %s, %s, %s, NULL, NULL, NULL, FALSE, NULL, %s, %s)
                    ON CONFLICT (roblox_id, battle_id, clan_name)
                    DO UPDATE SET
                        points = GREATEST(cross_clan_player_history.points, EXCLUDED.points),
                        clan_member_rank = COALESCE(EXCLUDED.clan_member_rank, cross_clan_player_history.clan_member_rank),
                        clan_member_count = COALESCE(EXCLUDED.clan_member_count, cross_clan_player_history.clan_member_count),
                        cached_at = NOW()
                """, (rid, str(battle_id), str(battle_id), CLAN_NAME, int(pts or 0), rank, clan_size))
                cur.execute("""
                    INSERT INTO cross_clan_participants (battle_id, clan_name, roblox_id, place, captured_at)
                    VALUES (%s, %s, %s, NULL, NOW())
                    ON CONFLICT DO NOTHING
                """, (str(battle_id), CLAN_NAME, rid))
                n += 1
        conn.commit()
        print(f"[war-cache] froze {n} MCWV snapshot rows for {battle_id}")
        return n
    except Exception as exc:
        print(f"[war-cache] freeze snapshot failed: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return 0


async def auto_cache_war_end(battle_id):
    _sync_from_main()
    """Auto-cache a war's data when it ends. Called from war_poll_loop.
    
    Triggers a full 50k clan scan via the sitemap so we capture ALL
    contributor data while it's still available (the API only returns
    full PointContributions for active/recently-ended battles).
    Runs in the background so the war-end announcement isn't delayed."""
    if not battle_id:
        return
    if not DATABASE_URL:
        return
    frozen = 0
    try:
        frozen = await asyncio.to_thread(freeze_mcwv_war_from_last_snapshot, battle_id) or 0
    except Exception as exc:
        print(f"[cross-clan cache] freeze failed: {exc}")
        ops_log_soon(
            "cache",
            title="MCWV freeze failed",
            description=f"`{battle_id}`\n`{type(exc).__name__}: {exc}`",
            level="error",
        )
    print(f"[cross-clan cache] auto-cache full scan queued for {battle_id}")
    ops_log_soon(
        "cache",
        title="War-end scan queued",
        description=f"**{battle_id}**\nFull clan scan while the API still has the roster.",
        level="info",
        fields=[{"name": "MCWV freeze", "value": f"{frozen} players", "inline": True}],
        edit_key=f"scan:{battle_id}",
    )
    queue_full_scan(battle_id, include_participants=True, label=f"war-end:{battle_id}")


def _rank_one_battle_sql(conn, battle_id):
    """CW-style global rank: unique players, MAX(points), total = unique count."""
    if not battle_id:
        return 0
    key = str(battle_id)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH ids AS (
                    SELECT DISTINCT battle_id
                    FROM cross_clan_player_history
                    WHERE battle_id = %s
                       OR regexp_replace(lower(COALESCE(battle_id,'')), '[^a-z0-9]+', '', 'g')
                        = regexp_replace(lower(%s), '[^a-z0-9]+', '', 'g')
                ),
                best AS (
                    SELECT h.roblox_id::text AS roblox_id, MAX(h.points) AS pts
                    FROM cross_clan_player_history h
                    JOIN ids ON ids.battle_id = h.battle_id
                    WHERE COALESCE(h.points, 0) > 0
                    GROUP BY h.roblox_id::text
                ),
                ranked AS (
                    SELECT roblox_id,
                           ROW_NUMBER() OVER (ORDER BY pts DESC, roblox_id ASC) AS gr,
                           COUNT(*) OVER () AS gt
                    FROM best
                )
                UPDATE cross_clan_player_history h
                SET rank = r.gr,
                    total_contributors = r.gt
                FROM ranked r, ids
                WHERE h.roblox_id::text = r.roblox_id
                  AND h.battle_id = ids.battle_id
            """, (key, key))
            updated = cur.rowcount
        conn.commit()
        print(f"[war-cache] ranked {updated} rows for {key}")
        return int(updated or 0)
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[war-cache] rank failed for {battle_id}: {exc}")
        return 0


async def _auto_cache_full_scan(battle_id, include_participants=False, only_clans=None):
    _sync_from_main()
    """Background task: scan all 50k clans (or a priority subset) for one battle.

    include_participants=True also captures AwardUserIDs into
    cross_clan_participants (CW-Bot parity: everyone who participated, even
    0-point members) for clans with a real placement or that matter to us."""
    battle_id = str(battle_id)
    scan_session = aiohttp.ClientSession()
    scan_conn = None
    started = time.time()
    fetch_failed = 0

    try:
        if only_clans:
            clan_names = [str(n) for n in only_clans if n]
            print(f"[auto-cache] priority scan: {len(clan_names)} clans for {battle_id}")
        else:
            clan_names = await fetch_all_clan_names_from_sitemap(scan_session)
        if not clan_names:
            print(f"[auto-cache] no clans for {battle_id}")
            ops_log_soon(
                "cache",
                title="Scan aborted",
                description=f"**{battle_id}** — sitemap returned no clans.",
                level="warning",
                edit_key=f"scan:{battle_id}",
            )
            return

        scan_conn = await _open_scan_connection()

        def _ensure(c):
            try:
                with c.cursor() as cur:
                    cur.execute("""CREATE TABLE IF NOT EXISTS cross_clan_player_history (
                        id BIGSERIAL PRIMARY KEY, roblox_id TEXT NOT NULL, battle_id TEXT NOT NULL,
                        battle_name TEXT, clan_name TEXT NOT NULL, points BIGINT NOT NULL DEFAULT 0,
                        rank INTEGER, total_contributors INTEGER, clan_place INTEGER,
                        earned_medal BOOLEAN DEFAULT FALSE, start_time BIGINT,
                        cached_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        UNIQUE (roblox_id, battle_id, clan_name))""")
                    cur.execute("CREATE INDEX IF NOT EXISTS cross_clan_history_roblox_idx ON cross_clan_player_history (roblox_id)")
                    cur.execute("CREATE INDEX IF NOT EXISTS cross_clan_history_battle_idx ON cross_clan_player_history (battle_id)")
                    cur.execute("""CREATE TABLE IF NOT EXISTS cross_clan_participants (
                        battle_id TEXT NOT NULL,
                        clan_name TEXT NOT NULL,
                        roblox_id TEXT NOT NULL,
                        place INTEGER,
                        captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (battle_id, clan_name, roblox_id))""")
                    cur.execute("CREATE INDEX IF NOT EXISTS cross_clan_participants_roblox_idx ON cross_clan_participants (roblox_id)")
                c.commit()
            except Exception:
                try:
                    c.rollback()
                except Exception:
                    pass
        await asyncio.to_thread(_ensure, scan_conn)

        # Clans whose participants we ALWAYS keep: MCWV + clans of tracked users.
        def _priority_clans(c):
            priority = {_normalize_clan_name(CLAN_NAME)}
            try:
                with c.cursor() as cur:
                    cur.execute("""
                        SELECT DISTINCT clan_name FROM cross_clan_player_history
                        WHERE roblox_id IN (SELECT TRIM(roblox_id) FROM users WHERE roblox_id IS NOT NULL)
                    """)
                    for (cn,) in cur.fetchall():
                        norm = _normalize_clan_name(str(cn))
                        if norm:
                            priority.add(norm)
            except Exception as exc:
                print(f"[auto-cache] priority clans query failed: {exc}")
                try:
                    c.rollback()
                except Exception:
                    pass
            return priority

        priority_clans = await asyncio.to_thread(_priority_clans, scan_conn) if include_participants else set()

        CONCURRENCY = WAR_CACHE_SCAN_CONCURRENCY
        clans_with_data = 0
        total_contribs = 0
        total_participants = 0
        pending_rows = []
        pending_participants = []

        print(f"[auto-cache] scanning {len(clan_names)} clans for {battle_id} (concurrency={CONCURRENCY}, participants={include_participants})")
        ops_log_soon(
            "cache",
            title="Scanning clans",
            description=f"**{battle_id}**",
            level="info",
            fields=[
                {"name": "Clans", "value": f"{len(clan_names):,}", "inline": True},
                {"name": "Participants", "value": "yes" if include_participants else "no", "inline": True},
            ],
            edit_key=f"scan:{battle_id}",
        )

        for batch_start in range(0, len(clan_names), CONCURRENCY):
            batch = clan_names[batch_start:batch_start + CONCURRENCY]
            results = await asyncio.gather(
                *(_fetch_clan_contributions(scan_session, name) for name in batch),
                return_exceptions=True,
            )

            for name, result in zip(batch, results):
                if isinstance(result, Exception) or result is None:
                    fetch_failed += 1
                    continue
                if not result:
                    continue
                rows, participants_by_battle, places_by_battle = result
                # Filter to this battle (fuzzy — RoyalBattle2026 vs royal battle 2026)
                norm = normalize_hourly_battle_key(battle_id)
                battle_rows = []
                for r in rows or []:
                    if r[1] == battle_id or normalize_hourly_battle_key(r[1]) == norm:
                        battle_rows.append((r[0], str(battle_id), r[2], r[3], r[4], r[5], r[6], r[7] if len(r) > 7 else None))
                if battle_rows:
                    clans_with_data += 1
                    total_contribs += len(battle_rows)
                    pending_rows.extend(battle_rows)

                if include_participants:
                    parts = participants_by_battle.get(battle_id) or participants_by_battle.get(str(battle_id)) or []
                    if parts:
                        place = places_by_battle.get(battle_id) or places_by_battle.get(str(battle_id)) or 0
                        norm = _normalize_clan_name(str(name))
                        keep = (norm in priority_clans) or (0 < int(place or 0) <= WAR_CACHE_PARTICIPANT_MAX_PLACE)
                        if keep:
                            total_participants += len(parts)
                            pending_participants.extend((str(battle_id), str(name), uid, int(place) or None) for uid in parts)

            if len(pending_rows) >= 5000:
                await asyncio.to_thread(_insert_raw_contributions, scan_conn, pending_rows)
                pending_rows.clear()
            if len(pending_participants) >= 5000:
                await asyncio.to_thread(_insert_participants, scan_conn, pending_participants)
                pending_participants.clear()

            await asyncio.sleep(0)
            if (batch_start // CONCURRENCY + 1) % 100 == 0:
                elapsed = time.time() - started
                done = min(batch_start + CONCURRENCY, len(clan_names))
                print(f"[auto-cache] {done}/{len(clan_names)} clans, {total_contribs:,} contribs, {total_participants:,} participants, {elapsed:.0f}s")
                ops_log_soon(
                    "cache",
                    title="Scanning clans",
                    description=f"**{battle_id}**",
                    level="info",
                    fields=[
                        {"name": "Progress", "value": f"{done:,}/{len(clan_names):,}", "inline": True},
                        {"name": "Contribs", "value": f"{total_contribs:,}", "inline": True},
                        {"name": "Elapsed", "value": f"{elapsed:.0f}s", "inline": True},
                    ],
                    edit_key=f"scan:{battle_id}",
                )
                await asyncio.sleep(0)

        if pending_rows:
            await asyncio.to_thread(_insert_raw_contributions, scan_conn, pending_rows)
            pending_rows.clear()
        if pending_participants:
            await asyncio.to_thread(_insert_participants, scan_conn, pending_participants)
            pending_participants.clear()

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

        ranked = await asyncio.to_thread(_rank_one_battle_sql, scan_conn, battle_id)
        elapsed = time.time() - started
        print(f"[auto-cache] {battle_id}: {clans_with_data} clans, {total_contribs:,} contribs, {total_participants:,} participants, {ranked:,} ranked, {fetch_failed} failed, {elapsed:.0f}s")
        mins = elapsed / 60
        total_names = max(1, len(clan_names))
        complete = fetch_failed < max(100, int(total_names * 0.05))
        key = normalize_hourly_battle_key(battle_id)
        db_set_setting(f"mcwv_war_scan_complete_{key}", "1" if complete else "0")
        ops_log_soon(
            "cache",
            title="Scan finished" if complete else "Scan finished (degraded)",
            description=f"**{battle_id}** cached.",
            level="success" if complete else "warning",
            fields=[
                {"name": "Clans with data", "value": f"{clans_with_data:,}", "inline": True},
                {"name": "Contribs", "value": f"{total_contribs:,}", "inline": True},
                {"name": "Participants", "value": f"{total_participants:,}", "inline": True},
                {"name": "Ranked", "value": f"{ranked:,}", "inline": True},
                {"name": "Failed fetches", "value": f"{fetch_failed:,}", "inline": True},
                {"name": "Time", "value": f"{mins:.1f}m", "inline": True},
            ],
            edit_key=f"scan:{battle_id}",
        )

    except Exception as e:
        print(f"[auto-cache] FATAL: {e}")
        traceback.print_exc()
        ops_log_soon(
            "cache",
            title="Scan crashed",
            description=f"**{battle_id}**\n`{type(e).__name__}: {e}`",
            level="error",
            edit_key=f"scan:{battle_id}",
        )
    finally:
        try:
            await asyncio.to_thread(_close_scan_connection_sync, scan_conn)
        except Exception:
            pass
        try:
            await scan_session.close()
        except Exception:
            pass


def get_cached_player_history(roblox_id):
    _sync_from_main()
    """Read a player's full cross-clan history from the permanent cache.
    Returns a list of dicts sorted by start time (most recent first).
    Uses a dedicated connection so we never fight the bot's 1.8s timeout
    or a live scan writing on `conn`."""
    if not DATABASE_URL:
        return []
    rid = str(roblox_id).strip()
    c = None
    try:
        c = _open_scan_connection_sync()
        with c.cursor() as cur:
            # Equality on roblox_id uses cross_clan_history_roblox_idx.
            # TRIM() forced a seq scan of 900k+ rows and hit statement_timeout.
            cur.execute("""
                ALTER TABLE cross_clan_player_history
                    ADD COLUMN IF NOT EXISTS clan_member_rank INTEGER
            """)
            cur.execute("""
                ALTER TABLE cross_clan_player_history
                    ADD COLUMN IF NOT EXISTS clan_member_count INTEGER
            """)
            cur.execute("""
                SELECT battle_id, battle_name, clan_name, points, rank,
                       total_contributors, clan_place, earned_medal, start_time,
                       clan_member_rank, clan_member_count
                FROM cross_clan_player_history
                WHERE roblox_id = %s
                ORDER BY start_time DESC NULLS LAST, battle_id DESC
            """, (rid,))
            rows = cur.fetchall()
        parsed = []
        for r in rows:
            rank = int(r[4]) if r[4] not in (None, 0) else None
            total = int(r[5]) if r[5] not in (None, 0) else None
            parsed.append({
                "battleId": r[0],
                "title": r[1] or _friendly_battle_name(r[0]),
                "clan": r[2],
                "points": int(r[3] or 0),
                "rank": rank,
                "total": total,
                "clanPlace": r[6],
                "earnedMedal": bool(r[7]),
                "startTime": int(r[8]) if r[8] else 0,
                "clanMemberRank": int(r[9]) if r[9] not in (None, 0) else None,
                "clanMemberCount": int(r[10]) if r[10] not in (None, 0) else None,
                "betterThan": cw_better_pct(rank, total),
            })
        # One row per war: MCWV wins over a stale other-clan row.
        merged = {}
        for item in parsed:
            key = normalize_hourly_battle_key(item.get("battleId"))
            if not key:
                continue
            prev = merged.get(key)
            if prev is None:
                merged[key] = item
                continue
            this_mcwv = _normalize_clan_name(item.get("clan")) == _normalize_clan_name(CLAN_NAME)
            prev_mcwv = _normalize_clan_name(prev.get("clan")) == _normalize_clan_name(CLAN_NAME)
            if this_mcwv and not prev_mcwv:
                merged[key] = item
            elif this_mcwv == prev_mcwv and int(item.get("points") or 0) > int(prev.get("points") or 0):
                merged[key] = item
        return sorted(merged.values(), key=lambda x: (x.get("startTime") or 0, str(x.get("battleId") or "")), reverse=True)
    except Exception as e:
        print(f"[cross-clan cache] player history read failed: {e}")
        if c is not None:
            try:
                c.rollback()
            except Exception:
                pass
        return []
    finally:
        _close_scan_connection_sync(c)


def get_cached_battle_stats(battle_id):
    _sync_from_main()
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


# ---------------- GLOBAL BACKFILL (all clans from sitemap) ----------------
# Scans every clan on db.biggames.io (50k+) via the legacy PS99 API, extracts
# PointContributions for every battle, and rebuilds the cross_clan_player_history
# table with TRUE global ranks + totalContributors — same data CW Bot has.
#
# Two-pass design:
#   Pass 1: Fetch all clans concurrently, INSERT raw contributions (rank=NULL)
#   Pass 2: SQL window function computes global rank + total per battle

GLOBAL_BACKFILL_RUNNING = False


async def _fetch_clan_contributions(scan_session, clan_name):
    _sync_from_main()
    """Fetch ONE clan's contribution data for every battle it has.

    Returns (rows, participants_by_battle, places_by_battle) or [] on failure:
      rows: list of (roblox_id, battle_id, battle_name, clan_name, points,
             clan_place, earned_medal, start_time) — scorers only (points > 0).
      participants_by_battle: {battle_id: [roblox_id, ...]} — AwardUserIDs,
             everyone who participated including 0-point members.
      places_by_battle: {battle_id: place}
    """
    if not scan_session or getattr(scan_session, "closed", True):
        return []
    url = f"{PS99_API}/api/clan/{str(clan_name)}"
    payload = None
    for attempt in range(3):
        try:
            async with scan_session.get(
                url,
                headers={"User-Agent": "MCWV-Bot/1.0", "Accept": "application/json"},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as res:
                if res.status == 200:
                    payload = await res.json(content_type=None)
                    break
                if res.status in (429, 500, 502, 503, 504):
                    await asyncio.sleep(0.35 * (2 ** attempt) + random.random() * 0.25)
                    continue
                return None
        except Exception:
            if attempt >= 2:
                return None
            await asyncio.sleep(0.35 * (2 ** attempt))
    if payload is None:
        return None

    if not isinstance(payload, dict):
        return []
    data = payload.get("data", {})
    battles = data.get("Battles") or data.get("battles") or {}
    if not isinstance(battles, dict):
        return []

    rows = []
    participants_by_battle = {}
    places_by_battle = {}
    for battle_id, battle in battles.items():
        if not isinstance(battle, dict):
            continue
        contribs = battle.get("PointContributions") or battle.get("pointContributions") or []
        place = _safe_int(battle.get("Place") or battle.get("place"))
        medal = bool(battle.get("EarnedMedal") or battle.get("earnedMedal"))
        places_by_battle[str(battle_id)] = place
        for c in contribs if isinstance(contribs, list) else []:
            if not isinstance(c, dict):
                continue
            uid = str(c.get("UserID") or c.get("userId") or "").strip()
            pts = _safe_int(c.get("Points") or c.get("points"))
            if not uid or pts <= 0:
                continue
            rows.append((uid, str(battle_id), str(battle_id), str(clan_name), pts, place, medal, None))
        award_ids = battle.get("AwardUserIDs") or battle.get("awardUserIDs") or []
        if isinstance(award_ids, list) and award_ids:
            participants_by_battle[str(battle_id)] = [str(uid) for uid in award_ids]
    return rows, participants_by_battle, places_by_battle


async def fetch_all_clan_names_from_sitemap(scan_session=None):
    _sync_from_main()
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
                    clan_place = COALESCE(cross_clan_player_history.clan_place, EXCLUDED.clan_place),
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


def _insert_participants(conn, rows):
    """Batch-insert participant rows (AwardUserIDs) into cross_clan_participants.
    Rows: (battle_id, clan_name, roblox_id, place)."""
    if not rows:
        return 0
    try:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO cross_clan_participants (battle_id, clan_name, roblox_id, place)
                VALUES %s
                ON CONFLICT (battle_id, clan_name, roblox_id) DO NOTHING
            """, rows, page_size=1000)
        conn.commit()
        return len(rows)
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[auto-cache] participants insert failed: {exc}")
        return 0


async def _auto_cache_priority_scan(battle_id, clan_names):
    _sync_from_main()
    """Re-scan a small set of clans (MCWV + tracked users' clans) near war end
    so their final pushes are captured exactly, like CW Bot. Uses a DEDICATED
    DB connection so it can run safely alongside the full 50k scan."""
    if not clan_names or not DATABASE_URL:
        return
    battle_id = str(battle_id)
    scan_session = aiohttp.ClientSession()
    local_conn = None
    started = time.time()
    try:
        def _open():
            c = psycopg2.connect(
                DATABASE_URL,
                sslmode="require",
                connect_timeout=10,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=3,
            )
            c.autocommit = False
            return c

        local_conn = await asyncio.to_thread(_open)

        pending_rows = []
        pending_participants = []
        for name in clan_names:
            result = await _fetch_clan_contributions(scan_session, name)
            if not result:
                continue
            rows, participants_by_battle, places_by_battle = result
            battle_rows = [r for r in rows if r[1] == battle_id]
            pending_rows.extend(battle_rows)
            parts = participants_by_battle.get(battle_id) or participants_by_battle.get(str(battle_id)) or []
            if parts:
                place = places_by_battle.get(battle_id) or places_by_battle.get(str(battle_id)) or 0
                pending_participants.extend((str(battle_id), str(name), uid, int(place) or None) for uid in parts)

        if pending_rows:
            await asyncio.to_thread(_insert_raw_contributions, local_conn, pending_rows)
        if pending_participants:
            await asyncio.to_thread(_insert_participants, local_conn, pending_participants)

        def _rank():
            total = 0
            with local_conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM cross_clan_player_history WHERE battle_id = %s", (battle_id,))
                total = int(cur.fetchone()[0] or 0)
                if total:
                    cur.execute("""
                        WITH ranked AS (
                            SELECT id, ROW_NUMBER() OVER (ORDER BY points DESC, roblox_id ASC) AS gr, %s AS gt
                            FROM cross_clan_player_history WHERE battle_id = %s
                        )
                        UPDATE cross_clan_player_history h SET rank = r.gr, total_contributors = r.gt
                        FROM ranked r WHERE h.id = r.id
                    """, (total, battle_id))
            local_conn.commit()
            return total

        ranked_total = await asyncio.to_thread(_rank)
        print(f"[auto-cache] priority scan {battle_id}: {len(pending_rows)} contrib rows, {len(pending_participants)} participants, ranked over {ranked_total} in {time.time()-started:.0f}s")
    except Exception as exc:
        print(f"[auto-cache] priority scan FATAL: {exc}")
        traceback.print_exc()
    finally:
        if local_conn is not None:
            try:
                local_conn.close()
            except Exception:
                pass
        await scan_session.close()


async def get_priority_clan_names():
    _sync_from_main()
    """MCWV + every clan that any tracked user has history in (small set)."""
    clans = {str(CLAN_NAME)}
    if db_enabled():
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT clan_name FROM cross_clan_player_history
                    WHERE roblox_id IN (SELECT TRIM(roblox_id) FROM users WHERE roblox_id IS NOT NULL)
                """)
                for (cn,) in cur.fetchall():
                    if cn:
                        clans.add(str(cn))
        except Exception as exc:
            print(f"[war-cache] priority clans query failed: {exc}")
    return sorted(clans)


@tasks.loop(minutes=10)
async def war_cache_window_loop():
    """CW-Bot-parity scheduler: full capture in the last hours of a war."""
    _sync_from_main()
    if not DATABASE_URL or not db_enabled():
        return
    try:
        battle_id = await get_active_battle_id_for_placement()
        if not battle_id:
            return
        key = normalize_hourly_battle_key(str(battle_id))

        # Finish time: user's War Schedule first, then the API config.
        st = get_battles_row_for(battle_id)
        finish = float(st["finish"]) if (st and st.get("finish")) else None
        if not finish:
            payload = await fetch_json_for_placement(ACTIVE_BATTLE_API)
            cfg = {}
            if isinstance(payload, dict):
                data = payload.get("data", {}) if isinstance(payload.get("data"), dict) else {}
                cfg = data.get("configData", {}) if isinstance(data.get("configData"), dict) else {}
            finish = pick_first_int(cfg, ("FinishTime", "finishTime", "finish_time"))
            if finish and finish > 10_000_000_000:
                finish //= 1000
        if not finish:
            return

        now = time.time()
        hours_left = (finish - now) / 3600.0
        if hours_left <= 0:
            # Schedule says over; API may still have full 75s. Freeze once,
            # keep trying a full scan until one completes.
            if not db_get_setting(f"mcwv_war_cache_post_{key}"):
                db_set_setting(f"mcwv_war_cache_post_{key}", str(int(now)))
            try:
                await asyncio.to_thread(freeze_mcwv_war_from_last_snapshot, battle_id)
            except Exception as exc:
                print(f"[war-cache] post-end freeze failed: {exc}")
            if db_get_setting(f"mcwv_war_scan_complete_{key}") != "1" and not _get_scan_lock().locked():
                admin_log("War Cache Post-End Scan", f"{battle_id}: schedule ended, API still live — full capture queued.")
                queue_full_scan(battle_id, include_participants=True, label=f"post-end:{battle_id}")
            return

        # Rolling full scans in the last N hours so a restart doesn't mean zero data.
        if hours_left <= WAR_CACHE_PRE_SCAN_HOURS:
            last = db_get_setting(f"mcwv_war_cache_last_full_{key}")
            due = True
            if last:
                try:
                    due = (now - float(last)) >= WAR_CACHE_ROLLING_INTERVAL_SECONDS
                except Exception:
                    due = True
            if due and not _get_scan_lock().locked():
                admin_log("War Cache Rolling Scan", f"{battle_id}: T-{hours_left:.1f}h full capture queued.")
                queue_full_scan(battle_id, include_participants=True, label=f"rolling:{battle_id}")

        # 2) Priority re-scans of MCWV + tracked users' clans near the end.
        for phase, cutoff in (("prio_1h", 1.0), ("prio_10m", 1.0 / 6)):
            if hours_left <= cutoff and not db_get_setting(f"mcwv_war_cache_{phase}_{key}"):
                db_set_setting(f"mcwv_war_cache_{phase}_{key}", str(int(now)))
                clans = await get_priority_clan_names()
                admin_log("War Cache Priority Scan", f"{battle_id}: re-scanning {len(clans)} priority clans at T-{hours_left:.1f}h.")
                asyncio.create_task(_auto_cache_priority_scan(battle_id, clans))
                return
    except Exception as exc:
        print(f"[war-cache] window check failed: {exc}")


@war_cache_window_loop.before_loop
async def before_war_cache_window_loop():
    # This file is imported into main.py. `bot` is not in this module until
    # we copy it - without that, before_loop NameErrors, the loop never
    # stays up, and health-monitor spam-restarts War Cache every 5 min.
    _sync_from_main()
    b = globals().get("bot")
    if b is None:
        b = getattr(sys.modules.get("__main__"), "bot", None)
    if b is not None:
        await b.wait_until_ready()


@war_cache_window_loop.error
async def war_cache_window_loop_error(error):
    print(f"[war-cache] loop error: {type(error).__name__}: {error}")


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
            updated = _rank_one_battle_sql(conn, battle_id)
            total_updated += updated if updated > 0 else 0

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



async def _run_global_backfill(channel_id=None):
    _sync_from_main()
    """Scan every clan on the sitemap and cache every battle contribution.

    This is what /backfill_global is supposed to run. Two passes:
      1) fetch all clans, upsert raw PointContributions
      2) SQL window ranks per battle
    Always clears GLOBAL_BACKFILL_RUNNING.
    """
    scan_session = aiohttp.ClientSession()
    scan_conn = None
    started = time.time()
    notify = None
    try:
        if channel_id:
            try:
                notify = bot.get_channel(int(channel_id))
                if notify is None:
                    notify = await bot.fetch_channel(int(channel_id))
            except Exception:
                notify = None

        ops_log_soon(
            "cache",
            title="Global backfill started",
            description="Scanning every clan on the sitemap for all battles.",
            level="info",
            edit_key="global-backfill",
        )

        clan_names = await fetch_all_clan_names_from_sitemap(scan_session)
        if not clan_names:
            msg = "Global backfill aborted — sitemap returned no clans."
            print(f"[global backfill] {msg}")
            ops_log_soon("cache", title="Global backfill aborted", description=msg, level="warning", edit_key="global-backfill")
            if notify:
                try:
                    await notify.send(msg)
                except Exception:
                    pass
            return

        scan_conn = await _open_scan_connection()

        def _ensure(c):
            try:
                with c.cursor() as cur:
                    cur.execute("""CREATE TABLE IF NOT EXISTS cross_clan_player_history (
                        id BIGSERIAL PRIMARY KEY, roblox_id TEXT NOT NULL, battle_id TEXT NOT NULL,
                        battle_name TEXT, clan_name TEXT NOT NULL, points BIGINT NOT NULL DEFAULT 0,
                        rank INTEGER, total_contributors INTEGER, clan_place INTEGER,
                        earned_medal BOOLEAN DEFAULT FALSE, start_time BIGINT,
                        cached_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        UNIQUE (roblox_id, battle_id, clan_name))""")
                    cur.execute("CREATE INDEX IF NOT EXISTS cross_clan_history_roblox_idx ON cross_clan_player_history (roblox_id)")
                    cur.execute("CREATE INDEX IF NOT EXISTS cross_clan_history_battle_idx ON cross_clan_player_history (battle_id)")
                c.commit()
            except Exception:
                try:
                    c.rollback()
                except Exception:
                    pass

        await asyncio.to_thread(_ensure, scan_conn)

        CONCURRENCY = WAR_CACHE_SCAN_CONCURRENCY
        clans_with_data = 0
        total_contribs = 0
        pending_rows = []
        print(f"[global backfill] scanning {len(clan_names)} clans (concurrency={CONCURRENCY})")

        for batch_start in range(0, len(clan_names), CONCURRENCY):
            batch = clan_names[batch_start:batch_start + CONCURRENCY]
            results = await asyncio.gather(
                *(_fetch_clan_contributions(scan_session, name) for name in batch),
                return_exceptions=True,
            )
            for name, result in zip(batch, results):
                if isinstance(result, Exception) or not result or not isinstance(result, tuple):
                    continue
                rows = result[0] or []
                if rows:
                    clans_with_data += 1
                    total_contribs += len(rows)
                    pending_rows.extend(rows)
            if len(pending_rows) >= 5000:
                await asyncio.to_thread(_insert_raw_contributions, scan_conn, pending_rows)
                pending_rows.clear()
            await asyncio.sleep(0)
            if (batch_start // CONCURRENCY + 1) % 100 == 0:
                done = min(batch_start + CONCURRENCY, len(clan_names))
                elapsed = time.time() - started
                print(f"[global backfill] {done}/{len(clan_names)} clans, {total_contribs:,} contribs, {elapsed:.0f}s")
                ops_log_soon(
                    "cache",
                    title="Global backfill",
                    description="Scanning all clans.",
                    level="info",
                    fields=[
                        {"name": "Progress", "value": f"{done:,}/{len(clan_names):,}", "inline": True},
                        {"name": "Contribs", "value": f"{total_contribs:,}", "inline": True},
                        {"name": "Elapsed", "value": f"{elapsed:.0f}s", "inline": True},
                    ],
                    edit_key="global-backfill",
                )
                await asyncio.sleep(0)

        if pending_rows:
            await asyncio.to_thread(_insert_raw_contributions, scan_conn, pending_rows)
            pending_rows.clear()

        ranked = await asyncio.to_thread(_compute_global_ranks_sql, scan_conn)
        elapsed = time.time() - started
        mins = elapsed / 60
        summary = (
            f"Global backfill finished in **{mins:.1f}m**.\n"
            f"Clans with data: **{clans_with_data:,}** / {len(clan_names):,}\n"
            f"Contrib rows: **{total_contribs:,}**\n"
            f"Ranked: **{ranked:,}**"
        )
        print(f"[global backfill] done: {clans_with_data} clans, {total_contribs:,} contribs, {ranked:,} ranked, {elapsed:.0f}s")
        ops_log_soon(
            "cache",
            title="Global backfill finished",
            description=summary,
            level="success",
            edit_key="global-backfill",
        )
        if notify:
            try:
                await notify.send(summary)
            except Exception:
                pass
    except Exception as exc:
        print(f"[global backfill] FATAL: {exc}")
        traceback.print_exc()
        ops_log_soon(
            "cache",
            title="Global backfill crashed",
            description=f"`{type(exc).__name__}: {exc}`",
            level="error",
            edit_key="global-backfill",
        )
        if notify:
            try:
                await notify.send(f"Global backfill crashed: `{type(exc).__name__}: {exc}`")
            except Exception:
                pass
    finally:
        try:
            await asyncio.to_thread(_close_scan_connection_sync, scan_conn)
        except Exception:
            pass
        try:
            await scan_session.close()
        except Exception:
            pass


