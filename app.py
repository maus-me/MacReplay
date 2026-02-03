#!/usr/bin/env python3
import os
import shutil
import time
import subprocess
import re
import unicodedata
import io
import json
import gzip
import hashlib
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from datetime import datetime, timedelta, timezone
import xml.etree.ElementTree as ET
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from macreplay.config import (
    LOG_DIR,
    CONFIG_PATH,
    DB_PATH,
    EPG_CACHE_PATH,
    DATA_DIR,
    defaultPortal,
    loadConfig,
    getPortals,
    savePortals,
    getSettings,
)
from macreplay.db import get_db_connection, init_db
from macreplay.blueprints.settings import create_settings_blueprint
from macreplay.services.epg_cache import (
    save_epg_cache,
    load_epg_cache,
    is_epg_cache_valid,
)
from macreplay.blueprints.epg import create_epg_blueprint
from macreplay.blueprints.portal import create_portal_blueprint
from macreplay.blueprints.editor import create_editor_blueprint
from macreplay.blueprints.misc import create_misc_blueprint
from macreplay.blueprints.hdhr import create_hdhr_blueprint
from macreplay.blueprints.playlist import create_playlist_blueprint
from macreplay.blueprints.streaming import create_streaming_blueprint
from macreplay.services.jobs import JobManager
logger = logging.getLogger("MacReplay")
logger.setLevel(logging.INFO)
logFormat = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

# Group filter: include ungrouped channels only when no groups are active for a portal.
ACTIVE_GROUP_CONDITION = (
    "("
    "g.active = 1"
    " OR ("
    "  (c.genre_id IS NULL OR c.genre_id = '')"
    "  AND EXISTS ("
    "    SELECT 1 FROM groups g3"
    "    WHERE g3.portal_id = c.portal_id AND g3.genre_id = 'UNGROUPED' AND g3.active = 1"
    "  )"
    " )"
    " OR NOT EXISTS ("
    "  SELECT 1 FROM groups g2 WHERE g2.portal_id = c.portal_id AND g2.active = 1"
    " )"
    ")"
)

class FilterCache:
    def __init__(self, ttl_seconds=120):
        self.ttl_seconds = ttl_seconds
        self._data = {}
        self._lock = threading.Lock()

    def get(self, key):
        now = time.time()
        with self._lock:
            entry = self._data.get(key)
            if not entry:
                return None
            expires_at, value = entry
            if expires_at < now:
                self._data.pop(key, None)
                return None
            return value

    def set(self, key, value):
        expires_at = time.time() + self.ttl_seconds
        with self._lock:
            self._data[key] = (expires_at, value)

    def clear(self):
        with self._lock:
            self._data.clear()


filter_cache = FilterCache(ttl_seconds=120)

# EPG Refresh Interval: can be set via env (in hours), overrides settings if set
EPG_REFRESH_INTERVAL_ENV = os.getenv("EPG_REFRESH_INTERVAL", None)

# Channel Refresh Interval: can be set via env (in hours), overrides settings if set
# Set to 0 to disable automatic channel refresh
CHANNEL_REFRESH_INTERVAL_ENV = os.getenv("CHANNEL_REFRESH_INTERVAL", None)

log_file_path = os.path.join(LOG_DIR, "MacReplay.log")

# File logging
fileHandler = logging.FileHandler(log_file_path)
fileHandler.setFormatter(logFormat)
logger.addHandler(fileHandler)

# Console logging (docker logs)
consoleFormat = logging.Formatter("[%(levelname)s] %(message)s")
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(consoleFormat)
logger.addHandler(consoleHandler)

# Use system-installed ffmpeg and ffprobe (like STB-Proxy does)
ffmpeg_path = os.getenv("FFMPEG", "ffmpeg")
ffprobe_path = os.getenv("FFPROBE", "ffprobe")

# Check if the binaries exist
try:
    subprocess.run([ffmpeg_path, "-version"], capture_output=True, check=True)
    subprocess.run([ffprobe_path, "-version"], capture_output=True, check=True)
    logger.info("FFmpeg and FFprobe found and working")
except (subprocess.CalledProcessError, FileNotFoundError):
    logger.error("Error: ffmpeg or ffprobe not found!")


import stb
from flask import Flask
import secrets
import waitress
import sqlite3
import tempfile
import atexit

app = Flask(__name__)
app.secret_key = secrets.token_urlsafe(32)

# EPG refresh status tracking
epg_refresh_status = {
    "is_refreshing": False,
    "started_at": None,
    "completed_at": None,
    "last_error": None
}

# Bind settings (container internal)
BIND_HOST = os.getenv("BIND_HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8001"))

# Public hostname used inside generated URLs (m3u / hdhr / play links)
PUBLIC_HOST = os.getenv("PUBLIC_HOST")
if not PUBLIC_HOST:
    # Backward compatible fallback
    PUBLIC_HOST = os.getenv("HOST", f"{BIND_HOST}:{PORT}")

# IMPORTANT: the variable "host" is used all over the app to generate URLs
host = PUBLIC_HOST

logger.info(f"Public BaseURL: http://{host}")
logger.info(f"Using config file: {CONFIG_PATH}")
logger.info(f"Using database file: {DB_PATH}")


occupied = {}
cached_lineup = []
cached_playlist = None
last_playlist_host = None
cached_xmltv = None
last_updated = 0
epg_channel_ids = set()
epg_channel_ids_lock = threading.Lock()
epg_channel_map = {}
epg_channel_map_lock = threading.Lock()


def _set_cached_xmltv(value):
    global cached_xmltv
    cached_xmltv = value
    if value is None:
        _set_epg_channel_ids(set())
        _set_epg_channel_map({})
    else:
        parsed_ids = _parse_epg_channel_ids(value)
        epg_map, epg_ids = _rebuild_epg_channel_map_from_db()
        if epg_map:
            _set_epg_channel_ids(epg_ids if epg_ids else parsed_ids)
            _set_epg_channel_map(epg_map)
        else:
            _set_epg_channel_ids(parsed_ids)
            epg_map = _apply_epg_source_map(_parse_epg_channel_map(value), default_source="portal")
            _set_epg_channel_map(epg_map)


def _set_epg_channel_ids(ids):
    global epg_channel_ids
    with epg_channel_ids_lock:
        epg_channel_ids = set(ids or [])


def _get_epg_channel_ids():
    with epg_channel_ids_lock:
        return set(epg_channel_ids)


def _set_epg_channel_map(mapping):
    global epg_channel_map
    with epg_channel_map_lock:
        epg_channel_map = dict(mapping or {})


def _get_epg_channel_map():
    with epg_channel_map_lock:
        return dict(epg_channel_map)


def _parse_epg_channel_ids(xmltv):
    if not xmltv:
        return set()
    try:
        root = ET.fromstring(xmltv)
    except Exception:
        return set()
    ids = set()
    for channel in root.findall("channel"):
        cid = channel.get("id")
        if cid:
            ids.add(cid)
    for programme in root.findall("programme"):
        cid = programme.get("channel")
        if cid:
            ids.add(cid)
    return ids


def _parse_epg_channel_map(xmltv):
    if not xmltv:
        return {}
    try:
        root = ET.fromstring(xmltv)
    except Exception:
        return {}
    mapping = {}
    for channel in root.findall("channel"):
        cid = channel.get("id")
        if not cid:
            continue
        display_name = channel.findtext("display-name") or cid
        mapping[cid] = {"name": display_name}
    return mapping


def _normalize_epg_channel_map(mapping, default_source=None):
    normalized = {}
    for cid, value in (mapping or {}).items():
        if isinstance(value, dict):
            name = value.get("name") or value.get("display_name") or cid
            source = value.get("source")
        else:
            name = value or cid
            source = None
        if default_source and not source:
            source = default_source
        normalized[cid] = {"name": name, "source": source}
    return normalized


def _apply_epg_source_map(mapping, source_map=None, default_source=None):
    normalized = _normalize_epg_channel_map(mapping, default_source=default_source)
    if source_map:
        for cid, source in source_map.items():
            if cid in normalized:
                normalized[cid]["source"] = source
    return normalized


def _parse_custom_sources(settings):
    raw = settings.get("epg custom sources", "[]")
    if isinstance(raw, list):
        sources = raw
    else:
        try:
            sources = json.loads(raw) if raw else []
        except Exception:
            sources = []
    return [s for s in sources if isinstance(s, dict)]


def _get_epg_sources_dir():
    cache_dir = os.path.join(DATA_DIR, "epg_sources")
    os.makedirs(cache_dir, exist_ok=True)
    return cache_dir


def _get_epg_source_db_path(source_id):
    if not source_id:
        return None
    cache_dir = _get_epg_sources_dir()
    return os.path.join(cache_dir, f"{source_id}.sqlite")


def _open_epg_source_db(source_id):
    db_path = _get_epg_source_db_path(source_id)
    if not db_path:
        return None
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS epg_programmes (
            channel_id TEXT NOT NULL,
            start TEXT,
            stop TEXT,
            start_ts INTEGER,
            stop_ts INTEGER,
            title TEXT,
            description TEXT
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_epg_programmes_channel ON epg_programmes(channel_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_epg_programmes_start ON epg_programmes(start_ts)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_epg_programmes_stop ON epg_programmes(stop_ts)"
    )
    conn.commit()
    return conn


def _parse_xmltv_time_to_epoch(time_str):
    if not time_str:
        return None
    try:
        parts = time_str.split(" ")
        dt_str = parts[0]
        dt = datetime.strptime(dt_str, "%Y%m%d%H%M%S")
        if len(parts) > 1:
            tz_str = parts[1]
            tz_sign = 1 if tz_str[0] == "+" else -1
            tz_hours = int(tz_str[1:3])
            tz_mins = int(tz_str[3:5]) if len(tz_str) >= 5 else 0
            tz_offset = timedelta(hours=tz_sign * tz_hours, minutes=tz_sign * tz_mins)
            dt = dt.replace(tzinfo=timezone(tz_offset))
            dt = dt.astimezone(timezone.utc)
        else:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except (ValueError, AttributeError, IndexError):
        return None


def _ensure_epg_source_record(
    *, source_id, name, url, source_type, enabled=True, interval_hours=None, last_fetch=None, last_refresh=None
):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO epg_sources (source_id, name, url, source_type, enabled, interval_hours, last_fetch, last_refresh)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_id) DO UPDATE SET
                name = excluded.name,
                url = excluded.url,
                source_type = excluded.source_type,
                enabled = excluded.enabled,
                interval_hours = COALESCE(excluded.interval_hours, epg_sources.interval_hours),
                last_fetch = COALESCE(excluded.last_fetch, epg_sources.last_fetch),
                last_refresh = COALESCE(excluded.last_refresh, epg_sources.last_refresh)
            """,
            (
                source_id,
                name,
                url,
                source_type,
                1 if enabled else 0,
                interval_hours,
                last_fetch,
                last_refresh,
            ),
        )
        conn.commit()
    except Exception as e:
        logger.warning(f"Failed to upsert epg_sources: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _update_epg_channels_metadata(source_id, channels):
    if not source_id or not channels:
        return
    now_ts = time.time()
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM epg_channel_names WHERE source_id = ?", (source_id,))
        channel_rows = []
        name_rows = []
        for ch in channels:
            channel_rows.append(
                (
                    source_id,
                    ch["channel_id"],
                    ch.get("display_name"),
                    ch.get("icon"),
                    ch.get("lcn"),
                    now_ts,
                )
            )
            for name in ch.get("names") or []:
                name_rows.append((source_id, ch["channel_id"], name))
        cursor.executemany(
            """
            INSERT INTO epg_channels (source_id, channel_id, display_name, icon, lcn, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_id, channel_id) DO UPDATE SET
                display_name = excluded.display_name,
                icon = excluded.icon,
                lcn = excluded.lcn,
                updated_at = excluded.updated_at
            """,
            channel_rows,
        )
        if name_rows:
            cursor.executemany(
                """
                INSERT OR IGNORE INTO epg_channel_names (source_id, channel_id, name)
                VALUES (?, ?, ?)
                """,
                name_rows,
            )
        conn.commit()
    except Exception as e:
        logger.warning(f"Failed to update epg_channels metadata: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _rebuild_epg_channel_map_from_db():
    mapping = {}
    ids = set()
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT c.channel_id, c.display_name, s.name as source_name, c.source_id
            FROM epg_channels c
            LEFT JOIN epg_sources s ON s.source_id = c.source_id
            """
        )
        for row in cursor.fetchall():
            cid = row["channel_id"]
            ids.add(cid)
            source_label = row["source_name"] or row["source_id"] or "portal"
            mapping[cid] = {"name": row["display_name"] or cid, "source": source_label}
        conn.close()
    except Exception as e:
        logger.warning(f"Failed to rebuild epg channel map from DB: {e}")
    return mapping, ids


def _fetch_enabled_channels_for_epg(portal_id=None):
    rows = []
    conn = get_db_connection()
    cursor = conn.cursor()
    sql = f"""
        SELECT
            c.portal_id as portal_id,
            c.channel_id,
            c.name,
            c.number,
            c.logo,
            c.custom_name,
            c.auto_name,
            c.matched_name,
            c.custom_number,
            c.custom_epg_id
        FROM channels c
        LEFT JOIN groups g ON c.portal_id = g.portal_id AND c.genre_id = g.genre_id
        WHERE {ACTIVE_GROUP_CONDITION}
    """
    params = []
    if portal_id:
        sql += " AND c.portal_id = ?"
        params.append(portal_id)
    cursor.execute(sql, params)
    for row in cursor.fetchall():
        rows.append(dict(row))
    conn.close()
    return rows


def _resolve_epg_meta_for_ids(epg_ids):
    if not epg_ids:
        return {}
    meta = {}
    conn = get_db_connection()
    cursor = conn.cursor()
    ids = list(epg_ids)
    chunk_size = 900
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i:i + chunk_size]
        placeholders = ",".join(["?"] * len(chunk))
        cursor.execute(
            f"""
            SELECT source_id, channel_id, display_name, icon
            FROM epg_channels
            WHERE channel_id IN ({placeholders})
            """,
            chunk,
        )
        for row in cursor.fetchall():
            meta[row["channel_id"]] = {
                "source_id": row["source_id"],
                "display_name": row["display_name"],
                "icon": row["icon"],
            }
    conn.close()
    return meta


def _build_xmltv_from_db(enabled_channels, *, portals, past_cutoff_ts, future_cutoff_ts):
    epg_ids = set()
    for ch in enabled_channels:
        default_epg_id = effective_epg_name(ch["custom_name"], ch["auto_name"], ch["name"])
        if default_epg_id:
            epg_ids.add(default_epg_id)
        if ch.get("custom_epg_id"):
            epg_ids.add(ch["custom_epg_id"])

    epg_meta = _resolve_epg_meta_for_ids(epg_ids)
    root = ET.Element("tv")
    seen_channel_ids = set()
    channel_sources = {}

    for ch in enabled_channels:
        portal_id = ch["portal_id"]
        default_epg_id = effective_epg_name(ch["custom_name"], ch["auto_name"], ch["name"])
        custom_epg_id = ch.get("custom_epg_id") or ""

        if custom_epg_id:
            if custom_epg_id in epg_meta:
                epg_id = custom_epg_id
                source_id = epg_meta[custom_epg_id]["source_id"]
            else:
                epg_id = custom_epg_id
                source_id = None
        else:
            epg_id = default_epg_id
            source_id = epg_meta.get(epg_id, {}).get("source_id") if epg_id else None
            if not source_id:
                source_id = portal_id

        if not epg_id or epg_id in seen_channel_ids:
            continue
        seen_channel_ids.add(epg_id)

        display_name = epg_meta.get(epg_id, {}).get("display_name")
        if not display_name:
            display_name = effective_display_name(
                ch["custom_name"], ch["matched_name"], ch["auto_name"], ch["name"]
            )
        icon = epg_meta.get(epg_id, {}).get("icon") or ch.get("logo") or ""

        channel_ele = ET.SubElement(root, "channel", id=epg_id)
        ET.SubElement(channel_ele, "display-name").text = display_name or epg_id
        if icon:
            ET.SubElement(channel_ele, "icon", src=icon)

        if source_id:
            channel_sources.setdefault(source_id, set()).add(epg_id)

    for source_id, ids in channel_sources.items():
        conn = _open_epg_source_db(source_id)
        if conn is None:
            continue
        cursor = conn.cursor()
        id_list = list(ids)
        chunk_size = 900
        for i in range(0, len(id_list), chunk_size):
            chunk = id_list[i:i + chunk_size]
            placeholders = ",".join(["?"] * len(chunk))
            cursor.execute(
                f"""
                SELECT channel_id, start, stop, start_ts, stop_ts, title, description
                FROM epg_programmes
                WHERE channel_id IN ({placeholders})
                  AND stop_ts >= ?
                  AND start_ts <= ?
                ORDER BY start_ts ASC
                """,
                [*chunk, int(past_cutoff_ts), int(future_cutoff_ts)],
            )
            for row in cursor.fetchall():
                prog_ele = ET.SubElement(
                    root,
                    "programme",
                    channel=row["channel_id"],
                    start=row["start"] or "",
                    stop=row["stop"] or "",
                )
                ET.SubElement(prog_ele, "title").text = row["title"] or ""
                if row["description"]:
                    ET.SubElement(prog_ele, "desc").text = row["description"]
        conn.close()

    return ET.tostring(root, encoding="unicode"), seen_channel_ids


def _store_portal_epg_to_db(
    *,
    portal_id,
    portal_name,
    portal_url,
    enabled_channels,
    epg,
    portal_epg_offset,
    past_cutoff_ts,
    future_cutoff_ts,
):
    source_id = portal_id
    _ensure_epg_source_record(
        source_id=source_id,
        name=portal_name,
        url=portal_url,
        source_type="portal",
        enabled=True,
        interval_hours=get_epg_refresh_interval(),
        last_fetch=time.time(),
        last_refresh=time.time(),
    )

    channel_rows = []
    programme_rows = []
    offset_seconds = int(portal_epg_offset) * 3600

    if not epg:
        logger.warning("Portal EPG missing for %s; storing channel metadata only.", portal_name)

    for ch in enabled_channels:
        epg_id = effective_epg_name(
            ch["custom_name"], ch["auto_name"], ch["name"]
        )
        if not epg_id:
            continue
        display_name = effective_display_name(
            ch["custom_name"], ch["matched_name"], ch["auto_name"], ch["name"]
        )
        lcn = ch["custom_number"] if ch["custom_number"] else str(ch["number"])
        channel_rows.append(
            {
                "channel_id": epg_id,
                "display_name": display_name or epg_id,
                "icon": ch.get("logo") or "",
                "lcn": lcn,
                "names": [display_name] if display_name else [],
            }
        )

        if not epg:
            continue
        channel_id = str(ch["channel_id"])
        programmes = epg.get(channel_id) if isinstance(epg, dict) else None
        if not programmes:
            continue

        for p in programmes:
            start_ts = p.get("start_timestamp")
            stop_ts = p.get("stop_timestamp")
            if start_ts is None or stop_ts is None:
                continue
            try:
                start_ts = int(start_ts)
                stop_ts = int(stop_ts)
            except (TypeError, ValueError):
                continue
            if start_ts > 10**11 or stop_ts > 10**11:
                start_ts = int(start_ts / 1000)
                stop_ts = int(stop_ts / 1000)
            start_ts = start_ts + offset_seconds
            stop_ts = stop_ts + offset_seconds
            if stop_ts < past_cutoff_ts or start_ts > future_cutoff_ts:
                continue
            start_str = datetime.utcfromtimestamp(start_ts).strftime("%Y%m%d%H%M%S") + " +0000"
            stop_str = datetime.utcfromtimestamp(stop_ts).strftime("%Y%m%d%H%M%S") + " +0000"
            programme_rows.append(
                (
                    epg_id,
                    start_str,
                    stop_str,
                    start_ts,
                    stop_ts,
                    p.get("name") or display_name or "",
                    p.get("descr") or "",
                )
            )

    _update_epg_channels_metadata(source_id, channel_rows)

    conn = _open_epg_source_db(source_id)
    if conn is None:
        return
    cursor = conn.cursor()
    cursor.execute("DELETE FROM epg_programmes")
    if programme_rows:
        cursor.executemany(
            """
            INSERT INTO epg_programmes (channel_id, start, stop, start_ts, stop_ts, title, description)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            programme_rows,
        )
    conn.commit()
    conn.close()
    logger.info(
        "Stored portal EPG for %s: channels=%d programmes=%d",
        portal_name,
        len(channel_rows),
        len(programme_rows),
    )


def _resolve_custom_source_cache(source):
    url = (source.get("url") or "").strip()
    if not url:
        return None, None

    source_id = (source.get("id") or "").strip()
    if not source_id:
        source_id = hashlib.sha1(url.encode("utf-8")).hexdigest()

    cache_dir = _get_epg_sources_dir()
    cache_path = os.path.join(cache_dir, f"{source_id}.xml")
    meta_path = cache_path + ".meta"
    return cache_path, meta_path


def _read_custom_xmltv_from_cache(source):
    cache_path, _ = _resolve_custom_source_cache(source)
    if not cache_path or not os.path.exists(cache_path):
        return None
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return None


def _fetch_custom_xmltv(source, logger):
    url = (source.get("url") or "").strip()
    if not url:
        return None

    cache_path, meta_path = _resolve_custom_source_cache(source)
    if not cache_path or not meta_path:
        return None
    try:
        interval_hours = float(source.get("interval", source.get("interval_hours", 24)) or 24)
    except (TypeError, ValueError):
        interval_hours = 24

    use_cache = False
    if os.path.exists(cache_path) and interval_hours > 0:
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                last_fetch = float(f.read().strip())
            age_hours = (time.time() - last_fetch) / 3600
            if age_hours < interval_hours:
                use_cache = True
        except Exception:
            use_cache = False

    if use_cache:
        try:
            return _read_custom_xmltv_from_cache(source)
        except Exception:
            return None

    try:
        req = Request(url, headers={"User-Agent": "MacReplay"})
        with urlopen(req, timeout=20) as resp:
            data = resp.read()
        if url.endswith(".gz") or data[:2] == b"\x1f\x8b":
            data = gzip.decompress(data)
        xml_text = data.decode("utf-8", errors="replace")
        with open(cache_path, "w", encoding="utf-8") as f:
            f.write(xml_text)
        with open(meta_path, "w", encoding="utf-8") as f:
            f.write(str(time.time()))
        return xml_text
    except (HTTPError, URLError, OSError, ValueError) as e:
        logger.warning(f"Failed to fetch custom EPG source {url}: {e}")
        if os.path.exists(cache_path):
            return _read_custom_xmltv_from_cache(source)
        return None


def refresh_custom_sources(source_ids=None):
    settings = getSettings()
    sources = _parse_custom_sources(settings)
    if source_ids:
        source_ids = {s for s in source_ids if s}
        sources = [s for s in sources if s.get("id") in source_ids]
    sources = [s for s in sources if s.get("enabled") not in [False, "false"]]

    if not sources:
        return False

    epg_future_hours = int(settings.get("epg future hours", "24"))
    epg_past_hours = int(settings.get("epg past hours", "2"))
    now_ts = int(time.time())
    past_cutoff = now_ts - (epg_past_hours * 3600)
    future_cutoff = now_ts + (epg_future_hours * 3600)

    for source in sources:
        url = (source.get("url") or "").strip()
        if not url:
            continue
        source_id = (source.get("id") or "").strip()
        if not source_id:
            source_id = hashlib.sha1(url.encode("utf-8")).hexdigest()
        source_label = source.get("name") or url or source_id or "custom"
        interval_hours = source.get("interval", source.get("interval_hours", 24))

        xml_text = _fetch_custom_xmltv(source, logger)
        cache_path, meta_path = _resolve_custom_source_cache(source)
        if not xml_text or not cache_path:
            continue

        last_fetch = None
        if meta_path and os.path.exists(meta_path):
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    last_fetch = float(f.read().strip())
            except Exception:
                last_fetch = None

        _ensure_epg_source_record(
            source_id=source_id,
            name=source_label,
            url=url,
            source_type="custom",
            enabled=True,
            interval_hours=interval_hours,
            last_fetch=last_fetch,
            last_refresh=time.time(),
        )

        channels = []
        programme_rows = []
        source_db = _open_epg_source_db(source_id)
        if not source_db:
            continue
        try:
            cursor = source_db.cursor()
            cursor.execute("DELETE FROM epg_programmes")
            source_db.commit()

            for _, elem in ET.iterparse(cache_path, events=("end",)):
                if elem.tag == "channel":
                    cid = (elem.get("id") or "").strip()
                    if not cid:
                        elem.clear()
                        continue
                    display_names = [dn.text.strip() for dn in elem.findall("display-name") if dn.text]
                    display_name = display_names[0] if display_names else cid
                    icon = None
                    icon_elem = elem.find("icon")
                    if icon_elem is not None and icon_elem.get("src"):
                        icon = icon_elem.get("src")
                    lcn = elem.findtext("lcn")
                    channels.append(
                        {
                            "channel_id": cid,
                            "display_name": display_name,
                            "icon": icon,
                            "lcn": lcn,
                            "names": display_names,
                        }
                    )
                    elem.clear()
                    continue

                if elem.tag == "programme":
                    channel_attr = (elem.get("channel") or "").strip()
                    if not channel_attr:
                        elem.clear()
                        continue
                    start_attr = elem.get("start")
                    stop_attr = elem.get("stop")
                    start_ts = _parse_xmltv_time_to_epoch(start_attr)
                    stop_ts = _parse_xmltv_time_to_epoch(stop_attr)
                    if stop_ts is not None and stop_ts < past_cutoff:
                        elem.clear()
                        continue
                    if start_ts is not None and start_ts > future_cutoff:
                        elem.clear()
                        continue
                    title = elem.findtext("title") or ""
                    desc = elem.findtext("desc") or ""
                    programme_rows.append(
                        (channel_attr, start_attr, stop_attr, start_ts, stop_ts, title, desc)
                    )
                    if len(programme_rows) >= 500:
                        cursor.executemany(
                            """
                            INSERT INTO epg_programmes
                            (channel_id, start, stop, start_ts, stop_ts, title, description)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            programme_rows,
                        )
                        source_db.commit()
                        programme_rows = []
                    elem.clear()

            if programme_rows:
                cursor.executemany(
                    """
                    INSERT INTO epg_programmes
                    (channel_id, start, stop, start_ts, stop_ts, title, description)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    programme_rows,
                )
                source_db.commit()
        except Exception as e:
            logger.warning(f"Failed to parse/store custom EPG source {source_label}: {e}")
        finally:
            try:
                source_db.close()
            except Exception:
                pass

        _update_epg_channels_metadata(source_id, channels)

    epg_map, epg_ids = _rebuild_epg_channel_map_from_db()
    if epg_map:
        _set_epg_channel_map(epg_map)
        _set_epg_channel_ids(epg_ids)
    return True


def _format_start_tag(tag, attrib):
    if not attrib:
        return f"<{tag}>"
    attrs = " ".join(f'{key}="{value}"' for key, value in attrib.items())
    return f"<{tag} {attrs}>"

def _ensure_channel_display_name(elem, cid, fallback_name=None):
    display_name = elem.find("display-name")
    current_text = display_name.text.strip() if display_name is not None and display_name.text else ""
    if current_text:
        return current_text
    name_value = fallback_name or cid or ""
    if display_name is None:
        display_name = ET.SubElement(elem, "display-name")
    display_name.text = name_value
    return name_value

def _get_channel_metadata_map(epg_ids):
    metadata = {}
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT portal_id, name, custom_name, matched_name, auto_name, custom_epg_id, logo
            FROM channels
            """
        )
        for row in cursor.fetchall():
            epg_id = row["custom_epg_id"] if row["custom_epg_id"] else effective_epg_name(
                row["custom_name"], row["auto_name"], row["name"]
            )
            if not epg_id or epg_id not in epg_ids:
                continue
            display_name = effective_display_name(
                row["custom_name"], row["matched_name"], row["auto_name"], row["name"]
            )
            metadata[epg_id] = {
                "name": display_name or epg_id,
                "logo": row["logo"] or "",
            }
    except Exception as e:
        logger.warning(f"Failed to build channel metadata map: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return metadata


def _iterparse_source(xml_text=None, file_path=None):
    if file_path:
        return ET.iterparse(file_path, events=("end",))
    return ET.iterparse(io.StringIO(xml_text or ""), events=("end",))


def _write_partial_epg_cache(
    *,
    epg_ids,
    replace_ids,
    custom_sources,
    past_cutoff,
    cache_only,
):
    cache_path = EPG_CACHE_PATH
    tmp_path = cache_path + ".tmp"
    existing_channels = set()
    source_map = {}
    channel_name_map = {}
    channel_meta_map = _get_channel_metadata_map(epg_ids)
    added_programmes = 0
    root_tag = "tv"
    root_attrib = {}
    wrote_root = False

    with open(tmp_path, "w", encoding="utf-8") as out:
        out.write('<?xml version="1.0" encoding="UTF-8"?>\n')

        if os.path.exists(cache_path):
            tag_stack = []
            for event, elem in ET.iterparse(cache_path, events=("start", "end")):
                if event == "start":
                    tag_stack.append(elem.tag)
                    if not wrote_root:
                        root_tag = elem.tag
                        root_attrib = dict(elem.attrib)
                        out.write(_format_start_tag(root_tag, root_attrib))
                        wrote_root = True
                    continue

                if event != "end":
                    continue

                parent_tag = tag_stack[-2] if len(tag_stack) > 1 else None

                if elem.tag == root_tag:
                    tag_stack.pop()
                    continue

                if elem.tag == "channel":
                    cid = elem.get("id")
                    if cid:
                        if cid in replace_ids:
                            elem.clear()
                            tag_stack.pop()
                            continue
                        existing_channels.add(cid)
                        meta = channel_meta_map.get(cid)
                        _ensure_channel_display_name(elem, cid, fallback_name=meta["name"] if meta else None)
                        if meta and meta.get("logo"):
                            icon = elem.find("icon")
                            if icon is None:
                                icon = ET.SubElement(elem, "icon")
                            if not icon.get("src"):
                                icon.set("src", meta["logo"])
                    out.write(ET.tostring(elem, encoding="unicode"))
                elif elem.tag == "programme":
                    channel_attr = elem.get("channel")
                    if channel_attr in replace_ids:
                        elem.clear()
                        tag_stack.pop()
                        continue
                    out.write(ET.tostring(elem, encoding="unicode"))
                elif parent_tag == root_tag:
                    out.write(ET.tostring(elem, encoding="unicode"))

                elem.clear()
                tag_stack.pop()

        if not wrote_root:
            out.write(_format_start_tag(root_tag, root_attrib))

        for source in custom_sources:
            if source.get("enabled") in [False, "false"]:
                continue

            source_label = source.get("name") or source.get("url") or source.get("id") or "custom"
            source_path = None
            xml_text = None

            if cache_only:
                cache_path_source, _ = _resolve_custom_source_cache(source)
                if cache_path_source and os.path.exists(cache_path_source):
                    source_path = cache_path_source
                else:
                    logger.info(
                        "Skipping custom source %s (not cached yet)",
                        source_label,
                    )
                    continue
            else:
                xml_text = _fetch_custom_xmltv(source, logger)
                if not xml_text:
                    continue

            try:
                for _, elem in _iterparse_source(xml_text=xml_text, file_path=source_path):
                    if elem.tag == "channel":
                        cid = elem.get("id")
                        if not cid:
                            elem.clear()
                            continue
                        if cid in replace_ids:
                            source_map[cid] = source_label
                            if cid not in existing_channels:
                                existing_channels.add(cid)
                                display_name = _ensure_channel_display_name(
                                    elem,
                                    cid,
                                    fallback_name=elem.findtext("display-name") or cid,
                                )
                                channel_name_map[cid] = display_name
                                icon = elem.find("icon")
                                if icon is not None and not icon.get("src"):
                                    elem.remove(icon)
                                out.write(ET.tostring(elem, encoding="unicode"))
                        elem.clear()
                        continue

                    if elem.tag == "programme":
                        channel_attr = elem.get("channel")
                        if channel_attr not in epg_ids:
                            elem.clear()
                            continue
                        stop_attr = elem.get("stop")
                        if stop_attr:
                            try:
                                stop_time = datetime.strptime(stop_attr.split(" ")[0], "%Y%m%d%H%M%S")
                                if stop_time < past_cutoff:
                                    elem.clear()
                                    continue
                            except ValueError:
                                pass
                        out.write(ET.tostring(elem, encoding="unicode"))
                        added_programmes += 1
                        elem.clear()
            except Exception as e:
                logger.warning(f"Invalid XMLTV content from custom source: {e}")
                continue

        for cid in epg_ids:
            if cid in existing_channels or cid not in replace_ids:
                continue
            meta = channel_meta_map.get(cid, {})
            channel_ele = ET.Element("channel", id=cid)
            display_name = meta.get("name") or cid
            ET.SubElement(channel_ele, "display-name").text = display_name
            logo = meta.get("logo") or ""
            if logo:
                ET.SubElement(channel_ele, "icon", src=logo)
            out.write(ET.tostring(channel_ele, encoding="unicode"))
            existing_channels.add(cid)
            channel_name_map[cid] = display_name

        out.write(f"</{root_tag}>")

    os.replace(tmp_path, cache_path)
    return added_programmes, source_map, channel_name_map


def _build_epg_channel_map_from_cache(cache_path):
    mapping = {}
    ids = set()
    if not os.path.exists(cache_path):
        return mapping, ids
    try:
        for _, elem in ET.iterparse(cache_path, events=("end",)):
            if elem.tag != "channel":
                elem.clear()
                continue
            cid = elem.get("id")
            if cid:
                ids.add(cid)
                display_name = elem.findtext("display-name") or cid
                mapping[cid] = {"name": display_name, "source": "portal"}
            elem.clear()
    except Exception as e:
        logger.error(f"Error building channel map from cache: {e}")
    return mapping, ids


def refresh_xmltv_for_epg_ids(epg_ids, *, cache_only=False):
    if not epg_ids:
        return False, "No EPG IDs provided"

    if epg_refresh_status.get("is_refreshing"):
        return False, "EPG refresh already running"

    epg_refresh_status["is_refreshing"] = True
    epg_refresh_status["started_at"] = datetime.utcnow().isoformat()
    epg_refresh_status["last_error"] = None

    try:
        epg_ids = {str(cid).strip() for cid in epg_ids if str(cid).strip()}
        if not epg_ids:
            return False, "No valid EPG IDs provided"

        logger.info(
            "EPG cache rebuild started for %d IDs (db-only)",
            len(epg_ids),
        )

        settings = getSettings()
        past_hours = int(settings.get("epg past hours", "2"))
        future_hours = int(settings.get("epg future hours", "24"))
        past_cutoff_ts = int((datetime.utcnow() - timedelta(hours=past_hours)).timestamp())
        future_cutoff_ts = int((datetime.utcnow() + timedelta(hours=future_hours)).timestamp())

        enabled_rows = _fetch_enabled_channels_for_epg()
        xmltv_text, channel_ids = _build_xmltv_from_db(
            enabled_rows,
            portals=getPortals(),
            past_cutoff_ts=past_cutoff_ts,
            future_cutoff_ts=future_cutoff_ts,
        )

        global cached_xmltv, last_updated
        cached_xmltv = xmltv_text
        last_updated = time.time()
        save_epg_cache(cached_xmltv, last_updated, logger, EPG_CACHE_PATH)

        epg_map, epg_ids = _rebuild_epg_channel_map_from_db()
        _set_epg_channel_ids(epg_ids if epg_ids else set(channel_ids))
        if epg_map:
            _set_epg_channel_map(epg_map)

        logger.info("EPG cache rebuild completed (db-only)")
        return True, "EPG cache rebuilt from DB"
    except Exception as e:
        epg_refresh_status["last_error"] = str(e)
        logger.error(f"Error refreshing EPG for IDs: {e}")
        return False, str(e)
    finally:
        epg_refresh_status["is_refreshing"] = False
        epg_refresh_status["completed_at"] = datetime.utcnow().isoformat()


def _set_last_playlist_host(value):
    global last_playlist_host
    last_playlist_host = value


def _get_cached_lineup():
    return cached_lineup


def _get_cached_playlist():
    return cached_playlist


def _set_cached_playlist(value):
    global cached_playlist
    cached_playlist = value


def _get_last_playlist_host():
    return last_playlist_host


def get_epg_refresh_interval():
    """Get EPG refresh interval in hours. ENV variable takes precedence over settings."""
    if EPG_REFRESH_INTERVAL_ENV is not None:
        try:
            return float(EPG_REFRESH_INTERVAL_ENV)
        except ValueError:
            logger.warning(f"Invalid EPG_REFRESH_INTERVAL env value: {EPG_REFRESH_INTERVAL_ENV}, using settings")
    return float(getSettings().get("epg refresh interval", "0.5"))


def get_channel_refresh_interval():
    """Get channel refresh interval in hours. ENV variable takes precedence over settings.
    Returns 0 to disable automatic refresh."""
    if CHANNEL_REFRESH_INTERVAL_ENV is not None:
        try:
            return float(CHANNEL_REFRESH_INTERVAL_ENV)
        except ValueError:
            logger.warning(f"Invalid CHANNEL_REFRESH_INTERVAL env value: {CHANNEL_REFRESH_INTERVAL_ENV}, using settings")
    return float(getSettings().get("channel refresh interval", "24"))


d_ffmpegcmd = [
    "-re",                      # Flag for real-time streaming
    "-http_proxy", "<proxy>",   # Proxy setting
    "-timeout", "<timeout>",    # Timeout setting
    "-i", "<url>",              # Input URL
    "-map", "0",                # Map all streams
    "-codec", "copy",           # Copy codec (no re-encoding)
    "-f", "mpegts",             # Output format
    "-flush_packets", "0",      # Disable flushing packets (optimized for faster output)
    "-fflags", "+nobuffer",     # No buffering for low latency
    "-flags", "low_delay",      # Low delay flag
    "-strict", "experimental",  # Use experimental features
    "-analyzeduration", "0",    # Skip analysis duration for faster startup
    "-probesize", "32",         # Set probe size to reduce input analysis time
    "-copyts",                  # Copy timestamps (avoid recalculating)
    "-threads", "12",           # Enable multi-threading (adjust thread count as needed)
    "pipe:"                     # Output to pipe
]







def normalize_mac_data(mac_value):
    """Konvertiert altes MAC-Format (String) zu neuem Format (Dict).

    Altes Format: "April 23, 2026, 12:00 am" (nur Ablaufdatum als String)
    Neues Format: {"expiry": "...", "watchdog_timeout": 0, "playback_limit": 0}
    """
    if isinstance(mac_value, str):
        # Altes Format: nur Ablaufdatum als String
        return {
            "expiry": mac_value,
            "watchdog_timeout": 0,
            "playback_limit": 0
        }
    elif isinstance(mac_value, dict):
        # Neues Format: sicherstellen dass alle Felder existieren
        return {
            "expiry": mac_value.get("expiry", "Unknown"),
            "watchdog_timeout": mac_value.get("watchdog_timeout", 0),
            "playback_limit": mac_value.get("playback_limit", 0)
        }
    return {"expiry": "Unknown", "watchdog_timeout": 0, "playback_limit": 0}


DEFAULT_COUNTRY_CODES = {
    "AF", "AL", "ALB", "AR", "AT", "AU", "BE", "BG", "BR", "CA", "CH", "CN", "CZ",
    "DE", "DK", "EE", "ES", "FI", "FR", "GR", "HK", "HR", "HU", "IE", "IL",
    "IN", "IR", "IS", "IT", "JO", "JP", "KR", "KW", "LAT", "LB", "LT", "LU",
    "LV", "MA", "MK", "MO", "MX", "MXC", "NL", "NO", "NZ", "PL", "PT", "RO",
    "RS", "RU", "SA", "SE", "SG", "SI", "SK", "TR", "UA", "UK", "US", "USA",
}

DEFAULT_RESOLUTION_PATTERNS = [
    ("8K", r"\b(8K|4320P)\b"),
    ("UHD", r"\b(UHD|ULTRA|4K\+?|2160P)\b"),
    ("FHD", r"\b(FHD|1080P)\b"),
    ("HD", r"\b(HD|720P)\b"),
    ("SD", r"\b(SD|576P|480P)\b"),
]

DEFAULT_VIDEO_CODEC_PATTERNS = [
    ("AV1", r"\bAV1\b"),
    ("VP9", r"\bVP9\b"),
    ("HEVC", r"\b(HEVC|H\.?265|H265)\b"),
    ("H264", r"\b(H\.?264|H264|AVC)\b"),
    ("MPEG2", r"\bMPEG[- ]?2\b"),
]

DEFAULT_AUDIO_TAG_PATTERNS = [
    ("AAC", r"\bAAC\b"),
    ("AC3", r"\bAC3\b"),
    ("EAC3", r"\bEAC3\b"),
    ("DDP", r"\b(DD\+|DDP)\b"),
    ("DD", r"\bDD\b"),
    ("DTS", r"\bDTS\b"),
    ("MP3", r"\bMP3\b"),
    ("FLAC", r"\bFLAC\b"),
    ("DOLBY", r"\bDOLBY\b"),
    ("ATMOS", r"\bATMOS\b"),
    ("7.1", r"\b7\.1\b"),
    ("5.1", r"\b5\.1\b"),
    ("2.0", r"\b2\.0\b"),
]

DEFAULT_EVENT_PATTERNS = [
    ("PPV", r"\bPPV\b"),
    ("EVENT", r"\bEVENT\b"),
    ("LIVE EVENT", r"\bLIVE EVENT\b"),
    ("LIVE-EVENT", r"\bLIVE-EVENT\b"),
    ("NO EVENT", r"\bNO EVENT\b"),
    ("NO EVENT STREAMING", r"\bNO EVENT STREAMING\b"),
    ("MATCH TIME", r"\bMATCH TIME\b"),
]

DEFAULT_MISC_PATTERNS = [
    r"\bSAT\b(?!\s+\d{1,2}\b)",
    r"\bBAR\b",
]

channelsdvr_cache = {}
channelsdvr_cache_lock = threading.Lock()
channelsdvr_match_status = {}
channelsdvr_match_status_lock = threading.Lock()
channels_refresh_status = {}
channels_refresh_status_lock = threading.Lock()
channels_refresh_lock = threading.Lock()
DEFAULT_HEADER_PATTERNS = [
    r"^\s*([#*✦┃★]{2,})\s*(.+?)\s*\1\s*$",
]


def parse_labeled_patterns(value, defaults):
    if not value:
        return defaults
    patterns = []
    for line in str(value).splitlines():
        line = line.strip()
        if not line or "=" not in line:
            continue
        label, pattern = line.split("=", 1)
        label = label.strip()
        pattern = pattern.strip()
        if label and pattern:
            patterns.append((label, pattern))
    return patterns if patterns else defaults


def parse_list_patterns(value, defaults):
    if not value:
        return defaults
    patterns = []
    for line in str(value).splitlines():
        line = line.strip()
        if line:
            patterns.append(line)
    return patterns if patterns else defaults


def ensure_resolution_patterns(patterns):
    updated = []
    for label, pattern in patterns:
        if label == "UHD" and "ULTRA" not in pattern:
            if "UHD|" in pattern:
                pattern = pattern.replace("UHD|", "UHD|ULTRA|")
            elif "(UHD" in pattern:
                pattern = pattern.replace("(UHD", "(UHD|ULTRA")
            else:
                pattern = f"(?:{pattern}|ULTRA)"
        updated.append((label, pattern))
    return updated


def normalize_misc_patterns(patterns):
    updated = []
    for pattern in patterns:
        if pattern.strip() in (r"\bSAT\b", "SAT"):
            updated.append(r"(?<!\b\d\s)\bSAT(?![.\s]*\d)\b")
        else:
            updated.append(pattern)
    return updated


def normalize_event_label(pattern):
    label = re.sub(r"\\b", "", pattern)
    label = re.sub(r"[\\^$()?:]", "", label)
    label = re.sub(r"\s+", " ", label).strip()
    return label


def normalize_match_name(value):
    folded = ascii_fold(value).upper()
    folded = re.sub(r"[^A-Z0-9]+", " ", folded)
    tokens = re.sub(r"\s+", " ", folded).strip().split()
    number_map = {
        "EIN": "1",
        "EINS": "1",
        "ZWEI": "2",
        "DREI": "3",
        "VIER": "4",
        "FUNF": "5",
        "FUENF": "5",
        "SECHS": "6",
        "SIEBEN": "7",
        "ACHT": "8",
        "NEUN": "9",
        "ZEHN": "10",
    }
    normalized = [number_map.get(token, token) for token in tokens]
    return " ".join(normalized).strip()


def build_channelsdvr_index(normalized):
    exact = {}
    token_index = {}
    for idx, record in enumerate(normalized):
        norm = record.get("norm")
        if not norm:
            continue
        exact.setdefault(norm, record)
        for token in set(norm.split()):
            token_index.setdefault(token, set()).add(idx)
    return exact, token_index


def load_channelsdvr_cache(country, db_path, cache_dir):
    if not cache_dir:
        return None
    try:
        os.makedirs(cache_dir, exist_ok=True)
    except OSError:
        return None

    cache_path = os.path.join(cache_dir, f"{country}.json.gz")
    if not os.path.exists(cache_path):
        return None

    try:
        db_mtime = os.path.getmtime(db_path)
        with gzip.open(cache_path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
        if payload.get("db_mtime") != db_mtime:
            return None
        normalized = payload.get("normalized") or []
        converted = []
        for row in normalized:
            if len(row) >= 5:
                converted.append({
                    "norm": row[0],
                    "name": row[1],
                    "station_id": row[2],
                    "call_sign": row[3],
                    "logo_uri": row[4],
                })
            elif len(row) >= 2:
                # Backward compatibility with older cache format (norm, name)
                converted.append({
                    "norm": row[0],
                    "name": row[1],
                    "station_id": "",
                    "call_sign": "",
                    "logo_uri": "",
                })
        normalized = converted
        exact, token_index = build_channelsdvr_index(normalized)
        return {
            "exact": exact,
            "normalized": normalized,
            "token_index": token_index,
        }
    except Exception:
        return None


def save_channelsdvr_cache(country, db_path, cache_dir, normalized):
    if not cache_dir:
        return
    try:
        os.makedirs(cache_dir, exist_ok=True)
        payload = {
            "db_mtime": os.path.getmtime(db_path),
            "country": country,
            "normalized": [
                [
                    row.get("norm"),
                    row.get("name"),
                    row.get("station_id"),
                    row.get("call_sign"),
                    row.get("logo_uri"),
                ]
                for row in normalized
            ],
        }
        cache_path = os.path.join(cache_dir, f"{country}.json.gz")
        with gzip.open(cache_path, "wt", encoding="utf-8") as handle:
            json.dump(payload, handle)
    except Exception:
        pass


COUNTRY_ISO2_TO_ISO3 = {
    "DE": "DEU",
    "AT": "AUT",
    "CH": "CHE",
    "ES": "ESP",
    "FR": "FRA",
    "IT": "ITA",
    "NL": "NLD",
    "BE": "BEL",
    "DK": "DNK",
    "FI": "FIN",
    "NO": "NOR",
    "SE": "SWE",
    "PT": "PRT",
    "PL": "POL",
    "CZ": "CZE",
    "SK": "SVK",
    "HU": "HUN",
    "RO": "ROU",
    "BG": "BGR",
    "GR": "GRC",
    "UK": "GBR",
    "IE": "IRL",
    "US": "USA",
    "CA": "CAN",
    "BR": "BRA",
    "AR": "ARG",
    "MX": "MEX",
    "TR": "TUR",
    "RU": "RUS",
    "UA": "UKR",
    "AU": "AUS",
    "NZ": "NZL",
}


def normalize_market_country(country):
    if not country:
        return ""
    country = country.upper()
    return COUNTRY_ISO2_TO_ISO3.get(country, country)


def load_channelsdvr_records_for_country(country, db_path, include_lineup_channels=False):
    country = normalize_market_country(country)
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.execute("PRAGMA busy_timeout = 5000;")
    cursor = conn.cursor()
    if include_lineup_channels:
        query = """
            SELECT DISTINCT s.station_id, s.name, s.call_sign, s.logo_uri
            FROM stations s
            JOIN station_lineups sl ON sl.station_id = s.station_id
            JOIN lineup_markets lm ON lm.lineup_id = sl.lineup_id
            WHERE lm.country = ?
            UNION
            SELECT DISTINCT lc.station_id, lc.station_name, lc.call_sign, s.logo_uri
            FROM lineup_channels lc
            JOIN lineup_markets lm ON lm.lineup_id = lc.lineup_id
            LEFT JOIN stations s ON s.station_id = lc.station_id
            WHERE lm.country = ?
        """
        cursor.execute(query, (country, country))
    else:
        query = """
            SELECT DISTINCT s.station_id, s.name, s.call_sign, s.logo_uri
            FROM stations s
            JOIN station_lineups sl ON sl.station_id = s.station_id
            JOIN lineup_markets lm ON lm.lineup_id = sl.lineup_id
            WHERE lm.country = ?
        """
        cursor.execute(query, (country,))
    records = [
        {
            "station_id": row[0],
            "name": row[1],
            "call_sign": row[2],
            "logo_uri": row[3],
        }
        for row in cursor.fetchall()
        if row[1]
    ]
    conn.close()
    return records


def get_channelsdvr_cache_for_country(country, db_path, include_lineup_channels=False):
    cache_key = (country, db_path, include_lineup_channels)
    with channelsdvr_cache_lock:
        if cache_key in channelsdvr_cache:
            return channelsdvr_cache[cache_key]

    settings = getSettings()
    cache_enabled = settings.get("channelsdvr cache enabled", "true") == "true"
    cache_dir = settings.get("channelsdvr cache dir", "").strip()
    country_iso3 = normalize_market_country(country)
    if cache_enabled and cache_dir:
        cached = load_channelsdvr_cache(country_iso3, db_path, cache_dir)
        if cached:
            with channelsdvr_cache_lock:
                channelsdvr_cache[cache_key] = cached
            return cached

    records = load_channelsdvr_records_for_country(country, db_path, include_lineup_channels)
    normalized = []

    for record in records:
        norm = normalize_match_name(record.get("name"))
        if not norm:
            continue
        normalized.append({
            "norm": norm,
            "name": record.get("name"),
            "station_id": record.get("station_id"),
            "call_sign": record.get("call_sign"),
            "logo_uri": record.get("logo_uri"),
        })

    exact, token_index = build_channelsdvr_index(normalized)
    cache_entry = {
        "exact": exact,
        "normalized": normalized,
        "token_index": token_index,
    }

    if cache_enabled and cache_dir:
        save_channelsdvr_cache(country_iso3, db_path, cache_dir, normalized)

    with channelsdvr_cache_lock:
        channelsdvr_cache[cache_key] = cache_entry

    return cache_entry


def match_channelsdvr_name(raw_name, country, settings):
    if not raw_name or not country:
        return {}
    if settings.get("channelsdvr enabled", "false") != "true":
        return {}
    db_path = settings.get("channelsdvr db path", "").strip()
    if not db_path or not os.path.exists(db_path):
        return {}
    try:
        threshold = float(settings.get("channelsdvr match threshold", "0.72"))
    except ValueError:
        threshold = 0.72
    include_lineup_channels = settings.get("channelsdvr include lineup channels", "false") == "true"

    norm = normalize_match_name(raw_name)
    if not norm:
        return {}

    country_iso3 = normalize_market_country(country)
    norm_tokens = [t for t in norm.split() if t not in {country.upper(), country_iso3}]
    norm = " ".join(norm_tokens).strip()
    if not norm:
        return {}

    cache_entry = get_channelsdvr_cache_for_country(country, db_path, include_lineup_channels)
    exact = cache_entry["exact"]
    if norm in exact:
        if settings.get("channelsdvr debug", "false") == "true":
            logger.info(f"ChannelsDVR match (exact): {raw_name} -> {exact[norm].get('name')}")
        record = dict(exact[norm])
        record["score"] = 1.0
        return record

    tokens = norm.split()
    if len(tokens) < 2:
        return {}

    candidate_indices = set()
    for token in tokens:
        candidate_indices.update(cache_entry["token_index"].get(token, set()))

    best_score = 0.0
    best_record = {}
    token_set = set(tokens)
    for idx in candidate_indices:
        record = cache_entry["normalized"][idx]
        cand_norm = record.get("norm", "")
        cand_tokens = set(cand_norm.split())
        if not cand_tokens:
            continue
        score = len(token_set & cand_tokens) / max(len(token_set), len(cand_tokens))
        if score > best_score:
            best_score = score
            best_record = record

    if best_score >= threshold:
        if settings.get("channelsdvr debug", "false") == "true":
            logger.info(f"ChannelsDVR match ({best_score:.2f}): {raw_name} -> {best_record.get('name')}")
        record = dict(best_record)
        record["score"] = best_score
        return record
    if settings.get("channelsdvr debug", "false") == "true":
        logger.info(
            f"ChannelsDVR no match ({best_score:.2f}): {raw_name} (country {country} -> {country_iso3})"
        )
    return {}


def suggest_channelsdvr_matches(raw_name, country, settings, limit=8):
    if not raw_name or not country:
        return []
    if settings.get("channelsdvr enabled", "false") != "true":
        return []
    db_path = settings.get("channelsdvr db path", "").strip()
    if not db_path or not os.path.exists(db_path):
        return []
    try:
        threshold = float(settings.get("channelsdvr match threshold", "0.72"))
    except ValueError:
        threshold = 0.72
    include_lineup_channels = settings.get("channelsdvr include lineup channels", "false") == "true"

    norm = normalize_match_name(raw_name)
    if not norm:
        return []

    country_iso3 = normalize_market_country(country)
    norm_tokens = [t for t in norm.split() if t not in {country.upper(), country_iso3}]
    norm = " ".join(norm_tokens).strip()
    if not norm:
        return []

    cache_entry = get_channelsdvr_cache_for_country(country, db_path, include_lineup_channels)
    exact = cache_entry["exact"]
    results = []

    if norm in exact:
        record = dict(exact[norm])
        record["score"] = 1.0
        results.append(record)

    tokens = norm.split()
    if len(tokens) < 2:
        return results[:limit]

    candidate_indices = set()
    for token in tokens:
        candidate_indices.update(cache_entry["token_index"].get(token, set()))

    token_set = set(tokens)
    scored = []
    for idx in candidate_indices:
        record = cache_entry["normalized"][idx]
        cand_norm = record.get("norm", "")
        cand_tokens = set(cand_norm.split())
        if not cand_tokens:
            continue
        score = len(token_set & cand_tokens) / max(len(token_set), len(cand_tokens))
        scored.append((score, record))

    scored.sort(key=lambda item: item[0], reverse=True)
    min_score = max(0.2, threshold * 0.5)
    for score, record in scored:
        if score < min_score:
            break
        if results and record.get("station_id") == results[0].get("station_id") and results[0].get("score") == 1.0:
            continue
        entry = dict(record)
        entry["score"] = score
        results.append(entry)
        if len(results) >= limit:
            break

    return results[:limit]


def parse_event_patterns(value, defaults):
    if not value:
        return defaults
    patterns = []
    for line in str(value).splitlines():
        line = line.strip()
        if not line:
            continue
        if "=" in line:
            label, pattern = line.split("=", 1)
            label = label.strip()
            pattern = pattern.strip()
        else:
            label = normalize_event_label(line)
            pattern = line
        if label and pattern:
            patterns.append((label, pattern))
    return patterns if patterns else defaults


def parse_country_codes(value, defaults):
    if not value:
        return defaults
    tokens = re.split(r"[\s,]+", str(value).upper())
    codes = {token for token in tokens if token}
    return codes if codes else defaults


def parse_group_selection_patterns(value):
    if not value:
        return []
    patterns = []
    for line in str(value).splitlines():
        line = line.strip()
        if not line:
            continue
        patterns.append(line)
    return patterns


def build_tag_config(settings):
    hevc_only = [( "HEVC", r"\b(HEVC|H\.?265|H265)\b" )]
    return {
        "countries": parse_country_codes(settings.get("tag country codes"), DEFAULT_COUNTRY_CODES),
        "resolution": ensure_resolution_patterns(parse_labeled_patterns(settings.get("tag resolution patterns"), DEFAULT_RESOLUTION_PATTERNS)),
        "video": hevc_only,
        "audio": [],
        "event": parse_event_patterns(settings.get("tag event patterns"), DEFAULT_EVENT_PATTERNS),
        "misc": normalize_misc_patterns(parse_list_patterns(settings.get("tag misc patterns"), DEFAULT_MISC_PATTERNS)),
        "header": parse_list_patterns(settings.get("tag header patterns"), DEFAULT_HEADER_PATTERNS),
    }


def run_portal_matching(portal_id):
    settings = getSettings()
    if settings.get("channelsdvr enabled", "false") != "true":
        return 0

    portals = getPortals()
    portal = portals.get(portal_id)
    if not portal or portal.get("auto match", "false") != "true":
        return 0

    tag_config = build_tag_config(settings)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT c.channel_id, c.name, c.country, c.is_header
        FROM channels c
        LEFT JOIN groups g ON c.portal_id = g.portal_id AND c.genre_id = g.genre_id
        WHERE c.portal_id = ? AND {ACTIVE_GROUP_CONDITION}
    """, (portal_id,))

    rows = cursor.fetchall()
    matched = 0
    for row in rows:
        if row['is_header'] == 1:
            continue
        raw_name = row['name'] or ''
        if not raw_name or not row['country']:
            continue
        tag_info = extract_channel_tags(raw_name, tag_config, settings, allow_match=True)
        cursor.execute("""
            UPDATE channels
            SET matched_name = ?, matched_source = ?, matched_station_id = ?,
                matched_call_sign = ?, matched_logo = ?, matched_score = ?,
                display_name = COALESCE(NULLIF(custom_name, ''), NULLIF(?, ''), NULLIF(auto_name, ''), name)
            WHERE portal_id = ? AND channel_id = ?
        """, (
            tag_info.get("matched_name", ""),
            tag_info.get("matched_source", ""),
            tag_info.get("matched_station_id", ""),
            tag_info.get("matched_call_sign", ""),
            tag_info.get("matched_logo", ""),
            tag_info.get("matched_score", ""),
            tag_info.get("matched_name", ""),
            portal_id,
            row['channel_id']
        ))
        if tag_info.get("matched_name"):
            matched += 1

    conn.commit()
    conn.close()
    return matched


def ascii_fold(value):
    """Best-effort ASCII fold to detect tags (handles small-cap unicode letters)."""
    if value is None:
        return ""
    result = []
    homograph_map = {
        "ғ": "F",
        "Ғ": "F",
        "ᴴ": "H",
        "ʜ": "H",
        "ᴰ": "D",
        "ᴅ": "D",
        "ᵁ": "U",
        "ˡ": "L",
        "ᵗ": "T",
        "ʳ": "R",
        "ᵃ": "A",
    }
    for ch in str(value):
        if ch in homograph_map:
            result.append(homograph_map[ch])
            continue
        if "A" <= ch <= "Z" or "a" <= ch <= "z" or "0" <= ch <= "9":
            result.append(ch)
            continue
        try:
            name = unicodedata.name(ch)
        except ValueError:
            result.append(" ")
            continue
        if ch.isalnum():
            if (
                "LATIN LETTER SMALL CAPITAL" in name
                or "LATIN CAPITAL LETTER" in name
                or "LATIN SMALL LETTER" in name
                or "MODIFIER LETTER SMALL CAPITAL" in name
                or "MODIFIER LETTER SMALL" in name
            ):
                result.append(name.split()[-1])
            else:
                result.append(" ")
            continue
        try:
            name = unicodedata.name(ch)
        except ValueError:
            result.append(" ")
            continue
        if "SMALL CAPITAL" in name or "MODIFIER LETTER SMALL" in name:
            result.append(name.split()[-1])
            continue
        result.append(" ")
    return "".join(result)


def extract_channel_tags(raw_name, tag_config, settings=None, allow_match=True):
    """Extract tags and return cleaned name + metadata."""
    if not raw_name:
        return {
            "clean_name": "",
            "resolution": "",
            "video_codec": "",
            "country": "",
            "event_tags": "",
            "misc_tags": "",
            "is_header": 0,
            "is_event": 0,
            "is_raw": 0,
        }

    name = str(raw_name)
    folded = ascii_fold(name).upper()
    name_upper = re.sub(r"[^A-Z0-9\.\+]+", " ", folded)

    is_header = 0
    for pattern in tag_config["header"]:
        if re.match(pattern, name):
            is_header = 1
            break
    if not is_header:
        non_word = re.sub(r"[\w\s]", "", name, flags=re.UNICODE)
        if non_word and len(non_word) >= 4 and len(non_word) >= len(name.strip()) * 0.3:
            is_header = 1

    resolution = ""
    resolution_pattern = ""
    resolution_match_end = None
    segment_candidates = name.split("|") if "|" in name else [name]
    for segment in segment_candidates:
        segment_fold = ascii_fold(segment).upper()
        segment_upper = re.sub(r"[^A-Z0-9\.\+]+", " ", segment_fold)
        for label, pattern in tag_config["resolution"]:
            for match in re.finditer(pattern, segment_upper):
                tail = segment_upper[match.end():]
                if re.search(r"[A-Z0-9]", tail):
                    continue
                resolution = label
                resolution_pattern = pattern
                break
            if resolution:
                break
        if resolution:
            try:
                full_match = re.search(resolution_pattern, name_upper)
            except re.error:
                full_match = None
            if full_match:
                resolution_match_end = full_match.end()
            break

    video_codec = ""
    for label, pattern in tag_config["video"]:
        if re.search(pattern, name_upper):
            video_codec = label
            break
    if video_codec and video_codec != "HEVC":
        video_codec = ""

    raw_pattern = r"(?:\bRAW\b|ᴿᴬᵂ|ʀᴀᴡ)"
    is_raw = 1 if re.search(r"\bRAW\b", name_upper) or re.search(raw_pattern, name, flags=re.IGNORECASE) else 0

    is_event = 0
    event_tags = []
    for label, pattern in tag_config["event"]:
        if re.search(pattern, name_upper):
            is_event = 1
            event_tags.append(label)
    if event_tags:
        event_tags = list(dict.fromkeys(event_tags))

    misc_tags = []
    for pattern in tag_config["misc"]:
        try:
            match = re.search(pattern, name_upper)
        except re.error:
            match = None
        if not match:
            continue
        is_sat_pattern = pattern in (
            r"\bSAT(?![.\s]*\d)\b",
            r"(?<!\b\d\s)\bSAT(?![.\s]*\d)\b",
        )
        if is_sat_pattern:
            if resolution_match_end is None or match.start() <= resolution_match_end:
                continue
        misc_tags.append(pattern)
    misc_tags = list(dict.fromkeys([normalize_event_label(tag) for tag in misc_tags]))

    country = ""
    tokens = re.findall(r"[A-Z0-9]+", name_upper)
    for token in tokens:
        if token in tag_config["countries"]:
            country = token
            break

    canonical_name = ""
    matched_station_id = ""
    matched_call_sign = ""
    matched_logo = ""
    matched_score = ""
    if settings and allow_match and country and not is_header:
        match_input = raw_name
        folded_for_match = ascii_fold(raw_name).upper()
        for _label, pattern in tag_config["resolution"]:
            m = re.search(pattern, folded_for_match)
            if m:
                folded_for_match = folded_for_match[:m.start()].strip()
                break
        if folded_for_match:
            match_input = folded_for_match
        match_input_clean = match_input
        if country:
            match_input_clean = re.sub(
                rf"(?i)(^|[^A-Za-z0-9]){re.escape(country)}(?=$|[^A-Za-z0-9])",
                " ",
                match_input_clean,
            )
        match_input_clean = re.sub(r"(?i)\b3\s+SAT\b", "3SAT", match_input_clean)
        for pattern in tag_config["misc"]:
            match_input_clean = re.sub(pattern, " ", match_input_clean, flags=re.IGNORECASE)
        match_input_clean = re.sub(r"[:\-\|]+", " ", match_input_clean)
        match_input_clean = re.sub(r"\s+", " ", match_input_clean).strip()
        if match_input_clean:
            match_input = match_input_clean
        match = match_channelsdvr_name(match_input, country, settings)
        if match:
            canonical_name = match.get("name", "")
            matched_station_id = match.get("station_id", "")
            matched_call_sign = match.get("call_sign", "")
            matched_logo = match.get("logo_uri", "")
            matched_score = match.get("score", "")

    cleaned = name
    resolution_unicode_removals = {
        "UHD": r"(?:ᵁˡᵗʳᵃ)",
        "FHD": r"(?:ғʜᴅ)",
        "HD": r"(?:ᴴᴰ|ʜᴅ)",
    }
    dropped_tag_segments = False
    segment_split = re.split(r"\s*\|\s*", name) if "|" in name else [name]

    if len(segment_split) > 1:
        kept_segments = []
        removal_for_segments = []
        for _label, pattern in tag_config["resolution"]:
            removal_for_segments.append(pattern)
        for _label, pattern in tag_config["video"]:
            removal_for_segments.append(pattern)
        removal_for_segments.append(r"\bRAW\b")
        removal_for_segments.extend([pattern for _, pattern in tag_config["event"]])
        removal_for_segments.extend(tag_config["misc"])

        for segment in segment_split:
            segment_upper = re.sub(r"[^A-Z0-9\.\+]+", " ", ascii_fold(segment).upper())
            residual = segment_upper
            for pattern in removal_for_segments:
                residual = re.sub(pattern, " ", residual, flags=re.IGNORECASE)
            if tag_config["countries"]:
                tokens = re.findall(r"[A-Z0-9]+", residual)
                tokens = [token for token in tokens if token not in tag_config["countries"]]
                residual = " ".join(tokens)
            residual = re.sub(r"\s+", " ", residual).strip()
            if residual:
                kept_segments.append(segment)
            else:
                dropped_tag_segments = True

        if kept_segments:
            cleaned = " ".join(kept_segments)

    removal_patterns = []
    if resolution_pattern:
        removal_patterns.append(resolution_pattern)
    unicode_resolution = resolution_unicode_removals.get(resolution)
    if unicode_resolution:
        removal_patterns.append(unicode_resolution)
    for _label, pattern in tag_config["video"]:
        removal_patterns.append(pattern)
    removal_patterns.append(raw_pattern)
    if not dropped_tag_segments:
        removal_patterns.extend([pattern for _, pattern in tag_config["event"]])
    removal_patterns.extend(tag_config["misc"])

    for pattern in removal_patterns:
        cleaned = re.sub(pattern, " ", cleaned, flags=re.IGNORECASE)

    if country:
        cleaned = re.sub(rf"(?i)(^|[^A-Za-z0-9]){re.escape(country)}(?=$|[^A-Za-z0-9])", " ", cleaned)

    cleaned = re.sub(r"[^\w\s]", " ", cleaned, flags=re.UNICODE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    if resolution:
        normalized_cleaned = ascii_fold(cleaned).upper().split()
        if normalized_cleaned and normalized_cleaned[-1] == resolution:
            cleaned = " ".join(cleaned.split()[:-1]).strip()

    matched_name = canonical_name.strip() if canonical_name else ""
    matched_source = "channelsdvr" if matched_name else ""

    if not is_header and matched_name:
        cleaned = canonical_name.strip()

    if is_header:
        cleaned = name
        resolution = ""
        video_codec = ""
        country = ""
        event_tags = []
        misc_tags = []
        is_event = 0
        is_raw = 0

    return {
        "clean_name": cleaned,
        "resolution": resolution,
        "video_codec": video_codec,
        "country": country,
        "event_tags": ",".join(event_tags),
        "misc_tags": ",".join(misc_tags),
        "event_tags_list": event_tags,
        "misc_tags_list": misc_tags,
        "matched_name": matched_name,
        "matched_source": matched_source,
        "matched_station_id": matched_station_id,
        "matched_call_sign": matched_call_sign,
        "matched_logo": matched_logo,
        "matched_score": matched_score,
        "is_header": 1 if is_header else 0,
        "is_event": 1 if is_event else 0,
        "is_raw": 1 if is_raw else 0,
    }


def effective_display_name(custom_name, matched_name, auto_name, name):
    """Return preferred display name: custom > matched > auto > original."""
    return custom_name or matched_name or auto_name or name


def effective_epg_name(custom_name, auto_name, name):
    """Return preferred EPG name: original channel name (stable)."""
    return name


def sync_channel_tags(cursor, portal_id, channel_id, event_tags, misc_tags):
    """Replace channel tags for a channel using normalized tag storage."""
    cursor.execute(
        "DELETE FROM channel_tags WHERE portal_id = ? AND channel_id = ?",
        (portal_id, channel_id),
    )
    rows = []
    for value in event_tags or []:
        rows.append((portal_id, channel_id, "event", value))
    for value in misc_tags or []:
        rows.append((portal_id, channel_id, "misc", value))
    if rows:
        cursor.executemany(
            "INSERT OR IGNORE INTO channel_tags (portal_id, channel_id, tag_type, tag_value) VALUES (?, ?, ?, ?)",
            rows,
        )


def score_mac_for_selection(mac, mac_data, occupied_list, streams_per_mac):
    """Bewertet eine MAC für die Stream-Auswahl. Höherer Score = bessere Wahl.

    Scoring-Kriterien:
    - Watchdog Timeout: Höhere Werte (länger inaktiv) = besser
    - Verfügbare Stream-Slots: Mehr verfügbar = besser
    - Rückgabe -1 wenn MAC nicht verfügbar (alle Slots belegt)
    """
    data = normalize_mac_data(mac_data)
    score = 0

    # Zähle aktuelle Streams auf dieser MAC
    current_streams = sum(1 for o in occupied_list if o.get("mac") == mac)
    available_slots = streams_per_mac - current_streams

    if streams_per_mac != 0 and available_slots <= 0:
        return -1  # MAC ist voll belegt

    # Watchdog Timeout Scoring (höher = länger inaktiv = besser)
    watchdog = int(data.get("watchdog_timeout", 0) or 0)
    if watchdog > 1800:      # Grün: > 30 min idle
        score += 100
    elif watchdog > 300:     # Blau: 5-30 min
        score += 75
    elif watchdog >= 60:     # Gelb: 1-5 min
        score += 50
    elif watchdog > 0:       # Rot: < 1 min (sehr aktiv)
        score += 10
    # watchdog == 0 bedeutet keine Daten, neutral

    # Verfügbare Slots Bonus
    if streams_per_mac != 0:
        score += available_slots * 20

    return score


class HLSStreamManager:
    """Manages HLS streams with shared access and automatic cleanup."""
    
    def __init__(self, max_streams=10, inactive_timeout=30):
        self.streams = {}  # Key: "portalId_channelId", Value: stream info dict
        self.max_streams = max_streams
        self.inactive_timeout = inactive_timeout
        self.lock = threading.Lock()
        self.monitor_thread = None
        self.running = False
        
    def start_monitoring(self):
        """Start the background monitoring thread."""
        if not self.running:
            self.running = True
            self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self.monitor_thread.start()
            logger.info("HLS Stream Manager monitoring started")
    
    def _monitor_loop(self):
        """Background thread that monitors and cleans up inactive streams."""
        while self.running:
            try:
                time.sleep(10)  # Check every 10 seconds
                self._cleanup_inactive_streams()
            except Exception as e:
                logger.error(f"Error in HLS monitor loop: {e}")
    
    def _cleanup_inactive_streams(self):
        """Clean up streams that have been inactive or crashed."""
        current_time = time.time()
        streams_to_remove = []
        
        with self.lock:
            for stream_key, stream_info in self.streams.items():
                is_passthrough = stream_info.get('is_passthrough', False)
                
                # Skip process checks for passthrough streams
                if not is_passthrough:
                    # Check if process has crashed
                    if stream_info['process'].poll() is not None:
                        returncode = stream_info['process'].returncode
                        if returncode != 0:
                            logger.error(f"✗ FFmpeg process crashed for {stream_key} (exit code: {returncode})")
                            # Try to get stderr output
                            try:
                                stderr_output = stream_info['process'].stderr.read().decode('utf-8', errors='ignore')
                                if stderr_output:
                                    # Log last 1000 characters of error
                                    logger.error(f"FFmpeg stderr for {stream_key}:\n{stderr_output[-1000:]}")
                            except Exception as e:
                                logger.debug(f"Could not read FFmpeg stderr: {e}")
                        else:
                            logger.info(f"FFmpeg process exited cleanly for {stream_key}")
                        streams_to_remove.append(stream_key)
                        continue
                
                # Check if stream is inactive
                inactive_time = current_time - stream_info['last_accessed']
                if inactive_time > self.inactive_timeout:
                    stream_type = "passthrough" if is_passthrough else "FFmpeg"
                    logger.info(f"Cleaning up inactive {stream_type} stream {stream_key} (idle for {inactive_time:.1f}s)")
                    streams_to_remove.append(stream_key)
        
        # Clean up streams outside the lock to avoid blocking
        for stream_key in streams_to_remove:
            self._stop_stream(stream_key)
    
    def _stop_stream(self, stream_key):
        """Stop a stream and clean up its resources."""
        with self.lock:
            if stream_key not in self.streams:
                logger.debug(f"Attempted to stop non-existent stream: {stream_key}")
                return
            
            stream_info = self.streams[stream_key]
            is_passthrough = stream_info.get('is_passthrough', False)
            stream_type = "passthrough" if is_passthrough else "FFmpeg"
            
            logger.debug(f"Stopping {stream_type} stream: {stream_key}")
            
            # Terminate FFmpeg process (skip for passthrough streams)
            if not is_passthrough:
                try:
                    if stream_info['process'].poll() is None:
                        logger.debug(f"Terminating FFmpeg process (PID: {stream_info['process'].pid})")
                        stream_info['process'].terminate()
                        stream_info['process'].wait(timeout=5)
                        logger.debug(f"FFmpeg process terminated successfully")
                    else:
                        # Process already exited, log stderr if available
                        try:
                            stderr_output = stream_info['process'].stderr.read().decode('utf-8', errors='ignore')
                            if stderr_output:
                                logger.debug(f"FFmpeg stderr (last 500 chars): {stderr_output[-500:]}")
                        except:
                            pass
                except subprocess.TimeoutExpired:
                    logger.warning(f"FFmpeg process did not terminate gracefully, killing it")
                    try:
                        stream_info['process'].kill()
                    except:
                        pass
                except Exception as e:
                    logger.error(f"Error terminating FFmpeg for {stream_key}: {e}")
                    try:
                        stream_info['process'].kill()
                    except:
                        pass
            
            # Clean up temp directory
            try:
                if os.path.exists(stream_info['temp_dir']):
                    temp_dir = stream_info['temp_dir']
                    shutil.rmtree(temp_dir, ignore_errors=True)
                    logger.debug(f"Removed temp directory: {temp_dir}")
            except Exception as e:
                logger.error(f"Error cleaning up temp dir for {stream_key}: {e}")
            
            # Remove from active streams
            del self.streams[stream_key]
            logger.info(f"✓ {stream_type.capitalize()} stream {stream_key} stopped and cleaned up")
    
    def start_stream(self, portal_id, channel_id, stream_url, proxy=None):
        """Start or reuse an HLS stream for a channel."""
        stream_key = f"{portal_id}_{channel_id}"
        
        with self.lock:
            # Check if stream already exists
            if stream_key in self.streams:
                # Update last accessed time
                self.streams[stream_key]['last_accessed'] = time.time()
                logger.info(f"Reusing existing HLS stream for {stream_key}")
                return self.streams[stream_key]
            
            # Check concurrency limit
            if len(self.streams) >= self.max_streams:
                logger.error(f"Max concurrent streams ({self.max_streams}) reached")
                raise Exception(f"Maximum concurrent streams ({self.max_streams}) reached")
            
            # Get HLS settings
            settings = getSettings()
            segment_type = settings.get("hls segment type", "mpegts")  # Default to mpegts for compatibility
            segment_duration = settings.get("hls segment duration", "4")
            playlist_size = settings.get("hls playlist size", "6")
            timeout = int(settings.get("ffmpeg timeout", "5")) * 1000000
            
            # Detect if source is already HLS (e.g., Pluto TV stitcher URLs)
            is_source_hls = (".m3u8" in stream_url.lower() or 
                           "hls" in stream_url.lower() or 
                           "stitcher" in stream_url.lower())
            
            # Log detection result
            if is_source_hls:
                logger.info(f"Detected HLS source for {stream_key}: URL contains HLS indicators")
                logger.debug(f"Source URL: {stream_url[:100]}...")
            else:
                logger.info(f"Detected non-HLS source for {stream_key}, will use FFmpeg re-encoding")
                logger.debug(f"Source URL: {stream_url[:100]}...")
            
            # Create temp directory for HLS segments
            temp_dir = tempfile.mkdtemp(prefix=f"macreplay_hls_{stream_key}_")
            playlist_path = os.path.join(temp_dir, "stream.m3u8")
            master_playlist_path = os.path.join(temp_dir, "master.m3u8")
            logger.debug(f"Created temp directory for {stream_key}: {temp_dir}")
            
            # If source is already HLS, create a proxy/passthrough instead of re-encoding
            if is_source_hls:
                logger.info(f"Creating HLS passthrough for {stream_key} (no FFmpeg process)")
                
                # Store stream info with passthrough flag
                stream_info = {
                    'process': None,  # No FFmpeg process for passthrough
                    'temp_dir': temp_dir,
                    'playlist_path': playlist_path,
                    'master_playlist_path': master_playlist_path,
                    'last_accessed': time.time(),
                    'portal_id': portal_id,
                    'channel_id': channel_id,
                    'stream_url': stream_url,
                    'is_passthrough': True
                }
                
                # Create master playlist that points to the source
                with open(master_playlist_path, 'w') as f:
                    f.write("#EXTM3U\n")
                    f.write("#EXT-X-VERSION:7\n")
                    f.write(f'#EXT-X-STREAM-INF:BANDWIDTH=15000000,CODECS="avc1.640028,mp4a.40.2"\n')
                    f.write(stream_url + "\n")
                
                self.streams[stream_key] = stream_info
                logger.info(f"✓ HLS passthrough ready for {stream_key} (redirects to source)")
                logger.debug(f"Master playlist created at: {master_playlist_path}")
                
                return stream_info
            
            # Set segment pattern and init file based on segment type
            if segment_type == "fmp4":
                segment_pattern = os.path.join(temp_dir, "seg_%03d.m4s")
                init_filename = "init.mp4"
            else:
                segment_pattern = os.path.join(temp_dir, "seg_%03d.ts")
                init_filename = None
            
            # Build FFmpeg command for HLS
            # Based on working mpegts command, adapted for HLS
            ffmpeg_cmd = [
                "ffmpeg",
                "-fflags", "+genpts+igndts+nobuffer",
                "-err_detect", "aggressive",
                "-flags", "low_delay",
                "-reconnect", "1",
                "-reconnect_at_eof", "1",
                "-reconnect_streamed", "1",
                "-reconnect_delay_max", "15",
            ]
            
            # Add proxy if provided
            if proxy:
                ffmpeg_cmd.extend(["-http_proxy", proxy])
            
            # Add timeout
            ffmpeg_cmd.extend(["-timeout", str(timeout)])
            
            # Input and basic video settings
            ffmpeg_cmd.extend([
                "-i", stream_url,
                "-map", "0",                   # Map all streams
                "-c:v", "copy",                # Always copy video (never transcode)
                "-copyts",                     # Copy timestamps
                "-start_at_zero"               # Start at zero timestamp
            ])
            
            # Audio codec settings - always transcode for compatibility
            # (Based on working command that used AAC transcoding)
            ffmpeg_cmd.extend([
                "-c:a", "aac",                 # Transcode audio to AAC
                "-b:a", "256k",                # Audio bitrate
                "-af", "aresample=async=1"     # Audio resampling for sync
            ])
            logger.debug(f"Using AAC audio transcoding at 256k with async resampling")
            
            
            # HLS output settings with conditional flags
            # Removed delete_segments to prevent premature segment deletion
            hls_flags = "independent_segments+omit_endlist"
            
            # Add format-specific flags only when needed
            if segment_type == "mpegts":
                hls_flags += "+program_date_time"
                # MPEG-TS specific flags (from working command)
                ffmpeg_cmd.extend([
                    "-mpegts_flags", "pat_pmt_at_frames",
                    "-pcr_period", "20"
                ])
                logger.debug(f"Added MPEG-TS specific flags: pat_pmt_at_frames, pcr_period 20")
            
            ffmpeg_cmd.extend([
                "-f", "hls",
                "-hls_time", segment_duration,
                "-hls_list_size", playlist_size,
                "-hls_flags", hls_flags,
                "-hls_segment_type", segment_type,
                "-hls_segment_filename", segment_pattern,
                "-start_number", "0",
                "-flush_packets", "0"
            ])
            
            # Add init filename for fMP4
            if segment_type == "fmp4":
                ffmpeg_cmd.extend(["-hls_fmp4_init_filename", init_filename])
            
            # Output to stream.m3u8
            ffmpeg_cmd.append(playlist_path)
            
            # Start FFmpeg process
            try:
                # Log the FFmpeg command for debugging
                logger.info(f"Starting FFmpeg process for {stream_key}")
                logger.debug(f"FFmpeg command: {' '.join(ffmpeg_cmd)}")
                
                process = subprocess.Popen(
                    ffmpeg_cmd,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    text=True,
                    bufsize=1
                )
                
                logger.debug(f"FFmpeg process started with PID: {process.pid}")
                
                # Start thread to read FFmpeg stderr for error logging
                def log_ffmpeg_stderr():
                    try:
                        for line in process.stderr:
                            line = line.strip()
                            if line:
                                # Log important FFmpeg messages
                                if 'error' in line.lower() or 'failed' in line.lower():
                                    logger.error(f"FFmpeg[{process.pid}]: {line}")
                                elif 'warning' in line.lower():
                                    logger.warning(f"FFmpeg[{process.pid}]: {line}")
                                elif any(x in line.lower() for x in ['output', 'stream', 'duration', 'encoder']):
                                    logger.debug(f"FFmpeg[{process.pid}]: {line}")
                    except Exception as e:
                        logger.debug(f"FFmpeg stderr reader thread ended: {e}")
                
                import threading
                stderr_thread = threading.Thread(target=log_ffmpeg_stderr, daemon=True)
                stderr_thread.start()
                
                # Store stream info
                stream_info = {
                    'process': process,
                    'temp_dir': temp_dir,
                    'playlist_path': playlist_path,
                    'master_playlist_path': master_playlist_path,
                    'last_accessed': time.time(),
                    'portal_id': portal_id,
                    'channel_id': channel_id,
                    'stream_url': stream_url,
                    'is_passthrough': False
                }
                
                self.streams[stream_key] = stream_info
                
                # Create master playlist manually (FFmpeg doesn't create it for single streams)
                # This points to the stream.m3u8 that FFmpeg generates
                # Omit CODECS to let Plex auto-detect (more compatible)
                try:
                    with open(master_playlist_path, 'w') as f:
                        f.write("#EXTM3U\n")
                        f.write("#EXT-X-VERSION:3\n")  # Use v3 for max compatibility
                        f.write(f'#EXT-X-STREAM-INF:BANDWIDTH=5000000\n')
                        f.write("stream.m3u8\n")
                    logger.debug(f"Created master playlist at {master_playlist_path}")
                except Exception as e:
                    logger.warning(f"Failed to create master playlist: {e}")
                
                logger.info(f"✓ FFmpeg HLS stream ready for {stream_key}")
                logger.debug(f"Temp dir: {temp_dir}, PID: {process.pid}")
                
                return stream_info
                
            except Exception as e:
                logger.error(f"✗ Failed to start HLS stream for {stream_key}: {e}")
                logger.debug(f"Exception type: {type(e).__name__}")
                # Clean up temp dir on failure
                try:
                    shutil.rmtree(temp_dir, ignore_errors=True)
                    logger.debug(f"Cleaned up failed temp dir: {temp_dir}")
                except Exception as cleanup_error:
                    logger.debug(f"Could not clean up temp dir: {cleanup_error}")
                raise
    
    def get_file(self, portal_id, channel_id, filename):
        """Get a file from the HLS stream (playlist or segment)."""
        stream_key = f"{portal_id}_{channel_id}"
        
        with self.lock:
            if stream_key not in self.streams:
                logger.warning(f"File request for inactive stream: {stream_key}/{filename}")
                return None
            
            stream_info = self.streams[stream_key]
            stream_info['last_accessed'] = time.time()
            
            # Log file access
            is_passthrough = stream_info.get('is_passthrough', False)
            logger.debug(f"File request: {stream_key}/{filename} (passthrough={is_passthrough})")
            
            # Determine file path
            file_path = os.path.join(stream_info['temp_dir'], filename)
            
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                logger.debug(f"Serving file: {filename} ({file_size} bytes)")
                return file_path
            else:
                # File not found - check if FFmpeg died (only log error if it crashed)
                if not is_passthrough and stream_info['process']:
                    if stream_info['process'].poll() is not None:
                        exit_code = stream_info['process'].returncode
                        logger.error(f"FFmpeg process died for {stream_key} (exit code: {exit_code})")
                        logger.error(f"Missing file: {filename} (expected at {file_path})")
                # Don't log WARNING here - the caller will log if timeout occurs
                return None
    
    def cleanup_all(self):
        """Clean up all active streams (called on shutdown)."""
        logger.info("Cleaning up all HLS streams...")
        self.running = False
        
        stream_keys = list(self.streams.keys())
        for stream_key in stream_keys:
            self._stop_stream(stream_key)
        
        logger.info("All HLS streams cleaned up")


# Global HLS stream manager
hls_manager = HLSStreamManager(max_streams=10, inactive_timeout=30)


def fetch_portal_channels(portal_id, portal):
    portal_name = portal["name"]
    url = portal["url"]
    macs = list(portal["macs"].keys())
    proxy = portal["proxy"]

    logger.info(f"Fetching channels for portal: {portal_name}")

    # Query ALL MACs and collect which channels are available on which MACs
    # Structure: {channel_id: {data: {...}, available_macs: [mac1, mac2, ...]}}
    channels_by_id = {}
    all_genres = {}  # Merged genres from all MACs {genre_id: genre_name}

    for mac in macs:
        logger.info(f"Querying MAC: {mac}")
        try:
            token = stb.getToken(url, mac, proxy)
            if not token:
                logger.warning(f"Could not get token for MAC {mac}")
                continue

            stb.getProfile(url, mac, token, proxy)
            mac_channels_raw = stb.getAllChannels(url, mac, token, proxy)
            mac_genres = stb.getGenreNames(url, mac, token, proxy)

            if not mac_channels_raw:
                logger.warning(f"No channels returned for MAC {mac}")
                continue

            # Handle both list and dict formats (some portals return dict with channel IDs as keys)
            mac_channels = mac_channels_raw if isinstance(mac_channels_raw, list) else list(mac_channels_raw.values())

            # Merge genres
            if mac_genres:
                all_genres.update(mac_genres)

            logger.info(f"MAC {mac} returned {len(mac_channels)} channels")

            # Process channels from this MAC
            for channel in mac_channels:
                if not isinstance(channel, dict):
                    continue
                channel_id = str(channel["id"])
                genre_id = str(channel.get("tv_genre_id", ""))

                if channel_id not in channels_by_id:
                    # First time seeing this channel
                    channels_by_id[channel_id] = {
                        "data": channel,
                        "available_macs": [mac]
                    }
                else:
                    # Channel already exists, add this MAC to available_macs
                    if mac not in channels_by_id[channel_id]["available_macs"]:
                        channels_by_id[channel_id]["available_macs"].append(mac)

        except Exception as e:
            logger.error(f"Error fetching from MAC {mac}: {e}")

    return {
        "portal_id": portal_id,
        "portal": portal,
        "portal_name": portal_name,
        "channels_by_id": channels_by_id,
        "all_genres": all_genres
    }


def refresh_channels_cache(target_portal_id=None):
    """Refresh the channels cache from STB portals.

    Args:
        target_portal_id: Optional portal ID to refresh only that portal.
                         If None, refreshes all enabled portals.
    """
    if target_portal_id:
        logger.info(f"Starting channel cache refresh for portal: {target_portal_id}")
    else:
        logger.info("Starting channel cache refresh for all portals...")
    portals = getPortals()
    tag_config = build_tag_config(getSettings())
    conn = get_db_connection()
    cursor = conn.cursor()

    total_channels = 0

    def build_channel_hash(values):
        joined = "|".join(values)
        return hashlib.sha1(joined.encode("utf-8")).hexdigest()

    portal_ids = []
    for portal_id, portal in portals.items():
        if target_portal_id and portal_id != target_portal_id:
            continue
        if portal["enabled"] == "true":
            portal_ids.append(portal_id)

    results = []
    if target_portal_id or len(portal_ids) <= 1:
        for portal_id in portal_ids:
            portal = portals[portal_id]
            results.append(fetch_portal_channels(portal_id, portal))
    else:
        max_workers = min(4, len(portal_ids))
        logger.info(f"Fetching portals in parallel with {max_workers} workers")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(fetch_portal_channels, portal_id, portals[portal_id]): portal_id
                for portal_id in portal_ids
            }
            for future in as_completed(futures):
                portal_id = futures[future]
                try:
                    results.append(future.result())
                except Exception as e:
                    logger.error(f"Error fetching portal {portal_id}: {e}")

    for result in results:
        portal_id = result["portal_id"]
        portal = result["portal"]
        portal_name = result["portal_name"]
        channels_by_id = result["channels_by_id"]
        all_genres = result["all_genres"]
        portal_auto_normalize = portal.get("auto normalize names", "false") == "true"
        active_genres = set()
        try:
            cursor.execute("SELECT genre_id FROM groups WHERE portal_id = ? AND active = 1", (portal_id,))
            active_genres = {str(row[0]) for row in cursor.fetchall() if row[0]}
        except Exception as e:
            logger.debug(f"Could not load active genres for portal {portal_name}: {e}")

        existing_hashes = {}
        cursor.execute("SELECT channel_id, channel_hash FROM channels WHERE portal_id = ?", (portal_id,))
        for row in cursor.fetchall():
            existing_hashes[row["channel_id"]] = row["channel_hash"] or ""

        # Now insert all collected channels into the database
        if channels_by_id:
            logger.info(f"Processing {len(channels_by_id)} unique channels for {portal_name}")

            # Auto-merge channels with the same name (deduplication)
            # Group channels by normalized name
            channels_by_name = {}
            for channel_id, channel_info in channels_by_id.items():
                channel_name = str(channel_info["data"]["name"]).strip()
                if channel_name not in channels_by_name:
                    channels_by_name[channel_name] = []
                channels_by_name[channel_name].append((channel_id, channel_info))

            # Process duplicates - merge into primary channel
            channels_to_import = {}  # channel_id -> {data, available_macs, alternate_ids}
            merged_count = 0

            for channel_name, channel_list in channels_by_name.items():
                if len(channel_list) == 1:
                    # No duplicates - just add normally
                    ch_id, ch_info = channel_list[0]
                    channels_to_import[ch_id] = {
                        "data": ch_info["data"],
                        "available_macs": ch_info["available_macs"],
                        "alternate_ids": []
                    }
                else:
                    # Multiple channels with same name - merge them
                    # Sort by channel_id (use lowest ID as primary)
                    channel_list.sort(key=lambda x: int(x[0]) if x[0].isdigit() else float('inf'))
                    primary_id, primary_info = channel_list[0]

                    # Combine all MACs and alternate IDs
                    combined_macs = set(primary_info["available_macs"])
                    alternate_ids = []

                    for alt_id, alt_info in channel_list[1:]:
                        alternate_ids.append(alt_id)
                        combined_macs.update(alt_info["available_macs"])

                    channels_to_import[primary_id] = {
                        "data": primary_info["data"],
                        "available_macs": sorted(combined_macs),
                        "alternate_ids": alternate_ids
                    }

                    merged_count += len(channel_list) - 1
                    logger.debug(f"Auto-merged '{channel_name}': {primary_id} + alternates {alternate_ids}")

            if merged_count > 0:
                logger.info(f"Auto-merged {merged_count} duplicate channels by name for {portal_name}")

            # Count channels per genre (use deduplicated channels, full portal set)
            genre_channel_counts = {}
            for ch_info in channels_to_import.values():
                g_id = str(ch_info["data"].get("tv_genre_id", ""))
                genre_channel_counts[g_id] = genre_channel_counts.get(g_id, 0) + 1

            channels_imported = 0
            channels_updated = 0
            channels_skipped = 0
            channels_deleted = 0
            new_channel_ids = set()

            for channel_id, channel_info in channels_to_import.items():
                channel = channel_info["data"]
                available_macs = ",".join(sorted(channel_info["available_macs"]))
                alternate_ids = ",".join(sorted(channel_info["alternate_ids"]))

                channel_name = str(channel["name"])
                channel_number = str(channel["number"])
                genre_id = str(channel.get("tv_genre_id", ""))
                genre = str(all_genres.get(genre_id, ""))
                logo = str(channel.get("logo", ""))
                cmd = str(channel.get("cmd", ""))  # Cache stream command for fast streaming
                tag_info = extract_channel_tags(channel_name, tag_config, getSettings(), allow_match=False)
                auto_name = tag_info["clean_name"] if portal_auto_normalize and tag_info["clean_name"] else ""
                resolution = tag_info["resolution"]
                video_codec = tag_info["video_codec"]
                country = tag_info["country"]
                event_tags = tag_info["event_tags"]
                misc_tags = tag_info["misc_tags"]
                event_tags_list = tag_info.get("event_tags_list", [])
                misc_tags_list = tag_info.get("misc_tags_list", [])
                matched_name = tag_info["matched_name"]
                matched_source = tag_info["matched_source"]
                matched_station_id = tag_info.get("matched_station_id", "")
                matched_call_sign = tag_info.get("matched_call_sign", "")
                matched_logo = tag_info.get("matched_logo", "")
                matched_score = tag_info.get("matched_score", "")
                is_header = tag_info["is_header"]
                is_event = tag_info["is_event"]
                is_raw = tag_info["is_raw"]

                # Upsert into database
                enable_by_group = (
                    (not active_genres)
                    or (genre_id in active_genres)
                    or ((not genre_id) and ("UNGROUPED" in active_genres))
                )
                if not enable_by_group:
                    continue
                enabled_default = 0

                new_channel_ids.add(channel_id)

                display_name = effective_display_name("", matched_name, auto_name, channel_name)

                channel_hash = build_channel_hash(
                    [
                        portal_id,
                        channel_id,
                        portal_name,
                        channel_name,
                        channel_number,
                        genre,
                        genre_id,
                        logo,
                        auto_name,
                        resolution,
                        video_codec,
                        country,
                        event_tags,
                        misc_tags,
                        matched_name,
                        matched_source,
                        str(matched_station_id),
                        str(matched_call_sign),
                        str(matched_logo),
                        str(matched_score),
                        str(is_header),
                        str(is_event),
                        str(is_raw),
                        available_macs,
                        alternate_ids,
                        cmd,
                    ]
                )

                if existing_hashes.get(channel_id, "") == channel_hash:
                    channels_skipped += 1
                    continue

                cursor.execute('''
                    INSERT INTO channels (
                        portal_id, channel_id, portal_name, name, display_name, number, genre, genre_id, logo,
                        enabled, custom_name, auto_name, custom_number, custom_genre,
                        custom_epg_id, resolution, video_codec, country,
                            event_tags, misc_tags, matched_name, matched_source,
                            matched_station_id, matched_call_sign, matched_logo, matched_score,
                            is_header, is_event, is_raw, available_macs, alternate_ids, cmd,
                            channel_hash
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(portal_id, channel_id) DO UPDATE SET
                            portal_name = excluded.portal_name,
                            name = excluded.name,
                            display_name = COALESCE(NULLIF(channels.custom_name, ''), NULLIF(excluded.matched_name, ''), NULLIF(excluded.auto_name, ''), excluded.name),
                            number = excluded.number,
                            genre = excluded.genre,
                            genre_id = excluded.genre_id,
                            logo = excluded.logo,
                            auto_name = CASE
                                WHEN excluded.auto_name != '' THEN excluded.auto_name
                                ELSE channels.auto_name
                            END,
                            resolution = excluded.resolution,
                            video_codec = excluded.video_codec,
                            country = excluded.country,
                            event_tags = excluded.event_tags,
                            misc_tags = excluded.misc_tags,
                            matched_name = CASE
                                WHEN excluded.matched_name != '' THEN excluded.matched_name
                                ELSE channels.matched_name
                            END,
                            matched_source = CASE
                                WHEN excluded.matched_name != '' THEN excluded.matched_source
                                ELSE channels.matched_source
                            END,
                            matched_station_id = CASE
                                WHEN excluded.matched_name != '' THEN excluded.matched_station_id
                                ELSE channels.matched_station_id
                            END,
                            matched_call_sign = CASE
                                WHEN excluded.matched_name != '' THEN excluded.matched_call_sign
                                ELSE channels.matched_call_sign
                            END,
                            matched_logo = CASE
                                WHEN excluded.matched_name != '' THEN excluded.matched_logo
                                ELSE channels.matched_logo
                            END,
                            matched_score = CASE
                                WHEN excluded.matched_name != '' THEN excluded.matched_score
                                ELSE channels.matched_score
                            END,
                            custom_epg_id = channels.custom_epg_id,
                            is_header = excluded.is_header,
                            is_event = excluded.is_event,
                            is_raw = excluded.is_raw,
                            available_macs = excluded.available_macs,
                            alternate_ids = CASE
                                WHEN excluded.alternate_ids != '' THEN excluded.alternate_ids
                                ELSE channels.alternate_ids
                            END,
                            cmd = excluded.cmd,
                            channel_hash = excluded.channel_hash
                    ''', (
                        portal_id, channel_id, portal_name, channel_name, display_name, channel_number,
                        genre, genre_id, logo, enabled_default, "", auto_name, "", "", "",
                        resolution, video_codec, country, event_tags, misc_tags,
                        matched_name, matched_source, matched_station_id, matched_call_sign, matched_logo, matched_score,
                        is_header, is_event, is_raw,
                        available_macs, alternate_ids, cmd, channel_hash
                    ))

                sync_channel_tags(cursor, portal_id, channel_id, event_tags_list, misc_tags_list)

                if channel_id in existing_hashes:
                    channels_updated += 1
                else:
                    channels_imported += 1

            if existing_hashes:
                to_delete = [cid for cid in existing_hashes.keys() if cid not in new_channel_ids]
                if to_delete:
                    chunk_size = 500
                    for i in range(0, len(to_delete), chunk_size):
                        chunk = to_delete[i:i + chunk_size]
                        placeholders = ",".join(["?"] * len(chunk))
                        cursor.execute(
                            f"DELETE FROM channels WHERE portal_id = ? AND channel_id IN ({placeholders})",
                            [portal_id, *chunk],
                        )
                        cursor.execute(
                            f"DELETE FROM channel_tags WHERE portal_id = ? AND channel_id IN ({placeholders})",
                            [portal_id, *chunk],
                        )
                        channels_deleted += len(chunk)

            active_channels_count = len(new_channel_ids)
            total_channels += active_channels_count

            # Populate groups table from all_genres
            # Upsert groups - preserve active flag for existing groups
            for genre_id, genre_name in all_genres.items():
                channel_count = genre_channel_counts.get(str(genre_id), 0)
                cursor.execute('''
                    INSERT INTO groups (portal_id, genre_id, name, channel_count, active)
                    VALUES (?, ?, ?, ?, 1)
                    ON CONFLICT(portal_id, genre_id) DO UPDATE SET
                        name = excluded.name,
                        channel_count = excluded.channel_count
                ''', (portal_id, str(genre_id), genre_name, channel_count))

            ungrouped_count = genre_channel_counts.get("", 0)
            cursor.execute('''
                INSERT INTO groups (portal_id, genre_id, name, channel_count, active)
                VALUES (?, 'UNGROUPED', 'Ungrouped', ?, 0)
                ON CONFLICT(portal_id, genre_id) DO UPDATE SET
                    name = excluded.name,
                    channel_count = excluded.channel_count
            ''', (portal_id, ungrouped_count))

            # Auto-select groups based on settings (only if no active groups set yet)
            settings = getSettings()
            auto_group_enabled = settings.get("auto group selection enabled", "false") == "true"
            auto_group_patterns = parse_group_selection_patterns(settings.get("auto group selection patterns", ""))
            if auto_group_enabled and auto_group_patterns and not active_genres:
                matched_genres = []
                for genre_id, genre_name in all_genres.items():
                    name_upper = str(genre_name or "").upper()
                    for pattern in auto_group_patterns:
                        try:
                            if re.search(pattern, name_upper, re.IGNORECASE):
                                matched_genres.append(str(genre_id))
                                break
                        except re.error:
                            if pattern.upper() in name_upper:
                                matched_genres.append(str(genre_id))
                                break
                if matched_genres:
                    cursor.execute("UPDATE groups SET active = 0 WHERE portal_id = ?", (portal_id,))
                    placeholders = ",".join(["?"] * len(matched_genres))
                    cursor.execute(
                        f"UPDATE groups SET active = 1 WHERE portal_id = ? AND genre_id IN ({placeholders})",
                        [portal_id, *matched_genres],
                    )
                    logger.info(
                        f"Auto-selected {len(matched_genres)} groups for {portal_name} based on settings"
                    )

            stats_timestamp = datetime.utcnow().isoformat()
            cursor.execute("DELETE FROM group_stats WHERE portal_id = ?", (portal_id,))
            cursor.execute(
                """
                INSERT INTO group_stats (portal_id, portal_name, group_name, channel_count, updated_at)
                SELECT
                    ?,
                    ?,
                    CASE
                        WHEN COALESCE(NULLIF(custom_genre, ''), genre) IS NULL
                             OR COALESCE(NULLIF(custom_genre, ''), genre) = ''
                        THEN 'Ungrouped'
                        ELSE COALESCE(NULLIF(custom_genre, ''), genre)
                    END as group_name,
                    COUNT(*) as channel_count,
                    ?
                FROM channels
                WHERE portal_id = ?
                GROUP BY CASE
                    WHEN COALESCE(NULLIF(custom_genre, ''), genre) IS NULL
                         OR COALESCE(NULLIF(custom_genre, ''), genre) = ''
                    THEN 'Ungrouped'
                    ELSE COALESCE(NULLIF(custom_genre, ''), genre)
                END
                """,
                (portal_id, portal_name, stats_timestamp, portal_id),
            )

            cursor.execute(
                """
                SELECT COUNT(*) as total_groups,
                       SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) as active_groups
                FROM groups
                WHERE portal_id = ?
                """,
                (portal_id,),
            )
            row = cursor.fetchone()
            total_groups = row[0] or 0
            active_groups = row[1] or 0

            cursor.execute(
                """
                INSERT INTO portal_stats (portal_id, portal_name, total_channels, active_channels, total_groups, active_groups, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(portal_id) DO UPDATE SET
                    portal_name = excluded.portal_name,
                    total_channels = excluded.total_channels,
                    active_channels = excluded.active_channels,
                    total_groups = excluded.total_groups,
                    active_groups = excluded.active_groups,
                    updated_at = excluded.updated_at
                """,
                (
                    portal_id,
                    portal_name,
                    len(channels_to_import),
                    active_channels_count,
                    total_groups,
                    active_groups,
                    stats_timestamp,
                ),
            )

            filter_cache.clear()
            conn.commit()

            # Log summary
            mac_coverage = {}
            for ch_info in channels_to_import.values():
                num_macs = len(ch_info["available_macs"])
                mac_coverage[num_macs] = mac_coverage.get(num_macs, 0) + 1

            logger.info(
                f"Channels cached for {portal_name}: +{channels_imported} updated={channels_updated} skipped={channels_skipped} deleted={channels_deleted}"
            )
            logger.info(f"MAC coverage: {mac_coverage} (key=number of MACs, value=channel count)")
        else:
            logger.error(f"Failed to fetch channels for portal: {portal_name}")

    conn.close()
    logger.info(f"Channel cache refresh complete. Total channels: {total_channels}")
    return total_channels


def refresh_xmltv():
    global epg_refresh_status
    epg_refresh_status["is_refreshing"] = True
    epg_refresh_status["started_at"] = datetime.utcnow().isoformat()
    epg_refresh_status["last_error"] = None

    settings = getSettings()
    logger.info("Refreshing XMLTV...")

    epg_future_hours = int(settings.get("epg future hours", "24"))
    epg_past_hours = int(settings.get("epg past hours", "2"))

    portals = getPortals()

    past_cutoff = datetime.utcnow() - timedelta(hours=epg_past_hours)
    future_cutoff = datetime.utcnow() + timedelta(hours=epg_future_hours)
    past_cutoff_ts = int(past_cutoff.timestamp())
    future_cutoff_ts = int(future_cutoff.timestamp())

    enabled_rows = _fetch_enabled_channels_for_epg()
    enabled_by_portal = {}
    for row in enabled_rows:
        portal_id = row["portal_id"]
        enabled_by_portal.setdefault(portal_id, []).append(row)

    logger.info(
        f"Found {sum(len(v) for v in enabled_by_portal.values())} enabled channels across {len(enabled_by_portal)} portals"
    )

    for portal_id in enabled_by_portal:
        if portal_id not in portals:
            logger.warning(f"Portal {portal_id} not found in config, skipping")
            continue

        portal = portals[portal_id]
        if portal["enabled"] != "true":
            continue

        portal_name = portal["name"]
        fetch_epg = portal.get("fetch epg", "true") == "true"
        portal_epg_offset = int(portal.get("epg offset", 0))

        logger.info(
            "%s EPG | Portal: %s | offset: %s | channels: %s",
            "Fetching" if fetch_epg else "Skipping",
            portal_name,
            portal_epg_offset,
            len(enabled_by_portal[portal_id]),
        )

        url = portal["url"]
        macs = list(portal["macs"].keys())
        proxy = portal.get("proxy", "")

        epg = None
        if fetch_epg:
            for mac in macs:
                try:
                    token = stb.getToken(url, mac, proxy)
                    if not token:
                        logger.warning("EPG fetch: token missing for MAC %s (portal %s)", mac, portal_name)
                        continue
                    stb.getProfile(url, mac, token, proxy)
                    stb.getAllChannels(url, mac, token, proxy)
                    epg = stb.getEpg(url, mac, token, epg_future_hours, proxy)
                    if epg:
                        logger.info("Successfully fetched EPG from MAC %s", mac)
                        break
                    logger.warning("EPG fetch returned empty data for MAC %s (portal %s)", mac, portal_name)
                except Exception as e:
                    logger.error("Error fetching data for MAC %s: %s", mac, e)
                    continue

        _store_portal_epg_to_db(
            portal_id=portal_id,
            portal_name=portal_name,
            portal_url=url,
            enabled_channels=enabled_by_portal[portal_id],
            epg=epg,
            portal_epg_offset=portal_epg_offset,
            past_cutoff_ts=past_cutoff_ts,
            future_cutoff_ts=future_cutoff_ts,
        )

    xmltv_text, channel_ids = _build_xmltv_from_db(
        enabled_rows,
        portals=portals,
        past_cutoff_ts=past_cutoff_ts,
        future_cutoff_ts=future_cutoff_ts,
    )

    global cached_xmltv, last_updated
    cached_xmltv = xmltv_text
    last_updated = time.time()
    logger.debug("Generated XMLTV size: %s bytes", len(xmltv_text or ""))

    epg_map, epg_ids = _rebuild_epg_channel_map_from_db()
    _set_epg_channel_ids(epg_ids if epg_ids else set(channel_ids))
    if epg_map:
        _set_epg_channel_map(epg_map)

    save_epg_cache(cached_xmltv, last_updated, logger, EPG_CACHE_PATH)

    epg_refresh_status["is_refreshing"] = False
    epg_refresh_status["completed_at"] = datetime.utcnow().isoformat()
    logger.info("EPG refresh completed successfully.")


def refresh_xmltv_for_portal(portal_id):
    settings = getSettings()
    portal = getPortals().get(portal_id)
    if not portal:
        logger.warning("EPG refresh skipped: portal %s not found", portal_id)
        return False

    epg_future_hours = int(settings.get("epg future hours", "24"))
    epg_past_hours = int(settings.get("epg past hours", "2"))
    past_cutoff = datetime.utcnow() - timedelta(hours=epg_past_hours)
    future_cutoff = datetime.utcnow() + timedelta(hours=epg_future_hours)
    past_cutoff_ts = int(past_cutoff.timestamp())
    future_cutoff_ts = int(future_cutoff.timestamp())

    enabled_rows = _fetch_enabled_channels_for_epg(portal_id=portal_id)
    if not enabled_rows:
        logger.info("EPG refresh skipped for portal %s (no channels in active groups).", portal_id)
        return True

    portal_name = portal.get("name", portal_id)
    fetch_epg = portal.get("fetch epg", "true") == "true"
    portal_epg_offset = int(portal.get("epg offset", 0))
    url = portal.get("url", "")
    macs = list(portal.get("macs", {}).keys())
    proxy = portal.get("proxy", "")

    logger.info(
        "Refreshing EPG for portal: %s | fetch=%s | offset=%s | channels=%s",
        portal_name,
        fetch_epg,
        portal_epg_offset,
        len(enabled_rows),
    )

    epg = None
    if fetch_epg:
        for mac in macs:
            try:
                token = stb.getToken(url, mac, proxy)
                if not token:
                    logger.warning("EPG fetch: token missing for MAC %s (portal %s)", mac, portal_name)
                    continue
                stb.getProfile(url, mac, token, proxy)
                stb.getAllChannels(url, mac, token, proxy)
                epg = stb.getEpg(url, mac, token, epg_future_hours, proxy)
                if epg:
                    logger.info("Successfully fetched EPG from MAC %s (portal %s)", mac, portal_name)
                    break
                logger.warning("EPG fetch returned empty data for MAC %s (portal %s)", mac, portal_name)
            except Exception as e:
                logger.error("Error fetching EPG for MAC %s (portal %s): %s", mac, portal_name, e)
                continue
    else:
        logger.info("Skipping EPG fetch for portal %s (disabled).", portal_name)

    _store_portal_epg_to_db(
        portal_id=portal_id,
        portal_name=portal_name,
        portal_url=url,
        enabled_channels=enabled_rows,
        epg=epg,
        portal_epg_offset=portal_epg_offset,
        past_cutoff_ts=past_cutoff_ts,
        future_cutoff_ts=future_cutoff_ts,
    )

    refresh_xmltv_for_epg_ids(
        {row["custom_epg_id"] or effective_epg_name(row["custom_name"], row["auto_name"], row["name"]) for row in enabled_rows if (row.get("custom_epg_id") or effective_epg_name(row.get("custom_name"), row.get("auto_name"), row.get("name")))}
    )
    return True


def moveMac(portalId, mac):
    portals = getPortals()
    macs = portals[portalId]["macs"]
    x = macs[mac]
    del macs[mac]
    macs[mac] = x
    portals[portalId]["macs"] = macs
    savePortals(portals)


def refresh_lineup():
    global cached_lineup
    logger.info("Refreshing Lineup from database...")
    lineup = []
    
    # Read enabled channels from database
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(f'''
        SELECT
            c.portal_id as portal, c.channel_id, c.name, c.number,
            c.custom_name, c.auto_name, c.matched_name, c.custom_number
        FROM channels c
        LEFT JOIN groups g ON c.portal_id = g.portal_id AND c.genre_id = g.genre_id
        WHERE c.enabled = 1 AND {ACTIVE_GROUP_CONDITION}
        ORDER BY CAST(COALESCE(NULLIF(c.custom_number, ''), c.number) AS INTEGER)
    ''')
    
    for row in cursor.fetchall():
        portal = row['portal']
        channel_id = row['channel_id']
        channel_name = effective_display_name(
            row['custom_name'], row['matched_name'], row['auto_name'], row['name']
        )
        channel_number = row['custom_number'] if row['custom_number'] else row['number']
        
        # Use HLS URL if output format is set to HLS, otherwise use MPEG-TS
        if getSettings().get("output format", "mpegts") == "hls":
            url = f"http://{host}/hls/{portal}/{channel_id}/master.m3u8"
        else:
            url = f"http://{host}/play/{portal}/{channel_id}"
        
        lineup.append({
            "GuideNumber": str(channel_number),
            "GuideName": channel_name,
            "URL": url
        })
    
    conn.close()

    cached_lineup = lineup
    logger.info(f"Lineup refreshed with {len(lineup)} channels.")
    
    
job_manager = JobManager(
    logger=logger,
    refresh_channels_cache=refresh_channels_cache,
    run_portal_matching=run_portal_matching,
    refresh_xmltv=refresh_xmltv,
    refresh_xmltv_for_portal=refresh_xmltv_for_portal,
    refresh_epg_for_ids=refresh_xmltv_for_epg_ids,
    getSettings=getSettings,
    getPortals=getPortals,
    get_db_connection=get_db_connection,
    ACTIVE_GROUP_CONDITION=ACTIVE_GROUP_CONDITION,
    channelsdvr_match_status=channelsdvr_match_status,
    channelsdvr_match_status_lock=channelsdvr_match_status_lock,
    channels_refresh_status=channels_refresh_status,
    channels_refresh_status_lock=channels_refresh_status_lock,
    set_cached_xmltv=_set_cached_xmltv,
    effective_epg_name=effective_epg_name,
)

app.register_blueprint(create_settings_blueprint(job_manager.enqueue_epg_refresh))
app.register_blueprint(
    create_epg_blueprint(
        refresh_xmltv=refresh_xmltv,
        refresh_epg_for_ids=refresh_xmltv_for_epg_ids,
        enqueue_epg_refresh=job_manager.enqueue_epg_refresh,
        get_cached_xmltv=lambda: cached_xmltv,
        get_last_updated=lambda: last_updated,
        get_epg_refresh_status=lambda: epg_refresh_status,
        logger=logger,
        getPortals=getPortals,
        get_db_connection=get_db_connection,
        effective_epg_name=effective_epg_name,
        getSettings=getSettings,
        open_epg_source_db=_open_epg_source_db,
    )
)
app.register_blueprint(
    create_portal_blueprint(
        logger=logger,
        getPortals=getPortals,
        savePortals=savePortals,
        getSettings=getSettings,
        get_db_connection=get_db_connection,
        ACTIVE_GROUP_CONDITION=ACTIVE_GROUP_CONDITION,
        channelsdvr_match_status=channelsdvr_match_status,
        channelsdvr_match_status_lock=channelsdvr_match_status_lock,
        normalize_mac_data=normalize_mac_data,
        job_manager=job_manager,
        defaultPortal=defaultPortal,
        DB_PATH=DB_PATH,
        set_cached_xmltv=_set_cached_xmltv,
        filter_cache=filter_cache,
    )
)
app.register_blueprint(
    create_editor_blueprint(
        logger=logger,
        get_db_connection=get_db_connection,
        ACTIVE_GROUP_CONDITION=ACTIVE_GROUP_CONDITION,
        get_cached_xmltv=lambda: cached_xmltv,
        get_epg_channel_ids=_get_epg_channel_ids,
        get_epg_channel_map=_get_epg_channel_map,
        getSettings=getSettings,
        suggest_channelsdvr_matches=suggest_channelsdvr_matches,
        host=host,
        refresh_epg_for_ids=refresh_xmltv_for_epg_ids,
        refresh_lineup=refresh_lineup,
        enqueue_refresh_all=job_manager.enqueue_refresh_all,
        set_last_playlist_host=_set_last_playlist_host,
        filter_cache=filter_cache,
        effective_epg_name=effective_epg_name,
    )
)
app.register_blueprint(
    create_misc_blueprint(
        LOG_DIR=LOG_DIR,
        occupied=occupied,
        refresh_custom_sources=refresh_custom_sources,
    )
)

app.register_blueprint(
    create_hdhr_blueprint(
        host=host,
        getSettings=getSettings,
        refresh_lineup=refresh_lineup,
        get_cached_lineup=_get_cached_lineup,
    )
)
app.register_blueprint(
    create_playlist_blueprint(
        logger=logger,
        host=host,
        getSettings=getSettings,
        get_db_connection=get_db_connection,
        ACTIVE_GROUP_CONDITION=ACTIVE_GROUP_CONDITION,
        effective_display_name=effective_display_name,
        effective_epg_name=effective_epg_name,
        get_cached_playlist=_get_cached_playlist,
        set_cached_playlist=_set_cached_playlist,
        get_last_playlist_host=_get_last_playlist_host,
        set_last_playlist_host=_set_last_playlist_host,
    )
)
app.register_blueprint(
    create_streaming_blueprint(
        logger=logger,
        getPortals=getPortals,
        getSettings=getSettings,
        get_db_connection=get_db_connection,
        moveMac=moveMac,
        score_mac_for_selection=score_mac_for_selection,
        occupied=occupied,
        hls_manager=hls_manager,
    )
)


def start_epg_scheduler():
    """Start a background thread that periodically refreshes EPG data."""
    def epg_refresh_loop():
        while True:
            try:
                # Get refresh interval (env variable takes precedence over settings)
                interval_hours = get_epg_refresh_interval()
                # Convert to seconds, minimum 60 seconds
                interval_seconds = max(60, int(interval_hours * 3600))

                logger.info(f"EPG scheduler: Next refresh in {interval_hours} hours ({interval_seconds} seconds)")
                time.sleep(interval_seconds)

                logger.info("EPG scheduler: Queueing scheduled EPG refresh...")
                job_manager.enqueue_epg_refresh(reason="scheduled")
                logger.info("EPG scheduler: EPG refresh queued.")

            except Exception as e:
                logger.error(f"EPG scheduler error: {e}")
                # Wait 5 minutes before retrying on error
                time.sleep(300)

    scheduler_thread = threading.Thread(target=epg_refresh_loop, daemon=True)
    scheduler_thread.start()
    logger.info("EPG background scheduler started!")


def start_channel_scheduler():
    """Start a background thread that periodically refreshes channel data from portals."""
    def channel_refresh_loop():
        while True:
            try:
                # Get refresh interval (env variable takes precedence over settings)
                interval_hours = get_channel_refresh_interval()

                # If interval is 0, disable automatic refresh
                if interval_hours <= 0:
                    logger.info("Channel scheduler: Automatic channel refresh disabled (interval = 0)")
                    # Check again in 1 hour in case setting changes
                    time.sleep(3600)
                    continue

                # Convert to seconds, minimum 60 seconds
                interval_seconds = max(60, int(interval_hours * 3600))

                logger.info(f"Channel scheduler: Next refresh in {interval_hours} hours ({interval_seconds} seconds)")
                time.sleep(interval_seconds)

                logger.info("Channel scheduler: Queueing scheduled channel refresh...")
                total = job_manager.enqueue_refresh_all(reason="scheduled")
                logger.info("Channel scheduler: Channel refresh queued (%s portals).", total)

            except Exception as e:
                logger.error(f"Channel scheduler error: {e}")
                # Wait 5 minutes before retrying on error
                time.sleep(300)

    scheduler_thread = threading.Thread(target=channel_refresh_loop, daemon=True)
    scheduler_thread.start()
    logger.info("Channel background scheduler started!")


def start_refresh():
    # Run refresh functions in separate threads
    # First refresh channels cache, then refresh lineup and xmltv
    def refresh_all():
        # Check if database has any channels
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM channels")
        count = cursor.fetchone()[0]
        conn.close()

        # If no channels in database, refresh from portals
        if count == 0:
            logger.info("No channels in database, fetching from portals...")
            refresh_channels_cache()

        # Refresh lineup
        refresh_lineup()

        # Try to load EPG from persistent cache first
        global cached_xmltv, last_updated
        cached_xmltv, last_updated, cache_loaded = load_epg_cache(logger, EPG_CACHE_PATH)
        _set_cached_xmltv(cached_xmltv)

        if cache_loaded and is_epg_cache_valid(cached_xmltv, last_updated, get_epg_refresh_interval):
            interval = get_epg_refresh_interval()
            logger.info(f"EPG cache is valid (refresh interval: {interval}h), skipping initial fetch")
        else:
            if cache_loaded:
                logger.info("EPG cache loaded but expired, refreshing...")
            else:
                logger.info("No valid EPG cache, fetching fresh data...")
            refresh_xmltv()

    threading.Thread(target=refresh_all, daemon=True).start()

    # Start the EPG background scheduler
    start_epg_scheduler()

    # Start the channel background scheduler
    start_channel_scheduler()


if __name__ == "__main__":
    loadConfig()
    
    # Initialize the database
    init_db(getPortals, logger)

    # Start the refresh thread before the server
    start_refresh()
    
    # Start HLS stream manager monitoring
    hls_manager.start_monitoring()
    
    # Register cleanup handler for HLS streams
    atexit.register(hls_manager.cleanup_all)

    # Start the server
    if "TERM_PROGRAM" in os.environ.keys() and os.environ["TERM_PROGRAM"] == "vscode":
        app.run(host=BIND_HOST, port=PORT, debug=True)
    else:
        waitress.serve(app, host=BIND_HOST, port=PORT, _quiet=True, threads=24)
