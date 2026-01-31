#!/usr/bin/env python3
import os
import shutil
import time
import subprocess
import re
import unicodedata
import json
import gzip
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from macreplay.config import (
    LOG_DIR,
    CONFIG_PATH,
    DB_PATH,
    EPG_CACHE_PATH,
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
    "    WHERE g3.portal = c.portal AND g3.genre_id = 'UNGROUPED' AND g3.active = 1"
    "  )"
    " )"
    " OR NOT EXISTS ("
    "  SELECT 1 FROM groups g2 WHERE g2.portal = c.portal AND g2.active = 1"
    " )"
    ")"
)

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


def _set_cached_xmltv(value):
    global cached_xmltv
    cached_xmltv = value


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
    r"\bSAT\b",
    r"\bBAR\b",
]

channelsdvr_cache = {}
channelsdvr_cache_lock = threading.Lock()
channelsdvr_match_status = {}
channelsdvr_match_status_lock = threading.Lock()
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
    return {
        "countries": parse_country_codes(settings.get("tag country codes"), DEFAULT_COUNTRY_CODES),
        "resolution": ensure_resolution_patterns(parse_labeled_patterns(settings.get("tag resolution patterns"), DEFAULT_RESOLUTION_PATTERNS)),
        "video": parse_labeled_patterns(settings.get("tag video codec patterns"), DEFAULT_VIDEO_CODEC_PATTERNS),
        "audio": parse_labeled_patterns(settings.get("tag audio patterns"), DEFAULT_AUDIO_TAG_PATTERNS),
        "event": parse_event_patterns(settings.get("tag event patterns"), DEFAULT_EVENT_PATTERNS),
        "misc": parse_list_patterns(settings.get("tag misc patterns"), DEFAULT_MISC_PATTERNS),
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
        LEFT JOIN groups g ON c.portal = g.portal AND c.genre_id = g.genre_id
        WHERE c.portal = ? AND {ACTIVE_GROUP_CONDITION}
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
                matched_call_sign = ?, matched_logo = ?, matched_score = ?
            WHERE portal = ? AND channel_id = ?
        """, (
            tag_info.get("matched_name", ""),
            tag_info.get("matched_source", ""),
            tag_info.get("matched_station_id", ""),
            tag_info.get("matched_call_sign", ""),
            tag_info.get("matched_logo", ""),
            tag_info.get("matched_score", ""),
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
            "audio_tags": "",
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
            break

    video_codec = ""
    for label, pattern in tag_config["video"]:
        if re.search(pattern, name_upper):
            video_codec = label
            break
    if video_codec and video_codec != "HEVC":
        video_codec = ""

    audio_tags = []
    for label, pattern in tag_config["audio"]:
        if re.search(pattern, name_upper):
            audio_tags.append(label)

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
        if re.search(pattern, name_upper):
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
        for _label, pattern in tag_config["audio"]:
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
    for _label, pattern in tag_config["audio"]:
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
        audio_tags = []
        event_tags = []
        misc_tags = []
        is_event = 0
        is_raw = 0

    return {
        "clean_name": cleaned,
        "resolution": resolution,
        "video_codec": video_codec,
        "country": country,
        "audio_tags": ",".join(audio_tags),
        "event_tags": ",".join(event_tags),
        "misc_tags": ",".join(misc_tags),
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


def effective_channel_name(custom_name, auto_name, name):
    """Return preferred display name: custom > auto > original."""
    return custom_name or auto_name or name


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
            cursor.execute("SELECT genre_id FROM groups WHERE portal = ? AND active = 1", (portal_id,))
            active_genres = {str(row[0]) for row in cursor.fetchall() if row[0]}
        except Exception as e:
            logger.debug(f"Could not load active genres for portal {portal_name}: {e}")

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
                        "available_macs": list(combined_macs),
                        "alternate_ids": alternate_ids
                    }

                    merged_count += len(channel_list) - 1
                    logger.debug(f"Auto-merged '{channel_name}': {primary_id} + alternates {alternate_ids}")

            if merged_count > 0:
                logger.info(f"Auto-merged {merged_count} duplicate channels by name for {portal_name}")

            channels_imported = 0
            for channel_id, channel_info in channels_to_import.items():
                channel = channel_info["data"]
                available_macs = ",".join(channel_info["available_macs"])
                alternate_ids = ",".join(channel_info["alternate_ids"])

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
                audio_tags = tag_info["audio_tags"]
                event_tags = tag_info["event_tags"]
                misc_tags = tag_info["misc_tags"]
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
                cursor.execute('''
                    INSERT INTO channels (
                        portal, channel_id, portal_name, name, number, genre, genre_id, logo,
                        enabled, custom_name, auto_name, custom_number, custom_genre,
                        custom_epg_id, fallback_channel, resolution, video_codec, country,
                            audio_tags, event_tags, misc_tags, matched_name, matched_source,
                            matched_station_id, matched_call_sign, matched_logo, matched_score,
                            is_header, is_event, is_raw, available_macs, alternate_ids, cmd
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(portal, channel_id) DO UPDATE SET
                            portal_name = excluded.portal_name,
                            name = excluded.name,
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
                            audio_tags = excluded.audio_tags,
                            event_tags = excluded.event_tags,
                            misc_tags = excluded.misc_tags,
                            matched_name = excluded.matched_name,
                            matched_source = excluded.matched_source,
                            matched_station_id = excluded.matched_station_id,
                            matched_call_sign = excluded.matched_call_sign,
                            matched_logo = excluded.matched_logo,
                            matched_score = excluded.matched_score,
                            is_header = excluded.is_header,
                            is_event = excluded.is_event,
                            is_raw = excluded.is_raw,
                            available_macs = excluded.available_macs,
                            alternate_ids = CASE
                                WHEN excluded.alternate_ids != '' THEN excluded.alternate_ids
                                ELSE channels.alternate_ids
                            END,
                            cmd = excluded.cmd
                    ''', (
                        portal_id, channel_id, portal_name, channel_name, channel_number,
                        genre, genre_id, logo, 0, "", auto_name, "", "", "",
                        "", resolution, video_codec, country, audio_tags, event_tags, misc_tags,
                        matched_name, matched_source, matched_station_id, matched_call_sign, matched_logo, matched_score,
                        is_header, is_event, is_raw,
                        available_macs, alternate_ids, cmd
                    ))

                channels_imported += 1
                total_channels += 1

            # Populate groups table from all_genres
            # Count channels per genre (use deduplicated channels)
            genre_channel_counts = {}
            for ch_info in channels_to_import.values():
                g_id = str(ch_info["data"].get("tv_genre_id", ""))
                genre_channel_counts[g_id] = genre_channel_counts.get(g_id, 0) + 1

            # Upsert groups - preserve active flag for existing groups
            for genre_id, genre_name in all_genres.items():
                channel_count = genre_channel_counts.get(str(genre_id), 0)
                cursor.execute('''
                    INSERT INTO groups (portal, genre_id, name, channel_count, active)
                    VALUES (?, ?, ?, ?, 1)
                    ON CONFLICT(portal, genre_id) DO UPDATE SET
                        name = excluded.name,
                        channel_count = excluded.channel_count
                ''', (portal_id, str(genre_id), genre_name, channel_count))

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
                    cursor.execute("UPDATE groups SET active = 0 WHERE portal = ?", (portal_id,))
                    placeholders = ",".join(["?"] * len(matched_genres))
                    cursor.execute(
                        f"UPDATE groups SET active = 1 WHERE portal = ? AND genre_id IN ({placeholders})",
                        [portal_id, *matched_genres],
                    )
                    logger.info(
                        f"Auto-selected {len(matched_genres)} groups for {portal_name} based on settings"
                    )

            conn.commit()

            # Log summary
            mac_coverage = {}
            for ch_info in channels_to_import.values():
                num_macs = len(ch_info["available_macs"])
                mac_coverage[num_macs] = mac_coverage.get(num_macs, 0) + 1

            logger.info(f"Successfully cached {channels_imported} channels for {portal_name}")
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

    user_dir = os.path.expanduser("~")
    cache_dir = os.path.join(user_dir, "Evilvir.us")
    os.makedirs(cache_dir, exist_ok=True)
    cache_file = os.path.join(cache_dir, "MacReplayEPG.xml")

    past_cutoff = datetime.utcnow() - timedelta(hours=epg_past_hours)
    past_cutoff_str = past_cutoff.strftime("%Y%m%d%H%M%S") + " +0000"

    cached_programmes = []
    if os.path.exists(cache_file):
        try:
            tree = ET.parse(cache_file)
            root = tree.getroot()
            for programme in root.findall("programme"):
                stop_attr = programme.get("stop")
                if stop_attr:
                    try:
                        stop_time = datetime.strptime(stop_attr.split(" ")[0], "%Y%m%d%H%M%S")
                        if stop_time >= past_cutoff:
                            cached_programmes.append(
                                ET.tostring(programme, encoding="unicode")
                            )
                    except ValueError:
                        logger.warning(
                            f"Invalid stop time format in cached programme: {stop_attr}. Skipping."
                        )
            logger.info("Loaded existing programme data from cache.")
        except Exception as e:
            logger.error(f"Failed to load cache file: {e}")

    channels_xml = ET.Element("tv")
    programmes = ET.Element("tv")
    portals = getPortals()

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT
            c.portal, c.channel_id, c.name, c.number, c.logo,
            c.custom_name, c.auto_name, c.custom_number, c.custom_epg_id
        FROM channels c
        LEFT JOIN groups g ON c.portal = g.portal AND c.genre_id = g.genre_id
        WHERE c.enabled = 1 AND {ACTIVE_GROUP_CONDITION}
        """
    )

    enabled_by_portal = {}
    for row in cursor.fetchall():
        portal_id = row["portal"]
        if portal_id not in enabled_by_portal:
            enabled_by_portal[portal_id] = []
        enabled_by_portal[portal_id].append(
            {
                "channel_id": row["channel_id"],
                "name": row["name"],
                "number": row["number"],
                "logo": row["logo"],
                "custom_name": row["custom_name"],
                "auto_name": row["auto_name"],
                "custom_number": row["custom_number"],
                "custom_epg_id": row["custom_epg_id"],
            }
        )
    conn.close()

    logger.info(
        f"Found {sum(len(v) for v in enabled_by_portal.values())} enabled channels across {len(enabled_by_portal)} portals"
    )

    seen_channel_ids = set()

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

        if fetch_epg:
            logger.info(
                f"Fetching EPG | Portal: {portal_name} | offset: {portal_epg_offset} | channels: {len(enabled_by_portal[portal_id])}"
            )
        else:
            logger.info(
                f"Skipping EPG fetch for Portal: {portal_name} (disabled) | channels: {len(enabled_by_portal[portal_id])}"
            )

        url = portal["url"]
        macs = list(portal["macs"].keys())
        proxy = portal.get("proxy", "")

        epg = None
        if fetch_epg:
            for mac in macs:
                try:
                    token = stb.getToken(url, mac, proxy)
                    stb.getProfile(url, mac, token, proxy)
                    stb.getAllChannels(url, mac, token, proxy)
                    epg = stb.getEpg(url, mac, token, epg_future_hours, proxy)
                    if epg:
                        logger.info(f"Successfully fetched EPG from MAC {mac}")
                        break
                except Exception as e:
                    logger.error(f"Error fetching data for MAC {mac}: {e}")
                    continue

            if not epg:
                logger.warning(
                    f"Could not fetch EPG for portal {portal_name}, creating dummy entries"
                )

        for ch in enabled_by_portal[portal_id]:
            try:
                channelId = str(ch["channel_id"])
                channelName = effective_channel_name(
                    ch["custom_name"], ch["auto_name"], ch["name"]
                )
                channelNumber = (
                    ch["custom_number"] if ch["custom_number"] else str(ch["number"])
                )
                epgId = ch["custom_epg_id"] if ch["custom_epg_id"] else channelName
                channelLogo = ch["logo"] or ""

                if epgId in seen_channel_ids:
                    logger.debug(
                        f"Skipping duplicate channel: {channelName} (epgId: {epgId})"
                    )
                    continue
                seen_channel_ids.add(epgId)

                channelEle = ET.SubElement(channels_xml, "channel", id=epgId)
                ET.SubElement(channelEle, "display-name").text = channelName
                if channelLogo:
                    ET.SubElement(channelEle, "icon", src=channelLogo)

                if not epg or channelId not in epg or not epg.get(channelId):
                    logger.debug(
                        f"No EPG data found for channel {channelName} (ID: {channelId}), creating dummy entry"
                    )
                    start_time = datetime.utcnow().replace(
                        minute=0, second=0, microsecond=0
                    )
                    stop_time = start_time + timedelta(hours=24)
                    start = start_time.strftime("%Y%m%d%H%M%S") + " +0000"
                    stop = stop_time.strftime("%Y%m%d%H%M%S") + " +0000"
                    programmeEle = ET.SubElement(
                        programmes,
                        "programme",
                        start=start,
                        stop=stop,
                        channel=epgId,
                    )
                    ET.SubElement(programmeEle, "title").text = channelName
                    ET.SubElement(programmeEle, "desc").text = channelName
                else:
                    for p in epg.get(channelId):
                        try:
                            start_time = datetime.utcfromtimestamp(
                                p.get("start_timestamp")
                            ) + timedelta(hours=portal_epg_offset)
                            stop_time = datetime.utcfromtimestamp(
                                p.get("stop_timestamp")
                            ) + timedelta(hours=portal_epg_offset)
                            start = start_time.strftime("%Y%m%d%H%M%S") + " +0000"
                            stop = stop_time.strftime("%Y%m%d%H%M%S") + " +0000"
                            if start <= past_cutoff_str:
                                continue
                            programmeEle = ET.SubElement(
                                programmes,
                                "programme",
                                start=start,
                                stop=stop,
                                channel=epgId,
                            )
                            ET.SubElement(programmeEle, "title").text = p.get("name")
                            ET.SubElement(programmeEle, "desc").text = p.get("descr")
                        except Exception as e:
                            logger.error(
                                f"Error processing programme for channel {channelName} (ID: {channelId}): {e}"
                            )
            except Exception as e:
                logger.error(f"Error processing channel {ch}: {e}")

    xmltv = channels_xml
    seen_programmes = set()

    for programme in programmes.iter("programme"):
        prog_key = (
            programme.get("channel"),
            programme.get("start"),
            programme.get("stop"),
        )
        if prog_key not in seen_programmes:
            seen_programmes.add(prog_key)
            xmltv.append(programme)

    for cached in cached_programmes:
        prog_elem = ET.fromstring(cached)
        prog_key = (
            prog_elem.get("channel"),
            prog_elem.get("start"),
            prog_elem.get("stop"),
        )
        if prog_key not in seen_programmes:
            seen_programmes.add(prog_key)
            xmltv.append(prog_elem)

    logger.info(f"EPG: {len(seen_programmes)} unique programmes after deduplication")

    rough_string = ET.tostring(xmltv, encoding="unicode")
    reparsed = minidom.parseString(rough_string)
    formatted_xmltv = "\n".join(
        [
            line
            for line in reparsed.toprettyxml(indent="  ").splitlines()
            if line.strip()
        ]
    )

    with open(cache_file, "w", encoding="utf-8") as f:
        f.write(formatted_xmltv)
    logger.info("XMLTV cache updated.")

    global cached_xmltv, last_updated
    cached_xmltv = formatted_xmltv
    last_updated = time.time()
    logger.debug(f"Generated XMLTV: {formatted_xmltv}")

    save_epg_cache(cached_xmltv, last_updated, logger, EPG_CACHE_PATH)

    epg_refresh_status["is_refreshing"] = False
    epg_refresh_status["completed_at"] = datetime.utcnow().isoformat()
    logger.info("EPG refresh completed successfully.")


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
            c.portal, c.channel_id, c.name, c.number,
            c.custom_name, c.auto_name, c.custom_number
        FROM channels c
        LEFT JOIN groups g ON c.portal = g.portal AND c.genre_id = g.genre_id
        WHERE c.enabled = 1 AND {ACTIVE_GROUP_CONDITION}
        ORDER BY CAST(COALESCE(NULLIF(c.custom_number, ''), c.number) AS INTEGER)
    ''')
    
    for row in cursor.fetchall():
        portal = row['portal']
        channel_id = row['channel_id']
        channel_name = effective_channel_name(row['custom_name'], row['auto_name'], row['name'])
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
    
    
app.register_blueprint(create_settings_blueprint(refresh_xmltv))
app.register_blueprint(
    create_epg_blueprint(
        refresh_xmltv=refresh_xmltv,
        get_cached_xmltv=lambda: cached_xmltv,
        get_last_updated=lambda: last_updated,
        get_epg_refresh_status=lambda: epg_refresh_status,
        logger=logger,
        getPortals=getPortals,
        get_db_connection=get_db_connection,
        effective_channel_name=effective_channel_name,
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
        normalize_mac_data=normalize_mac_data,
        refresh_channels_cache=refresh_channels_cache,
        run_portal_matching=run_portal_matching,
        channelsdvr_match_status=channelsdvr_match_status,
        channelsdvr_match_status_lock=channelsdvr_match_status_lock,
        defaultPortal=defaultPortal,
        DB_PATH=DB_PATH,
        set_cached_xmltv=_set_cached_xmltv,
    )
)
app.register_blueprint(
    create_editor_blueprint(
        logger=logger,
        get_db_connection=get_db_connection,
        ACTIVE_GROUP_CONDITION=ACTIVE_GROUP_CONDITION,
        get_cached_xmltv=lambda: cached_xmltv,
        host=host,
        refresh_xmltv=refresh_xmltv,
        refresh_lineup=refresh_lineup,
        refresh_channels_cache=refresh_channels_cache,
        set_last_playlist_host=_set_last_playlist_host,
    )
)
app.register_blueprint(create_misc_blueprint(LOG_DIR=LOG_DIR, occupied=occupied))

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
        effective_channel_name=effective_channel_name,
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

                logger.info("EPG scheduler: Starting scheduled EPG refresh...")
                refresh_xmltv()
                logger.info("EPG scheduler: EPG refresh completed!")

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

                logger.info("Channel scheduler: Starting scheduled channel refresh...")
                total = refresh_channels_cache()
                logger.info(f"Channel scheduler: Channel refresh completed! {total} channels cached.")

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
