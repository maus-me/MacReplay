#!/usr/bin/env python3
import sys
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
from threading import Thread
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import time
logger = logging.getLogger("MacReplay")
logger.setLevel(logging.INFO)
logFormat = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

# ----------------------------
# Docker / Volume friendly paths
# ----------------------------
DATA_DIR = os.getenv("DATA_DIR", "/app/data")
LOG_DIR  = os.getenv("LOG_DIR", "/app/logs")

# CONFIG: allow absolute config file path from env
configFile = os.getenv("CONFIG", os.path.join(DATA_DIR, "MacReplay.json"))

# DB: allow absolute db path from env
dbPath = os.getenv("DB_PATH", os.path.join(DATA_DIR, "channels.db"))

# EPG Cache: allow absolute path from env
epgCachePath = os.getenv("EPG_CACHE_PATH", os.path.join(DATA_DIR, "epg_cache.xml"))

# Group filter: include ungrouped channels only when no groups are active for a portal.
ACTIVE_GROUP_CONDITION = (
    "(g.active = 1 OR NOT EXISTS ("
    "SELECT 1 FROM groups g2 WHERE g2.portal = c.portal AND g2.active = 1"
    "))"
)

# EPG Refresh Interval: can be set via env (in hours), overrides settings if set
EPG_REFRESH_INTERVAL_ENV = os.getenv("EPG_REFRESH_INTERVAL", None)

# Channel Refresh Interval: can be set via env (in hours), overrides settings if set
# Set to 0 to disable automatic channel refresh
CHANNEL_REFRESH_INTERVAL_ENV = os.getenv("CHANNEL_REFRESH_INTERVAL", None)

# Ensure directories exist
os.makedirs(os.path.dirname(configFile), exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

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


import flask
from flask import Flask, jsonify
import stb
import json
import subprocess
import uuid
import xml.etree.cElementTree as ET
from flask import (
    Flask,
    render_template,
    redirect,
    request,
    Response,
    make_response,
    flash,
    send_file,
)
from datetime import datetime, timezone
from functools import wraps
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
logger.info(f"Using config file: {configFile}")
logger.info(f"Using database file: {dbPath}")


occupied = {}
config = {}
cached_lineup = []
cached_playlist = None
last_playlist_host = None
cached_xmltv = None
last_updated = 0


def save_epg_cache():
    """Save EPG cache to file for persistence."""
    global cached_xmltv, last_updated
    if cached_xmltv is None:
        return
    try:
        # Save XMLTV content
        with open(epgCachePath, 'w', encoding='utf-8') as f:
            f.write(cached_xmltv)
        # Save metadata (timestamp) in a separate small file
        meta_path = epgCachePath + '.meta'
        with open(meta_path, 'w') as f:
            f.write(str(last_updated))
        logger.info(f"EPG cache saved to {epgCachePath}")
    except Exception as e:
        logger.error(f"Error saving EPG cache: {e}")


def load_epg_cache():
    """Load EPG cache from file if it exists and is valid."""
    global cached_xmltv, last_updated
    try:
        if not os.path.exists(epgCachePath):
            logger.info("No EPG cache file found")
            return False

        # Load metadata first to check age
        meta_path = epgCachePath + '.meta'
        if os.path.exists(meta_path):
            with open(meta_path, 'r') as f:
                last_updated = float(f.read().strip())
        else:
            # Use file modification time as fallback
            last_updated = os.path.getmtime(epgCachePath)

        # Load XMLTV content
        with open(epgCachePath, 'r', encoding='utf-8') as f:
            cached_xmltv = f.read()

        cache_age_hours = (time.time() - last_updated) / 3600
        logger.info(f"EPG cache loaded from {epgCachePath} (age: {cache_age_hours:.2f} hours)")
        return True
    except Exception as e:
        logger.error(f"Error loading EPG cache: {e}")
        return False


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


def is_epg_cache_valid():
    """Check if EPG cache is still valid based on refresh interval."""
    global last_updated
    if cached_xmltv is None or last_updated == 0:
        return False
    interval_hours = get_epg_refresh_interval()
    age_hours = (time.time() - last_updated) / 3600
    return age_hours < interval_hours


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







defaultSettings = {
    "stream method": "ffmpeg",
    "output format": "mpegts",
    "ffmpeg command": "-re -http_proxy <proxy> -timeout <timeout> -i <url> -map 0 -codec copy -f mpegts -flush_packets 0 -fflags +nobuffer -flags low_delay -strict experimental -analyzeduration 0 -probesize 32 -copyts -threads 12 pipe:",
    "hls segment type": "mpegts",
    "hls segment duration": "4",
    "hls playlist size": "6",
    "ffmpeg timeout": "5",
    "epg refresh interval": "0.5",
    "channel refresh interval": "24",
    "epg future hours": "24",
    "epg past hours": "2",
    "test streams": "true",
    "try all macs": "true",
    "parallel mac probing": "false",
    "parallel mac workers": "3",
    "use channel genres": "true",
    "use channel numbers": "true",
    "sort playlist by channel genre": "false",
    "sort playlist by channel number": "true",
    "sort playlist by channel name": "false",
    "enable security": "false",
    "username": "admin",
    "password": "12345",
    "enable hdhr": "true",
    "hdhr name": "MacReplay",
    "hdhr id": str(uuid.uuid4().hex),
    "hdhr tuners": "10",
    "tag country codes": "AF,AL,ALB,AR,AT,AU,BE,BG,BR,CA,CH,CN,CZ,DE,DK,EE,ES,FI,FR,GR,HK,HR,HU,IE,IL,IN,IR,IS,IT,JO,JP,KR,KW,LAT,LB,LT,LU,LV,MA,MK,MO,MX,MXC,NL,NO,NZ,PL,PT,RO,RS,RU,SA,SE,SG,SI,SK,TR,UA,UK,US,USA",
    "tag resolution patterns": "8K=\\b(8K|4320P)\\b\nUHD=\\b(UHD|ULTRA|4K\\+?|2160P)\\b\nFHD=\\b(FHD|1080P)\\b\nHD=\\b(HD|720P)\\b\nSD=\\b(SD|576P|480P)\\b",
    "tag video codec patterns": "AV1=\\bAV1\\b\nVP9=\\bVP9\\b\nHEVC=\\b(HEVC|H\\.?265|H265)\\b\nH264=\\b(H\\.?264|H264|AVC)\\b\nMPEG2=\\bMPEG[- ]?2\\b",
    "tag audio patterns": "AAC=\\bAAC\\b\nAC3=\\bAC3\\b\nEAC3=\\bEAC3\\b\nDDP=\\b(DD\\+|DDP)\\b\nDD=\\bDD\\b\nDTS=\\bDTS\\b\nMP3=\\bMP3\\b\nFLAC=\\bFLAC\\b\nDOLBY=\\bDOLBY\\b\nATMOS=\\bATMOS\\b\n7.1=\\b7\\.1\\b\n5.1=\\b5\\.1\\b\n2.0=\\b2\\.0\\b",
    "tag event patterns": "\\bPPV\\b\n\\bEVENT\\b\n\\bLIVE EVENT\\b\n\\bLIVE-EVENT\\b\n\\bNO EVENT\\b\n\\bNO EVENT STREAMING\\b\n\\bMATCH TIME\\b",
    "tag misc patterns": "\\bSAT\\b\n\\bBAR\\b",
    "tag header patterns": "^\\s*([#*✦┃★]{2,})\\s*(.+?)\\s*\\1\\s*$",
    "channelsdvr enabled": "false",
    "channelsdvr db path": "/app/data/channelidentifiarr.db",
    "channelsdvr match threshold": "0.72",
    "channelsdvr debug": "false",
    "channelsdvr include lineup channels": "false",
    "channelsdvr cache enabled": "true",
    "channelsdvr cache dir": "/app/data/channelsdvr_cache",
}

defaultPortal = {
    "enabled": "true",
    "name": "",
    "url": "",
    "macs": {},
    "streams per mac": "1",
    "epg offset": "0",
    "proxy": "",
    "fetch epg": "true",
    "selected_genres": [],  # Liste der Genre-IDs zum Importieren (leer = alle)
    "auto normalize names": "false",
}


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
    for idx, (norm, name) in enumerate(normalized):
        if not norm:
            continue
        exact.setdefault(norm, name)
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
        normalized = [(row[0], row[1]) for row in normalized if len(row) == 2]
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
            "normalized": normalized,
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


def load_channelsdvr_names_for_country(country, db_path, include_lineup_channels=False):
    country = normalize_market_country(country)
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.execute("PRAGMA busy_timeout = 5000;")
    cursor = conn.cursor()
    if include_lineup_channels:
        query = """
            SELECT DISTINCT s.name
            FROM stations s
            JOIN station_lineups sl ON sl.station_id = s.station_id
            JOIN lineup_markets lm ON lm.lineup_id = sl.lineup_id
            WHERE lm.country = ?
            UNION
            SELECT DISTINCT lc.station_name
            FROM lineup_channels lc
            JOIN lineup_markets lm ON lm.lineup_id = lc.lineup_id
            WHERE lm.country = ?
        """
        cursor.execute(query, (country, country))
    else:
        query = """
            SELECT DISTINCT s.name
            FROM stations s
            JOIN station_lineups sl ON sl.station_id = s.station_id
            JOIN lineup_markets lm ON lm.lineup_id = sl.lineup_id
            WHERE lm.country = ?
        """
        cursor.execute(query, (country,))
    names = [row[0] for row in cursor.fetchall() if row[0]]
    conn.close()
    return names


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

    names = load_channelsdvr_names_for_country(country, db_path, include_lineup_channels)
    normalized = []

    for name in names:
        norm = normalize_match_name(name)
        if not norm:
            continue
        normalized.append((norm, name))

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
        return ""
    if settings.get("channelsdvr enabled", "false") != "true":
        return ""
    db_path = settings.get("channelsdvr db path", "").strip()
    if not db_path or not os.path.exists(db_path):
        return ""
    try:
        threshold = float(settings.get("channelsdvr match threshold", "0.72"))
    except ValueError:
        threshold = 0.72
    include_lineup_channels = settings.get("channelsdvr include lineup channels", "false") == "true"

    norm = normalize_match_name(raw_name)
    if not norm:
        return ""

    country_iso3 = normalize_market_country(country)
    norm_tokens = [t for t in norm.split() if t not in {country.upper(), country_iso3}]
    norm = " ".join(norm_tokens).strip()
    if not norm:
        return ""

    cache_entry = get_channelsdvr_cache_for_country(country, db_path, include_lineup_channels)
    exact = cache_entry["exact"]
    if norm in exact:
        if settings.get("channelsdvr debug", "false") == "true":
            logger.info(f"ChannelsDVR match (exact): {raw_name} -> {exact[norm]}")
        return exact[norm]

    tokens = norm.split()
    if len(tokens) < 2:
        return ""

    candidate_indices = set()
    for token in tokens:
        candidate_indices.update(cache_entry["token_index"].get(token, set()))

    best_score = 0.0
    best_name = ""
    token_set = set(tokens)
    for idx in candidate_indices:
        cand_norm, cand_name = cache_entry["normalized"][idx]
        cand_tokens = set(cand_norm.split())
        if not cand_tokens:
            continue
        score = len(token_set & cand_tokens) / max(len(token_set), len(cand_tokens))
        if score > best_score:
            best_score = score
            best_name = cand_name

    if best_score >= threshold:
        if settings.get("channelsdvr debug", "false") == "true":
            logger.info(f"ChannelsDVR match ({best_score:.2f}): {raw_name} -> {best_name}")
        return best_name
    if settings.get("channelsdvr debug", "false") == "true":
        logger.info(
            f"ChannelsDVR no match ({best_score:.2f}): {raw_name} (country {country} -> {country_iso3})"
        )
    return ""


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
    if settings and allow_match and country and not is_header:
        canonical_name = match_channelsdvr_name(raw_name, country, settings)

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


def loadConfig():
    try:
        with open(configFile) as f:
            data = json.load(f)
    except:
        logger.warning("No existing config found. Creating a new one")
        data = {}

    data.setdefault("portals", {})
    data.setdefault("settings", {})

    settings = data["settings"]
    settingsOut = {}

    for setting, default in defaultSettings.items():
        value = settings.get(setting)
        if not value or type(default) != type(value):
            value = default
        settingsOut[setting] = value

    data["settings"] = settingsOut

    portals = data["portals"]
    portalsOut = {}

    for portal in portals:
        portalsOut[portal] = {}
        for setting, default in defaultPortal.items():
            value = portals[portal].get(setting)
            if not value or type(default) != type(value):
                value = default
            portalsOut[portal][setting] = value

    data["portals"] = portalsOut

    with open(configFile, "w") as f:
        json.dump(data, f, indent=4)

    return data


def getPortals():
    return config["portals"]


def savePortals(portals):
    with open(configFile, "w") as f:
        config["portals"] = portals
        json.dump(config, f, indent=4)


def getSettings():
    return config["settings"]


def saveSettings(settings):
    with open(configFile, "w") as f:
        config["settings"] = settings
        json.dump(config, f, indent=4)


def get_db_connection():
    """Get a database connection."""
    conn = sqlite3.connect(dbPath)
    conn.execute("PRAGMA busy_timeout = 5000;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Initialize the database and create tables if they don't exist."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            portal TEXT NOT NULL,
            channel_id TEXT NOT NULL,
            portal_name TEXT,
            name TEXT,
            number TEXT,
            genre TEXT,
            genre_id TEXT,
            logo TEXT,
            enabled INTEGER DEFAULT 0,
            custom_name TEXT,
            auto_name TEXT,
            custom_number TEXT,
            custom_genre TEXT,
            custom_epg_id TEXT,
            fallback_channel TEXT,
            resolution TEXT,
            video_codec TEXT,
            country TEXT,
            audio_tags TEXT,
            event_tags TEXT,
            misc_tags TEXT,
            matched_name TEXT,
            matched_source TEXT,
            is_header INTEGER DEFAULT 0,
            is_event INTEGER DEFAULT 0,
            is_raw INTEGER DEFAULT 0,
            PRIMARY KEY (portal, channel_id)
        )
    ''')

    # Add genre_id column if it doesn't exist (migration for existing databases)
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN genre_id TEXT")
    except:
        pass  # Column already exists

    # Add available_macs column to track which MACs can access the channel (comma-separated)
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN available_macs TEXT")
    except:
        pass  # Column already exists

    # Add alternate_ids column to store alternative channel IDs (comma-separated)
    # Used for merged channels - if primary ID fails, try alternates
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN alternate_ids TEXT")
    except:
        pass  # Column already exists

    # Add cmd column to cache the stream command URL (avoids fetching all channels on every stream)
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN cmd TEXT")
    except:
        pass  # Column already exists

    # Add auto_name column for auto-normalized channel names
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN auto_name TEXT")
    except:
        pass  # Column already exists

    # Add tag columns for extraction
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN resolution TEXT")
    except:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN video_codec TEXT")
    except:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN country TEXT")
    except:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN audio_tags TEXT")
    except:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN event_tags TEXT")
    except:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN misc_tags TEXT")
    except:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN matched_name TEXT")
    except:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN matched_source TEXT")
    except:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN is_header INTEGER DEFAULT 0")
    except:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN is_event INTEGER DEFAULT 0")
    except:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN is_raw INTEGER DEFAULT 0")
    except:
        pass  # Column already exists

    # Create indexes for better query performance
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_channels_enabled 
        ON channels(enabled)
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_channels_name 
        ON channels(name)
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_channels_portal
        ON channels(portal)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_channels_resolution
        ON channels(resolution)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_channels_video_codec
        ON channels(video_codec)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_channels_country
        ON channels(country)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_channels_is_event
        ON channels(is_event)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_channels_is_raw
        ON channels(is_raw)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_channels_is_header
        ON channels(is_header)
    ''')

    # Create groups table for genre/group management
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS groups (
            portal TEXT NOT NULL,
            genre_id TEXT NOT NULL,
            name TEXT,
            channel_count INTEGER DEFAULT 0,
            active INTEGER DEFAULT 1,
            PRIMARY KEY (portal, genre_id)
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_groups_active
        ON groups(portal, active)
    ''')

    conn.commit()

    # Migration: populate groups table from existing channels data
    cursor.execute("SELECT COUNT(*) FROM groups")
    if cursor.fetchone()[0] == 0:
        logger.info("Migrating: populating groups table from existing channels...")
        cursor.execute('''
            INSERT OR IGNORE INTO groups (portal, genre_id, name, channel_count, active)
            SELECT portal, genre_id, genre, COUNT(*) as cnt, 1
            FROM channels
            WHERE genre_id IS NOT NULL AND genre_id != ''
            GROUP BY portal, genre_id
        ''')
        # Set active flag based on selected_genres from JSON config
        try:
            portals = getPortals()
            for portal_id, portal in portals.items():
                selected_genres = portal.get("selected_genres", [])
                if selected_genres:
                    selected_genres = [str(g) for g in selected_genres]
                    # Deactivate all groups for this portal first
                    cursor.execute("UPDATE groups SET active = 0 WHERE portal = ?", [portal_id])
                    # Activate only selected groups
                    for genre_id in selected_genres:
                        cursor.execute("UPDATE groups SET active = 1 WHERE portal = ? AND genre_id = ?",
                                     [portal_id, genre_id])
                    logger.info(f"Migrated genre selection for portal {portal.get('name', portal_id)}: {len(selected_genres)} active groups")
        except Exception as e:
            logger.error(f"Error migrating genre selections: {e}")

        conn.commit()
        logger.info("Groups migration complete")

    conn.close()
    logger.info("Database initialized successfully")


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
                allow_match = True
                if active_genres:
                    allow_match = str(genre_id) in active_genres
                tag_info = extract_channel_tags(channel_name, tag_config, getSettings(), allow_match=allow_match)
                auto_name = tag_info["clean_name"] if portal_auto_normalize and tag_info["clean_name"] else ""
                resolution = tag_info["resolution"]
                video_codec = tag_info["video_codec"]
                country = tag_info["country"]
                audio_tags = tag_info["audio_tags"]
                event_tags = tag_info["event_tags"]
                misc_tags = tag_info["misc_tags"]
                matched_name = tag_info["matched_name"]
                matched_source = tag_info["matched_source"]
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
                            is_header, is_event, is_raw, available_macs, alternate_ids, cmd
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        matched_name, matched_source, is_header, is_event, is_raw,
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


def parse_frame_rate(value):
    if not value:
        return None
    if value == "0/0":
        return None
    if "/" in value:
        try:
            num, den = value.split("/", 1)
            num_f = float(num)
            den_f = float(den)
            if den_f == 0:
                return None
            return num_f / den_f
        except ValueError:
            return None
    try:
        return float(value)
    except ValueError:
        return None




def authorise(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        settings = getSettings()
        security = settings["enable security"]
        username = settings["username"]
        password = settings["password"]
        if (
            security == "false"
            or auth
            and auth.username == username
            and auth.password == password
        ):
            return f(*args, **kwargs)

        return make_response(
            "Could not verify your login!",
            401,
            {"WWW-Authenticate": 'Basic realm="Login Required"'},
        )

    return decorated


@app.route("/api/stream/info", methods=["POST"])
@authorise
def stream_info():
    data = request.get_json(silent=True) or {}
    portal = data.get("portal")
    channel_id = data.get("channelId")
    if not portal or not channel_id:
        return jsonify({"success": False, "message": "Missing portal or channelId"}), 400

    stream_url = f"http://{host}/play/{portal}/{channel_id}?web=true"
    headers = "User-Agent: Mozilla/5.0 (MacReplay)\r\n"
    attempts = [
        {"timeout": 8, "probesize": "2M", "analyzeduration": "2M"},
        {"timeout": 12, "probesize": "5M", "analyzeduration": "5M"},
        {"timeout": 18, "probesize": "10M", "analyzeduration": "10M"},
        {"timeout": 25, "probesize": "20M", "analyzeduration": "20M"},
    ]
    last_error = "ffprobe error"
    payload = None

    for attempt in attempts:
        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-probesize",
            attempt["probesize"],
            "-analyzeduration",
            attempt["analyzeduration"],
            "-headers",
            headers,
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=codec_name,width,height,avg_frame_rate",
            "-of",
            "json",
            stream_url,
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=attempt["timeout"])
        except FileNotFoundError:
            return jsonify({"success": False, "message": "ffprobe not found"}), 500
        except subprocess.TimeoutExpired:
            last_error = "ffprobe timed out"
            continue

        if result.returncode != 0:
            last_error = result.stderr.strip() or "ffprobe error"
            time.sleep(0.4)
            continue

        try:
            payload = json.loads(result.stdout or "{}")
        except json.JSONDecodeError:
            last_error = "Invalid ffprobe output"
            continue

        if payload:
            break
        time.sleep(0.4)

    if not payload:
        logger.error(f"ffprobe failed: {last_error}")
        return jsonify({"success": False, "message": last_error}), 502

    streams = payload.get("streams") or []
    if not streams:
        return jsonify({"success": False, "message": "No video stream found"}), 404

    stream = streams[0]
    fps = parse_frame_rate(stream.get("avg_frame_rate"))

    return jsonify({
        "success": True,
        "codec": stream.get("codec_name") or "",
        "width": stream.get("width") or 0,
        "height": stream.get("height") or 0,
        "fps": fps or 0,
    })


def moveMac(portalId, mac):
    portals = getPortals()
    macs = portals[portalId]["macs"]
    x = macs[mac]
    del macs[mac]
    macs[mac] = x
    portals[portalId]["macs"] = macs
    savePortals(portals)


@app.route("/api/portals", methods=["GET"])
@authorise
def portals():
    """Legacy template route"""
    portal_data = getPortals()

    # Get channel and group counts per portal from database
    portal_stats = {}
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get total and active channel counts per portal
        cursor.execute(f"""
            SELECT
                c.portal,
                COUNT(*) as total_channels,
                SUM(CASE WHEN {ACTIVE_GROUP_CONDITION} THEN 1 ELSE 0 END) as active_channels
            FROM channels c
            LEFT JOIN groups g ON c.portal = g.portal AND c.genre_id = g.genre_id
            GROUP BY c.portal
        """)
        for row in cursor.fetchall():
            portal_stats[row['portal']] = {
                'channels': row['active_channels'] or 0,
                'total_channels': row['total_channels'] or 0
            }

        # Get total and active group counts per portal from groups table
        cursor.execute("""
            SELECT
                portal,
                COUNT(*) as total_groups,
                SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) as active_groups
            FROM groups
            GROUP BY portal
        """)
        for row in cursor.fetchall():
            if row['portal'] in portal_stats:
                portal_stats[row['portal']]['groups'] = row['active_groups'] or 0
                portal_stats[row['portal']]['total_groups'] = row['total_groups'] or 0
            else:
                portal_stats[row['portal']] = {
                    'channels': 0,
                    'total_channels': 0,
                    'groups': row['active_groups'] or 0,
                    'total_groups': row['total_groups'] or 0
                }

        conn.close()
    except Exception as e:
        logger.error(f"Error getting portal stats: {e}")

    # Initialize stats for portals not yet in database
    for portal_id, portal in portal_data.items():
        if portal_id not in portal_stats:
            portal_stats[portal_id] = {
                'channels': 0,
                'total_channels': 0,
                'groups': 0,
                'total_groups': 0
            }

    return render_template("portals.html", portals=portal_data, portal_stats=portal_stats)


@app.route("/api/portal/mac/delete", methods=["POST"])
@authorise
def delete_portal_mac():
    """API endpoint to delete a single MAC from a portal"""
    try:
        data = request.get_json()
        portal_id = data.get("portal_id")
        mac = data.get("mac")

        if not portal_id or not mac:
            return jsonify({"success": False, "message": "Missing portal_id or mac"})

        portals = getPortals()
        if portal_id not in portals:
            return jsonify({"success": False, "message": "Portal not found"})

        if mac not in portals[portal_id].get("macs", {}):
            return jsonify({"success": False, "message": "MAC not found in portal"})

        # Delete the MAC
        del portals[portal_id]["macs"][mac]
        savePortals(portals)

        logger.info(f"Deleted MAC({mac}) from Portal({portals[portal_id].get('name', portal_id)})")
        return jsonify({"success": True})

    except Exception as e:
        logger.error(f"Error deleting MAC: {e}")
        return jsonify({"success": False, "message": str(e)})


@app.route("/api/portals/data", methods=["GET"])
@authorise
def portals_data():
    """API endpoint to get portals data"""
    return jsonify(getPortals())


@app.route("/api/portal/macs/refresh", methods=["POST"])
@authorise
def refresh_portal_macs():
    """Refresh MAC data (watchdog, playback_limit, expiry) for all MACs in a portal."""
    try:
        data = request.get_json()
        portal_id = data.get("portal_id")

        if not portal_id:
            return jsonify({"success": False, "message": "Portal ID required"})

        portals = getPortals()
        if portal_id not in portals:
            return jsonify({"success": False, "message": "Portal not found"})

        portal = portals[portal_id]
        url = portal["url"]
        proxy = portal.get("proxy", "")

        updated_count = 0
        errors = []

        for mac in list(portal["macs"].keys()):
            try:
                token = stb.getToken(url, mac, proxy)
                if token:
                    # Get profile data (watchdog_timeout, playback_limit)
                    profile = stb.getProfile(url, mac, token, proxy)
                    # Get expiry from separate API call (different endpoint)
                    expiry = stb.getExpires(url, mac, token, proxy)

                    if profile or expiry:
                        # Keep existing expiry if new one is empty
                        old_data = normalize_mac_data(portal["macs"].get(mac, {}))
                        if not expiry:
                            expiry = old_data.get("expiry", "Unknown")

                        watchdog_timeout = int(profile.get("watchdog_timeout", 0)) if profile else 0
                        playback_limit = int(profile.get("playback_limit", 0)) if profile else 0

                        # Update MAC data
                        portal["macs"][mac] = {
                            "expiry": expiry if expiry else "Unknown",
                            "watchdog_timeout": watchdog_timeout,
                            "playback_limit": playback_limit
                        }
                        updated_count += 1
                        logger.info(f"Refreshed MAC {mac}: expiry={expiry}, watchdog={watchdog_timeout}, streams={playback_limit}")
                    else:
                        errors.append(f"{mac}: Could not get profile or expiry")
                else:
                    errors.append(f"{mac}: Could not get token")
            except Exception as e:
                errors.append(f"{mac}: {str(e)}")
                logger.error(f"Error refreshing MAC {mac}: {e}")

        # Save updated portal data
        portals[portal_id] = portal
        savePortals(portals)

        message = f"Updated {updated_count} of {len(portal['macs'])} MACs"
        if errors:
            message += f". Errors: {len(errors)}"

        logger.info(f"MAC refresh for portal {portal.get('name', portal_id)}: {message}")
        return jsonify({
            "success": True,
            "message": message,
            "updated": updated_count,
            "errors": errors,
            "macs": portal["macs"]  # Return updated MAC data
        })

    except Exception as e:
        logger.error(f"Error refreshing MACs: {e}")
        return jsonify({"success": False, "message": str(e)})


@app.route("/api/portal/genres", methods=["POST"])
@authorise
def get_portal_genres():
    """Fetch available genres from a portal for selection during add/edit."""
    try:
        data = request.get_json()
        url = data.get("url")
        mac = data.get("mac")
        proxy = data.get("proxy", "")

        if not url or not mac:
            return jsonify({"success": False, "message": "URL and MAC are required"})

        # URL auflösen falls nötig
        if not url.endswith(".php"):
            resolved_url = stb.getUrl(url, proxy)
            if not resolved_url:
                return jsonify({"success": False, "message": "Could not resolve portal URL"})
            url = resolved_url

        # Token holen
        token = stb.getToken(url, mac, proxy)
        if not token:
            return jsonify({"success": False, "message": "Could not authenticate with portal"})

        # Profil abrufen (für Authentifizierung)
        stb.getProfile(url, mac, token, proxy)

        # Genres abrufen
        genres = stb.getGenres(url, mac, token, proxy)

        if not genres:
            return jsonify({"success": False, "message": "No genres found"})

        # Channels abrufen um pro Genre zu zählen
        channels = stb.getAllChannels(url, mac, token, proxy)
        channel_counts = {}
        if channels:
            # Handle both list and dict formats (some portals return dict with channel IDs as keys)
            channel_list = channels if isinstance(channels, list) else list(channels.values())
            for channel in channel_list:
                if isinstance(channel, dict):
                    genre_id = str(channel.get("tv_genre_id", ""))
                    channel_counts[genre_id] = channel_counts.get(genre_id, 0) + 1

        # Liste von {id, title, channel_count} Objekten zurückgeben
        genre_list = [
            {
                "id": str(g["id"]),
                "title": g["title"],
                "channel_count": channel_counts.get(str(g["id"]), 0)
            }
            for g in genres
        ]
        # Alphabetisch sortieren
        genre_list.sort(key=lambda x: x["title"].lower())

        logger.info(f"Fetched {len(genre_list)} genres with channel counts from portal")
        return jsonify({"success": True, "genres": genre_list})

    except Exception as e:
        logger.error(f"Error fetching genres: {e}")
        return jsonify({"success": False, "message": str(e)})


@app.route("/api/portal/groups", methods=["POST"])
@authorise
def get_portal_groups():
    """Get groups from database for a portal (fast, no API call needed)."""
    try:
        data = request.get_json()
        portal_id = data.get("portal_id")

        if not portal_id:
            return jsonify({"success": False, "message": "Portal ID required"})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT genre_id, name, channel_count, active
            FROM groups
            WHERE portal = ?
            ORDER BY name
        """, [portal_id])

        groups = []
        for row in cursor.fetchall():
            groups.append({
                "id": row['genre_id'],
                "title": row['name'] or f"Group {row['genre_id']}",
                "channel_count": row['channel_count'] or 0,
                "active": row['active'] == 1
            })

        conn.close()

        if not groups:
            return jsonify({"success": False, "message": "No groups found in database. Please refresh channels first."})

        logger.info(f"Loaded {len(groups)} groups from database for portal {portal_id}")
        return jsonify({"success": True, "groups": groups})

    except Exception as e:
        logger.error(f"Error getting groups from database: {e}")
        return jsonify({"success": False, "message": str(e)})


@app.route("/api/portal/genres/update", methods=["POST"])
@authorise
def update_portal_genres():
    """Update selected genres for a portal by toggling groups.active flag."""
    global cached_xmltv
    try:
        data = request.get_json()
        portal_id = data.get("portal_id")
        selected_genres = data.get("selected_genres", [])

        if not portal_id:
            return jsonify({"success": False, "message": "Portal ID required"})

        portals = getPortals()
        if portal_id not in portals:
            return jsonify({"success": False, "message": "Portal not found"})

        portal = portals[portal_id]
        logger.info(f"Updating genres for portal {portal.get('name', portal_id)}")
        logger.info(f"Selected genres: {selected_genres}")

        # Update groups.active flag in database
        conn = get_db_connection()
        cursor = conn.cursor()

        # Set all groups for this portal to inactive
        cursor.execute("UPDATE groups SET active = 0 WHERE portal = ?", [portal_id])

        # Activate selected groups
        if selected_genres:
            placeholders = ",".join(["?" for _ in selected_genres])
            cursor.execute(
                f"UPDATE groups SET active = 1 WHERE portal = ? AND genre_id IN ({placeholders})",
                [portal_id] + [str(g) for g in selected_genres]
            )

        conn.commit()

        # Get updated counts for response
        cursor.execute("""
            SELECT
                COUNT(*) as total_groups,
                SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) as active_groups,
                SUM(channel_count) as total_channels,
                SUM(CASE WHEN active = 1 THEN channel_count ELSE 0 END) as active_channels
            FROM groups WHERE portal = ?
        """, [portal_id])
        row = cursor.fetchone()
        conn.close()

        total_groups = row[0] or 0
        active_groups = row[1] or 0
        total_channels = row[2] or 0
        active_channels = row[3] or 0

        # Update JSON config for backwards compatibility
        portal["selected_genres"] = selected_genres
        portal["total_groups"] = total_groups
        portal["total_channels"] = total_channels
        portals[portal_id] = portal
        savePortals(portals)

        logger.info(f"Updated genres for {portal.get('name', portal_id)}: {active_groups}/{total_groups} groups active, {active_channels}/{total_channels} channels")

        # Clear EPG cache so it regenerates with new active groups
        cached_xmltv = None

        return jsonify({
            "success": True,
            "message": "Genres updated successfully",
            "total_groups": total_groups,
            "active_groups": active_groups,
            "total_channels": total_channels,
            "active_channels": active_channels
        })

    except Exception as e:
        logger.error(f"Error updating genres: {e}")
        return jsonify({"success": False, "message": str(e)})


@app.route("/portal/add", methods=["POST"])
@authorise
def portalsAdd():
    global cached_xmltv
    cached_xmltv = None
    id = uuid.uuid4().hex
    enabled = "true"
    name = request.form["name"]
    url = request.form["url"]
    macs = list(set(request.form["macs"].split(",")))
    streamsPerMac = request.form["streams per mac"]
    epgOffset = request.form["epg offset"]
    proxy = request.form["proxy"]
    fetchEpg = "true" if request.form.get("fetch epg") else "false"
    autoNormalize = "true" if request.form.get("auto normalize names") else "false"
    selectedGenres = request.form.getlist("selected_genres")  # Liste der Genre-IDs

    if not url.endswith(".php"):
        url = stb.getUrl(url, proxy)
        if not url:
            logger.error("Error getting URL for Portal({})".format(name))
            flash("Error getting URL for Portal({})".format(name), "danger")
            return redirect("/portals", code=302)

    macsd = {}
    tested_total = 0
    tested_success = 0
    tested_failed = 0

    for mac in macs:
        tested_total += 1
        logger.info(f"Testing MAC({mac}) for Portal({name})...")
        token = stb.getToken(url, mac, proxy)
        if token:
            logger.debug(f"Got token for MAC({mac}), getting profile and expiry...")
            profile = stb.getProfile(url, mac, token, proxy)
            expiry = stb.getExpires(url, mac, token, proxy)
            if expiry:
                # Neues Format: Dict mit expiry, watchdog_timeout, playback_limit
                macsd[mac] = {
                    "expiry": expiry,
                    "watchdog_timeout": profile.get("watchdog_timeout", 0) if profile else 0,
                    "playback_limit": profile.get("playback_limit", 0) if profile else 0
                }
                logger.info(
                    "Successfully tested MAC({}) for Portal({})".format(mac, name)
                )
                tested_success += 1
                continue
            else:
                logger.error(f"Failed to get expiry for MAC({mac}) for Portal({name})")
        else:
            logger.error(f"Failed to get token for MAC({mac}) for Portal({name})")

        logger.error("Error testing MAC({}) for Portal({})".format(mac, name))
        tested_failed += 1

    if tested_total > 0:
        if tested_success > 0 and tested_failed == 0:
            flash(
                f"{tested_success}/{tested_total} MACs successfully added for Portal({name})",
                "success",
            )
        elif tested_success > 0:
            flash(
                f"{tested_success}/{tested_total} MACs successfully tested for Portal({name}). {tested_failed} failed.",
                "warning",
            )
        else:
            flash(
                f"0/{tested_total} MACs successfully tested for Portal({name}).",
                "danger",
            )

    if len(macsd) > 0:
        portal = {
            "enabled": enabled,
            "name": name,
            "url": url,
            "macs": macsd,
            "streams per mac": streamsPerMac,
            "epg offset": epgOffset,
            "proxy": proxy,
            "fetch epg": fetchEpg,
            "selected_genres": selectedGenres,  # Liste der Genre-IDs zum Importieren
            "auto normalize names": autoNormalize,
        }

        for setting, default in defaultPortal.items():
            if not portal.get(setting):
                portal[setting] = default

        portals = getPortals()
        portals[id] = portal
        savePortals(portals)
        logger.info("Portal({}) added!".format(portal["name"]))
        flash("Portal({}) added!".format(portal["name"]), "success")

        # Refresh channel cache in background to load channels from the new portal
        def background_refresh():
            try:
                refresh_channels_cache()
                logger.info(f"Background channel refresh completed for new portal {name}")
            except Exception as e:
                logger.error(f"Error refreshing channels after portal add: {e}")

        thread = Thread(target=background_refresh, daemon=True)
        thread.start()
        flash("Channels are being loaded in the background.", "info")

    else:
        logger.error(
            "None of the MACs tested OK for Portal({}). Adding not successfull".format(
                name
            )
        )

    return redirect("/portals", code=302)


@app.route("/portal/update", methods=["POST"])
@authorise
def portalUpdate():
    global cached_xmltv
    cached_xmltv = None
    id = request.form["id"]
    enabled = request.form.get("enabled", "false")
    name = request.form["name"]
    url = request.form["url"]
    newmacs = list(set(request.form["macs"].split(",")))
    streamsPerMac = request.form["streams per mac"]
    epgOffset = request.form["epg offset"]
    proxy = request.form["proxy"]
    fetchEpg = "true" if request.form.get("fetch epg") else "false"
    autoNormalize = "true" if request.form.get("auto normalize names") else "false"
    retest = request.form.get("retest", None)
    selectedGenres = request.form.getlist("selected_genres")  # Liste der Genre-IDs

    if not url.endswith(".php"):
        url = stb.getUrl(url, proxy)
        if not url:
            logger.error("Error getting URL for Portal({})".format(name))
            flash("Error getting URL for Portal({})".format(name), "danger")
            return redirect("/portals", code=302)

    portals = getPortals()
    oldmacs = portals[id]["macs"]
    macsout = {}
    deadmacs = []
    tested_total = 0
    tested_success = 0
    tested_failed = 0

    for mac in newmacs:
        if retest or mac not in oldmacs.keys():
            tested_total += 1
            logger.info(f"Testing MAC({mac}) for Portal({name})...")
            token = stb.getToken(url, mac, proxy)
            if token:
                logger.debug(f"Got token for MAC({mac}), getting profile and expiry...")
                profile = stb.getProfile(url, mac, token, proxy)
                expiry = stb.getExpires(url, mac, token, proxy)
                if expiry:
                    # Neues Format: Dict mit expiry, watchdog_timeout, playback_limit
                    macsout[mac] = {
                        "expiry": expiry,
                        "watchdog_timeout": profile.get("watchdog_timeout", 0) if profile else 0,
                        "playback_limit": profile.get("playback_limit", 0) if profile else 0
                    }
                    logger.info(
                        "Successfully tested MAC({}) for Portal({})".format(mac, name)
                    )
                    tested_success += 1
                else:
                    logger.error(f"Failed to get expiry for MAC({mac}) for Portal({name})")
            else:
                logger.error(f"Failed to get token for MAC({mac}) for Portal({name})")

            if mac not in list(macsout.keys()):
                deadmacs.append(mac)
                tested_failed += 1

        if mac in oldmacs.keys() and mac not in deadmacs:
            # Altes Format beibehalten (wird bei Anzeige normalisiert)
            macsout[mac] = oldmacs[mac]

        if mac not in macsout.keys():
            logger.error("Error testing MAC({}) for Portal({})".format(mac, name))

    if tested_total > 0:
        if tested_success > 0 and tested_failed == 0:
            flash(
                f"{tested_success}/{tested_total} MACs successfully tested for Portal({name})",
                "success",
            )
        elif tested_success > 0:
            flash(
                f"{tested_success}/{tested_total} MACs successfully tested for Portal({name}). {tested_failed} failed.",
                "warning",
            )
        else:
            flash(
                f"0/{tested_total} MACs successfully tested for Portal({name}).",
                "danger",
            )

    if len(macsout) > 0:
        portals[id]["enabled"] = enabled
        portals[id]["name"] = name
        portals[id]["url"] = url
        portals[id]["macs"] = macsout
        portals[id]["streams per mac"] = streamsPerMac
        portals[id]["epg offset"] = epgOffset
        portals[id]["proxy"] = proxy
        portals[id]["fetch epg"] = fetchEpg
        portals[id]["selected_genres"] = selectedGenres  # Genre-Filter
        portals[id]["auto normalize names"] = autoNormalize
        savePortals(portals)
        logger.info("Portal({}) updated!".format(name))
        flash("Portal({}) updated!".format(name), "success")

    else:
        logger.error(
            "None of the MACs tested OK for Portal({}). Adding not successfull".format(
                name
            )
        )

    return redirect("/portals", code=302)


@app.route("/portal/remove", methods=["POST"])
@authorise
def portalRemove():
    id = request.form["deleteId"]
    portals = getPortals()
    
    # Check if portal exists
    if id not in portals:
        logger.error(f"Attempted to delete non-existent portal: {id}")
        # For API calls (JSON request), return JSON error
        if request.is_json or request.headers.get('Accept', '').startswith('application/json'):
            return jsonify({"error": "Portal not found"}), 404
        flash(f"Portal not found", "danger")
        return redirect("/portals", code=302)
    
    name = portals[id]["name"]
    del portals[id]
    savePortals(portals)
    logger.info("Portal ({}) removed!".format(name))

    # Remove channels for this portal from the database
    try:
        conn = sqlite3.connect(dbPath)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM channels WHERE portal = ?', (id,))
        deleted_count = cursor.rowcount
        conn.commit()
        conn.close()
        logger.info(f"Removed {deleted_count} channels for portal {name} from database")
    except Exception as e:
        logger.error(f"Error removing channels from database for portal {name}: {e}")

    # For API calls (JSON request), return JSON
    if request.is_json or request.headers.get('Accept', '').startswith('application/json'):
        return jsonify({"success": True, "message": f"Portal {name} removed"})

    flash("Portal ({}) removed!".format(name), "success")
    return redirect("/portals", code=302)


@app.route("/api/editor", methods=["GET"])
@authorise
def editor():
    """Legacy template route"""
    return render_template("editor.html")
    


@app.route("/api/editor_data", methods=["GET"])
@app.route("/editor_data", methods=["GET"])  # Keep old route for backward compatibility
@authorise
def editor_data():
    """Server-side DataTables endpoint with pagination and filtering."""
    try:
        # Get DataTables parameters
        draw = request.args.get('draw', type=int, default=1)
        start = request.args.get('start', type=int, default=0)
        length = request.args.get('length', type=int, default=250)
        search_value = request.args.get('search[value]', default='')

        # Get custom filter parameters
        portal_filter = request.args.get('portal', default='')
        group_filter = request.args.get('group', default='')
        duplicate_filter = request.args.get('duplicates', default='')
        resolution_filter = request.args.get('resolution', default='')
        video_filter = request.args.get('video', default='')
        country_filter = request.args.get('country', default='')
        event_tags_filter = request.args.get('event_tags', default='')
        misc_filter = request.args.get('misc', default='')
        raw_filter = request.args.get('raw', default='')
        event_filter = request.args.get('event', default='')
        header_filter = request.args.get('header', default='')

        # Map column indices to database columns
        column_map = {
            0: 'enabled',
            1: 'channel_id',  # Play button, not sortable but needs a column
            2: 'name',  # Channel name
            3: 'genre',
            4: 'number',
            5: 'epg_id',  # EPG ID - Special handling
            6: 'portal_name'
        }

        # Build the SQL query
        conn = get_db_connection()
        cursor = conn.cursor()

        # Base query - JOIN with groups table to filter by active groups
        base_query = f"""FROM channels c
            LEFT JOIN groups g ON c.portal = g.portal AND c.genre_id = g.genre_id
            WHERE {ACTIVE_GROUP_CONDITION}"""
        params = []
        
        # Add portal filter (supports multiple values)
        if portal_filter:
            portal_values = [p.strip() for p in portal_filter.split(',') if p.strip()]
            if portal_values:
                placeholders = ','.join(['?'] * len(portal_values))
                base_query += f" AND c.portal_name IN ({placeholders})"
                params.extend(portal_values)

        # Add group filter (check both custom_genre and genre, supports multiple values)
        if group_filter:
            genre_values = [g.strip() for g in group_filter.split(',') if g.strip()]
            if genre_values:
                placeholders = ','.join(['?'] * len(genre_values))
                base_query += f" AND (COALESCE(NULLIF(c.custom_genre, ''), c.genre) IN ({placeholders}))"
                params.extend(genre_values)

        # Add duplicate filter (only for enabled channels)
        if duplicate_filter == 'enabled_only':
            # Show only channels where the name appears multiple times among enabled channels
            base_query += """ AND c.enabled = 1 AND COALESCE(NULLIF(c.custom_name, ''), NULLIF(c.auto_name, ''), c.name) IN (
                SELECT COALESCE(NULLIF(custom_name, ''), NULLIF(auto_name, ''), name)
                FROM channels
                WHERE enabled = 1
                GROUP BY COALESCE(NULLIF(custom_name, ''), NULLIF(auto_name, ''), name)
                HAVING COUNT(*) > 1
            )"""
        elif duplicate_filter == 'unique_only':
            # Show only channels where the name appears once among enabled channels
            base_query += """ AND COALESCE(NULLIF(c.custom_name, ''), NULLIF(c.auto_name, ''), c.name) IN (
                SELECT COALESCE(NULLIF(custom_name, ''), NULLIF(auto_name, ''), name)
                FROM channels
                WHERE enabled = 1
                GROUP BY COALESCE(NULLIF(custom_name, ''), NULLIF(auto_name, ''), name)
                HAVING COUNT(*) = 1
            )"""

        # Tag filters
        if resolution_filter:
            values = [v.strip() for v in resolution_filter.split(',') if v.strip()]
            if values:
                placeholders = ','.join(['?'] * len(values))
                base_query += f" AND c.resolution IN ({placeholders})"
                params.extend(values)

        if video_filter:
            values = [v.strip() for v in video_filter.split(',') if v.strip()]
            if values:
                placeholders = ','.join(['?'] * len(values))
                base_query += f" AND c.video_codec IN ({placeholders})"
                params.extend(values)

        if country_filter:
            values = [v.strip() for v in country_filter.split(',') if v.strip()]
            if values:
                placeholders = ','.join(['?'] * len(values))
                base_query += f" AND c.country IN ({placeholders})"
                params.extend(values)

        if event_tags_filter:
            values = [v.strip() for v in event_tags_filter.split(',') if v.strip()]
            if values:
                like_clauses = []
                for value in values:
                    like_clauses.append("(',' || c.event_tags || ',') LIKE ?")
                    params.append(f"%,{value},%")
                base_query += " AND (" + " OR ".join(like_clauses) + ")"

        if misc_filter:
            values = [v.strip() for v in misc_filter.split(',') if v.strip()]
            if values:
                like_clauses = []
                for value in values:
                    like_clauses.append("(',' || c.misc_tags || ',') LIKE ?")
                    params.append(f"%,{value},%")
                base_query += " AND (" + " OR ".join(like_clauses) + ")"

        if raw_filter in ('true', 'include'):
            base_query += " AND c.is_raw = 1"
        elif raw_filter == 'exclude':
            base_query += " AND (c.is_raw = 0 OR c.is_raw IS NULL)"

        if event_filter in ('true', 'include'):
            base_query += " AND c.is_event = 1"
        elif event_filter == 'exclude':
            base_query += " AND (c.is_event = 0 OR c.is_event IS NULL)"

        if header_filter in ('true', 'include'):
            base_query += " AND c.is_header = 1"
        elif header_filter == 'exclude':
            base_query += " AND (c.is_header = 0 OR c.is_header IS NULL)"

        # Add search filter if provided
        if search_value:
            base_query += """ AND (
                c.name LIKE ? OR
                c.custom_name LIKE ? OR
                c.auto_name LIKE ? OR
                c.genre LIKE ? OR
                c.custom_genre LIKE ? OR
                c.number LIKE ? OR
                c.custom_number LIKE ? OR
                c.portal_name LIKE ? OR
                c.resolution LIKE ? OR
                c.video_codec LIKE ? OR
                c.country LIKE ? OR
                c.audio_tags LIKE ? OR
                c.event_tags LIKE ? OR
                c.misc_tags LIKE ?
            )"""
            search_param = f"%{search_value}%"
            params.extend([search_param] * 14)
        
        # Get total count (without filters)
        cursor.execute("SELECT COUNT(*) FROM channels")
        records_total = cursor.fetchone()[0]
        
        # Get filtered count
        count_query = f"SELECT COUNT(*) {base_query}"
        cursor.execute(count_query, params)
        records_filtered = cursor.fetchone()[0]
        
        # Build the ORDER BY clause handling multiple columns
        order_clauses = []
        i = 0
        while True:
            col_idx_key = f'order[{i}][column]'
            dir_key = f'order[{i}][dir]'
            
            if col_idx_key not in request.args:
                break
                
            col_idx = request.args.get(col_idx_key, type=int)
            direction = request.args.get(dir_key, default='asc')
            col_name = column_map.get(col_idx, 'name')
            
            if col_name == 'name':
                order_clauses.append(f"COALESCE(NULLIF(c.custom_name, ''), NULLIF(c.auto_name, ''), c.name) {direction}")
            elif col_name == 'genre':
                order_clauses.append(f"COALESCE(NULLIF(c.custom_genre, ''), c.genre) {direction}")
            elif col_name == 'number':
                order_clauses.append(f"CAST(COALESCE(NULLIF(c.custom_number, ''), c.number) AS INTEGER) {direction}")
            elif col_name == 'epg_id':
                order_clauses.append(f"COALESCE(NULLIF(c.custom_epg_id, ''), c.portal || c.channel_id) {direction}")
            else:
                order_clauses.append(f"c.{col_name} {direction}")
            i += 1

        if not order_clauses:
            order_clauses.append("COALESCE(NULLIF(c.custom_name, ''), NULLIF(c.auto_name, ''), c.name) ASC")
            
        order_clause = "ORDER BY " + ", ".join(order_clauses)
        
        data_query = f"""
            SELECT
                c.portal, c.channel_id, c.portal_name, c.name, c.number, c.genre, c.genre_id, c.logo,
                c.enabled, c.custom_name, c.auto_name, c.custom_number, c.custom_genre,
                c.custom_epg_id, c.available_macs, c.alternate_ids,
                c.resolution, c.video_codec, c.country, c.audio_tags, c.event_tags, c.misc_tags,
                c.matched_name, c.matched_source,
                c.is_raw, c.is_event, c.is_header
            {base_query}
            {order_clause}
            LIMIT ? OFFSET ?
        """
        
        params.extend([length, start])
        cursor.execute(data_query, params)
        
        # Store the channel data results first
        channel_rows = cursor.fetchall()
        
        # Get duplicate counts for enabled channels
        duplicate_counts_query = """
            SELECT
                COALESCE(NULLIF(custom_name, ''), NULLIF(auto_name, ''), name) as channel_name,
                COUNT(*) as count
            FROM channels
            WHERE enabled = 1
            GROUP BY COALESCE(NULLIF(custom_name, ''), NULLIF(auto_name, ''), name)
            HAVING COUNT(*) > 1
        """
        cursor.execute(duplicate_counts_query)
        duplicate_counts = {row['channel_name']: row['count'] for row in cursor.fetchall()}

        # Get list of channels that have EPG data
        epg_channels = set()
        if cached_xmltv:
            try:
                root = ET.fromstring(cached_xmltv)
                # Get all channel IDs that have at least one programme
                for programme in root.findall("programme"):
                    epg_channels.add(programme.get("channel"))
            except Exception as e:
                logger.debug(f"Could not parse EPG for editor: {e}")

        # Format the results for DataTables
        channels = []
        for row in channel_rows:
            portal = row['portal']
            channel_id = row['channel_id']
            effective_name = row['custom_name'] or row['auto_name'] or row['name']
            duplicate_count = duplicate_counts.get(effective_name, 0)
            
            # Check if this channel has EPG data (by custom EPG ID or channel name)
            epg_id = row['custom_epg_id'] or effective_name
            has_epg = epg_id in epg_channels

            channels.append({
                "portal": portal,
                "portalName": row['portal_name'] or '',
                "enabled": bool(row['enabled']),
                "channelNumber": row['number'] or '',
                "customChannelNumber": row['custom_number'] or '',
                "channelName": row['name'] or '',
                "customChannelName": row['custom_name'] or '',
                "autoChannelName": row['auto_name'] or '',
                "genre": row['genre'] or '',
                "genreId": row['genre_id'] or '',
                "customGenre": row['custom_genre'] or '',
                "channelId": channel_id,
                "customEpgId": row['custom_epg_id'] or '',
                "link": f"http://{host}/play/{portal}/{channel_id}?web=true",
                "logo": row['logo'] or '',
                "availableMacs": row['available_macs'] or '',
                "alternateIds": row['alternate_ids'] or '',
                "resolution": row['resolution'] or '',
                "videoCodec": row['video_codec'] or '',
                "country": row['country'] or '',
                "audioTags": row['audio_tags'] or '',
                "eventTags": row['event_tags'] or '',
                "miscTags": row['misc_tags'] or '',
                "matchedName": row['matched_name'] or '',
                "matchedSource": row['matched_source'] or '',
                "isRaw": bool(row['is_raw']),
                "isEvent": bool(row['is_event']),
                "isHeader": bool(row['is_header']),
                "duplicateCount": duplicate_count if row['enabled'] else 0,
                "hasEpg": has_epg
            })
        
        conn.close()
        
        # Return DataTables format
        return flask.jsonify({
            "draw": draw,
            "recordsTotal": records_total,
            "recordsFiltered": records_filtered,
            "data": channels
        })
        
    except Exception as e:
        logger.error(f"Error in editor_data: {e}")
        return flask.jsonify({
            "draw": draw if 'draw' in locals() else 1,
            "recordsTotal": 0,
            "recordsFiltered": 0,
            "data": [],
            "error": str(e)
        }), 500


@app.route("/api/editor/portals", methods=["GET"])
@app.route("/editor/portals", methods=["GET"])  # Keep old route for backward compatibility
@authorise
def editor_portals():
    """Get list of unique portals for filter dropdown."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT DISTINCT portal_name
            FROM channels
            WHERE portal_name IS NOT NULL AND portal_name != ''
            ORDER BY portal_name
        """)
        
        portals = [row['portal_name'] for row in cursor.fetchall()]
        conn.close()
        
        return flask.jsonify({"portals": portals})
    except Exception as e:
        logger.error(f"Error in editor_portals: {e}")
        return flask.jsonify({"portals": [], "error": str(e)}), 500


@app.route("/api/editor/genres", methods=["GET"])
@app.route("/editor/genres", methods=["GET"])  # Keep old route for backward compatibility
@authorise
def editor_genres():
    """Get list of unique genres for filter dropdown, optionally filtered by portal."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if portal filter is provided
        portal = flask.request.args.get('portal', '').strip()

        if portal:
            # Filter genres by portal
            cursor.execute("""
                SELECT DISTINCT COALESCE(NULLIF(custom_genre, ''), genre) as genre
                FROM channels
                WHERE COALESCE(NULLIF(custom_genre, ''), genre) IS NOT NULL
                    AND COALESCE(NULLIF(custom_genre, ''), genre) != ''
                    AND COALESCE(NULLIF(custom_genre, ''), genre) != 'None'
                    AND portal = ?
                ORDER BY genre
            """, (portal,))
        else:
            # Return all genres if no portal filter
            cursor.execute("""
                SELECT DISTINCT COALESCE(NULLIF(custom_genre, ''), genre) as genre
                FROM channels
                WHERE COALESCE(NULLIF(custom_genre, ''), genre) IS NOT NULL
                    AND COALESCE(NULLIF(custom_genre, ''), genre) != ''
                    AND COALESCE(NULLIF(custom_genre, ''), genre) != 'None'
                ORDER BY genre
            """)

        genres = [row['genre'] for row in cursor.fetchall()]
        conn.close()

        return flask.jsonify({"genres": genres})
    except Exception as e:
        logger.error(f"Error in editor_genres: {e}")
        return flask.jsonify({"genres": [], "error": str(e)}), 500


@app.route("/api/editor/tag-values", methods=["GET"])
@authorise
def editor_tag_values():
    """Return distinct tag values for editor filters."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT DISTINCT resolution FROM channels WHERE resolution IS NOT NULL AND resolution != ''")
        resolutions = sorted({row['resolution'] for row in cursor.fetchall() if row['resolution']})

        cursor.execute("SELECT DISTINCT video_codec FROM channels WHERE video_codec IS NOT NULL AND video_codec != ''")
        video_codecs = sorted({row['video_codec'] for row in cursor.fetchall() if row['video_codec']})

        cursor.execute("SELECT DISTINCT country FROM channels WHERE country IS NOT NULL AND country != ''")
        countries = sorted({row['country'] for row in cursor.fetchall() if row['country']})

        cursor.execute("SELECT DISTINCT event_tags FROM channels WHERE event_tags IS NOT NULL AND event_tags != ''")
        event_values = set()
        for row in cursor.fetchall():
            for tag in (row['event_tags'] or '').split(','):
                tag = tag.strip()
                if tag:
                    event_values.add(tag)

        cursor.execute("SELECT DISTINCT misc_tags FROM channels WHERE misc_tags IS NOT NULL AND misc_tags != ''")
        misc_values = set()
        for row in cursor.fetchall():
            for tag in (row['misc_tags'] or '').split(','):
                tag = tag.strip()
                if tag:
                    misc_values.add(tag)

        conn.close()

        return flask.jsonify({
            "resolutions": resolutions,
            "video_codecs": video_codecs,
            "countries": countries,
            "event_tags": sorted(event_values),
            "misc_tags": sorted(misc_values),
        })
    except Exception as e:
        logger.error(f"Error in editor_tag_values: {e}")
        return flask.jsonify({
            "resolutions": [],
            "video_codecs": [],
            "countries": [],
            "event_tags": [],
            "misc_tags": [],
            "error": str(e)
        }), 500


@app.route("/api/editor/genres-grouped", methods=["GET"])
@app.route("/editor/genres-grouped", methods=["GET"])
@authorise
def editor_genres_grouped():
    """Get genres grouped by portal for multi-select dropdown."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get all portal names (not IDs) - using DISTINCT on portal_name
        cursor.execute("SELECT DISTINCT portal_name FROM channels WHERE portal_name IS NOT NULL AND portal_name != '' ORDER BY portal_name")
        portal_names = [row['portal_name'] for row in cursor.fetchall()]

        genres_by_portal = []
        for portal_name in portal_names:
            cursor.execute("""
                SELECT DISTINCT COALESCE(NULLIF(custom_genre, ''), genre) as genre
                FROM channels
                WHERE portal_name = ?
                    AND COALESCE(NULLIF(custom_genre, ''), genre) IS NOT NULL
                    AND COALESCE(NULLIF(custom_genre, ''), genre) != ''
                    AND COALESCE(NULLIF(custom_genre, ''), genre) != 'None'
                ORDER BY genre
            """, (portal_name,))

            genres = [row['genre'] for row in cursor.fetchall()]
            if genres:  # Only add portal if it has genres
                genres_by_portal.append({
                    'portal': portal_name,
                    'genres': genres
                })

        conn.close()

        return flask.jsonify({"genres_by_portal": genres_by_portal})
    except Exception as e:
        logger.error(f"Error in editor_genres_grouped: {e}")
        return flask.jsonify({"genres_by_portal": [], "error": str(e)}), 500


@app.route("/api/editor/duplicate-counts", methods=["GET"])
@app.route("/editor/duplicate-counts", methods=["GET"])  # Keep old route for backward compatibility
@authorise
def editor_duplicate_counts():
    """Get duplicate counts for all channel names (only enabled channels)."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                COALESCE(NULLIF(custom_name, ''), NULLIF(auto_name, ''), name) as channel_name,
                COUNT(*) as count
            FROM channels
            WHERE enabled = 1
            GROUP BY COALESCE(NULLIF(custom_name, ''), NULLIF(auto_name, ''), name)
            ORDER BY count DESC, channel_name
        """)
        
        counts = [{"channel_name": row['channel_name'], "count": row['count']} 
                 for row in cursor.fetchall()]
        conn.close()
        
        return flask.jsonify({"counts": counts})
    except Exception as e:
        logger.error(f"Error in editor_duplicate_counts: {e}")
        return flask.jsonify({"counts": [], "error": str(e)}), 500


@app.route("/api/editor/deactivate-duplicates", methods=["POST"])
@app.route("/editor/deactivate-duplicates", methods=["POST"])  # Keep old route for backward compatibility
@authorise
def editor_deactivate_duplicates():
    """Deactivate duplicate enabled channels, keeping only the first occurrence."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Find all duplicate channels (using ROW_NUMBER to identify which to keep)
        find_duplicates_query = """
            WITH ranked_channels AS (
                SELECT 
                    portal,
                    channel_id,
                    COALESCE(NULLIF(custom_name, ''), NULLIF(auto_name, ''), name) as effective_name,
                    ROW_NUMBER() OVER (
                        PARTITION BY COALESCE(NULLIF(custom_name, ''), NULLIF(auto_name, ''), name) 
                        ORDER BY portal, channel_id
                    ) as row_num
                FROM channels
                WHERE enabled = 1
            )
            SELECT portal, channel_id, effective_name, row_num
            FROM ranked_channels
            WHERE effective_name IN (
                SELECT effective_name
                FROM ranked_channels
                GROUP BY effective_name
                HAVING COUNT(*) > 1
            )
            AND row_num > 1
            ORDER BY effective_name, row_num
        """
        
        cursor.execute(find_duplicates_query)
        duplicates_to_deactivate = cursor.fetchall()
        
        # Deactivate the duplicate channels
        deactivated_count = 0
        for dup in duplicates_to_deactivate:
            cursor.execute("""
                UPDATE channels
                SET enabled = 0
                WHERE portal = ? AND channel_id = ?
            """, (dup['portal'], dup['channel_id']))
            deactivated_count += 1
        
        conn.commit()
        conn.close()
        
        # Reset playlist cache to force regeneration
        global last_playlist_host
        last_playlist_host = None
        
        logger.info(f"Deactivated {deactivated_count} duplicate channels")
        
        return flask.jsonify({
            "success": True,
            "deactivated": deactivated_count,
            "message": f"Deactivated {deactivated_count} duplicate channels"
        })
        
    except Exception as e:
        logger.error(f"Error in editor_deactivate_duplicates: {e}")
        return flask.jsonify({
            "success": False,
            "deactivated": 0,
            "error": str(e)
        }), 500


@app.route("/api/editor/save", methods=["POST"])
@app.route("/editor/save", methods=["POST"])  # Keep old route for backward compatibility
@authorise
def editorSave():
    global cached_xmltv, last_playlist_host
    #cached_xmltv = None # The tv guide will be updated next time its downloaded
    threading.Thread(target=refresh_xmltv, daemon=True).start() #Force update in a seperate thread
    last_playlist_host = None     # The playlist will be updated next time it is downloaded
    Thread(target=refresh_lineup).start() # Update the channel lineup for plex.
    
    enabledEdits = json.loads(request.form["enabledEdits"])
    numberEdits = json.loads(request.form["numberEdits"])
    nameEdits = json.loads(request.form["nameEdits"])
    groupEdits = json.loads(request.form["groupEdits"])
    epgEdits = json.loads(request.form["epgEdits"])
    
    # Update SQLite database
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Process enabled/disabled edits
        for edit in enabledEdits:
            portal = edit["portal"]
            channel_id = edit["channel id"]
            enabled = 1 if edit["enabled"] else 0
            
            cursor.execute('''
                UPDATE channels 
                SET enabled = ? 
                WHERE portal = ? AND channel_id = ?
            ''', (enabled, portal, channel_id))
        
        # Process custom number edits
        for edit in numberEdits:
            portal = edit["portal"]
            channel_id = edit["channel id"]
            custom_number = edit["custom number"]
            
            cursor.execute('''
                UPDATE channels 
                SET custom_number = ? 
                WHERE portal = ? AND channel_id = ?
            ''', (custom_number, portal, channel_id))
        
        # Process custom name edits
        for edit in nameEdits:
            portal = edit["portal"]
            channel_id = edit["channel id"]
            custom_name = edit["custom name"]
            
            cursor.execute('''
                UPDATE channels 
                SET custom_name = ? 
                WHERE portal = ? AND channel_id = ?
            ''', (custom_name, portal, channel_id))
        
        # Process custom group edits
        for edit in groupEdits:
            portal = edit["portal"]
            channel_id = edit["channel id"]
            custom_genre = edit["custom genre"]
            
            cursor.execute('''
                UPDATE channels 
                SET custom_genre = ? 
                WHERE portal = ? AND channel_id = ?
            ''', (custom_genre, portal, channel_id))
        
        # Process custom EPG ID edits
        for edit in epgEdits:
            portal = edit["portal"]
            channel_id = edit["channel id"]
            custom_epg_id = edit["custom epg id"]
            
            cursor.execute('''
                UPDATE channels 
                SET custom_epg_id = ? 
                WHERE portal = ? AND channel_id = ?
            ''', (custom_epg_id, portal, channel_id))
        
        conn.commit()
        logger.info("Channel edits saved to database!")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error saving channel edits: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        conn.close()

    return jsonify({"success": True, "message": "Playlist config saved!"})


@app.route("/api/editor/merge", methods=["POST"])
@authorise
def editor_merge_channels():
    """Merge two channels - add secondary's ID to primary's alternate_ids, then delete secondary."""
    try:
        data = request.get_json()
        primary_portal = data.get("primaryPortal")
        primary_channel_id = data.get("primaryChannelId")
        secondary_portal = data.get("secondaryPortal")
        secondary_channel_id = data.get("secondaryChannelId")

        if not all([primary_portal, primary_channel_id, secondary_portal, secondary_channel_id]):
            return jsonify({"success": False, "error": "Missing required fields"}), 400

        if primary_portal != secondary_portal:
            return jsonify({"success": False, "error": "Channels must be from the same portal"}), 400

        if primary_channel_id == secondary_channel_id:
            return jsonify({"success": False, "error": "Cannot merge a channel with itself"}), 400

        conn = get_db_connection()
        cursor = conn.cursor()

        # Get current alternate_ids and available_macs from primary channel
        cursor.execute(
            "SELECT alternate_ids, available_macs FROM channels WHERE portal = ? AND channel_id = ?",
            [primary_portal, primary_channel_id]
        )
        primary_row = cursor.fetchone()
        if not primary_row:
            conn.close()
            return jsonify({"success": False, "error": "Primary channel not found"}), 404

        # Get available_macs from secondary channel
        cursor.execute(
            "SELECT available_macs FROM channels WHERE portal = ? AND channel_id = ?",
            [secondary_portal, secondary_channel_id]
        )
        secondary_row = cursor.fetchone()
        if not secondary_row:
            conn.close()
            return jsonify({"success": False, "error": "Secondary channel not found"}), 404

        # Build new alternate_ids list
        current_alternates = []
        if primary_row[0]:
            current_alternates = [aid.strip() for aid in primary_row[0].split(",") if aid.strip()]

        if secondary_channel_id not in current_alternates:
            current_alternates.append(secondary_channel_id)

        new_alternate_ids = ",".join(current_alternates)

        # Merge available_macs (combine both, deduplicate)
        primary_macs = set()
        if primary_row[1]:
            primary_macs = set(m.strip() for m in primary_row[1].split(",") if m.strip())
        if secondary_row[0]:
            secondary_macs = set(m.strip() for m in secondary_row[0].split(",") if m.strip())
            primary_macs.update(secondary_macs)

        new_available_macs = ",".join(sorted(primary_macs))

        # Update primary channel with new alternate_ids and merged available_macs
        cursor.execute(
            "UPDATE channels SET alternate_ids = ?, available_macs = ? WHERE portal = ? AND channel_id = ?",
            [new_alternate_ids, new_available_macs, primary_portal, primary_channel_id]
        )

        # Delete the secondary channel
        cursor.execute(
            "DELETE FROM channels WHERE portal = ? AND channel_id = ?",
            [secondary_portal, secondary_channel_id]
        )

        conn.commit()
        conn.close()

        logger.info(f"Merged channel {secondary_channel_id} into {primary_channel_id} for portal {primary_portal}")

        return jsonify({
            "success": True,
            "message": f"Channel merged successfully. {secondary_channel_id} is now an alternate for {primary_channel_id}",
            "alternateIds": new_alternate_ids
        })

    except Exception as e:
        logger.error(f"Error merging channels: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/editor/search-for-merge", methods=["POST"])
@authorise
def editor_search_for_merge():
    """Search for channels to merge with (same portal, different ID)."""
    try:
        data = request.get_json()
        portal = data.get("portal")
        exclude_channel_id = data.get("excludeChannelId")
        query = data.get("query", "").strip()

        if not portal or not query or len(query) < 2:
            return jsonify({"success": True, "channels": []})

        conn = get_db_connection()
        cursor = conn.cursor()

        # Search by name or channel_id
        search_pattern = f"%{query}%"
        cursor.execute("""
            SELECT channel_id, name, custom_name, auto_name, genre
            FROM channels
            WHERE portal = ?
              AND channel_id != ?
              AND (name LIKE ? OR custom_name LIKE ? OR auto_name LIKE ? OR channel_id LIKE ?)
            LIMIT 10
        """, [portal, exclude_channel_id, search_pattern, search_pattern, search_pattern, search_pattern])

        channels = []
        for row in cursor.fetchall():
            effective_name = row["custom_name"] or row["auto_name"] or row["name"]
            channels.append({
                "channelId": row["channel_id"],
                "name": effective_name,
                "customName": row["custom_name"] or "",
                "genre": row["genre"] or ""
            })

        conn.close()
        return jsonify({"success": True, "channels": channels})

    except Exception as e:
        logger.error(f"Error searching channels for merge: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/editor/reset", methods=["POST"])
@app.route("/editor/reset", methods=["POST"])  # Keep old route for backward compatibility
@authorise
def editorReset():
    """Reset all channel customizations in the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            UPDATE channels 
            SET enabled = 0,
                custom_name = '',
                custom_number = '',
                custom_genre = '',
                custom_epg_id = ''
        ''')
        
        conn.commit()
        logger.info("All channel customizations reset!")
        flash("Playlist reset!", "success")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error resetting channels: {e}")
        flash(f"Error resetting: {e}", "danger")
    finally:
        conn.close()
    
    return redirect("/editor", code=302)


@app.route("/api/editor/refresh", methods=["POST"])
@app.route("/editor/refresh", methods=["POST"])  # Keep old route for backward compatibility
@authorise
def editorRefresh():
    """Manually trigger a refresh of the channel cache."""
    try:
        total = refresh_channels_cache()
        logger.info(f"Channel cache refreshed: {total} channels")
        return flask.jsonify({"status": "success", "total": total})
    except Exception as e:
        logger.error(f"Error refreshing channel cache: {e}")
        return flask.jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/portal/refresh", methods=["POST"])
@authorise
def refreshPortalChannels():
    """Refresh channels for a specific portal."""
    try:
        data = request.get_json()
        portal_id = data.get("portal_id")

        if not portal_id:
            return flask.jsonify({"status": "error", "message": "Portal ID required"}), 400

        portals = getPortals()
        if portal_id not in portals:
            return flask.jsonify({"status": "error", "message": "Portal not found"}), 404

        portal_name = portals[portal_id].get("name", portal_id)
        logger.info(f"Refreshing channels for portal: {portal_name}")

        total = refresh_channels_cache(target_portal_id=portal_id)
        logger.info(f"Portal {portal_name} channel refresh completed: {total} channels")

        # Get updated stats for this portal
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get channel counts
        cursor.execute(f"""
            SELECT
                COUNT(*) as total_channels,
                SUM(CASE WHEN {ACTIVE_GROUP_CONDITION} THEN 1 ELSE 0 END) as active_channels
            FROM channels c
            LEFT JOIN groups g ON c.portal = g.portal AND c.genre_id = g.genre_id
            WHERE c.portal = ?
        """, [portal_id])
        ch_row = cursor.fetchone()

        # Get group counts
        cursor.execute("""
            SELECT
                COUNT(*) as total_groups,
                SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) as active_groups
            FROM groups WHERE portal = ?
        """, [portal_id])
        gr_row = cursor.fetchone()
        conn.close()

        stats = {
            "total_channels": ch_row[0] or 0,
            "channels": ch_row[1] or 0,
            "total_groups": gr_row[0] or 0,
            "groups": gr_row[1] or 0
        }

        return flask.jsonify({
            "status": "success",
            "total": total,
            "portal": portal_name,
            "stats": stats
        })
    except Exception as e:
        logger.error(f"Error refreshing portal channels: {e}")
        return flask.jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/settings", methods=["GET"])
@authorise
def settings():
    """Legacy template route"""
    settings = getSettings()
    return render_template(
        "settings.html", settings=settings, defaultSettings=defaultSettings
    )


@app.route("/api/settings/data", methods=["GET"])
@authorise
def settings_data():
    """API endpoint to get settings"""
    return jsonify(getSettings())


@app.route("/settings/save", methods=["POST"])
@authorise
def save():
    settings = {}

    for setting, _ in defaultSettings.items():
        value = request.form.get(setting, "false")
        settings[setting] = value

    saveSettings(settings)
    logger.info("Settings saved!")
    Thread(target=refresh_xmltv).start()
    flash("Settings saved!", "success")
    return redirect("/settings", code=302)

# Route to serve the cached playlist.m3u
@app.route("/playlist.m3u", methods=["GET"])
@authorise
def playlist():
    global cached_playlist, last_playlist_host
    
    logger.info("Playlist Requested")
    
    # Detect the current host dynamically
    current_host = host
    
    # Regenerate the playlist if it is empty or the host has changed
    if cached_playlist is None or len(cached_playlist) == 0 or last_playlist_host != current_host:
        logger.info(f"Regenerating playlist due to host change: {last_playlist_host} -> {current_host}")
        last_playlist_host = current_host
        generate_playlist()

    return Response(cached_playlist, mimetype="text/plain")

# Function to manually trigger playlist update
@app.route("/update_playlistm3u", methods=["POST"])
def update_playlistm3u():
    generate_playlist()
    return Response("Playlist updated successfully", status=200)

def generate_playlist():
    global cached_playlist
    logger.info("Generating playlist.m3u from database...")

    # Detect the host dynamically from the request
    playlist_host = host
    
    channels = []
    
    # Read enabled channels from database
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Build order clause based on settings
    order_clause = ""
    if getSettings().get("sort playlist by channel name", "true") == "true":
        order_clause = "ORDER BY COALESCE(NULLIF(c.custom_name, ''), NULLIF(c.auto_name, ''), c.name)"
    elif getSettings().get("use channel numbers", "true") == "true":
        if getSettings().get("sort playlist by channel number", "false") == "true":
            order_clause = "ORDER BY CAST(COALESCE(NULLIF(c.custom_number, ''), c.number) AS INTEGER)"
    elif getSettings().get("use channel genres", "true") == "true":
        if getSettings().get("sort playlist by channel genre", "false") == "true":
            order_clause = "ORDER BY COALESCE(NULLIF(c.custom_genre, ''), c.genre)"
    
    cursor.execute(f'''
        SELECT
            c.portal, c.channel_id, c.name, c.number, c.genre,
            c.custom_name, c.auto_name, c.custom_number, c.custom_genre, c.custom_epg_id
        FROM channels c
        LEFT JOIN groups g ON c.portal = g.portal AND c.genre_id = g.genre_id
        WHERE c.enabled = 1 AND {ACTIVE_GROUP_CONDITION}
        {order_clause}
    ''')
    
    for row in cursor.fetchall():
        portal = row['portal']
        channel_id = row['channel_id']
        
        # Use custom values if available, otherwise use defaults
        channel_name = effective_channel_name(row['custom_name'], row['auto_name'], row['name'])
        channel_number = row['custom_number'] if row['custom_number'] else row['number']
        genre = row['custom_genre'] if row['custom_genre'] else row['genre']
        epg_id = row['custom_epg_id'] if row['custom_epg_id'] else channel_name
        
        channel_entry = "#EXTINF:-1" + ' tvg-id="' + epg_id
        
        if getSettings().get("use channel numbers", "true") == "true":
            channel_entry += '" tvg-chno="' + str(channel_number)
        
        if getSettings().get("use channel genres", "true") == "true":
            channel_entry += '" group-title="' + str(genre)
        
        channel_entry += '",' + channel_name + "\n"
        
        # Use HLS URL if output format is set to HLS, otherwise use MPEG-TS
        if getSettings().get("output format", "mpegts") == "hls":
            channel_entry += f"http://{playlist_host}/hls/{portal}/{channel_id}/master.m3u8"
        else:
            channel_entry += f"http://{playlist_host}/play/{portal}/{channel_id}"
        
        channels.append(channel_entry)
    
    conn.close()

    playlist = "#EXTM3U \n"
    playlist = playlist + "\n".join(channels)

    # Update the cache
    cached_playlist = playlist
    logger.info(f"Playlist generated and cached with {len(channels)} channels.")
    
def refresh_xmltv():
    global epg_refresh_status
    epg_refresh_status["is_refreshing"] = True
    epg_refresh_status["started_at"] = datetime.utcnow().isoformat()
    epg_refresh_status["last_error"] = None

    settings = getSettings()
    logger.info("Refreshing XMLTV...")

    # Get EPG settings
    epg_future_hours = int(settings.get("epg future hours", "24"))
    epg_past_hours = int(settings.get("epg past hours", "2"))

    # Set up paths for XMLTV cache
    user_dir = os.path.expanduser("~")
    cache_dir = os.path.join(user_dir, "Evilvir.us")
    os.makedirs(cache_dir, exist_ok=True)
    cache_file = os.path.join(cache_dir, "MacReplayEPG.xml")

    # Define date cutoff for programme filtering (based on epg past hours setting)
    past_cutoff = datetime.utcnow() - timedelta(hours=epg_past_hours)
    past_cutoff_str = past_cutoff.strftime("%Y%m%d%H%M%S") + " +0000"

    # Load existing cache if it exists
    cached_programmes = []
    if os.path.exists(cache_file):
        try:
            tree = ET.parse(cache_file)
            root = tree.getroot()
            for programme in root.findall("programme"):
                stop_attr = programme.get("stop")  # Get the 'stop' attribute
                if stop_attr:
                    try:
                        # Parse the stop time and compare with the cutoff
                        stop_time = datetime.strptime(stop_attr.split(" ")[0], "%Y%m%d%H%M%S")
                        if stop_time >= past_cutoff:  # Keep only recent programmes
                            cached_programmes.append(ET.tostring(programme, encoding="unicode"))
                    except ValueError as e:
                        logger.warning(f"Invalid stop time format in cached programme: {stop_attr}. Skipping.")
            logger.info("Loaded existing programme data from cache.")
        except Exception as e:
            logger.error(f"Failed to load cache file: {e}")

    # Initialize new XMLTV data
    channels_xml = ET.Element("tv")
    programmes = ET.Element("tv")
    portals = getPortals()

    # Read enabled channels from database (grouped by portal)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f'''
        SELECT
            c.portal, c.channel_id, c.name, c.number, c.logo,
            c.custom_name, c.auto_name, c.custom_number, c.custom_epg_id
        FROM channels c
        LEFT JOIN groups g ON c.portal = g.portal AND c.genre_id = g.genre_id
        WHERE c.enabled = 1 AND {ACTIVE_GROUP_CONDITION}
    ''')

    # Group enabled channels by portal
    enabled_by_portal = {}
    for row in cursor.fetchall():
        portal_id = row['portal']
        if portal_id not in enabled_by_portal:
            enabled_by_portal[portal_id] = []
        enabled_by_portal[portal_id].append({
            'channel_id': row['channel_id'],
            'name': row['name'],
            'number': row['number'],
            'logo': row['logo'],
            'custom_name': row['custom_name'],
            'auto_name': row['auto_name'],
            'custom_number': row['custom_number'],
            'custom_epg_id': row['custom_epg_id']
        })
    conn.close()

    logger.info(f"Found {sum(len(v) for v in enabled_by_portal.values())} enabled channels across {len(enabled_by_portal)} portals")

    # Track seen channel IDs to prevent duplicate channel definitions
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
            logger.info(f"Fetching EPG | Portal: {portal_name} | offset: {portal_epg_offset} | channels: {len(enabled_by_portal[portal_id])}")
        else:
            logger.info(f"Skipping EPG fetch for Portal: {portal_name} (disabled) | channels: {len(enabled_by_portal[portal_id])}")

        url = portal["url"]
        macs = list(portal["macs"].keys())
        proxy = portal.get("proxy", "")

        # Try to get EPG data from portal (only if fetch epg is enabled)
        allChannels = None
        epg = None
        if fetch_epg:
            for mac in macs:
                try:
                    token = stb.getToken(url, mac, proxy)
                    stb.getProfile(url, mac, token, proxy)
                    allChannels = stb.getAllChannels(url, mac, token, proxy)
                    epg = stb.getEpg(url, mac, token, epg_future_hours, proxy)
                    if epg:
                        logger.info(f"Successfully fetched EPG from MAC {mac}")
                        break
                except Exception as e:
                    logger.error(f"Error fetching data for MAC {mac}: {e}")
                    continue

            if not epg:
                logger.warning(f"Could not fetch EPG for portal {portal_name}, creating dummy entries")

        # Process enabled channels for this portal
        for ch in enabled_by_portal[portal_id]:
            try:
                channelId = str(ch['channel_id'])
                channelName = effective_channel_name(ch['custom_name'], ch['auto_name'], ch['name'])
                channelNumber = ch['custom_number'] if ch['custom_number'] else str(ch['number'])
                epgId = ch['custom_epg_id'] if ch['custom_epg_id'] else channelName
                channelLogo = ch['logo'] or ""

                # Skip if this channel (by epgId) was already added from another portal
                if epgId in seen_channel_ids:
                    logger.debug(f"Skipping duplicate channel: {channelName} (epgId: {epgId})")
                    continue
                seen_channel_ids.add(epgId)

                # Add channel to XML
                channelEle = ET.SubElement(channels_xml, "channel", id=epgId)
                ET.SubElement(channelEle, "display-name").text = channelName
                if channelLogo:
                    ET.SubElement(channelEle, "icon", src=channelLogo)

                # Add programme data
                if not epg or channelId not in epg or not epg.get(channelId):
                    logger.debug(f"No EPG data found for channel {channelName} (ID: {channelId}), creating dummy entry")
                    start_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
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
                            start_time = datetime.utcfromtimestamp(p.get("start_timestamp")) + timedelta(hours=portal_epg_offset)
                            stop_time = datetime.utcfromtimestamp(p.get("stop_timestamp")) + timedelta(hours=portal_epg_offset)
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
                            logger.error(f"Error processing programme for channel {channelName} (ID: {channelId}): {e}")
            except Exception as e:
                logger.error(f"Error processing channel {ch}: {e}")

    # Combine channels and programmes into a single XML document
    xmltv = channels_xml

    # Track seen programmes by (channel, start, stop) to prevent duplicates
    seen_programmes = set()

    for programme in programmes.iter("programme"):
        prog_key = (programme.get("channel"), programme.get("start"), programme.get("stop"))
        if prog_key not in seen_programmes:
            seen_programmes.add(prog_key)
            xmltv.append(programme)

    # Add cached programmes, ensuring no duplicates based on (channel, start, stop)
    for cached in cached_programmes:
        prog_elem = ET.fromstring(cached)
        prog_key = (prog_elem.get("channel"), prog_elem.get("start"), prog_elem.get("stop"))
        if prog_key not in seen_programmes:
            seen_programmes.add(prog_key)
            xmltv.append(prog_elem)

    logger.info(f"EPG: {len(seen_programmes)} unique programmes after deduplication")

    # Pretty-print the XML with blank line removal
    rough_string = ET.tostring(xmltv, encoding="unicode")
    reparsed = minidom.parseString(rough_string)
    formatted_xmltv = "\n".join([line for line in reparsed.toprettyxml(indent="  ").splitlines() if line.strip()])

    # Save updated cache
    with open(cache_file, "w", encoding="utf-8") as f:
        f.write(formatted_xmltv)
    logger.info("XMLTV cache updated.")

    # Update global cache
    global cached_xmltv, last_updated
    cached_xmltv = formatted_xmltv
    last_updated = time.time()
    logger.debug(f"Generated XMLTV: {formatted_xmltv}")

    # Save to persistent cache file
    save_epg_cache()

    # Update refresh status
    epg_refresh_status["is_refreshing"] = False
    epg_refresh_status["completed_at"] = datetime.utcnow().isoformat()
    logger.info("EPG refresh completed successfully.")


# Endpoint to get the XMLTV data
@app.route("/xmltv", methods=["GET"])
@authorise
def xmltv():
    global cached_xmltv, last_updated
    logger.info("Guide Requested")

    # If no cache exists at all, we must wait for initial fetch
    if cached_xmltv is None:
        logger.info("No EPG cache exists, fetching now (this may take a moment)...")
        refresh_xmltv()
    # If cache exists but is stale, trigger background refresh and return cached data immediately
    elif (time.time() - last_updated) > 900:  # 900 seconds = 15 minutes
        logger.info("EPG cache is stale, triggering background refresh...")
        threading.Thread(target=refresh_xmltv, daemon=True).start()

    return Response(
        cached_xmltv,
        mimetype="text/xml",
    )


# EPG Viewer page
@app.route("/epg")
@authorise
def epg_viewer():
    return render_template("epg.html")


# API endpoint for EPG data (JSON format for the viewer)
@app.route("/api/epg")
@authorise
def api_epg():
    """Return EPG data as JSON for the EPG viewer."""
    global cached_xmltv

    if cached_xmltv is None:
        return jsonify({"channels": [], "programmes": []})

    try:
        # Parse the cached XMLTV data
        root = ET.fromstring(cached_xmltv)

        # Get portal names for channels from database
        portals = getPortals()
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT portal, name, custom_name, auto_name, custom_epg_id
            FROM channels WHERE enabled = 1
        ''')
        channel_portal_map = {}
        for row in cursor.fetchall():
            # EPG ID is custom_epg_id if set, otherwise channel name
            channel_name = effective_channel_name(row['custom_name'], row['auto_name'], row['name'])
            epg_id = row['custom_epg_id'] if row['custom_epg_id'] else channel_name
            portal_id = row['portal']
            portal_name = portals.get(portal_id, {}).get('name', portal_id)
            channel_portal_map[epg_id] = portal_name
        conn.close()

        channels = []
        for channel in root.findall("channel"):
            channel_id = channel.get("id")
            display_name = channel.find("display-name")
            icon = channel.find("icon")
            channels.append({
                "id": channel_id,
                "name": display_name.text if display_name is not None else channel_id,
                "logo": icon.get("src") if icon is not None else None,
                "portal": channel_portal_map.get(channel_id, "")
            })

        programmes = []
        now = datetime.now(timezone.utc)

        def parse_xmltv_time(time_str):
            """Parse XMLTV time format: 20240126120000 +0100"""
            if not time_str:
                return None
            try:
                # Split into datetime and timezone parts
                parts = time_str.split(" ")
                dt_str = parts[0]

                # Parse the datetime part
                dt = datetime.strptime(dt_str, "%Y%m%d%H%M%S")

                # Parse timezone offset if present
                if len(parts) > 1:
                    tz_str = parts[1]  # e.g., "+0100" or "-0500"
                    tz_sign = 1 if tz_str[0] == '+' else -1
                    tz_hours = int(tz_str[1:3])
                    tz_mins = int(tz_str[3:5]) if len(tz_str) >= 5 else 0
                    tz_offset = timedelta(hours=tz_sign * tz_hours, minutes=tz_sign * tz_mins)
                    # Create timezone-aware datetime and convert to UTC
                    dt = dt.replace(tzinfo=timezone(tz_offset))
                    dt = dt.astimezone(timezone.utc)
                else:
                    # No timezone info, assume UTC
                    dt = dt.replace(tzinfo=timezone.utc)

                return dt
            except (ValueError, AttributeError, IndexError) as e:
                logger.debug(f"Error parsing XMLTV time '{time_str}': {e}")
                return None

        for programme in root.findall("programme"):
            channel_id = programme.get("channel")
            start_str = programme.get("start")
            stop_str = programme.get("stop")

            start_time = parse_xmltv_time(start_str)
            stop_time = parse_xmltv_time(stop_str)

            if not start_time or not stop_time:
                continue

            title_elem = programme.find("title")
            desc_elem = programme.find("desc")

            programmes.append({
                "channel": channel_id,
                "start": start_time.isoformat(),
                "stop": stop_time.isoformat(),
                "start_timestamp": start_time.timestamp(),
                "stop_timestamp": stop_time.timestamp(),
                "title": title_elem.text if title_elem is not None else "Unknown",
                "description": desc_elem.text if desc_elem is not None else "",
                "is_current": start_time <= now <= stop_time,
                "is_past": stop_time < now
            })

        # Sort programmes by start time
        programmes.sort(key=lambda x: x["start_timestamp"])

        # Debug info: find time range of programmes
        if programmes:
            earliest = min(p["start_timestamp"] for p in programmes)
            latest = max(p["stop_timestamp"] for p in programmes)
            current_count = sum(1 for p in programmes if p["is_current"])
            logger.debug(f"EPG API: {len(programmes)} programmes, {current_count} current, range: {datetime.utcfromtimestamp(earliest)} - {datetime.utcfromtimestamp(latest)} UTC")

        # Calculate time range
        earliest_ts = min(p["start_timestamp"] for p in programmes) if programmes else 0
        latest_ts = max(p["stop_timestamp"] for p in programmes) if programmes else 0

        return jsonify({
            "channels": channels,
            "programmes": programmes,
            "last_updated": last_updated,
            "current_time": now.isoformat(),
            "debug": {
                "server_time_utc": now.isoformat(),
                "container_tz": os.environ.get("TZ", "UTC"),
                "programme_count": len(programmes),
                "current_programme_count": sum(1 for p in programmes if p["is_current"]),
                "earliest_programme": datetime.utcfromtimestamp(earliest_ts).isoformat() + "Z" if earliest_ts else None,
                "latest_programme": datetime.utcfromtimestamp(latest_ts).isoformat() + "Z" if latest_ts else None
            }
        })

    except Exception as e:
        logger.error(f"Error parsing EPG data: {e}")
        return jsonify({"error": str(e), "channels": [], "programmes": []})


@app.route("/api/epg/refresh", methods=["POST"])
@authorise
def api_epg_refresh():
    """Trigger a manual EPG refresh."""
    global epg_refresh_status
    try:
        # Check if already refreshing
        if epg_refresh_status["is_refreshing"]:
            return jsonify({
                "status": "already_running",
                "message": "EPG refresh is already in progress",
                "started_at": epg_refresh_status["started_at"]
            })

        # Start refresh in background thread
        refresh_thread = threading.Thread(target=refresh_xmltv, daemon=True)
        refresh_thread.start()
        logger.info("Manual EPG refresh triggered via API")
        return jsonify({"status": "started", "message": "EPG refresh started"})
    except Exception as e:
        logger.error(f"Error triggering EPG refresh: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/epg/status", methods=["GET"])
@authorise
def api_epg_status():
    """Get EPG refresh status."""
    return jsonify({
        "is_refreshing": epg_refresh_status["is_refreshing"],
        "started_at": epg_refresh_status["started_at"],
        "completed_at": epg_refresh_status["completed_at"],
        "last_error": epg_refresh_status["last_error"],
        "last_updated": last_updated
    })


@app.route("/play/<portalId>/<channelId>", methods=["GET"])
def channel(portalId, channelId):
    def streamData():
        def occupy():
            occupied.setdefault(portalId, [])
            occupied.get(portalId, []).append(
                {
                    "mac": mac,
                    "channel id": channelId,
                    "channel name": channelName,
                    "client": ip,
                    "portal name": portalName,
                    "start time": startTime,
                }
            )
            logger.info("Occupied Portal({}):MAC({})".format(portalId, mac))

        def unoccupy():
            occupied.get(portalId, []).remove(
                {
                    "mac": mac,
                    "channel id": channelId,
                    "channel name": channelName,
                    "client": ip,
                    "portal name": portalName,
                    "start time": startTime,
                }
            )
            logger.info("Unoccupied Portal({}):MAC({})".format(portalId, mac))

        try:
            startTime = datetime.now(timezone.utc).timestamp()
            occupy()
            with subprocess.Popen(
                ffmpegcmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            ) as ffmpeg_sp:
                while True:
                    chunk = ffmpeg_sp.stdout.read(1024)
                    if len(chunk) == 0:
                        if ffmpeg_sp.poll() != 0:
                            logger.info("Ffmpeg closed with error({}). Moving MAC({}) for Portal({})".format(str(ffmpeg_sp.poll()), mac, portalName))
                            moveMac(portalId, mac)
                        break
                    yield chunk
        except:
            pass
        finally:
            unoccupy()
            ffmpeg_sp.kill()

    def testStream():
        timeout = int(getSettings()["ffmpeg timeout"]) * int(1000000)
        ffprobecmd = ["ffprobe", "-timeout", str(timeout), "-i", link]

        if proxy:
            ffprobecmd.insert(1, "-http_proxy")
            ffprobecmd.insert(2, proxy)

        with subprocess.Popen(
            ffprobecmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ) as ffprobe_sb:
            ffprobe_sb.communicate()
            if ffprobe_sb.returncode == 0:
                return True
            else:
                return False

    def isMacFree():
        count = 0
        for i in occupied.get(portalId, []):
            if i["mac"] == mac:
                count = count + 1
        if count < streamsPerMac:
            return True
        else:
            return False

    portal = getPortals().get(portalId)
    portalName = portal.get("name")
    url = portal.get("url")
    streamsPerMac = int(portal.get("streams per mac"))
    proxy = portal.get("proxy")
    web = request.args.get("web")
    ip = request.remote_addr
    channelName = portal.get("custom channel names", {}).get(channelId)

    # Get available_macs, alternate_ids, cmd, and name from database
    available_macs = []
    alternate_ids = []
    cached_cmd = None
    cached_channel_name = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT available_macs, alternate_ids, cmd, name FROM channels WHERE portal = ? AND channel_id = ?", [portalId, channelId])
        row = cursor.fetchone()
        if row:
            if row[0]:
                available_macs = [m.strip() for m in row[0].split(",") if m.strip()]
            if row[1]:
                alternate_ids = [aid.strip() for aid in row[1].split(",") if aid.strip()]
            if row[2]:
                cached_cmd = row[2]
            if row[3]:
                cached_channel_name = row[3]
        conn.close()
    except Exception as e:
        logger.debug(f"Could not get channel data for channel {channelId}: {e}")

    # Build list of channel IDs to try (primary first, then alternates)
    channel_ids_to_try = [channelId] + alternate_ids
    if alternate_ids:
        logger.debug(f"Channel {channelId} has alternate IDs: {alternate_ids}")

    # MACs nach Score sortieren (idle MACs bevorzugen)
    macs_dict = portal["macs"]
    occupied_list = occupied.get(portalId, [])
    mac_scores = []
    for mac, mac_data in macs_dict.items():
        score = score_mac_for_selection(mac, mac_data, occupied_list, streamsPerMac)
        mac_scores.append((mac, score))

    # Nach Score sortieren (höchster Score zuerst), MACs mit Score -1 ans Ende
    mac_scores.sort(key=lambda x: (x[1] >= 0, x[1]), reverse=True)
    macs = [m[0] for m in mac_scores if m[1] >= 0] or list(macs_dict.keys())

    # Prioritize MACs that are known to have this channel (from available_macs)
    # Keep score-sorted order within available_macs, but put them first
    if available_macs:
        # Filter to MACs that are both available for this channel AND in our portal
        valid_available = [m for m in macs if m in available_macs]
        other_macs = [m for m in macs if m not in available_macs]
        if valid_available:
            macs = valid_available + other_macs
            logger.debug(f"Prioritizing {len(valid_available)} available MACs for channel {channelId}")

    logger.debug(f"MAC scores for Portal({portalName}): {mac_scores[:5]}")  # Log top 5

    logger.info(
        "IP({}) requested Portal({}):Channel({})".format(ip, portalId, channelId)
    )

    # Helper function to probe a single MAC
    def probe_single_mac(mac_to_test):
        """Probe a single MAC and return result dict or None if failed."""
        try:
            if streamsPerMac != 0 and not isMacFree():
                return None

            logger.info(
                "Trying Portal({}):MAC({}):Channel({})".format(portalId, mac_to_test, channelId)
            )

            token = stb.getToken(url, mac_to_test, proxy)
            if not token:
                return None

            stb.getProfile(url, mac_to_test, token, proxy)

            cmd = None
            found_channel_name = portal.get("custom channel names", {}).get(channelId) or cached_channel_name

            # OPTIMIZATION: Use cached cmd from database if available (skips getAllChannels!)
            if cached_cmd:
                cmd = cached_cmd
                logger.debug(f"Using cached cmd for channel {channelId}")
            else:
                # Fallback: fetch all channels (slow, only for channels without cached cmd)
                logger.debug(f"No cached cmd, fetching all channels for MAC {mac_to_test}")
                channels = stb.getAllChannels(url, mac_to_test, token, proxy)

                if not channels:
                    return None

                used_channel_id = channelId

                # Try primary channel ID first, then alternates
                for try_channel_id in channel_ids_to_try:
                    for c in channels:
                        if str(c["id"]) == try_channel_id:
                            if found_channel_name is None:
                                found_channel_name = c["name"]
                            cmd = c["cmd"]
                            used_channel_id = try_channel_id
                            if try_channel_id != channelId:
                                logger.info(f"Using alternate channel ID {try_channel_id} instead of {channelId}")
                            break
                    if cmd:
                        break

            if not cmd:
                return None

            if "http://localhost/" in cmd:
                link = stb.getLink(url, mac_to_test, token, cmd, proxy)
            else:
                link = cmd.split(" ")[1]

            if not link:
                return None

            # Test stream if enabled
            if getSettings().get("test streams", "true") != "false":
                timeout = int(getSettings()["ffmpeg timeout"]) * int(1000000)
                ffprobecmd = ["ffprobe", "-timeout", str(timeout), "-i", link]
                if proxy:
                    ffprobecmd.insert(1, "-http_proxy")
                    ffprobecmd.insert(2, proxy)

                with subprocess.Popen(
                    ffprobecmd,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                ) as ffprobe_sb:
                    ffprobe_sb.communicate()
                    if ffprobe_sb.returncode != 0:
                        return None

            return {
                "mac": mac_to_test,
                "token": token,
                "link": link,
                "channelName": found_channel_name
            }
        except Exception as e:
            logger.error(f"Error probing MAC({mac_to_test}): {e}")
            return None

    freeMac = False
    result = None
    failed_macs = []

    # Check if parallel probing is enabled
    parallel_enabled = getSettings().get("parallel mac probing", "false") == "true"
    max_workers = int(getSettings().get("parallel mac workers", "3"))

    if parallel_enabled and len(macs) > 1:
        # Parallel MAC probing
        logger.info(f"Using parallel MAC probing with {max_workers} workers for {len(macs)} MACs")

        with ThreadPoolExecutor(max_workers=min(max_workers, len(macs))) as executor:
            # Submit all MAC probing tasks
            future_to_mac = {executor.submit(probe_single_mac, mac): mac for mac in macs}

            # Process results as they complete
            for future in as_completed(future_to_mac):
                mac = future_to_mac[future]
                try:
                    probe_result = future.result()
                    if probe_result:
                        result = probe_result
                        freeMac = True
                        # Cancel remaining futures
                        for f in future_to_mac:
                            f.cancel()
                        break
                    else:
                        failed_macs.append(mac)
                except Exception as e:
                    logger.error(f"Exception probing MAC({mac}): {e}")
                    failed_macs.append(mac)
    else:
        # Sequential MAC probing (original behavior)
        for mac in macs:
            probe_result = probe_single_mac(mac)
            if probe_result:
                result = probe_result
                freeMac = True
                break
            else:
                failed_macs.append(mac)
                if not getSettings().get("try all macs", "true") == "true":
                    break

    # Move failed MACs to end of list
    for failed_mac in failed_macs:
        logger.info("Moving MAC({}) for Portal({})".format(failed_mac, portalName))
        moveMac(portalId, failed_mac)

    # If we found a working MAC, stream it
    if result:
        mac = result["mac"]
        link = result["link"]
        channelName = result["channelName"]

        if web:
            ffmpegcmd = [
                "ffmpeg",
                "-loglevel",
                "panic",
                "-hide_banner",
                "-i",
                link,
                "-vcodec",
                "copy",
                "-f",
                "mp4",
                "-movflags",
                "frag_keyframe+empty_moov",
                "pipe:",
            ]
            if proxy:
                ffmpegcmd.insert(1, "-http_proxy")
                ffmpegcmd.insert(2, proxy)
            return Response(streamData(), mimetype="application/octet-stream")

        else:
            if getSettings().get("stream method", "ffmpeg") == "ffmpeg":
                ffmpegcmd = str(getSettings()["ffmpeg command"])
                ffmpegcmd = ffmpegcmd.replace("<url>", link)
                ffmpegcmd = ffmpegcmd.replace(
                    "<timeout>",
                    str(int(getSettings()["ffmpeg timeout"]) * int(1000000)),
                )
                if proxy:
                    ffmpegcmd = ffmpegcmd.replace("<proxy>", proxy)
                else:
                    ffmpegcmd = ffmpegcmd.replace("-http_proxy <proxy>", "")
                " ".join(ffmpegcmd.split())  # cleans up multiple whitespaces
                ffmpegcmd = ffmpegcmd.split()
                return Response(
                    streamData(), mimetype="application/octet-stream"
                )
            else:
                logger.info("Redirect sent")
                return redirect(link)

    if freeMac:
        logger.info(
            "No working streams found for Portal({}):Channel({})".format(
                portalId, channelId
            )
        )
    else:
        logger.info(
            "No free MAC for Portal({}):Channel({})".format(portalId, channelId)
        )

    return make_response("No streams available", 503)


@app.route("/hls/<portalId>/<channelId>/<path:filename>", methods=["GET"])
def hls_stream(portalId, channelId, filename):
    """Serve HLS streams (playlists and segments)."""
    
    # Get portal info
    portal = getPortals().get(portalId)
    if not portal:
        logger.error(f"Portal {portalId} not found for HLS request")
        return make_response("Portal not found", 404)
    
    portalName = portal.get("name")
    url = portal.get("url")
    macs = list(portal["macs"].keys())
    proxy = portal.get("proxy")
    ip = request.remote_addr
    
    logger.info(f"HLS request from IP({ip}) for Portal({portalId}):Channel({channelId}):File({filename})")
    
    # Check if we already have this stream
    stream_key = f"{portalId}_{channelId}"
    
    # First, check if stream is already active
    stream_exists = stream_key in hls_manager.streams
    
    if stream_exists:
        logger.debug(f"Stream already active for {stream_key}, checking for file: {filename}")
        # For active streams, wait a bit for the file if it's a playlist
        if filename.endswith('.m3u8'):
            is_passthrough = hls_manager.streams[stream_key].get('is_passthrough', False)
            max_wait = 100 if not is_passthrough else 10  # 10s for FFmpeg, 1s for passthrough
            logger.debug(f"Waiting for {filename} from active stream (passthrough={is_passthrough})")
            
            for wait_count in range(max_wait):
                file_path = hls_manager.get_file(portalId, channelId, filename)
                if file_path:
                    logger.debug(f"File ready after {wait_count * 0.1:.1f}s")
                    break
                time.sleep(0.1)
        else:
            # For segments, just try to get the file
            file_path = hls_manager.get_file(portalId, channelId, filename)
    else:
        logger.debug(f"Stream not active, will need to start it")
        file_path = None
    
    # If file doesn't exist and this is a playlist/segment request, start the stream
    if not file_path and (filename.endswith('.m3u8') or filename.endswith('.ts') or filename.endswith('.m4s')):
        # Get the stream URL
        logger.debug(f"Fetching stream URL for channel {channelId} from portal {portalName}")
        link = None
        for mac in macs:
            try:
                logger.debug(f"Trying MAC: {mac}")
                token = stb.getToken(url, mac, proxy)
                if token:
                    stb.getProfile(url, mac, token, proxy)
                    channels = stb.getAllChannels(url, mac, token, proxy)
                    
                    if channels:
                        for c in channels:
                            if str(c["id"]) == channelId:
                                cmd = c["cmd"]
                                if "http://localhost/" in cmd:
                                    link = stb.getLink(url, mac, token, cmd, proxy)
                                else:
                                    link = cmd.split(" ")[1]
                                logger.debug(f"Found stream URL for channel {channelId}")
                                break
                    
                    if link:
                        break
            except Exception as e:
                logger.error(f"Error getting stream URL for HLS with MAC {mac}: {e}")
                continue
        
        if not link:
            logger.error(f"✗ Could not get stream URL for Portal({portalId}):Channel({channelId}) - tried {len(macs)} MAC(s)")
            return make_response("Stream not available", 503)
        
        # Start the HLS stream
        try:
            logger.debug(f"Starting new stream for {stream_key}")
            stream_info = hls_manager.start_stream(portalId, channelId, link, proxy)
            
            # Wait for FFmpeg to create the requested file
            # For non-passthrough streams, FFmpeg needs time to start encoding
            is_passthrough = stream_info.get('is_passthrough', False)
            
            if filename.endswith('.m3u8'):
                # For playlist requests, wait up to 10 seconds for FFmpeg to create the file
                logger.debug(f"Waiting for playlist file: {filename} (passthrough={is_passthrough})")
                max_wait = 100 if not is_passthrough else 10  # 10s for FFmpeg, 1s for passthrough
                
                for wait_count in range(max_wait):
                    file_path = hls_manager.get_file(portalId, channelId, filename)
                    if file_path:
                        logger.debug(f"Playlist ready after {wait_count * 0.1:.1f}s")
                        break
                    time.sleep(0.1)
                
                if not file_path:
                    logger.warning(f"Playlist {filename} not ready after {max_wait * 0.1:.0f} seconds")
                    # Check if FFmpeg process crashed
                    if not is_passthrough and stream_key in hls_manager.streams:
                        process = hls_manager.streams[stream_key]['process']
                        if process.poll() is not None:
                            logger.error(f"FFmpeg crashed during startup (exit code: {process.returncode})")
                        else:
                            # FFmpeg is still running, check what files exist in temp dir
                            temp_dir = hls_manager.streams[stream_key]['temp_dir']
                            try:
                                files = os.listdir(temp_dir)
                                logger.warning(f"FFmpeg still running but {filename} not found. Temp dir contains: {files}")
                            except Exception as e:
                                logger.error(f"Could not list temp dir: {e}")
            else:
                # For segment requests, wait a bit for the segment to be created
                logger.debug(f"Waiting for segment file: {filename}")
                for wait_count in range(30):  # 30 * 0.1 = 3 seconds
                    file_path = hls_manager.get_file(portalId, channelId, filename)
                    if file_path:
                        logger.debug(f"Segment ready after {wait_count * 0.1:.1f}s")
                        break
                    time.sleep(0.1)
                
                if not file_path:
                    logger.warning(f"Segment {filename} not ready after 3 seconds")
        
        except Exception as e:
            logger.error(f"✗ Error starting HLS stream: {e}")
            logger.debug(f"Exception details: {type(e).__name__}: {str(e)}")
            return make_response("Error starting stream", 500)
    
    # Serve the file
    if file_path and os.path.exists(file_path):
        try:
            if filename.endswith('.m3u8'):
                mimetype = 'application/vnd.apple.mpegurl'
            elif filename.endswith('.ts'):
                mimetype = 'video/mp2t'
            elif filename.endswith('.m4s') or filename.endswith('.mp4'):
                mimetype = 'video/mp4'
            else:
                mimetype = 'application/octet-stream'
            
            file_size = os.path.getsize(file_path)
            logger.debug(f"Serving {filename} ({file_size} bytes, {mimetype})")
            
            # For playlist files, log what segments are actually available
            if filename.endswith('.m3u8') and file_path:
                try:
                    temp_dir = hls_manager.streams[stream_key]['temp_dir']
                    available_files = [f for f in os.listdir(temp_dir) if f.endswith('.ts') or f.endswith('.m4s')]
                    logger.debug(f"Available segments in temp dir: {sorted(available_files)}")
                except Exception as e:
                    logger.debug(f"Could not list segments: {e}")
            
            # For playlists, log the content for debugging
            if filename.endswith('.m3u8') and file_size < 5000:  # Only log small playlists
                try:
                    with open(file_path, 'r') as f:
                        content = f.read()
                        logger.debug(f"Playlist content:\n{content}")
                except Exception as e:
                    logger.debug(f"Could not read playlist content: {e}")
            
            return send_file(file_path, mimetype=mimetype)
        except Exception as e:
            logger.error(f"✗ Error serving HLS file {filename}: {e}")
            return make_response("Error serving file", 500)
    else:
        logger.warning(f"✗ HLS file not found: {filename} for {stream_key}")
        return make_response("File not found", 404)


@app.route("/api/dashboard")
@authorise
def dashboard():
    """Legacy template route"""
    return render_template("dashboard.html")


@app.route("/streaming")
@authorise
def streaming():
    return flask.jsonify(occupied)


@app.route("/log")
@authorise
def log():
    logFilePath = os.path.join(LOG_DIR, "MacReplay.log")
    try:
        with open(logFilePath) as f:
            return f.read()
    except FileNotFoundError:
        return "Log file not found"


@app.route("/logs")
@authorise
def logs_page():
    return render_template("logs.html")


@app.route("/logs/stream")
@authorise
def logs_stream():
    logFilePath = os.path.join(LOG_DIR, "MacReplay.log")
    lines_param = request.args.get('lines', '500')

    try:
        with open(logFilePath, 'r', encoding='utf-8', errors='replace') as f:
            all_lines = f.readlines()

        # Clean up lines (remove empty lines, strip whitespace)
        all_lines = [line.rstrip() for line in all_lines if line.strip()]

        if lines_param != 'all':
            try:
                num_lines = int(lines_param)
                all_lines = all_lines[-num_lines:]
            except ValueError:
                pass

        return flask.jsonify({"lines": all_lines, "total": len(all_lines)})
    except FileNotFoundError:
        return flask.jsonify({"lines": [], "error": "Log file not found"})
    except Exception as e:
        return flask.jsonify({"lines": [], "error": str(e)})


# HD Homerun #


def hdhr(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        settings = getSettings()
        security = settings["enable security"]
        username = settings["username"]
        password = settings["password"]
        hdhrenabled = settings["enable hdhr"]
        if (
            security == "false"
            or auth
            and auth.username == username
            and auth.password == password
        ):
            if hdhrenabled:
                return f(*args, **kwargs)
        return make_response("Error", 404)

    return decorated


@app.route("/discover.json", methods=["GET"])
@hdhr
def discover():
    logger.info("HDHR Status Requested.")
    settings = getSettings()
    name = settings["hdhr name"]
    id = settings["hdhr id"]
    tuners = settings["hdhr tuners"]
    data = {
        "BaseURL": host,
        "DeviceAuth": name,
        "DeviceID": id,
        "FirmwareName": "MacReplay",
        "FirmwareVersion": "666",
        "FriendlyName": name,
        "LineupURL": host + "/lineup.json",
        "Manufacturer": "Evilvirus",
        "ModelNumber": "666",
        "TunerCount": int(tuners),
    }
    return flask.jsonify(data)


@app.route("/lineup_status.json", methods=["GET"])
@hdhr
def status():
    data = {
        "ScanInProgress": 0,
        "ScanPossible": 0,
        "Source": "Cable",
        "SourceList": ["Cable"],
    }
    return flask.jsonify(data)


# Function to refresh the lineup
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
    
    
# Endpoint to get the current lineup
@app.route("/lineup.json", methods=["GET"])
@app.route("/lineup.post", methods=["POST"])
@hdhr
def lineup():
    logger.info("Lineup Requested")
    if not cached_lineup:  # Refresh lineup if cache is empty
        refresh_lineup()
    logger.info("Lineup Delivered")
    return jsonify(cached_lineup)

# Endpoint to manually refresh the lineup
@app.route("/refresh_lineup", methods=["POST"])
def refresh_lineup_endpoint():
    refresh_lineup()
    return jsonify({"status": "Lineup refreshed successfully"})

@app.route("/", methods=["GET"])
def home():
    """Serve React app"""
    try:
        return app.send_static_file('dist/index.html')
    except:
        # Fallback to redirect if React build doesn't exist
        return redirect("/api/portals", code=302)


# Catch-all route to redirect to template routes or serve static files
# This must be the last route defined!
@app.route("/<path:path>")
def catch_all(path):
    """Redirect to template routes or serve static files"""
    # Redirect template routes to their API equivalents
    if path == 'portals':
        return redirect("/api/portals", code=302)
    elif path == 'editor':
        return redirect("/api/editor", code=302)
    elif path == 'settings':
        return redirect("/api/settings", code=302)
    elif path == 'dashboard':
        return redirect("/api/dashboard", code=302)
    
    # Check if it's a file in static/dist (like assets)
    try:
        return app.send_static_file(f'dist/{path}')
    except:
        # For any other path, redirect to portals (main page)
        return redirect("/api/portals", code=302)


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
        cache_loaded = load_epg_cache()

        if cache_loaded and is_epg_cache_valid():
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
    config = loadConfig()
    
    # Initialize the database
    init_db()

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
