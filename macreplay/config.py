import json
import os
import tempfile
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime
try:
    import fcntl  # Unix-only file locking
except Exception:  # pragma: no cover - non-Unix platforms
    fcntl = None

# ----------------------------
# Docker / Volume friendly paths
# ----------------------------
DATA_DIR = os.getenv("DATA_DIR", "/app/data")
LOG_DIR = os.getenv("LOG_DIR", "/app/logs")

# CONFIG: allow absolute config file path from env
CONFIG_PATH = os.getenv("CONFIG", os.path.join(DATA_DIR, "MacReplay.json"))

# DB: allow absolute db path from env
DB_PATH = os.getenv("DB_PATH", os.path.join(DATA_DIR, "channels.db"))

# EPG Cache: allow absolute path from env
EPG_CACHE_PATH = os.getenv("EPG_CACHE_PATH", os.path.join(DATA_DIR, "epg_cache.xml"))

# Ensure directories exist
os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# In-memory config (mirrors original app.py behavior)
config = {}
_config_lock = threading.Lock()
_lock_path = CONFIG_PATH + ".lock"


def is_true(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).lower() == "true"


def _coerce_value(default, value):
    if value is None:
        return default
    if isinstance(default, bool):
        return is_true(value)
    if isinstance(default, int) and not isinstance(default, bool):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default
    if isinstance(default, float):
        try:
            return float(value)
        except (TypeError, ValueError):
            return default
    if isinstance(default, list):
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                return parsed if isinstance(parsed, list) else default
            except Exception:
                return default
        return default
    if isinstance(default, str):
        return str(value)
    return value


def _coerce_settings(settings):
    settings_out = {}
    for setting, default in defaultSettings.items():
        settings_out[setting] = _coerce_value(default, settings.get(setting))
    return settings_out


def _coerce_portals(portals):
    portals_out = {}
    for portal_id, pdata in portals.items():
        portals_out[portal_id] = {}
        for setting, default in defaultPortal.items():
            portals_out[portal_id][setting] = _coerce_value(
                default, pdata.get(setting)
            )
    return portals_out


@contextmanager
def _file_lock():
    """Best-effort cross-process lock using fcntl on Unix."""
    if fcntl is None:
        yield
        return
    os.makedirs(os.path.dirname(_lock_path), exist_ok=True)
    with open(_lock_path, "w", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


defaultSettings = {
    "stream method": "ffmpeg",
    "output format": "mpegts",
    "ffmpeg command": "-re -http_proxy <proxy> -timeout <timeout> -i <url> -map 0 -codec copy -f mpegts -flush_packets 0 -fflags +nobuffer -flags low_delay -strict experimental -analyzeduration 0 -probesize 32 -copyts -threads 12 pipe:",
    "hls segment type": "mpegts",
    "hls segment duration": "4",
    "hls playlist size": "6",
    "ffmpeg timeout": 5,
    "epg refresh interval": 0.5,
    "channel refresh interval": 24,
    "epg future hours": 24,
    "epg past hours": 2,
    "epg custom sources": [],
    "test streams": True,
    "try all macs": True,
    "parallel mac probing": False,
    "parallel mac workers": 3,
    "use channel genres": True,
    "use channel numbers": True,
    "sort playlist by channel genre": False,
    "sort playlist by channel number": True,
    "sort playlist by channel name": False,
    "playlist name format": "({prefix}) {name} ({suffix})",
    "enable security": False,
    "username": "admin",
    "password": "12345",
    "enable hdhr": True,
    "hdhr name": "MacReplay",
    "hdhr id": str(uuid.uuid4().hex),
    "hdhr tuners": 10,
    "tag country codes": "AF,AL,ALB,AR,AT,AU,BE,BG,BR,CA,CH,CN,CZ,DE,DK,EE,ES,FI,FR,GR,HK,HR,HU,IE,IL,IN,IR,IS,IT,JO,JP,KR,KW,LAT,LB,LT,LU,LV,MA,MK,MO,MX,MXC,NL,NO,NZ,PL,PT,RO,RS,RU,SA,SE,SG,SI,SK,TR,UA,UK,US,USA",
    "tag resolution patterns": "8K=\\b(8K|4320P)\\b\nUHD=\\b(UHD|ULTRA|4K\\+?|2160P)\\b\nFHD=\\b(FHD|1080P)\\b\nHD=\\b(HD|720P)\\b\nSD=\\b(SD|576P|480P)\\b",
    "tag video codec patterns": "AV1=\\bAV1\\b\nVP9=\\bVP9\\b\nHEVC=\\b(HEVC|H\\.?265|H265)\\b\nH264=\\b(H\\.?264|H264|AVC)\\b\nMPEG2=\\bMPEG[- ]?2\\b",
    "tag audio patterns": "AAC=\\bAAC\\b\nAC3=\\bAC3\\b\nEAC3=\\bEAC3\\b\nDDP=\\b(DD\\+|DDP)\\b\nDD=\\bDD\\b\nDTS=\\bDTS\\b\nMP3=\\bMP3\\b\nFLAC=\\bFLAC\\b\nDOLBY=\\bDOLBY\\b\nATMOS=\\bATMOS\\b\n7.1=\\b7\\.1\\b\n5.1=\\b5\\.1\\b\n2.0=\\b2\\.0\\b",
    "tag event patterns": "\\bPPV\\b\n\\bEVENT\\b\n\\bLIVE EVENT\\b\n\\bLIVE-EVENT\\b\n\\bNO EVENT\\b\n\\bNO EVENT STREAMING\\b\n\\bMATCH TIME\\b",
    "tag misc patterns": "(?<!\\b\\d\\s)\\bSAT(?![.\\s]*\\d)\\b\n\\bBAR\\b",
    "tag header patterns": "^\\s*([#*✦┃★]{2,})\\s*(.+?)\\s*\\1\\s*$",
    "channelsdvr enabled": False,
    "channelsdvr db path": "/app/data/channelidentifiarr.db",
    "channelsdvr match threshold": 0.72,
    "channelsdvr debug": False,
    "channelsdvr include lineup channels": False,
    "channelsdvr cache enabled": True,
    "channelsdvr cache dir": "/app/data/channelsdvr_cache",
    "auto group selection enabled": False,
    "auto group selection patterns": "",
    "vacuum channels interval hours": 0,
    "vacuum epg interval hours": 0,
    "sportsdb enabled": False,
    "sportsdb api key": "123",
    "sportsdb api version": "v1",
    "sportsdb import sports": "Soccer,Ice Hockey",
    "sportsdb cache ttl hours": 24,
    "espn enabled": False,
    "espn import sports": "soccer,basketball,football",
    "espn cache ttl hours": 24,
    "events match debug": False,
    "events match window hours": 0.75,
    "events cleanup interval minutes": 5,
}

defaultPortal = {
    "enabled": True,
    "name": "",
    "portal code": "",
    "url": "",
    "macs": {},
    "streams per mac": 1,
    "epg offset": 0,
    "proxy": "",
    "fetch epg": True,
    "selected_genres": [],  # Liste der Genre-IDs zum Importieren (leer = alle)
    "auto normalize names": False,
    "auto match": False,
}


def _write_config(data):
    config_dir = os.path.dirname(CONFIG_PATH)
    os.makedirs(config_dir, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", delete=False, dir=config_dir, encoding="utf-8"
    ) as tmp:
        json.dump(data, tmp, indent=4)
        tmp_path = tmp.name
    os.replace(tmp_path, CONFIG_PATH)


def loadConfig():
    global config
    with _config_lock, _file_lock():
        try:
            with open(CONFIG_PATH, encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            # Back up corrupt config for inspection
            if os.path.exists(CONFIG_PATH):
                ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                backup_path = f"{CONFIG_PATH}.corrupt.{ts}"
                try:
                    os.replace(CONFIG_PATH, backup_path)
                except Exception:
                    pass
            data = {}

    data.setdefault("portals", {})
    data.setdefault("settings", {})

    settings = data["settings"]
    data["settings"] = _coerce_settings(settings)

    portals = data["portals"]
    data["portals"] = _coerce_portals(portals)

    with _file_lock():
        _write_config(data)

    config = data
    return data


def getPortals():
    return config["portals"]


def savePortals(portals):
    config["portals"] = _coerce_portals(portals)
    with _config_lock, _file_lock():
        _write_config(config)


def getSettings():
    return config["settings"]


def saveSettings(settings):
    config["settings"] = _coerce_settings(settings)
    with _config_lock, _file_lock():
        _write_config(config)
