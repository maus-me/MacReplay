#!/usr/bin/env python3
import sys
import os
import shutil
import time
import subprocess
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
import threading
from threading import Thread
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
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
}


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
            logo TEXT,
            enabled INTEGER DEFAULT 0,
            custom_name TEXT,
            custom_number TEXT,
            custom_genre TEXT,
            custom_epg_id TEXT,
            fallback_channel TEXT,
            PRIMARY KEY (portal, channel_id)
        )
    ''')
    
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
    
    conn.commit()
    conn.close()
    logger.info("Database initialized successfully")


def refresh_channels_cache():
    """Refresh the channels cache from STB portals."""
    logger.info("Starting channel cache refresh...")
    portals = getPortals()
    conn = get_db_connection()
    cursor = conn.cursor()
    
    total_channels = 0
    
    for portal_id in portals:
        portal = portals[portal_id]
        if portal["enabled"] == "true":
            portal_name = portal["name"]
            url = portal["url"]
            macs = list(portal["macs"].keys())
            proxy = portal["proxy"]

            logger.info(f"Fetching channels for portal: {portal_name}")
            
            # Try each MAC until we get channel data
            all_channels = None
            genres = None
            for mac in macs:
                logger.info(f"Trying MAC: {mac}")
                try:
                    token = stb.getToken(url, mac, proxy)
                    if token:
                        stb.getProfile(url, mac, token, proxy)
                        all_channels = stb.getAllChannels(url, mac, token, proxy)
                        genres = stb.getGenreNames(url, mac, token, proxy)
                        if all_channels and genres:
                            break
                except Exception as e:
                    logger.error(f"Error fetching from MAC {mac}: {e}")
                    all_channels = None
                    genres = None
            
            if all_channels and genres:
                logger.info(f"Processing {len(all_channels)} channels for {portal_name}")
                
                for channel in all_channels:
                    channel_id = str(channel["id"])
                    channel_name = str(channel["name"])
                    channel_number = str(channel["number"])
                    genre_id = str(channel.get("tv_genre_id", ""))
                    genre = str(genres.get(genre_id, ""))
                    logo = str(channel.get("logo", ""))

                    # Upsert into database (new channels start disabled with empty custom values)
                    cursor.execute('''
                        INSERT INTO channels (
                            portal, channel_id, portal_name, name, number, genre, logo,
                            enabled, custom_name, custom_number, custom_genre,
                            custom_epg_id, fallback_channel
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(portal, channel_id) DO UPDATE SET
                            portal_name = excluded.portal_name,
                            name = excluded.name,
                            number = excluded.number,
                            genre = excluded.genre,
                            logo = excluded.logo
                    ''', (
                        portal_id, channel_id, portal_name, channel_name, channel_number,
                        genre, logo, 0, "", "", "", "", ""
                    ))
                    
                    total_channels += 1
                
                conn.commit()
                logger.info(f"Successfully cached {len(all_channels)} channels for {portal_name}")
            else:
                logger.error(f"Failed to fetch channels for portal: {portal_name}")
    
    conn.close()
    logger.info(f"Channel cache refresh complete. Total channels: {total_channels}")
    return total_channels


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

        # Get channel count per portal
        cursor.execute("""
            SELECT portal, COUNT(*) as channel_count
            FROM channels
            GROUP BY portal
        """)
        for row in cursor.fetchall():
            portal_stats[row['portal']] = {'channels': row['channel_count'], 'groups': 0}

        # Get distinct genre count per portal
        cursor.execute("""
            SELECT portal, COUNT(DISTINCT COALESCE(NULLIF(custom_genre, ''), genre)) as group_count
            FROM channels
            GROUP BY portal
        """)
        for row in cursor.fetchall():
            if row['portal'] in portal_stats:
                portal_stats[row['portal']]['groups'] = row['group_count']
            else:
                portal_stats[row['portal']] = {'channels': 0, 'groups': row['group_count']}

        conn.close()
    except Exception as e:
        logger.error(f"Error getting portal stats: {e}")

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

    if not url.endswith(".php"):
        url = stb.getUrl(url, proxy)
        if not url:
            logger.error("Error getting URL for Portal({})".format(name))
            flash("Error getting URL for Portal({})".format(name), "danger")
            return redirect("/portals", code=302)

    macsd = {}

    for mac in macs:
        logger.info(f"Testing MAC({mac}) for Portal({name})...")
        token = stb.getToken(url, mac, proxy)
        if token:
            logger.debug(f"Got token for MAC({mac}), getting profile and expiry...")
            stb.getProfile(url, mac, token, proxy)
            expiry = stb.getExpires(url, mac, token, proxy)
            if expiry:
                macsd[mac] = expiry
                logger.info(
                    "Successfully tested MAC({}) for Portal({})".format(mac, name)
                )
                flash(
                    "Successfully tested MAC({}) for Portal({})".format(mac, name),
                    "success",
                )
                continue
            else:
                logger.error(f"Failed to get expiry for MAC({mac}) for Portal({name})")
        else:
            logger.error(f"Failed to get token for MAC({mac}) for Portal({name})")

        logger.error("Error testing MAC({}) for Portal({})".format(mac, name))
        flash("Error testing MAC({}) for Portal({})".format(mac, name), "danger")

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
    retest = request.form.get("retest", None)

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

    for mac in newmacs:
        if retest or mac not in oldmacs.keys():
            logger.info(f"Testing MAC({mac}) for Portal({name})...")
            token = stb.getToken(url, mac, proxy)
            if token:
                logger.debug(f"Got token for MAC({mac}), getting profile and expiry...")
                stb.getProfile(url, mac, token, proxy)
                expiry = stb.getExpires(url, mac, token, proxy)
                if expiry:
                    macsout[mac] = expiry
                    logger.info(
                        "Successfully tested MAC({}) for Portal({})".format(mac, name)
                    )
                    flash(
                        "Successfully tested MAC({}) for Portal({})".format(mac, name),
                        "success",
                    )
                else:
                    logger.error(f"Failed to get expiry for MAC({mac}) for Portal({name})")
            else:
                logger.error(f"Failed to get token for MAC({mac}) for Portal({name})")

            if mac not in list(macsout.keys()):
                deadmacs.append(mac)

        if mac in oldmacs.keys() and mac not in deadmacs:
            macsout[mac] = oldmacs[mac]

        if mac not in macsout.keys():
            logger.error("Error testing MAC({}) for Portal({})".format(mac, name))
            flash("Error testing MAC({}) for Portal({})".format(mac, name), "danger")

    if len(macsout) > 0:
        portals[id]["enabled"] = enabled
        portals[id]["name"] = name
        portals[id]["url"] = url
        portals[id]["macs"] = macsout
        portals[id]["streams per mac"] = streamsPerMac
        portals[id]["epg offset"] = epgOffset
        portals[id]["proxy"] = proxy
        portals[id]["fetch epg"] = fetchEpg
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
        
        # Map column indices to database columns
        column_map = {
            0: 'enabled',
            1: 'channel_id',  # Play button, not sortable but needs a column
            2: 'name',  # Channel name
            3: 'genre',
            4: 'number',
            5: 'epg_id',  # EPG ID - Special handling
            6: 'fallback_channel',
            7: 'portal_name'
        }
        
        # Build the SQL query
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Base query
        base_query = "FROM channels WHERE 1=1"
        params = []
        
        # Add portal filter (supports multiple values)
        if portal_filter:
            portal_values = [p.strip() for p in portal_filter.split(',') if p.strip()]
            if portal_values:
                placeholders = ','.join(['?'] * len(portal_values))
                base_query += f" AND portal_name IN ({placeholders})"
                params.extend(portal_values)

        # Add group filter (check both custom_genre and genre, supports multiple values)
        if group_filter:
            genre_values = [g.strip() for g in group_filter.split(',') if g.strip()]
            if genre_values:
                placeholders = ','.join(['?'] * len(genre_values))
                base_query += f" AND (COALESCE(NULLIF(custom_genre, ''), genre) IN ({placeholders}))"
                params.extend(genre_values)
        
        # Add duplicate filter (only for enabled channels)
        if duplicate_filter == 'enabled_only':
            # Show only channels where the name appears multiple times among enabled channels
            base_query += """ AND enabled = 1 AND COALESCE(NULLIF(custom_name, ''), name) IN (
                SELECT COALESCE(NULLIF(custom_name, ''), name)
                FROM channels
                WHERE enabled = 1
                GROUP BY COALESCE(NULLIF(custom_name, ''), name)
                HAVING COUNT(*) > 1
            )"""
        elif duplicate_filter == 'unique_only':
            # Show only channels where the name appears once among enabled channels
            base_query += """ AND COALESCE(NULLIF(custom_name, ''), name) IN (
                SELECT COALESCE(NULLIF(custom_name, ''), name)
                FROM channels
                WHERE enabled = 1
                GROUP BY COALESCE(NULLIF(custom_name, ''), name)
                HAVING COUNT(*) = 1
            )"""
        
        # Add search filter if provided
        if search_value:
            base_query += """ AND (
                name LIKE ? OR 
                custom_name LIKE ? OR 
                genre LIKE ? OR 
                custom_genre LIKE ? OR
                number LIKE ? OR
                custom_number LIKE ? OR
                portal_name LIKE ?
            )"""
            search_param = f"%{search_value}%"
            params.extend([search_param] * 7)
        
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
                order_clauses.append(f"COALESCE(NULLIF(custom_name, ''), name) {direction}")
            elif col_name == 'genre':
                order_clauses.append(f"COALESCE(NULLIF(custom_genre, ''), genre) {direction}")
            elif col_name == 'number':
                order_clauses.append(f"CAST(COALESCE(NULLIF(custom_number, ''), number) AS INTEGER) {direction}")
            elif col_name == 'epg_id':
                order_clauses.append(f"COALESCE(NULLIF(custom_epg_id, ''), portal || channel_id) {direction}")
            else:
                order_clauses.append(f"{col_name} {direction}")
            i += 1
            
        if not order_clauses:
            order_clauses.append("COALESCE(NULLIF(custom_name, ''), name) ASC")
            
        order_clause = "ORDER BY " + ", ".join(order_clauses)
        
        data_query = f"""
            SELECT 
                portal, channel_id, portal_name, name, number, genre, logo,
                enabled, custom_name, custom_number, custom_genre, 
                custom_epg_id, fallback_channel
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
                COALESCE(NULLIF(custom_name, ''), name) as channel_name,
                COUNT(*) as count
            FROM channels
            WHERE enabled = 1
            GROUP BY COALESCE(NULLIF(custom_name, ''), name)
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
            channel_name = row['custom_name'] or row['name']
            duplicate_count = duplicate_counts.get(channel_name, 0)
            
            # Check if this channel has EPG data (by custom EPG ID or channel name)
            epg_id = row['custom_epg_id'] or channel_name
            has_epg = epg_id in epg_channels

            channels.append({
                "portal": portal,
                "portalName": row['portal_name'] or '',
                "enabled": bool(row['enabled']),
                "channelNumber": row['number'] or '',
                "customChannelNumber": row['custom_number'] or '',
                "channelName": row['name'] or '',
                "customChannelName": row['custom_name'] or '',
                "genre": row['genre'] or '',
                "customGenre": row['custom_genre'] or '',
                "channelId": channel_id,
                "customEpgId": row['custom_epg_id'] or '',
                "fallbackChannel": row['fallback_channel'] or '',
                "link": f"http://{host}/play/{portal}/{channel_id}?web=true",
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
                COALESCE(NULLIF(custom_name, ''), name) as channel_name,
                COUNT(*) as count
            FROM channels
            WHERE enabled = 1
            GROUP BY COALESCE(NULLIF(custom_name, ''), name)
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
                    COALESCE(NULLIF(custom_name, ''), name) as effective_name,
                    ROW_NUMBER() OVER (
                        PARTITION BY COALESCE(NULLIF(custom_name, ''), name) 
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
    fallbackEdits = json.loads(request.form["fallbackEdits"])
    
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
        
        # Process fallback channel edits
        for edit in fallbackEdits:
            portal = edit["portal"]
            channel_id = edit["channel id"]
            fallback_channel = edit["channel name"]
            
            cursor.execute('''
                UPDATE channels 
                SET fallback_channel = ? 
                WHERE portal = ? AND channel_id = ?
            ''', (fallback_channel, portal, channel_id))
        
        conn.commit()
        logger.info("Channel edits saved to database!")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error saving channel edits: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        conn.close()

    return jsonify({"success": True, "message": "Playlist config saved!"})


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
                custom_epg_id = '',
                fallback_channel = ''
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
        order_clause = "ORDER BY COALESCE(NULLIF(custom_name, ''), name)"
    elif getSettings().get("use channel numbers", "true") == "true":
        if getSettings().get("sort playlist by channel number", "false") == "true":
            order_clause = "ORDER BY CAST(COALESCE(NULLIF(custom_number, ''), number) AS INTEGER)"
    elif getSettings().get("use channel genres", "true") == "true":
        if getSettings().get("sort playlist by channel genre", "false") == "true":
            order_clause = "ORDER BY COALESCE(NULLIF(custom_genre, ''), genre)"
    
    cursor.execute(f'''
        SELECT 
            portal, channel_id, name, number, genre,
            custom_name, custom_number, custom_genre, custom_epg_id
        FROM channels
        WHERE enabled = 1
        {order_clause}
    ''')
    
    for row in cursor.fetchall():
        portal = row['portal']
        channel_id = row['channel_id']
        
        # Use custom values if available, otherwise use defaults
        channel_name = row['custom_name'] if row['custom_name'] else row['name']
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
    cursor.execute('''
        SELECT
            portal, channel_id, name, number, logo,
            custom_name, custom_number, custom_epg_id
        FROM channels
        WHERE enabled = 1
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
                channelName = ch['custom_name'] if ch['custom_name'] else ch['name']
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
            SELECT portal, name, custom_name, custom_epg_id
            FROM channels WHERE enabled = 1
        ''')
        channel_portal_map = {}
        for row in cursor.fetchall():
            # EPG ID is custom_epg_id if set, otherwise channel name
            epg_id = row['custom_epg_id'] if row['custom_epg_id'] else (row['custom_name'] if row['custom_name'] else row['name'])
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
    macs = list(portal["macs"].keys())
    streamsPerMac = int(portal.get("streams per mac"))
    proxy = portal.get("proxy")
    web = request.args.get("web")
    ip = request.remote_addr
    channelName = portal.get("custom channel names", {}).get(channelId)

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
            channels = stb.getAllChannels(url, mac_to_test, token, proxy)

            if not channels:
                return None

            cmd = None
            found_channel_name = portal.get("custom channel names", {}).get(channelId)
            for c in channels:
                if str(c["id"]) == channelId:
                    if found_channel_name is None:
                        found_channel_name = c["name"]
                    cmd = c["cmd"]
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

    if not web:
        logger.info(
            "Portal({}):Channel({}) is not working. Looking for fallbacks...".format(
                portalId, channelId
            )
        )

        portals = getPortals()
        for portal in portals:
            if portals[portal]["enabled"] == "true":
                fallbackChannels = portals[portal]["fallback channels"]
                if channelName and channelName in fallbackChannels.values():
                    url = portals[portal].get("url")
                    macs = list(portals[portal]["macs"].keys())
                    proxy = portals[portal].get("proxy")
                    for mac in macs:
                        channels = None
                        cmd = None
                        link = None
                        if streamsPerMac == 0 or isMacFree():
                            for k, v in fallbackChannels.items():
                                if v == channelName:
                                    try:
                                        token = stb.getToken(url, mac, proxy)
                                        stb.getProfile(url, mac, token, proxy)
                                        channels = stb.getAllChannels(
                                            url, mac, token, proxy
                                        )
                                    except:
                                        logger.info(
                                            "Unable to connect to fallback Portal({}) using MAC({})".format(
                                                portalId, mac
                                            )
                                        )
                                    if channels:
                                        fChannelId = k
                                        for c in channels:
                                            if str(c["id"]) == fChannelId:
                                                cmd = c["cmd"]
                                                break
                                        if cmd:
                                            if "http://localhost/" in cmd:
                                                link = stb.getLink(
                                                    url, mac, token, cmd, proxy
                                                )
                                            else:
                                                link = cmd.split(" ")[1]
                                            if link:
                                                if testStream():
                                                    logger.info(
                                                        "Fallback found for Portal({}):Channel({})".format(
                                                            portalId, channelId
                                                        )
                                                    )
                                                    if (
                                                        getSettings().get(
                                                            "stream method", "ffmpeg"
                                                        )
                                                        == "ffmpeg"
                                                    ):
                                                        ffmpegcmd = str(
                                                            getSettings()[
                                                                "ffmpeg command"
                                                            ]
                                                        )
                                                        ffmpegcmd = ffmpegcmd.replace(
                                                            "<url>", link
                                                        )
                                                        ffmpegcmd = ffmpegcmd.replace(
                                                            "<timeout>",
                                                            str(
                                                                int(
                                                                    getSettings()[
                                                                        "ffmpeg timeout"
                                                                    ]
                                                                )
                                                                * int(1000000)
                                                            ),
                                                        )
                                                        if proxy:
                                                            ffmpegcmd = (
                                                                ffmpegcmd.replace(
                                                                    "<proxy>", proxy
                                                                )
                                                            )
                                                        else:
                                                            ffmpegcmd = ffmpegcmd.replace(
                                                                "-http_proxy <proxy>",
                                                                "",
                                                            )
                                                        " ".join(
                                                            ffmpegcmd.split()
                                                        )  # cleans up multiple whitespaces
                                                        ffmpegcmd = ffmpegcmd.split()
                                                        return Response(
                                                            streamData(),
                                                            mimetype="application/octet-stream",
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
    
    cursor.execute('''
        SELECT 
            portal, channel_id, name, number,
            custom_name, custom_number
        FROM channels
        WHERE enabled = 1
        ORDER BY CAST(COALESCE(NULLIF(custom_number, ''), number) AS INTEGER)
    ''')
    
    for row in cursor.fetchall():
        portal = row['portal']
        channel_id = row['channel_id']
        channel_name = row['custom_name'] if row['custom_name'] else row['name']
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
