# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MacReplay is an IPTV Portal Proxy that connects Stalker Portal MAC address services to media platforms like Plex. It translates portal APIs into standard M3U playlists and XMLTV EPG data.

## Commands

### Development
```bash
# Run locally (requires Python 3.7+ and FFmpeg)
pip install -r requirements.txt
python app.py

# Run with Docker
docker-compose up -d --build

# View logs
docker-compose logs -f
```

### Testing
```bash
pytest           # Run all tests
pytest -v        # Verbose output
pytest -k test_name  # Run specific test
```

## Architecture

### Core Files

- **app.py** - Main Flask application (~3700 lines)
  - All HTTP routes and API endpoints (50+)
  - Portal/MAC management with genre selection
  - Channel database operations (SQLite)
  - M3U playlist and XMLTV EPG generation
  - HLS stream proxying via FFmpeg
  - Background schedulers for EPG and channel refresh

- **stb.py** - Stalker Portal API client
  - `getToken()` - Authenticate with portal
  - `getProfile()` - Get MAC profile (watchdog_timeout, playback_limit)
  - `getExpires()` - Get MAC expiration date
  - `getAllChannels()` - Fetch channel list
  - `getGenres()` - Get genre/group list
  - `getLink()` - Get stream URL for channel
  - `getEpg()` - Fetch EPG data

### Data Storage

- **MacReplay.json** - Portal configuration
  - Portals with URLs, MACs, settings
  - MAC data includes: expiry, watchdog_timeout, playback_limit
  - Selected genres per portal

- **channels.db** - SQLite database
  - Channels with custom names, numbers, genres
  - `available_macs` column tracks which MACs can access each channel
  - Enable/disable state, fallback channel configuration

### Key Concepts

**Portal**: A Stalker Portal server URL (e.g., `http://example.com/c/`)

**MAC**: Authentication credential for portal access. Each MAC has:
- Expiration date
- Watchdog timeout (seconds since last activity - indicates if MAC is in use)
- Playback limit (max concurrent streams)

**Genre/Group**: Channel category from portal. Users can filter which genres to import.

**available_macs**: When channels are refreshed, ALL MACs are queried and the system records which MACs can access each channel. During streaming, only MACs from this list are used.

### MAC Scoring Algorithm

When selecting a MAC for streaming (`score_mac_for_selection()`):
1. Skip expired MACs
2. Score based on watchdog_timeout (prefer idle MACs > 300s)
3. Consider playback_limit (available concurrent slots)
4. Select highest scoring MAC

Watchdog interpretation:
- < 60s: Very active (currently streaming)
- 60-300s: Recently used
- > 300s: Idle (preferred for new streams)

### Request Flow (Streaming)

1. Client requests `/play/{portalId}/{channelId}`
2. System looks up `available_macs` for that channel
3. MACs are scored (prefer idle, available slots)
4. Best MAC gets token from portal via `stb.getToken()`
5. Stream URL fetched via `stb.getLink()`
6. FFmpeg proxies HLS stream to client

### Background Schedulers

- **EPG Scheduler**: Refreshes EPG data (default: every 6 hours)
- **Channel Scheduler**: Syncs channels from all portals (default: every 24 hours)
- Both run as daemon threads, configured via environment variables

### Key API Endpoints

| Endpoint | Purpose |
|----------|---------|
| `/api/portals` | List all portals with MACs |
| `/portal/add`, `/portal/update`, `/portal/remove` | Portal CRUD |
| `/api/editor/channels` | Get channels for editor |
| `/api/editor/save` | Save channel configuration |
| `/play/{portalId}/{channelId}` | Stream a channel |
| `/playlist.m3u` | Generate M3U playlist |
| `/xmltv` | Generate XMLTV EPG |
| `/api/dashboard` | System statistics |

### Templates

Jinja2 templates in `templates/`:
- `portals.html` - Portal/MAC management with genre selection
- `editor.html` - Channel playlist editor with filtering
- `epg.html` - EPG viewer
- `settings.html` - Application settings
- `dashboard.html` - System overview

## Environment Variables

```
HOST=0.0.0.0:8001               # Legacy bind address
BIND_HOST=0.0.0.0               # Flask bind host
PORT=8001                       # Flask bind port
PUBLIC_HOST=<hostname>          # External hostname for generated URLs
CONFIG=/app/data/MacReplay.json # Config file path
DB_PATH=/app/data/channels.db   # Database path
DATA_DIR=/app/data              # Data directory
LOG_DIR=/app/logs               # Log directory
TZ=Europe/Berlin                # Timezone for API requests
FFMPEG=ffmpeg                   # FFmpeg binary
FFPROBE=ffprobe                 # FFprobe binary
EPG_REFRESH_INTERVAL=6          # Hours between EPG refresh
CHANNEL_REFRESH_INTERVAL=24     # Hours between channel sync (0=disabled)
```
