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

- **app.py** - Main Flask application (~3200 lines)
  - All HTTP routes and API endpoints
  - Portal/MAC management
  - Channel database operations (SQLite)
  - M3U playlist and XMLTV EPG generation
  - Stream proxying with FFmpeg

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
- Watchdog timeout (activity indicator)
- Playback limit (max concurrent streams)

**Genre/Group**: Channel category from portal. Users can filter which genres to import.

**available_macs**: When channels are refreshed, ALL MACs are queried and the system records which MACs can access each channel. During streaming, MACs from this list are prioritized.

### Request Flow (Streaming)

1. Client requests `/play/{portalId}/{channelId}`
2. System looks up `available_macs` for that channel
3. MACs are scored (prefer idle, available slots)
4. Best MAC gets token from portal
5. Stream URL fetched via `stb.getLink()`
6. FFmpeg proxies the stream to client

### Templates

Jinja2 templates in `templates/`:
- `portals.html` - Portal/MAC management with genre selection
- `editor.html` - Channel playlist editor with filtering
- `epg.html` - EPG viewer
- `settings.html` - Application settings

## Environment Variables

```
HOST=0.0.0.0:8001
CONFIG=/app/data/MacReplay.json
DB_PATH=/app/data/channels.db
TZ=Europe/Berlin
EPG_REFRESH_INTERVAL=6  # hours
CHANNEL_REFRESH_INTERVAL=24  # hours, 0 to disable
```
