# AGENTS.md

Guidance for AI coding agents working in this repository.

## Project Summary
- MacReplay (stb-proxy) is a Flask app that proxies Stalker/MAC portals and generates M3U/XMLTV for Plex and similar clients.
- Docker-first setup with persistent data/logs volumes.

## Key Entry Points
- `app.py`: Flask app, routes, EPG/playlist generation, DB access, logs, background refresh.
- `stb.py`: Stalker portal API client and streaming helpers.
- `templates/`: Jinja UI (dashboard, editor, portals, logs, settings, epg).
- `static/`: CSS/JS assets.

## Runtime Data (Docker Defaults)
- `DATA_DIR`: `/app/data`
- `LOG_DIR`: `/app/logs`
- Config JSON: `/app/data/MacReplay.json`
- SQLite DB: `/app/data/channels.db`
- EPG cache: `/app/data/epg_cache.xml`

## Important Env Vars
- `BIND_HOST`, `PORT`: listen address/port (default `0.0.0.0:8001`)
- `PUBLIC_HOST` or `HOST`: used for generated URLs
- `CONFIG`, `DB_PATH`, `DATA_DIR`, `LOG_DIR`, `EPG_CACHE_PATH`
- `FFMPEG`, `FFPROBE`
- `EPG_REFRESH_INTERVAL`, `CHANNEL_REFRESH_INTERVAL`

## Primary Routes (Selected)
- `/dashboard` (via `/api/dashboard`): active streams + quick downloads
- `/editor` + `/api/editor_*`: channel editor + bulk ops
- `/portals`: portal/MAC management + genre groups
- `/logs` and `/logs/stream`: live log viewer
- `/xmltv`, `/playlist.m3u`: generated outputs
- `/streaming`: active stream list JSON

## Docker
- Start: `docker-compose up -d --build`
- Logs: `docker-compose logs -f`
