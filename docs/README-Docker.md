# MacReplay Docker Setup

This guide explains how to run MacReplay in a Docker container.

## Prerequisites
- Docker and Docker Compose installed

## Quick Start
1. Build and run with Compose: `docker-compose up -d`.
2. Open `http://localhost:8001` in your browser.

## Configuration
Data is persisted via volumes:
- `./data/` for `MacReplay.json`
- `./logs/` for application logs

Common environment variables:
- `HOST=0.0.0.0:8001`
- `CONFIG=/app/data/MacReplay.json`
- `DB_PATH=/app/data/channels.db`
- `DATA_DIR=/app/data`
- `LOG_DIR=/app/logs`
- `BIND_HOST=0.0.0.0`
- `PORT=8001`
- `PUBLIC_HOST=<hostname>`

## Example Compose
```yaml
services:
  macreplay:
    build: .
    ports:
      - "8001:8001"
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
    restart: unless-stopped
```

## Plex Integration
Use the Docker host IP instead of `127.0.0.1`:
- Docker Desktop: `http://host.docker.internal:8001`
- Linux: `http://YOUR_HOST_IP:8001`

Plex setup values:
- XMLTV: `http://YOUR_HOST_IP:8001/xmltv`
- Playlist: `http://YOUR_HOST_IP:8001/playlist.m3u`

## Management Commands
- Logs: `docker-compose logs -f`
- Restart: `docker-compose restart`
- Update: `docker-compose down` then `docker-compose up -d --build`
- Backup: `tar -czf macreplay-backup-$(date +%Y%m%d).tar.gz data/`

## Troubleshooting
- Container will not start: check `docker-compose logs` and verify port availability.
- Web UI not reachable: confirm `docker-compose ps` and port mappings.
- Streams not working: check `docker exec <container_name> ffmpeg -version` and review logs.

## Security Notes
- Change default credentials in settings.
- Use a reverse proxy for external access.
- Enable authentication in MacReplay settings.
- Keep images updated.
