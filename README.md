# MacReplay

MacReplay is an improved version of STB-Proxy that connects MAC address portals with Plex or M3U-based software and generates M3U/XMLTV outputs.

## Features
- Cross-platform via Docker
- MAC portal integration for Plex or M3U clients
- Multiple MACs per portal
- Multiple portals in one playlist
- Fallback channels for reliability
- Duplicate detection and cleanup
- Portal/genre filters and fast search
- Autocomplete for fallback setup

## Requirements
- Docker and Docker Compose
- Plex Pass if connecting to Plex (may no longer be required with recent Plex updates)

## Quick Start (Docker)
1. Clone or download this repository.
2. Build and run: `docker-compose up -d --build`.
3. Open `http://localhost:8001` in your browser.

For more Docker details, see `README-Docker.md`.

## Additional Docs
- `docs/README-Docker.md`
- `docs/DB_SCHEMA.md`
- `docs/IDEAS.md`
- `docs/TAGS_IDEAS.md`

## Configuration Workflow
1. Add portals on the Portals page with portal URLs and MAC addresses.
2. Configure channels in the Playlist Editor. Use filters, enable or disable channels, set custom names or numbers, and configure fallbacks.
3. Set up Plex. Go to Live TV and DVR, choose XMLTV, and use:
   - `http://YOUR_SERVER_IP:8001/xmltv`
   - `http://YOUR_SERVER_IP:8001/playlist.m3u`

## Playlist Editor Tips
- Use the Portal and Genre filters to reduce large lists.
- Use the Enabled Duplicates filter to find duplicates and deactivate extras.
- Use the Fallback For field to assign backups for critical channels.

## Troubleshooting
- TV guide not populated: check `http://localhost:13681/xmltv`. If channels show without listings, the provider likely does not supply a guide.
- Plex does not update after changes: delete the DVR in Plex and re-add it.
- Error getting channel data or XMLTV: retest the portal and verify MAC validity.
- Channels not loading: verify portal settings and check dashboard logs.
- Duplicate detection not working: only enabled channels are considered duplicates.

## Known Issues
Channel logos may not display in a browser due to HTTPS mixed-content restrictions. This does not affect Plex apps and most clients. If logos are still missing, the provider likely does not supply them.

## Credits
MacReplay is based on the original STB-Proxy by Chris230291.

## Disclaimer
This tool is provided as-is and is intended for educational purposes only. Use responsibly and in compliance with applicable laws and terms of service.
