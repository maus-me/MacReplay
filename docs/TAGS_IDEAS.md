# Channel Tag Extraction Ideas

Goal
- Clean up channel names by removing noisy tags while preserving them as structured metadata.
- Store cleaned name in `custom_name`, keep original in `name`.

Core features
- Tag extraction during import/sync.
- Optional manual re-run with preview.
- Configurable regex rules per tag group in Settings.
- Header detection (decorative group titles) with optional hide/filter.

Suggested tag groups
- Resolution: SD, HD, FHD, UHD, 4K, 8K, 480p, 576p, 720p, 1080p, 2160p, 4320p
- Video codec: H.264/AVC, H.265/HEVC, VP9, AV1, MPEG-2
- Audio: AAC, AC3, EAC3, DD, DD+, DTS, MP3, FLAC, 2.0, 5.1, 7.1
- Country/region: DE, AT, CH, UK, US/USA, FR, ES, IT, NL, PL, TR, RO, GR, PT, AR, CA, AU, BE, etc.
- Flags/emoji: map to country codes
- Event/status: PPV, EVENT, LIVE, REPLAY, BACKUP, NO EVENT STREAMING, MATCH TIME, 24/7
- Quality/labels: RAW, GOLD, VIP, PLUS, EN/DE/FR (audio/lang tags)

Header channel detection (group titles)
- Symmetric decoration around a title, e.g. "## TITLE ##" or "✦ ✦ TITLE ✦ ✦".
- Mostly decorative characters and generic category words (MOVIES, SPORTS, DOCUMENTARY, KIDS).
- Often lacks numbers, regions, and recognizable station tokens.

Header regex ideas (draft)
- Symmetric decoration:
  - ^\s*([#*✦┃★]{2,})\s*(.+?)\s*\1\s*$
- Heavy decoration (prefix/suffix):
  - ^[\s#*✦┃★._-]{6,}.*[\s#*✦┃★._-]{6,}$

Storage options
- Best for filtering: add columns for `resolution`, `video_codec`, `country`.
- Most flexible: single JSON column (e.g. `tags_json`) with grouped values.
- CSV is simple but poor for querying; use only for low-importance fields.

Extraction flow (suggested)
1) Parse name -> extract tags by group.
2) Remove tags + cleanup whitespace/separators.
3) Save cleaned name into `custom_name` (if empty or overwrite enabled).
4) Save tag fields/JSON and header flag.

Preview/report
- Provide a dry-run list: original -> cleaned, extracted tags, header flag.
- Allow per-channel opt-out or revert.

Notes
- Not every channel will have tags; leave fields NULL/empty.
- Avoid stripping real content keywords (SPORT, NEWS, MOVIES) unless detected as header.
