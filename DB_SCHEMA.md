# Database Schema (SQLite)

Source: live schema from `/host_opt/stb-proxy/data/channels.db` and `init_db()` in `app.py`.

## Table: `channels`

| Field | Type | Purpose |
|---|---|---|
| `portal` | TEXT | Portal ID (part of primary key) |
| `channel_id` | TEXT | Channel ID within portal (part of primary key) |
| `portal_name` | TEXT | Display name of portal |
| `name` | TEXT | Original channel name from portal |
| `display_name` | TEXT | Effective display name used in UI/sorting |
| `number` | TEXT | Original channel number from portal |
| `genre` | TEXT | Original genre/group name |
| `genre_id` | TEXT | Original genre ID |
| `logo` | TEXT | Logo URL |
| `enabled` | INTEGER | 1/0 whether channel is active |
| `custom_name` | TEXT | User-defined channel name |
| `auto_name` | TEXT | Auto-normalized channel name (if enabled per portal) |
| `custom_number` | TEXT | User-defined channel number |
| `custom_genre` | TEXT | User-defined genre |
| `custom_epg_id` | TEXT | User-defined EPG ID |
| `fallback_channel` | TEXT | Name of fallback channel |
| `resolution` | TEXT | Extracted resolution tag (e.g. HD, FHD, UHD) |
| `video_codec` | TEXT | Extracted video codec tag |
| `country` | TEXT | Extracted country/region code |
| `audio_tags` | TEXT | Extracted audio tags (CSV) |
| `event_tags` | TEXT | Extracted event tags (CSV) |
| `misc_tags` | TEXT | Extracted misc tags (CSV) |
| `matched_name` | TEXT | Matched external name |
| `matched_source` | TEXT | Match source/provider |
| `matched_station_id` | TEXT | Matched station ID |
| `matched_call_sign` | TEXT | Matched call sign |
| `matched_logo` | TEXT | Matched logo URL |
| `matched_score` | REAL | Match confidence score |
| `is_header` | INTEGER | 1/0 if channel is a header/group title |
| `is_event` | INTEGER | 1/0 if channel is an event/PPV |
| `is_raw` | INTEGER | 1/0 if channel has RAW tag |
| `available_macs` | TEXT | CSV of MACs that can access this channel |
| `alternate_ids` | TEXT | CSV of alternate channel IDs (merge/fallback) |
| `cmd` | TEXT | Cached stream command/URL |
| `channel_hash` | TEXT | Hash for incremental refresh comparison |

Indexes:
- `idx_channels_enabled` on `channels(enabled)`
- `idx_channels_name` on `channels(name)`
- `idx_channels_display_name` on `channels(display_name)`
- `idx_channels_portal` on `channels(portal)`
- `idx_channels_portal_name` on `channels(portal_name)`
- `idx_channels_genre_id` on `channels(genre_id)`
- `idx_channels_resolution` on `channels(resolution)`
- `idx_channels_video_codec` on `channels(video_codec)`
- `idx_channels_country` on `channels(country)`
- `idx_channels_is_event` on `channels(is_event)`
- `idx_channels_is_raw` on `channels(is_raw)`
- `idx_channels_is_header` on `channels(is_header)`

## Table: `groups`

| Field | Type | Purpose |
|---|---|---|
| `portal` | TEXT | Portal ID (part of primary key) |
| `genre_id` | TEXT | Genre ID (part of primary key) |
| `name` | TEXT | Group/genre name |
| `channel_count` | INTEGER | Number of channels in the group |
| `active` | INTEGER | 1/0 whether group is active |

Index:
- `idx_groups_active` on `groups(portal, active)`

## Table: `portal_stats`

| Field | Type | Purpose |
|---|---|---|
| `portal` | TEXT | Portal ID (primary key) |
| `portal_name` | TEXT | Display name of portal |
| `channel_count` | INTEGER | Total channels for portal |
| `updated_at` | TEXT | ISO timestamp of last update |

## Table: `group_stats`

| Field | Type | Purpose |
|---|---|---|
| `portal` | TEXT | Portal ID |
| `portal_name` | TEXT | Display name of portal |
| `group_name` | TEXT | Group/genre name |
| `channel_count` | INTEGER | Channels in group |
| `updated_at` | TEXT | ISO timestamp of last update |

Index:
- `idx_group_stats_portal` on `group_stats(portal)`
