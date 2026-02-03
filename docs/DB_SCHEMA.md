# Database Schema (SQLite)

Source: `macreplay/db.py` and the runtime database at `/app/data/channels.db`.
Per-source EPG programmes are stored in separate SQLite files under `/app/data/epg_sources/`.

## Table: `channels`

| Field | Type | Purpose |
|---|---|---|
| `portal_id` | TEXT | Portal ID (part of primary key) |
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
| `resolution` | TEXT | Extracted resolution tag (e.g. HD, FHD, UHD) |
| `video_codec` | TEXT | Extracted video codec tag |
| `country` | TEXT | Extracted country/region code |
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
- `idx_channels_portal_id` on `channels(portal_id)`
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
| `portal_id` | TEXT | Portal ID (part of primary key) |
| `genre_id` | TEXT | Genre ID (part of primary key) |
| `name` | TEXT | Group/genre name |
| `channel_count` | INTEGER | Number of channels in the group |
| `active` | INTEGER | 1/0 whether group is active |

Index:
- `idx_groups_active` on `groups(portal_id, active)`

## Table: `portal_stats`

| Field | Type | Purpose |
|---|---|---|
| `portal_id` | TEXT | Portal ID (primary key) |
| `portal_name` | TEXT | Display name of portal |
| `total_channels` | INTEGER | Total channels for portal |
| `active_channels` | INTEGER | Enabled channels for portal |
| `total_groups` | INTEGER | Total groups for portal |
| `active_groups` | INTEGER | Active groups for portal |
| `updated_at` | TEXT | ISO timestamp of last update |

## Table: `channel_tags`

| Field | Type | Purpose |
|---|---|---|
| `portal_id` | TEXT | Portal ID (part of primary key) |
| `channel_id` | TEXT | Channel ID (part of primary key) |
| `tag_type` | TEXT | Tag category (e.g. event, misc) |
| `tag_value` | TEXT | Tag value |

Indexes:
- `idx_channel_tags_type_value` on `channel_tags(tag_type, tag_value)`
- `idx_channel_tags_channel` on `channel_tags(portal_id, channel_id)`

## Table: `group_stats`

| Field | Type | Purpose |
|---|---|---|
| `portal_id` | TEXT | Portal ID |
| `portal_name` | TEXT | Display name of portal |
| `group_name` | TEXT | Group/genre name |
| `channel_count` | INTEGER | Channels in group |
| `updated_at` | TEXT | ISO timestamp of last update |

Index:
- `idx_group_stats_portal_id` on `group_stats(portal_id)`

## Table: `epg_sources`

| Field | Type | Purpose |
|---|---|---|
| `source_id` | TEXT | Source ID (portal ID or custom source ID) |
| `name` | TEXT | Display name of the source |
| `url` | TEXT | Source URL (for custom sources) |
| `source_type` | TEXT | `portal` or `custom` |
| `enabled` | INTEGER | 1/0 whether source is enabled |
| `interval_hours` | REAL | Refresh interval |
| `last_fetch` | REAL | Last fetch timestamp (epoch) |
| `last_refresh` | REAL | Last refresh timestamp (epoch) |

## Table: `epg_channels`

| Field | Type | Purpose |
|---|---|---|
| `source_id` | TEXT | Source ID (part of primary key) |
| `channel_id` | TEXT | EPG channel ID (part of primary key) |
| `display_name` | TEXT | Display name from source |
| `icon` | TEXT | Icon/logo URL |
| `lcn` | TEXT | LCN/number if provided |
| `updated_at` | REAL | Timestamp of last update |

Indexes:
- `idx_epg_channels_channel` on `epg_channels(channel_id)`
- `idx_epg_channels_source` on `epg_channels(source_id)`

## Table: `epg_channel_names`

| Field | Type | Purpose |
|---|---|---|
| `source_id` | TEXT | Source ID (part of primary key) |
| `channel_id` | TEXT | EPG channel ID (part of primary key) |
| `name` | TEXT | Alternate display name |

Index:
- `idx_epg_channel_names_name` on `epg_channel_names(name)`

## Per-source DB: `epg_programmes`

Location: `/app/data/epg_sources/<source_id>.sqlite`

| Field | Type | Purpose |
|---|---|---|
| `channel_id` | TEXT | EPG channel ID |
| `start` | TEXT | XMLTV start timestamp string |
| `stop` | TEXT | XMLTV stop timestamp string |
| `start_ts` | INTEGER | Start timestamp (epoch) |
| `stop_ts` | INTEGER | Stop timestamp (epoch) |
| `title` | TEXT | Programme title |
| `description` | TEXT | Programme description |

Indexes:
- `idx_epg_programmes_channel` on `epg_programmes(channel_id)`
- `idx_epg_programmes_start` on `epg_programmes(start_ts)`
- `idx_epg_programmes_stop` on `epg_programmes(stop_ts)`
