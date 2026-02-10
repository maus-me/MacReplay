# Database Schema (SQLite)

Quelle: Runtime-Schema aus `data/channels.db` sowie per-Source EPG-DBs unter `data/epg_sources/*.sqlite`.

## Main DB: `channels.db`

### Table: `channels`

Primary Key: `(portal_id, channel_id)`

| Field | Type | Purpose |
|---|---|---|
| `portal_id` | TEXT | Portal ID (PK) |
| `channel_id` | TEXT | Channel ID im Portal (PK) |
| `portal_name` | TEXT | Portal-Anzeigename |
| `name` | TEXT | Originaler Kanalname |
| `number` | TEXT | Originale Kanalnummer |
| `genre` | TEXT | Originale Gruppe/Genre |
| `genre_id` | TEXT | Originale Genre-ID |
| `logo` | TEXT | Logo-URL |
| `custom_name` | TEXT | Benutzerdefinierter Name |
| `custom_number` | TEXT | Benutzerdefinierte Nummer |
| `custom_genre` | TEXT | Benutzerdefinierte Gruppe |
| `custom_epg_id` | TEXT | Benutzerdefinierte EPG-ID |
| `enabled` | INTEGER | 1/0 aktiv |
| `auto_name` | TEXT | Automatisch normalisierter Name |
| `display_name` | TEXT | Effektiver Name für UI/Sortierung |
| `resolution` | TEXT | Auflösungstag |
| `video_codec` | TEXT | Codec-Tag |
| `country` | TEXT | Länder-/Regionscode |
| `event_tags` | TEXT | Event-Tags (CSV/serialisiert) |
| `misc_tags` | TEXT | Sonstige Tags (CSV/serialisiert) |
| `matched_name` | TEXT | Gematchter externer Name |
| `matched_source` | TEXT | Match-Quelle |
| `matched_station_id` | TEXT | Gematchte Station-ID |
| `matched_call_sign` | TEXT | Gematchtes Call Sign |
| `matched_logo` | TEXT | Gematchtes Logo |
| `matched_score` | REAL | Match-Score |
| `is_header` | INTEGER | 1/0 Header-Kanal |
| `is_event` | INTEGER | 1/0 Event-Kanal |
| `is_raw` | INTEGER | 1/0 RAW-Kanal |
| `available_macs` | TEXT | Verfügbare MACs (serialisiert) |
| `alternate_ids` | TEXT | Alternate IDs (serialisiert) |
| `cmd` | TEXT | Gecachte Stream-Command/URL |
| `channel_hash` | TEXT | Hash für Incremental-Refresh |

Indexes:
- `idx_channels_enabled` on `(enabled)`
- `idx_channels_name` on `(name)`
- `idx_channels_display_name` on `(display_name)`
- `idx_channels_portal_id` on `(portal_id)`
- `idx_channels_portal_name` on `(portal_name)`
- `idx_channels_genre_id` on `(genre_id)`
- `idx_channels_resolution` on `(resolution)`
- `idx_channels_video_codec` on `(video_codec)`
- `idx_channels_country` on `(country)`
- `idx_channels_is_event` on `(is_event)`
- `idx_channels_is_raw` on `(is_raw)`
- `idx_channels_is_header` on `(is_header)`

---

### Table: `groups`

Primary Key: `(portal_id, genre_id)`

| Field | Type | Purpose |
|---|---|---|
| `portal_id` | TEXT | Portal ID (PK) |
| `genre_id` | TEXT | Genre-ID (PK) |
| `name` | TEXT | Gruppenname |
| `channel_count` | INTEGER | Anzahl Kanäle in Gruppe |
| `active` | INTEGER | 1/0 Gruppe aktiv |

Index:
- `idx_groups_active` on `(portal_id, active)`

---

### Table: `portal_stats`

Primary Key: `portal_id`

| Field | Type | Purpose |
|---|---|---|
| `portal_id` | TEXT | Portal ID (PK) |
| `portal_name` | TEXT | Portalname |
| `total_channels` | INTEGER | Gesamtkanäle |
| `active_channels` | INTEGER | Aktive Kanäle |
| `total_groups` | INTEGER | Gesamtgruppen |
| `active_groups` | INTEGER | Aktive Gruppen |
| `updated_at` | TEXT | Letzte Aktualisierung |

Index:
- `idx_portal_stats_name` on `(portal_name)`

---

### Table: `channel_tags`

Primary Key: `(portal_id, channel_id, tag_type, tag_value)`

| Field | Type | Purpose |
|---|---|---|
| `portal_id` | TEXT | Portal ID (PK) |
| `channel_id` | TEXT | Channel ID (PK) |
| `tag_type` | TEXT | Tag-Typ (`event`, `misc`, ...) |
| `tag_value` | TEXT | Tag-Wert |

Indexes:
- `idx_channel_tags_type_value` on `(tag_type, tag_value)`
- `idx_channel_tags_channel` on `(portal_id, channel_id)`

---

### Table: `group_stats`

Primary Key: `(portal_id, group_name)`

| Field | Type | Purpose |
|---|---|---|
| `portal_id` | TEXT | Portal ID |
| `portal_name` | TEXT | Portalname |
| `group_name` | TEXT | Gruppenname |
| `channel_count` | INTEGER | Kanalanzahl |
| `updated_at` | TEXT | Letzte Aktualisierung |

Index:
- `idx_group_stats_portal_id` on `(portal_id)`

---

### Table: `epg_sources`

Primary Key: `source_id`

| Field | Type | Purpose |
|---|---|---|
| `source_id` | TEXT | Source-ID (Portal oder Custom) |
| `name` | TEXT | Anzeigename |
| `url` | TEXT | URL (bei Custom-Source) |
| `source_type` | TEXT | `portal` oder `custom` |
| `enabled` | INTEGER | 1/0 aktiv |
| `interval_hours` | REAL | Refresh-Intervall |
| `last_fetch` | REAL | Letzter Download (epoch) |
| `last_refresh` | REAL | Letzter vollständiger Refresh (epoch) |

---

### Table: `epg_channels`

Primary Key: `(source_id, channel_id)`

| Field | Type | Purpose |
|---|---|---|
| `source_id` | TEXT | Source-ID (PK) |
| `channel_id` | TEXT | EPG Channel-ID (PK) |
| `display_name` | TEXT | Anzeigename aus Source |
| `icon` | TEXT | Icon/Logo |
| `lcn` | TEXT | Logical Channel Number |
| `updated_at` | REAL | Aktualisierungszeit |

Indexes:
- `idx_epg_channels_channel` on `(channel_id)`
- `idx_epg_channels_source` on `(source_id)`

---

### Table: `epg_channel_names`

Primary Key: `(source_id, channel_id, name)`

| Field | Type | Purpose |
|---|---|---|
| `source_id` | TEXT | Source-ID (PK) |
| `channel_id` | TEXT | Channel-ID (PK) |
| `name` | TEXT | Alternativname |

Index:
- `idx_epg_channel_names_name` on `(name)`

---

### Table: `event_rules`

Primary Key: `id` (AUTOINCREMENT)

| Field | Type | Purpose |
|---|---|---|
| `id` | INTEGER | Regel-ID |
| `name` | TEXT | Regelname |
| `enabled` | INTEGER | 1/0 aktiv |
| `sport` | TEXT | Sportfilter |
| `league_filters` | TEXT | Liga-Filter (JSON) |
| `team_filters` | TEXT | Team-Filter (JSON) |
| `channel_groups` | TEXT | Gruppenfilter (JSON) |
| `channel_regex` | TEXT | Kanalname-Regex |
| `epg_pattern` | TEXT | EPG-Pattern |
| `extract_regex` | TEXT | Regex für Home/Away-Extraktion |
| `output_template` | TEXT | Ausgabe-Template Kanalname |
| `priority` | INTEGER | Priorität |
| `created_at` | TEXT | Erstellt |
| `updated_at` | TEXT | Aktualisiert |
| `output_group_name` | TEXT | Zielgruppe für generierte Kanäle |
| `channel_number_start` | INTEGER | Startnummer für generierte Kanäle |
| `provider` | TEXT | Provider (`sportsdb`, `espn`) |
| `use_espn_events` | INTEGER | ESPN Scoreboard-Modus |
| `espn_event_window_hours` | INTEGER | Event-Zeitfenster in Stunden |

Indexes:
- `idx_event_rules_enabled` on `(enabled)`
- `idx_event_rules_priority` on `(priority)`

---

### Table: `event_generated_channels`

Primary Key: `(portal_id, channel_id)`

| Field | Type | Purpose |
|---|---|---|
| `portal_id` | TEXT | Erstellter Kanal: Portal-ID |
| `channel_id` | TEXT | Erstellter Kanal: Channel-ID |
| `event_id` | TEXT | Externe Event-ID |
| `created_at` | REAL | Erstellzeit |
| `expires_at` | REAL | Ablaufzeit |
| `source_portal_id` | TEXT | Quellportal des gematchten Streams |
| `source_channel_id` | TEXT | Quellkanal des gematchten Streams |
| `rule_id` | INTEGER | Verweis auf `event_rules.id` |
| `event_home` | TEXT | Heimteam |
| `event_away` | TEXT | Auswärtsteam |
| `event_start` | TEXT | Spielstart |
| `event_sport` | TEXT | Sport |
| `event_league` | TEXT | Liga |

Indexes:
- `idx_event_generated_event` on `(event_id)`
- `idx_event_generated_source` on `(source_portal_id, source_channel_id)`

---

### SportsDB Cache Tables

#### `sportsdb_sports_cache`
- PK: `sport_name`
- Felder: `sport_name`, `sport_id`, `updated_at`, `raw_json`

#### `sportsdb_leagues_cache`
- PK: `league_id`
- Felder: `league_id`, `league_name`, `sport_name`, `updated_at`, `raw_json`
- Index: `idx_sportsdb_leagues_sport` on `(sport_name)`

#### `sportsdb_teams_cache`
- PK: `team_id`
- Felder: `team_id`, `team_name`, `league_id`, `league_name`, `sport_name`, `updated_at`, `raw_json`, `team_aliases`
- Indexes:
  - `idx_sportsdb_teams_league_id` on `(league_id)`
  - `idx_sportsdb_teams_league_name` on `(league_name)`

---

### ESPN Cache Tables

#### `espn_sports_cache`
- PK: `sport_key`
- Felder: `sport_key`, `sport_name`, `updated_at`, `raw_json`

#### `espn_leagues_cache`
- PK: `league_key`
- Felder: `league_key`, `league_name`, `sport_key`, `updated_at`, `raw_json`
- Index: `idx_espn_leagues_sport` on `(sport_key)`

#### `espn_teams_cache`
- PK: `team_key`
- Felder: `team_key`, `team_id`, `team_name`, `team_aliases`, `sport_key`, `league_key`, `league_name`, `updated_at`, `raw_json`
- Index: `idx_espn_teams_league` on `(league_key)`

#### `espn_scoreboard_cache`
- PK: `(league_key, date_key)`
- Felder: `league_key`, `date_key`, `fetched_at`, `raw_json`

---

## Per-source EPG DBs: `data/epg_sources/<source_id>.sqlite`

### Table: `epg_programmes`

| Field | Type | Purpose |
|---|---|---|
| `channel_id` | TEXT | EPG Channel-ID |
| `start` | TEXT | XMLTV Start (raw) |
| `stop` | TEXT | XMLTV Stop (raw) |
| `start_ts` | INTEGER | Start als epoch |
| `stop_ts` | INTEGER | Stop als epoch |
| `title` | TEXT | Programmtitel |
| `description` | TEXT | Beschreibung |
| `sub_title` | TEXT | Untertitel |
| `categories` | TEXT | Kategorien (serialisiert) |
| `episode_num` | TEXT | Episodennummer |
| `episode_system` | TEXT | Episoden-System |
| `rating` | TEXT | Altersfreigabe/Rating |
| `programme_icon` | TEXT | Programm-Icon |
| `air_date` | TEXT | Ausstrahlungsdatum |
| `previously_shown` | INTEGER | Wiederholung 1/0 |
| `series_id` | TEXT | Serien-ID |
| `extra_json` | TEXT | Zusätzliche XML-Felder (JSON) |

Indexes:
- `idx_epg_programmes_channel` on `(channel_id)`
- `idx_epg_programmes_start` on `(start_ts)`
- `idx_epg_programmes_stop` on `(stop_ts)`

