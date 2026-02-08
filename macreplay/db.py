import os
import sqlite3

from .config import DB_PATH, DATA_DIR


def get_db_connection():
    """Get a database connection."""
    db_path = os.getenv("DB_PATH", DB_PATH)
    if db_path.startswith("file:"):
        conn = sqlite3.connect(db_path, uri=True)
    else:
        conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA busy_timeout = 5000;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.row_factory = sqlite3.Row
    return conn


def init_db(get_portals, logger):
    """Initialize the database and create tables if they don't exist."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            portal_id TEXT NOT NULL,
            channel_id TEXT NOT NULL,
            portal_name TEXT,
            name TEXT,
            number TEXT,
            genre TEXT,
            genre_id TEXT,
            logo TEXT,
            custom_name TEXT,
            custom_number TEXT,
            custom_genre TEXT,
            custom_epg_id TEXT,
            enabled INTEGER DEFAULT 0,
            auto_name TEXT,
            display_name TEXT,
            resolution TEXT,
            video_codec TEXT,
            country TEXT,
            event_tags TEXT,
            misc_tags TEXT,
            matched_name TEXT,
            matched_source TEXT,
            matched_station_id TEXT,
            matched_call_sign TEXT,
            matched_logo TEXT,
            matched_score REAL,
            is_header INTEGER DEFAULT 0,
            is_event INTEGER DEFAULT 0,
            is_raw INTEGER DEFAULT 0,
            available_macs TEXT,
            alternate_ids TEXT,
            cmd TEXT,
            channel_hash TEXT,
            PRIMARY KEY (portal_id, channel_id)
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
        CREATE INDEX IF NOT EXISTS idx_channels_display_name
        ON channels(display_name)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_channels_portal_id
        ON channels(portal_id)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_channels_portal_name
        ON channels(portal_name)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_channels_genre_id
        ON channels(genre_id)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_channels_resolution
        ON channels(resolution)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_channels_video_codec
        ON channels(video_codec)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_channels_country
        ON channels(country)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_channels_is_event
        ON channels(is_event)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_channels_is_raw
        ON channels(is_raw)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_channels_is_header
        ON channels(is_header)
    ''')

    # Create groups table for genre/group management
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS groups (
            portal_id TEXT NOT NULL,
            genre_id TEXT NOT NULL,
            name TEXT,
            channel_count INTEGER DEFAULT 0,
            active INTEGER DEFAULT 1,
            PRIMARY KEY (portal_id, genre_id)
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_groups_active
        ON groups(portal_id, active)
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS portal_stats (
            portal_id TEXT PRIMARY KEY,
            portal_name TEXT,
            total_channels INTEGER DEFAULT 0,
            active_channels INTEGER DEFAULT 0,
            total_groups INTEGER DEFAULT 0,
            active_groups INTEGER DEFAULT 0,
            updated_at TEXT
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_portal_stats_name
        ON portal_stats(portal_name)
    ''')

    # EPG sources metadata (central mapping)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS epg_sources (
            source_id TEXT PRIMARY KEY,
            name TEXT,
            url TEXT,
            source_type TEXT,
            enabled INTEGER DEFAULT 1,
            interval_hours REAL,
            last_fetch REAL,
            last_refresh REAL
        )
    ''')

    # EPG channels metadata per source
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS epg_channels (
            source_id TEXT NOT NULL,
            channel_id TEXT NOT NULL,
            display_name TEXT,
            icon TEXT,
            lcn TEXT,
            updated_at REAL,
            PRIMARY KEY (source_id, channel_id)
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_epg_channels_channel
        ON epg_channels(channel_id)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_epg_channels_source
        ON epg_channels(source_id)
    ''')

    # Optional alternate display-names per channel
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS epg_channel_names (
            source_id TEXT NOT NULL,
            channel_id TEXT NOT NULL,
            name TEXT NOT NULL,
            PRIMARY KEY (source_id, channel_id, name)
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_epg_channel_names_name
        ON epg_channel_names(name)
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS channel_tags (
            portal_id TEXT NOT NULL,
            channel_id TEXT NOT NULL,
            tag_type TEXT NOT NULL,
            tag_value TEXT NOT NULL,
            PRIMARY KEY (portal_id, channel_id, tag_type, tag_value)
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_channel_tags_type_value
        ON channel_tags(tag_type, tag_value)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_channel_tags_channel
        ON channel_tags(portal_id, channel_id)
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS group_stats (
            portal_id TEXT NOT NULL,
            portal_name TEXT,
            group_name TEXT NOT NULL,
            channel_count INTEGER DEFAULT 0,
            updated_at TEXT,
            PRIMARY KEY (portal_id, group_name)
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_group_stats_portal_id
        ON group_stats(portal_id)
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS event_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            enabled INTEGER DEFAULT 1,
            provider TEXT DEFAULT 'sportsdb',
            use_espn_events INTEGER DEFAULT 0,
            espn_event_window_hours INTEGER DEFAULT 72,
            sport TEXT DEFAULT '',
            league_filters TEXT DEFAULT '[]',
            team_filters TEXT DEFAULT '[]',
            channel_groups TEXT DEFAULT '[]',
            channel_regex TEXT DEFAULT '',
            epg_pattern TEXT DEFAULT '',
            extract_regex TEXT DEFAULT '',
            output_template TEXT DEFAULT '{home} vs {away} | {date} {time}',
            output_group_name TEXT DEFAULT 'EVENTS',
            channel_number_start INTEGER DEFAULT 10000,
            priority INTEGER DEFAULT 100,
            created_at TEXT,
            updated_at TEXT
        )
    ''')

    # Forward-compatible column migration for existing installations.
    cols = {row["name"] for row in cursor.execute("PRAGMA table_info(event_rules)").fetchall()}
    if "provider" not in cols:
        cursor.execute("ALTER TABLE event_rules ADD COLUMN provider TEXT DEFAULT 'sportsdb'")
    if "use_espn_events" not in cols:
        cursor.execute("ALTER TABLE event_rules ADD COLUMN use_espn_events INTEGER DEFAULT 0")
    if "espn_event_window_hours" not in cols:
        cursor.execute("ALTER TABLE event_rules ADD COLUMN espn_event_window_hours INTEGER DEFAULT 72")
    if "output_group_name" not in cols:
        cursor.execute("ALTER TABLE event_rules ADD COLUMN output_group_name TEXT DEFAULT 'EVENTS'")
    if "channel_number_start" not in cols:
        cursor.execute("ALTER TABLE event_rules ADD COLUMN channel_number_start INTEGER DEFAULT 10000")

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_event_rules_enabled
        ON event_rules(enabled)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_event_rules_priority
        ON event_rules(priority)
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sportsdb_sports_cache (
            sport_name TEXT PRIMARY KEY,
            sport_id TEXT,
            updated_at REAL,
            raw_json TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sportsdb_leagues_cache (
            league_id TEXT PRIMARY KEY,
            league_name TEXT NOT NULL,
            sport_name TEXT,
            updated_at REAL,
            raw_json TEXT
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_sportsdb_leagues_sport
        ON sportsdb_leagues_cache(sport_name)
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sportsdb_teams_cache (
            team_id TEXT PRIMARY KEY,
            team_name TEXT NOT NULL,
            team_aliases TEXT DEFAULT '[]',
            league_id TEXT,
            league_name TEXT,
            sport_name TEXT,
            updated_at REAL,
            raw_json TEXT
        )
    ''')

    team_cols = {row["name"] for row in cursor.execute("PRAGMA table_info(sportsdb_teams_cache)").fetchall()}
    if "team_aliases" not in team_cols:
        cursor.execute("ALTER TABLE sportsdb_teams_cache ADD COLUMN team_aliases TEXT DEFAULT '[]'")

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_sportsdb_teams_league_id
        ON sportsdb_teams_cache(league_id)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_sportsdb_teams_league_name
        ON sportsdb_teams_cache(league_name)
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS espn_sports_cache (
            sport_key TEXT PRIMARY KEY,
            sport_name TEXT NOT NULL,
            updated_at REAL,
            raw_json TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS espn_leagues_cache (
            league_key TEXT PRIMARY KEY,
            league_name TEXT NOT NULL,
            sport_key TEXT NOT NULL,
            updated_at REAL,
            raw_json TEXT
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_espn_leagues_sport
        ON espn_leagues_cache(sport_key)
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS espn_teams_cache (
            team_key TEXT PRIMARY KEY,
            team_id TEXT,
            team_name TEXT NOT NULL,
            team_aliases TEXT DEFAULT '[]',
            sport_key TEXT NOT NULL,
            league_key TEXT NOT NULL,
            league_name TEXT,
            updated_at REAL,
            raw_json TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS espn_scoreboard_cache (
            league_key TEXT NOT NULL,
            date_key TEXT NOT NULL,
            fetched_at REAL,
            raw_json TEXT,
            PRIMARY KEY (league_key, date_key)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS event_generated_channels (
            portal_id TEXT NOT NULL,
            channel_id TEXT NOT NULL,
            event_id TEXT,
            source_portal_id TEXT,
            source_channel_id TEXT,
            created_at REAL,
            expires_at REAL,
            PRIMARY KEY (portal_id, channel_id)
        )
    ''')

    event_channel_cols = {row["name"] for row in cursor.execute("PRAGMA table_info(event_generated_channels)").fetchall()}
    if "source_portal_id" not in event_channel_cols:
        cursor.execute("ALTER TABLE event_generated_channels ADD COLUMN source_portal_id TEXT")
    if "source_channel_id" not in event_channel_cols:
        cursor.execute("ALTER TABLE event_generated_channels ADD COLUMN source_channel_id TEXT")

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_event_generated_event
        ON event_generated_channels(event_id)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_event_generated_source
        ON event_generated_channels(source_portal_id, source_channel_id)
    ''')

    espn_team_cols = {row["name"] for row in cursor.execute("PRAGMA table_info(espn_teams_cache)").fetchall()}
    if "team_aliases" not in espn_team_cols:
        cursor.execute("ALTER TABLE espn_teams_cache ADD COLUMN team_aliases TEXT DEFAULT '[]'")

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_espn_teams_league
        ON espn_teams_cache(league_key)
    ''')

    conn.commit()

    conn.close()


def vacuum_channels_db():
    """VACUUM the main channels database."""
    conn = get_db_connection()
    conn.execute("VACUUM")
    conn.close()


def vacuum_epg_dbs():
    """VACUUM all per-source EPG SQLite databases."""
    epg_dir = os.path.join(DATA_DIR, "epg_sources")
    if not os.path.isdir(epg_dir):
        return 0
    count = 0
    for name in os.listdir(epg_dir):
        if not name.endswith(".sqlite"):
            continue
        path = os.path.join(epg_dir, name)
        try:
            conn = sqlite3.connect(path)
            conn.execute("VACUUM")
            conn.close()
            count += 1
        except Exception:
            continue
    return count
