import sqlite3

from .config import DB_PATH


def get_db_connection():
    """Get a database connection."""
    conn = sqlite3.connect(DB_PATH)
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

    conn.commit()

    conn.close()


def cleanup_db(*, vacuum=False):
    """Cleanup derived tag fields and optionally VACUUM/ANALYZE the DB."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE channels
        SET video_codec = ''
        """
    )
    updated = cursor.rowcount
    conn.commit()

    if vacuum:
        conn.execute("ANALYZE")
        conn.execute("VACUUM")

    conn.close()
    return {"updated": updated, "vacuumed": bool(vacuum)}
