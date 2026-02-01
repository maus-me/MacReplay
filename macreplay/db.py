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
            portal TEXT NOT NULL,
            channel_id TEXT NOT NULL,
            portal_name TEXT,
            name TEXT,
            display_name TEXT,
            number TEXT,
            genre TEXT,
            genre_id TEXT,
            logo TEXT,
            enabled INTEGER DEFAULT 0,
            custom_name TEXT,
            auto_name TEXT,
            custom_number TEXT,
            custom_genre TEXT,
            custom_epg_id TEXT,
            fallback_channel TEXT,
            resolution TEXT,
            video_codec TEXT,
            country TEXT,
            audio_tags TEXT,
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
            PRIMARY KEY (portal, channel_id)
        )
    ''')

    # Add genre_id column if it doesn't exist (migration for existing databases)
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN genre_id TEXT")
    except Exception:
        pass  # Column already exists

    # Add available_macs column to track which MACs can access the channel (comma-separated)
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN available_macs TEXT")
    except Exception:
        pass  # Column already exists

    # Add alternate_ids column to store alternative channel IDs (comma-separated)
    # Used for merged channels - if primary ID fails, try alternates
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN alternate_ids TEXT")
    except Exception:
        pass  # Column already exists

    # Add cmd column to cache the stream command URL (avoids fetching all channels on every stream)
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN cmd TEXT")
    except Exception:
        pass  # Column already exists

    # Add channel_hash column to support incremental refresh updates
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN channel_hash TEXT")
    except Exception:
        pass  # Column already exists

    # Add display_name column for fast name lookup/sort
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN display_name TEXT")
    except Exception:
        pass  # Column already exists

    # Add auto_name column for auto-normalized channel names
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN auto_name TEXT")
    except Exception:
        pass  # Column already exists

    # Add tag columns for extraction
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN resolution TEXT")
    except Exception:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN video_codec TEXT")
    except Exception:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN country TEXT")
    except Exception:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN audio_tags TEXT")
    except Exception:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN event_tags TEXT")
    except Exception:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN misc_tags TEXT")
    except Exception:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN matched_name TEXT")
    except Exception:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN matched_source TEXT")
    except Exception:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN matched_station_id TEXT")
    except Exception:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN matched_call_sign TEXT")
    except Exception:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN matched_logo TEXT")
    except Exception:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN matched_score REAL")
    except Exception:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN is_header INTEGER DEFAULT 0")
    except Exception:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN is_event INTEGER DEFAULT 0")
    except Exception:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE channels ADD COLUMN is_raw INTEGER DEFAULT 0")
    except Exception:
        pass  # Column already exists

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
        CREATE INDEX IF NOT EXISTS idx_channels_portal
        ON channels(portal)
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

    # Backfill display_name for existing rows (custom > matched > auto > name)
    try:
        cursor.execute('''
            UPDATE channels
            SET display_name = COALESCE(NULLIF(custom_name, ''), NULLIF(matched_name, ''), NULLIF(auto_name, ''), name)
            WHERE display_name IS NULL OR display_name = ''
        ''')
    except Exception:
        pass

    # Create groups table for genre/group management
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS groups (
            portal TEXT NOT NULL,
            genre_id TEXT NOT NULL,
            name TEXT,
            channel_count INTEGER DEFAULT 0,
            active INTEGER DEFAULT 1,
            PRIMARY KEY (portal, genre_id)
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_groups_active
        ON groups(portal, active)
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS portal_stats (
            portal TEXT PRIMARY KEY,
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
            portal TEXT NOT NULL,
            portal_name TEXT,
            group_name TEXT NOT NULL,
            channel_count INTEGER DEFAULT 0,
            updated_at TEXT,
            PRIMARY KEY (portal, group_name)
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_group_stats_portal
        ON group_stats(portal)
    ''')

    conn.commit()

    # Migration: populate groups table from existing channels data
    cursor.execute("SELECT COUNT(*) FROM groups")
    if cursor.fetchone()[0] == 0:
        logger.info("Migrating: populating groups table from existing channels...")
        cursor.execute('''
            INSERT OR IGNORE INTO groups (portal, genre_id, name, channel_count, active)
            SELECT portal, genre_id, genre, COUNT(*) as cnt, 1
            FROM channels
            WHERE genre_id IS NOT NULL AND genre_id != ''
            GROUP BY portal, genre_id
        ''')
        # Set active flag based on selected_genres from JSON config
        try:
            portals = get_portals()
            for portal_id, portal in portals.items():
                selected_genres = portal.get("selected_genres", [])
                if selected_genres:
                    selected_genres = [str(g) for g in selected_genres]
                    # Deactivate all groups for this portal first
                    cursor.execute("UPDATE groups SET active = 0 WHERE portal = ?", [portal_id])
                    # Activate only selected groups
                    for genre_id in selected_genres:
                        cursor.execute(
                            "UPDATE groups SET active = 1 WHERE portal = ? AND genre_id = ?",
                            [portal_id, genre_id],
                        )
                    logger.info(
                        f"Migrated genre selection for portal {portal.get('name', portal_id)}: {len(selected_genres)} active groups"
                    )
        except Exception as e:
            logger.error(f"Error migrating genre selections: {e}")

        conn.commit()

    conn.close()


def cleanup_db(*, vacuum=False):
    """Cleanup derived tag fields and optionally VACUUM/ANALYZE the DB."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE channels
        SET video_codec = '', audio_tags = ''
        """
    )
    updated = cursor.rowcount
    conn.commit()

    if vacuum:
        conn.execute("ANALYZE")
        conn.execute("VACUUM")

    conn.close()
    return {"updated": updated, "vacuumed": bool(vacuum)}
