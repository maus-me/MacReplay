import os
import time


def save_epg_cache(cached_xmltv, last_updated, logger, epg_cache_path):
    """Save EPG cache to file for persistence."""
    if cached_xmltv is None:
        return False
    try:
        with open(epg_cache_path, "w", encoding="utf-8") as f:
            f.write(cached_xmltv)
        meta_path = epg_cache_path + ".meta"
        with open(meta_path, "w") as f:
            f.write(str(last_updated))
        logger.info(f"EPG cache saved to {epg_cache_path}")
        return True
    except Exception as e:
        logger.error(f"Error saving EPG cache: {e}")
        return False


def load_epg_cache(logger, epg_cache_path):
    """Load EPG cache from file if it exists and is valid."""
    try:
        if not os.path.exists(epg_cache_path):
            logger.info("No EPG cache file found")
            return None, 0, False

        meta_path = epg_cache_path + ".meta"
        if os.path.exists(meta_path):
            with open(meta_path, "r") as f:
                last_updated = float(f.read().strip())
        else:
            last_updated = os.path.getmtime(epg_cache_path)

        with open(epg_cache_path, "r", encoding="utf-8") as f:
            cached_xmltv = f.read()

        cache_age_hours = (time.time() - last_updated) / 3600
        logger.info(
            f"EPG cache loaded from {epg_cache_path} (age: {cache_age_hours:.2f} hours)"
        )
        return cached_xmltv, last_updated, True
    except Exception as e:
        logger.error(f"Error loading EPG cache: {e}")
        return None, 0, False


def is_epg_cache_valid(cached_xmltv, last_updated, get_epg_refresh_interval):
    """Check if EPG cache is still valid based on refresh interval."""
    if cached_xmltv is None or last_updated == 0:
        return False
    interval_hours = get_epg_refresh_interval()
    age_hours = (time.time() - last_updated) / 3600
    return age_hours < interval_hours
