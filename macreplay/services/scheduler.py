import threading
import time


def start_epg_scheduler(state):
    """Start a background thread that periodically refreshes EPG data."""
    def epg_refresh_loop():
        while True:
            try:
                scheduler = state.scheduler
                interval_hours = scheduler.get_epg_refresh_interval()
                interval_seconds = max(60, int(interval_hours * 3600))

                scheduler.logger.info(
                    "EPG scheduler: Next refresh in %s hours (%s seconds)",
                    interval_hours,
                    interval_seconds,
                )
                time.sleep(interval_seconds)

                scheduler.logger.info("EPG scheduler: Queueing scheduled EPG refresh...")
                scheduler.job_manager.enqueue_epg_refresh(reason="scheduled")
                scheduler.logger.info("EPG scheduler: EPG refresh queued.")

            except Exception as exc:
                scheduler.logger.error("EPG scheduler error: %s", exc)
                time.sleep(300)

    scheduler_thread = threading.Thread(target=epg_refresh_loop, daemon=True)
    scheduler_thread.start()
    state.scheduler.logger.info("EPG background scheduler started!")


def start_channel_scheduler(state):
    """Start a background thread that periodically refreshes channel data from portals."""
    def channel_refresh_loop():
        while True:
            try:
                scheduler = state.scheduler
                interval_hours = scheduler.get_channel_refresh_interval()

                if interval_hours <= 0:
                    scheduler.logger.info(
                        "Channel scheduler: Automatic channel refresh disabled (interval = 0)"
                    )
                    time.sleep(3600)
                    continue

                interval_seconds = max(60, int(interval_hours * 3600))

                scheduler.logger.info(
                    "Channel scheduler: Next refresh in %s hours (%s seconds)",
                    interval_hours,
                    interval_seconds,
                )
                time.sleep(interval_seconds)

                scheduler.logger.info("Channel scheduler: Queueing scheduled channel refresh...")
                total = scheduler.job_manager.enqueue_refresh_all(reason="scheduled")
                scheduler.logger.info(
                    "Channel scheduler: Channel refresh queued (%s portals).", total
                )

            except Exception as exc:
                scheduler.logger.error("Channel scheduler error: %s", exc)
                time.sleep(300)

    scheduler_thread = threading.Thread(target=channel_refresh_loop, daemon=True)
    scheduler_thread.start()
    state.scheduler.logger.info("Channel background scheduler started!")


def start_vacuum_channels_scheduler(*, getSettings, logger):
    def vacuum_loop():
        while True:
            try:
                interval_hours = float(getSettings().get("vacuum channels interval hours", 0) or 0)
                if interval_hours <= 0:
                    time.sleep(3600)
                    continue
                logger.info("Channels DB vacuum scheduler: next run in %s hours", interval_hours)
                time.sleep(max(60, int(interval_hours * 3600)))
                logger.info("Channels DB vacuum scheduler: running VACUUM...")
                from macreplay.db import vacuum_channels_db
                vacuum_channels_db()
                logger.info("Channels DB vacuum scheduler: completed.")
            except Exception as exc:
                logger.error("Channels DB vacuum scheduler error: %s", exc)
                time.sleep(300)

    threading.Thread(target=vacuum_loop, daemon=True).start()
    logger.info("Channels DB vacuum scheduler started!")


def start_vacuum_epg_scheduler(*, getSettings, logger):
    def vacuum_loop():
        while True:
            try:
                interval_hours = float(getSettings().get("vacuum epg interval hours", 0) or 0)
                if interval_hours <= 0:
                    time.sleep(3600)
                    continue
                logger.info("EPG DB vacuum scheduler: next run in %s hours", interval_hours)
                time.sleep(max(60, int(interval_hours * 3600)))
                logger.info("EPG DB vacuum scheduler: running VACUUM...")
                from macreplay.db import vacuum_epg_dbs
                count = vacuum_epg_dbs()
                logger.info("EPG DB vacuum scheduler: completed (%s dbs).", count)
            except Exception as exc:
                logger.error("EPG DB vacuum scheduler error: %s", exc)
                time.sleep(300)

    threading.Thread(target=vacuum_loop, daemon=True).start()
    logger.info("EPG DB vacuum scheduler started!")


def start_custom_epg_scheduler(*, refresh_custom_sources, logger):
    """Start a lightweight scheduler for custom XMLTV sources.

    The per-source interval enforcement is handled inside refresh_custom_sources().
    This loop only triggers periodic checks.
    """

    def custom_epg_loop():
        while True:
            try:
                logger.info("Custom EPG scheduler: checking custom sources...")
                refresh_custom_sources()
            except Exception as exc:
                logger.error("Custom EPG scheduler error: %s", exc)
            # Keep checks frequent enough so per-source intervals are respected reliably.
            time.sleep(300)

    threading.Thread(target=custom_epg_loop, daemon=True).start()
    logger.info("Custom EPG scheduler started!")


def start_event_channel_cleanup_scheduler(*, getSettings, logger):
    """Start scheduler that removes expired event-generated channels."""

    def cleanup_loop():
        while True:
            try:
                interval_min = float(
                    getSettings().get("events cleanup interval minutes", 5) or 5
                )
                if interval_min <= 0:
                    time.sleep(3600)
                    continue

                time.sleep(max(30, int(interval_min * 60)))
                from macreplay.db import cleanup_expired_event_channels

                deleted = cleanup_expired_event_channels()
                if deleted:
                    logger.info(
                        "Event channel cleanup: removed %s expired channel(s).",
                        deleted,
                    )
            except Exception as exc:
                logger.error("Event channel cleanup scheduler error: %s", exc)
                time.sleep(300)

    threading.Thread(target=cleanup_loop, daemon=True).start()
    logger.info("Event channel cleanup scheduler started!")
