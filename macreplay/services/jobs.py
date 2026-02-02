import time
import threading
from collections import deque
from datetime import datetime


class JobManager:
    def __init__(
        self,
        *,
        logger,
        refresh_channels_cache,
        run_portal_matching,
        refresh_xmltv,
        getSettings,
        getPortals,
        get_db_connection,
        ACTIVE_GROUP_CONDITION,
        channelsdvr_match_status,
        channelsdvr_match_status_lock,
        channels_refresh_status,
        channels_refresh_status_lock,
        set_cached_xmltv=None,
        max_workers=2,
        max_retries=2,
    ):
        self.logger = logger
        self.refresh_channels_cache = refresh_channels_cache
        self.run_portal_matching = run_portal_matching
        self.refresh_xmltv = refresh_xmltv
        self.getSettings = getSettings
        self.getPortals = getPortals
        self.get_db_connection = get_db_connection
        self.ACTIVE_GROUP_CONDITION = ACTIVE_GROUP_CONDITION
        self.channelsdvr_match_status = channelsdvr_match_status
        self.channelsdvr_match_status_lock = channelsdvr_match_status_lock
        self.channels_refresh_status = channels_refresh_status
        self.channels_refresh_status_lock = channels_refresh_status_lock
        self.set_cached_xmltv = set_cached_xmltv

        self.queue = deque()
        self.queue_lock = threading.Lock()
        self.queued_keys = set()
        self.in_flight = set()
        self.in_flight_lock = threading.Lock()
        self.portal_locks = {}
        self.portal_locks_lock = threading.Lock()
        self.epg_lock = threading.Lock()

        self.worker_state_lock = threading.Lock()
        self.running_workers = 0
        self.max_workers = max_workers
        self.max_retries = max_retries

        self.portal_refresh_status = {}
        self.portal_refresh_status_lock = threading.Lock()

    def enqueue_refresh_portal(self, portal_id, reason="manual"):
        return self._enqueue_job("refresh_portal", portal_id, reason=reason)

    def enqueue_refresh_all(self, reason="scheduled"):
        portals = self.getPortals()
        enqueued = 0
        for portal_id, portal in portals.items():
            if portal.get("enabled") == "true":
                status = self.enqueue_refresh_portal(portal_id, reason=reason)
                if status in ("queued", "running"):
                    enqueued += 1
        return enqueued

    def enqueue_epg_refresh(self, reason="manual"):
        return self._enqueue_job("refresh_epg", None, reason=reason)

    def get_portal_refresh_status(self, portal_id):
        with self.portal_refresh_status_lock:
            return dict(self.portal_refresh_status.get(portal_id) or {})

    def _enqueue_job(self, job_type, portal_id, reason=None):
        key = (job_type, portal_id)
        with self.in_flight_lock:
            if key in self.in_flight:
                return "running"
        with self.queue_lock:
            if key in self.queued_keys:
                return "queued"
            job = {
                "type": job_type,
                "portal_id": portal_id,
                "reason": reason or "",
                "attempts": 0,
                "run_at": time.time(),
            }
            self.queue.append(job)
            self.queued_keys.add(key)
        if job_type == "refresh_portal":
            self._mark_portal_refresh_queued(portal_id, reason)
            self._mark_match_queued_if_needed(portal_id)
        self._ensure_workers()
        return "queued"

    def _ensure_workers(self):
        with self.worker_state_lock:
            while self.running_workers < self.max_workers:
                with self.queue_lock:
                    if not self.queue:
                        return
                thread = threading.Thread(target=self._worker, daemon=True)
                thread.start()
                self.running_workers += 1

    def _worker(self):
        try:
            while True:
                job = None
                with self.queue_lock:
                    if not self.queue:
                        return
                    job = self.queue.popleft()
                    key = (job["type"], job["portal_id"])
                    self.queued_keys.discard(key)

                now = time.time()
                if job["run_at"] > now:
                    with self.queue_lock:
                        self.queue.append(job)
                        self.queued_keys.add(key)
                    time.sleep(0.5)
                    continue

                with self.in_flight_lock:
                    self.in_flight.add(key)

                portal_lock = None
                if job["portal_id"]:
                    portal_lock = self._get_portal_lock(job["portal_id"])
                    portal_lock.acquire()
                elif job["type"] == "refresh_epg":
                    if not self.epg_lock.acquire(blocking=False):
                        with self.in_flight_lock:
                            self.in_flight.discard(key)
                        with self.queue_lock:
                            self.queue.append(job)
                            self.queued_keys.add(key)
                        time.sleep(0.5)
                        continue

                try:
                    self._run_job(job)
                except Exception as exc:
                    job["attempts"] += 1
                    if job["attempts"] <= self.max_retries:
                        backoff = min(60, 2 ** job["attempts"])
                        job["run_at"] = time.time() + backoff
                        with self.queue_lock:
                            self.queue.append(job)
                            self.queued_keys.add(key)
                        self.logger.error(
                            "Job %s for %s failed (retry in %ss): %s",
                            job["type"],
                            job["portal_id"],
                            backoff,
                            exc,
                        )
                    else:
                        self._mark_job_error(job, exc)
                finally:
                    if portal_lock:
                        portal_lock.release()
                    if job["type"] == "refresh_epg" and self.epg_lock.locked():
                        self.epg_lock.release()
                    with self.in_flight_lock:
                        self.in_flight.discard(key)
        finally:
            with self.worker_state_lock:
                self.running_workers = max(0, self.running_workers - 1)

    def _run_job(self, job):
        job_type = job["type"]
        if job_type == "refresh_portal":
            self._run_refresh_portal(job["portal_id"])
            return
        if job_type == "refresh_epg":
            self.logger.info("EPG job: refresh_xmltv")
            self.refresh_xmltv()
            return
        self.logger.warning("Unknown job type: %s", job_type)

    def _run_refresh_portal(self, portal_id):
        portals = self.getPortals()
        portal = portals.get(portal_id, {})
        portal_name = portal.get("name", portal_id)
        self.logger.info("Job refresh_portal started: %s", portal_name)

        self._mark_portal_refresh_running(portal_id)
        if self.set_cached_xmltv:
            self.set_cached_xmltv(None)

        total = self.refresh_channels_cache(target_portal_id=portal_id)
        stats = self._compute_portal_stats(portal_id)
        stats["total"] = total
        self._mark_portal_refresh_completed(portal_id, stats)

        if self._should_match_portal(portal_id):
            self._run_matching(portal_id)

        self.enqueue_epg_refresh(reason="portal_refresh")
        self.logger.info("Job refresh_portal completed: %s", portal_name)

    def _run_matching(self, portal_id):
        with self.channelsdvr_match_status_lock:
            self.channelsdvr_match_status[portal_id] = {
                "status": "running",
                "started_at": datetime.utcnow().isoformat(),
                "completed_at": None,
                "matched": 0,
                "error": None,
            }
        try:
            matched_count = self.run_portal_matching(portal_id)
            with self.channelsdvr_match_status_lock:
                self.channelsdvr_match_status[portal_id].update(
                    {
                        "status": "completed",
                        "completed_at": datetime.utcnow().isoformat(),
                        "matched": matched_count,
                    }
                )
        except Exception as exc:
            with self.channelsdvr_match_status_lock:
                self.channelsdvr_match_status[portal_id].update(
                    {
                        "status": "error",
                        "completed_at": datetime.utcnow().isoformat(),
                        "error": str(exc),
                    }
                )
            raise

    def _should_match_portal(self, portal_id):
        settings = self.getSettings()
        if settings.get("channelsdvr enabled", "false") != "true":
            return False
        portal = self.getPortals().get(portal_id, {})
        return portal.get("auto match", "false") == "true"

    def _mark_match_queued_if_needed(self, portal_id):
        if not self._should_match_portal(portal_id):
            return
        with self.channelsdvr_match_status_lock:
            self.channelsdvr_match_status[portal_id] = {
                "status": "queued",
                "started_at": None,
                "completed_at": None,
                "matched": 0,
                "error": None,
            }

    def _mark_portal_refresh_queued(self, portal_id, reason):
        with self.portal_refresh_status_lock:
            self.portal_refresh_status[portal_id] = {
                "status": "queued",
                "queued_at": datetime.utcnow().isoformat(),
                "started_at": None,
                "completed_at": None,
                "error": None,
                "stats": None,
                "reason": reason or "",
            }

    def _mark_portal_refresh_running(self, portal_id):
        with self.portal_refresh_status_lock:
            status = self.portal_refresh_status.get(portal_id) or {}
            status.update(
                {
                    "status": "running",
                    "started_at": datetime.utcnow().isoformat(),
                    "completed_at": None,
                    "error": None,
                }
            )
            self.portal_refresh_status[portal_id] = status

        with self.channels_refresh_status_lock:
            self.channels_refresh_status["status"] = "running"
            self.channels_refresh_status["portal_id"] = portal_id
            self.channels_refresh_status["started_at"] = datetime.utcnow().isoformat()
            self.channels_refresh_status["completed_at"] = None
            self.channels_refresh_status["error"] = None

    def _mark_portal_refresh_completed(self, portal_id, stats):
        with self.portal_refresh_status_lock:
            status = self.portal_refresh_status.get(portal_id) or {}
            status.update(
                {
                    "status": "completed",
                    "completed_at": datetime.utcnow().isoformat(),
                    "error": None,
                    "stats": stats,
                }
            )
            self.portal_refresh_status[portal_id] = status

        with self.channels_refresh_status_lock:
            self.channels_refresh_status["status"] = "completed"
            self.channels_refresh_status["completed_at"] = datetime.utcnow().isoformat()

    def _mark_job_error(self, job, exc):
        if job["type"] == "refresh_portal":
            with self.portal_refresh_status_lock:
                status = self.portal_refresh_status.get(job["portal_id"]) or {}
                status.update(
                    {
                        "status": "error",
                        "completed_at": datetime.utcnow().isoformat(),
                        "error": str(exc),
                    }
                )
                self.portal_refresh_status[job["portal_id"]] = status
            with self.channels_refresh_status_lock:
                self.channels_refresh_status["status"] = "error"
                self.channels_refresh_status["completed_at"] = datetime.utcnow().isoformat()
                self.channels_refresh_status["error"] = str(exc)
        self.logger.error("Job %s failed: %s", job["type"], exc)

    def _compute_portal_stats(self, portal_id):
        conn = self.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT
                COUNT(*) as total_channels,
                SUM(CASE WHEN {self.ACTIVE_GROUP_CONDITION} THEN 1 ELSE 0 END) as active_channels
            FROM channels c
            LEFT JOIN groups g ON c.portal_id = g.portal_id AND c.genre_id = g.genre_id
            WHERE c.portal_id = ?
            """,
            [portal_id],
        )
        ch_row = cursor.fetchone()

        cursor.execute(
            """
            SELECT
                COUNT(*) as total_groups,
                SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) as active_groups
            FROM groups WHERE portal_id = ?
            """,
            [portal_id],
        )
        gr_row = cursor.fetchone()
        conn.close()
        return {
            "total_channels": ch_row[0] or 0,
            "channels": ch_row[1] or 0,
            "total_groups": gr_row[0] or 0,
            "groups": gr_row[1] or 0,
        }

    def _get_portal_lock(self, portal_id):
        with self.portal_locks_lock:
            lock = self.portal_locks.get(portal_id)
            if lock is None:
                lock = threading.Lock()
                self.portal_locks[portal_id] = lock
            return lock
