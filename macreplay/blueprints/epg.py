import json
import os
from datetime import datetime, timezone, timedelta

from flask import Blueprint, Response, jsonify, render_template, request

from ..security import authorise


def create_epg_blueprint(
    *,
    refresh_xmltv,
    refresh_epg_for_ids,
    enqueue_epg_refresh,
    get_cached_xmltv,
    get_last_updated,
    get_epg_refresh_status,
    logger,
    getPortals,
    get_db_connection,
    effective_epg_name,
    getSettings,
    open_epg_source_db,
):
    bp = Blueprint("epg", __name__)

    @bp.route("/xmltv", methods=["GET"])
    @authorise
    def xmltv():
        logger.info("Guide Requested")

        cached_xmltv = get_cached_xmltv()
        last_updated = get_last_updated()

        if cached_xmltv is None:
            logger.info("No EPG cache exists, fetching now (this may take a moment)...")
            refresh_xmltv()
        elif (datetime.now(timezone.utc).timestamp() - last_updated) > 900:
            logger.info("EPG cache is stale, triggering background refresh...")
            enqueue_epg_refresh(reason="cache_stale")

        cached_xmltv = get_cached_xmltv()
        return Response(cached_xmltv, mimetype="text/xml")

    @bp.route("/epg")
    @authorise
    def epg_viewer():
        return render_template("epg.html")

    @bp.route("/api/epg")
    @authorise
    def api_epg():
        try:
            settings = getSettings()
            epg_past_hours = int(settings.get("epg past hours", "2"))
            epg_future_hours = int(settings.get("epg future hours", "24"))
            now = datetime.now(timezone.utc)
            past_cutoff_ts = int((now - timedelta(hours=epg_past_hours)).timestamp())
            future_cutoff_ts = int((now + timedelta(hours=epg_future_hours)).timestamp())

            portals = getPortals()
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT source_id, name
                FROM epg_sources
                """
            )
            source_name_map = {
                row["source_id"]: (row["name"] or row["source_id"])
                for row in cursor.fetchall()
            }
            cursor.execute(
                """
                SELECT
                    portal_id as portal,
                    name,
                    logo,
                    custom_name,
                    auto_name,
                    matched_name,
                    custom_epg_id,
                    number,
                    custom_number
                FROM channels WHERE enabled = 1
                """
            )
            rows = cursor.fetchall()
            epg_ids = set()
            for row in rows:
                if row["custom_epg_id"]:
                    epg_ids.add(row["custom_epg_id"])
                default_id = effective_epg_name(row["custom_name"], row["auto_name"], row["name"])
                if default_id:
                    epg_ids.add(default_id)

            epg_meta = {}
            if epg_ids:
                ids = list(epg_ids)
                chunk_size = 900
                for i in range(0, len(ids), chunk_size):
                    chunk = ids[i:i + chunk_size]
                    placeholders = ",".join(["?"] * len(chunk))
                    cursor.execute(
                        f"""
                        SELECT source_id, channel_id, display_name, icon
                        FROM epg_channels
                        WHERE channel_id IN ({placeholders})
                        """,
                        chunk,
                    )
                    for row in cursor.fetchall():
                        epg_meta.setdefault(row["channel_id"], []).append(
                            {
                                "source_id": row["source_id"],
                                "display_name": row["display_name"],
                                "icon": row["icon"],
                            }
                        )

            for channel_id in epg_meta:
                epg_meta[channel_id].sort(key=lambda entry: entry["source_id"] or "")
            conn.close()

            channel_portal_map = {}
            channel_sources = {}
            channels = []
            seen_channel_keys = set()
            matched_group_counts = {}
            for row in rows:
                matched_name = (row["matched_name"] or "").strip()
                if not matched_name:
                    continue
                key = matched_name.lower()
                matched_group_counts[key] = matched_group_counts.get(key, 0) + 1

            def resolve_display_name(row):
                return (
                    row["custom_name"]
                    or row["matched_name"]
                    or row["auto_name"]
                    or row["name"]
                )

            def select_epg_meta(epg_id, portal_id):
                entries = epg_meta.get(epg_id)
                if not entries:
                    return None
                for entry in entries:
                    if entry["source_id"] == portal_id:
                        return entry
                return entries[0]

            for row in rows:
                portal_id = row["portal"]
                portal_name = portals.get(portal_id, {}).get("name", portal_id)
                default_id = effective_epg_name(row["custom_name"], row["auto_name"], row["name"])
                custom_id = row["custom_epg_id"] or ""

                if custom_id:
                    epg_id = custom_id
                    epg_entry = select_epg_meta(custom_id, portal_id)
                    source_id = epg_entry["source_id"] if epg_entry else None
                else:
                    epg_id = default_id
                    epg_entry = select_epg_meta(epg_id, portal_id) if epg_id else None
                    source_id = epg_entry["source_id"] if epg_entry else None
                    if not source_id and epg_id:
                        source_id = portal_id

                if not epg_id:
                    continue
                matched_name = (row["matched_name"] or "").strip()
                matched_key = matched_name.lower() if matched_name else ""
                is_grouped = bool(matched_key and matched_group_counts.get(matched_key, 0) > 1)
                if is_grouped:
                    channel_key = f"group::{matched_key}"
                else:
                    channel_key = f"{portal_id}::{epg_id}"

                display_name = (
                    (epg_entry["display_name"] if epg_entry else None)
                    or resolve_display_name(row)
                )
                icon = (epg_entry["icon"] if epg_entry else None) or row["logo"]
                channel_number = row["custom_number"] or row["number"]
                if is_grouped:
                    portal_label = f"{matched_group_counts.get(matched_key, 0)} Portale"
                else:
                    portal_label = portal_name
                channel_portal_map[channel_key] = portal_label
                source_name = None
                if source_id:
                    source_name = (
                        source_name_map.get(source_id)
                        or portals.get(source_id, {}).get("name")
                        or source_id
                    )
                if source_id:
                    channel_sources.setdefault(source_id, {}).setdefault(epg_id, set()).add(channel_key)
                if channel_key in seen_channel_keys:
                    continue
                seen_channel_keys.add(channel_key)
                channels.append(
                    {
                        "id": channel_key,
                        "epg_id": epg_id,
                        "name": display_name or epg_id,
                        "number": channel_number,
                        "logo": icon,
                        "portal": portal_label,
                        "source_id": source_id,
                        "source_name": source_name,
                    }
                )

            programmes = []

            for source_id, ids in channel_sources.items():
                conn = open_epg_source_db(source_id)
                if conn is None:
                    continue
                cursor = conn.cursor()
                id_list = list(ids.keys())
                chunk_size = 900
                for i in range(0, len(id_list), chunk_size):
                    chunk = id_list[i:i + chunk_size]
                    placeholders = ",".join(["?"] * len(chunk))
                    cursor.execute(
                        f"""
                        SELECT
                            channel_id, start, stop, start_ts, stop_ts, title, description,
                            sub_title, categories, episode_num, episode_system, rating,
                            programme_icon, air_date, previously_shown, series_id
                        FROM epg_programmes
                        WHERE channel_id IN ({placeholders})
                          AND stop_ts >= ?
                          AND start_ts <= ?
                        ORDER BY start_ts ASC
                        """,
                        [*chunk, past_cutoff_ts, future_cutoff_ts],
                    )
                    for row in cursor.fetchall():
                        start_ts = row["start_ts"]
                        stop_ts = row["stop_ts"]
                        if start_ts is None or stop_ts is None:
                            continue
                        start_dt = datetime.fromtimestamp(start_ts, tz=timezone.utc)
                        stop_dt = datetime.fromtimestamp(stop_ts, tz=timezone.utc)
                        channel_keys = ids.get(row["channel_id"], set())
                        if not channel_keys:
                            continue
                        base_programme = {
                            "start": start_dt.isoformat(),
                            "stop": stop_dt.isoformat(),
                            "start_timestamp": start_ts,
                            "stop_timestamp": stop_ts,
                            "title": row["title"] or "Unknown",
                            "description": row["description"] or "",
                            "sub_title": row["sub_title"] or "",
                            "categories": json.loads(row["categories"])
                            if row["categories"]
                            else [],
                            "episode_num": row["episode_num"] or "",
                            "episode_system": row["episode_system"] or "",
                            "rating": row["rating"] or "",
                            "programme_icon": row["programme_icon"] or "",
                            "air_date": row["air_date"] or "",
                            "previously_shown": row["previously_shown"],
                            "series_id": row["series_id"] or "",
                            "is_current": start_dt <= now <= stop_dt,
                            "is_past": stop_dt < now,
                        }
                        for channel_key in channel_keys:
                            programmes.append({"channel": channel_key, **base_programme})
                conn.close()

            programmes.sort(key=lambda x: x["start_timestamp"])

            if programmes:
                earliest = min(p["start_timestamp"] for p in programmes)
                latest = max(p["stop_timestamp"] for p in programmes)
                current_count = sum(1 for p in programmes if p["is_current"])
                logger.debug(
                    "EPG API: %s programmes, %s current, range: %s - %s UTC",
                    len(programmes),
                    current_count,
                    datetime.utcfromtimestamp(earliest),
                    datetime.utcfromtimestamp(latest),
                )

            earliest_ts = (
                min(p["start_timestamp"] for p in programmes) if programmes else 0
            )
            latest_ts = max(p["stop_timestamp"] for p in programmes) if programmes else 0

            return jsonify(
                {
                    "channels": channels,
                    "programmes": programmes,
                    "last_updated": get_last_updated(),
                    "current_time": now.isoformat(),
                    "debug": {
                        "server_time_utc": now.isoformat(),
                        "container_tz": os.environ.get("TZ", "UTC"),
                        "programme_count": len(programmes),
                        "current_programme_count": sum(
                            1 for p in programmes if p["is_current"]
                        ),
                        "earliest_programme": datetime.utcfromtimestamp(earliest_ts).isoformat() + "Z"
                        if earliest_ts
                        else None,
                        "latest_programme": datetime.utcfromtimestamp(latest_ts).isoformat() + "Z"
                        if latest_ts
                        else None,
                    },
                }
            )

        except Exception as e:
            logger.error(f"Error parsing EPG data: {e}")
            return jsonify({"error": str(e), "channels": [], "programmes": []})

    @bp.route("/api/epg/refresh", methods=["POST"])
    @authorise
    def api_epg_refresh():
        epg_refresh_status = get_epg_refresh_status()
        try:
            if epg_refresh_status["is_refreshing"]:
                return jsonify(
                    {
                        "status": "already_running",
                        "message": "EPG refresh is already in progress",
                        "started_at": epg_refresh_status["started_at"],
                    }
                )

            payload = None
            try:
                payload = request.get_json(silent=True) or {}
            except Exception:
                payload = {}

            epg_ids = payload.get("epg_ids") or payload.get("epgIds") or []
            if isinstance(epg_ids, str):
                epg_ids = [epg_ids]
            epg_ids = [value for value in epg_ids if str(value).strip()]

            if epg_ids:
                ok, message = refresh_epg_for_ids(epg_ids, cache_only=True)
                status = "success" if ok else "error"
                return jsonify({"status": status, "message": message})

            enqueue_epg_refresh(reason="manual_api")
            logger.info("Manual EPG refresh triggered via API")
            return jsonify({"status": "started", "message": "EPG refresh started"})
        except Exception as e:
            logger.error(f"Error triggering EPG refresh: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500

    @bp.route("/api/epg/status", methods=["GET"])
    @authorise
    def api_epg_status():
        epg_refresh_status = get_epg_refresh_status()
        return jsonify(
            {
                "is_refreshing": epg_refresh_status["is_refreshing"],
                "started_at": epg_refresh_status["started_at"],
                "completed_at": epg_refresh_status["completed_at"],
                "last_error": epg_refresh_status["last_error"],
                "last_updated": get_last_updated(),
            }
        )

    return bp
