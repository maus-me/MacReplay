import os
import threading
from datetime import datetime, timezone, timedelta
import xml.etree.ElementTree as ET

from flask import Blueprint, Response, jsonify, render_template

from ..security import authorise


def create_epg_blueprint(
    *,
    refresh_xmltv,
    get_cached_xmltv,
    get_last_updated,
    get_epg_refresh_status,
    logger,
    getPortals,
    get_db_connection,
    effective_channel_name,
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
            threading.Thread(target=refresh_xmltv, daemon=True).start()

        cached_xmltv = get_cached_xmltv()
        return Response(cached_xmltv, mimetype="text/xml")

    @bp.route("/epg")
    @authorise
    def epg_viewer():
        return render_template("epg.html")

    @bp.route("/api/epg")
    @authorise
    def api_epg():
        cached_xmltv = get_cached_xmltv()

        if cached_xmltv is None:
            return jsonify({"channels": [], "programmes": []})

        try:
            root = ET.fromstring(cached_xmltv)

            portals = getPortals()
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT portal, name, custom_name, auto_name, custom_epg_id
                FROM channels WHERE enabled = 1
                """
            )
            channel_portal_map = {}
            for row in cursor.fetchall():
                channel_name = effective_channel_name(
                    row["custom_name"], row["auto_name"], row["name"]
                )
                epg_id = row["custom_epg_id"] if row["custom_epg_id"] else channel_name
                portal_id = row["portal"]
                portal_name = portals.get(portal_id, {}).get("name", portal_id)
                channel_portal_map[epg_id] = portal_name
            conn.close()

            channels = []
            for channel in root.findall("channel"):
                channel_id = channel.get("id")
                display_name = channel.find("display-name")
                icon = channel.find("icon")
                channels.append(
                    {
                        "id": channel_id,
                        "name": display_name.text if display_name is not None else channel_id,
                        "logo": icon.get("src") if icon is not None else None,
                        "portal": channel_portal_map.get(channel_id, ""),
                    }
                )

            programmes = []
            now = datetime.now(timezone.utc)

            def parse_xmltv_time(time_str):
                if not time_str:
                    return None
                try:
                    parts = time_str.split(" ")
                    dt_str = parts[0]
                    dt = datetime.strptime(dt_str, "%Y%m%d%H%M%S")
                    if len(parts) > 1:
                        tz_str = parts[1]
                        tz_sign = 1 if tz_str[0] == "+" else -1
                        tz_hours = int(tz_str[1:3])
                        tz_mins = int(tz_str[3:5]) if len(tz_str) >= 5 else 0
                        tz_offset = timedelta(
                            hours=tz_sign * tz_hours, minutes=tz_sign * tz_mins
                        )
                        dt = dt.replace(tzinfo=timezone(tz_offset))
                        dt = dt.astimezone(timezone.utc)
                    else:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                except (ValueError, AttributeError, IndexError) as e:
                    logger.debug(f"Error parsing XMLTV time '{time_str}': {e}")
                    return None

            for programme in root.findall("programme"):
                channel_id = programme.get("channel")
                start_str = programme.get("start")
                stop_str = programme.get("stop")

                start_time = parse_xmltv_time(start_str)
                stop_time = parse_xmltv_time(stop_str)

                if not start_time or not stop_time:
                    continue

                title_elem = programme.find("title")
                desc_elem = programme.find("desc")

                programmes.append(
                    {
                        "channel": channel_id,
                        "start": start_time.isoformat(),
                        "stop": stop_time.isoformat(),
                        "start_timestamp": start_time.timestamp(),
                        "stop_timestamp": stop_time.timestamp(),
                        "title": title_elem.text if title_elem is not None else "Unknown",
                        "description": desc_elem.text if desc_elem is not None else "",
                        "is_current": start_time <= now <= stop_time,
                        "is_past": stop_time < now,
                    }
                )

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

            refresh_thread = threading.Thread(target=refresh_xmltv, daemon=True)
            refresh_thread.start()
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
