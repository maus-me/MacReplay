import json
import threading
import xml.etree.ElementTree as ET

import flask
from flask import Blueprint, jsonify, redirect, render_template, request, flash

from ..security import authorise


def create_editor_blueprint(
    *,
    logger,
    get_db_connection,
    ACTIVE_GROUP_CONDITION,
    get_cached_xmltv,
    host,
    refresh_xmltv,
    refresh_lineup,
    refresh_channels_cache,
    set_last_playlist_host,
):
    bp = Blueprint("editor", __name__)

    @bp.route("/api/editor", methods=["GET"])
    @authorise
    def editor():
        """Legacy template route"""
        return render_template("editor.html")

    @bp.route("/api/editor_data", methods=["GET"])
    @bp.route("/editor_data", methods=["GET"])
    @authorise
    def editor_data():
        try:
            draw = request.args.get("draw", type=int, default=1)
            start = request.args.get("start", type=int, default=0)
            length = request.args.get("length", type=int, default=250)
            search_value = request.args.get("search[value]", default="")

            portal_filter = request.args.get("portal", default="")
            group_filter = request.args.get("group", default="")
            duplicate_filter = request.args.get("duplicates", default="")
            resolution_include = request.args.get("resolution_include", default="")
            resolution_exclude = request.args.get("resolution_exclude", default="")
            codec_filter = request.args.get("codec", default="")
            country_filter = request.args.get("country", default="")
            event_tags_filter = request.args.get("event_tags", default="")
            misc_include = request.args.get("misc_include", default="")
            misc_exclude = request.args.get("misc_exclude", default="")
            raw_filter = request.args.get("raw", default="")
            event_filter = request.args.get("event", default="")
            header_filter = request.args.get("header", default="")
            match_filter = request.args.get("match", default="")

            column_map = {
                0: "enabled",
                1: "channel_id",
                2: "number",
                3: "name",
                4: "genre",
                5: "portal_name",
            }

            conn = get_db_connection()
            cursor = conn.cursor()

            base_query = f"""FROM channels c
                LEFT JOIN groups g ON c.portal = g.portal AND c.genre_id = g.genre_id
                WHERE {ACTIVE_GROUP_CONDITION}"""
            params = []

            if portal_filter:
                portal_values = [p.strip() for p in portal_filter.split(",") if p.strip()]
                if portal_values:
                    placeholders = ",".join(["?"] * len(portal_values))
                    base_query += f" AND c.portal_name IN ({placeholders})"
                    params.extend(portal_values)

            if group_filter:
                genre_values = [g.strip() for g in group_filter.split(",") if g.strip()]
                if genre_values:
                    include_ungrouped = "Ungrouped" in genre_values
                    genre_values = [g for g in genre_values if g != "Ungrouped"]

                    clauses = []
                    if genre_values:
                        placeholders = ",".join(["?"] * len(genre_values))
                        clauses.append(
                            f"COALESCE(NULLIF(c.custom_genre, ''), c.genre) IN ({placeholders})"
                        )
                        params.extend(genre_values)

                    if include_ungrouped:
                        clauses.append(
                            "(c.genre_id IS NULL OR c.genre_id = '' OR COALESCE(NULLIF(c.custom_genre, ''), c.genre) IS NULL OR COALESCE(NULLIF(c.custom_genre, ''), c.genre) = '')"
                        )

                    if clauses:
                        base_query += " AND (" + " OR ".join(clauses) + ")"

            if duplicate_filter == "enabled_only":
                base_query += """ AND c.enabled = 1 AND COALESCE(NULLIF(c.custom_name, ''), NULLIF(c.auto_name, ''), c.name) IN (
                    SELECT COALESCE(NULLIF(custom_name, ''), NULLIF(auto_name, ''), name)
                    FROM channels
                    WHERE enabled = 1
                    GROUP BY COALESCE(NULLIF(custom_name, ''), NULLIF(auto_name, ''), name)
                    HAVING COUNT(*) > 1
                )"""
            elif duplicate_filter == "unique_only":
                base_query += """ AND COALESCE(NULLIF(c.custom_name, ''), NULLIF(c.auto_name, ''), c.name) IN (
                    SELECT COALESCE(NULLIF(custom_name, ''), NULLIF(auto_name, ''), name)
                    FROM channels
                    WHERE enabled = 1
                    GROUP BY COALESCE(NULLIF(custom_name, ''), NULLIF(auto_name, ''), name)
                    HAVING COUNT(*) = 1
                )"""

            if resolution_include:
                values = [v.strip() for v in resolution_include.split(",") if v.strip()]
                if values:
                    placeholders = ",".join(["?"] * len(values))
                    base_query += f" AND c.resolution IN ({placeholders})"
                    params.extend(values)

            if resolution_exclude:
                values = [v.strip() for v in resolution_exclude.split(",") if v.strip()]
                if values:
                    placeholders = ",".join(["?"] * len(values))
                    base_query += f" AND (c.resolution IS NULL OR c.resolution = '' OR c.resolution NOT IN ({placeholders}))"
                    params.extend(values)

            if codec_filter in ("true", "include"):
                base_query += " AND c.video_codec = 'HEVC'"
            elif codec_filter == "exclude":
                base_query += " AND (c.video_codec IS NULL OR c.video_codec = '' OR c.video_codec != 'HEVC')"

            if country_filter:
                values = [v.strip() for v in country_filter.split(",") if v.strip()]
                if values:
                    placeholders = ",".join(["?"] * len(values))
                    base_query += f" AND c.country IN ({placeholders})"
                    params.extend(values)

            if event_tags_filter:
                values = [v.strip() for v in event_tags_filter.split(",") if v.strip()]
                if values:
                    like_clauses = []
                    for value in values:
                        like_clauses.append("(',' || c.event_tags || ',') LIKE ?")
                        params.append(f"%,{value},%")
                    base_query += " AND (" + " OR ".join(like_clauses) + ")"

            if misc_include:
                values = [v.strip() for v in misc_include.split(",") if v.strip()]
                if values:
                    like_clauses = []
                    for value in values:
                        like_clauses.append("(',' || c.misc_tags || ',') LIKE ?")
                        params.append(f"%,{value},%")
                    base_query += " AND (" + " OR ".join(like_clauses) + ")"

            if misc_exclude:
                values = [v.strip() for v in misc_exclude.split(",") if v.strip()]
                if values:
                    not_like = []
                    for value in values:
                        not_like.append("(',' || c.misc_tags || ',') NOT LIKE ?")
                        params.append(f"%,{value},%")
                    base_query += " AND (" + " AND ".join(not_like) + ")"

            if raw_filter in ("true", "include"):
                base_query += " AND c.is_raw = 1"
            elif raw_filter == "exclude":
                base_query += " AND (c.is_raw = 0 OR c.is_raw IS NULL)"

            if event_filter in ("true", "include"):
                base_query += " AND c.is_event = 1"
            elif event_filter == "exclude":
                base_query += " AND (c.is_event = 0 OR c.is_event IS NULL)"

            if header_filter in ("true", "include"):
                base_query += " AND c.is_header = 1"
            elif header_filter == "exclude":
                base_query += " AND (c.is_header = 0 OR c.is_header IS NULL)"

            if match_filter in ("true", "include"):
                base_query += " AND c.matched_name IS NOT NULL AND c.matched_name != ''"
            elif match_filter == "exclude":
                base_query += " AND (c.matched_name IS NULL OR c.matched_name = '')"

            if search_value:
                base_query += """ AND (
                    c.name LIKE ? OR
                    c.custom_name LIKE ? OR
                    c.auto_name LIKE ? OR
                    c.genre LIKE ? OR
                    c.custom_genre LIKE ? OR
                    c.number LIKE ? OR
                    c.custom_number LIKE ? OR
                    c.portal_name LIKE ? OR
                    c.resolution LIKE ? OR
                    c.video_codec LIKE ? OR
                    c.country LIKE ? OR
                    c.audio_tags LIKE ? OR
                    c.event_tags LIKE ? OR
                    c.misc_tags LIKE ?
                )"""
                search_param = f"%{search_value}%"
                params.extend([search_param] * 14)

            cursor.execute("SELECT COUNT(*) FROM channels")
            records_total = cursor.fetchone()[0]

            count_query = f"SELECT COUNT(*) {base_query}"
            cursor.execute(count_query, params)
            records_filtered = cursor.fetchone()[0]

            order_clauses = []
            i = 0
            while True:
                col_idx_key = f"order[{i}][column]"
                dir_key = f"order[{i}][dir]"
                if col_idx_key not in request.args:
                    break
                col_idx = request.args.get(col_idx_key, type=int)
                direction = request.args.get(dir_key, default="asc")
                col_name = column_map.get(col_idx, "name")

                if col_name == "name":
                    order_clauses.append(
                        f"COALESCE(NULLIF(c.custom_name, ''), NULLIF(c.auto_name, ''), c.name) {direction}"
                    )
                elif col_name == "genre":
                    order_clauses.append(
                        f"COALESCE(NULLIF(c.custom_genre, ''), c.genre) {direction}"
                    )
                elif col_name == "number":
                    order_clauses.append(
                        f"CAST(COALESCE(NULLIF(c.custom_number, ''), c.number) AS INTEGER) {direction}"
                    )
                elif col_name == "epg_id":
                    order_clauses.append(
                        f"COALESCE(NULLIF(c.custom_epg_id, ''), c.portal || c.channel_id) {direction}"
                    )
                else:
                    order_clauses.append(f"c.{col_name} {direction}")
                i += 1

            if not order_clauses:
                order_clauses.append(
                    "COALESCE(NULLIF(c.custom_name, ''), NULLIF(c.auto_name, ''), c.name) ASC"
                )

            order_clause = "ORDER BY " + ", ".join(order_clauses)

            data_query = f"""
                SELECT
                    c.portal, c.channel_id, c.portal_name, c.name, c.number, c.genre, c.genre_id, c.logo,
                    c.enabled, c.custom_name, c.auto_name, c.custom_number, c.custom_genre,
                    c.custom_epg_id, c.available_macs, c.alternate_ids,
                    c.resolution, c.video_codec, c.country, c.audio_tags, c.event_tags, c.misc_tags,
                    c.matched_name, c.matched_source, c.matched_station_id, c.matched_call_sign, c.matched_logo, c.matched_score,
                    c.is_raw, c.is_event, c.is_header
                {base_query}
                {order_clause}
                LIMIT ? OFFSET ?
            """

            params.extend([length, start])
            cursor.execute(data_query, params)
            channel_rows = cursor.fetchall()

            duplicate_counts_query = """
                SELECT
                    COALESCE(NULLIF(custom_name, ''), NULLIF(auto_name, ''), name) as channel_name,
                    COUNT(*) as count
                FROM channels
                WHERE enabled = 1
                GROUP BY COALESCE(NULLIF(custom_name, ''), NULLIF(auto_name, ''), name)
                HAVING COUNT(*) > 1
            """
            cursor.execute(duplicate_counts_query)
            duplicate_counts = {
                row["channel_name"]: row["count"] for row in cursor.fetchall()
            }

            epg_channels = set()
            cached_xmltv = get_cached_xmltv()
            if cached_xmltv:
                try:
                    root = ET.fromstring(cached_xmltv)
                    for programme in root.findall("programme"):
                        epg_channels.add(programme.get("channel"))
                except Exception as e:
                    logger.debug(f"Could not parse EPG for editor: {e}")

            channels = []
            for row in channel_rows:
                portal = row["portal"]
                channel_id = row["channel_id"]
                effective_name = row["custom_name"] or row["auto_name"] or row["name"]
                duplicate_count = duplicate_counts.get(effective_name, 0)
                epg_id = row["custom_epg_id"] or effective_name
                has_epg = epg_id in epg_channels

                channels.append(
                    {
                        "portal": portal,
                        "portalName": row["portal_name"] or "",
                        "enabled": bool(row["enabled"]),
                        "channelNumber": row["number"] or "",
                        "customChannelNumber": row["custom_number"] or "",
                        "channelName": row["name"] or "",
                        "customChannelName": row["custom_name"] or "",
                        "autoChannelName": row["auto_name"] or "",
                        "genre": row["genre"] or "",
                        "genreId": row["genre_id"] or "",
                        "customGenre": row["custom_genre"] or "",
                        "channelId": channel_id,
                        "customEpgId": row["custom_epg_id"] or "",
                        "link": f"http://{host}/play/{portal}/{channel_id}?web=true",
                        "logo": row["logo"] or "",
                        "availableMacs": row["available_macs"] or "",
                        "alternateIds": row["alternate_ids"] or "",
                        "resolution": row["resolution"] or "",
                        "videoCodec": row["video_codec"] or "",
                        "country": row["country"] or "",
                        "audioTags": row["audio_tags"] or "",
                        "eventTags": row["event_tags"] or "",
                        "miscTags": row["misc_tags"] or "",
                        "matchedName": row["matched_name"] or "",
                        "matchedSource": row["matched_source"] or "",
                        "matchedStationId": row["matched_station_id"] or "",
                        "matchedCallSign": row["matched_call_sign"] or "",
                        "matchedLogo": row["matched_logo"] or "",
                        "matchedScore": row["matched_score"] or "",
                        "isRaw": bool(row["is_raw"]),
                        "isEvent": bool(row["is_event"]),
                        "isHeader": bool(row["is_header"]),
                        "duplicateCount": duplicate_count if row["enabled"] else 0,
                        "hasEpg": has_epg,
                    }
                )

            conn.close()

            return flask.jsonify(
                {
                    "draw": draw,
                    "recordsTotal": records_total,
                    "recordsFiltered": records_filtered,
                    "data": channels,
                }
            )

        except Exception as e:
            logger.error(f"Error in editor_data: {e}")
            return flask.jsonify(
                {
                    "draw": draw if "draw" in locals() else 1,
                    "recordsTotal": 0,
                    "recordsFiltered": 0,
                    "data": [],
                    "error": str(e),
                }
            ), 500

    @bp.route("/api/editor/portals", methods=["GET"])
    @bp.route("/editor/portals", methods=["GET"])
    @authorise
    def editor_portals():
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT DISTINCT portal_name
                FROM channels
                WHERE portal_name IS NOT NULL AND portal_name != ''
                ORDER BY portal_name
                """
            )

            portals = [row["portal_name"] for row in cursor.fetchall()]
            conn.close()

            return flask.jsonify({"portals": portals})
        except Exception as e:
            logger.error(f"Error in editor_portals: {e}")
            return flask.jsonify({"portals": [], "error": str(e)}), 500

    @bp.route("/api/editor/genres", methods=["GET"])
    @bp.route("/editor/genres", methods=["GET"])
    @authorise
    def editor_genres():
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            portal = flask.request.args.get("portal", "").strip()

            if portal:
                cursor.execute(
                    """
                    SELECT DISTINCT COALESCE(NULLIF(custom_genre, ''), genre) as genre
                    FROM channels
                    WHERE COALESCE(NULLIF(custom_genre, ''), genre) IS NOT NULL
                        AND COALESCE(NULLIF(custom_genre, ''), genre) != ''
                        AND COALESCE(NULLIF(custom_genre, ''), genre) != 'None'
                        AND portal = ?
                    ORDER BY genre
                    """,
                    (portal,),
                )
            else:
                cursor.execute(
                    """
                    SELECT DISTINCT COALESCE(NULLIF(custom_genre, ''), genre) as genre
                    FROM channels
                    WHERE COALESCE(NULLIF(custom_genre, ''), genre) IS NOT NULL
                        AND COALESCE(NULLIF(custom_genre, ''), genre) != ''
                        AND COALESCE(NULLIF(custom_genre, ''), genre) != 'None'
                    ORDER BY genre
                    """
                )

            genres = [row["genre"] for row in cursor.fetchall()]
            conn.close()

            return flask.jsonify({"genres": genres})
        except Exception as e:
            logger.error(f"Error in editor_genres: {e}")
            return flask.jsonify({"genres": [], "error": str(e)}), 500

    @bp.route("/api/editor/tag-values", methods=["GET"])
    @authorise
    def editor_tag_values():
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute(
                "SELECT DISTINCT resolution FROM channels WHERE resolution IS NOT NULL AND resolution != ''"
            )
            resolutions = sorted(
                {row["resolution"] for row in cursor.fetchall() if row["resolution"]}
            )

            cursor.execute(
                "SELECT DISTINCT video_codec FROM channels WHERE video_codec IS NOT NULL AND video_codec != ''"
            )
            video_codecs = sorted(
                {row["video_codec"] for row in cursor.fetchall() if row["video_codec"]}
            )

            cursor.execute(
                "SELECT DISTINCT country FROM channels WHERE country IS NOT NULL AND country != ''"
            )
            countries = sorted(
                {row["country"] for row in cursor.fetchall() if row["country"]}
            )

            cursor.execute(
                "SELECT DISTINCT event_tags FROM channels WHERE event_tags IS NOT NULL AND event_tags != ''"
            )
            event_values = set()
            for row in cursor.fetchall():
                for tag in (row["event_tags"] or "").split(","):
                    tag = tag.strip()
                    if tag:
                        event_values.add(tag)

            cursor.execute(
                "SELECT DISTINCT misc_tags FROM channels WHERE misc_tags IS NOT NULL AND misc_tags != ''"
            )
            misc_values = set()
            for row in cursor.fetchall():
                for tag in (row["misc_tags"] or "").split(","):
                    tag = tag.strip()
                    if tag:
                        misc_values.add(tag)

            conn.close()

            return flask.jsonify(
                {
                    "resolutions": resolutions,
                    "video_codecs": video_codecs,
                    "countries": countries,
                    "event_tags": sorted(event_values),
                    "misc_tags": sorted(misc_values),
                }
            )
        except Exception as e:
            logger.error(f"Error in editor_tag_values: {e}")
            return flask.jsonify(
                {
                    "resolutions": [],
                    "video_codecs": [],
                    "countries": [],
                    "event_tags": [],
                    "misc_tags": [],
                    "error": str(e),
                }
            ), 500

    @bp.route("/api/editor/genres-grouped", methods=["GET"])
    @bp.route("/editor/genres-grouped", methods=["GET"])
    @authorise
    def editor_genres_grouped():
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute(
                "SELECT DISTINCT portal_name FROM channels WHERE portal_name IS NOT NULL AND portal_name != '' ORDER BY portal_name"
            )
            portal_names = [row["portal_name"] for row in cursor.fetchall()]

            genres_by_portal = []
            for portal_name in portal_names:
                cursor.execute(
                    """
                    SELECT DISTINCT COALESCE(NULLIF(custom_genre, ''), genre) as genre
                    FROM channels
                    WHERE portal_name = ?
                        AND COALESCE(NULLIF(custom_genre, ''), genre) IS NOT NULL
                        AND COALESCE(NULLIF(custom_genre, ''), genre) != ''
                        AND COALESCE(NULLIF(custom_genre, ''), genre) != 'None'
                    ORDER BY genre
                    """,
                    (portal_name,),
                )

                genres = [row["genre"] for row in cursor.fetchall()]
                if "Ungrouped" not in genres:
                    genres.insert(0, "Ungrouped")
                if genres:
                    genres_by_portal.append({"portal": portal_name, "genres": genres})

            conn.close()

            return flask.jsonify({"genres_by_portal": genres_by_portal})
        except Exception as e:
            logger.error(f"Error in editor_genres_grouped: {e}")
            return flask.jsonify({"genres_by_portal": [], "error": str(e)}), 500

    @bp.route("/api/editor/duplicate-counts", methods=["GET"])
    @bp.route("/editor/duplicate-counts", methods=["GET"])
    @authorise
    def editor_duplicate_counts():
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT 
                    COALESCE(NULLIF(custom_name, ''), NULLIF(auto_name, ''), name) as channel_name,
                    COUNT(*) as count
                FROM channels
                WHERE enabled = 1
                GROUP BY COALESCE(NULLIF(custom_name, ''), NULLIF(auto_name, ''), name)
                ORDER BY count DESC, channel_name
                """
            )

            counts = [
                {"channel_name": row["channel_name"], "count": row["count"]}
                for row in cursor.fetchall()
            ]
            conn.close()

            return flask.jsonify({"counts": counts})
        except Exception as e:
            logger.error(f"Error in editor_duplicate_counts: {e}")
            return flask.jsonify({"counts": [], "error": str(e)}), 500

    @bp.route("/api/editor/save", methods=["POST"])
    @bp.route("/editor/save", methods=["POST"])
    @authorise
    def editorSave():
        threading.Thread(target=refresh_xmltv, daemon=True).start()
        set_last_playlist_host(None)
        threading.Thread(target=refresh_lineup).start()

        enabledEdits = json.loads(request.form["enabledEdits"])
        numberEdits = json.loads(request.form["numberEdits"])
        nameEdits = json.loads(request.form["nameEdits"])
        groupEdits = json.loads(request.form["groupEdits"])
        epgEdits = json.loads(request.form["epgEdits"])

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            for edit in enabledEdits:
                portal = edit["portal"]
                channel_id = edit["channel id"]
                enabled = 1 if edit["enabled"] else 0

                cursor.execute(
                    """
                    UPDATE channels 
                    SET enabled = ? 
                    WHERE portal = ? AND channel_id = ?
                    """,
                    (enabled, portal, channel_id),
                )

            for edit in numberEdits:
                portal = edit["portal"]
                channel_id = edit["channel id"]
                custom_number = edit["custom number"]

                cursor.execute(
                    """
                    UPDATE channels 
                    SET custom_number = ? 
                    WHERE portal = ? AND channel_id = ?
                    """,
                    (custom_number, portal, channel_id),
                )

            for edit in nameEdits:
                portal = edit["portal"]
                channel_id = edit["channel id"]
                custom_name = edit["custom name"]

                cursor.execute(
                    """
                    UPDATE channels 
                    SET custom_name = ? 
                    WHERE portal = ? AND channel_id = ?
                    """,
                    (custom_name, portal, channel_id),
                )

            for edit in groupEdits:
                portal = edit["portal"]
                channel_id = edit["channel id"]
                custom_genre = edit["custom genre"]

                cursor.execute(
                    """
                    UPDATE channels 
                    SET custom_genre = ? 
                    WHERE portal = ? AND channel_id = ?
                    """,
                    (custom_genre, portal, channel_id),
                )

            for edit in epgEdits:
                portal = edit["portal"]
                channel_id = edit["channel id"]
                custom_epg_id = edit["custom epg id"]

                cursor.execute(
                    """
                    UPDATE channels 
                    SET custom_epg_id = ? 
                    WHERE portal = ? AND channel_id = ?
                    """,
                    (custom_epg_id, portal, channel_id),
                )

            conn.commit()
            logger.info("Channel edits saved to database!")

        except Exception as e:
            conn.rollback()
            logger.error(f"Error saving channel edits: {e}")
            return jsonify({"success": False, "error": str(e)}), 500
        finally:
            conn.close()

        return jsonify({"success": True, "message": "Playlist config saved!"})

    @bp.route("/api/editor/merge", methods=["POST"])
    @authorise
    def editor_merge_channels():
        try:
            data = request.get_json()
            primary_portal = data.get("primaryPortal")
            primary_channel_id = data.get("primaryChannelId")
            secondary_portal = data.get("secondaryPortal")
            secondary_channel_id = data.get("secondaryChannelId")

            if not all(
                [primary_portal, primary_channel_id, secondary_portal, secondary_channel_id]
            ):
                return jsonify({"success": False, "error": "Missing required fields"}), 400

            if primary_portal != secondary_portal:
                return (
                    jsonify({"success": False, "error": "Channels must be from the same portal"}),
                    400,
                )

            if primary_channel_id == secondary_channel_id:
                return (
                    jsonify({"success": False, "error": "Cannot merge a channel with itself"}),
                    400,
                )

            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute(
                "SELECT alternate_ids, available_macs FROM channels WHERE portal = ? AND channel_id = ?",
                [primary_portal, primary_channel_id],
            )
            primary_row = cursor.fetchone()
            if not primary_row:
                conn.close()
                return jsonify({"success": False, "error": "Primary channel not found"}), 404

            cursor.execute(
                "SELECT available_macs FROM channels WHERE portal = ? AND channel_id = ?",
                [secondary_portal, secondary_channel_id],
            )
            secondary_row = cursor.fetchone()
            if not secondary_row:
                conn.close()
                return (
                    jsonify({"success": False, "error": "Secondary channel not found"}),
                    404,
                )

            current_alternates = []
            if primary_row[0]:
                current_alternates = [
                    aid.strip() for aid in primary_row[0].split(",") if aid.strip()
                ]

            if secondary_channel_id not in current_alternates:
                current_alternates.append(secondary_channel_id)

            new_alternate_ids = ",".join(current_alternates)

            primary_macs = set()
            if primary_row[1]:
                primary_macs = set(m.strip() for m in primary_row[1].split(",") if m.strip())
            if secondary_row[0]:
                secondary_macs = set(
                    m.strip() for m in secondary_row[0].split(",") if m.strip()
                )
                primary_macs.update(secondary_macs)

            new_available_macs = ",".join(sorted(primary_macs))

            cursor.execute(
                "UPDATE channels SET alternate_ids = ?, available_macs = ? WHERE portal = ? AND channel_id = ?",
                [new_alternate_ids, new_available_macs, primary_portal, primary_channel_id],
            )

            cursor.execute(
                "DELETE FROM channels WHERE portal = ? AND channel_id = ?",
                [secondary_portal, secondary_channel_id],
            )

            conn.commit()
            conn.close()

            logger.info(
                "Merged channel %s into %s for portal %s",
                secondary_channel_id,
                primary_channel_id,
                primary_portal,
            )

            return jsonify(
                {
                    "success": True,
                    "message": f"Channel merged successfully. {secondary_channel_id} is now an alternate for {primary_channel_id}",
                    "alternateIds": new_alternate_ids,
                }
            )

        except Exception as e:
            logger.error(f"Error merging channels: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @bp.route("/api/editor/search-for-merge", methods=["POST"])
    @authorise
    def editor_search_for_merge():
        try:
            data = request.get_json()
            portal = data.get("portal")
            exclude_channel_id = data.get("excludeChannelId")
            query = data.get("query", "").strip()

            if not portal or not query or len(query) < 2:
                return jsonify({"success": True, "channels": []})

            conn = get_db_connection()
            cursor = conn.cursor()

            search_pattern = f"%{query}%"
            cursor.execute(
                """
                SELECT channel_id, name, custom_name, auto_name, genre
                FROM channels
                WHERE portal = ?
                  AND channel_id != ?
                  AND (name LIKE ? OR custom_name LIKE ? OR auto_name LIKE ? OR channel_id LIKE ?)
                LIMIT 10
                """,
                [
                    portal,
                    exclude_channel_id,
                    search_pattern,
                    search_pattern,
                    search_pattern,
                    search_pattern,
                ],
            )

            channels = []
            for row in cursor.fetchall():
                effective_name = row["custom_name"] or row["auto_name"] or row["name"]
                channels.append(
                    {
                        "channelId": row["channel_id"],
                        "name": effective_name,
                        "customName": row["custom_name"] or "",
                        "genre": row["genre"] or "",
                    }
                )

            conn.close()
            return jsonify({"success": True, "channels": channels})

        except Exception as e:
            logger.error(f"Error searching channels for merge: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @bp.route("/api/editor/reset", methods=["POST"])
    @bp.route("/editor/reset", methods=["POST"])
    @authorise
    def editorReset():
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                UPDATE channels 
                SET enabled = 0,
                    custom_name = '',
                    custom_number = '',
                    custom_genre = '',
                    custom_epg_id = ''
                """
            )

            conn.commit()
            logger.info("All channel customizations reset!")
            flash("Playlist reset!", "success")

        except Exception as e:
            conn.rollback()
            logger.error(f"Error resetting channels: {e}")
            flash(f"Error resetting: {e}", "danger")
        finally:
            conn.close()

        return redirect("/editor", code=302)

    @bp.route("/api/editor/refresh", methods=["POST"])
    @bp.route("/editor/refresh", methods=["POST"])
    @authorise
    def editorRefresh():
        try:
            total = refresh_channels_cache()
            logger.info(f"Channel cache refreshed: {total} channels")
            return flask.jsonify({"status": "success", "total": total})
        except Exception as e:
            logger.error(f"Error refreshing channel cache: {e}")
            return flask.jsonify({"status": "error", "message": str(e)}), 500

    return bp
