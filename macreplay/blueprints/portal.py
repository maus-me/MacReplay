import sqlite3
import uuid
from datetime import datetime

from flask import Blueprint, jsonify, redirect, render_template, request, flash

from ..security import authorise
import stb


def create_portal_blueprint(
    *,
    logger,
    getPortals,
    savePortals,
    getSettings,
    get_db_connection,
    ACTIVE_GROUP_CONDITION,
    channelsdvr_match_status,
    channelsdvr_match_status_lock,
    normalize_mac_data,
    job_manager,
    defaultPortal,
    DB_PATH,
    set_cached_xmltv,
    filter_cache,
):
    bp = Blueprint("portal", __name__)

    @bp.route("/api/portals", methods=["GET"])
    @authorise
    def portals():
        """Legacy template route"""
        portal_data = getPortals()

        portal_stats = {}
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT portal_id, total_channels, active_channels, total_groups, active_groups
                FROM portal_stats
                """
            )
            for row in cursor.fetchall():
                portal_stats[row["portal_id"]] = {
                    "channels": row["active_channels"] or 0,
                    "total_channels": row["total_channels"] or 0,
                    "groups": row["active_groups"] or 0,
                    "total_groups": row["total_groups"] or 0,
                }
            conn.close()
        except Exception as e:
            logger.error(f"Error getting portal stats: {e}")

        for portal_id, _portal in portal_data.items():
            if portal_id not in portal_stats:
                portal_stats[portal_id] = {
                    "channels": 0,
                    "total_channels": 0,
                    "groups": 0,
                    "total_groups": 0,
                }

        return render_template(
            "portals.html",
            portals=portal_data,
            portal_stats=portal_stats,
            settings=getSettings(),
        )

    @bp.route("/api/portal/genres", methods=["POST"])
    @authorise
    def update_portal_genres():
        data = request.get_json(silent=True) or {}
        portal_id = data.get("portal_id")
        selected_genres = data.get("selected_genres") or []

        if not portal_id:
            return jsonify({"success": False, "message": "Portal ID required"}), 400

        portals = getPortals()
        if portal_id not in portals:
            return jsonify({"success": False, "message": "Portal not found"}), 404

        selected_genres = [str(g) for g in selected_genres if g is not None]

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute("UPDATE groups SET active = 0 WHERE portal_id = ?", (portal_id,))
            if selected_genres:
                placeholders = ",".join(["?"] * len(selected_genres))
                cursor.execute(
                    f"UPDATE groups SET active = 1 WHERE portal_id = ? AND genre_id IN ({placeholders})",
                    [portal_id, *selected_genres],
                )

            cursor.execute(
                """
                SELECT COUNT(*) as total_groups,
                       SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) as active_groups
                FROM groups
                WHERE portal_id = ?
                """,
                (portal_id,),
            )
            row = cursor.fetchone()
            total_groups = row[0] or 0
            active_groups = row[1] or 0

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM channels c
                LEFT JOIN groups g ON c.portal_id = g.portal_id AND c.genre_id = g.genre_id
                WHERE c.portal_id = ? AND {ACTIVE_GROUP_CONDITION}
                """.format(ACTIVE_GROUP_CONDITION=ACTIVE_GROUP_CONDITION),
                (portal_id,),
            )
            active_channels = cursor.fetchone()[0] or 0

            cursor.execute(
                "SELECT COUNT(*) FROM channels WHERE portal_id = ?",
                (portal_id,),
            )
            total_channels = cursor.fetchone()[0] or 0

            portal = portals[portal_id]
            portal_name = portal.get("name", portal_id)
            cursor.execute(
                """
                INSERT INTO portal_stats (portal_id, portal_name, total_channels, active_channels, total_groups, active_groups, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(portal_id) DO UPDATE SET
                    portal_name = excluded.portal_name,
                    total_channels = excluded.total_channels,
                    active_channels = excluded.active_channels,
                    total_groups = excluded.total_groups,
                    active_groups = excluded.active_groups,
                    updated_at = excluded.updated_at
                """,
                (
                    portal_id,
                    portal_name,
                    total_channels,
                    active_channels,
                    total_groups,
                    active_groups,
                    datetime.utcnow().isoformat(),
                ),
            )

            conn.commit()
            conn.close()

            portal["selected_genres"] = selected_genres
            portal["total_groups"] = total_groups
            portal["total_channels"] = total_channels
            portals[portal_id] = portal
            savePortals(portals)
            filter_cache.clear()

            logger.info(
                "Updated genres for %s: %s/%s groups active, %s/%s channels",
                portal_name,
                active_groups,
                total_groups,
                active_channels,
                total_channels,
            )

            set_cached_xmltv(None)

            match_started = False
            if (
                getSettings().get("channelsdvr enabled", "false") == "true"
                and portals.get(portal_id, {}).get("auto match", "false") == "true"
            ):
                match_started = True

            refresh_status = job_manager.enqueue_refresh_portal(
                portal_id, reason="groups_update"
            )

            return jsonify(
                {
                    "success": True,
                    "message": "Genres updated successfully",
                    "total_groups": total_groups,
                    "active_groups": active_groups,
                    "total_channels": total_channels,
                    "active_channels": active_channels,
                    "match_started": match_started,
                    "refresh_status": refresh_status,
                }
            )

        except Exception as e:
            logger.error(f"Error updating genres: {e}")
            return jsonify({"success": False, "message": str(e)}), 500

    @bp.route("/api/portal/match/status", methods=["POST"])
    @authorise
    def portal_match_status():
        data = request.get_json(silent=True) or {}
        portal_id = data.get("portal_id")
        if not portal_id:
            return jsonify({"success": False, "message": "Portal ID required"})
        with channelsdvr_match_status_lock:
            status = channelsdvr_match_status.get(portal_id)
        if not status:
            return jsonify({"success": True, "status": "idle"})
        return jsonify({"success": True, **status})

    @bp.route("/portal/add", methods=["POST"])
    @authorise
    def portalsAdd():
        set_cached_xmltv(None)
        portal_id = uuid.uuid4().hex
        enabled = "true"
        name = request.form["name"]
        url = request.form["url"]
        macs = list(set(request.form["macs"].split(",")))
        streamsPerMac = request.form["streams per mac"]
        epgOffset = request.form["epg offset"]
        proxy = request.form["proxy"]
        fetchEpg = "true" if request.form.get("fetch epg") else "false"
        autoNormalize = "true" if request.form.get("auto normalize names") else "false"
        autoMatch = "true" if request.form.get("auto match") else "false"
        selectedGenres = request.form.getlist("selected_genres")

        if not url.endswith(".php"):
            url = stb.getUrl(url, proxy)
            if not url:
                logger.error("Error getting URL for Portal(%s)", name)
                flash(f"Error getting URL for Portal({name})", "danger")
                return redirect("/portals", code=302)

        macsd = {}
        tested_total = 0
        tested_success = 0
        tested_failed = 0

        for mac in macs:
            tested_total += 1
            logger.info("Testing MAC(%s) for Portal(%s)...", mac, name)
            token = stb.getToken(url, mac, proxy)
            if token:
                logger.debug("Got token for MAC(%s), getting profile and expiry...", mac)
                profile = stb.getProfile(url, mac, token, proxy)
                expiry = stb.getExpires(url, mac, token, proxy)
                if expiry:
                    macsd[mac] = {
                        "expiry": expiry,
                        "watchdog_timeout": profile.get("watchdog_timeout", 0)
                        if profile
                        else 0,
                        "playback_limit": profile.get("playback_limit", 0)
                        if profile
                        else 0,
                    }
                    logger.info("Successfully tested MAC(%s) for Portal(%s)", mac, name)
                    tested_success += 1
                    continue
                logger.error("Failed to get expiry for MAC(%s) for Portal(%s)", mac, name)
            else:
                logger.error("Failed to get token for MAC(%s) for Portal(%s)", mac, name)

            logger.error("Error testing MAC(%s) for Portal(%s)", mac, name)
            tested_failed += 1

        if tested_total > 0:
            if tested_success > 0 and tested_failed == 0:
                flash(
                    f"{tested_success}/{tested_total} MACs successfully added for Portal({name})",
                    "success",
                )
            elif tested_success > 0:
                flash(
                    f"{tested_success}/{tested_total} MACs successfully tested for Portal({name}). {tested_failed} failed.",
                    "warning",
                )
            else:
                flash(
                    f"0/{tested_total} MACs successfully tested for Portal({name}).",
                    "danger",
                )

        if len(macsd) > 0:
            portal = {
                "enabled": enabled,
                "name": name,
                "url": url,
                "macs": macsd,
                "streams per mac": streamsPerMac,
                "epg offset": epgOffset,
                "proxy": proxy,
                "fetch epg": fetchEpg,
                "selected_genres": selectedGenres,
                "auto normalize names": autoNormalize,
                "auto match": autoMatch,
            }

            for setting, default in defaultPortal.items():
                if not portal.get(setting):
                    portal[setting] = default

            portals = getPortals()
            portals[portal_id] = portal
            savePortals(portals)
            filter_cache.clear()
            logger.info("Portal(%s) added!", portal["name"])
            flash(f"Portal({portal['name']}) added!", "success")

            job_manager.enqueue_refresh_all(reason="portal_add")
            flash("Channels are being loaded in the background.", "info")

        else:
            logger.error(
                "None of the MACs tested OK for Portal(%s). Adding not successful",
                name,
            )

        return redirect("/portals", code=302)

    @bp.route("/portal/update", methods=["POST"])
    @authorise
    def portalUpdate():
        set_cached_xmltv(None)
        portal_id = request.form["id"]
        enabled = request.form.get("enabled", "false")
        name = request.form["name"]
        url = request.form["url"]
        newmacs = list(set(request.form["macs"].split(",")))
        streamsPerMac = request.form["streams per mac"]
        epgOffset = request.form["epg offset"]
        proxy = request.form["proxy"]
        fetchEpg = "true" if request.form.get("fetch epg") else "false"
        autoNormalize = "true" if request.form.get("auto normalize names") else "false"
        autoMatch = "true" if request.form.get("auto match") else "false"
        retest = request.form.get("retest", None)
        selectedGenres = request.form.getlist("selected_genres")

        if not url.endswith(".php"):
            url = stb.getUrl(url, proxy)
            if not url:
                logger.error("Error getting URL for Portal(%s)", name)
                flash(f"Error getting URL for Portal({name})", "danger")
                return redirect("/portals", code=302)

        portals = getPortals()
        oldmacs = portals[portal_id]["macs"]
        macsout = {}
        deadmacs = []
        tested_total = 0
        tested_success = 0
        tested_failed = 0

        for mac in newmacs:
            if retest or mac not in oldmacs.keys():
                tested_total += 1
                logger.info("Testing MAC(%s) for Portal(%s)...", mac, name)
                token = stb.getToken(url, mac, proxy)
                if token:
                    logger.debug(
                        "Got token for MAC(%s), getting profile and expiry...", mac
                    )
                    profile = stb.getProfile(url, mac, token, proxy)
                    expiry = stb.getExpires(url, mac, token, proxy)
                    if expiry:
                        macsout[mac] = {
                            "expiry": expiry,
                            "watchdog_timeout": profile.get("watchdog_timeout", 0)
                            if profile
                            else 0,
                            "playback_limit": profile.get("playback_limit", 0)
                            if profile
                            else 0,
                        }
                        logger.info(
                            "Successfully tested MAC(%s) for Portal(%s)", mac, name
                        )
                        tested_success += 1
                    else:
                        logger.error(
                            "Failed to get expiry for MAC(%s) for Portal(%s)", mac, name
                        )
                else:
                    logger.error(
                        "Failed to get token for MAC(%s) for Portal(%s)", mac, name
                    )

                if mac not in list(macsout.keys()):
                    deadmacs.append(mac)
                    tested_failed += 1

            if mac in oldmacs.keys() and mac not in deadmacs:
                macsout[mac] = oldmacs[mac]

            if mac not in macsout.keys():
                logger.error("Error testing MAC(%s) for Portal(%s)", mac, name)

        if tested_total > 0:
            if tested_success > 0 and tested_failed == 0:
                flash(
                    f"{tested_success}/{tested_total} MACs successfully tested for Portal({name})",
                    "success",
                )
            elif tested_success > 0:
                flash(
                    f"{tested_success}/{tested_total} MACs successfully tested for Portal({name}). {tested_failed} failed.",
                    "warning",
                )
            else:
                flash(
                    f"0/{tested_total} MACs successfully tested for Portal({name}).",
                    "danger",
                )

        if len(macsout) > 0:
            portals[portal_id]["enabled"] = enabled
            portals[portal_id]["name"] = name
            portals[portal_id]["url"] = url
            portals[portal_id]["macs"] = macsout
            portals[portal_id]["streams per mac"] = streamsPerMac
            portals[portal_id]["epg offset"] = epgOffset
            portals[portal_id]["proxy"] = proxy
            portals[portal_id]["fetch epg"] = fetchEpg
            portals[portal_id]["selected_genres"] = selectedGenres
            portals[portal_id]["auto normalize names"] = autoNormalize
            portals[portal_id]["auto match"] = autoMatch
            savePortals(portals)
            filter_cache.clear()
            logger.info("Portal(%s) updated!", name)
            flash(f"Portal({name}) updated!", "success")

        else:
            logger.error(
                "None of the MACs tested OK for Portal(%s). Adding not successful",
                name,
            )

        return redirect("/portals", code=302)

    @bp.route("/portal/remove", methods=["POST"])
    @authorise
    def portalRemove():
        portal_id = request.form["deleteId"]
        portals = getPortals()

        if portal_id not in portals:
            logger.error(f"Attempted to delete non-existent portal: {portal_id}")
            if request.is_json or request.headers.get("Accept", "").startswith(
                "application/json"
            ):
                return jsonify({"error": "Portal not found"}), 404
            flash("Portal not found", "danger")
            return redirect("/portals", code=302)

        name = portals[portal_id]["name"]
        del portals[portal_id]
        savePortals(portals)
        filter_cache.clear()
        logger.info("Portal (%s) removed!", name)

        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM channels WHERE portal_id = ?", (portal_id,))
            deleted_count = cursor.rowcount
            cursor.execute("DELETE FROM group_stats WHERE portal_id = ?", (portal_id,))
            cursor.execute("DELETE FROM portal_stats WHERE portal_id = ?", (portal_id,))
            conn.commit()
            conn.close()
            logger.info(
                "Removed %s channels for portal %s from database",
                deleted_count,
                name,
            )
        except Exception as e:
            logger.error(f"Error removing channels from database for portal {name}: {e}")

        if request.is_json or request.headers.get("Accept", "").startswith(
            "application/json"
        ):
            return jsonify({"success": True, "message": f"Portal {name} removed"})

        flash(f"Portal ({name}) removed!", "success")
        return redirect("/portals", code=302)

    @bp.route("/api/portal/refresh", methods=["POST"])
    @authorise
    def refreshPortalChannels():
        try:
            data = request.get_json()
            portal_id = data.get("portal_id")

            if not portal_id:
                return jsonify({"status": "error", "message": "Portal ID required"}), 400

            portals = getPortals()
            if portal_id not in portals:
                return (
                    jsonify({"status": "error", "message": "Portal not found"}),
                    404,
                )

            portal_name = portals[portal_id].get("name", portal_id)
            logger.info("Refreshing channels for portal: %s", portal_name)
            refresh_status = job_manager.enqueue_refresh_portal(
                portal_id, reason="manual_refresh"
            )
            status_payload = job_manager.get_portal_refresh_status(portal_id)
            status_payload.update(
                {
                    "status": refresh_status,
                    "portal": portal_name,
                }
            )
            return jsonify(status_payload), 202
        except Exception as e:
            logger.error(f"Error refreshing portal channels: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500

    @bp.route("/api/portal/refresh/status", methods=["POST"])
    @authorise
    def portal_refresh_status():
        data = request.get_json(silent=True) or {}
        portal_id = data.get("portal_id")
        if not portal_id:
            return jsonify({"status": "error", "message": "Portal ID required"}), 400
        status = job_manager.get_portal_refresh_status(portal_id)
        if not status:
            return jsonify({"status": "idle"})
        return jsonify(status)

    return bp
