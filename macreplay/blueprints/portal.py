import sqlite3
import uuid
from threading import Thread
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
    normalize_mac_data,
    refresh_channels_cache,
    run_portal_matching,
    channelsdvr_match_status,
    channelsdvr_match_status_lock,
    defaultPortal,
    DB_PATH,
    set_cached_xmltv,
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
                f"""
                SELECT
                    c.portal,
                    COUNT(*) as total_channels,
                    SUM(CASE WHEN {ACTIVE_GROUP_CONDITION} THEN 1 ELSE 0 END) as active_channels
                FROM channels c
                LEFT JOIN groups g ON c.portal = g.portal AND c.genre_id = g.genre_id
                GROUP BY c.portal
                """
            )
            for row in cursor.fetchall():
                portal_stats[row["portal"]] = {
                    "channels": row["active_channels"] or 0,
                    "total_channels": row["total_channels"] or 0,
                }

            cursor.execute(
                """
                SELECT
                    portal,
                    COUNT(*) as total_groups,
                    SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) as active_groups
                FROM groups
                GROUP BY portal
                """
            )
            for row in cursor.fetchall():
                if row["portal"] in portal_stats:
                    portal_stats[row["portal"]]["groups"] = row["active_groups"] or 0
                    portal_stats[row["portal"]]["total_groups"] = row[
                        "total_groups"
                    ] or 0
                else:
                    portal_stats[row["portal"]] = {
                        "channels": 0,
                        "total_channels": 0,
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

    @bp.route("/api/portal/mac/delete", methods=["POST"])
    @authorise
    def delete_portal_mac():
        try:
            data = request.get_json()
            portal_id = data.get("portal_id")
            mac = data.get("mac")

            if not portal_id or not mac:
                return jsonify({"success": False, "message": "Missing portal_id or mac"})

            portals = getPortals()
            if portal_id not in portals:
                return jsonify({"success": False, "message": "Portal not found"})

            if mac not in portals[portal_id].get("macs", {}):
                return jsonify({"success": False, "message": "MAC not found in portal"})

            del portals[portal_id]["macs"][mac]
            savePortals(portals)

            logger.info(
                "Deleted MAC(%s) from Portal(%s)",
                mac,
                portals[portal_id].get("name", portal_id),
            )
            return jsonify({"success": True})

        except Exception as e:
            logger.error(f"Error deleting MAC: {e}")
            return jsonify({"success": False, "message": str(e)})

    @bp.route("/api/portals/data", methods=["GET"])
    @authorise
    def portals_data():
        return jsonify(getPortals())

    @bp.route("/api/portal/macs/refresh", methods=["POST"])
    @authorise
    def refresh_portal_macs():
        try:
            data = request.get_json()
            portal_id = data.get("portal_id")

            if not portal_id:
                return jsonify({"success": False, "message": "Portal ID required"})

            portals = getPortals()
            if portal_id not in portals:
                return jsonify({"success": False, "message": "Portal not found"})

            portal = portals[portal_id]
            url = portal["url"]
            proxy = portal.get("proxy", "")

            updated_count = 0
            errors = []

            for mac in list(portal["macs"].keys()):
                try:
                    token = stb.getToken(url, mac, proxy)
                    if token:
                        profile = stb.getProfile(url, mac, token, proxy)
                        expiry = stb.getExpires(url, mac, token, proxy)

                        if profile or expiry:
                            old_data = normalize_mac_data(portal["macs"].get(mac, {}))
                            if not expiry:
                                expiry = old_data.get("expiry", "Unknown")

                            watchdog_timeout = (
                                int(profile.get("watchdog_timeout", 0)) if profile else 0
                            )
                            playback_limit = (
                                int(profile.get("playback_limit", 0)) if profile else 0
                            )

                            portal["macs"][mac] = {
                                "expiry": expiry if expiry else "Unknown",
                                "watchdog_timeout": watchdog_timeout,
                                "playback_limit": playback_limit,
                            }
                            updated_count += 1
                            logger.info(
                                "Refreshed MAC %s: expiry=%s, watchdog=%s, streams=%s",
                                mac,
                                expiry,
                                watchdog_timeout,
                                playback_limit,
                            )
                        else:
                            errors.append(f"{mac}: Could not get profile or expiry")
                    else:
                        errors.append(f"{mac}: Could not get token")
                except Exception as e:
                    errors.append(f"{mac}: {str(e)}")
                    logger.error(f"Error refreshing MAC {mac}: {e}")

            portals[portal_id] = portal
            savePortals(portals)

            message = f"Updated {updated_count} of {len(portal['macs'])} MACs"
            if errors:
                message += f". Errors: {len(errors)}"

            logger.info(
                "MAC refresh for portal %s: %s",
                portal.get("name", portal_id),
                message,
            )
            return jsonify(
                {
                    "success": True,
                    "message": message,
                    "updated": updated_count,
                    "errors": errors,
                    "macs": portal["macs"],
                }
            )

        except Exception as e:
            logger.error(f"Error refreshing MACs: {e}")
            return jsonify({"success": False, "message": str(e)})

    @bp.route("/api/portal/genres", methods=["POST"])
    @authorise
    def get_portal_genres():
        try:
            data = request.get_json()
            url = data.get("url")
            mac = data.get("mac")
            proxy = data.get("proxy", "")

            if not url or not mac:
                return jsonify({"success": False, "message": "URL and MAC are required"})

            if not url.endswith(".php"):
                resolved_url = stb.getUrl(url, proxy)
                if not resolved_url:
                    return jsonify(
                        {"success": False, "message": "Could not resolve portal URL"}
                    )
                url = resolved_url

            token = stb.getToken(url, mac, proxy)
            if not token:
                return jsonify(
                    {"success": False, "message": "Could not authenticate with portal"}
                )

            stb.getProfile(url, mac, token, proxy)
            genres = stb.getGenres(url, mac, token, proxy)

            if not genres:
                return jsonify({"success": False, "message": "No genres found"})

            channels = stb.getAllChannels(url, mac, token, proxy)
            channel_counts = {}
            if channels:
                channel_list = (
                    channels if isinstance(channels, list) else list(channels.values())
                )
                for channel in channel_list:
                    if isinstance(channel, dict):
                        genre_id = str(channel.get("tv_genre_id", ""))
                        channel_counts[genre_id] = channel_counts.get(genre_id, 0) + 1

            genre_list = [
                {
                    "id": str(g["id"]),
                    "title": g["title"],
                    "channel_count": channel_counts.get(str(g["id"]), 0),
                }
                for g in genres
            ]
            genre_list.sort(key=lambda x: x["title"].lower())

            logger.info(
                "Fetched %s genres with channel counts from portal", len(genre_list)
            )
            return jsonify({"success": True, "genres": genre_list})

        except Exception as e:
            logger.error(f"Error fetching genres: {e}")
            return jsonify({"success": False, "message": str(e)})

    @bp.route("/api/portal/groups", methods=["POST"])
    @authorise
    def get_portal_groups():
        try:
            data = request.get_json()
            portal_id = data.get("portal_id")

            if not portal_id:
                return jsonify({"success": False, "message": "Portal ID required"})

            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT COUNT(*) as cnt
                FROM channels
                WHERE portal = ? AND (genre_id IS NULL OR genre_id = '')
                """,
                [portal_id],
            )
            ungrouped_count = cursor.fetchone()[0] or 0

            cursor.execute(
                """
                SELECT genre_id, name, channel_count, active
                FROM groups
                WHERE portal = ?
                ORDER BY name
                """,
                [portal_id],
            )

            groups = []
            has_ungrouped = False
            for row in cursor.fetchall():
                if row["genre_id"] == "UNGROUPED":
                    has_ungrouped = True
                    groups.append(
                        {
                            "id": "UNGROUPED",
                            "title": "Ungrouped",
                            "channel_count": ungrouped_count,
                            "active": row["active"] == 1,
                        }
                    )
                    continue
                groups.append(
                    {
                        "id": row["genre_id"],
                        "title": row["name"] or f"Group {row['genre_id']}",
                        "channel_count": row["channel_count"] or 0,
                        "active": row["active"] == 1,
                    }
                )

            if not has_ungrouped:
                groups.insert(
                    0,
                    {
                        "id": "UNGROUPED",
                        "title": "Ungrouped",
                        "channel_count": ungrouped_count,
                        "active": False,
                    },
                )

            conn.close()

            if not groups:
                return jsonify(
                    {
                        "success": False,
                        "message": "No groups found in database. Please refresh channels first.",
                    }
                )

            logger.info(
                "Loaded %s groups from database for portal %s", len(groups), portal_id
            )
            return jsonify({"success": True, "groups": groups})

        except Exception as e:
            logger.error(f"Error getting groups from database: {e}")
            return jsonify({"success": False, "message": str(e)})

    @bp.route("/api/portal/genres/update", methods=["POST"])
    @authorise
    def update_portal_genres():
        try:
            data = request.get_json()
            portal_id = data.get("portal_id")
            selected_genres = [str(g) for g in data.get("selected_genres", [])]

            if not portal_id:
                return jsonify({"success": False, "message": "Portal ID required"})

            portals = getPortals()
            if portal_id not in portals:
                return jsonify({"success": False, "message": "Portal not found"})

            portal = portals[portal_id]
            logger.info(
                "Updating genres for portal %s", portal.get("name", portal_id)
            )
            logger.info("Selected genres: %s", selected_genres)

            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT COUNT(*) as cnt
                FROM channels
                WHERE portal = ? AND (genre_id IS NULL OR genre_id = '')
                """,
                [portal_id],
            )
            ungrouped_count = cursor.fetchone()[0] or 0
            cursor.execute(
                """
                INSERT INTO groups (portal, genre_id, name, channel_count, active)
                VALUES (?, 'UNGROUPED', 'Ungrouped', ?, 0)
                ON CONFLICT(portal, genre_id) DO UPDATE SET
                    name = excluded.name,
                    channel_count = excluded.channel_count
                """,
                (portal_id, ungrouped_count),
            )

            cursor.execute("UPDATE groups SET active = 0 WHERE portal = ?", [portal_id])

            if selected_genres:
                placeholders = ",".join(["?" for _ in selected_genres])
                cursor.execute(
                    f"UPDATE groups SET active = 1 WHERE portal = ? AND genre_id IN ({placeholders})",
                    [portal_id] + selected_genres,
                )

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) as total_groups,
                    SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) as active_groups,
                    SUM(channel_count) as total_channels,
                    SUM(CASE WHEN active = 1 THEN channel_count ELSE 0 END) as active_channels
                FROM groups WHERE portal = ?
                """,
                [portal_id],
            )
            row = cursor.fetchone()
            conn.close()

            total_groups = row[0] or 0
            active_groups = row[1] or 0
            total_channels = row[2] or 0
            active_channels = row[3] or 0

            portal["selected_genres"] = selected_genres
            portal["total_groups"] = total_groups
            portal["total_channels"] = total_channels
            portals[portal_id] = portal
            savePortals(portals)

            logger.info(
                "Updated genres for %s: %s/%s groups active, %s/%s channels",
                portal.get("name", portal_id),
                active_groups,
                total_groups,
                active_channels,
                total_channels,
            )

            set_cached_xmltv(None)

            match_started = False
            if getSettings().get("channelsdvr enabled", "false") == "true":
                def background_match():
                    with channelsdvr_match_status_lock:
                        channelsdvr_match_status[portal_id] = {
                            "status": "running",
                            "started_at": datetime.utcnow().isoformat(),
                            "completed_at": None,
                            "matched": 0,
                            "error": None,
                        }
                    try:
                        matched_count = run_portal_matching(portal_id)
                        with channelsdvr_match_status_lock:
                            channelsdvr_match_status[portal_id].update(
                                {
                                    "status": "completed",
                                    "completed_at": datetime.utcnow().isoformat(),
                                    "matched": matched_count,
                                }
                            )
                    except Exception as e:
                        with channelsdvr_match_status_lock:
                            channelsdvr_match_status[portal_id].update(
                                {
                                    "status": "error",
                                    "completed_at": datetime.utcnow().isoformat(),
                                    "error": str(e),
                                }
                            )
                        logger.error(f"Matching failed for portal {portal_id}: {e}")

                Thread(target=background_match, daemon=True).start()
                match_started = True

            return jsonify(
                {
                    "success": True,
                    "message": "Genres updated successfully",
                    "total_groups": total_groups,
                    "active_groups": active_groups,
                    "total_channels": total_channels,
                    "active_channels": active_channels,
                    "match_started": match_started,
                }
            )

        except Exception as e:
            logger.error(f"Error updating genres: {e}")
            return jsonify({"success": False, "message": str(e)})

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
            logger.info("Portal(%s) added!", portal["name"])
            flash(f"Portal({portal['name']}) added!", "success")

            def background_refresh():
                try:
                    refresh_channels_cache()
                    logger.info(
                        "Background channel refresh completed for new portal %s", name
                    )
                except Exception as e:
                    logger.error(f"Error refreshing channels after portal add: {e}")

            thread = Thread(target=background_refresh, daemon=True)
            thread.start()
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
        logger.info("Portal (%s) removed!", name)

        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM channels WHERE portal = ?", (portal_id,))
            deleted_count = cursor.rowcount
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

            total = refresh_channels_cache(target_portal_id=portal_id)
            logger.info(
                "Portal %s channel refresh completed: %s channels",
                portal_name,
                total,
            )

            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute(
                f"""
                SELECT
                    COUNT(*) as total_channels,
                    SUM(CASE WHEN {ACTIVE_GROUP_CONDITION} THEN 1 ELSE 0 END) as active_channels
                FROM channels c
                LEFT JOIN groups g ON c.portal = g.portal AND c.genre_id = g.genre_id
                WHERE c.portal = ?
                """,
                [portal_id],
            )
            ch_row = cursor.fetchone()

            cursor.execute(
                """
                SELECT
                    COUNT(*) as total_groups,
                    SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) as active_groups
                FROM groups WHERE portal = ?
                """,
                [portal_id],
            )
            gr_row = cursor.fetchone()
            conn.close()

            stats = {
                "total_channels": ch_row[0] or 0,
                "channels": ch_row[1] or 0,
                "total_groups": gr_row[0] or 0,
                "groups": gr_row[1] or 0,
            }

            return jsonify(
                {
                    "status": "success",
                    "total": total,
                    "portal": portal_name,
                    "stats": stats,
                }
            )
        except Exception as e:
            logger.error(f"Error refreshing portal channels: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500

    return bp
