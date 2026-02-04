from flask import Blueprint, Response

from ..security import authorise


def create_playlist_blueprint(
    *,
    logger,
    host,
    getSettings,
    get_db_connection,
    ACTIVE_GROUP_CONDITION,
    effective_display_name,
    effective_epg_name,
    get_cached_playlist,
    set_cached_playlist,
    get_last_playlist_host,
    set_last_playlist_host,
):
    bp = Blueprint("playlist", __name__)

    def generate_playlist():
        logger.info("Generating playlist.m3u from database...")

        channels = []

        conn = get_db_connection()
        cursor = conn.cursor()

        order_clause = ""
        if getSettings().get("sort playlist by channel name", True):
            order_clause = (
                "ORDER BY COALESCE(NULLIF(c.custom_name, ''), NULLIF(c.auto_name, ''), c.name)"
            )
        elif getSettings().get("use channel numbers", True):
            if getSettings().get("sort playlist by channel number", False):
                order_clause = (
                    "ORDER BY CAST(COALESCE(NULLIF(c.custom_number, ''), c.number) AS INTEGER)"
                )
        elif getSettings().get("use channel genres", True):
            if getSettings().get("sort playlist by channel genre", False):
                order_clause = (
                    "ORDER BY COALESCE(NULLIF(c.custom_genre, ''), c.genre)"
                )

        cursor.execute(
            f"""
            SELECT
                c.portal_id as portal, c.channel_id, c.name, c.number, c.genre,
                c.custom_name, c.auto_name, c.matched_name, c.custom_number, c.custom_genre, c.custom_epg_id
            FROM channels c
            LEFT JOIN groups g ON c.portal_id = g.portal_id AND c.genre_id = g.genre_id
            WHERE c.enabled = 1 AND {ACTIVE_GROUP_CONDITION}
            {order_clause}
            """
        )

        for row in cursor.fetchall():
            portal = row["portal"]
            channel_id = row["channel_id"]

            channel_name = effective_display_name(
                row["custom_name"], row["matched_name"], row["auto_name"], row["name"]
            )
            channel_number = row["custom_number"] if row["custom_number"] else row["number"]
            channel_number = channel_number or ""
            genre = row["custom_genre"] if row["custom_genre"] else row["genre"]
            epg_id = row["custom_epg_id"] if row["custom_epg_id"] else effective_epg_name(
                row["custom_name"], row["auto_name"], row["name"]
            )

            channel_entry = (
                "#EXTINF:-1"
                + ' tvg-id="'
                + epg_id
                + '"'
                + ' tvg-name="'
                + channel_name
                + '"'
                + ' group-title="'
                + (genre or "")
                + '",'
                + channel_number
                + " "
                + channel_name
            )

            url = f"http://{host}/play/{portal}/{channel_id}?web=true"

            channels.append(channel_entry)
            channels.append(url)

        conn.close()

        playlist_content = "#EXTM3U\n" + "\n".join(channels)
        set_cached_playlist(playlist_content)

    @bp.route("/playlist.m3u", methods=["GET"])
    @authorise
    def playlist():
        logger.info("Playlist Requested")

        current_host = host
        cached_playlist = get_cached_playlist()

        if (
            cached_playlist is None
            or len(cached_playlist) == 0
            or get_last_playlist_host() != current_host
        ):
            logger.info(
                "Regenerating playlist due to host change: %s -> %s",
                get_last_playlist_host(),
                current_host,
            )
            set_last_playlist_host(current_host)
            generate_playlist()

        return Response(get_cached_playlist(), mimetype="text/plain")

    @bp.route("/update_playlistm3u", methods=["POST"])
    def update_playlistm3u():
        generate_playlist()
        return Response("Playlist updated successfully", status=200)

    return bp
