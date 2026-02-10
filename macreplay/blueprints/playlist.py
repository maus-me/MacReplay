import re
from urllib.parse import urlparse

from flask import Blueprint, Response, request

from ..security import authorise


def create_playlist_blueprint(
    *,
    logger,
    host,
    getPortals,
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

    def _normalize_host(value):
        if not value:
            return ""
        text = str(value).strip()
        if not text:
            return ""
        if "//" in text:
            try:
                parsed = urlparse(text)
                if parsed.netloc:
                    return parsed.netloc
            except Exception:
                pass
        return text

    def _determine_base_host():
        forwarded_proto = request.headers.get("X-Forwarded-Proto")
        forwarded_host = request.headers.get("X-Forwarded-Host")
        forwarded_port = request.headers.get("X-Forwarded-Port")
        if forwarded_host:
            if forwarded_port and ":" not in forwarded_host:
                return f"{forwarded_host}:{forwarded_port}", forwarded_proto or request.scheme
            return forwarded_host, forwarded_proto or request.scheme
        return request.host, request.scheme

    def _normalize_alias(value):
        if not value:
            return ""
        text = str(value).strip().lower()
        text = re.sub(r"[^a-z0-9]", "", text)
        return text

    def _normalize_country_code(value):
        if not value:
            return ""
        text = str(value).strip().upper()
        text = re.sub(r"[^A-Z]", "", text)
        if not text:
            return ""
        return text[:2]

    def _normalize_quality(value):
        if not value:
            return ""
        text = str(value).strip().upper()
        return text

    def _is_hevc(value):
        if not value:
            return False
        text = str(value).lower()
        return "hevc" in text or "h265" in text

    def _format_display_name(fmt, *, name, country, portal_code, quality, hevc, event):
        prefix = " | ".join([p for p in [country, portal_code] if p])
        suffix_parts = []
        if quality:
            suffix_parts.append(quality)
        if hevc:
            suffix_parts.append("HEVC")
        if event:
            suffix_parts.append("EVENT")
        suffix = " | ".join([p for p in suffix_parts if p])
        mapping = {
            "prefix": prefix,
            "name": name,
            "suffix": suffix,
            "country": country,
            "portal_code": portal_code,
            "quality": quality,
            "hevc": "HEVC" if hevc else "",
            "event": "EVENT" if event else "",
        }
        try:
            formatted = str(fmt or "").format_map({**{k: "" for k in mapping}, **mapping})
        except Exception:
            formatted = f"{prefix} {name} {suffix}"
        formatted = re.sub(r"\(\s*\)", "", formatted)
        formatted = re.sub(r"\s{2,}", " ", formatted).strip()
        formatted = re.sub(r"\(\s+", "(", formatted)
        formatted = re.sub(r"\s+\)", ")", formatted)
        return formatted

    def generate_playlist(base_host, scheme):
        logger.info("Generating playlist.m3u from database...")

        channels = []

        conn = get_db_connection()
        cursor = conn.cursor()
        portals = getPortals() or {}

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
                c.custom_name, c.auto_name, c.matched_name, c.custom_number, c.custom_genre, c.custom_epg_id,
                c.is_event, c.country, c.resolution, c.video_codec
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
            if row["is_event"] and not genre:
                genre = "EVENTS"
            epg_id = row["custom_epg_id"] if row["custom_epg_id"] else effective_epg_name(
                row["custom_name"], row["auto_name"], row["name"]
            )

            portal_code = ""
            portal_data = portals.get(portal, {})
            if portal_data:
                portal_code = str(portal_data.get("portal code", "")).strip().upper()
                portal_code = re.sub(r"[^A-Z0-9]", "", portal_code)
                if portal_code:
                    portal_code = portal_code[:2]

            country_code = _normalize_country_code(row["country"])
            quality = _normalize_quality(row["resolution"])
            is_hevc = _is_hevc(row["video_codec"])
            is_event = bool(row["is_event"])
            name_format = getSettings().get("playlist name format", "({prefix}) {name} ({suffix})")
            display_name = _format_display_name(
                name_format,
                name=channel_name,
                country=country_code,
                portal_code=portal_code,
                quality=quality,
                hevc=is_hevc,
                event=is_event,
            )

            tvg_id = display_name if row["is_event"] else epg_id
            tvg_name = display_name
            channel_entry = (
                "#EXTINF:-1"
                + ' tvg-id="'
                + tvg_id
                + '"'
                + ' tvg-name="'
                + tvg_name
                + '"'
                + ' group-title="'
                + (genre or "")
                + '",'
                + channel_number
                + " "
                + display_name
            )

            url = f"{scheme}://{base_host}/play/{portal}/{channel_id}?web=true"

            channels.append(channel_entry)
            if row["is_event"]:
                channels.append(f"#EXTGRP:{genre or 'EVENTS'}")
            channels.append(url)

        conn.close()

        playlist_content = "#EXTM3U\n" + "\n".join(channels)
        return playlist_content

    @bp.route("/playlist.m3u", methods=["GET"])
    @authorise
    def playlist():
        logger.info("Playlist Requested")

        base_host, scheme = _determine_base_host()
        current_host = _normalize_host(base_host) or host
        cache_key = f"{scheme}://{current_host}"
        cached_playlist = get_cached_playlist() or {}

        logger.info(
            "Regenerating playlist for request host: %s",
            current_host,
        )
        set_last_playlist_host(current_host)
        playlist_content = generate_playlist(current_host, scheme)
        cached_playlist[cache_key] = playlist_content
        set_cached_playlist(cached_playlist)

        return Response(playlist_content, mimetype="text/plain")

    @bp.route("/update_playlistm3u", methods=["POST"])
    def update_playlistm3u():
        base_host, scheme = _determine_base_host()
        current_host = _normalize_host(base_host) or host
        cache_key = f"{scheme}://{current_host}"
        playlist_content = generate_playlist(current_host, scheme)
        cached_playlist = get_cached_playlist() or {}
        cached_playlist[cache_key] = playlist_content
        set_cached_playlist(cached_playlist)
        return Response("Playlist updated successfully", status=200)

    return bp
