import json
import re
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import requests
from flask import Blueprint, jsonify, render_template, request

from ..security import authorise
from ..services.espn_catalog import ESPN_CATALOG


def create_events_blueprint(
    *,
    logger,
    get_db_connection,
    getSettings,
    open_epg_source_db,
    effective_epg_name,
):
    bp = Blueprint("events", __name__)
    SPORTSDB_BASE_V2 = "https://www.thesportsdb.com/api/v2/json"
    ESPN_SITE_API_BASE = "https://site.api.espn.com/apis/site/v2/sports"

    def _loads_list(raw):
        if isinstance(raw, list):
            return raw
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
                return parsed if isinstance(parsed, list) else []
            except Exception:
                return []
        return []

    def _normalize_alias(text):
        if not text:
            return ""
        return re.sub(r"[^a-z0-9]", "", str(text).lower())

    def _build_alias_abbrev_map(cursor):
        alias_map = {}
        try:
            rows = cursor.execute(
                "SELECT team_name, team_aliases FROM espn_teams_cache"
            ).fetchall()
        except Exception:
            return alias_map
        for row in rows:
            aliases = []
            if row["team_name"]:
                aliases.append(row["team_name"])
            if row["team_aliases"]:
                aliases.extend(_loads_list(row["team_aliases"]))
            aliases = [a for a in aliases if a]
            if not aliases:
                continue
            short = None
            for alias in aliases:
                cleaned = re.sub(r"[^A-Za-z0-9]", "", alias)
                if 2 <= len(cleaned) <= 4 and cleaned.isalnum():
                    short = cleaned.upper()
                    break
            if not short:
                short = min(aliases, key=lambda x: len(str(x)))
            for alias in aliases:
                norm = _normalize_alias(alias)
                if norm and norm not in alias_map:
                    alias_map[norm] = short
        return alias_map

    def _apply_event_template(template, group_template, event, alias_map):
        home = event.get("home") or ""
        away = event.get("away") or ""
        sport = event.get("sport") or ""
        league = event.get("league") or ""
        start_raw = event.get("start") or ""
        start_dt = None
        if start_raw:
            try:
                start_dt = datetime.fromisoformat(start_raw.replace("Z", "+00:00")).astimezone()
            except Exception:
                start_dt = None
        date_str = start_dt.strftime("%Y-%m-%d") if start_dt else ""
        time_str = start_dt.strftime("%H:%M") if start_dt else ""
        home_abbr = alias_map.get(_normalize_alias(home), home)
        away_abbr = alias_map.get(_normalize_alias(away), away)
        event_name = (template or "").strip()
        event_name = event_name.replace("{home}", home).replace("{away}", away)
        event_name = event_name.replace("{home_abbr}", home_abbr).replace("{away_abbr}", away_abbr)
        event_name = event_name.replace("{home_short}", home_abbr).replace("{away_short}", away_abbr)
        event_name = event_name.replace("{sport}", sport).replace("{league}", league)
        event_name = event_name.replace("{date}", date_str).replace("{time}", time_str)
        event_name = event_name.strip() or f"{home} vs {away}".strip()
        output_group = (group_template or "").replace("{sport}", sport).replace("{league}", league).strip()
        if not output_group:
            output_group = group_template or "EVENTS"
        return event_name, output_group

    def _update_generated_channels_for_rule(cursor, rule_id, output_template, output_group):
        alias_map = _build_alias_abbrev_map(cursor)
        rows = cursor.execute(
            """
            SELECT portal_id, channel_id, event_home, event_away, event_start, event_sport, event_league
            FROM event_generated_channels
            WHERE rule_id = ?
            """,
            (rule_id,),
        ).fetchall()
        updated = 0
        for row in rows:
            if not (row["event_home"] or row["event_away"]):
                continue
            event = {
                "home": row["event_home"] or "",
                "away": row["event_away"] or "",
                "start": row["event_start"] or "",
                "sport": row["event_sport"] or "",
                "league": row["event_league"] or "",
            }
            event_name, group_name = _apply_event_template(
                output_template, output_group, event, alias_map
            )
            cursor.execute(
                """
                UPDATE channels
                SET name = ?, custom_name = ?, display_name = ?, custom_genre = ?, genre = ?
                WHERE portal_id = ? AND channel_id = ?
                """,
                (
                    event_name,
                    event_name,
                    event_name,
                    group_name,
                    group_name,
                    row["portal_id"],
                    row["channel_id"],
                ),
            )
            updated += 1
        return updated

    def _resolve_espn_names(sport_key, league_key):
        sport_name = sport_key
        league_name = league_key
        for sport in ESPN_CATALOG:
            if sport.get("key") == sport_key:
                sport_name = sport.get("name") or sport_key
                for league in sport.get("leagues") or []:
                    if league.get("key") == league_key:
                        league_name = league.get("name") or league_key
                        return sport_name, league_name
        return sport_name, league_name

    def _extract_team_aliases(team):
        aliases = []
        if not isinstance(team, dict):
            return aliases

        # SportsDB can expose alternates in different fields depending on endpoint/version.
        candidates = [
            team.get("strAlternate"),
            team.get("strTeamAlternate"),
            team.get("strTeamShort"),
            team.get("strKeywords"),
            team.get("strTeam"),
        ]
        for value in candidates:
            if not value:
                continue
            if isinstance(value, list):
                parts = [str(v).strip() for v in value if str(v).strip()]
            else:
                parts = [p.strip() for p in re.split(r"[;,|/]", str(value)) if p.strip()]
            aliases.extend(parts)

        # Keep order stable and remove duplicates (case-insensitive).
        deduped = []
        seen = set()
        for alias in aliases:
            key = alias.lower()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(alias)
        return deduped

    def _split_tags(value):
        return [v.strip() for v in str(value or "").split(",") if v.strip()]

    def _sync_channel_tags(cursor, portal_id, channel_id, event_tags, misc_tags):
        cursor.execute(
            "DELETE FROM channel_tags WHERE portal_id = ? AND channel_id = ?",
            (portal_id, channel_id),
        )
        rows = []
        for value in event_tags or []:
            rows.append((portal_id, channel_id, "event", value))
        for value in misc_tags or []:
            rows.append((portal_id, channel_id, "misc", value))
        if rows:
            cursor.executemany(
                "INSERT OR IGNORE INTO channel_tags (portal_id, channel_id, tag_type, tag_value) VALUES (?, ?, ?, ?)",
                rows,
            )

    def _cleanup_expired_event_channels(cursor):
        now_ts = time.time()
        rows = cursor.execute(
            """
            SELECT portal_id, channel_id
            FROM event_generated_channels
            WHERE expires_at IS NOT NULL AND expires_at <= ?
            """,
            (now_ts,),
        ).fetchall()
        if not rows:
            return 0
        to_delete = [(row["portal_id"], row["channel_id"]) for row in rows]
        cursor.executemany(
            "DELETE FROM channels WHERE portal_id = ? AND channel_id = ?",
            to_delete,
        )
        cursor.executemany(
            "DELETE FROM channel_tags WHERE portal_id = ? AND channel_id = ?",
            to_delete,
        )
        cursor.executemany(
            "DELETE FROM event_generated_channels WHERE portal_id = ? AND channel_id = ?",
            to_delete,
        )
        return len(to_delete)

    def _serialize_rule_payload(payload):
        return {
            "name": (payload.get("name") or "").strip(),
            "enabled": 1 if payload.get("enabled", True) else 0,
            "provider": (payload.get("provider") or "sportsdb").strip().lower(),
            "use_espn_events": 1 if payload.get("use_espn_events", False) else 0,
            "espn_event_window_hours": int(payload.get("espn_event_window_hours", 72) or 72),
            "sport": (payload.get("sport") or "").strip(),
            "league_filters": json.dumps(_loads_list(payload.get("league_filters", []))),
            "team_filters": json.dumps(_loads_list(payload.get("team_filters", []))),
            "channel_groups": json.dumps(_loads_list(payload.get("channel_groups", []))),
            "channel_regex": (payload.get("channel_regex") or "").strip(),
            "epg_pattern": (payload.get("epg_pattern") or "").strip(),
            "extract_regex": (payload.get("extract_regex") or "").strip(),
            "output_template": (payload.get("output_template") or "{home} vs {away} | {date} {time}").strip(),
            "output_group_name": (payload.get("output_group_name") or "EVENTS").strip(),
            "channel_number_start": int(payload.get("channel_number_start", 10000) or 10000),
            "priority": int(payload.get("priority", 100) or 100),
        }

    def _normalize_provider(value):
        provider = (value or "sportsdb").strip().lower()
        return provider if provider in ("sportsdb", "espn") else "sportsdb"

    def _sportsdb_config():
        settings = getSettings()
        enabled = str(settings.get("sportsdb enabled", "false")).lower() == "true"
        api_key = (settings.get("sportsdb api key") or "123").strip()
        api_version = str(settings.get("sportsdb api version", "v1") or "v1").strip().lower()
        if api_version not in ("v1", "v2"):
            api_version = "v1"
        raw_sports = settings.get("sportsdb import sports", "")
        sports = [s.strip() for s in str(raw_sports).split(",") if s.strip()]
        try:
            ttl_hours = int(settings.get("sportsdb cache ttl hours", 24) or 24)
        except (TypeError, ValueError):
            ttl_hours = 24
        return {
            "enabled": enabled,
            "api_key": api_key or "123",
            "api_version": api_version,
            "sports": set(sports),
            "ttl_seconds": max(1, ttl_hours) * 3600,
        }

    def _espn_config():
        settings = getSettings()
        enabled = str(settings.get("espn enabled", "false")).lower() == "true"
        raw_sports = settings.get("espn import sports", "")
        sports = {s.strip().lower() for s in str(raw_sports).split(",") if s.strip()}
        try:
            ttl_hours = int(settings.get("espn cache ttl hours", 24) or 24)
        except (TypeError, ValueError):
            ttl_hours = 24
        return {
            "enabled": enabled,
            "sports": sports,
            "ttl_seconds": max(1, ttl_hours) * 3600,
        }

    def _sportsdb_get_v2(path, params=None):
        cfg = _sportsdb_config()
        safe_path = str(path or "").lstrip("/")
        url = f"{SPORTSDB_BASE_V2}/{safe_path}"
        safe_params = params or {}
        query = urlencode(safe_params, doseq=True)
        request_label = f"v2:{safe_path}?{query}" if query else f"v2:{safe_path}"
        start = time.time()
        logger.info("SportsDB request start: %s", request_label)
        response = requests.get(
            url,
            params=safe_params,
            headers={"X-API-KEY": cfg["api_key"]},
            timeout=12,
        )
        response.raise_for_status()
        data = response.json()
        duration_ms = int((time.time() - start) * 1000)
        top_keys = ",".join(sorted(list(data.keys()))) if isinstance(data, dict) else type(data).__name__
        logger.info(
            "SportsDB request done: %s status=%s duration_ms=%s keys=%s",
            request_label,
            response.status_code,
            duration_ms,
            top_keys,
        )
        return data

    def _sportsdb_get_v1(path, params=None):
        cfg = _sportsdb_config()
        safe_path = str(path or "").lstrip("/")
        url = f"https://www.thesportsdb.com/api/v1/json/{cfg['api_key']}/{safe_path}"
        safe_params = params or {}
        query = urlencode(safe_params, doseq=True)
        request_label = f"v1:{safe_path}?{query}" if query else f"v1:{safe_path}"
        start = time.time()
        logger.info("SportsDB request start: %s", request_label)
        response = requests.get(url, params=safe_params, timeout=12)
        response.raise_for_status()
        data = response.json()
        duration_ms = int((time.time() - start) * 1000)
        top_keys = ",".join(sorted(list(data.keys()))) if isinstance(data, dict) else type(data).__name__
        logger.info(
            "SportsDB request done: %s status=%s duration_ms=%s keys=%s",
            request_label,
            response.status_code,
            duration_ms,
            top_keys,
        )
        return data

    def _espn_get(path, params=None):
        safe_path = str(path or "").strip("/")
        url = f"{ESPN_SITE_API_BASE}/{safe_path}"
        safe_params = params or {}
        query = urlencode(safe_params, doseq=True)
        request_label = f"espn:{safe_path}?{query}" if query else f"espn:{safe_path}"
        start = time.time()
        logger.info("ESPN request start: %s", request_label)
        response = requests.get(url, params=safe_params, timeout=12)
        response.raise_for_status()
        data = response.json()
        duration_ms = int((time.time() - start) * 1000)
        top_keys = ",".join(sorted(list(data.keys()))) if isinstance(data, dict) else type(data).__name__
        logger.info(
            "ESPN request done: %s status=%s duration_ms=%s keys=%s",
            request_label,
            response.status_code,
            duration_ms,
            top_keys,
        )
        return data

    def _cache_is_stale(updated_at, ttl_seconds):
        if updated_at is None:
            return True
        return (time.time() - float(updated_at)) > ttl_seconds

    def _refresh_espn_catalog(force=False):
        cfg = _espn_config()
        if not cfg["enabled"]:
            return {"ok": False, "error": "espn disabled in settings"}

        now = time.time()
        conn = get_db_connection()
        try:
            should_refresh = force
            if not should_refresh:
                row = conn.execute(
                    "SELECT MAX(updated_at) AS updated_at FROM espn_sports_cache"
                ).fetchone()
                should_refresh = _cache_is_stale(row["updated_at"] if row else None, cfg["ttl_seconds"])
            if not should_refresh:
                return {"ok": True, "refreshed": False}

            for sport in ESPN_CATALOG:
                sport_key = sport["key"]
                if cfg["sports"] and sport_key not in cfg["sports"]:
                    continue
                conn.execute(
                    """
                    INSERT INTO espn_sports_cache (sport_key, sport_name, updated_at, raw_json)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(sport_key) DO UPDATE SET
                        sport_name = excluded.sport_name,
                        updated_at = excluded.updated_at,
                        raw_json = excluded.raw_json
                    """,
                    (sport_key, sport["name"], now, json.dumps(sport)),
                )
                for league in sport.get("leagues", []):
                    conn.execute(
                        """
                        INSERT INTO espn_leagues_cache (league_key, league_name, sport_key, updated_at, raw_json)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(league_key) DO UPDATE SET
                            league_name = excluded.league_name,
                            sport_key = excluded.sport_key,
                            updated_at = excluded.updated_at,
                            raw_json = excluded.raw_json
                        """,
                        (
                            league["key"],
                            league["name"],
                            sport_key,
                            now,
                            json.dumps(league),
                        ),
                    )
            conn.commit()
            return {"ok": True, "refreshed": True}
        finally:
            conn.close()

    def _parse_espn_team_rows(data):
        teams = []
        if isinstance(data, dict):
            sports = data.get("sports") or []
            for sport in sports:
                leagues = sport.get("leagues") or []
                for league in leagues:
                    for item in league.get("teams") or []:
                        team = item.get("team") if isinstance(item, dict) and isinstance(item.get("team"), dict) else item
                        if isinstance(team, dict):
                            teams.append(team)
            for item in data.get("teams") or []:
                team = item.get("team") if isinstance(item, dict) and isinstance(item.get("team"), dict) else item
                if isinstance(team, dict):
                    teams.append(team)
        return teams

    def _extract_espn_team_aliases(team):
        aliases = []
        candidates = [
            team.get("displayName"),
            team.get("shortDisplayName"),
            team.get("abbreviation"),
            team.get("nickname"),
            team.get("name"),
            team.get("location"),
        ]
        for value in candidates:
            text = str(value or "").strip()
            if text:
                aliases.append(text)
        deduped = []
        seen = set()
        for alias in aliases:
            key = alias.lower()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(alias)
        return deduped

    def _refresh_espn_teams_for_league(sport_key, league_key, force=False):
        cfg = _espn_config()
        if not cfg["enabled"]:
            return {"ok": False, "error": "espn disabled in settings"}

        conn = get_db_connection()
        now = time.time()
        try:
            league_row = conn.execute(
                "SELECT league_name, sport_key FROM espn_leagues_cache WHERE league_key = ?",
                (league_key,),
            ).fetchone()
            if not league_row:
                return {"ok": False, "error": "league not found in cache"}
            if sport_key and league_row["sport_key"] != sport_key:
                return {"ok": False, "error": "league does not belong to selected sport"}

            should_refresh = force
            if not should_refresh:
                row = conn.execute(
                    "SELECT MAX(updated_at) AS updated_at FROM espn_teams_cache WHERE league_key = ?",
                    (league_key,),
                ).fetchone()
                should_refresh = _cache_is_stale(row["updated_at"] if row else None, cfg["ttl_seconds"])
            if not should_refresh:
                logger.info("ESPN teams cache hit: sport=%s league=%s", league_row["sport_key"], league_key)
                return {"ok": True, "refreshed": False}

            payload = _espn_get(f"{league_row['sport_key']}/{league_key}/teams")
            team_rows = _parse_espn_team_rows(payload)
            logger.info(
                "ESPN teams lookup: sport=%s league=%s teams=%s",
                league_row["sport_key"],
                league_key,
                len(team_rows),
            )
            seen_team_keys = set()
            for team in team_rows:
                team_id = str(team.get("id") or "").strip()
                team_name = str(team.get("displayName") or team.get("name") or "").strip()
                if not team_name:
                    continue
                team_key = f"{league_row['sport_key']}::{league_key}::{team_id or team_name.lower()}"
                seen_team_keys.add(team_key)
                conn.execute(
                    """
                    INSERT INTO espn_teams_cache
                    (team_key, team_id, team_name, team_aliases, sport_key, league_key, league_name, updated_at, raw_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(team_key) DO UPDATE SET
                        team_id = excluded.team_id,
                        team_name = excluded.team_name,
                        team_aliases = excluded.team_aliases,
                        sport_key = excluded.sport_key,
                        league_key = excluded.league_key,
                        league_name = excluded.league_name,
                        updated_at = excluded.updated_at,
                        raw_json = excluded.raw_json
                    """,
                    (
                        team_key,
                        team_id or None,
                        team_name,
                        json.dumps(_extract_espn_team_aliases(team)),
                        league_row["sport_key"],
                        league_key,
                        league_row["league_name"],
                        now,
                        json.dumps(team),
                    ),
                )
            if seen_team_keys:
                placeholders = ",".join(["?"] * len(seen_team_keys))
                conn.execute(
                    f"""
                    DELETE FROM espn_teams_cache
                    WHERE league_key = ?
                      AND team_key NOT IN ({placeholders})
                    """,
                    [league_key, *seen_team_keys],
                )
            conn.commit()
            return {"ok": True, "refreshed": True}
        except requests.RequestException as exc:
            logger.warning("ESPN teams refresh failed for %s/%s: %s", sport_key, league_key, exc)
            return {"ok": False, "error": str(exc)}
        finally:
            conn.close()

    def _fetch_espn_upcoming_events(sport_key, league_key, window_hours):
        events = []
        now = datetime.now(timezone.utc)
        window_end = now + timedelta(hours=window_hours)
        window_start = now - timedelta(hours=window_hours)
        seen_ids = set()

        def _fetch_scoreboard(date_key):
            ttl_seconds = 6 * 60 * 60
            conn = get_db_connection()
            try:
                row = conn.execute(
                    """
                    SELECT fetched_at, raw_json
                    FROM espn_scoreboard_cache
                    WHERE league_key = ? AND date_key = ?
                    """,
                    (league_key, date_key),
                ).fetchone()
                if row and row["raw_json"] and row["fetched_at"]:
                    age = time.time() - float(row["fetched_at"])
                    if age < ttl_seconds:
                        try:
                            return json.loads(row["raw_json"])
                        except Exception:
                            pass
                data = _espn_get(f"{sport_key}/{league_key}/scoreboard", params={"dates": date_key})
                conn.execute(
                    """
                    INSERT INTO espn_scoreboard_cache (league_key, date_key, fetched_at, raw_json)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(league_key, date_key) DO UPDATE SET
                        fetched_at = excluded.fetched_at,
                        raw_json = excluded.raw_json
                    """,
                    (league_key, date_key, time.time(), json.dumps(data)),
                )
                conn.commit()
                return data
            finally:
                conn.close()

        sport_name, league_name = _resolve_espn_names(sport_key, league_key)

        def _add_events(data):
            for event in data.get("events") or []:
                try:
                    start = event.get("date")
                    if not start:
                        continue
                    start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
                except Exception:
                    continue
                if not (window_start <= start_dt <= window_end):
                    continue
                competitions = event.get("competitions") or []
                for comp in competitions:
                    competitors = comp.get("competitors") or []
                    home = ""
                    away = ""
                    home_score = None
                    away_score = None
                    for c in competitors:
                        team = c.get("team") or {}
                        name = team.get("displayName") or team.get("name") or ""
                        if c.get("homeAway") == "home":
                            home = name
                            home_score = c.get("score")
                        elif c.get("homeAway") == "away":
                            away = name
                            away_score = c.get("score")
                    status = comp.get("status") or {}
                    status_type = status.get("type") or {}
                    state = status_type.get("state") or ""
                    short_detail = status_type.get("shortDetail") or status_type.get("detail") or ""
                    if home or away:
                        event_id = event.get("id")
                        if event_id in seen_ids:
                            continue
                        seen_ids.add(event_id)
                        events.append(
                            {
                                "start_dt": start_dt,
                                "home": home,
                                "away": away,
                                "home_score": home_score,
                                "away_score": away_score,
                                "status": short_detail,
                                "state": state,
                                "event_id": event_id,
                                "sport": sport_name,
                                "league": league_name,
                                "sport_key": sport_key,
                                "league_key": league_key,
                            }
                        )

        for offset in range(-2, 3):
            day = (now + timedelta(days=offset)).strftime("%Y%m%d")
            data = _fetch_scoreboard(day)
            _add_events(data)
        return events

    def _refresh_sports_and_leagues(force=False):
        cfg = _sportsdb_config()
        if not cfg["enabled"]:
            return {"ok": False, "error": "sportsdb disabled in settings"}

        now = time.time()
        conn = get_db_connection()
        try:
            should_refresh = force
            if not should_refresh:
                row = conn.execute(
                    "SELECT MAX(updated_at) AS updated_at FROM sportsdb_sports_cache"
                ).fetchone()
                should_refresh = _cache_is_stale(row["updated_at"] if row else None, cfg["ttl_seconds"])

            if not should_refresh:
                return {"ok": True, "refreshed": False}

            if cfg["api_version"] == "v2":
                sports_data = _sportsdb_get_v2("all/sports")
            else:
                sports_data = _sportsdb_get_v1("all_sports.php")
            sports_rows = sports_data.get("sports") or []
            for sport in sports_rows:
                sport_name = (sport.get("strSport") or "").strip()
                if not sport_name:
                    continue
                conn.execute(
                    """
                    INSERT INTO sportsdb_sports_cache (sport_name, sport_id, updated_at, raw_json)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(sport_name) DO UPDATE SET
                        sport_id = excluded.sport_id,
                        updated_at = excluded.updated_at,
                        raw_json = excluded.raw_json
                    """,
                    (sport_name, sport.get("idSport"), now, json.dumps(sport)),
                )

            if cfg["api_version"] == "v2":
                leagues_data = _sportsdb_get_v2("all/leagues")
            else:
                leagues_data = _sportsdb_get_v1("all_leagues.php")
            league_rows = leagues_data.get("leagues") or []
            for league in league_rows:
                sport_name = (league.get("strSport") or "").strip()
                if cfg["sports"] and sport_name not in cfg["sports"]:
                    continue
                league_id = (league.get("idLeague") or "").strip()
                league_name = (league.get("strLeague") or "").strip()
                if not league_id or not league_name:
                    continue
                conn.execute(
                    """
                    INSERT INTO sportsdb_leagues_cache (league_id, league_name, sport_name, updated_at, raw_json)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(league_id) DO UPDATE SET
                        league_name = excluded.league_name,
                        sport_name = excluded.sport_name,
                        updated_at = excluded.updated_at,
                        raw_json = excluded.raw_json
                    """,
                    (league_id, league_name, sport_name, now, json.dumps(league)),
                )
            conn.commit()
            return {"ok": True, "refreshed": True}
        except requests.RequestException as exc:
            logger.warning("SportsDB refresh failed: %s", exc)
            return {"ok": False, "error": str(exc)}
        finally:
            conn.close()

    def _refresh_teams_for_league(league_id, force=False):
        cfg = _sportsdb_config()
        if not cfg["enabled"]:
            return {"ok": False, "error": "sportsdb disabled in settings"}

        conn = get_db_connection()
        now = time.time()
        try:
            league_row = conn.execute(
                "SELECT league_name, sport_name FROM sportsdb_leagues_cache WHERE league_id = ?",
                (league_id,),
            ).fetchone()
            if not league_row:
                return {"ok": False, "error": "league not found in cache"}
            if cfg["sports"] and (league_row["sport_name"] or "") not in cfg["sports"]:
                return {"ok": False, "error": "league sport not enabled in settings"}

            should_refresh = force
            if not should_refresh:
                row = conn.execute(
                    "SELECT MAX(updated_at) AS updated_at FROM sportsdb_teams_cache WHERE league_id = ?",
                    (league_id,),
                ).fetchone()
                should_refresh = _cache_is_stale(row["updated_at"] if row else None, cfg["ttl_seconds"])

            if not should_refresh:
                logger.info(
                    "SportsDB teams cache hit: league_id=%s league_name=%s (no external request)",
                    league_id,
                    league_row["league_name"],
                )
                return {"ok": True, "refreshed": False}

            team_rows = []
            if cfg["api_version"] == "v2":
                teams_data = _sportsdb_get_v2(f"list/teams/{league_id}")
                team_rows = teams_data.get("teams") or []
                logger.info(
                    "SportsDB teams lookup (v2 list): league_id=%s league_name=%s teams=%s",
                    league_id,
                    league_row["league_name"],
                    len(team_rows),
                )
            else:
                league_name = (league_row["league_name"] or "").strip()
                teams_data = _sportsdb_get_v1("search_all_teams.php", params={"l": league_name})
                team_rows = teams_data.get("teams") or []
                if not team_rows and " " in league_name:
                    teams_data = _sportsdb_get_v1(
                        "search_all_teams.php",
                        params={"l": league_name.replace(" ", "_")},
                    )
                    team_rows = teams_data.get("teams") or []
                logger.info(
                    "SportsDB teams lookup (v1 search_all_teams): league_id=%s league_name=%s teams=%s",
                    league_id,
                    league_name,
                    len(team_rows),
                )

            seen_team_ids = set()
            for team in team_rows:
                team_id = (team.get("idTeam") or "").strip()
                team_name = (team.get("strTeam") or "").strip()
                if not team_id or not team_name:
                    continue
                seen_team_ids.add(team_id)
                conn.execute(
                    """
                    INSERT INTO sportsdb_teams_cache
                    (team_id, team_name, team_aliases, league_id, league_name, sport_name, updated_at, raw_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(team_id) DO UPDATE SET
                        team_name = excluded.team_name,
                        team_aliases = excluded.team_aliases,
                        league_id = excluded.league_id,
                        league_name = excluded.league_name,
                        sport_name = excluded.sport_name,
                        updated_at = excluded.updated_at,
                        raw_json = excluded.raw_json
                    """,
                    (
                        team_id,
                        team_name,
                        json.dumps(_extract_team_aliases(team)),
                        league_id,
                        league_row["league_name"],
                        league_row["sport_name"],
                        now,
                        json.dumps(team),
                    ),
                )

            # Remove stale team rows for this league to keep selections accurate after refresh.
            if seen_team_ids:
                placeholders = ",".join(["?"] * len(seen_team_ids))
                conn.execute(
                    f"""
                    DELETE FROM sportsdb_teams_cache
                    WHERE league_id = ?
                      AND team_id NOT IN ({placeholders})
                    """,
                    [league_id, *seen_team_ids],
                )
            logger.info(
                "SportsDB teams cache updated: league_id=%s league_name=%s team_count=%s",
                league_id,
                league_row["league_name"],
                len(seen_team_ids),
            )
            conn.commit()
            return {"ok": True, "refreshed": True}
        except requests.RequestException as exc:
            logger.warning("SportsDB teams refresh failed for league %s: %s", league_id, exc)
            return {"ok": False, "error": str(exc)}
        finally:
            conn.close()

    def _row_to_rule(row):
        return {
            "id": row["id"],
            "name": row["name"],
            "enabled": bool(row["enabled"]),
            "provider": _normalize_provider(row["provider"] if "provider" in row.keys() else "sportsdb"),
            "use_espn_events": bool(row["use_espn_events"]) if "use_espn_events" in row.keys() else False,
            "espn_event_window_hours": row["espn_event_window_hours"] if "espn_event_window_hours" in row.keys() else 72,
            "sport": row["sport"] or "",
            "league_filters": _loads_list(row["league_filters"]),
            "team_filters": _loads_list(row["team_filters"]),
            "channel_groups": _loads_list(row["channel_groups"]),
            "channel_regex": row["channel_regex"] or "",
            "epg_pattern": row["epg_pattern"] or "",
            "extract_regex": row["extract_regex"] or "",
            "output_template": row["output_template"] or "",
            "output_group_name": row["output_group_name"] or "EVENTS",
            "channel_number_start": row["channel_number_start"] if row["channel_number_start"] is not None else 10000,
            "priority": row["priority"] if row["priority"] is not None else 100,
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _compile_regex(value):
        pattern = (value or "").strip()
        if not pattern:
            return None, None
        try:
            return re.compile(pattern, re.IGNORECASE), None
        except re.error as exc:
            return None, str(exc)

    def _filter_channels_for_payload(payload):
        groups = _loads_list(payload.get("groups", []))
        channel_regex = (payload.get("channel_regex") or "").strip()
        regex = None
        if channel_regex:
            regex, err = _compile_regex(channel_regex)
            if err:
                return None, 0, f"invalid channel regex: {err}"

        conn = get_db_connection()
        try:
            rows = conn.execute(
                """
                SELECT
                    c.portal_id,
                    c.portal_name,
                    c.channel_id,
                    c.name,
                    c.custom_name,
                    c.auto_name,
                    c.matched_name,
                    c.custom_epg_id,
                    COALESCE(NULLIF(c.custom_name, ''), NULLIF(c.display_name, ''), NULLIF(c.auto_name, ''), c.name) AS channel_name,
                    COALESCE(NULLIF(c.custom_genre, ''), NULLIF(c.genre, ''), 'UNGROUPED') AS group_name
                FROM channels c
                WHERE c.enabled = 1
                ORDER BY c.portal_name COLLATE NOCASE, channel_name COLLATE NOCASE
                """
            ).fetchall()
        finally:
            conn.close()

        filtered = []
        group_name_set = set()
        group_token_set = set()
        for value in groups:
            raw = str(value).strip()
            if not raw:
                continue
            if "::" in raw:
                group_token_set.add(raw)
                continue
            group_name_set.add(raw)

        for row in rows:
            group_name = row["group_name"] or "UNGROUPED"
            channel_name = row["channel_name"] or ""
            row_token = f"{row['portal_id']}::{group_name}"
            if (group_name_set or group_token_set) and (
                group_name not in group_name_set and row_token not in group_token_set
            ):
                continue
            if regex and not regex.search(channel_name):
                continue
            filtered.append(row)
        return filtered, len(rows), None

    def _expand_team_filters(provider, team_filters):
        if not team_filters:
            return set()
        provider = _normalize_provider(provider)
        raw_filters = {str(v).strip().lower() for v in team_filters if str(v).strip()}
        if not raw_filters:
            return set()

        conn = get_db_connection()
        try:
            expanded = set(raw_filters)
            if provider == "espn":
                placeholders = ",".join(["?"] * len(raw_filters))
                like_params = []
                like_clauses = []
                for name in raw_filters:
                    like_clauses.append("team_aliases LIKE ?")
                    like_params.append(f"%{name}%")
                where = "team_name IN ({})".format(placeholders)
                if like_clauses:
                    where = f"({where} OR {' OR '.join(like_clauses)})"
                rows = conn.execute(
                    f"""
                    SELECT team_name, team_aliases
                    FROM espn_teams_cache
                    WHERE {where}
                    """,
                    [*raw_filters, *like_params],
                ).fetchall()
                for row in rows:
                    expanded.add((row["team_name"] or "").strip().lower())
                    for alias in _loads_list(row["team_aliases"]):
                        if str(alias).strip():
                            expanded.add(str(alias).strip().lower())
            else:
                placeholders = ",".join(["?"] * len(raw_filters))
                like_params = []
                like_clauses = []
                for name in raw_filters:
                    like_clauses.append("team_aliases LIKE ?")
                    like_params.append(f"%{name}%")
                where = "team_name IN ({})".format(placeholders)
                if like_clauses:
                    where = f"({where} OR {' OR '.join(like_clauses)})"
                rows = conn.execute(
                    f"""
                    SELECT team_name, team_aliases
                    FROM sportsdb_teams_cache
                    WHERE {where}
                    """,
                    [*raw_filters, *like_params],
                ).fetchall()
                for row in rows:
                    expanded.add((row["team_name"] or "").strip().lower())
                    for alias in _loads_list(row["team_aliases"]):
                        if str(alias).strip():
                            expanded.add(str(alias).strip().lower())
            return expanded
        finally:
            conn.close()

    @bp.route("/events", methods=["GET"])
    @authorise
    def events_page():
        return render_template("events.html", settings=getSettings())

    @bp.route("/api/events/rules", methods=["GET"])
    @authorise
    def list_rules():
        conn = get_db_connection()
        try:
            rows = conn.execute(
                "SELECT * FROM event_rules ORDER BY priority ASC, id ASC"
            ).fetchall()
            return jsonify({"ok": True, "rules": [_row_to_rule(r) for r in rows]})
        finally:
            conn.close()

    @bp.route("/api/events/rules", methods=["POST"])
    @authorise
    def create_rule():
        payload = request.get_json(silent=True) or {}
        rule = _serialize_rule_payload(payload.get("rule") or {})
        if not rule["name"]:
            return jsonify({"ok": False, "error": "name is required"}), 400

        now = datetime.utcnow().isoformat()
        conn = get_db_connection()
        try:
            cur = conn.execute(
                """
                INSERT INTO event_rules
                (name, enabled, provider, use_espn_events, espn_event_window_hours, sport, league_filters, team_filters, channel_groups, channel_regex,
                 epg_pattern, extract_regex, output_template, output_group_name, channel_number_start,
                 priority, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    rule["name"],
                    rule["enabled"],
                    _normalize_provider(rule["provider"]),
                    rule["use_espn_events"],
                    rule["espn_event_window_hours"],
                    rule["sport"],
                    rule["league_filters"],
                    rule["team_filters"],
                    rule["channel_groups"],
                    rule["channel_regex"],
                    rule["epg_pattern"],
                    rule["extract_regex"],
                    rule["output_template"],
                    rule["output_group_name"],
                    rule["channel_number_start"],
                    rule["priority"],
                    now,
                    now,
                ),
            )
            conn.commit()
            return jsonify({"ok": True, "id": cur.lastrowid})
        finally:
            conn.close()

    @bp.route("/api/events/rules/<int:rule_id>", methods=["PUT"])
    @authorise
    def update_rule(rule_id):
        payload = request.get_json(silent=True) or {}
        rule = _serialize_rule_payload(payload.get("rule") or {})
        if not rule["name"]:
            return jsonify({"ok": False, "error": "name is required"}), 400
        now = datetime.utcnow().isoformat()
        conn = get_db_connection()
        try:
            cur = conn.execute(
                """
                UPDATE event_rules
                SET name = ?, enabled = ?, provider = ?, use_espn_events = ?, espn_event_window_hours = ?, sport = ?, league_filters = ?, team_filters = ?,
                    channel_groups = ?, channel_regex = ?, epg_pattern = ?, extract_regex = ?,
                    output_template = ?, output_group_name = ?, channel_number_start = ?,
                    priority = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    rule["name"],
                    rule["enabled"],
                    _normalize_provider(rule["provider"]),
                    rule["use_espn_events"],
                    rule["espn_event_window_hours"],
                    rule["sport"],
                    rule["league_filters"],
                    rule["team_filters"],
                    rule["channel_groups"],
                    rule["channel_regex"],
                    rule["epg_pattern"],
                    rule["extract_regex"],
                    rule["output_template"],
                    rule["output_group_name"],
                    rule["channel_number_start"],
                    rule["priority"],
                    now,
                    rule_id,
                ),
            )
            updated_channels = _update_generated_channels_for_rule(
                conn.cursor(), rule_id, rule["output_template"], rule["output_group_name"]
            )
            conn.commit()
            if cur.rowcount == 0:
                return jsonify({"ok": False, "error": "rule not found"}), 404
            return jsonify({"ok": True, "updated_channels": updated_channels})
        finally:
            conn.close()

    @bp.route("/api/events/rules/<int:rule_id>", methods=["DELETE"])
    @authorise
    def delete_rule(rule_id):
        conn = get_db_connection()
        try:
            cur = conn.execute("DELETE FROM event_rules WHERE id = ?", (rule_id,))
            conn.commit()
            if cur.rowcount == 0:
                return jsonify({"ok": False, "error": "rule not found"}), 404
            return jsonify({"ok": True})
        finally:
            conn.close()

    @bp.route("/api/events/groups", methods=["GET"])
    @authorise
    def list_groups():
        conn = get_db_connection()
        try:
            rows = conn.execute(
                """
                SELECT DISTINCT name
                FROM groups
                WHERE name IS NOT NULL AND TRIM(name) <> ''
                ORDER BY name COLLATE NOCASE
                """
            ).fetchall()
            return jsonify({"ok": True, "groups": [r["name"] for r in rows]})
        finally:
            conn.close()

    @bp.route("/api/events/groups/detailed", methods=["GET"])
    @authorise
    def list_groups_detailed():
        conn = get_db_connection()
        try:
            rows = conn.execute(
                """
                SELECT
                    g.portal_id,
                    COALESCE(ps.portal_name, c.portal_name, g.portal_id) AS portal_name,
                    g.name AS group_name,
                    COALESCE(g.channel_count, 0) AS channel_count,
                    COALESCE(g.active, 1) AS active
                FROM groups g
                LEFT JOIN portal_stats ps ON ps.portal_id = g.portal_id
                LEFT JOIN (
                    SELECT portal_id, MAX(portal_name) AS portal_name
                    FROM channels
                    GROUP BY portal_id
                ) c ON c.portal_id = g.portal_id
                WHERE g.name IS NOT NULL AND TRIM(g.name) <> ''
                ORDER BY portal_name COLLATE NOCASE, group_name COLLATE NOCASE
                """
            ).fetchall()
            groups = [
                {
                    "portal_id": row["portal_id"],
                    "portal_name": row["portal_name"],
                    "group_name": row["group_name"],
                    "channel_count": int(row["channel_count"] or 0),
                    "active": bool(row["active"]),
                    "token": f"{row['portal_id']}::{row['group_name']}",
                }
                for row in rows
            ]
            return jsonify({"ok": True, "groups": groups})
        finally:
            conn.close()

    @bp.route("/api/events/sportsdb/refresh", methods=["POST"])
    @authorise
    def refresh_sportsdb_cache():
        payload = request.get_json(silent=True) or {}
        force = bool(payload.get("force", False))
        result = _refresh_sports_and_leagues(force=force)
        code = 200 if result.get("ok") else 400
        return jsonify(result), code

    @bp.route("/api/events/sportsdb/sports", methods=["GET"])
    @authorise
    def sportsdb_sports():
        _refresh_sports_and_leagues(force=False)
        cfg = _sportsdb_config()
        conn = get_db_connection()
        try:
            rows = conn.execute(
                "SELECT sport_name, sport_id FROM sportsdb_sports_cache ORDER BY sport_name COLLATE NOCASE"
            ).fetchall()
            sports = []
            for row in rows:
                if cfg["sports"] and row["sport_name"] not in cfg["sports"]:
                    continue
                # Use sport_name as id/key so it matches league.sport in this provider.
                sports.append({"name": row["sport_name"], "id": row["sport_name"]})
            return jsonify({"ok": True, "sports": sports})
        finally:
            conn.close()

    @bp.route("/api/events/sportsdb/leagues", methods=["GET"])
    @authorise
    def sportsdb_leagues():
        _refresh_sports_and_leagues(force=False)
        sport = (request.args.get("sport") or "").strip()
        cfg = _sportsdb_config()
        conn = get_db_connection()
        try:
            query = """
                SELECT league_id, league_name, sport_name
                FROM sportsdb_leagues_cache
                WHERE 1=1
            """
            params = []
            if sport:
                query += " AND sport_name = ?"
                params.append(sport)
            query += " ORDER BY league_name COLLATE NOCASE"
            rows = conn.execute(query, params).fetchall()
            leagues = []
            for row in rows:
                if cfg["sports"] and row["sport_name"] not in cfg["sports"]:
                    continue
                leagues.append(
                    {
                        "id": row["league_id"],
                        "name": row["league_name"],
                        "sport": row["sport_name"],
                    }
                )
            return jsonify({"ok": True, "leagues": leagues})
        finally:
            conn.close()

    @bp.route("/api/events/sportsdb/teams", methods=["GET"])
    @authorise
    def sportsdb_teams():
        league_id = (request.args.get("league_id") or "").strip()
        force = str(request.args.get("force", "")).lower() in ("1", "true", "yes")
        if not league_id:
            return jsonify({"ok": False, "error": "league_id is required"}), 400

        logger.info("SportsDB teams API called: league_id=%s force=%s", league_id, force)

        refresh_result = _refresh_teams_for_league(league_id, force=force)
        if not refresh_result.get("ok"):
            return jsonify(refresh_result), 400

        conn = get_db_connection()
        try:
            rows = conn.execute(
                """
                SELECT team_id, team_name, league_id, league_name, sport_name
                     , team_aliases
                FROM sportsdb_teams_cache
                WHERE league_id = ?
                ORDER BY team_name COLLATE NOCASE
                """,
                (league_id,),
            ).fetchall()
            teams = [
                {
                    "id": row["team_id"],
                    "name": row["team_name"],
                    "league_id": row["league_id"],
                    "league_name": row["league_name"],
                    "sport": row["sport_name"],
                    "aliases": _loads_list(row["team_aliases"]),
                }
                for row in rows
            ]
            logger.info(
                "SportsDB teams API result: league_id=%s refreshed=%s returned_teams=%s",
                league_id,
                refresh_result.get("refreshed", False),
                len(teams),
            )
            return jsonify({"ok": True, "teams": teams, "refreshed": refresh_result.get("refreshed", False)})
        finally:
            conn.close()

    @bp.route("/api/events/espn/refresh", methods=["POST"])
    @authorise
    def refresh_espn_cache():
        payload = request.get_json(silent=True) or {}
        force = bool(payload.get("force", False))
        result = _refresh_espn_catalog(force=force)
        code = 200 if result.get("ok") else 400
        return jsonify(result), code

    @bp.route("/api/events/espn/sports", methods=["GET"])
    @authorise
    def espn_sports():
        _refresh_espn_catalog(force=False)
        cfg = _espn_config()
        conn = get_db_connection()
        try:
            rows = conn.execute(
                "SELECT sport_key, sport_name FROM espn_sports_cache ORDER BY sport_name COLLATE NOCASE"
            ).fetchall()
            sports = []
            for row in rows:
                if cfg["sports"] and row["sport_key"] not in cfg["sports"]:
                    continue
                sports.append({"id": row["sport_key"], "name": row["sport_name"]})
            return jsonify({"ok": True, "sports": sports})
        finally:
            conn.close()

    @bp.route("/api/events/espn/leagues", methods=["GET"])
    @authorise
    def espn_leagues():
        _refresh_espn_catalog(force=False)
        sport = (request.args.get("sport") or "").strip()
        cfg = _espn_config()
        conn = get_db_connection()
        try:
            query = """
                SELECT league_key, league_name, sport_key
                FROM espn_leagues_cache
                WHERE 1=1
            """
            params = []
            if sport:
                query += " AND sport_key = ?"
                params.append(sport)
            query += " ORDER BY league_name COLLATE NOCASE"
            rows = conn.execute(query, params).fetchall()
            leagues = []
            for row in rows:
                if cfg["sports"] and row["sport_key"] not in cfg["sports"]:
                    continue
                leagues.append(
                    {"id": row["league_key"], "name": row["league_name"], "sport": row["sport_key"]}
                )
            return jsonify({"ok": True, "leagues": leagues})
        finally:
            conn.close()

    @bp.route("/api/events/espn/teams", methods=["GET"])
    @authorise
    def espn_teams():
        sport = (request.args.get("sport") or "").strip()
        league_id = (request.args.get("league_id") or "").strip()
        force = str(request.args.get("force", "")).lower() in ("1", "true", "yes")
        if not sport or not league_id:
            return jsonify({"ok": False, "error": "sport and league_id are required"}), 400

        logger.info("ESPN teams API called: sport=%s league_id=%s force=%s", sport, league_id, force)
        refresh_result = _refresh_espn_teams_for_league(sport, league_id, force=force)
        if not refresh_result.get("ok"):
            return jsonify(refresh_result), 400

        conn = get_db_connection()
        try:
            rows = conn.execute(
                """
                SELECT team_id, team_name, team_aliases, sport_key, league_key, league_name
                FROM espn_teams_cache
                WHERE league_key = ?
                ORDER BY team_name COLLATE NOCASE
                """,
                (league_id,),
            ).fetchall()
            teams = [
                {
                    "id": row["team_id"],
                    "name": row["team_name"],
                    "league_id": row["league_key"],
                    "league_name": row["league_name"],
                    "sport": row["sport_key"],
                    "aliases": _loads_list(row["team_aliases"]),
                }
                for row in rows
            ]
            logger.info(
                "ESPN teams API result: sport=%s league_id=%s refreshed=%s returned_teams=%s",
                sport,
                league_id,
                refresh_result.get("refreshed", False),
                len(teams),
            )
            return jsonify({"ok": True, "teams": teams, "refreshed": refresh_result.get("refreshed", False)})
        finally:
            conn.close()

    @bp.route("/api/events/preview/channels", methods=["POST"])
    @authorise
    def preview_channels():
        payload = request.get_json(silent=True) or {}
        filtered_rows, total_rows, err = _filter_channels_for_payload(payload)
        if err:
            return jsonify({"ok": False, "error": err}), 400
        filtered = [
            {
                "portal_id": row["portal_id"],
                "portal_name": row["portal_name"],
                "channel_id": row["channel_id"],
                "channel_name": row["channel_name"] or "",
                "group_name": row["group_name"] or "UNGROUPED",
            }
            for row in filtered_rows
        ]

        return jsonify(
            {
                "ok": True,
                "total_channels": total_rows,
                "matched_channels": len(filtered),
                "channels": filtered[:200],
                "truncated": len(filtered) > 200,
            }
        )

    @bp.route("/api/events/preview/espn_event", methods=["POST"])
    @authorise
    def preview_espn_event():
        payload = request.get_json(silent=True) or {}
        sport_key = (payload.get("sport") or "").strip()
        league_filters = payload.get("league_filters") or []
        try:
            espn_window_hours = int(payload.get("espn_event_window_hours", 72) or 72)
        except (TypeError, ValueError):
            espn_window_hours = 72
        if not sport_key or not league_filters:
            return jsonify({"ok": False, "error": "sport + league required"}), 400
        league_key = str(league_filters[0] or "").strip()
        if not league_key:
            return jsonify({"ok": False, "error": "league required"}), 400
        try:
            events = _fetch_espn_upcoming_events(sport_key, league_key, espn_window_hours)
        except Exception as exc:
            logger.error("ESPN preview error: %s", exc)
            return jsonify({"ok": False, "error": "ESPN preview failed"}), 500
        if not events:
            return jsonify({"ok": True, "event": None})
        event = events[0]
        alias_map = {}
        try:
            conn = get_db_connection()
            alias_map = _build_alias_abbrev_map(conn.cursor())
        except Exception:
            alias_map = {}
        finally:
            try:
                conn.close()
            except Exception:
                pass
        home = event.get("home") or ""
        away = event.get("away") or ""
        home_abbr = alias_map.get(_normalize_alias(home), home)
        away_abbr = alias_map.get(_normalize_alias(away), away)
        return jsonify(
            {
                "ok": True,
                "event": {
                    "event_id": event.get("event_id"),
                    "start": event["start_dt"].isoformat() if event.get("start_dt") else "",
                    "home": home,
                    "away": away,
                    "home_abbr": home_abbr,
                    "away_abbr": away_abbr,
                    "sport": event.get("sport") or "",
                    "league": event.get("league") or "",
                },
            }
        )

    @bp.route("/api/events/preview/programmes", methods=["POST"])
    @authorise
    def preview_programmes():
        payload = request.get_json(silent=True) or {}
        provider = _normalize_provider(payload.get("provider") or "sportsdb")
        use_espn_events = bool(payload.get("use_espn_events")) and provider == "espn"
        try:
            espn_window_hours = int(payload.get("espn_event_window_hours", 72) or 72)
        except (TypeError, ValueError):
            espn_window_hours = 72
        filtered_rows, _, err = _filter_channels_for_payload(payload)
        if err:
            return jsonify({"ok": False, "error": err}), 400

        epg_pattern = (payload.get("epg_pattern") or "").strip()
        extract_regex = (payload.get("extract_regex") or "").strip()
        team_filters = _expand_team_filters(provider, _loads_list(payload.get("team_filters", [])))
        league_filters = {str(v).strip().lower() for v in _loads_list(payload.get("league_filters", [])) if str(v).strip()}

        epg_re = None
        if epg_pattern:
            epg_re, err = _compile_regex(epg_pattern)
            if err:
                return jsonify({"ok": False, "error": f"invalid epg pattern: {err}"}), 400
        extract_re = None
        if extract_regex:
            extract_re, err = _compile_regex(extract_regex)
            if err:
                return jsonify({"ok": False, "error": f"invalid extract regex: {err}"}), 400

        now = datetime.now(timezone.utc)
        settings = getSettings()
        debug_match = str(settings.get("events match debug", "false")).lower() == "true"
        try:
            past_h = int(settings.get("epg past hours", "2"))
            future_h = int(settings.get("epg future hours", "24"))
            match_window_hours = float(settings.get("events match window hours", 6) or 6)
        except (TypeError, ValueError):
            past_h = 2
            future_h = 24
            match_window_hours = 6

        if use_espn_events:
            past_h = 0
            future_h = max(1, espn_window_hours)
        past_cutoff_ts = int((now - timedelta(hours=past_h)).timestamp())
        future_cutoff_ts = int((now + timedelta(hours=future_h)).timestamp())

        espn_events = []
        espn_matchers = []
        espn_match_channels = {}
        espn_match_programs = {}
        espn_replay_channels = {}
        espn_replay_programs = {}
        if use_espn_events:
            sport_key = (payload.get("sport") or "").strip()
            league_list = _loads_list(payload.get("league_filters", []))
            if not sport_key:
                return jsonify({"ok": False, "error": "ESPN events mode requires sport + league selection"}), 400
            raw_league_keys = [str(league_key).strip() for league_key in league_list if str(league_key).strip()]
            league_keys = []
            alias_index = {}
            conn = get_db_connection()
            try:
                if raw_league_keys:
                    rows = conn.execute(
                        """
                        SELECT league_key, league_name
                        FROM espn_leagues_cache
                        WHERE sport_key = ?
                        """,
                        (sport_key,),
                    ).fetchall()
                    by_id = {str(row["league_key"]): row["league_key"] for row in rows}
                    by_name = {str(row["league_name"]).lower(): row["league_key"] for row in rows}
                    for value in raw_league_keys:
                        if value in by_id:
                            league_keys.append(value)
                            continue
                        key = value.lower()
                        if key in by_name:
                            league_keys.append(str(by_name[key]))
                if not league_keys:
                    rows = conn.execute(
                        """
                        SELECT league_key
                        FROM espn_leagues_cache
                        WHERE sport_key = ?
                        """,
                        (sport_key,),
                    ).fetchall()
                    league_keys = [str(row["league_key"]) for row in rows]
                if not league_keys:
                    return jsonify({"ok": False, "error": "ESPN events mode requires sport + league selection"}), 400
                if league_keys:
                    placeholders = ",".join(["?"] * len(league_keys))
                    rows = conn.execute(
                        f"""
                        SELECT team_name, team_aliases
                        FROM espn_teams_cache
                        WHERE league_key IN ({placeholders})
                        """,
                        league_keys,
                    ).fetchall()
                    for row in rows:
                        name = str(row["team_name"] or "").strip()
                        aliases = []
                        if row["team_aliases"]:
                            try:
                                parsed = json.loads(row["team_aliases"])
                                if isinstance(parsed, list):
                                    aliases = [str(item).strip() for item in parsed if str(item).strip()]
                            except Exception:
                                aliases = []
                        if name:
                            aliases.append(name)
                        deduped = []
                        seen = set()
                        for alias in aliases:
                            key = alias.lower()
                            if key in seen:
                                continue
                            seen.add(key)
                            deduped.append(alias)
                        if name:
                            alias_index[name.lower()] = deduped
            finally:
                conn.close()

            def _normalize_text(value):
                text = str(value or "").lower()
                text = (
                    text.replace("ä", "ae")
                    .replace("ö", "oe")
                    .replace("ü", "ue")
                    .replace("ß", "ss")
                )
                translations = {
                    "cologne": "koln",
                    "koeln": "koln",
                    "munich": "munchen",
                    "nuremberg": "nurnberg",
                }
                for src, dst in translations.items():
                    text = text.replace(src, dst)
                cleaned = []
                last_space = False
                for ch in text:
                    if ch.isalnum():
                        cleaned.append(ch)
                        last_space = False
                    else:
                        if not last_space:
                            cleaned.append(" ")
                            last_space = True
                return " ".join("".join(cleaned).split())

            _stop_tokens = {"fc", "sc", "sv", "vfb", "vfl", "tsg", "rb", "fsv", "1", "2", "3", "04", "05", "07", "09", "1860", "1899"}

            def _tokenize_name(value):
                tokens = _normalize_text(value).split()
                return {t for t in tokens if len(t) >= 3 and t not in _stop_tokens}

            def _resolve_aliases(name):
                raw = str(name or "").strip()
                if not raw:
                    return []
                key = raw.lower()
                if key in alias_index:
                    return alias_index[key]
                for candidate_key, candidate_aliases in alias_index.items():
                    if key in candidate_key or candidate_key in key:
                        return candidate_aliases
                return [raw]

            for league_key in league_keys:
                try:
                    espn_events.extend(
                        _fetch_espn_upcoming_events(sport_key, league_key, espn_window_hours)
                    )
                except requests.RequestException as exc:
                    logger.warning("ESPN scoreboard fetch failed: %s", exc)
            if not espn_events:
                return jsonify(
                    {
                        "ok": True,
                        "matched_events": 0,
                        "events": [],
                        "espn_events": [],
                        "truncated": False,
                    }
                )

            for ev in espn_events:
                event_id = str(ev.get("event_id") or "").strip() or None
                ev["event_id"] = event_id
                home_aliases = _resolve_aliases(ev.get("home"))
                away_aliases = _resolve_aliases(ev.get("away"))
                ev["_home_aliases"] = [_normalize_text(alias) for alias in home_aliases if alias]
                ev["_away_aliases"] = [_normalize_text(alias) for alias in away_aliases if alias]
                ev["_home_tokens"] = [_tokenize_name(alias) for alias in home_aliases if alias]
                ev["_away_tokens"] = [_tokenize_name(alias) for alias in away_aliases if alias]
                if event_id:
                    espn_match_channels.setdefault(event_id, set())
                    espn_match_programs.setdefault(event_id, {})
                    espn_replay_channels.setdefault(event_id, set())
                    espn_replay_programs.setdefault(event_id, {})
                espn_matchers.append(ev)

        source_name_map = {}
        epg_channel_sources = {}
        channel_lookup = {}
        conn = get_db_connection()
        try:
            for row in conn.execute("SELECT source_id, name FROM epg_sources").fetchall():
                source_name_map[row["source_id"]] = row["name"] or row["source_id"]

            epg_ids = set()
            for row in filtered_rows:
                epg_id = row["custom_epg_id"] or effective_epg_name(
                    row["custom_name"], row["auto_name"], row["name"]
                )
                if not epg_id:
                    continue
                epg_ids.add(epg_id)
                channel_key = f"{row['portal_id']}::{epg_id}"
                channel_lookup[channel_key] = row

            if epg_ids:
                ids = list(epg_ids)
                chunk_size = 900
                for i in range(0, len(ids), chunk_size):
                    chunk = ids[i:i + chunk_size]
                    placeholders = ",".join(["?"] * len(chunk))
                    rows = conn.execute(
                        f"""
                        SELECT source_id, channel_id
                        FROM epg_channels
                        WHERE channel_id IN ({placeholders})
                        """,
                        chunk,
                    ).fetchall()
                    for row in rows:
                        epg_channel_sources.setdefault(row["channel_id"], []).append(row["source_id"])
        finally:
            conn.close()

        source_channel_ids = {}
        for channel_key, channel in channel_lookup.items():
            if "::" in channel_key:
                _, epg_id = channel_key.split("::", 1)
            else:
                epg_id = channel_key
            source_ids = epg_channel_sources.get(epg_id, [])
            owner_portal = channel["portal_id"]
            source_id = None
            if owner_portal in source_ids:
                source_id = owner_portal
            elif source_ids:
                source_id = source_ids[0]
            else:
                source_id = owner_portal
            source_channel_ids.setdefault(source_id, {}).setdefault(epg_id, []).append(channel_key)

        events = []
        debug_samples = []
        debug_seen = 0
        debug_matches = 0
        for source_id, channel_ids in source_channel_ids.items():
            db = open_epg_source_db(source_id)
            if db is None:
                continue
            try:
                cursor = db.cursor()
                id_list = list(channel_ids.keys())
                chunk_size = 900
                for i in range(0, len(id_list), chunk_size):
                    chunk = id_list[i:i + chunk_size]
                    placeholders = ",".join(["?"] * len(chunk))
                    rows = cursor.execute(
                        f"""
                        SELECT channel_id, start_ts, stop_ts, title, description, sub_title, categories
                        FROM epg_programmes
                        WHERE channel_id IN ({placeholders})
                          AND stop_ts >= ?
                          AND start_ts <= ?
                        ORDER BY start_ts ASC
                        """,
                        [*chunk, past_cutoff_ts, future_cutoff_ts],
                    ).fetchall()
                    for row in rows:
                        title = row["title"] or ""
                        description = row["description"] or ""
                        sub_title = row["sub_title"] or ""
                        categories_raw = row["categories"] or ""
                        categories_text = ""
                        if categories_raw:
                            try:
                                parsed_categories = json.loads(categories_raw)
                                if isinstance(parsed_categories, list):
                                    categories_text = " ".join(
                                        str(item).strip()
                                        for item in parsed_categories
                                        if str(item).strip()
                                    )
                                else:
                                    categories_text = str(parsed_categories).strip()
                            except Exception:
                                categories_text = str(categories_raw).strip()

                        text = "\n".join(
                            part for part in [title, sub_title, description, categories_text] if part
                        )
                        if epg_re and not epg_re.search(text):
                            continue

                        home = ""
                        away = ""
                        if extract_re:
                            match = extract_re.search(text)
                            if not match:
                                continue
                            if "home" in match.groupdict():
                                home = (match.group("home") or "").strip()
                            if "away" in match.groupdict():
                                away = (match.group("away") or "").strip()
                            if not home and match.lastindex and match.lastindex >= 1:
                                home = (match.group(1) or "").strip()
                            if not away and match.lastindex and match.lastindex >= 2:
                                away = (match.group(2) or "").strip()

                        lower_text = text.lower()
                        normalized_text = _normalize_text(text)
                        text_tokens = _tokenize_name(text)
                        if team_filters or use_espn_events:
                            checks = {lower_text}
                            if home:
                                checks.add(home.lower())
                            if away:
                                checks.add(away.lower())

                            team_matched = False
                            if use_espn_events:
                                for ev in espn_matchers:
                                    ev_home = ev.get("_home_aliases") or []
                                    ev_away = ev.get("_away_aliases") or []
                                    ev_home_tokens = ev.get("_home_tokens") or []
                                    ev_away_tokens = ev.get("_away_tokens") or []
                                    if not ev_home and not ev_away:
                                        continue
                                    home_ok = any(tokens and tokens.issubset(text_tokens) for tokens in ev_home_tokens) or any(
                                        alias and alias in normalized_text for alias in ev_home
                                    )
                                    away_ok = any(tokens and tokens.issubset(text_tokens) for tokens in ev_away_tokens) or any(
                                        alias and alias in normalized_text for alias in ev_away
                                    )
                                    if home_ok and away_ok:
                                        team_matched = True
                                        event_id = ev.get("event_id")
                                        if event_id and event_id in espn_match_channels:
                                            channel_keys = channel_ids.get(row["channel_id"], [])
                                            event_start = ev.get("start_dt")
                                            is_replay = False
                                            if event_start:
                                                delta = abs((event_start - datetime.fromtimestamp(row["start_ts"], tz=timezone.utc)).total_seconds())
                                                is_replay = delta > (match_window_hours * 3600)
                                            for channel_key in channel_keys:
                                                if is_replay:
                                                    espn_replay_channels[event_id].add(channel_key)
                                                    program_map = espn_replay_programs.get(event_id)
                                                else:
                                                    espn_match_channels[event_id].add(channel_key)
                                                    program_map = espn_match_programs.get(event_id)
                                                if program_map is not None and channel_key not in program_map:
                                                    program_map[channel_key] = {
                                                        "title": title,
                                                        "start_ts": row["start_ts"],
                                                        "stop_ts": row["stop_ts"],
                                                        "description": description,
                                                        "sub_title": sub_title,
                                                    }
                                        break
                            else:
                                team_matched = any(any(team in item for item in checks) for team in team_filters)

                            if not team_matched:
                                if debug_match and debug_seen < 20:
                                    debug_samples.append(
                                        {
                                            "channel": row["channel_id"],
                                            "title": title,
                                            "sub_title": sub_title,
                                            "description": description[:180] if description else "",
                                            "filters": sorted(list(team_filters))[:10],
                                        }
                                    )
                                    debug_seen += 1
                                continue
                            debug_matches += 1
                        if league_filters and not use_espn_events and not any(league in lower_text for league in league_filters):
                            continue

                        channel_keys = channel_ids.get(row["channel_id"], [])
                        if not channel_keys:
                            continue
                        for channel_key in channel_keys:
                            channel = channel_lookup.get(channel_key)
                            if not channel:
                                continue
                            events.append(
                                {
                                    "source_id": source_id,
                                    "source_name": source_name_map.get(source_id, source_id),
                                    "portal_name": channel["portal_name"] or channel["portal_id"],
                                    "channel_name": channel["channel_name"] or channel["name"] or row["channel_id"],
                                    "channel_id": channel["channel_id"],
                                    "epg_channel_id": row["channel_id"],
                                    "start_ts": row["start_ts"],
                                    "stop_ts": row["stop_ts"],
                                    "start": datetime.fromtimestamp(row["start_ts"], tz=timezone.utc).isoformat(),
                                    "stop": datetime.fromtimestamp(row["stop_ts"], tz=timezone.utc).isoformat(),
                                    "title": title,
                                    "sub_title": sub_title,
                                    "description": description,
                                    "categories": categories_text,
                                    "home": home,
                                    "away": away,
                                }
                            )
            finally:
                db.close()

        events.sort(key=lambda item: item["start_ts"])
        if debug_match and team_filters:
            logger.info(
                "Event match debug: provider=%s team_filters=%s matched=%s samples=%s",
                provider,
                len(team_filters),
                debug_matches,
                len(debug_samples),
            )
            for sample in debug_samples:
                logger.info(
                    "Event match miss: channel=%s title=%s sub_title=%s desc=%s",
                    sample["channel"],
                    sample["title"],
                    sample["sub_title"],
                    sample["description"],
                )
        espn_events_view = []
        if use_espn_events:
            created_map = {}
            if espn_events:
                event_ids = [str(ev.get("event_id")) for ev in espn_events if ev.get("event_id")]
                if event_ids:
                    conn = get_db_connection()
                    try:
                        placeholders = ",".join(["?"] * len(event_ids))
                        rows = conn.execute(
                            f"""
                            SELECT event_id, portal_id, channel_id, source_portal_id, source_channel_id
                            FROM event_generated_channels
                            WHERE event_id IN ({placeholders})
                            """,
                            event_ids,
                        ).fetchall()
                        for row in rows:
                            key = (
                                str(row["event_id"] or ""),
                                str(row["source_portal_id"] or ""),
                                str(row["source_channel_id"] or ""),
                            )
                            created_map[key] = {
                                "portal_id": row["portal_id"],
                                "channel_id": row["channel_id"],
                            }
                    finally:
                        conn.close()
            for ev in espn_events:
                event_id = ev.get("event_id")
                matched_channels = []
                replay_channels = []
                if event_id and event_id in espn_match_channels:
                    grouped = {}
                    for channel_key in sorted(espn_match_channels[event_id]):
                        if "::" in channel_key:
                            portal_id, _ = channel_key.split("::", 1)
                        else:
                            portal_id = ""
                        channel = channel_lookup.get(channel_key)
                        if not channel:
                            continue
                        program_info = espn_match_programs.get(event_id, {}).get(channel_key, {})
                        created_key = (str(event_id or ""), str(channel["portal_id"]), str(channel["channel_id"]))
                        created_entry = created_map.get(created_key)
                        matched_name = str(channel["matched_name"] or "").strip() if "matched_name" in channel.keys() else ""
                        group_key = matched_name.lower() if matched_name else f"{channel['portal_id']}::{channel['channel_id']}"
                        member = {
                            "portal_id": portal_id or channel["portal_id"],
                            "channel_id": channel["channel_id"],
                            "channel_name": channel["channel_name"] or channel["name"] or channel["channel_id"],
                            "portal_name": channel["portal_name"] or channel["portal_id"],
                            "group_name": channel["group_name"] if "group_name" in channel.keys() else "",
                            "program_title": program_info.get("title", ""),
                            "program_sub_title": program_info.get("sub_title", ""),
                            "program_description": program_info.get("description", ""),
                            "program_start": datetime.fromtimestamp(program_info["start_ts"], tz=timezone.utc).isoformat()
                            if program_info.get("start_ts")
                            else "",
                            "created_event_channel_id": created_entry["channel_id"] if created_entry else "",
                            "created_event_portal_id": created_entry["portal_id"] if created_entry else "",
                        }
                        grouped.setdefault(group_key, []).append(member)
                    for group_key, members in grouped.items():
                        members_sorted = sorted(members, key=lambda item: (item.get("portal_name") or "", item.get("channel_name") or ""))
                        primary = members_sorted[0]
                        is_group = len(members_sorted) > 1
                        label = f"{len(members_sorted)} Portale" if is_group else primary["portal_name"]
                        created_count = sum(1 for m in members_sorted if m.get("created_event_channel_id"))
                        matched_channels.append(
                            {
                                **primary,
                                "portal_name": label,
                                "group_key": group_key,
                                "is_group": is_group,
                                "group_members": members_sorted,
                                "group_created": created_count == len(members_sorted),
                                "group_partial": created_count > 0 and created_count < len(members_sorted),
                            }
                        )
                if event_id and event_id in espn_replay_channels:
                    grouped = {}
                    for channel_key in sorted(espn_replay_channels[event_id]):
                        if "::" in channel_key:
                            portal_id, _ = channel_key.split("::", 1)
                        else:
                            portal_id = ""
                        channel = channel_lookup.get(channel_key)
                        if not channel:
                            continue
                        program_info = espn_replay_programs.get(event_id, {}).get(channel_key, {})
                        matched_name = str(channel["matched_name"] or "").strip() if "matched_name" in channel.keys() else ""
                        group_key = matched_name.lower() if matched_name else f"{channel['portal_id']}::{channel['channel_id']}"
                        member = {
                            "portal_id": portal_id or channel["portal_id"],
                            "channel_id": channel["channel_id"],
                            "channel_name": channel["channel_name"] or channel["name"] or channel["channel_id"],
                            "portal_name": channel["portal_name"] or channel["portal_id"],
                            "group_name": channel["group_name"] if "group_name" in channel.keys() else "",
                            "program_title": program_info.get("title", ""),
                            "program_sub_title": program_info.get("sub_title", ""),
                            "program_description": program_info.get("description", ""),
                            "program_start": datetime.fromtimestamp(program_info["start_ts"], tz=timezone.utc).isoformat()
                            if program_info.get("start_ts")
                            else "",
                            "is_replay": True,
                        }
                        grouped.setdefault(group_key, []).append(member)
                    for group_key, members in grouped.items():
                        members_sorted = sorted(members, key=lambda item: (item.get("portal_name") or "", item.get("channel_name") or ""))
                        primary = members_sorted[0]
                        is_group = len(members_sorted) > 1
                        label = f"{len(members_sorted)} Portale" if is_group else primary["portal_name"]
                        replay_channels.append(
                            {
                                **primary,
                                "portal_name": label,
                                "group_key": group_key,
                                "is_group": is_group,
                                "group_members": members_sorted,
                            }
                        )
                matched = len(matched_channels)
                replays = len(replay_channels)
                espn_events_view.append(
                    {
                        "event_id": event_id,
                        "start": ev["start_dt"].isoformat(),
                        "home": ev.get("home") or "",
                        "away": ev.get("away") or "",
                        "sport": ev.get("sport") or "",
                        "league": ev.get("league") or "",
                        "home_score": ev.get("home_score"),
                        "away_score": ev.get("away_score"),
                        "status": ev.get("status") or "",
                        "state": ev.get("state") or "",
                        "finished": (ev.get("state") or "") == "post",
                        "matched_streams": matched,
                        "matched_channels": matched_channels,
                        "replay_streams": replays,
                        "replay_channels": replay_channels,
                    }
                )
            espn_events_view.sort(key=lambda item: item["start"])

        return jsonify(
            {
                "ok": True,
                "matched_events": len(events),
                "events": events[:200],
                "espn_events": espn_events_view,
                "truncated": len(events) > 200,
            }
        )

    @bp.route("/api/events/create_channel", methods=["POST"])
    @authorise
    def create_event_channel():
        payload = request.get_json(silent=True) or {}
        portal_id = (payload.get("portal_id") or "").strip()
        source_channel_id = (payload.get("channel_id") or "").strip()
        event_id = (payload.get("event_id") or "").strip()
        rule_id = payload.get("rule_id")
        home = (payload.get("home") or "").strip()
        away = (payload.get("away") or "").strip()
        sport = (payload.get("sport") or "").strip()
        league = (payload.get("league") or "").strip()
        start_raw = (payload.get("start") or "").strip()
        output_group = (payload.get("output_group_name") or "EVENTS").strip()
        output_template = (payload.get("output_template") or "{home} vs {away} | {date} {time}").strip()
        try:
            channel_number_start = int(payload.get("channel_number_start", 10000) or 10000)
        except (TypeError, ValueError):
            channel_number_start = 10000

        if not portal_id or not source_channel_id or not event_id:
            return jsonify({"ok": False, "error": "portal_id, channel_id, event_id required"}), 400
        if not home and not away:
            return jsonify({"ok": False, "error": "event name missing"}), 400

        start_dt = None
        if start_raw:
            try:
                start_dt = datetime.fromisoformat(start_raw.replace("Z", "+00:00")).astimezone()
            except Exception:
                start_dt = None

        safe_channel_id = re.sub(r"[^a-zA-Z0-9_-]+", "-", f"event-{event_id}-{source_channel_id}").strip("-")

        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            alias_map = _build_alias_abbrev_map(cursor)
            event_name, output_group = _apply_event_template(
                output_template,
                output_group,
                {
                    "home": home,
                    "away": away,
                    "sport": sport,
                    "league": league,
                    "start": start_raw,
                },
                alias_map,
            )
            if not event_name or event_name.strip().lower() in {"vs", "vs |", "|"}:
                event_name = f"Event {event_id}".strip()
            _cleanup_expired_event_channels(cursor)
            cursor.execute(
                "SELECT * FROM channels WHERE portal_id = ? AND channel_id = ?",
                (portal_id, source_channel_id),
            )
            source = cursor.fetchone()
            if not source:
                return jsonify({"ok": False, "error": "source channel not found"}), 404

            cursor.execute(
                """
                SELECT MAX(CAST(custom_number AS INTEGER)) AS max_num
                FROM channels
                WHERE portal_id = ?
                  AND is_event = 1
                  AND custom_number != ''
                  AND CAST(custom_number AS INTEGER) >= ?
                """,
                (portal_id, channel_number_start),
            )
            used_rows = cursor.execute(
                """
                SELECT custom_number
                FROM channels
                WHERE portal_id = ?
                  AND is_event = 1
                  AND custom_genre = ?
                  AND custom_number != ''
                """,
                (portal_id, output_group),
            ).fetchall()
            used_numbers = set()
            for row in used_rows:
                try:
                    used_numbers.add(int(row["custom_number"]))
                except (TypeError, ValueError):
                    continue
            next_num = channel_number_start
            while next_num in used_numbers:
                next_num += 1

            expires_at = (start_dt + timedelta(hours=24)).timestamp() if start_dt else time.time() + 24 * 3600

            event_tags = _split_tags(source["event_tags"])
            misc_tags = _split_tags(source["misc_tags"])

            cursor.execute(
                """
                INSERT INTO channels (
                    portal_id, channel_id, portal_name, name, number, genre, genre_id, logo,
                    custom_name, custom_number, custom_genre, custom_epg_id, enabled, auto_name, display_name,
                    resolution, video_codec, country, event_tags, misc_tags,
                    matched_name, matched_source, matched_station_id, matched_call_sign, matched_logo, matched_score,
                    is_header, is_event, is_raw, available_macs, alternate_ids, cmd, channel_hash
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?
                )
                ON CONFLICT(portal_id, channel_id) DO UPDATE SET
                    name = excluded.name,
                    display_name = excluded.display_name,
                    custom_name = excluded.custom_name,
                    custom_number = excluded.custom_number,
                    custom_genre = excluded.custom_genre,
                    genre = excluded.genre,
                    logo = excluded.logo,
                    enabled = excluded.enabled,
                    is_event = excluded.is_event,
                    event_tags = excluded.event_tags,
                    misc_tags = excluded.misc_tags,
                    cmd = excluded.cmd,
                    available_macs = excluded.available_macs,
                    alternate_ids = excluded.alternate_ids
                """,
                (
                    portal_id,
                    safe_channel_id,
                    source["portal_name"],
                    event_name,
                    source["number"],
                    output_group,
                    source["genre_id"],
                    source["logo"],
                    event_name,
                    str(next_num),
                    output_group,
                    "",
                    1,
                    event_name,
                    event_name,
                    source["resolution"],
                    source["video_codec"],
                    source["country"],
                    source["event_tags"] or "",
                    source["misc_tags"] or "",
                    source["matched_name"],
                    source["matched_source"],
                    source["matched_station_id"],
                    source["matched_call_sign"],
                    source["matched_logo"],
                    source["matched_score"],
                    0,
                    1,
                    0,
                    source["available_macs"],
                    source["alternate_ids"],
                    source["cmd"],
                    "",
                ),
            )
            cursor.execute(
                """
                INSERT INTO event_generated_channels (
                    portal_id, channel_id, event_id, rule_id,
                    source_portal_id, source_channel_id,
                    event_home, event_away, event_start, event_sport, event_league,
                    created_at, expires_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(portal_id, channel_id) DO UPDATE SET
                    event_id = excluded.event_id,
                    rule_id = excluded.rule_id,
                    source_portal_id = excluded.source_portal_id,
                    source_channel_id = excluded.source_channel_id,
                    event_home = excluded.event_home,
                    event_away = excluded.event_away,
                    event_start = excluded.event_start,
                    event_sport = excluded.event_sport,
                    event_league = excluded.event_league,
                    created_at = excluded.created_at,
                    expires_at = excluded.expires_at
                """,
                (
                    portal_id,
                    safe_channel_id,
                    event_id,
                    rule_id,
                    portal_id,
                    source_channel_id,
                    home,
                    away,
                    start_raw,
                    sport,
                    league,
                    time.time(),
                    expires_at,
                ),
            )
            _sync_channel_tags(cursor, portal_id, safe_channel_id, event_tags, misc_tags)
            conn.commit()
        finally:
            conn.close()

        return jsonify({"ok": True, "channel_id": safe_channel_id, "name": event_name})

    @bp.route("/api/events/delete_channel", methods=["POST"])
    @authorise
    def delete_event_channel():
        payload = request.get_json(silent=True) or {}
        portal_id = (payload.get("portal_id") or "").strip()
        channel_id = (payload.get("channel_id") or "").strip()
        if not portal_id or not channel_id:
            return jsonify({"ok": False, "error": "portal_id and channel_id are required"}), 400

        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM channels WHERE portal_id = ? AND channel_id = ?",
                (portal_id, channel_id),
            )
            cursor.execute(
                "DELETE FROM channel_tags WHERE portal_id = ? AND channel_id = ?",
                (portal_id, channel_id),
            )
            cursor.execute(
                "DELETE FROM event_generated_channels WHERE portal_id = ? AND channel_id = ?",
                (portal_id, channel_id),
            )
            conn.commit()
        finally:
            conn.close()

        return jsonify({"ok": True})

    return bp
