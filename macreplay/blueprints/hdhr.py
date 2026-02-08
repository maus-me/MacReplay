import threading

from urllib.parse import urlparse

from flask import Blueprint, jsonify, make_response, request


def create_hdhr_blueprint(
    *,
    host,
    getSettings,
    refresh_lineup,
    get_cached_lineup,
):
    bp = Blueprint("hdhr", __name__)

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

    def _base_url():
        forwarded_proto = request.headers.get("X-Forwarded-Proto")
        forwarded_host = request.headers.get("X-Forwarded-Host")
        forwarded_port = request.headers.get("X-Forwarded-Port")
        if forwarded_host:
            if forwarded_port and ":" not in forwarded_host:
                return f"{forwarded_proto or request.scheme}://{forwarded_host}:{forwarded_port}"
            return f"{forwarded_proto or request.scheme}://{forwarded_host}"
        return f"{request.scheme}://{request.host}"

    def hdhr(f):
        def decorated(*args, **kwargs):
            auth = request.authorization
            settings = getSettings()
            security = settings["enable security"]
            username = settings["username"]
            password = settings["password"]
            hdhrenabled = settings["enable hdhr"]
            if not security or (
                auth and auth.username == username and auth.password == password
            ):
                if hdhrenabled:
                    return f(*args, **kwargs)
            return make_response("Error", 404)

        decorated.__name__ = f.__name__
        return decorated

    @bp.route("/discover.json", methods=["GET"])
    @hdhr
    def discover():
        settings = getSettings()
        name = settings["hdhr name"]
        device_id = settings["hdhr id"]
        tuners = settings["hdhr tuners"]
        base_url = _base_url()
        data = {
            "BaseURL": base_url,
            "DeviceAuth": name,
            "DeviceID": device_id,
            "FirmwareName": "MacReplay",
            "FirmwareVersion": "666",
            "FriendlyName": name,
            "LineupURL": base_url + "/lineup.json",
            "Manufacturer": "Evilvirus",
            "ModelNumber": "666",
            "TunerCount": int(tuners),
        }
        return jsonify(data)

    @bp.route("/lineup_status.json", methods=["GET"])
    @hdhr
    def status():
        data = {
            "ScanInProgress": 0,
            "ScanPossible": 0,
            "Source": "Cable",
            "SourceList": ["Cable"],
        }
        return jsonify(data)

    @bp.route("/lineup.json", methods=["GET"])
    @bp.route("/lineup.post", methods=["POST"])
    @hdhr
    def lineup():
        base_url = _base_url()
        cached = get_cached_lineup() or []
        if not cached:
            refresh_lineup(base_url)
            cached = get_cached_lineup() or []
        else:
            sample_url = cached[0].get("URL") if isinstance(cached[0], dict) else ""
            if sample_url and not str(sample_url).startswith(base_url):
                refresh_lineup(base_url)
                cached = get_cached_lineup() or []
        return jsonify(cached)

    @bp.route("/refresh_lineup", methods=["POST"])
    def refresh_lineup_endpoint():
        refresh_lineup(_base_url())
        return jsonify({"status": "Lineup refreshed successfully"})

    return bp
