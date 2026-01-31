import threading

from flask import Blueprint, jsonify, make_response, request


def create_hdhr_blueprint(
    *,
    host,
    getSettings,
    refresh_lineup,
    get_cached_lineup,
):
    bp = Blueprint("hdhr", __name__)

    def hdhr(f):
        def decorated(*args, **kwargs):
            auth = request.authorization
            settings = getSettings()
            security = settings["enable security"]
            username = settings["username"]
            password = settings["password"]
            hdhrenabled = settings["enable hdhr"]
            if security == "false" or (
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
        data = {
            "BaseURL": host,
            "DeviceAuth": name,
            "DeviceID": device_id,
            "FirmwareName": "MacReplay",
            "FirmwareVersion": "666",
            "FriendlyName": name,
            "LineupURL": host + "/lineup.json",
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
        if not get_cached_lineup():
            refresh_lineup()
        return jsonify(get_cached_lineup())

    @bp.route("/refresh_lineup", methods=["POST"])
    def refresh_lineup_endpoint():
        refresh_lineup()
        return jsonify({"status": "Lineup refreshed successfully"})

    return bp
