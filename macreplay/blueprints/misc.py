import os
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

import flask
from flask import Blueprint, Response, jsonify, redirect, render_template, request

from ..config import DATA_DIR
from ..security import authorise


def create_misc_blueprint(*, LOG_DIR, occupied, refresh_custom_sources=None):
    bp = Blueprint("misc", __name__)

    @bp.route("/api/dashboard")
    @authorise
    def dashboard():
        """Legacy template route"""
        return render_template("dashboard.html")

    @bp.route("/streaming")
    @authorise
    def streaming():
        return flask.jsonify(occupied)

    @bp.route("/log")
    @authorise
    def log():
        logFilePath = os.path.join(LOG_DIR, "MacReplay.log")
        try:
            with open(logFilePath) as f:
                return f.read()
        except FileNotFoundError:
            return "Log file not found"

    @bp.route("/logs")
    @authorise
    def logs_page():
        return render_template("logs.html")

    @bp.route("/logs/stream")
    @authorise
    def logs_stream():
        logFilePath = os.path.join(LOG_DIR, "MacReplay.log")
        lines_param = request.args.get("lines", "500")

        try:
            with open(logFilePath, "r", encoding="utf-8", errors="replace") as f:
                all_lines = f.readlines()

            all_lines = [line.rstrip() for line in all_lines if line.strip()]

            if lines_param != "all":
                try:
                    num_lines = int(lines_param)
                    all_lines = all_lines[-num_lines:]
                except ValueError:
                    pass

            return flask.jsonify({"lines": all_lines, "total": len(all_lines)})
        except FileNotFoundError:
            return flask.jsonify({"lines": [], "error": "Log file not found"})
        except Exception as e:
            return flask.jsonify({"lines": [], "error": str(e)})

    @bp.route("/api/epg/source/refresh", methods=["POST"])
    @authorise
    def epg_source_refresh():
        payload = request.get_json(silent=True) or {}
        source_id = (payload.get("id") or "").strip()
        if not source_id:
            return jsonify({"ok": False, "error": "missing id"}), 400
        if not all(ch.isalnum() or ch in ("-", "_") for ch in source_id):
            return jsonify({"ok": False, "error": "invalid id"}), 400

        cache_dir = os.path.join(DATA_DIR, "epg_sources")
        cache_path = os.path.join(cache_dir, f"{source_id}.xml")
        meta_path = cache_path + ".meta"
        for path in (cache_path, meta_path):
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass

        if refresh_custom_sources:
            refresh_custom_sources([source_id])

        return jsonify({"ok": True})

    @bp.route("/api/image-proxy")
    @authorise
    def image_proxy():
        url = (request.args.get("url") or "").strip()
        if not url:
            return jsonify({"ok": False, "error": "missing url"}), 400
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return jsonify({"ok": False, "error": "invalid url"}), 400

        try:
            req = Request(url, headers={"User-Agent": "MacReplay"})
            with urlopen(req, timeout=10) as resp:
                content_type = resp.headers.get("Content-Type", "image/jpeg")
                data = resp.read(2 * 1024 * 1024)
            return Response(data, content_type=content_type)
        except (HTTPError, URLError, OSError, ValueError):
            return jsonify({"ok": False, "error": "fetch failed"}), 502

    @bp.route("/", methods=["GET"])
    def home():
        try:
            return flask.current_app.send_static_file("dist/index.html")
        except Exception:
            return redirect("/api/portals", code=302)

    @bp.route("/<path:path>")
    def catch_all(path):
        if path == "portals":
            return redirect("/api/portals", code=302)
        if path == "editor":
            return redirect("/api/editor", code=302)
        if path == "settings":
            return redirect("/settings", code=302)
        if path == "dashboard":
            return redirect("/api/dashboard", code=302)

        try:
            return flask.current_app.send_static_file(f"dist/{path}")
        except Exception:
            return redirect("/api/portals", code=302)

    return bp
