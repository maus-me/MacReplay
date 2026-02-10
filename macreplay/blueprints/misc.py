import os
import sqlite3
import threading
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

import flask
from flask import Blueprint, Response, jsonify, redirect, render_template, request

from ..config import DATA_DIR
from ..security import authorise


def create_misc_blueprint(*, LOG_DIR, occupied, refresh_custom_sources=None, get_epg_source_status=None):
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
            worker = threading.Thread(
                target=refresh_custom_sources,
                args=([source_id],),
                daemon=True,
            )
            worker.start()

        return jsonify({"ok": True})

    @bp.route("/api/epg/source/status")
    @authorise
    def epg_source_status():
        source_id = (request.args.get("id") or "").strip()
        if not source_id:
            return jsonify({"ok": False, "error": "missing id"}), 400
        if get_epg_source_status is None:
            return jsonify({"ok": True, "status": "unknown", "detail": None, "updated_at": None})
        status = get_epg_source_status(source_id) or {}
        return jsonify(
            {
                "ok": True,
                "status": status.get("status", "unknown"),
                "detail": status.get("detail"),
                "updated_at": status.get("updated_at"),
            }
        )

    @bp.route("/api/epg/sources/meta")
    @authorise
    def epg_sources_meta():
        try:
            db_path = os.path.join(DATA_DIR, "channels.db")
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT source_id, source_type, last_fetch, last_refresh
                FROM epg_sources
                """
            ).fetchall()
            conn.close()

            return jsonify(
                {
                    "ok": True,
                    "sources": [
                        {
                            "source_id": row["source_id"],
                            "source_type": row["source_type"],
                            "last_fetch": row["last_fetch"],
                            "last_refresh": row["last_refresh"],
                        }
                        for row in rows
                    ],
                }
            )
        except Exception as exc:
            return jsonify({"ok": False, "error": str(exc)}), 500

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
            placeholder = (
                "<svg xmlns='http://www.w3.org/2000/svg' width='36' height='36' viewBox='0 0 36 36'>"
                "<rect width='36' height='36' rx='6' fill='#252b33'/>"
                "<path d='M11 12h14a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2H11a2 2 0 0 1-2-2v-9a2 2 0 0 1 2-2zm2 14h10'"
                " fill='none' stroke='#8b99a6' stroke-width='2' stroke-linecap='round'/>"
                "</svg>"
            )
            return Response(placeholder, content_type="image/svg+xml")

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
