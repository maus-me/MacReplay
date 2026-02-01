import os

import flask
from flask import Blueprint, jsonify, redirect, render_template, request

from ..db import cleanup_db
from ..security import authorise


def create_misc_blueprint(*, LOG_DIR, occupied):
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

    @bp.route("/api/db/cleanup", methods=["POST"])
    @authorise
    def db_cleanup():
        payload = request.get_json(silent=True) or {}
        vacuum = bool(payload.get("vacuum"))
        result = cleanup_db(vacuum=vacuum)
        return jsonify({"ok": True, "result": result})

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
            return redirect("/api/settings", code=302)
        if path == "dashboard":
            return redirect("/api/dashboard", code=302)

        try:
            return flask.current_app.send_static_file(f"dist/{path}")
        except Exception:
            return redirect("/api/portals", code=302)

    return bp
