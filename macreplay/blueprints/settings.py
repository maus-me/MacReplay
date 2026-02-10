import logging
from flask import Blueprint, jsonify, redirect, render_template, request, flash

from ..config import defaultSettings, getSettings, saveSettings
from ..db import vacuum_channels_db, vacuum_epg_dbs
from ..security import authorise

logger = logging.getLogger("MacReplay")


def create_settings_blueprint(enqueue_epg_refresh):
    bp = Blueprint("settings", __name__)

    @bp.route("/api/settings", methods=["GET"])
    @authorise
    def settings():
        """Legacy template route"""
        return redirect("/settings", code=302)

    @bp.route("/settings", defaults={"section": "general"}, methods=["GET"])
    @bp.route("/settings/<section>", methods=["GET"])
    @authorise
    def settings_page(section):
        settings = getSettings()
        templates = {
            "general": "settings/general.html",
            "epg": "settings/epg.html",
            "channels": "settings/channels.html",
            "database": "settings/database.html",
            "security": "settings/security.html",
            "hdhr": "settings/hdhr.html",
        }
        template_name = templates.get(section, "settings/general.html")
        active_section = section if section in templates else "general"
        return render_template(
            template_name,
            settings=settings,
            defaultSettings=defaultSettings,
            active_section=active_section,
        )

    @bp.route("/api/settings/data", methods=["GET"])
    @authorise
    def settings_data():
        """API endpoint to get settings"""
        return jsonify(getSettings())

    @bp.route("/settings/save", methods=["POST"])
    @authorise
    def save():
        settings = getSettings()

        for setting, _ in defaultSettings.items():
            if setting not in request.form:
                continue
            values = request.form.getlist(setting)
            if values:
                settings[setting] = values[-1]

        saveSettings(settings)
        logger.info("Settings saved!")
        flash("Settings saved!", "success")
        return redirect("/settings", code=302)

    @bp.route("/api/settings/epg_sources", methods=["POST"])
    @authorise
    def save_epg_sources():
        payload = request.get_json(silent=True) or {}
        sources = payload.get("sources", "[]")
        settings = getSettings()
        settings["epg custom sources"] = sources
        saveSettings(settings)
        return jsonify({"ok": True})

    @bp.route("/api/settings/vacuum/channels", methods=["POST"])
    @authorise
    def vacuum_channels():
        try:
            vacuum_channels_db()
            return jsonify({"ok": True})
        except Exception as exc:
            return jsonify({"ok": False, "message": str(exc)}), 500

    @bp.route("/api/settings/vacuum/epg", methods=["POST"])
    @authorise
    def vacuum_epg():
        try:
            count = vacuum_epg_dbs()
            return jsonify({"ok": True, "count": count})
        except Exception as exc:
            return jsonify({"ok": False, "message": str(exc)}), 500

    return bp
