import logging
from flask import Blueprint, jsonify, redirect, render_template, request, flash

from ..config import defaultSettings, getSettings, saveSettings
from ..security import authorise

logger = logging.getLogger("MacReplay")


def create_settings_blueprint(enqueue_epg_refresh):
    bp = Blueprint("settings", __name__)

    @bp.route("/api/settings", methods=["GET"])
    @authorise
    def settings():
        """Legacy template route"""
        settings = getSettings()
        return render_template(
            "settings.html", settings=settings, defaultSettings=defaultSettings
        )

    @bp.route("/api/settings/data", methods=["GET"])
    @authorise
    def settings_data():
        """API endpoint to get settings"""
        return jsonify(getSettings())

    @bp.route("/settings/save", methods=["POST"])
    @authorise
    def save():
        settings = {}

        for setting, _ in defaultSettings.items():
            value = request.form.get(setting, "false")
            settings[setting] = value

        saveSettings(settings)
        logger.info("Settings saved!")
        enqueue_epg_refresh(reason="settings_save")
        flash("Settings saved!", "success")
        return redirect("/settings", code=302)

    return bp
