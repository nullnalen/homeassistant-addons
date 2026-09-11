"""
Webserver for Roblox Foreldrekontroll addon.
Starter polling i bakgrunnen og serverer REST API + statisk webgrensesnitt.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

from roblox_poller import RobloxPoller, load_approved, load_state, save_approved

_LOGGER = logging.getLogger(__name__)

STATE_FILE = Path("/data/state.json")
APPROVED_FILE = Path("/data/approved_games.json")
WWW_DIR = Path("/usr/bin/www")

app = Flask(__name__, static_folder=str(WWW_DIR))

# Ingress path prefix (satt av HA Supervisor)
INGRESS_PATH = os.environ.get("INGRESS_PATH", "")


@app.route("/")
def index():
    return send_from_directory(str(WWW_DIR), "index.html")


@app.route("/<path:filename>")
def static_files(filename):
    return send_from_directory(str(WWW_DIR), filename)


# --- REST API ---

@app.route("/api/state")
def api_state():
    """Returnerer full state: skjermtid, spill, presence, godkjente spill."""
    state = load_state()
    approved = load_approved()
    child = state.get("child", {})
    presence = state.get("presence", {})

    top_universes = child.get("top_universes", [])
    for game in top_universes:
        uid = game.get("universe_id")
        game["approved"] = uid in approved
        if game["blocked"]:
            game["status"] = "blocked"
        elif game["approved"]:
            game["status"] = "approved"
        else:
            game["status"] = "unknown"

    # Aktiv presence med status
    current_game = None
    if presence.get("in_game") and presence.get("universe_id"):
        uid = presence["universe_id"]
        current_game = {
            "universe_id": uid,
            "name": presence.get("game_name") or str(uid),
            "approved": uid in approved,
            "blocked": uid in set(child.get("blocked_universe_ids", [])),
        }
        current_game["status"] = (
            "blocked" if current_game["blocked"]
            else "approved" if current_game["approved"]
            else "unknown"
        )

    last_slow = state.get("last_slow_update")
    last_fast = state.get("last_fast_update")

    return jsonify({
        "auth_error": state.get("auth_error", False),
        "screentime_today": child.get("screentime_today", 0),
        "screentime_week": child.get("screentime_week", 0),
        "daily_limit": child.get("daily_limit"),
        "age_level": child.get("age_level"),
        "daily_data": child.get("daily_data", []),
        "top_universes": top_universes,
        "presence": {
            "online": presence.get("online", False),
            "in_game": presence.get("in_game", False),
            "game_name": presence.get("game_name"),
            "universe_id": presence.get("universe_id"),
        },
        "current_game": current_game,
        "approved_count": len(approved),
        "last_slow_update": last_slow,
        "last_fast_update": last_fast,
        "last_slow_update_ago": int(time.time() - last_slow) if last_slow else None,
    })


@app.route("/api/games/approve", methods=["POST"])
def api_approve():
    """Godkjenn et spill (legg til i approved_games.json)."""
    body = request.get_json(force=True)
    universe_id = body.get("universe_id")
    if not universe_id:
        return jsonify({"error": "universe_id påkrevd"}), 400

    approved = load_approved()
    approved.add(int(universe_id))
    save_approved(approved)
    _LOGGER.info("Godkjente spill %s", universe_id)
    return jsonify({"ok": True, "approved": list(approved)})


@app.route("/api/games/unapprove", methods=["POST"])
def api_unapprove():
    """Fjern godkjenning for et spill."""
    body = request.get_json(force=True)
    universe_id = body.get("universe_id")
    if not universe_id:
        return jsonify({"error": "universe_id påkrevd"}), 400

    approved = load_approved()
    approved.discard(int(universe_id))
    save_approved(approved)
    return jsonify({"ok": True, "approved": list(approved)})


@app.route("/api/games/approved")
def api_approved_list():
    """Returnerer alle godkjente universe_id-er."""
    approved = load_approved()
    state = load_state()
    name_cache = {int(k): v for k, v in state.get("name_cache", {}).items()}
    result = [
        {"universe_id": uid, "name": name_cache.get(uid, str(uid))}
        for uid in sorted(approved)
    ]
    return jsonify({"approved": result})


@app.route("/api/health")
def api_health():
    state = load_state()
    return jsonify({
        "ok": not state.get("auth_error", False),
        "auth_error": state.get("auth_error", False),
        "version": os.environ.get("ADDON_VERSION", "1.0.0"),
    })


def run_poller_thread() -> None:
    """Kjør polling-loop i en egen trå med egen event loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    poller = RobloxPoller()
    try:
        loop.run_until_complete(poller.run())
    except Exception as err:
        _LOGGER.error("Poller krasjet: %s", err)
    finally:
        loop.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Start polling i bakgrunnen
    poller_thread = threading.Thread(target=run_poller_thread, daemon=True, name="roblox-poller")
    poller_thread.start()
    _LOGGER.info("Poller-tråd startet")

    port = int(os.environ.get("PORT", "8099"))
    _LOGGER.info("Webserver starter på port %d", port)
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
