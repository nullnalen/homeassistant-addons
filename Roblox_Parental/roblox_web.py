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

import aiohttp
from flask import Flask, jsonify, request, send_from_directory

from roblox_api import RobloxApiError, RobloxAuthError, RobloxParentalClient
from roblox_poller import RobloxPoller, load_approved, load_state, save_approved

_LOGGER = logging.getLogger(__name__)

OPTIONS_FILE = Path("/data/options.json")
AUTH_FILE = Path("/data/roblox_auth.json")
STATE_FILE = Path("/data/state.json")
APPROVED_FILE = Path("/data/approved_games.json")
WWW_DIR = Path("/usr/bin/www")

app = Flask(__name__, static_folder=str(WWW_DIR))

# Poller-instans deles mellom trådene slik at reload fungerer
_poller: RobloxPoller | None = None
_poller_lock = threading.Lock()


def read_options() -> dict:
    if OPTIONS_FILE.exists():
        try:
            return json.loads(OPTIONS_FILE.read_text())
        except Exception:
            pass
    return {}


def read_auth() -> dict:
    """Les cookie og child_user_ids fra separat fil som HA aldri rører."""
    if AUTH_FILE.exists():
        try:
            return json.loads(AUTH_FILE.read_text())
        except Exception:
            pass
    return {}


def write_auth(cookie: str, child_ids: list[int]) -> None:
    AUTH_FILE.write_text(json.dumps({"roblosecurity_cookie": cookie, "child_user_ids": child_ids}, indent=2))


def is_configured() -> bool:
    auth = read_auth()
    return bool(auth.get("roblosecurity_cookie")) and bool(auth.get("child_user_ids"))


@app.route("/")
def index():
    return send_from_directory(str(WWW_DIR), "index.html")


@app.route("/<path:filename>")
def static_files(filename):
    return send_from_directory(str(WWW_DIR), filename)


# --- Oppsett-API ---

@app.route("/api/setup/status")
def api_setup_status():
    """Forteller frontend om addon er konfigurert."""
    return jsonify({"configured": is_configured()})


@app.route("/api/setup/fetch-children", methods=["POST"])
def api_fetch_children():
    """Valider cookie og hent liste over barn."""
    body = request.get_json(force=True)
    cookie = (body.get("cookie") or "").strip()
    if not cookie:
        return jsonify({"error": "Cookie er påkrevd"}), 400

    async def _fetch():
        client = RobloxParentalClient(cookie)
        try:
            me = await client.authenticate()
            children = await client.get_children()
            return me, children
        finally:
            await client.close()

    try:
        loop = asyncio.new_event_loop()
        me, children = loop.run_until_complete(_fetch())
        loop.close()
    except RobloxAuthError:
        return jsonify({"error": "Cookie er ugyldig eller utløpt"}), 401
    except Exception as e:
        return jsonify({"error": f"Tilkoblingsfeil: {e}"}), 502

    return jsonify({
        "parent": {"id": me.get("id"), "name": me.get("displayName", me.get("name"))},
        "children": [
            {"id": c["userId"], "name": c.get("displayName", c.get("name", str(c["userId"])))}
            for c in children
        ],
    })


@app.route("/api/setup/save", methods=["POST"])
def api_setup_save():
    """Lagre cookie + valgte barn til roblox_auth.json (ikke options.json) og restart poller."""
    body = request.get_json(force=True)
    cookie = (body.get("cookie") or "").strip()
    child_ids = body.get("child_ids")  # liste med int

    if not cookie or not child_ids:
        return jsonify({"error": "cookie og child_ids er påkrevd"}), 400

    child_ids = [int(c) for c in child_ids]

    # Cookie og barn lagres i egen fil som HA aldri overskriver
    write_auth(cookie, child_ids)

    os.environ["ROBLOSECURITY_COOKIE"] = cookie
    os.environ["CHILD_USER_IDS"] = ",".join(str(c) for c in child_ids)
    _restart_poller()

    return jsonify({"ok": True})


# --- REST API ---

@app.route("/api/state")
def api_state():
    """Returnerer full state per barn."""
    if not is_configured():
        return jsonify({"configured": False}), 200

    state = load_state()
    approved = load_approved()
    child_ids = read_auth().get("child_user_ids", [])

    children_data = state.get("children", {})
    presences = state.get("presences", {})
    name_cache = {int(k): v for k, v in state.get("name_cache", {}).items()}

    children_out = []
    for child_id in child_ids:
        key = str(child_id)
        child = children_data.get(key, {})
        presence = presences.get(key, {})

        top_universes = child.get("top_universes", [])
        for game in top_universes:
            uid = game.get("universe_id")
            game["approved"] = uid in approved
            game["status"] = (
                "blocked" if game["blocked"]
                else "approved" if game["approved"]
                else "unknown"
            )

        current_game = None
        if presence.get("in_game") and presence.get("universe_id"):
            uid = presence["universe_id"]
            current_game = {
                "universe_id": uid,
                "name": presence.get("game_name") or name_cache.get(uid, str(uid)),
                "approved": uid in approved,
                "blocked": uid in set(child.get("blocked_universe_ids", [])),
            }
            current_game["status"] = (
                "blocked" if current_game["blocked"]
                else "approved" if current_game["approved"]
                else "unknown"
            )

        friends = state.get("friends", {}).get(str(child_id), [])

        children_out.append({
            "child_id": child_id,
            "display_name": name_cache.get(child_id, f"Barn {child_id}"),
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
            "friends": friends,
        })

    last_slow = state.get("last_slow_update")
    return jsonify({
        "configured": True,
        "auth_error": state.get("auth_error", False),
        "children": children_out,
        "approved_count": len(approved),
        "last_slow_update": last_slow,
        "last_slow_update_ago": int(time.time() - last_slow) if last_slow else None,
    })


@app.route("/api/games/approve", methods=["POST"])
def api_approve():
    body = request.get_json(force=True)
    universe_id = body.get("universe_id")
    if not universe_id:
        return jsonify({"error": "universe_id påkrevd"}), 400
    approved = load_approved()
    approved.add(int(universe_id))
    save_approved(approved)
    return jsonify({"ok": True})


@app.route("/api/games/unapprove", methods=["POST"])
def api_unapprove():
    body = request.get_json(force=True)
    universe_id = body.get("universe_id")
    if not universe_id:
        return jsonify({"error": "universe_id påkrevd"}), 400
    approved = load_approved()
    approved.discard(int(universe_id))
    save_approved(approved)
    return jsonify({"ok": True})


@app.route("/api/games/block", methods=["POST"])
def api_block():
    """Blokker et spill via Roblox foreldrekontroll-API."""
    body = request.get_json(force=True)
    universe_id = body.get("universe_id")
    child_id = body.get("child_id")
    if not universe_id or not child_id:
        return jsonify({"error": "universe_id og child_id påkrevd"}), 400

    cookie = read_auth().get("roblosecurity_cookie", "")
    if not cookie:
        return jsonify({"error": "Ikke konfigurert"}), 400

    async def _block():
        client = RobloxParentalClient(cookie)
        try:
            await client.block_experience(int(child_id), int(universe_id))
        finally:
            await client.close()

    try:
        loop = asyncio.new_event_loop()
        loop.run_until_complete(_block())
        loop.close()
    except RobloxAuthError:
        return jsonify({"error": "Cookie ugyldig eller utløpt"}), 401
    except RobloxApiError as e:
        return jsonify({"error": str(e)}), 502

    # Fjern fra godkjent-liste hvis den var der
    approved = load_approved()
    approved.discard(int(universe_id))
    save_approved(approved)

    _LOGGER.info("Blokkerte spill universe_id=%s for barn %s", universe_id, child_id)
    return jsonify({"ok": True})


@app.route("/api/games/unblock", methods=["POST"])
def api_unblock():
    """Avblokker et spill via Roblox foreldrekontroll-API."""
    body = request.get_json(force=True)
    universe_id = body.get("universe_id")
    child_id = body.get("child_id")
    if not universe_id or not child_id:
        return jsonify({"error": "universe_id og child_id påkrevd"}), 400

    cookie = read_auth().get("roblosecurity_cookie", "")
    if not cookie:
        return jsonify({"error": "Ikke konfigurert"}), 400

    async def _unblock():
        client = RobloxParentalClient(cookie)
        try:
            await client.unblock_experience(int(child_id), int(universe_id))
        finally:
            await client.close()

    try:
        loop = asyncio.new_event_loop()
        loop.run_until_complete(_unblock())
        loop.close()
    except RobloxAuthError:
        return jsonify({"error": "Cookie ugyldig eller utløpt"}), 401
    except RobloxApiError as e:
        return jsonify({"error": str(e)}), 502

    _LOGGER.info("Avblokkerte spill universe_id=%s for barn %s", universe_id, child_id)
    return jsonify({"ok": True})


@app.route("/api/friends/<int:child_id>")
def api_friends(child_id: int):
    """Henter venneliste med navn for et barn."""
    state = load_state()
    friends = state.get("friends", {}).get(str(child_id), [])
    return jsonify({"friends": friends, "child_id": child_id})


@app.route("/api/health")
def api_health():
    state = load_state()
    return jsonify({
        "ok": not state.get("auth_error", False),
        "configured": is_configured(),
        "version": os.environ.get("ADDON_VERSION", "1.0.0"),
    })


# --- Poller-styring ---

def _restart_poller() -> None:
    global _poller
    with _poller_lock:
        if _poller is not None:
            _poller.stop()
        _poller = RobloxPoller()
        t = threading.Thread(target=_run_poller, args=(_poller,), daemon=True, name="roblox-poller")
        t.start()


def _run_poller(poller: RobloxPoller) -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
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

    # Les cookie og barn fra roblox_auth.json (HA rører aldri denne filen)
    auth = read_auth()
    if auth.get("roblosecurity_cookie"):
        os.environ.setdefault("ROBLOSECURITY_COOKIE", auth["roblosecurity_cookie"])
    if auth.get("child_user_ids"):
        os.environ.setdefault("CHILD_USER_IDS", ",".join(str(c) for c in auth["child_user_ids"]))

    if is_configured():
        _restart_poller()
        _LOGGER.info("Poller startet (konfigurert)")
    else:
        _LOGGER.info("Ikke konfigurert ennå — venter på oppsett via webgrensesnitt")

    port = int(os.environ.get("PORT", "8099"))
    _LOGGER.info("Webserver starter på port %d", port)
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
