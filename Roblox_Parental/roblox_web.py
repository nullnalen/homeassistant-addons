"""
Webserver for Roblox Foreldrekontroll addon.
Starter polling i bakgrunnen og serverer REST API + statisk webgrensesnitt.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import threading
import time
from pathlib import Path

import aiohttp
import requests
from flask import Flask, Response, jsonify, request, send_from_directory

from roblox_ai import check_ollama_available
from roblox_api import RobloxApiError, RobloxAuthError, RobloxParentalClient
from roblox_poller import RobloxPoller, load_approved, load_state, save_approved

_LOGGER = logging.getLogger(__name__)

OPTIONS_FILE = Path("/data/options.json")
AUTH_FILE = Path("/data/roblox_auth.json")
STATE_FILE = Path("/data/state.json")
APPROVED_FILE = Path("/data/approved_games.json")
IMAGE_CACHE_DIR = Path("/data/image_cache")
WWW_DIR = Path("/usr/bin/www")

IMAGE_CACHE_DIR.mkdir(parents=True, exist_ok=True)

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
    """Les cookie og child_user_ids fra separat fil som HA aldri rører.
    Migrerer automatisk fra options.json hvis roblox_auth.json mangler."""
    if AUTH_FILE.exists():
        try:
            return json.loads(AUTH_FILE.read_text())
        except Exception:
            pass

    # Migrasjon: hent fra options.json og skriv til auth-filen
    opts = read_options()
    cookie = opts.get("roblosecurity_cookie", "")
    child_ids = opts.get("child_user_ids", [])
    if cookie and child_ids:
        _LOGGER.info("Migrerer cookie og child_user_ids fra options.json til roblox_auth.json")
        write_auth(cookie, [int(c) for c in child_ids])
        return {"roblosecurity_cookie": cookie, "child_user_ids": child_ids}

    # Siste utvei: les fra env (satt av polleren ved oppstart)
    cookie = os.environ.get("ROBLOSECURITY_COOKIE", "")
    raw_ids = os.environ.get("CHILD_USER_IDS", "")
    child_ids = [int(x) for x in raw_ids.replace(" ", "").split(",") if x.isdigit()]
    if cookie and child_ids:
        write_auth(cookie, child_ids)
        return {"roblosecurity_cookie": cookie, "child_user_ids": child_ids}

    return {}


def write_auth(cookie: str, child_ids: list[int]) -> None:
    AUTH_FILE.write_text(json.dumps({"roblosecurity_cookie": cookie, "child_user_ids": child_ids}, indent=2))


def is_configured() -> bool:
    auth = read_auth()
    return bool(auth.get("roblosecurity_cookie")) and bool(auth.get("child_user_ids"))


_ADDON_VERSION = os.environ.get("ADDON_VERSION", "0")


@app.route("/")
def index():
    html = (WWW_DIR / "index.html").read_text()
    html = html.replace('src="app.js"', f'src="app.js?v={_ADDON_VERSION}"')
    html = html.replace('href="style.css"', f'href="style.css?v={_ADDON_VERSION}"')
    resp = app.response_class(html, mimetype="text/html")
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.route("/<path:filename>")
def static_files(filename):
    resp = send_from_directory(str(WWW_DIR), filename)
    resp.headers["Cache-Control"] = "no-store"
    return resp


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
            # Berik alltid med siste data fra details_cache (AI-analyse oppdaterer kun der)
            details = state.get("details_cache", {}).get(str(uid), {})
            for field in ("screenshots", "like_ratio", "up_votes", "down_votes",
                          "creator_name", "creator_type", "creator_verified",
                          "visits", "favorite_count", "created", "updated",
                          "name_history", "ai_verdict", "ai_summary", "ai_concerns", "ai_safe_age"):
                cache_val = details.get(field)
                if cache_val is not None:
                    game[field] = cache_val
                elif field not in game:
                    game[field] = None

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
    enforce_allowlist = os.environ.get("ENFORCE_ALLOWLIST", "false").lower() == "true"
    return jsonify({
        "configured": True,
        "auth_error": state.get("auth_error", False),
        "enforce_allowlist": enforce_allowlist,
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


@app.route("/api/debug/state")
def api_debug_state():
    """Rå state.json — for feilsøking. Viser om thumbnail/description faktisk er hentet."""
    state = load_state()
    children = state.get("children", {})
    out = {}
    for child_id, child in children.items():
        universes = child.get("top_universes", [])
        out[child_id] = [
            {
                "name": g.get("name"),
                "has_thumbnail": bool(g.get("thumbnail_url")),
                "has_description": bool(g.get("description")),
                "genre": g.get("genre"),
                "playing": g.get("playing"),
            }
            for g in universes
        ]
    friends = {k: len(v) for k, v in state.get("friends", {}).items()}
    return jsonify({
        "last_slow_update": state.get("last_slow_update"),
        "games_per_child": out,
        "friend_counts": friends,
        "details_cache_size": len(state.get("details_cache", {})),
    })


@app.route("/api/ai/status")
def api_ai_status():
    """Sjekker om Ollama er tilgjengelig og viser analyse-statistikk."""
    state = load_state()
    details = state.get("details_cache", {})
    total = len(details)
    analyzed = sum(1 for d in details.values() if d.get("ai_verdict"))
    verdicts: dict[str, int] = {}
    for d in details.values():
        v = d.get("ai_verdict")
        if v:
            verdicts[v] = verdicts.get(v, 0) + 1

    async def _check():
        return await check_ollama_available()

    loop = asyncio.new_event_loop()
    available = loop.run_until_complete(_check())
    loop.close()

    return jsonify({
        "ollama_available": available,
        "ollama_url": os.environ.get("OLLAMA_URL", ""),
        "ollama_model": os.environ.get("OLLAMA_MODEL", ""),
        "games_total": total,
        "games_analyzed": analyzed,
        "games_pending": total - analyzed,
        "verdicts": verdicts,
    })


@app.route("/api/image-proxy")
def api_image_proxy():
    """Henter og cacher eksterne bilder lokalt — omgår nettverksblokkeringer hos bruker."""
    url = request.args.get("url", "").strip()
    if not url or not url.startswith("https://"):
        return "", 400

    cache_key = hashlib.sha256(url.encode()).hexdigest()
    # Behold original filendelse for korrekt content-type
    ext = url.split("?")[0].rsplit(".", 1)[-1].lower()
    if ext not in ("webp", "png", "jpg", "jpeg", "gif"):
        ext = "webp"
    cache_path = IMAGE_CACHE_DIR / f"{cache_key}.{ext}"

    if not cache_path.exists():
        try:
            resp = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code != 200:
                return "", 502
            cache_path.write_bytes(resp.content)
        except Exception as err:
            _LOGGER.warning("Image proxy feilet for %s: %s", url, err)
            return "", 502

    content_types = {"webp": "image/webp", "png": "image/png", "jpg": "image/jpeg",
                     "jpeg": "image/jpeg", "gif": "image/gif"}
    ct = content_types.get(ext, "image/webp")
    return Response(
        cache_path.read_bytes(),
        mimetype=ct,
        headers={"Cache-Control": "public, max-age=86400"},
    )


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
