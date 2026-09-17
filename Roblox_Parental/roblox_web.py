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

from roblox_ai import analyze_game, check_ollama_available
from roblox_api import RobloxApiError, RobloxAuthError, RobloxParentalClient
from roblox_poller import RobloxPoller, load_approved, load_state, save_approved

_LOGGER = logging.getLogger(__name__)

OPTIONS_FILE = Path("/data/options.json")
AUTH_FILE = Path("/data/roblox_auth.json")
STATE_FILE = Path("/data/state.json")
APPROVED_FILE = Path("/data/approved_games.json")
NOTES_FILE = Path("/data/game_notes.json")
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


def load_notes() -> dict[str, str]:
    if NOTES_FILE.exists():
        try:
            return json.loads(NOTES_FILE.read_text())
        except Exception:
            pass
    return {}


def save_notes(notes: dict[str, str]) -> None:
    NOTES_FILE.write_text(json.dumps(notes, ensure_ascii=False, indent=2))


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
        me, children = asyncio.run(_fetch())
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


@app.route("/api/setup/update-cookie", methods=["POST"])
def api_update_cookie():
    """Oppdater kun cookie — beholder eksisterende barn. Restarter polleren."""
    body = request.get_json(force=True)
    cookie = (body.get("cookie") or "").strip()
    if not cookie:
        return jsonify({"error": "cookie er påkrevd"}), 400

    auth = read_auth()
    child_ids = auth.get("child_user_ids", [])
    if not child_ids:
        return jsonify({"error": "Ingen barn konfigurert — bruk fullstendig oppsett"}), 400

    async def _validate():
        client = RobloxParentalClient(cookie)
        try:
            return await client.authenticate()
        finally:
            await client.close()

    _LOGGER.info("Cookie-oppdatering: lengde=%d, starter=%s", len(cookie), cookie[:30])
    try:
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(_validate())
        finally:
            loop.close()
    except RobloxAuthError as e:
        _LOGGER.warning("Cookie-validering feilet: %s", e)
        return jsonify({"error": "Cookie er ugyldig eller utløpt"}), 401
    except Exception as e:
        _LOGGER.warning("Cookie-validering unntak: %s", e)
        return jsonify({"error": f"Tilkoblingsfeil: {e}"}), 502

    write_auth(cookie, [int(c) for c in child_ids])
    os.environ["ROBLOSECURITY_COOKIE"] = cookie
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
    notes = load_notes()
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
            game["note"] = notes.get(str(uid), "")

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
            "robux_balance": child.get("robux_balance"),
            "daily_data": child.get("daily_data", []),
            "top_universes": top_universes,
            "presence": {
                "online": presence.get("online", False),
                "in_game": presence.get("in_game", False),
                "in_studio": presence.get("in_studio", False),
                "game_name": presence.get("game_name"),
                "universe_id": presence.get("universe_id"),
                "place_id": presence.get("place_id"),
                "game_id": presence.get("game_id"),
                "last_online": presence.get("last_online"),
                "last_location": presence.get("last_location"),
                "friends_playing_with": presence.get("friends_playing_with", []),
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
        asyncio.run(_block())
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
        asyncio.run(_unblock())
    except RobloxAuthError:
        return jsonify({"error": "Cookie ugyldig eller utløpt"}), 401
    except RobloxApiError as e:
        return jsonify({"error": str(e)}), 502

    _LOGGER.info("Avblokkerte spill universe_id=%s for barn %s", universe_id, child_id)
    return jsonify({"ok": True})


@app.route("/api/games/note", methods=["POST"])
def api_set_note():
    body = request.get_json(force=True)
    universe_id = body.get("universe_id")
    note = (body.get("note") or "").strip()
    if not universe_id:
        return jsonify({"error": "universe_id påkrevd"}), 400
    notes = load_notes()
    if note:
        notes[str(universe_id)] = note
    else:
        notes.pop(str(universe_id), None)
    save_notes(notes)
    return jsonify({"ok": True})


@app.route("/api/report/all")
def api_report_all():
    """Eksporterer alle spill som en kompakt markdown-liste for AI-analyse."""
    state = load_state()
    approved = load_approved()
    notes = load_notes()
    now = __import__("datetime").datetime.now().strftime("%Y-%m-%d")

    # Samle unike spill på tvers av alle barn, med samlet skjermtid
    seen: dict[int, dict] = {}
    child_minutes: dict[int, dict[str, int]] = {}
    name_cache = {int(k): v for k, v in state.get("name_cache", {}).items()}

    for child_key, child in state.get("children", {}).items():
        child_name = name_cache.get(int(child_key), f"Barn {child_key}")
        for g in child.get("top_universes", []):
            uid = g.get("universe_id")
            if not uid:
                continue
            if uid not in seen:
                seen[uid] = dict(g)
                details = state.get("details_cache", {}).get(str(uid), {})
                for field in ("content_descriptors", "age_rating", "minimum_age",
                              "like_ratio", "creator_name", "creator_type",
                              "creator_verified", "name_history", "ai_verdict",
                              "ai_summary", "ai_concerns", "visits", "playing"):
                    if seen[uid].get(field) is None:
                        seen[uid][field] = details.get(field)
            child_minutes.setdefault(uid, {})[child_name] = g.get("minutes", 0)

    # Sorter etter total skjermtid
    games = sorted(seen.values(), key=lambda g: sum(child_minutes.get(g["universe_id"], {}).values()), reverse=True)

    maturity_map = {"minimal": "Minimal", "moderate": "Moderat", "restricted": "Begrenset", "unrated": "Ikke vurdert av Roblox"}
    verdict_map = {"gronn": "✅ Greit", "gul": "⚠️ Foreldres skjønn", "rod": "❌ Ikke anbefalt"}

    lines = [
        f"# Roblox spilliste — {now}",
        f"Totalt {len(games)} spill. Eksportert fra Roblox Foreldrekontroll (HA-addon).",
        "",
        "**Foreslått spørsmål:** Se over denne listen og hjelp meg å prioritere hvilke spill jeg bør vurdere nærmere. Marker spill med bekymringsfulle deskriptorer, lav AI-vurdering, eller uverifisert utvikler.",
        "",
        "---",
        "",
    ]

    for g in games:
        uid = g["universe_id"]
        name = g.get("name") or str(uid)
        status = "Godkjent" if uid in approved else ("Blokkert" if g.get("blocked") else "Ikke vurdert")
        mins = sum(child_minutes.get(uid, {}).values())
        h, m = divmod(mins, 60)
        time_str = f"{h}t {m}m" if h else f"{m}m"

        lines.append(f"## {name}")

        meta = [f"Status: {status}", f"Skjermtid: {time_str}"]
        age = g.get("age_rating")
        if age:
            min_age = g.get("minimum_age")
            meta.append(f"Aldersanbefaling: {maturity_map.get(age, age)}" + (f" ({min_age}+)" if min_age else ""))
        creator = g.get("creator_name")
        if creator:
            ctype = "Gruppe" if g.get("creator_type") == "Group" else "Enkeltbruker"
            verified = "verifisert" if g.get("creator_verified") else "ikke verifisert"
            meta.append(f"Utvikler: {creator} ({ctype}, {verified})")
        lr = g.get("like_ratio")
        if lr is not None:
            meta.append(f"Like-ratio: {lr}%")
        nh = g.get("name_history") or []
        if len(nh) >= 3:
            meta.append(f"Navnehistorikk: {len(nh)} navnebytter")

        lines.append("  ".join(f"_{x}_" for x in meta))

        descriptors = g.get("content_descriptors") or []
        if descriptors:
            lines.append("Innhold: " + ", ".join(f"**{d}**" for d in descriptors))

        verdict = g.get("ai_verdict")
        if verdict:
            lines.append(f"AI: {verdict_map.get(verdict, verdict)}" + (f" — {g['ai_summary']}" if g.get("ai_summary") else ""))
            concerns = [c for c in (g.get("ai_concerns") or []) if c]
            if concerns:
                lines.append("Bekymringer: " + "; ".join(concerns))

        note = notes.get(str(uid), "").strip()
        if note:
            lines.append(f"Notat: {note}")

        lines.append("")

    report_text = "\n".join(lines)
    return Response(report_text, mimetype="text/plain; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="spilliste_{now}.md"'})


@app.route("/api/report/<int:universe_id>")
def api_report(universe_id: int):
    """Genererer en strukturert foreldrevurderingsrapport for ett spill."""
    state = load_state()
    approved = load_approved()
    notes = load_notes()

    # Finn spillet på tvers av alle barn
    game = None
    child_minutes: dict[str, int] = {}
    for child_key, child in state.get("children", {}).items():
        for g in child.get("top_universes", []):
            if g.get("universe_id") == universe_id:
                if game is None:
                    game = dict(g)
                child_minutes[child_key] = g.get("minutes", 0)

    # Berik fra details_cache
    details = state.get("details_cache", {}).get(str(universe_id), {})
    if game is None:
        game = dict(details) if details else {}
        game["universe_id"] = universe_id

    for field in ("screenshots", "like_ratio", "up_votes", "down_votes",
                  "creator_name", "creator_type", "creator_verified",
                  "visits", "favorite_count", "created", "updated",
                  "name_history", "ai_verdict", "ai_summary", "ai_concerns",
                  "ai_safe_age", "description", "genre", "playing",
                  "age_rating", "minimum_age", "content_descriptors", "name"):
        if game.get(field) is None:
            game[field] = details.get(field)

    name = game.get("name") or f"Universe {universe_id}"
    now = __import__("datetime").datetime.now().strftime("%Y-%m-%d")

    # Bygg rapport
    lines = [
        f"# Foreldrevurdering: {name}",
        f"Dato: {now}  |  Universe ID: {universe_id}  |  Kilde: Roblox Foreldrekontroll (HA-addon)",
        "",
        "## Spillinformasjon",
    ]

    genre = game.get("genre") or "—"
    creator_name = game.get("creator_name") or "—"
    creator_type = "Gruppe" if game.get("creator_type") == "Group" else "Enkeltbruker"
    creator_verified = "✓ Verifisert" if game.get("creator_verified") else "Ikke verifisert"
    created = (game.get("created") or "—")[:10]
    visits = game.get("visits")
    playing = game.get("playing")
    favorites = game.get("favorite_count")
    like_ratio = game.get("like_ratio")
    up = game.get("up_votes")
    down = game.get("down_votes")

    lines += [
        f"- **Sjanger:** {genre}",
        f"- **Utvikler:** {creator_name} ({creator_type}, {creator_verified})",
        f"- **Opprettet:** {created}",
    ]
    if visits is not None:
        lines.append(f"- **Totalt besøk:** {visits:,}".replace(",", " "))
    if playing is not None:
        lines.append(f"- **Spiller nå:** {playing:,}".replace(",", " "))
    if favorites is not None:
        lines.append(f"- **Favoritter:** {favorites:,}".replace(",", " "))
    if like_ratio is not None:
        vote_str = f"{up:,} opp / {down:,} ned".replace(",", " ") if up is not None else ""
        lines.append(f"- **Like-ratio:** {like_ratio}% ({vote_str})")

    name_history = game.get("name_history") or []
    if len(name_history) >= 2:
        lines.append(f"- **Navnehistorikk:** {len(name_history)} registrerte navn ({', '.join(name_history[:4])}{'…' if len(name_history) > 4 else ''})")

    lines += ["", "## Aldersanbefaling (Roblox)"]
    age_rating = game.get("age_rating")
    minimum_age = game.get("minimum_age")
    maturity_map = {"minimal": "Minimal", "moderate": "Moderat", "restricted": "Begrenset"}
    if age_rating:
        lines.append(f"- **Innholdsmodenhet:** {maturity_map.get(age_rating, age_rating)}")
    if minimum_age:
        lines.append(f"- **Minimumsalder:** {minimum_age}+")
    descriptors = game.get("content_descriptors") or []
    if descriptors:
        lines.append(f"- **Innholdsdeskriptorer:** {', '.join(descriptors)}")
    if not age_rating and not descriptors:
        lines.append("- Ingen offisiell aldersanbefaling registrert")

    lines += ["", "## Beskrivelse"]
    description = (game.get("description") or "").strip()
    lines.append(description if description else "_Ingen beskrivelse tilgjengelig._")

    # Skjermtid
    if child_minutes:
        lines += ["", "## Barnets bruk denne uken"]
        name_cache = {int(k): v for k, v in state.get("name_cache", {}).items()}
        for child_id_str, mins in child_minutes.items():
            child_display = name_cache.get(int(child_id_str), f"Barn {child_id_str}")
            h, m = divmod(mins, 60)
            time_str = f"{h}t {m}m" if h else f"{m} min"
            lines.append(f"- **{child_display}:** {time_str}")

    # Status
    status = "Godkjent" if universe_id in approved else ("Blokkert" if game.get("blocked") else "Ikke vurdert")
    lines += ["", f"## Status i foreldrekontroll: {status}"]

    # Foreldernotat
    parent_note = notes.get(str(universe_id), "").strip()
    if parent_note:
        lines += ["", "## Forelderens notat"]
        lines.append(parent_note)

    # AI-vurdering
    ai_verdict = game.get("ai_verdict")
    ai_summary = game.get("ai_summary")
    ai_concerns = game.get("ai_concerns") or []
    ai_safe_age = game.get("ai_safe_age")

    if ai_verdict:
        verdict_map = {"gronn": "✅ Greit for barn", "gul": "⚠️ Foreldres skjønn", "rod": "❌ Ikke anbefalt"}
        lines += ["", "## AI-vurdering (lokal analyse)"]
        lines.append(f"**{verdict_map.get(ai_verdict, ai_verdict)}**")
        if ai_safe_age:
            lines.append(f"Anbefalt minimumsalder: {ai_safe_age} år")
        if ai_summary:
            lines.append(f"\n{ai_summary}")
        if ai_concerns:
            lines += ["", "**Bekymringer:**"]
            for c in ai_concerns:
                lines.append(f"- {c}")

    lines += [
        "",
        "---",
        f"_Rapport generert av Roblox Foreldrekontroll addon. "
        f"Lim inn i en AI-tjeneste for dypere analyse._",
        "",
        "**Foreslått spørsmål til AI:**",
        f'Er "{name}" et passende Roblox-spill for et barn på 9-12 år? '
        f"Basert på informasjonen over, hva bør en forelder spesielt være oppmerksom på?",
    ]

    report_text = "\n".join(lines)
    return Response(report_text, mimetype="text/plain; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="rapport_{universe_id}.md"'})


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

    available = asyncio.run(_check())

    return jsonify({
        "ollama_available": available,
        "ollama_url": os.environ.get("OLLAMA_URL", ""),
        "ollama_model": os.environ.get("OLLAMA_MODEL", ""),
        "games_total": total,
        "games_analyzed": analyzed,
        "games_pending": total - analyzed,
        "verdicts": verdicts,
    })


@app.route("/api/ai/analyze/<int:universe_id>", methods=["POST"])
def api_ai_analyze(universe_id: int):
    """Kjør AI-analyse for ett spill med en gang (brukerutløst)."""
    state = load_state()
    details = state.get("details_cache", {}).get(str(universe_id))
    if not details:
        return jsonify({"error": "Spill ikke funnet i cache"}), 404

    async def _run():
        if not await check_ollama_available():
            return None
        return await analyze_game(details)

    result = asyncio.run(_run())

    if result is None:
        return jsonify({"error": "Ollama ikke tilgjengelig eller analyse feilet"}), 503

    details["ai_verdict"] = result["verdict"]
    details["ai_summary"] = result["summary"]
    details["ai_concerns"] = result["concerns"]
    details["ai_safe_age"] = result["safe_age"]
    state["details_cache"][str(universe_id)] = details
    _save_state = __import__("roblox_poller", fromlist=["save_state"]).save_state
    _save_state(state)

    return jsonify({"ok": True, **result})


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
