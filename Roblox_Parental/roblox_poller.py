"""
Polling-loop for Roblox foreldrekontroll.
Kjører to loops: slow (skjermtid/spill) og fast (presence).
Skriver state til /data/state.json og sender HA-varsler ved hendelser.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import aiohttp

from roblox_api import (
    RobloxApiError,
    RobloxAuthError,
    RobloxParentalClient,
    RobloxRateLimitError,
)

_LOGGER = logging.getLogger(__name__)

STATE_FILE = Path("/data/state.json")
APPROVED_FILE = Path("/data/approved_games.json")

# Presence-typer fra Roblox API
PRESENCE_OFFLINE = 0
PRESENCE_ONLINE = 1
PRESENCE_IN_GAME = 2
PRESENCE_IN_STUDIO = 3

# Nattmodus: stopp fast poll i disse timene (lokal tid)
NIGHT_HOUR_START = 23
NIGHT_HOUR_END = 7

# Eksponentiell backoff: maks ventetid i sekunder
MAX_BACKOFF = 60 * 60  # 1 time


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {
        "auth_error": False,
        "last_slow_update": None,
        "last_fast_update": None,
        "child": {},
        "presence": {},
        "name_cache": {},
    }


def save_state(state: dict) -> None:
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, default=str))
    tmp.replace(STATE_FILE)


def load_approved() -> set[int]:
    if APPROVED_FILE.exists():
        try:
            data = json.loads(APPROVED_FILE.read_text())
            return set(data.get("approved", []))
        except Exception:
            pass
    return set()


def save_approved(approved: set[int]) -> None:
    APPROVED_FILE.write_text(json.dumps({"approved": list(approved)}))


def _jitter(interval: float, pct: float = 0.2) -> float:
    """Legg til ±pct tilfeldig variasjon så kall ikke er robotaktig regelmessige."""
    return interval * (1 + random.uniform(-pct, pct))


def _is_night() -> bool:
    hour = datetime.now().hour
    if NIGHT_HOUR_START > NIGHT_HOUR_END:
        return hour >= NIGHT_HOUR_START or hour < NIGHT_HOUR_END
    return NIGHT_HOUR_START <= hour < NIGHT_HOUR_END


def _any_child_online(state: dict) -> bool:
    presences = state.get("presences", {})
    return any(p.get("online", False) for p in presences.values())


async def send_ha_notification(
    ha_token: str,
    title: str,
    message: str,
    action_approve: int | None = None,
    action_block: int | None = None,
) -> None:
    """Send push-varsling via HA REST API."""
    if not ha_token:
        return

    supervisor_url = "http://supervisor/core/api/services/notify/mobile_app_notify"
    headers = {"Authorization": f"Bearer {ha_token}", "Content-Type": "application/json"}

    data: dict[str, Any] = {"title": title, "message": message}

    if action_approve is not None and action_block is not None:
        data["data"] = {
            "actions": [
                {"action": f"APPROVE_{action_approve}", "title": "Godkjenn"},
                {"action": f"BLOCK_{action_block}", "title": "Blokker"},
            ]
        }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(supervisor_url, headers=headers, json=data) as resp:
                if resp.status not in (200, 201):
                    _LOGGER.warning("HA-varsling feilet: %s", resp.status)
    except Exception as err:
        _LOGGER.warning("Klarte ikke sende HA-varsling: %s", err)


class RobloxPoller:
    def __init__(self) -> None:
        self._cookie = os.environ.get("ROBLOSECURITY_COOKIE", "")
        raw_ids = os.environ.get("CHILD_USER_IDS", os.environ.get("CHILD_USER_ID", ""))
        self._child_ids: list[int] = [int(x) for x in raw_ids.replace(" ", "").split(",") if x.isdigit()]
        self._stopped = False
        self._slow_interval = int(os.environ.get("SLOW_POLL_INTERVAL", "30")) * 60
        self._fast_interval = int(os.environ.get("FAST_POLL_INTERVAL", "2")) * 60
        self._presence_enabled = os.environ.get("PRESENCE_ENABLED", "true").lower() == "true"
        self._ha_token = os.environ.get("HA_TOKEN", "")

        self._state = load_state()
        self._client: RobloxParentalClient | None = None

        # Per-barn notifikasjons-tracking
        self._last_notified_universe: dict[int, int | None] = {}
        self._last_limit_notified: dict[int, bool] = {}

        # Backoff-tracking per loop
        self._slow_consecutive_errors = 0
        self._fast_consecutive_errors = 0

    def _get_client(self) -> RobloxParentalClient:
        if self._client is None:
            name_cache = {int(k): v for k, v in self._state.get("name_cache", {}).items()}
            details_cache = {int(k): v for k, v in self._state.get("details_cache", {}).items()}
            self._client = RobloxParentalClient(self._cookie, name_cache, details_cache)
        return self._client

    def _rebuild_client(self) -> None:
        new_cookie = os.environ.get("ROBLOSECURITY_COOKIE", "")
        if new_cookie != self._cookie:
            self._cookie = new_cookie
            if self._client:
                asyncio.create_task(self._client.close())
            self._client = None

    def _backoff(self, consecutive_errors: int, base_interval: float) -> float:
        """Eksponentiell backoff med jitter. Første feil = 1x, andre = 2x, osv. opp til MAX_BACKOFF."""
        wait = min(base_interval * (2 ** consecutive_errors), MAX_BACKOFF)
        return _jitter(wait, 0.15)

    async def run_slow(self) -> None:
        """Poller skjermtid, spilliste, blokkerte spill og innstillinger for alle barn."""
        while not self._stopped:
            self._rebuild_client()
            if not self._cookie or not self._child_ids:
                _LOGGER.warning("Cookie eller child_user_ids ikke satt — venter...")
                await asyncio.sleep(60)
                continue

            # Hopp over slow poll om natten hvis ingen er online
            if _is_night() and not _any_child_online(self._state):
                _LOGGER.debug("Nattmodus — hopper over slow poll")
                await asyncio.sleep(_jitter(self._slow_interval))
                continue

            try:
                client = self._get_client()
                children_data = {}
                for child_id in self._child_ids:
                    try:
                        children_data[str(child_id)] = await self._fetch_slow(client, child_id)
                    except RobloxApiError as err:
                        _LOGGER.warning("API-feil for barn %d: %s", child_id, err)

                self._state["children"] = children_data
                self._state["name_cache"] = {str(k): v for k, v in client.name_cache.items()}
                self._state["details_cache"] = {str(k): v for k, v in client.details_cache.items()}
                self._state["auth_error"] = False
                self._state["last_slow_update"] = time.time()

                friends_data = {}
                for child_id in self._child_ids:
                    try:
                        friends = await client.get_friends_with_names(child_id)
                        friends_data[str(child_id)] = friends
                        _LOGGER.info("Barn %d har %d venner", child_id, len(friends))
                    except RobloxApiError as err:
                        _LOGGER.warning("Klarte ikke hente venner for barn %d: %s", child_id, err)
                self._state["friends"] = friends_data

                save_state(self._state)

                for child_id, child_data in children_data.items():
                    _LOGGER.info(
                        "Slow poll barn %s: %d min i dag, %d spill",
                        child_id,
                        child_data.get("screentime_today", 0),
                        len(child_data.get("top_universes", [])),
                    )
                    await self._check_slow_alerts(int(child_id), child_data)

                self._slow_consecutive_errors = 0

            except RobloxAuthError as err:
                _LOGGER.error("Auth-feil: %s — oppdater cookie i addon-konfig", err)
                self._state["auth_error"] = True
                save_state(self._state)
                # Auth-feil: ikke prøv igjen for aggressivt, vent lenge
                self._slow_consecutive_errors += 1
                await asyncio.sleep(self._backoff(self._slow_consecutive_errors, self._slow_interval))
                continue

            except RobloxRateLimitError:
                self._slow_consecutive_errors += 1
                wait = self._backoff(self._slow_consecutive_errors, self._slow_interval)
                _LOGGER.warning("Rate limited (slow) — venter %.0f s", wait)
                await asyncio.sleep(wait)
                continue

            await asyncio.sleep(_jitter(self._slow_interval))

    async def run_fast(self) -> None:
        """Poller presence for alle barn."""
        while not self._stopped:
            self._rebuild_client()
            if not self._cookie or not self._child_ids or not self._presence_enabled:
                await asyncio.sleep(self._fast_interval)
                continue

            # Nattmodus: stopp fast poll helt om natten
            if _is_night():
                _LOGGER.debug("Nattmodus — pauser presence-poll")
                await asyncio.sleep(60)
                continue

            try:
                client = self._get_client()
                presences = {}
                for child_id in self._child_ids:
                    try:
                        presence = await client.get_presence(child_id)
                    except RobloxApiError as err:
                        _LOGGER.debug("Presence-feil barn %d: %s", child_id, err)
                        continue

                    presence_type = presence.get("userPresenceType", PRESENCE_OFFLINE)
                    online = presence_type in (PRESENCE_ONLINE, PRESENCE_IN_GAME, PRESENCE_IN_STUDIO)
                    in_game = presence_type == PRESENCE_IN_GAME

                    universe_id: int | None = None
                    game_name: str | None = None

                    if in_game:
                        uid = presence.get("universeId")
                        if uid:
                            universe_id = int(uid)
                            names = await client.resolve_names([universe_id])
                            game_name = names.get(universe_id)
                        else:
                            game_name = presence.get("lastLocation")

                    prev = self._state.get("presences", {}).get(str(child_id), {})
                    was_in_game = prev.get("in_game", False)
                    prev_universe = prev.get("universe_id")

                    presences[str(child_id)] = {
                        "online": online,
                        "in_game": in_game,
                        "game_name": game_name,
                        "universe_id": universe_id,
                        "last_updated": time.time(),
                    }

                    if in_game and universe_id and (not was_in_game or universe_id != prev_universe):
                        await self._check_game_approval(child_id, universe_id, game_name)

            except RobloxAuthError as err:
                _LOGGER.error("Auth-feil i fast poll: %s", err)
                self._state["auth_error"] = True
                save_state(self._state)
                self._fast_consecutive_errors += 1
                await asyncio.sleep(self._backoff(self._fast_consecutive_errors, self._fast_interval))
                continue

            except RobloxRateLimitError:
                self._fast_consecutive_errors += 1
                wait = self._backoff(self._fast_consecutive_errors, self._fast_interval)
                _LOGGER.warning("Rate limited (fast) — venter %.0f s", wait)
                await asyncio.sleep(wait)
                continue

            self._state["presences"] = presences
            self._state["name_cache"] = {str(k): v for k, v in client.name_cache.items()}
            save_state(self._state)

            self._fast_consecutive_errors = 0

            # Hvis ingen barn er online: poll sjeldnere (sparer API-kall)
            if not any(p.get("online", False) for p in presences.values()):
                await asyncio.sleep(_jitter(self._fast_interval * 3))
            else:
                await asyncio.sleep(_jitter(self._fast_interval))

    async def _fetch_slow(self, client: RobloxParentalClient, child_id: int) -> dict:
        screentime_days = await client.get_weekly_screentime(child_id)
        today_minutes = 0
        week_minutes = 0
        daily_data: list[dict] = []
        for entry in screentime_days:
            mins = entry.get("minutesPlayed", 0)
            days_ago = entry.get("daysAgo", -1)
            week_minutes += mins
            if days_ago == 0:
                today_minutes = mins
            daily_data.append({"daysAgo": days_ago, "minutes": mins})

        top_universes_raw = await client.get_top_universes(child_id)
        universe_ids = [int(u["universeId"]) for u in top_universes_raw if "universeId" in u]
        details = await client.resolve_game_details(universe_ids)
        blocked_ids = await client.get_blocked(child_id)

        top_universes = [
            {
                "universe_id": int(u["universeId"]),
                "name": details.get(int(u["universeId"]), {}).get("name", str(u["universeId"])),
                "description": details.get(int(u["universeId"]), {}).get("description", ""),
                "playing": details.get(int(u["universeId"]), {}).get("playing", 0),
                "genre": details.get(int(u["universeId"]), {}).get("genre", ""),
                "thumbnail_url": details.get(int(u["universeId"]), {}).get("thumbnail_url"),
                "minutes": u.get("weeklyMinutes", 0),
                "blocked": int(u["universeId"]) in blocked_ids,
            }
            for u in top_universes_raw
            if "universeId" in u
        ]

        settings = await client.get_child_settings(child_id)
        daily_limit = (settings.get("dailyScreenTimeLimit") or {}).get("currentValue")
        age_level = (settings.get("contentAgeRestriction") or {}).get("currentValue")

        return {
            "screentime_today": today_minutes,
            "screentime_week": week_minutes,
            "daily_data": daily_data,
            "top_universes": top_universes,
            "blocked_universe_ids": list(blocked_ids),
            "daily_limit": daily_limit,
            "age_level": age_level,
        }

    async def _check_slow_alerts(self, child_id: int, child_data: dict) -> None:
        today = child_data.get("screentime_today", 0)
        limit = child_data.get("daily_limit")
        notified = self._last_limit_notified.get(child_id, False)
        if limit and today >= limit and not notified:
            self._last_limit_notified[child_id] = True
            name = child_data.get("display_name", str(child_id))
            await send_ha_notification(
                self._ha_token,
                "Roblox dagsgrense nådd",
                f"{name} har spilt {today} minutter i dag (grense: {limit} min)",
            )
        elif limit and today < limit:
            self._last_limit_notified[child_id] = False

    async def _check_game_approval(self, child_id: int, universe_id: int, game_name: str | None) -> None:
        approved = load_approved()
        children_data = self._state.get("children", {})
        blocked_ids = set(children_data.get(str(child_id), {}).get("blocked_universe_ids", []))
        display_name = game_name or str(universe_id)

        if universe_id in approved:
            _LOGGER.debug("Spill '%s' er godkjent", display_name)
            return

        if universe_id in blocked_ids:
            _LOGGER.info("Barn %d startet blokkert spill '%s' — varsler", child_id, display_name)
            await send_ha_notification(
                self._ha_token,
                "Blokkert Roblox-spill startet",
                f"Barnet startet '{display_name}' som er blokkert. Sjekk blokkering.",
            )
            return

        last_notified = self._last_notified_universe.get(child_id)
        if universe_id != last_notified:
            self._last_notified_universe[child_id] = universe_id
            _LOGGER.info("Ukjent spill '%s' (%d) — varsler forelder", display_name, universe_id)
            await send_ha_notification(
                self._ha_token,
                "Nytt Roblox-spill",
                f"Barnet spiller '{display_name}' — ikke godkjent ennå.",
                action_approve=universe_id,
                action_block=universe_id,
            )

    def stop(self) -> None:
        self._stopped = True

    async def run(self) -> None:
        _LOGGER.info("Roblox Poller starter (%d barn)", len(self._child_ids))
        tasks = [asyncio.create_task(self.run_slow())]
        if self._presence_enabled:
            tasks.append(asyncio.create_task(self.run_fast()))
        await asyncio.gather(*tasks)


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    poller = RobloxPoller()
    await poller.run()


if __name__ == "__main__":
    asyncio.run(main())
