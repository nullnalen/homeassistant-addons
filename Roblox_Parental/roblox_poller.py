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
import time
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
        # Støtter kommaseparert liste fra env: "123,456" eller enkelt tall
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

    def _get_client(self) -> RobloxParentalClient:
        name_cache = {int(k): v for k, v in self._state.get("name_cache", {}).items()}
        if self._client is None:
            self._client = RobloxParentalClient(self._cookie, name_cache)
        return self._client

    def _rebuild_client(self) -> None:
        """Rebuild client med oppdatert cookie (etter at bruker har oppdatert config)."""
        new_cookie = os.environ.get("ROBLOSECURITY_COOKIE", "")
        if new_cookie != self._cookie:
            self._cookie = new_cookie
            if self._client:
                asyncio.create_task(self._client.close())
            self._client = None

    async def run_slow(self) -> None:
        """Poller skjermtid, spilliste, blokkerte spill og innstillinger for alle barn."""
        while not self._stopped:
            self._rebuild_client()
            if not self._cookie or not self._child_ids:
                _LOGGER.warning("Cookie eller child_user_ids ikke satt — venter...")
                await asyncio.sleep(60)
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
                self._state["auth_error"] = False
                self._state["last_slow_update"] = time.time()
                save_state(self._state)

                for child_id, child_data in children_data.items():
                    _LOGGER.info(
                        "Slow poll barn %s: %d min i dag, %d spill",
                        child_id,
                        child_data.get("screentime_today", 0),
                        len(child_data.get("top_universes", [])),
                    )
                    await self._check_slow_alerts(int(child_id), child_data)

            except RobloxAuthError as err:
                _LOGGER.error("Auth-feil: %s — oppdater cookie i addon-konfig", err)
                self._state["auth_error"] = True
                save_state(self._state)
                await asyncio.sleep(self._slow_interval * 2)
                continue

            except RobloxRateLimitError:
                _LOGGER.warning("Rate limited — venter dobbelt intervall")
                await asyncio.sleep(self._slow_interval * 2)
                continue

            await asyncio.sleep(self._slow_interval)

    async def run_fast(self) -> None:
        """Poller presence for alle barn."""
        while not self._stopped:
            self._rebuild_client()
            if not self._cookie or not self._child_ids or not self._presence_enabled:
                await asyncio.sleep(self._fast_interval)
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
                await asyncio.sleep(self._fast_interval * 5)
                continue

            except RobloxRateLimitError:
                await asyncio.sleep(self._fast_interval * 3)
                continue

            except RobloxRateLimitError:
                await asyncio.sleep(self._fast_interval * 3)
                continue

            self._state["presences"] = presences
            self._state["name_cache"] = {str(k): v for k, v in client.name_cache.items()}
            save_state(self._state)

            await asyncio.sleep(self._fast_interval)

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
        names = await client.resolve_names(universe_ids)
        blocked_ids = await client.get_blocked(child_id)

        top_universes = [
            {
                "universe_id": int(u["universeId"]),
                "name": names.get(int(u["universeId"]), str(u["universeId"])),
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
        """Varsle om dagsgrense er nådd."""
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
        """Sjekk om spillet er godkjent, varsle forelder hvis ukjent."""
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
