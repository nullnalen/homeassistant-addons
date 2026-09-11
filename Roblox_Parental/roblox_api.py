"""Roblox Parental API client — ingen HA-avhengigheter."""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp

_LOGGER = logging.getLogger(__name__)

BASE_URL = "https://apis.roblox.com"
USERS_URL = "https://users.roblox.com"
GAMES_URL = "https://games.roblox.com"
PRESENCE_URL = "https://presence.roblox.com"

URL_AUTHENTICATED = f"{USERS_URL}/v1/users/authenticated"
URL_CHILDREN_INFO = f"{BASE_URL}/parental-controls-api/v1/parental-controls/children-info"
URL_WEEKLY_SCREENTIME = f"{BASE_URL}/parental-controls-api/v1/parental-controls/get-weekly-screentime"
URL_TOP_UNIVERSES = f"{BASE_URL}/parental-controls-api/v1/parental-controls/get-top-weekly-screentime-by-universe"
URL_BLOCKED_EXPERIENCES = f"{BASE_URL}/experience-blocking-api/v1/get-blocked-experiences"
URL_CHILD_SETTINGS = f"{BASE_URL}/parental-controls-api/v1/parental-controls/child-settings"
URL_GRANT_CONSENT = f"{BASE_URL}/parental-controls-api/v1/parental-controls/grant-consent"
URL_GAMES = f"{GAMES_URL}/v1/games"
URL_PRESENCE = f"{PRESENCE_URL}/v1/presence/users"
FRIENDS_URL = "https://friends.roblox.com"
URL_FRIENDS = f"{FRIENDS_URL}/v1/users/{{user_id}}/friends/find"
URL_PROFILES = f"{BASE_URL}/user-profile-api/v1/user/profiles/get-profiles"

USER_AGENT = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148"


class RobloxAuthError(Exception):
    pass


class RobloxRateLimitError(Exception):
    pass


class RobloxApiError(Exception):
    pass


class RobloxParentalClient:
    def __init__(self, cookie: str, name_cache: dict[int, str] | None = None) -> None:
        self._cookie = cookie
        self._csrf_token: str | None = None
        self._name_cache: dict[int, str] = name_cache or {}
        self._session: aiohttp.ClientSession | None = None

    def _make_session(self) -> aiohttp.ClientSession:
        return aiohttp.ClientSession(
            headers={
                "User-Agent": USER_AGENT,
                "Accept-Encoding": "gzip, deflate",
                "Accept": "application/json",
            },
            cookies={".ROBLOSECURITY": self._cookie},
        )

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = self._make_session()
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _get(self, url: str, params: dict | None = None) -> Any:
        session = await self._get_session()
        try:
            async with session.get(url, params=params) as resp:
                if resp.status == 401:
                    raise RobloxAuthError("Cookie ugyldig eller utløpt (401)")
                if resp.status == 403:
                    raise RobloxAuthError("Forbudt (403) på GET — cookie kan være ugyldig")
                if resp.status == 429:
                    raise RobloxRateLimitError("Rate limited (429)")
                if resp.status >= 500:
                    raise RobloxApiError(f"Serverfeil {resp.status}")
                resp.raise_for_status()
                return await resp.json(content_type=None)
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            raise RobloxApiError(f"Nettverksfeil: {err}") from err

    async def _post(self, url: str, json: dict) -> Any:
        session = await self._get_session()

        async def _do_post() -> aiohttp.ClientResponse:
            headers: dict[str, str] = {}
            if self._csrf_token:
                headers["x-csrf-token"] = self._csrf_token
            return session.post(url, json=json, headers=headers)

        try:
            async with await _do_post() as resp:
                if resp.status == 403:
                    new_csrf = resp.headers.get("x-csrf-token")
                    if new_csrf:
                        self._csrf_token = new_csrf
                        async with await _do_post() as retry:
                            if retry.status == 401:
                                raise RobloxAuthError("Cookie ugyldig (401 på retry)")
                            if retry.status == 403:
                                raise RobloxAuthError("Forbudt (403) etter csrf-retry")
                            if retry.status == 429:
                                raise RobloxRateLimitError("Rate limited (429)")
                            if retry.status >= 500:
                                raise RobloxApiError(f"Serverfeil {retry.status}")
                            retry.raise_for_status()
                            return await retry.json(content_type=None)
                    raise RobloxAuthError("Forbudt (403) — ingen csrf-token i svar")
                if resp.status == 401:
                    raise RobloxAuthError("Cookie ugyldig eller utløpt (401)")
                if resp.status == 429:
                    raise RobloxRateLimitError("Rate limited (429)")
                if resp.status >= 500:
                    raise RobloxApiError(f"Serverfeil {resp.status}")
                resp.raise_for_status()
                return await resp.json(content_type=None)
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            raise RobloxApiError(f"Nettverksfeil: {err}") from err

    async def authenticate(self) -> dict:
        return await self._get(URL_AUTHENTICATED)

    async def get_children(self) -> list[dict]:
        data = await self._get(URL_CHILDREN_INFO)
        return data.get("childrenInfoList", [])

    async def get_weekly_screentime(self, child_id: int) -> list[dict]:
        data = await self._get(URL_WEEKLY_SCREENTIME, params={"userId": child_id})
        return data.get("dailyScreentimes", [])

    async def get_top_universes(self, child_id: int) -> list[dict]:
        data = await self._get(URL_TOP_UNIVERSES, params={"userId": child_id})
        return data.get("universeWeeklyScreentimes", [])

    async def get_blocked(self, child_id: int) -> set[int]:
        data = await self._post(
            URL_BLOCKED_EXPERIENCES,
            {"targetUserId": child_id, "limit": 50, "offset": 0},
        )
        entries = data.get("blockedExperiences", data.get("experiences", []))
        return {int(e["universeId"]) for e in entries if "universeId" in e}

    async def get_child_settings(self, child_id: int) -> dict:
        return await self._get(URL_CHILD_SETTINGS, params={"childUserId": child_id})

    async def resolve_names(self, universe_ids: list[int]) -> dict[int, str]:
        to_fetch = [uid for uid in universe_ids if uid not in self._name_cache]
        if to_fetch:
            chunk_size = 50
            for i in range(0, len(to_fetch), chunk_size):
                chunk = to_fetch[i: i + chunk_size]
                try:
                    data = await self._get(URL_GAMES, params={"universeIds": ",".join(str(u) for u in chunk)})
                    for game in data.get("data", []):
                        uid = int(game["id"])
                        self._name_cache[uid] = game.get("name", str(uid))
                except RobloxApiError as err:
                    _LOGGER.warning("Klarte ikke slå opp spillnavn: %s", err)
        return {uid: self._name_cache.get(uid, str(uid)) for uid in universe_ids}

    async def get_presence(self, child_id: int) -> dict:
        data = await self._post(URL_PRESENCE, {"userIds": [child_id]})
        users = data.get("userPresences", [])
        return users[0] if users else {}

    async def block_experience(self, child_id: int, universe_id: int) -> None:
        await self._post(URL_GRANT_CONSENT, {
            "childUserId": child_id,
            "consentType": "ManageExperience",
            "details": {"experienceManagementAction": "Block", "universeId": universe_id},
        })

    async def unblock_experience(self, child_id: int, universe_id: int) -> None:
        await self._post(URL_GRANT_CONSENT, {
            "childUserId": child_id,
            "consentType": "ManageExperience",
            "details": {"experienceManagementAction": "Unblock", "universeId": universe_id},
        })

    async def get_friends(self, user_id: int) -> list[int]:
        url = URL_FRIENDS.format(user_id=user_id)
        data = await self._get(url)
        return [item["id"] for item in data.get("PageItems", []) if "id" in item]

    async def get_profiles(self, user_ids: list[int]) -> dict[int, str]:
        """Henter visningsnavn for en liste med user_ids i én batch."""
        if not user_ids:
            return {}
        data = await self._post(URL_PROFILES, {
            "userIds": user_ids,
            "fields": ["names.combinedName"],
        })
        return {
            p["userId"]: p.get("names", {}).get("combinedName", str(p["userId"]))
            for p in data.get("profileDetails", [])
        }

    async def get_friends_with_names(self, user_id: int) -> list[dict]:
        """Henter venneliste med navn i to kall."""
        friend_ids = await self.get_friends(user_id)
        if not friend_ids:
            return []
        names = await self.get_profiles(friend_ids)
        return [{"id": uid, "name": names.get(uid, str(uid))} for uid in friend_ids]

    @property
    def name_cache(self) -> dict[int, str]:
        return dict(self._name_cache)
