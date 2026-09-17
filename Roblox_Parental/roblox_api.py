"""Roblox Parental API client — ingen HA-avhengigheter."""
from __future__ import annotations

import asyncio
import logging
import time
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
URL_GAME_VOTES = f"{GAMES_URL}/v1/games/votes"
URL_GAME_PASSES = f"{GAMES_URL}/v1/games/{{universe_id}}/game-passes"
URL_GAME_NAME_HISTORY = f"{GAMES_URL}/v1/games/{{universe_id}}/name-history"
URL_PRESENCE = f"{PRESENCE_URL}/v1/presence/users"
FRIENDS_URL = "https://friends.roblox.com"
URL_FRIENDS = f"{FRIENDS_URL}/v1/users/{{user_id}}/friends/find"
URL_PROFILES = f"{BASE_URL}/user-profile-api/v1/user/profiles/get-profiles"
THUMBNAILS_URL = "https://thumbnails.roblox.com"
URL_GAME_THUMBNAILS = f"{THUMBNAILS_URL}/v1/batch"
URL_GAME_SCREENSHOTS = f"{THUMBNAILS_URL}/v1/games/multiget/thumbnails"
URL_AGE_RECOMMENDATIONS = f"{BASE_URL}/experience-guidelines-service/v1beta1/multi-age-recommendation"
URL_UNIVERSE_FROM_PLACE = "https://apis.roblox.com/universes/v1/places/{place_id}/universe"
ECONOMY_URL = "https://economy.roblox.com"
URL_ROBUX_BALANCE = f"{ECONOMY_URL}/v1/users/{{user_id}}/currency"

USER_AGENT = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148"

DETAILS_CACHE_TTL = 7 * 24 * 3600  # 7 dager


class RobloxAuthError(Exception):
    pass


class RobloxRateLimitError(Exception):
    pass


class RobloxApiError(Exception):
    pass


class RobloxParentalClient:
    def __init__(self, cookie: str, name_cache: dict[int, str] | None = None, details_cache: dict[int, dict] | None = None) -> None:
        self._cookie = cookie
        self._csrf_token: str | None = None
        self._name_cache: dict[int, str] = name_cache or {}
        self._details_cache: dict[int, dict] = details_cache or {}
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
                    body = await resp.text()
                    _LOGGER.warning("403 på GET %s — body: %s", url, body[:200])
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

        def _make_ctx():
            headers: dict[str, str] = {}
            if self._csrf_token:
                headers["x-csrf-token"] = self._csrf_token
            return session.post(url, json=json, headers=headers)

        try:
            async with _make_ctx() as resp:
                if resp.status == 403:
                    new_csrf = resp.headers.get("x-csrf-token")
                    if new_csrf:
                        self._csrf_token = new_csrf
                        async with _make_ctx() as retry:
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
        details = await self.resolve_game_details(universe_ids)
        return {uid: details[uid]["name"] for uid in universe_ids if uid in details}

    async def resolve_game_details(self, universe_ids: list[int]) -> dict[int, dict]:
        """Henter navn, beskrivelse, spillertall, sjanger, votes, creator og screenshots."""
        now = time.time()
        missing_info = [
            uid for uid in universe_ids
            if uid not in self._details_cache
            or now - self._details_cache[uid].get("cached_at", 0) > DETAILS_CACHE_TTL
        ]
        missing_thumb = [uid for uid in universe_ids if uid in self._details_cache and self._details_cache[uid].get("thumbnail_url") is None]

        chunk_size = 50
        if missing_info:
            for i in range(0, len(missing_info), chunk_size):
                chunk = missing_info[i: i + chunk_size]
                try:
                    data = await self._get(URL_GAMES, params={"universeIds": ",".join(str(u) for u in chunk)})
                    for game in data.get("data", []):
                        uid = int(game["id"])
                        creator = game.get("creator") or {}
                        existing = self._details_cache.get(uid, {})
                        self._details_cache[uid] = {
                            "name": game.get("name", str(uid)),
                            "description": (game.get("description") or "").strip()[:500],
                            "playing": game.get("playing", 0),
                            "visits": game.get("visits", 0),
                            "genre": game.get("genre", ""),
                            "root_place_id": game.get("rootPlaceId"),
                            "created": (game.get("created") or "")[:10],
                            "updated": (game.get("updated") or "")[:10],
                            "creator_name": creator.get("name", ""),
                            "creator_type": creator.get("type", ""),
                            "creator_verified": creator.get("hasVerifiedBadge", False),
                            "favorite_count": game.get("favoritedCount", 0),
                            "thumbnail_url": existing.get("thumbnail_url"),
                            "screenshots": existing.get("screenshots", []),
                            "age_rating": None,
                            "minimum_age": None,
                            "content_descriptors": [],
                            "like_ratio": None,
                            "up_votes": None,
                            "down_votes": None,
                            "name_history": existing.get("name_history", []),
                            "ai_verdict": existing.get("ai_verdict"),
                            "ai_summary": existing.get("ai_summary"),
                            "ai_concerns": existing.get("ai_concerns", []),
                            "ai_safe_age": existing.get("ai_safe_age"),
                            "cached_at": now,
                        }
                        self._name_cache[uid] = self._details_cache[uid]["name"]
                except RobloxApiError as err:
                    _LOGGER.warning("Klarte ikke slå opp spillinfo: %s", err)

            # Aldersanbefaling
            age_ids = [uid for uid in missing_info if uid in self._details_cache]
            for i in range(0, len(age_ids), chunk_size):
                chunk = age_ids[i: i + chunk_size]
                try:
                    adata = await self._post(URL_AGE_RECOMMENDATIONS, {"universeIds": chunk})
                    for entry in adata.get("ageRecommendationDetailsByUniverse", []):
                        uid = entry.get("universeId")
                        if not uid or uid not in self._details_cache:
                            continue
                        summary = (entry.get("ageRecommendationDetails") or {}).get("ageRecommendationSummary") or {}
                        rec = summary.get("ageRecommendation") or {}
                        descriptors = [
                            d["descriptorDisplayName"]
                            for d in (entry.get("ageRecommendationDetails") or {}).get("experienceDescriptorUsages", {}).get("items", [])
                            if d.get("contains") and d.get("descriptorDisplayName")
                        ]
                        maturity = rec.get("contentMaturity")
                        self._details_cache[uid]["age_rating"] = maturity
                        # unrated = Roblox har ikke vurdert spillet, default-alder er ikke meningsfull
                        self._details_cache[uid]["minimum_age"] = rec.get("minimumAge") if maturity != "unrated" else None
                        self._details_cache[uid]["content_descriptors"] = descriptors
                except RobloxApiError as err:
                    _LOGGER.warning("Klarte ikke hente aldersanbefaling: %s", err)

            # Votes (like-ratio)
            vote_ids = [uid for uid in missing_info if uid in self._details_cache]
            for i in range(0, len(vote_ids), chunk_size):
                chunk = vote_ids[i: i + chunk_size]
                try:
                    vdata = await self._get(URL_GAME_VOTES, params={"universeIds": ",".join(str(u) for u in chunk)})
                    for entry in vdata.get("data", []):
                        uid = int(entry["id"])
                        if uid not in self._details_cache:
                            continue
                        up = entry.get("upVotes", 0)
                        down = entry.get("downVotes", 0)
                        total = up + down
                        self._details_cache[uid]["up_votes"] = up
                        self._details_cache[uid]["down_votes"] = down
                        self._details_cache[uid]["like_ratio"] = round(up / total * 100) if total else None
                except RobloxApiError as err:
                    _LOGGER.warning("Klarte ikke hente votes: %s", err)

            # Navnehistorikk (krever auth) — rødt flagg om spillet bytter navn
            for uid in missing_info:
                if uid not in self._details_cache:
                    continue
                try:
                    url = URL_GAME_NAME_HISTORY.format(universe_id=uid)
                    hdata = await self._get(url, params={"limit": 10, "sortOrder": "Desc"})
                    names = [e["name"] for e in hdata.get("data", []) if e.get("name")]
                    self._details_cache[uid]["name_history"] = names
                except RobloxApiError:
                    pass  # Ikke kritisk

        # Screenshots via multiget (én GET, returnerer inntil countPerUniverse bilder)
        screenshot_ids = [uid for uid in universe_ids if uid in self._details_cache and not self._details_cache[uid].get("screenshots")]
        if screenshot_ids:
            for i in range(0, len(screenshot_ids), 25):
                chunk = screenshot_ids[i: i + 25]
                try:
                    sdata = await self._get(
                        URL_GAME_SCREENSHOTS,
                        params={
                            "universeIds": ",".join(str(u) for u in chunk),
                            "countPerUniverse": 5,
                            "size": "768x432",
                            "format": "Webp",
                            "isCircular": "false",
                        },
                    )
                    for entry in sdata.get("data", []):
                        uid = int(entry["universeId"])
                        if uid not in self._details_cache:
                            continue
                        urls = [t["imageUrl"] for t in entry.get("thumbnails", []) if t.get("state") == "Completed" and t.get("imageUrl")]
                        if urls:
                            self._details_cache[uid]["screenshots"] = urls
                except RobloxApiError as err:
                    _LOGGER.warning("Klarte ikke hente screenshots: %s", err)

        # Ikon-thumbnails for nye + de som mangler
        thumb_ids_set = set(missing_info) | set(missing_thumb)
        if thumb_ids_set:
            thumb_ids = [uid for uid in thumb_ids_set if uid in self._details_cache]
            for i in range(0, len(thumb_ids), chunk_size):
                chunk = thumb_ids[i: i + chunk_size]
                batch = [
                    {
                        "requestId": f"{uid}::GameIcon:256x256:webp:regular:::false:false",
                        "type": "GameIcon",
                        "targetId": uid,
                        "token": "",
                        "format": "webp",
                        "size": "256x256",
                        "version": "",
                    }
                    for uid in chunk
                ]
                try:
                    tdata = await self._post(URL_GAME_THUMBNAILS, batch)
                    for entry in tdata.get("data", []):
                        if entry.get("state") == "Completed":
                            req_id = entry.get("requestId", "")
                            uid = int(req_id.split("::")[0]) if "::" in req_id else None
                            if uid and uid in self._details_cache:
                                self._details_cache[uid]["thumbnail_url"] = entry.get("imageUrl")
                except RobloxApiError as err:
                    _LOGGER.warning("Klarte ikke hente thumbnails: %s", err)

        _default = {
            "name": "", "description": "", "playing": 0, "visits": 0, "genre": "",
            "root_place_id": None, "created": "", "updated": "",
            "creator_name": "", "creator_type": "", "creator_verified": False, "favorite_count": 0,
            "thumbnail_url": None, "screenshots": [],
            "age_rating": None, "minimum_age": None, "content_descriptors": [],
            "like_ratio": None, "up_votes": None, "down_votes": None,
            "name_history": [],
            "ai_verdict": None, "ai_summary": None, "ai_concerns": [], "ai_safe_age": None,
        }
        return {uid: self._details_cache.get(uid, {**_default, "name": str(uid)}) for uid in universe_ids}

    async def get_presence(self, child_id: int) -> dict:
        data = await self._post(URL_PRESENCE, {"userIds": [child_id]})
        users = data.get("userPresences", [])
        return users[0] if users else {}

    async def get_presences(self, user_ids: list[int]) -> dict[int, dict]:
        """Henter presence for flere brukere i ett kall. Returnerer {user_id: presence}."""
        if not user_ids:
            return {}
        data = await self._post(URL_PRESENCE, {"userIds": user_ids})
        return {int(p["userId"]): p for p in data.get("userPresences", []) if "userId" in p}

    async def get_robux_balance(self, user_id: int) -> int | None:
        # Economy-APIet returnerer 403 for barnekontoen — ikke en ekte auth-feil
        url = URL_ROBUX_BALANCE.format(user_id=user_id)
        session = await self._get_session()
        try:
            async with session.get(url) as resp:
                if resp.status in (401, 403, 404):
                    return None
                if resp.status == 429:
                    raise RobloxRateLimitError("Rate limited (429)")
                if resp.status >= 500:
                    raise RobloxApiError(f"Serverfeil {resp.status}")
                resp.raise_for_status()
                data = await resp.json(content_type=None)
                return data.get("robux")
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            raise RobloxApiError(f"Nettverksfeil: {err}") from err

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

    async def get_friends_in_same_game(self, child_id: int, universe_id: int) -> list[dict]:
        """Finn venner som spiller samme universe som barnet akkurat nå."""
        friend_ids = await self.get_friends(child_id)
        if not friend_ids:
            return []
        # Presence-APIet håndterer inntil 50 brukere per kall
        chunk_size = 50
        same_game: list[dict] = []
        for i in range(0, len(friend_ids), chunk_size):
            chunk = friend_ids[i: i + chunk_size]
            try:
                presences = await self.get_presences(chunk)
                for uid, p in presences.items():
                    if p.get("universeId") and int(p["universeId"]) == universe_id:
                        same_game.append({"id": uid, "name": str(uid)})
            except RobloxApiError:
                pass
        if not same_game:
            return []
        names = await self.get_profiles([f["id"] for f in same_game])
        for f in same_game:
            f["name"] = names.get(f["id"], str(f["id"]))
        return same_game

    @property
    def name_cache(self) -> dict[int, str]:
        return dict(self._name_cache)

    @property
    def details_cache(self) -> dict[int, dict]:
        return dict(self._details_cache)
