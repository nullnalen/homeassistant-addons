"""
AI-basert spillvurdering via lokal Ollama-instans.
Analyserer spilldata og gir en norsk foreldrevurdering.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any

import aiohttp

_LOGGER = logging.getLogger(__name__)

OLLAMA_BASE = os.environ.get("OLLAMA_URL", "http://192.168.1.28:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.1:8b")
OLLAMA_TIMEOUT = 60  # sekunder


def _build_prompt(game: dict) -> str:
    name = game.get("name", "Ukjent")
    description = (game.get("description") or "").strip()[:300]
    age_rating = game.get("age_rating") or "ukjent"
    minimum_age = game.get("minimum_age")
    descriptors = game.get("content_descriptors") or []
    creator_name = game.get("creator_name", "")
    creator_type = game.get("creator_type", "")
    creator_verified = game.get("creator_verified", False)
    like_ratio = game.get("like_ratio")
    playing = game.get("playing", 0)
    name_history = game.get("name_history") or []

    age_str = f"{age_rating}"
    if minimum_age:
        age_str += f" ({minimum_age}+)"

    creator_str = creator_name
    if creator_type == "Group":
        creator_str += " (gruppe"
        creator_str += ", verifisert)" if creator_verified else ", ikke verifisert)"
    else:
        creator_str += " (enkeltbruker"
        creator_str += ", verifisert)" if creator_verified else ", ikke verifisert)"

    lines = [
        f"Spill: {name}",
        f"Aldersanbefaling: {age_str}",
    ]
    if descriptors:
        lines.append(f"Innholdsdeskriptorer: {', '.join(descriptors)}")
    lines.append(f"Skapt av: {creator_str}")
    if like_ratio is not None:
        lines.append(f"Like-ratio: {like_ratio}%")
    if playing:
        lines.append(f"Spilles av: {playing:,} akkurat naa")
    if description:
        lines.append(f"Beskrivelse: {description}")
    if len(name_history) >= 3:
        lines.append(f"Navnehistorikk: {len(name_history)} navnebytter (mulig modererings-unnvikelse)")

    game_info = "\n".join(lines)

    return (
        f"Vurder dette Roblox-spillet for en norsk forelder. "
        f"Svar KUN med et JSON-objekt, ingen annen tekst.\n\n"
        f"{game_info}\n\n"
        f"Vurderingsnivaaer: gronn=greit for barn, gul=foreldres skjonn, rod=ikke anbefalt for barn.\n\n"
        f'Svar med JSON: {{"verdict":"gronn","sammendrag":"kort norsk setning maks 15 ord",'
        f'"bekymringer":["evt punkt"],"trygt_fra_alder":7}}'
    )


async def analyze_game(game: dict) -> dict | None:
    """Kjører AI-analyse på spilldata. Returnerer dict med verdict/sammendrag/bekymringer/trygt_fra_alder."""
    prompt = _build_prompt(game)
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {"temperature": 0.1, "num_predict": 150},
    }

    try:
        timeout = aiohttp.ClientTimeout(total=OLLAMA_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(f"{OLLAMA_BASE}/api/generate", json=payload) as resp:
                if resp.status != 200:
                    _LOGGER.warning("Ollama svarte %d for spill '%s'", resp.status, game.get("name"))
                    return None
                data = await resp.json(content_type=None)
    except Exception as err:
        _LOGGER.warning("Ollama tilkoblingsfeil for '%s': %s", game.get("name"), err)
        return None

    raw = data.get("response", "")
    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        _LOGGER.warning("Ollama svarte ikke-JSON for '%s': %s", game.get("name"), raw[:100])
        return None

    # Normaliser verdict til gyldige verdier
    verdict = str(result.get("verdict", "")).lower()
    if verdict not in ("gronn", "gul", "rod"):
        # Prøv å tolke fri tekst
        if any(w in verdict for w in ("rød", "rod", "red", "ikke", "avoid", "unngå")):
            verdict = "rod"
        elif any(w in verdict for w in ("gul", "yellow", "caution", "forsiktig", "skjønn")):
            verdict = "gul"
        else:
            verdict = "gronn"

    concerns = result.get("bekymringer") or result.get("concerns") or []
    if isinstance(concerns, str):
        concerns = [concerns] if concerns else []
    concerns = [str(c) for c in concerns if c and str(c).strip() != "evt punkt"][:3]

    safe_age = result.get("trygt_fra_alder") or result.get("safe_age")
    try:
        safe_age = int(safe_age) if safe_age is not None else None
    except (ValueError, TypeError):
        safe_age = None

    return {
        "verdict": verdict,
        "summary": str(result.get("sammendrag") or result.get("summary") or ""),
        "concerns": concerns,
        "safe_age": safe_age,
    }


async def check_ollama_available() -> bool:
    try:
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(f"{OLLAMA_BASE}/api/tags") as resp:
                return resp.status == 200
    except Exception:
        return False
