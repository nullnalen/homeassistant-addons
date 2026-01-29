#!/usr/bin/env python3
import os
import sys
import json
import re
import logging
import asyncio
import aiohttp
import mysql.connector
from datetime import datetime
from bs4 import BeautifulSoup

# RUN_LOCALLY blir False hvis miljøvariabelen ikke er satt eller ikke finnes.
RUN_LOCALLY = os.getenv("RUN_LOCALLY", "false").lower() == "true"
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.handlers.clear()
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(console_handler)

if RUN_LOCALLY:
    # Kjører lokalt — les konfig fra miljøvariabler
    logger.info("Kjører lokalt med testkonfig.")
    options = {
        "databasehost": os.getenv("DB_HOST", "localhost"),
        "databaseusername": os.getenv("DB_USER", ""),
        "databasepassword": os.getenv("DB_PASSWORD", ""),
        "databasename": os.getenv("DB_NAME", "finn_no"),
        "databaseport": os.getenv("DB_PORT", "3306")
    }
else:
    try:
        options_str = os.getenv("SUPERVISOR_OPTIONS", "{}")
        options = json.loads(options_str)
    except json.JSONDecodeError as e:
        logger.error("JSON-dekodingsfeil ved lasting av SUPERVISOR_OPTIONS: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.error("Ukjent feil ved lasting av SUPERVISOR_OPTIONS: %s", e)
        sys.exit(1)

FINN_API_BASE = "https://www.finn.no/mobility/search/api/search/SEARCH_ID_CAR_MOBILE_HOME"

def build_search_url(opts: dict) -> str:
    """
    Bygg søke-URL fra konfigurerbare parametere.
    """
    from urllib.parse import urlencode
    params = []
    locations = opts.get("locations", [])
    if not isinstance(locations, list):
        locations = [locations]
    for loc in locations:
        params.append(("location", loc))
    segments = opts.get("mobile_home_segments", [])
    if not isinstance(segments, list):
        segments = [segments]
    for seg in segments:
        params.append(("mobile_home_segment", seg))
    params.extend([
        ("price_from", opts.get("price_from", 300000)),
        ("price_to", opts.get("price_to", 700000)),
        ("mileage_to", opts.get("mileage_to", 122000)),
        ("year_from", opts.get("year_from", 2006)),
        ("no_of_sleepers_from", opts.get("no_of_sleepers_from", 4)),
        ("weight_to", opts.get("weight_to", 3501)),
        ("sort", opts.get("sort", "YEAR_DESC")),
    ])
    return f"{FINN_API_BASE}?{urlencode(params)}"

LISTINGS_PAGE_URL = build_search_url(options)
DRY_RUN = options.get("dry_run", False)
DATE_FORMAT = "%d. %b. %Y %H:%M"

DB_CONFIG = {
    "host": options.get("databasehost", ""),
    "user": options.get("databaseusername", ""),
    "passwd": options.get("databasepassword", ""),
    "database": options.get("databasename", ""),
    "port": options.get("databaseport", 3306)
}

def connect_to_database() -> mysql.connector.connection.MySQLConnection | None:
    """
    Koble til MySQL-databasen.
    """
    try:
        conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=10)
        logger.info("Koblet til databasen.")
        return conn
    except mysql.connector.Error as err:
        logger.error(f"Feil ved tilkobling til databasen: {err}")
        return None
    except Exception as e:
        logger.error(f"Uventet feil ved tilkobling til databasen: {e}")
        return None

async def fetch_json(session: aiohttp.ClientSession, url: str) -> dict | None:
    """
    Hent JSON-data fra gitt URL.
    Validerer at responsen inneholder forventet struktur.
    """
    logger.info("Henter JSON fra FINN API...")
    try:
        async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as response:
            if response.status != 200:
                logger.error(f"FINN API returnerte HTTP {response.status} for {url}")
                return None
            data = await response.json()
            if not isinstance(data, dict):
                logger.error(f"Uventet responstype fra FINN API: {type(data).__name__} (forventet dict)")
                return None
            if "docs" not in data:
                logger.error(
                    "FINN API-responsen mangler 'docs'-feltet. "
                    "API-strukturen kan ha endret seg. "
                    f"Nøkler i responsen: {list(data.keys())}"
                )
                return None
            return data
    except aiohttp.ContentTypeError as e:
        logger.error(f"FINN API returnerte ikke JSON (mulig HTML-feilside): {e}")
        return None
    except Exception as e:
        logger.error(f"Feil ved henting av JSON fra {url}: {e}")
        return None

async def fetch_all_pages(session: aiohttp.ClientSession, base_url: str) -> list[dict]:
    """
    Henter alle sider fra FINN API med paginering.
    """
    all_ads = []
    seen_ids = set()
    offset = 0

    logger.info("Henter første side med offset 0...")
    initial_data = await fetch_json(session, f"{base_url}&offset={offset}")
    if not initial_data:
        logger.error("Kunne ikke hente første side.")
        return []

    # Hent totalt antall treff fra metadata
    metadata = initial_data.get("metadata", {})
    result_size = metadata.get("result_size", {})
    total_matches = result_size.get("match_count", 0)
    if total_matches == 0:
        docs = initial_data.get("docs", [])
        if docs:
            total_matches = len(docs)
            logger.warning(f"match_count ikke funnet i metadata, bruker fallback: {total_matches}")
        else:
            logger.error("Ingen annonser funnet (docs er tom).")
            return []

    # Bruk faktisk sidesize fra første respons
    page_size = len(initial_data.get("docs", []))
    logger.info(f"Totalt antall annonser: {total_matches}, sidesize: {page_size}")

    # Hent og legg til første side (med dedup)
    for ad in extract_info_from_json(initial_data):
        if ad["Finnkode"] not in seen_ids:
            seen_ids.add(ad["Finnkode"])
            all_ads.append(ad)

    if page_size == 0:
        return all_ads

    # Hent videre sider med offset
    for offset in range(page_size, total_matches, page_size):
        logger.debug(f"Henter side med offset {offset}")
        await asyncio.sleep(0.2)  # Rate limit
        paged_url = f"{base_url}&offset={offset}"
        data = await fetch_json(session, paged_url)
        if data:
            for ad in extract_info_from_json(data):
                if ad["Finnkode"] not in seen_ids:
                    seen_ids.add(ad["Finnkode"])
                    all_ads.append(ad)
        else:
            logger.warning(f"Kunne ikke hente side med offset {offset}")

    logger.info(f"Totalt hentet {len(all_ads)} unike annonser (av {total_matches} treff fra API).")
    return all_ads


def extract_info_from_json(json_data: dict) -> list[dict]:
    """
    Ekstraher relevante felter fra FINN JSON-data.
    Validerer at hver annonse har kritiske felter.
    """
    try:
        ads = json_data.get("docs", [])
        if not ads:
            return []

        # Sjekk at første annonse har forventede nøkler
        first = ads[0]
        expected_keys = {"id", "heading", "canonical_url"}
        missing = expected_keys - set(first.keys())
        if missing:
            logger.error(
                f"FINN API-annonser mangler forventede felter: {missing}. "
                f"Tilgjengelige nøkler: {list(first.keys())}. "
                "API-strukturen kan ha endret seg."
            )
            return []

        extracted_data = []
        for ad in ads:
            finnkode = ad.get("id")
            url = ad.get("canonical_url")
            if not finnkode or not url:
                logger.warning(f"Hopper over annonse uten id/url: {ad.get('heading', 'ukjent')}")
                continue
            timestamp = ad.get("timestamp")
            formatted_date = datetime.fromtimestamp(timestamp / 1000).strftime(DATE_FORMAT) if timestamp else "Ukjent"
            extracted_data.append({
                "Finnkode": finnkode,
                "Annonsenavn": ad.get("heading"),
                "Pris": ad.get("price", {}).get("amount"),
                "Modell": ad.get("year"),
                "Kilometerstand": ad.get("mileage"),
                "Oppdatert": formatted_date,
                "URL": url,
                "Detaljer": {}
            })
        return extracted_data
    except Exception as e:
        logger.error(f"Feil ved ekstraksjon av JSON-data: {e}")
        return []

async def fetch_html(session: aiohttp.ClientSession, url: str) -> str | None:
    """
    Hent HTML-innhold fra gitt URL.
    """
    try:
        async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as response:
            response.raise_for_status()
            return await response.text()
    except Exception as e:
        logger.error(f"Feil ved henting av HTML fra {url}: {e}")
        return None

def extract_detailed_ad_info(html_content: str) -> dict:
    """
    Ekstraher detaljer fra annonse-HTML.
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        info_dict = {}

        spesifikasjoner = soup.find('dl', class_='emptycheck')
        if spesifikasjoner:
            items = spesifikasjoner.find_all(['dt', 'dd'])
            for i in range(0, len(items), 2):
                key = items[i].text.strip()
                value = items[i+1].text.strip()
                info_dict[key] = value

        desc_tag = soup.find('meta', property='og:description')
        # Sett beskrivelse både som egen nøkkel og i info_dict for konsistens
        beskrivelse = desc_tag['content'] if desc_tag else "Ikke tilgjengelig"
        info_dict["Beskrivelse"] = beskrivelse

        if RUN_LOCALLY:
            logger.debug("--- Detaljer fra annonse ---")
            for k, v in info_dict.items():
                logger.debug(f"{k}: {v}")

        return info_dict
    except Exception as e:
        logger.error(f"Feil under detaljuttrekk: {e}")
        return {}

async def fetch_and_combine_data(session, ads, max_concurrent=5):
    semaphore = asyncio.Semaphore(max_concurrent)

    async def fetch_details(ad):
        async with semaphore:
            await asyncio.sleep(0.2)
            html = await fetch_html(session, ad["URL"])
            if html:
                ad["Detaljer"] = extract_detailed_ad_info(html)
            return ad

    return await asyncio.gather(*(fetch_details(ad) for ad in ads))

def normalize_and_format_price(price: str, output_format: bool = True) -> str | int | None:
    """
    Normaliser og formater pris.
    Returnerer int hvis output_format=False, ellers formatert streng.
    """
    try:
        normalized = re.sub(r"[^\d]", "", str(price))
        price_as_int = int(normalized)
        if output_format:
            return f"{price_as_int:,.0f} kr".replace(",", " ")
        else:
            return price_as_int
    except Exception as e:
        logger.error(f"Feil ved formatering av pris: {e}")
        return None

def format_kilometerstand(km: str) -> str:
    """
    Formater kilometerstand.
    """
    try:
        normalized = re.sub(r"[^\d]", "", str(km))
        return f"{int(normalized):,} km".replace(",", " ")
    except Exception as e:
        logger.error(f"Feil ved formatering av kilometerstand: {e}")
        return "Ukjent"

def update_database(ads: list[dict], dry_run: bool = False) -> None:
    """
    Oppdater database med annonser.
    Logger eksplisitt hvis noen felt endres.
    Pris lagres som int i databasen.

    Hvis dry_run=True: kobler til DB og leser eksisterende data for sammenligning,
    men utfører ingen INSERT/UPDATE. Logger hva som ville blitt endret.
    """
    mode = "DRY RUN" if dry_run else "LIVE"
    logger.info(f"[{mode}] Starter databaseoppdatering for {len(ads)} annonser.")

    conn = connect_to_database()
    if not conn:
        if dry_run:
            logger.warning("[DRY RUN] Ingen DB-tilkobling — kan ikke sammenligne med eksisterende data.")
            _log_dry_run_summary(ads)
            return
        logger.error("Ingen tilkobling til databasen. Avbryter oppdatering.")
        return

    try:
        cursor = conn.cursor()
        if not ads:
            logger.warning("Ingen annonser å oppdatere i databasen.")

        nye_annonser = 0
        endrede_annonser = 0
        uendrede_annonser = 0

        for ad in ads:
            finnkode = ad["Finnkode"]
            ny_pris_int = normalize_and_format_price(ad["Pris"], output_format=False)
            if ny_pris_int is None:
                logger.error(f"Kan ikke lagre annonse {finnkode}: pris ikke gyldig ({ad['Pris']})")
                continue

            # Hent eksisterende verdier for alle felter
            cursor.execute(
                "SELECT Annonsenavn, Modell, Kilometerstand, Girkasse, Beskrivelse, Nyttelast, Typebobil, Oppdatert, URL, Pris FROM bobil WHERE Finnkode = %s",
                (finnkode,)
            )
            row = cursor.fetchone()
            felt_navn = [
                "Annonsenavn", "Modell", "Kilometerstand", "Girkasse", "Beskrivelse",
                "Nyttelast", "Typebobil", "Oppdatert", "URL", "Pris"
            ]
            nye_verdier = [
                ad["Annonsenavn"],
                ad["Modell"],
                format_kilometerstand(ad["Kilometerstand"]),
                ad["Detaljer"].get("Girkasse", "Ikke oppgitt"),
                ad["Detaljer"].get("Beskrivelse", "Ikke tilgjengelig"),
                ad["Detaljer"].get("Nyttelast", "Ikke oppgitt"),
                ad["Detaljer"].get("Type bobil", "Ikke oppgitt"),
                ad["Oppdatert"],
                ad["URL"],
                ny_pris_int
            ]
            if row:
                endringer = []
                for idx, (gammel, ny) in enumerate(zip(row, nye_verdier)):
                    if felt_navn[idx] == "Pris":
                        try:
                            gammel_int = int(re.sub(r"[^\d]", "", str(gammel)))
                        except Exception:
                            gammel_int = None
                        if gammel_int != ny:
                            endringer.append(f"{felt_navn[idx]}: {gammel_int} -> {ny}")
                    else:
                        if str(gammel) != str(ny):
                            endringer.append(f"{felt_navn[idx]}: '{gammel}' -> '{ny}'")
                if endringer:
                    endrede_annonser += 1
                    logger.info(f"[{mode}] Endringer for Finnkode {finnkode}: {', '.join(endringer)}")
                else:
                    uendrede_annonser += 1
            else:
                nye_annonser += 1
                logger.info(f"[{mode}] Ny annonse: Finnkode {finnkode} — {ad['Annonsenavn']} ({normalize_and_format_price(ad['Pris'])})")

            if not dry_run:
                query = """
                    INSERT INTO bobil (Finnkode, Annonsenavn, Modell, Kilometerstand, Girkasse, Beskrivelse, Nyttelast, Typebobil, Oppdatert, URL, Pris)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        Annonsenavn = VALUES(Annonsenavn),
                        Modell = VALUES(Modell),
                        Kilometerstand = VALUES(Kilometerstand),
                        Girkasse = VALUES(Girkasse),
                        Beskrivelse = VALUES(Beskrivelse),
                        Nyttelast = VALUES(Nyttelast),
                        Typebobil = VALUES(Typebobil),
                        Oppdatert = VALUES(Oppdatert),
                        URL = VALUES(URL),
                        Pris = VALUES(Pris)
                """
                data = (
                    finnkode,
                    ad["Annonsenavn"],
                    ad["Modell"],
                    format_kilometerstand(ad["Kilometerstand"]),
                    ad["Detaljer"].get("Girkasse", "Ikke oppgitt"),
                    ad["Detaljer"].get("Beskrivelse", "Ikke tilgjengelig"),
                    ad["Detaljer"].get("Nyttelast", "Ikke oppgitt"),
                    ad["Detaljer"].get("Type bobil", "Ikke oppgitt"),
                    ad["Oppdatert"],
                    ad["URL"],
                    ny_pris_int
                )
                try:
                    cursor.execute(query, data)
                except Exception as e:
                    logger.error(f"Feil ved lagring av annonse {finnkode}: {e}")

        if not dry_run:
            conn.commit()

        logger.info(
            f"[{mode}] Oppsummering: {nye_annonser} nye, {endrede_annonser} endret, "
            f"{uendrede_annonser} uendret av {len(ads)} annonser."
        )
    except mysql.connector.Error as err:
        logger.error(f"Feil ved databaseoppdatering: {err}")
    except Exception as e:
        logger.error(f"Uventet feil ved databaseoppdatering: {e}")
    finally:
        conn.close()


def _log_dry_run_summary(ads: list[dict]) -> None:
    """Logg en oppsummering av hentet data når DB ikke er tilgjengelig i dry_run."""
    logger.info(f"[DRY RUN] Hentet {len(ads)} annonser fra FINN:")
    for ad in ads[:5]:
        logger.info(
            f"  {ad['Finnkode']} — {ad['Annonsenavn']} — "
            f"{normalize_and_format_price(ad['Pris'])} — {ad['Modell']}"
        )
    if len(ads) > 5:
        logger.info(f"  ... og {len(ads) - 5} til.")

async def main() -> None:
    """
    Hovedfunksjon for scriptet.
    """
    logger.info("Starter script...")
    if DRY_RUN:
        logger.info("*** DRY RUN MODUS — ingen data vil bli skrevet til databasen ***")
    logger.info(f"Søke-URL: {LISTINGS_PAGE_URL}")
    async with aiohttp.ClientSession() as session:
        ads_data = await fetch_all_pages(session, LISTINGS_PAGE_URL)
        if not ads_data:
            logger.error("Ingen annonser hentet fra FINN API.")
            return

        detailed_ads = await fetch_and_combine_data(session, ads_data)

        update_database(detailed_ads, dry_run=DRY_RUN)

    logger.info("Avslutter script...")


if __name__ == "__main__":
    asyncio.run(main())
