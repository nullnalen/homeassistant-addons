#!/usr/bin/env python3
import os
import sys
import json
import re
import logging
import asyncio
import aiohttp
import mysql.connector
from datetime import datetime, timedelta
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
DATE_FORMAT = "%d. %m. %Y %H:%M"

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

async def fetch_json(session: aiohttp.ClientSession, url: str, max_retries: int = 3) -> dict | None:
    """
    Hent JSON-data fra gitt URL med retry ved feil.
    Validerer at responsen inneholder forventet struktur.
    """
    logger.info("Henter JSON fra FINN API...")
    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 429:
                    wait = 2 ** attempt
                    logger.warning(f"Rate limited (429), venter {wait}s (forsøk {attempt}/{max_retries})")
                    await asyncio.sleep(wait)
                    continue
                if response.status >= 500:
                    wait = 2 ** attempt
                    logger.warning(f"Serverfeil HTTP {response.status}, venter {wait}s (forsøk {attempt}/{max_retries})")
                    await asyncio.sleep(wait)
                    continue
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
        except (aiohttp.ContentTypeError, aiohttp.ClientError, asyncio.TimeoutError) as e:
            wait = 2 ** attempt
            logger.warning(f"Nettverksfeil (forsøk {attempt}/{max_retries}): {e}. Venter {wait}s...")
            if attempt < max_retries:
                await asyncio.sleep(wait)
            else:
                logger.error(f"Ga opp etter {max_retries} forsøk for {url}: {e}")
                return None
        except Exception as e:
            logger.error(f"Uventet feil ved henting av JSON fra {url}: {e}")
            return None
    return None

async def fetch_all_pages(session: aiohttp.ClientSession, base_url: str) -> list[dict]:
    """
    Henter alle sider fra FINN API med paginering.
    """
    all_ads = []
    seen_ids = set()
    page = 1

    logger.info("Henter side 1...")
    initial_data = await fetch_json(session, f"{base_url}&page={page}")
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
    total_pages = (total_matches + page_size - 1) // page_size if page_size > 0 else 1
    logger.info(f"Totalt antall annonser: {total_matches}, sidesize: {page_size}, sider: {total_pages}")

    # Hent og legg til første side (med dedup)
    for ad in extract_info_from_json(initial_data):
        if ad["Finnkode"] not in seen_ids:
            seen_ids.add(ad["Finnkode"])
            all_ads.append(ad)

    # Hent videre sider
    for page in range(2, total_pages + 1):
        logger.debug(f"Henter side {page} av {total_pages}")
        await asyncio.sleep(0.2)  # Rate limit
        paged_url = f"{base_url}&page={page}"
        data = await fetch_json(session, paged_url)
        if data:
            for ad in extract_info_from_json(data):
                if ad["Finnkode"] not in seen_ids:
                    seen_ids.add(ad["Finnkode"])
                    all_ads.append(ad)
        else:
            logger.warning(f"Kunne ikke hente side {page}")

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
            # Hent bilde-URL fra API (første bilde)
            images = ad.get("images", [])
            image_url = ""
            if images:
                img = images[0] if isinstance(images[0], dict) else {}
                image_url = img.get("url", img.get("uri", ""))
                # Finn.no bruker ofte path-baserte URLs
                if image_url and not image_url.startswith("http"):
                    image_url = f"https://images.finncdn.no/dynamic/480x360c/{image_url}"

            # Hent lokasjon
            location = ad.get("location", "")
            if isinstance(location, dict):
                location = location.get("name", "")

            # Hent kjennemerke og understellsnummer direkte fra Finn API-JSON
            regno_raw = ad.get("regno", "") or ""
            regno = regno_raw.strip().upper().replace(" ", "")
            chassis = (ad.get("chassis_number", "") or "").strip().upper()

            extracted_data.append({
                "Finnkode": finnkode,
                "Annonsenavn": ad.get("heading"),
                "Pris": ad.get("price", {}).get("amount"),
                "Modell": ad.get("year"),
                "Kilometerstand": ad.get("mileage"),
                "Oppdatert": formatted_date,
                "URL": url,
                "ImageURL": image_url,
                "Lokasjon": location,
                "Detaljer": {},
                "Kjennemerke": regno,
                "Understellsnummer": chassis,
            })
        return extracted_data
    except Exception as e:
        logger.error(f"Feil ved ekstraksjon av JSON-data: {e}")
        return []

async def fetch_html(session: aiohttp.ClientSession, url: str, max_retries: int = 3) -> str | None:
    """
    Hent HTML-innhold fra gitt URL med retry ved feil.
    """
    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 429 or response.status >= 500:
                    wait = 2 ** attempt
                    logger.warning(f"HTTP {response.status} for {url}, venter {wait}s (forsøk {attempt}/{max_retries})")
                    if attempt < max_retries:
                        await asyncio.sleep(wait)
                        continue
                    return None
                response.raise_for_status()
                return await response.text()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            wait = 2 ** attempt
            logger.warning(f"Nettverksfeil for {url} (forsøk {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                await asyncio.sleep(wait)
            else:
                logger.error(f"Ga opp HTML-henting etter {max_retries} forsøk for {url}: {e}")
                return None
        except Exception as e:
            logger.error(f"Uventet feil ved henting av HTML fra {url}: {e}")
            return None
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
        nye_titler = []
        prisfall_titler = []
        nye_prislogger = []  # (finnkode, pris) for nye annonser — skrives etter bobil-INSERT

        for ad in ads:
            finnkode = ad["Finnkode"]
            ny_pris_int = normalize_and_format_price(ad["Pris"], output_format=False)
            if ny_pris_int is None:
                logger.error(f"Kan ikke lagre annonse {finnkode}: pris ikke gyldig ({ad['Pris']})")
                continue

            # Hent eksisterende verdier for alle felter
            cursor.execute(
                "SELECT Annonsenavn, Modell, Kilometerstand, Girkasse, Beskrivelse, Nyttelast, Typebobil, Oppdatert, URL, Pris, ImageURL, Lokasjon, Kjennemerke, SvvMerke, SvvHandelsbetegnelse, SvvTypebetegnelse, SvvAarsmodell, SvvForstegangNorge, SvvRegistreringsstatus, SvvEuKontrollfrist, SvvEuSistGodkjent, SvvFarge, SvvKarosseritype, SvvAntallDorer, SvvDrivstoff, SvvMotorvolum, SvvMotoreffekt, SvvAntallSylindre, SvvGirkassetype, SvvAntallGir, SvvMaksHastighet, SvvElektrisk, SvvLengde, SvvBredde, SvvHoyde, SvvEgenvekt, SvvNyttelast, SvvTotalvekt, SvvTillattTotalvekt, SvvTilhengervektMedBrems, SvvTilhengervektUtenBrems, SvvVertikalKoplingslast, SvvEuroKlasse, SvvSitteplasser, SvvKjoretoytype FROM bobil WHERE Finnkode = %s",
                (finnkode,)
            )
            row = cursor.fetchone()
            svv = ad.get("VegvesenData") or {}
            felt_navn = [
                "Annonsenavn", "Modell", "Kilometerstand", "Girkasse", "Beskrivelse",
                "Nyttelast", "Typebobil", "Oppdatert", "URL", "Pris", "ImageURL", "Lokasjon",
                "Kjennemerke", "SvvMerke", "SvvHandelsbetegnelse", "SvvTypebetegnelse",
                "SvvAarsmodell", "SvvForstegangNorge", "SvvRegistreringsstatus",
                "SvvEuKontrollfrist", "SvvEuSistGodkjent",
                "SvvFarge", "SvvKarosseritype", "SvvAntallDorer",
                "SvvDrivstoff", "SvvMotorvolum", "SvvMotoreffekt", "SvvAntallSylindre",
                "SvvGirkassetype", "SvvAntallGir", "SvvMaksHastighet", "SvvElektrisk",
                "SvvLengde", "SvvBredde", "SvvHoyde",
                "SvvEgenvekt", "SvvNyttelast", "SvvTotalvekt", "SvvTillattTotalvekt",
                "SvvTilhengervektMedBrems", "SvvTilhengervektUtenBrems", "SvvVertikalKoplingslast",
                "SvvEuroKlasse", "SvvSitteplasser", "SvvKjoretoytype",
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
                ny_pris_int,
                ad.get("ImageURL", ""),
                ad.get("Lokasjon", ""),
                ad.get("Kjennemerke", "") or "",
                svv.get("svv_merke"),
                svv.get("svv_handelsbetegnelse"),
                svv.get("svv_typebetegnelse"),
                svv.get("svv_aarsmodell"),
                svv.get("svv_forstegang_norge"),
                svv.get("svv_registreringsstatus"),
                svv.get("svv_eu_kontrollfrist"),
                svv.get("svv_eu_sist_godkjent"),
                svv.get("svv_farge"),
                svv.get("svv_karosseritype"),
                svv.get("svv_antall_dorer"),
                svv.get("svv_drivstoff"),
                svv.get("svv_motorvolum"),
                svv.get("svv_motoreffekt"),
                svv.get("svv_antall_sylindre"),
                svv.get("svv_girkassetype"),
                svv.get("svv_antall_gir"),
                svv.get("svv_maks_hastighet"),
                svv.get("svv_elektrisk"),
                svv.get("svv_lengde"),
                svv.get("svv_bredde"),
                svv.get("svv_hoyde"),
                svv.get("svv_egenvekt"),
                svv.get("svv_nyttelast"),
                svv.get("svv_totalvekt"),
                svv.get("svv_tillatt_totalvekt"),
                svv.get("svv_tilhengervekt_med_brems"),
                svv.get("svv_tilhengervekt_uten_brems"),
                svv.get("svv_vertikal_koplingslast"),
                svv.get("svv_euro_klasse"),
                svv.get("svv_sitteplasser"),
                svv.get("svv_kjoretoytype"),
            ]
            if row:
                endringer = []
                pris_endret = False
                for idx, (gammel, ny) in enumerate(zip(row, nye_verdier)):
                    if felt_navn[idx] == "Pris":
                        try:
                            gammel_int = int(re.sub(r"[^\d]", "", str(gammel)))
                        except Exception:
                            gammel_int = None
                        if gammel_int != ny:
                            endringer.append(f"{felt_navn[idx]}: {gammel_int} -> {ny}")
                            pris_endret = True
                    else:
                        if str(gammel) != str(ny):
                            endringer.append(f"{felt_navn[idx]}: '{gammel}' -> '{ny}'")
                if endringer:
                    endrede_annonser += 1
                    logger.info(f"[{mode}] Endringer for Finnkode {finnkode}: {', '.join(endringer)}")
                    # Spor prisfall for varsling
                    if pris_endret:
                        try:
                            gammel_pris = int(re.sub(r"[^\d]", "", str(row[felt_navn.index("Pris")])))
                            if ny_pris_int < gammel_pris:
                                diff = gammel_pris - ny_pris_int
                                prisfall_titler.append(
                                    f"{ad['Annonsenavn']}: {normalize_and_format_price(gammel_pris)} → {normalize_and_format_price(ny_pris_int)} (-{normalize_and_format_price(diff)})"
                                )
                        except Exception:
                            pass
                    # Logg prisendring til prisendringer-tabellen
                    if pris_endret and not dry_run:
                        try:
                            cursor.execute(
                                "INSERT INTO prisendringer (Finnkode, Pris) VALUES (%s, %s)",
                                (finnkode, ny_pris_int)
                            )
                        except Exception as e:
                            logger.error(f"Feil ved logging av prisendring for {finnkode}: {e}")
                else:
                    uendrede_annonser += 1
            else:
                nye_annonser += 1
                nye_titler.append(f"{ad['Annonsenavn']} ({normalize_and_format_price(ad['Pris'])})")
                logger.info(f"[{mode}] Ny annonse: Finnkode {finnkode} — {ad['Annonsenavn']} ({normalize_and_format_price(ad['Pris'])})")
                # Samle opp for prislogg etter bobil-INSERT (FK-rekkefølge)
                if not dry_run:
                    nye_prislogger.append((finnkode, ny_pris_int))

            if not dry_run:
                # Bygg SVV ON DUPLICATE KEY UPDATE-del dynamisk for alle SVV-felter
                svv_cols = [
                    "SvvMerke", "SvvHandelsbetegnelse", "SvvTypebetegnelse",
                    "SvvAarsmodell", "SvvForstegangNorge", "SvvRegistreringsstatus",
                    "SvvEuKontrollfrist", "SvvEuSistGodkjent",
                    "SvvFarge", "SvvKarosseritype", "SvvAntallDorer",
                    "SvvDrivstoff", "SvvMotorvolum", "SvvMotoreffekt", "SvvAntallSylindre",
                    "SvvGirkassetype", "SvvAntallGir", "SvvMaksHastighet", "SvvElektrisk",
                    "SvvLengde", "SvvBredde", "SvvHoyde",
                    "SvvEgenvekt", "SvvNyttelast", "SvvTotalvekt", "SvvTillattTotalvekt",
                    "SvvTilhengervektMedBrems", "SvvTilhengervektUtenBrems", "SvvVertikalKoplingslast",
                    "SvvEuroKlasse", "SvvSitteplasser", "SvvKjoretoytype",
                ]
                svv_upsert = ",\n                        ".join(
                    f"{c} = IF(VALUES({c}) IS NOT NULL, VALUES({c}), {c})" for c in svv_cols
                )
                placeholders = ", ".join(["%s"] * (14 + len(svv_cols)))
                query = f"""
                    INSERT INTO bobil (
                        Finnkode, Annonsenavn, Modell, Kilometerstand, Girkasse, Beskrivelse,
                        Nyttelast, Typebobil, Oppdatert, URL, Pris, ImageURL, Lokasjon,
                        Kjennemerke, {", ".join(svv_cols)}
                    ) VALUES ({placeholders})
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
                        Pris = VALUES(Pris),
                        ImageURL = VALUES(ImageURL),
                        Lokasjon = VALUES(Lokasjon),
                        Kjennemerke = VALUES(Kjennemerke),
                        {svv_upsert}
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
                    ny_pris_int,
                    ad.get("ImageURL", ""),
                    ad.get("Lokasjon", ""),
                    ad.get("Kjennemerke", "") or "",
                    svv.get("svv_merke"),
                    svv.get("svv_handelsbetegnelse"),
                    svv.get("svv_typebetegnelse"),
                    svv.get("svv_aarsmodell"),
                    svv.get("svv_forstegang_norge"),
                    svv.get("svv_registreringsstatus"),
                    svv.get("svv_eu_kontrollfrist") or None,
                    svv.get("svv_eu_sist_godkjent") or None,
                    svv.get("svv_farge"),
                    svv.get("svv_karosseritype"),
                    svv.get("svv_antall_dorer"),
                    svv.get("svv_drivstoff"),
                    svv.get("svv_motorvolum"),
                    svv.get("svv_motoreffekt"),
                    svv.get("svv_antall_sylindre"),
                    svv.get("svv_girkassetype"),
                    svv.get("svv_antall_gir"),
                    svv.get("svv_maks_hastighet"),
                    svv.get("svv_elektrisk"),
                    svv.get("svv_lengde"),
                    svv.get("svv_bredde"),
                    svv.get("svv_hoyde"),
                    svv.get("svv_egenvekt"),
                    svv.get("svv_nyttelast"),
                    svv.get("svv_totalvekt"),
                    svv.get("svv_tillatt_totalvekt"),
                    svv.get("svv_tilhengervekt_med_brems"),
                    svv.get("svv_tilhengervekt_uten_brems"),
                    svv.get("svv_vertikal_koplingslast"),
                    svv.get("svv_euro_klasse"),
                    svv.get("svv_sitteplasser"),
                    svv.get("svv_kjoretoytype"),
                )
                try:
                    cursor.execute(query, data)
                except Exception as e:
                    # Finn hvilken verdi som er en dict for debugging
                    bad = [(i, type(v).__name__, repr(v)[:60]) for i, v in enumerate(data) if isinstance(v, (dict, list))]
                    if bad:
                        logger.error(f"Feil ved lagring av annonse {finnkode}: dict/list-verdier på posisjon(er): {bad}")
                    else:
                        logger.error(f"Feil ved lagring av annonse {finnkode}: {e}")

        if not dry_run:
            conn.commit()
            # Logg første pris for nye annonser etter bobil-INSERT er commitet (FK-krav)
            for fk, pris in nye_prislogger:
                try:
                    cursor.execute(
                        "INSERT INTO prisendringer (Finnkode, Pris) VALUES (%s, %s)",
                        (fk, pris)
                    )
                except Exception as e:
                    logger.error(f"Feil ved logging av første pris for {fk}: {e}")
            if nye_prislogger:
                conn.commit()

        logger.info(
            f"[{mode}] Oppsummering: {nye_annonser} nye, {endrede_annonser} endret, "
            f"{uendrede_annonser} uendret av {len(ads)} annonser."
        )

        # Send HA-varsling ved nye annonser eller prisfall
        if not dry_run and (nye_titler or prisfall_titler):
            parts = []
            if nye_titler:
                parts.append(f"**{len(nye_titler)} nye annonser:**\n" + "\n".join(f"- {t}" for t in nye_titler[:10]))
                if len(nye_titler) > 10:
                    parts.append(f"... og {len(nye_titler) - 10} til")
            if prisfall_titler:
                parts.append(f"**{len(prisfall_titler)} prisfall:**\n" + "\n".join(f"- {t}" for t in prisfall_titler[:10]))
                if len(prisfall_titler) > 10:
                    parts.append(f"... og {len(prisfall_titler) - 10} til")
            send_ha_notification("Bobil-oppdatering", "\n\n".join(parts))
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

def mark_removed_ads(current_ads: list[dict], dry_run: bool = False) -> None:
    """
    Oppdater SistSett for aktive annonser, og marker som solgt de som ikke
    har vært i søkeresultatet på over 48 timer. Dette unngår feilmerking av
    annonser som midlertidig faller utenfor søkefilteret (f.eks. prisøkning).
    """
    mode = "DRY RUN" if dry_run else "LIVE"
    conn = connect_to_database()
    if not conn:
        return

    try:
        cursor = conn.cursor()

        # Sørg for at SistSett-kolonnen finnes
        try:
            cursor.execute("ALTER TABLE bobil ADD COLUMN SistSett DATETIME NULL")
            logger.info("La til kolonne SistSett i bobil-tabellen.")
            conn.commit()
        except Exception as e:
            if "Duplicate column" not in str(e) and "1060" not in str(e):
                logger.error(f"Feil ved ALTER TABLE SistSett: {e}")

        active_ids = {ad["Finnkode"] for ad in current_ads}
        now = datetime.now()

        # Oppdater SistSett for alle annonser vi ser i dag
        if not dry_run and active_ids:
            cursor.executemany(
                "UPDATE bobil SET SistSett = %s WHERE Finnkode = %s",
                [(now, fk) for fk in active_ids]
            )

        # Hent aktive annonser som ikke er sett på over 48 timer
        cursor.execute(
            "SELECT Finnkode FROM bobil WHERE (Solgt = 0 OR Solgt IS NULL) "
            "AND (SistSett IS NULL OR SistSett < %s)",
            (now - timedelta(hours=48),)
        )
        stale_ids = {row[0] for row in cursor.fetchall()} - active_ids

        if not stale_ids:
            logger.info(f"[{mode}] Ingen annonser å markere som solgt.")
            if not dry_run:
                conn.commit()
            return

        logger.info(f"[{mode}] {len(stale_ids)} annonser ikke sett på over 48t — markeres som solgt.")

        for finnkode in stale_ids:
            logger.info(f"[{mode}] Markerer Finnkode {finnkode} som Solgt/Fjernet.")
            if not dry_run:
                cursor.execute(
                    "UPDATE bobil SET Solgt = 1 WHERE Finnkode = %s",
                    (finnkode,)
                )
                try:
                    cursor.execute(
                        "INSERT INTO prisendringer (Finnkode, Pris) VALUES (%s, %s)",
                        (finnkode, "Solgt/Fjernet")
                    )
                except Exception as e:
                    logger.error(f"Feil ved logging av solgt-status for {finnkode}: {e}")

        if not dry_run:
            conn.commit()
        logger.info(f"[{mode}] Markerte {len(stale_ids)} annonser som Solgt/Fjernet.")
    except Exception as e:
        logger.error(f"Feil ved markering av fjernede annonser: {e}")
    finally:
        conn.close()


def send_ha_notification(title: str, message: str) -> None:
    """Send varsling til Home Assistant via Supervisor API."""
    import urllib.request
    token = os.getenv("SUPERVISOR_TOKEN")
    if not token:
        logger.debug("Ingen SUPERVISOR_TOKEN, hopper over HA-varsling.")
        return
    try:
        data = json.dumps({"title": title, "message": message}).encode()
        req = urllib.request.Request(
            "http://supervisor/core/api/services/persistent_notification/create",
            data=data,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
        logger.info("HA-varsling sendt: %s", title)
    except Exception as e:
        logger.warning("Kunne ikke sende HA-varsling: %s", e)


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

        detailed_ads = await enrich_ads_with_vegvesen(session, detailed_ads)

        update_database(detailed_ads, dry_run=DRY_RUN)

        # Marker annonser som ikke lenger finnes i søkeresultatene
        mark_removed_ads(detailed_ads, dry_run=DRY_RUN)

    logger.info("Avslutter script...")


def run_scraper():
    """Wrapper for å kjøre scraperen. Kan kalles fra bobil_web.py."""
    asyncio.run(main())


if __name__ == "__main__":
    run_scraper()


# -- Vegvesen-integrasjon --------------------------------------------------

SVV_API_URL = "https://akfell-datautlevering.atlas.vegvesen.no/enkeltoppslag/kjoretoydata"

def get_svv_api_key():
    return options.get("vegvesen_api_key") or os.getenv("VEGVESEN_API_KEY")

async def fetch_vegvesen_data(session, kjennemerke=None, chassis=None):
    api_key = get_svv_api_key()
    if not api_key:
        logger.debug("Ingen Vegvesen API-nokkel konfigurert.")
        return None
    if kjennemerke:
        param = "kjennemerke=" + kjennemerke.strip().upper().replace(" ", "")
        ident = kjennemerke
    elif chassis:
        param = "understellsnummer=" + chassis.strip().upper()
        ident = chassis
    else:
        return None
    url = SVV_API_URL + "?" + param
    logger.info("Vegvesen-oppslag for " + ident + "...")
    try:
        async with session.get(
            url,
            headers={"SVV-Authorization": "Apikey " + api_key, "Accept": "application/json"},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                logger.info("Vegvesen-data hentet for " + ident)
                return parse_vegvesen_data(data)
            else:
                text = await resp.text()
                logger.warning("Vegvesen API HTTP " + str(resp.status) + " for " + ident + ": " + text[:200])
                return None
    except Exception as e:
        logger.warning("Feil ved Vegvesen-oppslag for " + ident + ": " + str(e))
        return None

def parse_vegvesen_data(data):
    result = {}
    try:
        liste = data.get("kjoretoydataListe", [])
        k = liste[0] if liste else data

        td = k.get("godkjenning", {}).get("tekniskGodkjenning", {}).get("tekniskeData", {})

        # Generelt
        merke_list = td.get("generelt", {}).get("merke", [{}])
        result["svv_merke"] = merke_list[0].get("merke") if merke_list else None
        handel = td.get("generelt", {}).get("handelsbetegnelse") or []
        result["svv_handelsbetegnelse"] = handel[0] if handel else None
        result["svv_typebetegnelse"] = td.get("generelt", {}).get("typebetegnelse")
        result["svv_kjoretoytype"] = td.get("generelt", {}).get("tekniskKode", {}).get("kodeBeskrivelse")

        # Årsmodell + første registrering Norge
        forsteg_dato = k.get("forstegangsregistrering", {}).get("registrertForstegangNorgeDato", "")
        if forsteg_dato and len(forsteg_dato) >= 4:
            result["svv_aarsmodell"] = int(forsteg_dato[:4])
            result["svv_forstegang_norge"] = forsteg_dato
        else:
            result["svv_aarsmodell"] = None
            result["svv_forstegang_norge"] = None

        # Registreringsstatus
        reg = k.get("registrering", {})
        result["svv_registreringsstatus"] = reg.get("registreringsstatus", {}).get("kodeBeskrivelse")

        # EU-kontroll (dato kommer som streng YYYY-MM-DD fra API)
        pkk = k.get("periodiskKjoretoyKontroll", {})
        result["svv_eu_kontrollfrist"] = str(pkk["kontrollfrist"]) if pkk.get("kontrollfrist") else None
        result["svv_eu_sist_godkjent"] = str(pkk["sistGodkjent"]) if pkk.get("sistGodkjent") else None

        # Karosseri og farge
        karosseri = td.get("karosseriOgLasteplan", {})
        farge = karosseri.get("rFarge", [{}])
        result["svv_farge"] = farge[0].get("kodeBeskrivelse") if farge else None
        result["svv_karosseritype"] = (karosseri.get("karosseritype") or {}).get("kodeBeskrivelse")
        result["svv_antall_dorer"] = karosseri.get("antallDorer", [None])[0] if karosseri.get("antallDorer") else None

        # Motor og drivverk
        motor_driv = td.get("motorOgDrivverk", {})
        motor = motor_driv.get("motor", [{}])
        m0 = motor[0] if motor else {}
        drivstoff = m0.get("drivstoff", [{}])
        result["svv_drivstoff"] = drivstoff[0].get("drivstoffKode", {}).get("kodeBeskrivelse") if drivstoff else None
        result["svv_motorvolum"] = m0.get("slagvolum")
        result["svv_motoreffekt"] = drivstoff[0].get("maksNettoEffekt") if drivstoff else None
        result["svv_antall_sylindre"] = m0.get("antallSylindre")
        result["svv_girkassetype"] = (motor_driv.get("girkassetype") or {}).get("kodeBeskrivelse")
        result["svv_antall_gir"] = motor_driv.get("antallGir")
        result["svv_maks_hastighet"] = motor_driv.get("maksimumHastighet", [None])[0] if motor_driv.get("maksimumHastighet") else None
        elektrisk = motor_driv.get("utelukkendeElektriskDrift") or motor_driv.get("hybridElektriskKjoretoy")
        result["svv_elektrisk"] = bool(elektrisk) if elektrisk is not None else None

        # Dimensjoner
        dim = td.get("dimensjoner", {})
        result["svv_lengde"] = dim.get("lengde")
        result["svv_bredde"] = dim.get("bredde")
        result["svv_hoyde"] = dim.get("hoyde")

        # Vekter
        vekter = td.get("vekter", {})
        result["svv_egenvekt"] = vekter.get("egenvekt")
        result["svv_nyttelast"] = vekter.get("nyttelast")
        result["svv_totalvekt"] = vekter.get("tekniskTillattTotalvekt")
        result["svv_tillatt_totalvekt"] = vekter.get("tillattTotalvekt")
        result["svv_tilhengervekt_med_brems"] = vekter.get("tillattTilhengervektMedBrems")
        result["svv_tilhengervekt_uten_brems"] = vekter.get("tillattTilhengervektUtenBrems")
        result["svv_vertikal_koplingslast"] = vekter.get("tillattVertikalKoplingslast")

        # Miljø
        miljo = td.get("miljodata", {})
        result["svv_euro_klasse"] = miljo.get("euroKlasse")

        # Sitteplasser
        result["svv_sitteplasser"] = td.get("persontall", {}).get("sitteplasserTotalt")
    except Exception:
        pass
    return result

def extract_regnr_from_ad(ad):
    """Returner (kjennemerke, understellsnummer) for Vegvesen-oppslag.
    Prøver JSON-felter fra Finn API først, deretter HTML-scraped Detaljer."""
    import re as _re

    # 1. Kjennemerke direkte fra Finn API-JSON
    kjennemerke = ad.get("Kjennemerke", "")
    if kjennemerke and _re.match(r"^[A-Z]{2}[0-9]{4,5}$", kjennemerke):
        return kjennemerke, None

    # 2. Understellsnummer direkte fra Finn API-JSON
    chassis = ad.get("Understellsnummer", "")
    if chassis and len(chassis) >= 5:
        return None, chassis

    # 3. Fallback: HTML-scraped Detaljer
    detaljer = ad.get("Detaljer", {})
    for key in ["Registreringsnummer", "Reg.nr.", "Reg.nr", "Kjennemerke", "Skiltnummer"]:
        val = detaljer.get(key, "")
        if val:
            clean = val.strip().upper().replace(" ", "")
            if _re.match(r"^[A-Z]{2}[0-9]{4,5}$", clean):
                return clean, None

    return None, None

async def enrich_ads_with_vegvesen(session, ads):
    if not get_svv_api_key():
        logger.info("Vegvesen API-nokkel ikke satt, hopper over SVV-oppslag.")
        return ads
    semaphore = asyncio.Semaphore(3)
    async def enrich(ad):
        kjennemerke, chassis = extract_regnr_from_ad(ad)
        if not kjennemerke and not chassis:
            ad["VegvesenData"] = {}
            return ad
        async with semaphore:
            await asyncio.sleep(0.3)
            svv = await fetch_vegvesen_data(session, kjennemerke=kjennemerke, chassis=chassis)
            ad["VegvesenData"] = svv or {}
            if svv:
                ident = kjennemerke or chassis
                logger.info("  Finnkode " + str(ad["Finnkode"]) + ": SVV OK (" + ident + ") - " + str(svv.get("svv_merke")) + " " + str(svv.get("svv_handelsbetegnelse")))
        return ad
    return list(await asyncio.gather(*(enrich(ad) for ad in ads)))
