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
AUTODB_SEARCH_URL = "https://www.autodb.no/s/extsearch/"
AUTODB_DETAIL_URL = "https://www.autodb.no/a/view"

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
                "SELECT Annonsenavn, Modell, Kilometerstand, Girkasse, Beskrivelse, Nyttelast, Typebobil, Oppdatert, URL, Pris, ImageURL, Lokasjon, Kjennemerke, SvvMerke, SvvHandelsbetegnelse, SvvTypebetegnelse, SvvAarsmodell, SvvForstegangNorge, SvvRegistreringsstatus, SvvEuKontrollfrist, SvvEuSistGodkjent, SvvFarge, SvvKarosseritype, SvvAntallDorer, SvvDrivstoff, SvvMotorvolum, SvvMotoreffekt, SvvAntallSylindre, SvvGirkassetype, SvvAntallGir, SvvMaksHastighet, SvvElektrisk, SvvLengde, SvvBredde, SvvHoyde, SvvEgenvekt, SvvNyttelast, SvvTotalvekt, SvvTillattTotalvekt, SvvTilhengervektMedBrems, SvvTilhengervektUtenBrems, SvvVertikalKoplingslast, SvvEuroKlasse, SvvSitteplasser, SvvKjoretoytype, Sengelayout, VendbareForerstoler, Heftelser, HeftelseSjekket FROM bobil WHERE Finnkode = %s",
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
                "Sengelayout", "VendbareForerstoler",
                "Heftelser", "HeftelseSjekket",
            ]
            tekst_nlp = " ".join(filter(None, [
                ad.get("Annonsenavn", ""),
                ad["Detaljer"].get("Beskrivelse", ""),
            ]))
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
                detect_sengelayout(tekst_nlp),
                detect_vendbare_forseter(tekst_nlp),
                ad.get("Heftelser"),
                ad.get("HeftelseSjekket"),
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
                            # Ikke rapporter SVV-felt som "endret" til None — DB beholder eksisterende verdi
                            if ny is None and felt_navn[idx].startswith("Svv"):
                                continue
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
                placeholders = ", ".join(["%s"] * (19 + len(svv_cols)))
                query = f"""
                    INSERT INTO bobil (
                        Finnkode, Annonsenavn, Modell, Kilometerstand, Girkasse, Beskrivelse,
                        Nyttelast, Typebobil, Oppdatert, URL, Pris, ImageURL, Lokasjon,
                        Kjennemerke, {", ".join(svv_cols)},
                        Sengelayout, VendbareForerstoler, Heftelser, HeftelseSjekket, Kilde
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
                        {svv_upsert},
                        Sengelayout = IF(VALUES(Sengelayout) IS NOT NULL, VALUES(Sengelayout), Sengelayout),
                        VendbareForerstoler = IF(VALUES(VendbareForerstoler) IS NOT NULL, VALUES(VendbareForerstoler), VendbareForerstoler),
                        Heftelser = IF(VALUES(Heftelser) IS NOT NULL, VALUES(Heftelser), Heftelser),
                        HeftelseSjekket = IF(VALUES(HeftelseSjekket) IS NOT NULL, VALUES(HeftelseSjekket), HeftelseSjekket),
                        Kilde = IF(Kilde = 'autodb', 'finn+autodb', IF(Kilde IS NULL, 'finn', Kilde))
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
                    detect_sengelayout(tekst_nlp),
                    detect_vendbare_forseter(tekst_nlp),
                    ad.get("Heftelser"),
                    ad.get("HeftelseSjekket"),
                    "finn",
                )
                try:
                    cursor.execute(query, data)
                except Exception as e:
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

        # Hent aktive annonser (kun Finn-kildet) som ikke er sett på over 48 timer
        cursor.execute(
            "SELECT Finnkode FROM bobil WHERE (Solgt = 0 OR Solgt IS NULL) "
            "AND Finnkode > 0 "
            "AND (Kilde IS NULL OR Kilde IN ('finn', 'finn+autodb')) "
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


AUTODB_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "autodb-cookie-consent": "none",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}


async def fetch_autodb_pages(session: aiohttp.ClientSession, opts: dict) -> list[dict]:
    """
    Hent alle sider fra autodb.no søke-API med paginering.
    Returnerer liste av rå autodb-annonser (list-API-felter).
    """
    price_from = opts.get("price_from", 300000)
    price_to = opts.get("price_to", 700000)
    mileage_to = opts.get("mileage_to", 122000)
    year_from = opts.get("year_from", 2006)

    params = {
        "type": "hmaMobileHome",
        "price": f"{price_from}-{price_to}",
        "km": f"-{mileage_to}",
        "yearmodel": f"{year_from}-",
        "limit": 30,
    }

    all_ads = []
    seen_ids = set()
    page = 0

    while True:
        params["page"] = page
        from urllib.parse import urlencode
        url = AUTODB_SEARCH_URL + "?" + urlencode(params)
        logger.info(f"Henter autodb side {page}...")
        try:
            async with session.get(
                url,
                headers={**AUTODB_HEADERS, "Referer": "https://www.autodb.no/"},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status != 200:
                    logger.error(f"autodb søke-API HTTP {resp.status} på side {page}")
                    break
                data = await resp.json()
        except Exception as e:
            logger.error(f"Feil ved henting av autodb side {page}: {e}")
            break

        ads = data.get("data", [])
        total = data.get("count", 0)
        limit = data.get("limit", 30)

        if not ads:
            break

        for ad in ads:
            aid = ad.get("aditemid")
            if aid and aid not in seen_ids:
                seen_ids.add(aid)
                all_ads.append(ad)

        logger.info(f"autodb side {page}: {len(ads)} annonser (totalt {total})")

        if (page + 1) * limit >= total:
            break
        page += 1
        await asyncio.sleep(0.5)

    logger.info(f"autodb: hentet totalt {len(all_ads)} unike annonser")
    return all_ads


async def fetch_autodb_detail(session: aiohttp.ClientSession, aditemid: int) -> dict | None:
    """
    Hent detaljdata for én autodb-annonse, inkl. kjennemerke.
    """
    url = f"{AUTODB_DETAIL_URL}?idlist={aditemid}"
    try:
        async with session.get(
            url,
            headers={
                **AUTODB_HEADERS,
                "Referer": f"https://www.autodb.no/view/{aditemid}",
            },
            timeout=aiohttp.ClientTimeout(total=20),
        ) as resp:
            if resp.status != 200:
                logger.debug(f"autodb detalj HTTP {resp.status} for {aditemid}")
                return None
            return await resp.json()
    except Exception as e:
        logger.warning(f"Feil ved autodb-detalj for {aditemid}: {e}")
        return None


def parse_autodb_ad(list_ad: dict, detail: dict | None) -> dict:
    """
    Kombiner autodb liste- og detaljdata til et standardisert annonseobjekt.
    Bruker samme nøkkelstruktur som Finn.no-annonser der mulig.
    """
    aditemid = list_ad.get("aditemid")

    # Kjennemerke fra detaljrespons — feltet ligger i typedata.regNo (skjult hvis hideRegNo=True)
    kjennemerke = ""
    if detail:
        items = detail if isinstance(detail, list) else [detail]
        for item in items:
            td = item.get("typedata") or {}
            hidden = td.get("hideRegNo", False)
            regnr = "" if hidden else (td.get("regNo") or "")
            if not regnr:
                # Fallback: sjekk toppnivå-felter
                regnr = (item.get("licenseplate") or item.get("regno") or item.get("registrationNumber") or "")
            regnr = regnr.strip().upper().replace(" ", "")
            if regnr:
                kjennemerke = regnr
                break

    km = list_ad.get("km") or 0
    pris = list_ad.get("price") or 0
    yearmodel = list_ad.get("yearmodel")
    title = list_ad.get("title") or f"AutoDB {aditemid}"
    main_img = list_ad.get("mainImageId")
    img_url = f"https://www.autodb.no/assets/img/items/{main_img}.jpg" if main_img else ""

    return {
        "AutodbId": aditemid,
        "Finnkode": None,
        "Annonsenavn": title,
        "Pris": pris,
        "Modell": yearmodel,
        "Kilometerstand": km,
        "Oppdatert": list_ad.get("timeModified") or list_ad.get("timePublished") or "",
        "URL": f"https://www.autodb.no/b/{aditemid}",
        "ImageURL": img_url,
        "Lokasjon": list_ad.get("ccounty") or "",
        "Kjennemerke": kjennemerke,
        "Understellsnummer": "",
        "Detaljer": {
            "Beskrivelse": "",
            "Girkasse": "Ikke oppgitt",
            "Nyttelast": "Ikke oppgitt",
            "Type bobil": "Ikke oppgitt",
        },
        "Kilde": "autodb",
    }


async def fetch_and_enrich_autodb(session: aiohttp.ClientSession, opts: dict) -> list[dict]:
    """Hent alle autodb-annonser og berik med detaljdata (inkl. kjennemerke)."""
    list_ads = await fetch_autodb_pages(session, opts)
    if not list_ads:
        return []

    semaphore = asyncio.Semaphore(4)

    async def enrich_one(list_ad):
        aditemid = list_ad.get("aditemid")
        if not aditemid:
            return None
        async with semaphore:
            await asyncio.sleep(0.3)
            detail = await fetch_autodb_detail(session, aditemid)
        return parse_autodb_ad(list_ad, detail)

    results = await asyncio.gather(*(enrich_one(ad) for ad in list_ads))
    return [r for r in results if r is not None]


def get_existing_kjennemerker() -> dict:
    """
    Returner {kjennemerke: finnkode} for alle bobiler i databasen med kjennemerke.
    Brukes til dedup mellom Finn og autodb.
    """
    conn = connect_to_database()
    if not conn:
        return {}
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT Kjennemerke, Finnkode FROM bobil WHERE Kjennemerke IS NOT NULL AND Kjennemerke != ''"
        )
        return {row[0]: row[1] for row in cursor.fetchall()}
    except Exception as e:
        logger.warning(f"Kunne ikke hente eksisterende kjennemerker: {e}")
        return {}
    finally:
        conn.close()


def update_database_autodb(ads: list[dict], existing_kjennemerker: dict, dry_run: bool = False) -> None:
    """
    Lagre autodb-annonser i databasen.
    - Hopper over hvis samme kjennemerke allerede finnes (Finn.no vinner).
    - Bruker AutodbId som primærnøkkel (negativ, for å unngå kollisjon med Finnkode).
    - Oppdaterer Kilde-kolonnen på eksisterende Finn-annonser til 'finn+autodb'.
    """
    mode = "DRY RUN" if dry_run else "LIVE"
    conn = connect_to_database()
    if not conn:
        return

    try:
        cursor = conn.cursor()
        nye = 0
        duplikat = 0
        oppdatert_kilde = 0

        for ad in ads:
            kjennemerke = ad.get("Kjennemerke") or ""
            autodb_id = ad.get("AutodbId")

            # Dedup: samme kjennemerke finnes allerede (Finn.no-oppføring)
            if kjennemerke and kjennemerke in existing_kjennemerker:
                finn_finnkode = existing_kjennemerker[kjennemerke]
                duplikat += 1
                logger.debug(f"autodb {autodb_id} — kjennemerke {kjennemerke} finnes som Finnkode {finn_finnkode}, oppdaterer kilde")
                if not dry_run:
                    cursor.execute(
                        "UPDATE bobil SET Kilde = 'finn+autodb', AutodbId = %s WHERE Finnkode = %s AND (Kilde = 'finn' OR Kilde IS NULL)",
                        (autodb_id, finn_finnkode),
                    )
                continue

            # Ny autodb-eksklusiv annonse — bruk negativ AutodbId som Finnkode-surrogate
            surrogate_finnkode = -int(autodb_id) if autodb_id else None
            if surrogate_finnkode is None:
                continue

            ny_pris_int = int(ad.get("Pris") or 0) or None
            if not ny_pris_int:
                logger.warning(f"autodb {autodb_id}: ingen pris, hopper over")
                continue

            km_raw = ad.get("Kilometerstand") or 0
            km_str = f"{int(km_raw):,} km".replace(",", " ") if km_raw else "Ikke oppgitt"

            oppdatert_raw = ad.get("Oppdatert") or ""
            try:
                if oppdatert_raw:
                    dt = datetime.fromisoformat(oppdatert_raw.replace("Z", "+00:00"))
                    oppdatert_str = dt.strftime(DATE_FORMAT)
                else:
                    oppdatert_str = "Ukjent"
            except Exception:
                oppdatert_str = oppdatert_raw[:16] if oppdatert_raw else "Ukjent"

            svv = ad.get("VegvesenData") or {}
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
            svv_data = (
                svv.get("svv_merke"), svv.get("svv_handelsbetegnelse"), svv.get("svv_typebetegnelse"),
                svv.get("svv_aarsmodell"), svv.get("svv_forstegang_norge"), svv.get("svv_registreringsstatus"),
                svv.get("svv_eu_kontrollfrist") or None, svv.get("svv_eu_sist_godkjent") or None,
                svv.get("svv_farge"), svv.get("svv_karosseritype"), svv.get("svv_antall_dorer"),
                svv.get("svv_drivstoff"), svv.get("svv_motorvolum"), svv.get("svv_motoreffekt"), svv.get("svv_antall_sylindre"),
                svv.get("svv_girkassetype"), svv.get("svv_antall_gir"), svv.get("svv_maks_hastighet"), svv.get("svv_elektrisk"),
                svv.get("svv_lengde"), svv.get("svv_bredde"), svv.get("svv_hoyde"),
                svv.get("svv_egenvekt"), svv.get("svv_nyttelast"), svv.get("svv_totalvekt"), svv.get("svv_tillatt_totalvekt"),
                svv.get("svv_tilhengervekt_med_brems"), svv.get("svv_tilhengervekt_uten_brems"), svv.get("svv_vertikal_koplingslast"),
                svv.get("svv_euro_klasse"), svv.get("svv_sitteplasser"), svv.get("svv_kjoretoytype"),
            )
            svv_upsert = ",\n                        ".join(
                f"{c} = IF(VALUES({c}) IS NOT NULL, VALUES({c}), {c})" for c in svv_cols
            )
            tekst_nlp = ad.get("Annonsenavn", "") or ""
            placeholders_a = ", ".join(["%s"] * (20 + len(svv_cols)))

            if not dry_run:
                try:
                    cursor.execute(f"""
                        INSERT INTO bobil (
                            Finnkode, AutodbId, Annonsenavn, Modell, Kilometerstand,
                            Girkasse, Beskrivelse, Nyttelast, Typebobil,
                            Oppdatert, URL, Pris, ImageURL, Lokasjon, Kjennemerke,
                            {", ".join(svv_cols)},
                            Sengelayout, VendbareForerstoler, Heftelser, HeftelseSjekket,
                            Kilde
                        ) VALUES ({placeholders_a})
                        ON DUPLICATE KEY UPDATE
                            Annonsenavn = VALUES(Annonsenavn),
                            Modell = VALUES(Modell),
                            Kilometerstand = VALUES(Kilometerstand),
                            Oppdatert = VALUES(Oppdatert),
                            URL = VALUES(URL),
                            Pris = VALUES(Pris),
                            ImageURL = VALUES(ImageURL),
                            Lokasjon = VALUES(Lokasjon),
                            Kjennemerke = VALUES(Kjennemerke),
                            {svv_upsert},
                            Heftelser = IF(VALUES(Heftelser) IS NOT NULL, VALUES(Heftelser), Heftelser),
                            HeftelseSjekket = IF(VALUES(HeftelseSjekket) IS NOT NULL, VALUES(HeftelseSjekket), HeftelseSjekket),
                            Kilde = VALUES(Kilde)
                    """, (
                        surrogate_finnkode,
                        autodb_id,
                        ad["Annonsenavn"],
                        ad.get("Modell"),
                        km_str,
                        "Ikke oppgitt",
                        "",
                        "Ikke oppgitt",
                        "Ikke oppgitt",
                        oppdatert_str,
                        ad["URL"],
                        ny_pris_int,
                        ad.get("ImageURL", ""),
                        ad.get("Lokasjon", ""),
                        kjennemerke,
                        *svv_data,
                        detect_sengelayout(tekst_nlp),
                        detect_vendbare_forseter(tekst_nlp),
                        ad.get("Heftelser"),
                        ad.get("HeftelseSjekket"),
                        "autodb",
                    ))
                    # Logg startpris
                    cursor.execute(
                        "INSERT IGNORE INTO prisendringer (Finnkode, Pris) VALUES (%s, %s)",
                        (surrogate_finnkode, ny_pris_int),
                    )
                    nye += 1
                    logger.info(f"[autodb] Ny annonse: {autodb_id} — {ad['Annonsenavn']} ({ny_pris_int})")
                except Exception as e:
                    logger.error(f"Feil ved lagring av autodb {autodb_id}: {e}")
            else:
                nye += 1

        if not dry_run:
            conn.commit()

        logger.info(
            f"[{mode}] autodb: {nye} nye, {duplikat} duplikater (samme kjennemerke som Finn)"
        )
    except Exception as e:
        logger.error(f"Feil i update_database_autodb: {e}")
    finally:
        conn.close()


async def main() -> None:
    """
    Hovedfunksjon for scriptet.
    """
    logger.info("Starter script...")
    if DRY_RUN:
        logger.info("*** DRY RUN MODUS — ingen data vil bli skrevet til databasen ***")
    logger.info(f"Søke-URL: {LISTINGS_PAGE_URL}")
    async with aiohttp.ClientSession() as session:
        # --- Finn.no ---
        ads_data = await fetch_all_pages(session, LISTINGS_PAGE_URL)
        if not ads_data:
            logger.error("Ingen annonser hentet fra FINN API.")
        else:
            detailed_ads = await fetch_and_combine_data(session, ads_data)
            detailed_ads = await enrich_ads_with_vegvesen(session, detailed_ads)
            detailed_ads = await enrich_ads_with_heftelser(session, detailed_ads)
            update_database(detailed_ads, dry_run=DRY_RUN)
            mark_removed_ads(detailed_ads, dry_run=DRY_RUN)

        # --- autodb.no ---
        logger.info("Starter autodb.no-scraping...")
        autodb_ads = await fetch_and_enrich_autodb(session, options)
        if autodb_ads:
            autodb_ads = await enrich_ads_with_vegvesen(session, autodb_ads)
            autodb_ads = await enrich_ads_with_heftelser(session, autodb_ads)
            existing_kjennemerker = get_existing_kjennemerker()
            update_database_autodb(autodb_ads, existing_kjennemerker, dry_run=DRY_RUN)
        else:
            logger.warning("Ingen annonser hentet fra autodb.no.")

    logger.info("Avslutter script...")


def run_scraper():
    """Wrapper for å kjøre scraperen. Kan kalles fra bobil_web.py."""
    asyncio.run(main())


if __name__ == "__main__":
    run_scraper()


# -- NLP-analyse av beskrivelse -------------------------------------------

SENGE_MØNSTRE = {
    'senkeseng': r'senkeseng|heve.?senk',
    'køyer': r'køyer|koyer|køye\b|koye\b',
    'alkove': r'alkove',
    'enkelsenger': r'enkle senger|langsgående senger|enkelt.?seng',
    'queenbed': r'queen.?bed|queenbed|queen bed',
    'dobbeltseng': r'dobbeltseng|dobbelt.?seng',
}

VENDBAR_MØNSTRE = {
    True: r'kan snu|vendbar|snubar|snu begge|vendbare',
    False: r'kan ikke snu|ikke snubar|ikke vendbar',
}


def detect_sengelayout(tekst: str) -> str | None:
    if not tekst:
        return None
    t = tekst.lower()
    for navn, pattern in SENGE_MØNSTRE.items():
        if re.search(pattern, t):
            return navn
    return None


def detect_vendbare_forseter(tekst: str) -> int | None:
    if not tekst:
        return None
    t = tekst.lower()
    if re.search(VENDBAR_MØNSTRE[False], t):
        return 0
    if re.search(VENDBAR_MØNSTRE[True], t):
        return 1
    return None


# -- Brønnøysund heftelsessjekk -------------------------------------------

BRREG_URL = "https://rettsstiftelser.brreg.no/nb/oppslag/motorvogn/{kjennemerke}"

# Mønster som matcher "Det er ingen oppføringer"-teksten i HTML
_BRREG_INGEN_RE = re.compile(r"Det er ingen oppf", re.IGNORECASE)
# Mønster som teller antall heftelser fra heading-teksten, f.eks. "3 rettsstiftelser"
_BRREG_ANTALL_RE = re.compile(r"(\d+)\s+rettsstiftels", re.IGNORECASE)


async def fetch_heftelser(session: aiohttp.ClientSession, kjennemerke: str) -> int | None:
    """
    Sjekk om kjøretøyet har aktive heftelser/pant i Brønnøysund Løsøreregisteret.
    Brreg bruker server-side rendered Next.js — ingen JSON API, parser HTML.
    Returnerer antall heftelser (0 = ingen), None ved nettverksfeil.
    """
    if not kjennemerke:
        return None
    regnr = kjennemerke.strip().upper().replace(" ", "")
    url = BRREG_URL.format(kjennemerke=regnr)
    try:
        async with session.get(
            url,
            headers={
                "Accept": "text/html,application/xhtml+xml,*/*",
                "User-Agent": "Mozilla/5.0 (compatible; finn-bobil-addon/1.0)",
                "Referer": "https://www.brreg.no/",
            },
            timeout=aiohttp.ClientTimeout(total=20),
            allow_redirects=True,
        ) as resp:
            if resp.status != 200:
                logger.debug(f"Brreg {regnr}: HTTP {resp.status}")
                return None
            html = await resp.text()

            if _BRREG_INGEN_RE.search(html):
                logger.info(f"Brreg {regnr}: ingen heftelser")
                return 0

            m = _BRREG_ANTALL_RE.search(html)
            if m:
                antall = int(m.group(1))
                logger.info(f"Brreg {regnr}: {antall} heftelse(r) funnet")
                return antall

            # Siden lastet men vi klarte ikke tolke svaret
            logger.warning(f"Brreg {regnr}: ukjent sideformat, kunne ikke telle heftelser")
            return None
    except Exception as e:
        logger.warning(f"Feil ved Brreg-oppslag for {regnr}: {e}")
        return None


def get_finnkoder_med_heftelsessjekk() -> set:
    """Returner sett av finnkoder som allerede har fått heftelsessjekk (ikke NULL HeftelseSjekket)."""
    conn = connect_to_database()
    if not conn:
        return set()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT Finnkode FROM bobil WHERE HeftelseSjekket IS NOT NULL")
        return {row[0] for row in cursor.fetchall()}
    except Exception as e:
        logger.warning(f"Kunne ikke hente finnkoder med heftelsessjekk: {e}")
        return set()
    finally:
        conn.close()


def save_heftelser(finnkode: int, antall: int | None) -> None:
    """Lagre heftelsesresultat i databasen."""
    if antall is None:
        return
    conn = connect_to_database()
    if not conn:
        return
    try:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE bobil SET Heftelser = %s, HeftelseSjekket = %s WHERE Finnkode = %s",
            (antall, datetime.now(), finnkode),
        )
        conn.commit()
    except Exception as e:
        logger.error(f"Feil ved lagring av heftelser for {finnkode}: {e}")
    finally:
        conn.close()


async def enrich_ads_with_heftelser(session: aiohttp.ClientSession, ads: list[dict]) -> list[dict]:
    """Berik annonser med heftelsesdata fra Brønnøysund."""
    har_sjekket = get_finnkoder_med_heftelsessjekk()
    semaphore = asyncio.Semaphore(3)

    async def sjekk(ad):
        if ad["Finnkode"] in har_sjekket:
            return ad
        kjennemerke = ad.get("Kjennemerke", "") or ""
        if not kjennemerke:
            return ad
        async with semaphore:
            await asyncio.sleep(0.3)
            antall = await fetch_heftelser(session, kjennemerke)
            if antall is not None:
                ad["Heftelser"] = antall
                ad["HeftelseSjekket"] = datetime.now()
        return ad

    return list(await asyncio.gather(*(sjekk(ad) for ad in ads)))


# -- SVV km-historikk -------------------------------------------------------

SVV_KM_API_URL = "https://akfell-datautlevering.atlas.vegvesen.no/enkeltoppslag/kjoretoydata"


def ensure_km_historikk_table() -> None:
    """Opprett km_historikk-tabellen hvis den ikke finnes."""
    conn = connect_to_database()
    if not conn:
        return
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS km_historikk (
                id INT AUTO_INCREMENT PRIMARY KEY,
                Finnkode INT NOT NULL,
                Dato VARCHAR(20) NOT NULL,
                Km INT NOT NULL,
                Kilde VARCHAR(50) DEFAULT 'SVV',
                UNIQUE KEY uq_finnkode_dato (Finnkode, Dato),
                KEY idx_finnkode (Finnkode)
            )
        """)
        conn.commit()
    except Exception as e:
        logger.error(f"Feil ved oppretting av km_historikk: {e}")
    finally:
        conn.close()


async def fetch_svv_km_historikk(session: aiohttp.ClientSession, kjennemerke: str, api_key: str) -> list[dict]:
    """
    Hent EU-kontrollhistorikk med km-stand fra SVV Autosys.
    Returnerer liste av {Dato, Km} eller tom liste.
    Km-historikk ligger i kjøretøydataListe[0].godkjenning.periodiskeKontroller
    eller som eget objekt avhengig av API-versjon.
    """
    if not kjennemerke or not api_key:
        return []
    url = SVV_KM_API_URL + "?kjennemerke=" + kjennemerke.strip().upper().replace(" ", "")
    try:
        async with session.get(
            url,
            headers={"SVV-Authorization": "Apikey " + api_key, "Accept": "application/json"},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
    except Exception as e:
        logger.warning(f"Feil ved SVV km-oppslag for {kjennemerke}: {e}")
        return []

    try:
        liste = data.get("kjoretoydataListe", [])
        k = liste[0] if liste else data

        resultater = []

        # Forsøk 1: periodiskeKontroller direkte på kjøretøyet
        kontroller = k.get("periodiskeKontroller", [])

        # Forsøk 2: under godkjenning
        if not kontroller:
            kontroller = k.get("godkjenning", {}).get("periodiskeKontroller", [])

        # Forsøk 3: under godkjenning.kjoretoygodkjenning
        if not kontroller:
            kontroller = (
                k.get("godkjenning", {})
                .get("kjoretoygodkjenning", {})
                .get("periodiskeKontroller", [])
            )

        for kontroll in kontroller:
            dato = kontroll.get("kontrollDato") or kontroll.get("dato") or kontroll.get("godkjentDato")
            km = kontroll.get("kmStand") or kontroll.get("kilometerstand") or kontroll.get("kmstand")
            if dato and km is not None:
                try:
                    resultater.append({"Dato": str(dato)[:10], "Km": int(km)})
                except (ValueError, TypeError):
                    pass

        if resultater:
            logger.info(f"SVV km-historikk for {kjennemerke}: {len(resultater)} kontroller")
        else:
            logger.debug(
                f"SVV km-historikk for {kjennemerke}: ingen kontroller funnet i responsen. "
                "Merk: enkeltoppslag/kjoretoydata returnerer ikke alltid periodiskeKontroller "
                "med kmStand — dette endepunktet må verifiseres mot SVV-dokumentasjonen."
            )
        return resultater
    except Exception as e:
        logger.warning(f"Feil ved parsing av SVV km-historikk for {kjennemerke}: {e}")
        return []


def save_km_historikk(finnkode: int, km_data: list[dict]) -> None:
    """Lagre km-historikk i databasen (INSERT IGNORE for å unngå duplikater)."""
    if not km_data:
        return
    conn = connect_to_database()
    if not conn:
        return
    try:
        cursor = conn.cursor()
        cursor.executemany(
            "INSERT IGNORE INTO km_historikk (Finnkode, Dato, Km) VALUES (%s, %s, %s)",
            [(finnkode, d["Dato"], d["Km"]) for d in km_data],
        )
        conn.commit()
        logger.debug(f"Lagret {cursor.rowcount} km-datapunkter for Finnkode {finnkode}")
    except Exception as e:
        logger.error(f"Feil ved lagring av km-historikk for {finnkode}: {e}")
    finally:
        conn.close()


def get_finnkoder_med_km_historikk() -> set:
    """Returner sett av finnkoder som allerede har km-historikk lagret."""
    conn = connect_to_database()
    if not conn:
        return set()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT Finnkode FROM km_historikk")
        return {row[0] for row in cursor.fetchall()}
    except Exception:
        return set()
    finally:
        conn.close()


async def enrich_ads_with_km_historikk(session: aiohttp.ClientSession, ads: list[dict]) -> list[dict]:
    """Berik annonser med EU-kontroll km-historikk fra SVV."""
    api_key = options.get("vegvesen_api_key") or os.getenv("VEGVESEN_API_KEY")
    if not api_key:
        return ads

    ensure_km_historikk_table()
    har_km = get_finnkoder_med_km_historikk()
    semaphore = asyncio.Semaphore(2)

    async def hent(ad):
        if ad["Finnkode"] in har_km:
            return ad
        kjennemerke = ad.get("Kjennemerke", "") or ""
        if not kjennemerke:
            return ad
        async with semaphore:
            await asyncio.sleep(0.5)
            km_data = await fetch_svv_km_historikk(session, kjennemerke, api_key)
            if km_data:
                save_km_historikk(ad["Finnkode"], km_data)
                ad["KmHistorikk"] = km_data
        return ad

    return list(await asyncio.gather(*(hent(ad) for ad in ads)))


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
            elif resp.status == 204:
                logger.debug("Vegvesen API 204 for " + ident + " — ingen data, markerer i DB")
                return {"svv_registreringsstatus": "INGEN_DATA"}
            else:
                logger.debug("Vegvesen API HTTP " + str(resp.status) + " for " + ident)
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
        result["svv_kjoretoytype"] = td.get("generelt", {}).get("tekniskKode", {}).get("kodeNavn")

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
        result["svv_karosseritype"] = (karosseri.get("karosseritype") or {}).get("kodeNavn")
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
        result["svv_euro_klasse"] = (miljo.get("euroKlasse") or {}).get("kodeVerdi")

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

def get_finnkoder_med_svv_data():
    """Returner sett av finnkoder som allerede har SVV-data i databasen."""
    conn = connect_to_database()
    if not conn:
        return set()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT Finnkode FROM bobil WHERE SvvMerke IS NOT NULL OR SvvRegistreringsstatus = 'INGEN_DATA'")
        return {row[0] for row in cursor.fetchall()}
    except Exception as e:
        logger.warning(f"Kunne ikke hente finnkoder med SVV-data: {e}")
        return set()
    finally:
        conn.close()

async def enrich_ads_with_vegvesen(session, ads):
    if not get_svv_api_key():
        logger.info("Vegvesen API-nokkel ikke satt, hopper over SVV-oppslag.")
        return ads
    har_svv = get_finnkoder_med_svv_data()
    semaphore = asyncio.Semaphore(3)
    async def enrich(ad):
        if ad["Finnkode"] in har_svv:
            ad["VegvesenData"] = {}
            return ad
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
