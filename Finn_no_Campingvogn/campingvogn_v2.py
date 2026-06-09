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
from urllib.parse import urlencode
from bs4 import BeautifulSoup

HTTP_TIMEOUT = aiohttp.ClientTimeout(total=30)
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0"}
MAX_RETRIES = 3

RUN_LOCALLY = os.getenv("RUN_LOCALLY", "false").lower() == "true"
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.handlers.clear()
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(console_handler)

if RUN_LOCALLY:
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

FINN_API_BASE = "https://www.finn.no/mobility/search/api/search/SEARCH_ID_CAR_CARAVAN"
TABLE = "campingvogn_elbil"
PRISENDRINGER_TABLE = "campingvogn_elbil_prisendringer"

# Finn.no caravan API bruker streng-range for soveplasser, f.eks. "2", "3-4", "5-6", "7+"
_SLEEPER_RANGES = ["2", "3-4", "5-6", "7+"]

def build_search_url(opts: dict) -> str:
    params = []
    locations = opts.get("locations", [])
    if not isinstance(locations, list):
        locations = [locations]
    for loc in locations:
        params.append(("location", loc))

    price_from = opts.get("price_from", 50000)
    price_to = opts.get("price_to", 500000)
    year_from = opts.get("year_from", 2010)
    weight_to = opts.get("weight_to", 2000)
    min_sleepers = opts.get("no_of_sleepers_from", 2)

    # Legg til alle soveplasser-ranger som er >= min_sleepers
    for r in _SLEEPER_RANGES:
        low = int(r.split("-")[0].replace("+", ""))
        if low >= min_sleepers:
            params.append(("no_of_sleepers", r))

    params.extend([
        ("price_from", price_from),
        ("price_to", price_to),
        ("year_from", year_from),
        ("weight_to", weight_to),
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
    "database": options.get("databasename", "finn_no"),
    "port": options.get("databaseport", 3306)
}

def connect_to_database() -> mysql.connector.connection.MySQLConnection | None:
    try:
        conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=10)
        logger.info("Koblet til databasen.")
        return conn
    except mysql.connector.Error as err:
        logger.error("Feil ved tilkobling til databasen: %s", err)
        return None
    except Exception as e:
        logger.error("Uventet feil ved tilkobling til databasen: %s", e)
        return None


async def fetch_json(session: aiohttp.ClientSession, url: str, max_retries: int = MAX_RETRIES) -> dict | None:
    logger.info("Henter JSON fra %s", url)
    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(url, headers=HTTP_HEADERS, timeout=HTTP_TIMEOUT) as response:
                if response.status == 429 or response.status >= 500:
                    wait = 2 ** attempt
                    logger.warning("HTTP %s for %s, venter %ds (forsøk %d/%d)", response.status, url, wait, attempt, max_retries)
                    await asyncio.sleep(wait)
                    continue
                if response.status != 200:
                    logger.error("HTTP %s for %s", response.status, url)
                    return None
                data = await response.json()
                if not isinstance(data, dict):
                    logger.error("Uventet responstype: %s fra %s", type(data).__name__, url)
                    return None
                if "docs" not in data:
                    logger.error("'docs'-feltet mangler i respons fra %s. Nøkler: %s", url, list(data.keys()))
                    return None
                return data
        except (aiohttp.ContentTypeError, aiohttp.ClientError, asyncio.TimeoutError) as e:
            wait = 2 ** attempt
            logger.warning("Nettverksfeil (forsøk %d/%d) for %s: %s", attempt, max_retries, url, e)
            if attempt < max_retries:
                await asyncio.sleep(wait)
            else:
                logger.error("Ga opp etter %d forsøk for %s: %s", max_retries, url, e)
                return None
        except Exception as e:
            logger.error("Uventet feil ved henting av JSON fra %s: %s", url, e)
            return None
    return None


async def fetch_all_pages(session: aiohttp.ClientSession, base_url: str) -> list[dict]:
    all_ads = []
    seen_ids = set()
    page = 1

    initial_data = await fetch_json(session, f"{base_url}&page={page}")
    if not initial_data:
        logger.error("Finn.no: Kunne ikke hente første side.")
        return []

    metadata = initial_data.get("metadata", {})
    result_size = metadata.get("result_size", {})
    total_matches = result_size.get("match_count", 0)
    if total_matches == 0:
        docs = initial_data.get("docs", [])
        if docs:
            total_matches = len(docs)
        else:
            logger.error("Finn.no: Ingen annonser funnet.")
            return []

    page_size = len(initial_data.get("docs", []))
    total_pages = (total_matches + page_size - 1) // page_size if page_size > 0 else 1
    logger.info("Finn.no: %d annonser, sidesize=%d, sider=%d", total_matches, page_size, total_pages)

    for ad in extract_info_from_json(initial_data):
        if ad["Finnkode"] not in seen_ids:
            seen_ids.add(ad["Finnkode"])
            all_ads.append(ad)

    for page in range(2, total_pages + 1):
        await asyncio.sleep(0.2)
        data = await fetch_json(session, f"{base_url}&page={page}")
        if data:
            for ad in extract_info_from_json(data):
                if ad["Finnkode"] not in seen_ids:
                    seen_ids.add(ad["Finnkode"])
                    all_ads.append(ad)

    logger.info("Finn.no: hentet %d unike annonser", len(all_ads))
    return all_ads


def extract_info_from_json(json_data: dict) -> list[dict]:
    try:
        ads = json_data.get("docs", [])
        if not ads:
            return []

        first = ads[0]
        missing = {"id", "heading", "canonical_url"} - set(first.keys())
        if missing:
            logger.error("Finn.no: annonser mangler forventede felter: %s", missing)
            return []

        extracted_data = []
        for ad in ads:
            finnkode = ad.get("id")
            url = ad.get("canonical_url")
            if not finnkode or not url:
                continue
            timestamp = ad.get("timestamp")
            formatted_date = datetime.fromtimestamp(timestamp / 1000).strftime(DATE_FORMAT) if timestamp else "Ukjent"
            image_url = ""
            img = ad.get("image") or {}
            if isinstance(img, dict):
                image_url = img.get("url", "")
                if image_url and not image_url.startswith("http"):
                    image_url = f"https://images.finncdn.no/dynamic/480x360c/{image_url}"
            location = ad.get("location", "")
            if isinstance(location, dict):
                location = location.get("name", "")
            regno_raw = ad.get("regno", "") or ""
            regno = regno_raw.strip().upper().replace(" ", "")
            org_id = ad.get("org_id")
            dealer_seg = ad.get("dealer_segment", "") or ""
            selger_type = "Privat" if dealer_seg.lower() == "privat" else ("Forhandler" if org_id else "")
            publisert_dato = datetime.fromtimestamp(timestamp / 1000) if timestamp else None

            # Hent campingvogn-spesifikke felt fra API
            specs = ad.get("main_search_criteria", []) or []
            specs_dict = {}
            for s in specs:
                if isinstance(s, dict):
                    specs_dict[s.get("key", "")] = s.get("value", "")

            extracted_data.append({
                "Finnkode": finnkode,
                "Annonsenavn": ad.get("heading"),
                "Pris": ad.get("price", {}).get("amount"),
                "Modell": ad.get("year"),
                "Oppdatert": formatted_date,
                "PublisertDato": publisert_dato,
                "URL": url,
                "ImageURL": image_url,
                "Lokasjon": location,
                "Kjennemerke": regno,
                "SelgerNavn": None,
                "SelgerType": selger_type or None,
                "SelgerOrgId": str(org_id) if org_id else None,
                "Detaljer": {},
            })
        return extracted_data
    except Exception as e:
        logger.error("Feil ved ekstraksjon av JSON-data: %s", e)
        return []


async def fetch_html(session: aiohttp.ClientSession, url: str, max_retries: int = MAX_RETRIES) -> str | None:
    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(url, headers=HTTP_HEADERS, timeout=HTTP_TIMEOUT) as response:
                if response.status == 429 or response.status >= 500:
                    wait = 2 ** attempt
                    if attempt < max_retries:
                        await asyncio.sleep(wait)
                        continue
                    return None
                response.raise_for_status()
                return await response.text()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            wait = 2 ** attempt
            if attempt < max_retries:
                await asyncio.sleep(wait)
            else:
                logger.error("Ga opp HTML-henting etter %d forsøk for %s: %s", max_retries, url, e)
                return None
        except Exception as e:
            logger.error("Uventet feil ved henting av HTML fra %s: %s", url, e)
            return None
    return None


def extract_detailed_ad_info(html_content: str) -> dict:
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        info_dict = {}
        spesifikasjoner = soup.find('dl', class_='emptycheck')
        if spesifikasjoner:
            items = spesifikasjoner.find_all(['dt', 'dd'])
            for i in range(0, len(items) - 1, 2):
                key = items[i].text.strip()
                value = items[i+1].text.strip()
                info_dict[key] = value
        desc_tag = soup.find('meta', property='og:description')
        info_dict["Beskrivelse"] = desc_tag['content'] if desc_tag else "Ikke tilgjengelig"
        return info_dict
    except Exception as e:
        logger.error("Feil under detaljuttrekk: %s", e)
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


def normalize_price(price) -> int | None:
    try:
        return int(re.sub(r"[^\d]", "", str(price)))
    except Exception:
        return None


def format_price(price) -> str:
    try:
        cleaned = re.sub(r"[^\d]", "", str(price))
        return f"{int(cleaned):,.0f} kr".replace(",", " ")
    except Exception:
        return "Ukjent"


# Kolonner i campingvogn_elbil-tabellen (eksisterende + nye)
_FELT_NAVN = [
    "Annonsenavn", "Modell", "Beskrivelse",
    "Egenvekt", "Lengde", "Bredde", "Soveplasser", "Nyttelast", "Totalvekt",
    "Oppdatert", "PublisertDato", "URL", "Pris", "ImageURL", "Lokasjon",
    "Kjennemerke", "SelgerNavn", "SelgerType", "SelgerOrgId",
    "SvvMerke", "SvvAarsmodell", "SvvForstegangNorge", "SvvRegistreringsstatus",
    "SvvEgenvekt", "SvvNyttelast", "SvvTillattTotalvekt", "SvvLengde", "SvvBredde",
    "SvvAntallAksler",
]

_SVV_COLS = [
    "SvvMerke", "SvvAarsmodell", "SvvForstegangNorge", "SvvRegistreringsstatus",
    "SvvEgenvekt", "SvvNyttelast", "SvvTillattTotalvekt", "SvvLengde", "SvvBredde",
    "SvvAntallAksler",
]


def _parse_int(val) -> int | None:
    try:
        return int(re.sub(r"[^\d]", "", str(val)))
    except Exception:
        return None


def _build_nye_verdier(ad: dict) -> list:
    svv = ad.get("VegvesenData") or {}
    det = ad.get("Detaljer") or {}
    return [
        ad["Annonsenavn"],
        ad.get("Modell"),
        det.get("Beskrivelse", ""),
        _parse_int(det.get("Egenvekt") or svv.get("svv_egenvekt")),
        _parse_int(det.get("Lengde") or svv.get("svv_lengde")),
        _parse_int(det.get("Bredde") or svv.get("svv_bredde")),
        _parse_int(det.get("Antall soveplasser") or det.get("Soveplasser")),
        _parse_int(det.get("Nyttelast") or svv.get("svv_nyttelast")),
        _parse_int(det.get("Totalvekt") or svv.get("svv_tillatt_totalvekt")),
        ad["Oppdatert"],
        ad.get("PublisertDato"),
        ad["URL"],
        normalize_price(ad["Pris"]),
        ad.get("ImageURL", ""),
        ad.get("Lokasjon", ""),
        ad.get("Kjennemerke", "") or "",
        ad.get("SelgerNavn"),
        ad.get("SelgerType"),
        ad.get("SelgerOrgId"),
        svv.get("svv_merke"),
        svv.get("svv_aarsmodell"),
        svv.get("svv_forstegang_norge"),
        svv.get("svv_registreringsstatus"),
        svv.get("svv_egenvekt"),
        svv.get("svv_nyttelast"),
        svv.get("svv_tillatt_totalvekt"),
        svv.get("svv_lengde"),
        svv.get("svv_bredde"),
        svv.get("svv_antall_aksler"),
    ]


def ensure_schema() -> None:
    """Sørg for at campingvogn_elbil-tabellen har alle nødvendige kolonner."""
    conn = connect_to_database()
    if not conn:
        return
    try:
        cursor = conn.cursor()

        # Opprett tabell hvis den ikke finnes (med alle kolonner)
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{TABLE}` (
                Finnkode BIGINT PRIMARY KEY,
                Annonsenavn VARCHAR(500),
                Modell VARCHAR(50),
                Beskrivelse TEXT,
                Egenvekt INT,
                Lengde INT,
                Bredde INT,
                Soveplasser INT,
                Nyttelast INT,
                Totalvekt INT,
                Oppdatert VARCHAR(50),
                PublisertDato DATETIME NULL,
                URL TEXT,
                Pris INT,
                ImageURL TEXT,
                Lokasjon VARCHAR(200),
                Kjennemerke VARCHAR(20),
                SelgerNavn VARCHAR(200),
                SelgerType VARCHAR(50),
                SelgerOrgId VARCHAR(50),
                SvvMerke VARCHAR(100),
                SvvAarsmodell INT,
                SvvForstegangNorge VARCHAR(20),
                SvvRegistreringsstatus VARCHAR(100),
                SvvEgenvekt INT,
                SvvNyttelast INT,
                SvvTillattTotalvekt INT,
                SvvLengde INT,
                SvvBredde INT,
                SvvAntallAksler INT,
                Solgt TINYINT(1) DEFAULT 0,
                SolgtDato DATETIME NULL,
                SistSett DATETIME NULL,
                Opprettet DATETIME DEFAULT CURRENT_TIMESTAMP
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)
        conn.commit()

        # Legg til manglende kolonner på eksisterende tabell (migrasjon)
        nye_kolonner = [
            ("ImageURL", "TEXT NULL"),
            ("Lokasjon", "VARCHAR(200) NULL"),
            ("Kjennemerke", "VARCHAR(20) NULL"),
            ("SelgerNavn", "VARCHAR(200) NULL"),
            ("SelgerType", "VARCHAR(50) NULL"),
            ("SelgerOrgId", "VARCHAR(50) NULL"),
            ("PublisertDato", "DATETIME NULL"),
            ("SvvMerke", "VARCHAR(100) NULL"),
            ("SvvAarsmodell", "INT NULL"),
            ("SvvForstegangNorge", "VARCHAR(20) NULL"),
            ("SvvRegistreringsstatus", "VARCHAR(100) NULL"),
            ("SvvEgenvekt", "INT NULL"),
            ("SvvNyttelast", "INT NULL"),
            ("SvvTillattTotalvekt", "INT NULL"),
            ("SvvLengde", "INT NULL"),
            ("SvvBredde", "INT NULL"),
            ("SvvAntallAksler", "INT NULL"),
            ("Solgt", "TINYINT(1) DEFAULT 0"),
            ("SolgtDato", "DATETIME NULL"),
            ("SistSett", "DATETIME NULL"),
            ("Opprettet", "DATETIME DEFAULT CURRENT_TIMESTAMP"),
        ]
        for col, typedef in nye_kolonner:
            try:
                cursor.execute(f"ALTER TABLE `{TABLE}` ADD COLUMN {col} {typedef}")
                logger.info("La til kolonne %s i %s.", col, TABLE)
            except Exception as e:
                if "Duplicate column" not in str(e) and "1060" not in str(e):
                    logger.error("Feil ved ALTER TABLE %s: %s", col, e)

        # Opprett prisendringer-tabell
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{PRISENDRINGER_TABLE}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                Finnkode BIGINT NOT NULL,
                Pris VARCHAR(100) NOT NULL,
                Tidspunkt DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_finnkode (Finnkode),
                INDEX idx_tidspunkt (Tidspunkt)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)

        # UNIQUE-nøkkel for deduplisering
        try:
            cursor.execute(f"ALTER TABLE `{PRISENDRINGER_TABLE}` ADD UNIQUE KEY uq_finnkode_pris (Finnkode, Pris(50))")
            logger.info("La til UNIQUE KEY på %s.", PRISENDRINGER_TABLE)
        except Exception as e:
            if "Duplicate key name" not in str(e) and "1061" not in str(e):
                logger.error("Feil ved UNIQUE KEY: %s", e)

        # bruker_data-tabell
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS campingvogn_bruker_data (
                Finnkode BIGINT PRIMARY KEY,
                Favoritt TINYINT(1) DEFAULT 0,
                Notat TEXT,
                PrisVarsel INT NULL,
                ScoreJustering INT DEFAULT 0,
                Oppdatert DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)
        conn.commit()
        logger.info("Skjema OK.")
    except Exception as e:
        logger.error("Feil ved ensure_schema: %s", e)
    finally:
        conn.close()


def update_database(ads: list[dict], dry_run: bool = False) -> None:
    mode = "DRY RUN" if dry_run else "LIVE"
    logger.info("[%s] Starter databaseoppdatering for %d annonser.", mode, len(ads))

    conn = connect_to_database()
    if not conn:
        return

    try:
        cursor = conn.cursor()
        nye = 0
        endret = 0
        uendret = 0
        prisfall_titler = []

        svv_upsert = ",\n                        ".join(
            f"{c} = IF(VALUES({c}) IS NOT NULL, VALUES({c}), {c})" for c in _SVV_COLS
        )

        for ad in ads:
            finnkode = ad["Finnkode"]
            ny_pris = normalize_price(ad["Pris"])
            if ny_pris is None:
                continue

            nye_verdier = _build_nye_verdier(ad)

            cursor.execute(f"SELECT Pris FROM `{TABLE}` WHERE Finnkode = %s", (finnkode,))
            row = cursor.fetchone()

            if row:
                gammel_pris = row[0]
                if gammel_pris != ny_pris:
                    endret += 1
                    if gammel_pris and ny_pris < gammel_pris:
                        prisfall_titler.append(
                            f"{ad['Annonsenavn']}: {format_price(gammel_pris)} → {format_price(ny_pris)}"
                        )
                    if not dry_run:
                        cursor.execute(
                            f"INSERT INTO `{PRISENDRINGER_TABLE}` (Finnkode, Pris) VALUES (%s, %s)",
                            (finnkode, ny_pris)
                        )
                else:
                    uendret += 1
            else:
                nye += 1
                logger.info("[%s] Ny: %s — %s", mode, finnkode, ad['Annonsenavn'])
                if not dry_run:
                    cursor.execute(
                        f"INSERT IGNORE INTO `{PRISENDRINGER_TABLE}` (Finnkode, Pris) VALUES (%s, %s)",
                        (finnkode, ny_pris)
                    )

            if not dry_run:
                cursor.execute(f"""
                    INSERT INTO `{TABLE}` (
                        Finnkode, Annonsenavn, Modell, Beskrivelse,
                        Egenvekt, Lengde, Bredde, Soveplasser, Nyttelast, Totalvekt,
                        Oppdatert, PublisertDato, URL, Pris, ImageURL, Lokasjon,
                        Kjennemerke, SelgerNavn, SelgerType, SelgerOrgId,
                        {", ".join(_SVV_COLS)}
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        {", ".join(["%s"] * len(_SVV_COLS))}
                    )
                    ON DUPLICATE KEY UPDATE
                        Annonsenavn = VALUES(Annonsenavn),
                        Modell = VALUES(Modell),
                        Beskrivelse = IF(VALUES(Beskrivelse) != '' AND VALUES(Beskrivelse) IS NOT NULL, VALUES(Beskrivelse), Beskrivelse),
                        Egenvekt = IF(VALUES(Egenvekt) IS NOT NULL, VALUES(Egenvekt), Egenvekt),
                        Lengde = IF(VALUES(Lengde) IS NOT NULL, VALUES(Lengde), Lengde),
                        Bredde = IF(VALUES(Bredde) IS NOT NULL, VALUES(Bredde), Bredde),
                        Soveplasser = IF(VALUES(Soveplasser) IS NOT NULL, VALUES(Soveplasser), Soveplasser),
                        Nyttelast = IF(VALUES(Nyttelast) IS NOT NULL, VALUES(Nyttelast), Nyttelast),
                        Totalvekt = IF(VALUES(Totalvekt) IS NOT NULL, VALUES(Totalvekt), Totalvekt),
                        Oppdatert = VALUES(Oppdatert),
                        PublisertDato = IF(PublisertDato IS NULL AND VALUES(PublisertDato) IS NOT NULL, VALUES(PublisertDato), PublisertDato),
                        URL = VALUES(URL),
                        Pris = VALUES(Pris),
                        ImageURL = IF(VALUES(ImageURL) != '' AND VALUES(ImageURL) IS NOT NULL, VALUES(ImageURL), ImageURL),
                        Lokasjon = IF(VALUES(Lokasjon) != '' AND VALUES(Lokasjon) IS NOT NULL, VALUES(Lokasjon), Lokasjon),
                        Kjennemerke = IF(VALUES(Kjennemerke) != '' AND VALUES(Kjennemerke) IS NOT NULL, VALUES(Kjennemerke), Kjennemerke),
                        SelgerNavn = IF(VALUES(SelgerNavn) IS NOT NULL, VALUES(SelgerNavn), SelgerNavn),
                        SelgerType = IF(VALUES(SelgerType) IS NOT NULL, VALUES(SelgerType), SelgerType),
                        SelgerOrgId = IF(VALUES(SelgerOrgId) IS NOT NULL, VALUES(SelgerOrgId), SelgerOrgId),
                        {svv_upsert}
                """, (
                    finnkode,
                    *nye_verdier[:19],
                    *[nye_verdier[_FELT_NAVN.index(c)] for c in _SVV_COLS],
                ))

        if not dry_run:
            conn.commit()

        logger.info("[%s] %d nye, %d endret, %d uendret.", mode, nye, endret, uendret)

        if not dry_run and prisfall_titler:
            send_ha_notification(
                "Campingvogn-prisfall",
                "**Prisfall:**\n" + "\n".join(f"- {t}" for t in prisfall_titler[:10])
            )
    except Exception as e:
        logger.error("Feil i update_database: %s", e)
    finally:
        conn.close()


async def mark_removed_ads(current_ads: list[dict], session: aiohttp.ClientSession | None = None, dry_run: bool = False) -> None:
    mode = "DRY RUN" if dry_run else "LIVE"
    conn = connect_to_database()
    if not conn:
        return

    try:
        cursor = conn.cursor()
        active_ids = {ad["Finnkode"] for ad in current_ads}
        now = datetime.now()

        if not dry_run and active_ids:
            cursor.executemany(
                f"UPDATE `{TABLE}` SET SistSett = %s WHERE Finnkode = %s",
                [(now, fk) for fk in active_ids]
            )

        cursor.execute(
            f"SELECT Finnkode FROM `{TABLE}` WHERE (Solgt = 0 OR Solgt IS NULL) "
            "AND (SistSett IS NULL OR SistSett < %s)",
            (now - timedelta(hours=48),)
        )
        stale_rows = [row[0] for row in cursor.fetchall() if row[0] not in active_ids]

        if not stale_rows:
            if not dry_run:
                conn.commit()
            return

        logger.info("[%s] %d kandidater ikke sett på over 48t — dobbeltsjekker...", mode, len(stale_rows))

        bekreftede = []
        semaphore = asyncio.Semaphore(3)

        async def sjekk(finnkode):
            async with semaphore:
                await asyncio.sleep(0.5)
                if session:
                    url = f"https://www.finn.no/mobility/item/{finnkode}"
                    try:
                        async with session.get(url, headers=HTTP_HEADERS, timeout=HTTP_TIMEOUT) as resp:
                            if resp.status == 404:
                                bekreftede.append(finnkode)
                                return
                            html = await resp.text()
                            if "ikke lenger tilgjengelig" in html.lower():
                                bekreftede.append(finnkode)
                    except Exception as e:
                        logger.warning("Feil ved verifisering av %s: %s", finnkode, e)
                else:
                    bekreftede.append(finnkode)

        await asyncio.gather(*(sjekk(fk) for fk in stale_rows))

        for finnkode in bekreftede:
            logger.info("[%s] Markerer %s som Solgt/Fjernet.", mode, finnkode)
            if not dry_run:
                cursor.execute(
                    f"UPDATE `{TABLE}` SET Solgt = 1, SolgtDato = %s WHERE Finnkode = %s",
                    (now, finnkode)
                )
                try:
                    cursor.execute(
                        f"INSERT INTO `{PRISENDRINGER_TABLE}` (Finnkode, Pris) VALUES (%s, %s)",
                        (finnkode, "Solgt/Fjernet")
                    )
                except Exception:
                    pass

        if not dry_run:
            conn.commit()
        logger.info("[%s] Markerte %d som Solgt/Fjernet.", mode, len(bekreftede))
    except Exception as e:
        logger.error("Feil i mark_removed_ads: %s", e)
    finally:
        conn.close()


def send_ha_notification(title: str, message: str) -> None:
    import urllib.request
    token = os.getenv("SUPERVISOR_TOKEN")
    if not token:
        return
    try:
        data = json.dumps({"title": title, "message": message}).encode()
        req = urllib.request.Request(
            "http://supervisor/core/api/services/persistent_notification/create",
            data=data,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        logger.warning("Kunne ikke sende HA-varsling: %s", e)


# -- SVV-integrasjon (samme logikk som bobil, tilpasset campingvogn) -------

_REGNR_RE = re.compile(r"^[A-Z]{2}[0-9]{4,5}$")

SVV_API_URL = "https://akfell-datautlevering.atlas.vegvesen.no/enkeltoppslag/kjoretoydata"


def parse_vegvesen_data(data: dict) -> dict:
    result = {}
    try:
        liste = data.get("kjoretoydataListe", [])
        k = liste[0] if liste else data
        td = k.get("godkjenning", {}).get("tekniskGodkjenning", {}).get("tekniskeData", {})

        merke_list = td.get("generelt", {}).get("merke", [{}])
        result["svv_merke"] = merke_list[0].get("merke") if merke_list else None

        forsteg_dato = k.get("forstegangsregistrering", {}).get("registrertForstegangNorgeDato", "")
        if forsteg_dato and len(forsteg_dato) >= 4:
            result["svv_aarsmodell"] = int(forsteg_dato[:4])
            result["svv_forstegang_norge"] = forsteg_dato
        else:
            result["svv_aarsmodell"] = None
            result["svv_forstegang_norge"] = None

        reg = k.get("registrering", {})
        result["svv_registreringsstatus"] = reg.get("registreringsstatus", {}).get("kodeBeskrivelse")

        dim = td.get("dimensjoner", {})
        result["svv_lengde"] = dim.get("lengde")
        result["svv_bredde"] = dim.get("bredde")

        vekter = td.get("vekter", {})
        result["svv_egenvekt"] = vekter.get("egenvekt")
        result["svv_nyttelast"] = vekter.get("nyttelast")
        result["svv_tillatt_totalvekt"] = vekter.get("tillattTotalvekt")

        # Antall aksler
        aksler = td.get("aksler", {})
        result["svv_antall_aksler"] = aksler.get("antallAksler")
    except Exception as e:
        logger.warning("Feil ved parsing av SVV-data: %s", e)
    return result


async def fetch_svv_data(session: aiohttp.ClientSession, kjennemerke: str, api_key: str) -> dict | None:
    url = f"{SVV_API_URL}?kjennemerke={kjennemerke}"
    headers = {**HTTP_HEADERS, "SVV-Authorization": api_key}
    try:
        async with session.get(url, headers=headers, timeout=HTTP_TIMEOUT) as resp:
            if resp.status != 200:
                logger.warning("SVV HTTP %s for %s", resp.status, kjennemerke)
                return None
            data = await resp.json()
            return parse_vegvesen_data(data)
    except Exception as e:
        logger.warning("Feil ved SVV-oppslag for %s: %s", kjennemerke, e)
        return None


async def enrich_ads_with_vegvesen(session: aiohttp.ClientSession, ads: list[dict]) -> list[dict]:
    api_key = options.get("vegvesen_api_key", "")
    if not api_key:
        logger.info("Vegvesen API-nøkkel ikke satt, hopper over SVV-oppslag.")
        for ad in ads:
            ad["VegvesenData"] = {}
        return ads

    semaphore = asyncio.Semaphore(3)

    async def enrich_one(ad):
        kjennemerke = (ad.get("Kjennemerke") or "").strip().upper().replace(" ", "")
        if not kjennemerke or not _REGNR_RE.match(kjennemerke):
            ad["VegvesenData"] = {}
            return ad
        async with semaphore:
            await asyncio.sleep(0.3)
            svv = await fetch_svv_data(session, kjennemerke, api_key)
            ad["VegvesenData"] = svv or {}
        return ad

    return list(await asyncio.gather(*(enrich_one(ad) for ad in ads)))


async def fetch_finn_ads(session: aiohttp.ClientSession) -> list[dict]:
    ads_data = await fetch_all_pages(session, LISTINGS_PAGE_URL)
    if not ads_data:
        logger.error("Ingen annonser hentet fra Finn.no API.")
        return []
    return list(await fetch_and_combine_data(session, ads_data))


async def main() -> None:
    logger.info("Starter campingvogn-scraper...")
    if DRY_RUN:
        logger.info("*** DRY RUN MODUS ***")

    ensure_schema()

    async with aiohttp.ClientSession() as session:
        ads = await fetch_finn_ads(session)
        if ads:
            ads = await enrich_ads_with_vegvesen(session, ads)
            update_database(ads, dry_run=DRY_RUN)
            await mark_removed_ads(ads, session=session, dry_run=DRY_RUN)

    logger.info("Ferdig.")


def run_scraper():
    asyncio.run(main())


if __name__ == "__main__":
    run_scraper()
