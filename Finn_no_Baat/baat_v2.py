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
_h = logging.StreamHandler()
_h.setLevel(logging.DEBUG)
_h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(_h)

if RUN_LOCALLY:
    logger.info("Kjorer lokalt med testkonfig.")
    options = {
        "databasehost": os.getenv("DB_HOST", "localhost"),
        "databaseusername": os.getenv("DB_USER", ""),
        "databasepassword": os.getenv("DB_PASSWORD", ""),
        "databasename": os.getenv("DB_NAME", "finn_no"),
        "databaseport": os.getenv("DB_PORT", "3306"),
    }
else:
    try:
        options = json.loads(os.getenv("SUPERVISOR_OPTIONS", "{}"))
    except json.JSONDecodeError as e:
        logger.error("JSON-dekodingsfeil ved lasting av SUPERVISOR_OPTIONS: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.error("Ukjent feil ved lasting av SUPERVISOR_OPTIONS: %s", e)
        sys.exit(1)

# Verifisert mot Finn juli 2026: bater ligger under /mobility/, ikke /boat/.
FINN_API_BASE = "https://www.finn.no/mobility/search/api/search/SEARCH_ID_BOAT_USED"
TABLE = "baat"
PRISENDRINGER_TABLE = "baat_prisendringer"

BOAT_CLASS_NAMES = {
    "7961": "Bowrider", "6923": "Cabincruiser", "2184": "Daycruiser",
    "3524": "Flybridge", "2186": "Gummibat/Jolle", "7962": "Pilothouse",
    "7343": "RIB", "2188": "Seilbat/Motorseiler",
    "3827": "Skjargardsjeep/Landstedsbat", "7960": "Speedbat",
    "6922": "Trebat/Snekke",
}
MOTOR_TYPE_NAMES = {"1": "Innenbords", "2": "Utenbords", "3": "Annet"}
FUEL_NAMES = {"1": "Bensin", "2": "Diesel", "3": "Gass", "4": "El", "5": "Gass+bensin"}


def build_search_url(opts: dict) -> str:
    params = []

    def multi(key, vals):
        if not vals:
            return
        if not isinstance(vals, list):
            vals = [vals]
        for v in vals:
            if str(v).strip():
                params.append((key, str(v).strip()))

    multi("location", opts.get("locations", []))
    multi("class", opts.get("boat_classes", []))
    multi("motor_type", opts.get("motor_types", []))
    multi("fuel", opts.get("fuels", []))

    params.extend([
        ("price_from", opts.get("price_from", 150000)),
        ("price_to", opts.get("price_to", 600000)),
        ("year_from", opts.get("year_from", 2005)),
    ])

    if opts.get("length_feet_from"):
        params.append(("length_feet_from", opts["length_feet_from"]))
    if opts.get("length_feet_to"):
        params.append(("length_feet_to", opts["length_feet_to"]))

    params.append(("sort", opts.get("sort", "PUBLISHED_DESC")))
    return f"{FINN_API_BASE}?{urlencode(params)}"


LISTINGS_PAGE_URL = build_search_url(options)
DRY_RUN = options.get("dry_run", False)
DATE_FORMAT = "%d. %m. %Y %H:%M"

DB_CONFIG = {
    "host": options.get("databasehost", ""),
    "user": options.get("databaseusername", ""),
    "passwd": options.get("databasepassword", ""),
    "database": options.get("databasename", "finn_no"),
    "port": options.get("databaseport", 3306),
}


def connect_to_database():
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


async def fetch_json(session, url, max_retries=MAX_RETRIES):
    logger.info("Henter JSON fra %s", url)
    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(url, headers=HTTP_HEADERS, timeout=HTTP_TIMEOUT) as resp:
                if resp.status == 429 or resp.status >= 500:
                    wait = 2 ** attempt
                    logger.warning("HTTP %s for %s, venter %ds (forsok %d/%d)",
                                   resp.status, url, wait, attempt, max_retries)
                    await asyncio.sleep(wait)
                    continue
                if resp.status != 200:
                    logger.error("HTTP %s for %s", resp.status, url)
                    return None
                data = await resp.json()
                if not isinstance(data, dict):
                    logger.error("Uventet responstype: %s fra %s", type(data).__name__, url)
                    return None
                if "docs" not in data:
                    logger.error("'docs' mangler i respons fra %s. Nokler: %s", url, list(data.keys()))
                    return None
                return data
        except (aiohttp.ContentTypeError, aiohttp.ClientError, asyncio.TimeoutError) as e:
            wait = 2 ** attempt
            logger.warning("Nettverksfeil (forsok %d/%d) for %s: %s", attempt, max_retries, url, e)
            if attempt < max_retries:
                await asyncio.sleep(wait)
            else:
                logger.error("Ga opp etter %d forsok for %s: %s", max_retries, url, e)
                return None
        except Exception as e:
            logger.error("Uventet feil ved henting av JSON fra %s: %s", url, e)
            return None
    return None


async def fetch_all_pages(session, base_url):
    all_ads = []
    seen = set()

    first = await fetch_json(session, f"{base_url}&page=1")
    if not first:
        logger.error("Finn.no: Kunne ikke hente forste side.")
        return []

    rs = first.get("metadata", {}).get("result_size", {})
    total = rs.get("match_count", 0)
    if total == 0:
        docs = first.get("docs", [])
        if docs:
            total = len(docs)
        else:
            logger.error("Finn.no: Ingen annonser funnet.")
            return []

    page_size = len(first.get("docs", []))
    total_pages = (total + page_size - 1) // page_size if page_size > 0 else 1
    logger.info("Finn.no: %d annonser, sidesize=%d, sider=%d", total, page_size, total_pages)

    for ad in extract_info_from_json(first):
        if ad["Finnkode"] not in seen:
            seen.add(ad["Finnkode"])
            all_ads.append(ad)

    for page in range(2, total_pages + 1):
        await asyncio.sleep(0.2)
        data = await fetch_json(session, f"{base_url}&page={page}")
        if data:
            for ad in extract_info_from_json(data):
                if ad["Finnkode"] not in seen:
                    seen.add(ad["Finnkode"])
                    all_ads.append(ad)

    logger.info("Finn.no: hentet %d unike annonser", len(all_ads))
    return all_ads


def _num(val):
    if val is None:
        return None
    try:
        return int(round(float(val)))
    except (TypeError, ValueError):
        return None


def extract_info_from_json(json_data: dict) -> list:
    try:
        ads = json_data.get("docs", [])
        if not ads:
            return []

        missing = {"id", "heading", "canonical_url"} - set(ads[0].keys())
        if missing:
            logger.error("Finn.no: annonser mangler forventede felter: %s", missing)
            return []

        out = []
        for ad in ads:
            finnkode = ad.get("id")
            url = ad.get("canonical_url")
            if not finnkode or not url:
                continue

            ts = ad.get("timestamp")
            formatted = datetime.fromtimestamp(ts / 1000).strftime(DATE_FORMAT) if ts else "Ukjent"
            publisert = datetime.fromtimestamp(ts / 1000) if ts else None

            image_url = ""
            img = ad.get("image") or {}
            if isinstance(img, dict):
                image_url = img.get("url", "")
                if image_url and not image_url.startswith("http"):
                    image_url = f"https://images.finncdn.no/dynamic/480x360c/{image_url}"

            loc = ad.get("location", "")
            if isinstance(loc, dict):
                loc = loc.get("name", "")

            seg = (ad.get("dealer_segment") or "").strip()
            selger = "Privat" if seg.lower() == "privat" else ("Forhandler" if seg else None)

            coords = ad.get("coordinates") or {}

            out.append({
                "Finnkode": finnkode,
                "Annonsenavn": ad.get("heading"),
                "Pris": (ad.get("price") or {}).get("amount"),
                "Modell": ad.get("year"),
                "Merke": ad.get("make"),
                "Baattype": ad.get("boat_class"),
                "LengdeFot": _num(ad.get("length")),
                "BreddeCm": _num(ad.get("width")),
                "MotorType": ad.get("motor_type"),
                "Drivstoff": ad.get("motor_fuel"),
                "MotorHk": _num(ad.get("motor_size")),
                "Toppfart": _num(ad.get("max_speed")),
                "Lat": coords.get("lat"),
                "Lon": coords.get("lon"),
                "Oppdatert": formatted,
                "PublisertDato": publisert,
                "URL": url,
                "ImageURL": image_url,
                "Lokasjon": loc,
                "SelgerType": selger,
                "Detaljer": {},
            })
        return out
    except Exception as e:
        logger.error("Feil ved ekstraksjon av JSON-data: %s", e)
        return []


async def fetch_html(session, url, max_retries=MAX_RETRIES):
    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(url, headers=HTTP_HEADERS, timeout=HTTP_TIMEOUT) as resp:
                if resp.status == 429 or resp.status >= 500:
                    if attempt < max_retries:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    return None
                resp.raise_for_status()
                return await resp.text()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt < max_retries:
                await asyncio.sleep(2 ** attempt)
            else:
                logger.error("Ga opp HTML-henting etter %d forsok for %s: %s", max_retries, url, e)
                return None
        except Exception as e:
            logger.error("Uventet feil ved henting av HTML fra %s: %s", url, e)
            return None
    return None


def extract_detailed_ad_info(html_content: str) -> dict:
    """Spesifikasjoner fra annonsesiden. Verifiserte nokler paa batannonser:
    Merke, Modellaar, Type, Drivstoff, Motor inkludert, Motorstorrelse,
    Motorfabrikant, Type motor, Topphastighet, Byggemateriale, Lengde i fot,
    Bredde, Sitteplasser, Soveplasser, Farge, Registreringsnummer."""
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        info = {}
        for dl in soup.find_all("dl"):
            items = dl.find_all(["dt", "dd"])
            for i in range(0, len(items) - 1, 2):
                if items[i].name == "dt" and items[i + 1].name == "dd":
                    k = items[i].get_text(strip=True)
                    v = items[i + 1].get_text(strip=True)
                    if k and k not in info:
                        info[k] = v
        d = soup.find("meta", property="og:description")
        info["Beskrivelse"] = d["content"] if d else "Ikke tilgjengelig"
        return info
    except Exception as e:
        logger.error("Feil under detaljuttrekk: %s", e)
        return {}


async def fetch_and_combine_data(session, ads, max_concurrent=5):
    sem = asyncio.Semaphore(max_concurrent)

    async def one(ad):
        async with sem:
            await asyncio.sleep(0.2)
            html = await fetch_html(session, ad["URL"])
            if html:
                ad["Detaljer"] = extract_detailed_ad_info(html)
        return ad

    return await asyncio.gather(*(one(a) for a in ads))


def normalize_price(price):
    try:
        return int(re.sub(r"[^\d]", "", str(price)))
    except Exception:
        return None


def format_price(price):
    try:
        return f"{int(re.sub(r'[^0-9]', '', str(price))):,.0f} kr".replace(",", " ")
    except Exception:
        return "Ukjent"


def _parse_int(val):
    try:
        return int(re.sub(r"[^\d]", "", str(val)))
    except Exception:
        return None


_COLS = [
    "Annonsenavn", "Modell", "Merke", "Baattype", "Beskrivelse",
    "LengdeFot", "BreddeCm", "MotorType", "Drivstoff", "MotorHk",
    "Motorfabrikant", "MotorInkludert", "Skrogmateriale", "Soveplasser",
    "Sitteplasser", "Regnr", "Farge", "Toppfart", "Lat", "Lon",
    "Oppdatert", "PublisertDato", "URL", "Pris", "ImageURL", "Lokasjon", "SelgerType",
]

_TEXTISH = {
    "Beskrivelse", "ImageURL", "Lokasjon", "Merke", "Baattype", "MotorType",
    "Drivstoff", "Skrogmateriale", "SelgerType", "Motorfabrikant",
    "MotorInkludert", "Regnr", "Farge",
}
_ALWAYS = {"Annonsenavn", "Modell", "Oppdatert", "URL", "Pris"}


def _build_nye_verdier(ad: dict) -> list:
    det = ad.get("Detaljer") or {}
    return [
        ad["Annonsenavn"],
        ad.get("Modell"),
        ad.get("Merke"),
        ad.get("Baattype"),
        det.get("Beskrivelse", ""),
        ad.get("LengdeFot"),
        ad.get("BreddeCm"),
        ad.get("MotorType"),
        ad.get("Drivstoff"),
        ad.get("MotorHk"),
        det.get("Motorfabrikant"),
        det.get("Motor inkludert"),
        det.get("Byggemateriale") or det.get("Skrogmateriale"),
        _parse_int(det.get("Soveplasser")),
        _parse_int(det.get("Sitteplasser")),
        (det.get("Registreringsnummer") or "").strip() or None,
        det.get("Farge"),
        ad.get("Toppfart"),
        ad.get("Lat"),
        ad.get("Lon"),
        ad["Oppdatert"],
        ad.get("PublisertDato"),
        ad["URL"],
        normalize_price(ad["Pris"]),
        ad.get("ImageURL", ""),
        ad.get("Lokasjon", ""),
        ad.get("SelgerType"),
    ]


def ensure_schema() -> None:
    conn = connect_to_database()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS `{TABLE}` (
                Finnkode BIGINT PRIMARY KEY,
                Annonsenavn VARCHAR(500),
                Modell VARCHAR(50),
                Merke VARCHAR(100),
                Baattype VARCHAR(100),
                Beskrivelse TEXT,
                LengdeFot INT,
                BreddeCm INT,
                MotorType VARCHAR(50),
                Drivstoff VARCHAR(50),
                MotorHk INT,
                Motorfabrikant VARCHAR(150),
                MotorInkludert VARCHAR(50),
                Skrogmateriale VARCHAR(100),
                Soveplasser INT,
                Sitteplasser INT,
                Regnr VARCHAR(30),
                Farge VARCHAR(50),
                Toppfart INT,
                Lat DOUBLE,
                Lon DOUBLE,
                Oppdatert VARCHAR(50),
                PublisertDato DATETIME NULL,
                URL TEXT,
                Pris INT,
                ImageURL TEXT,
                Lokasjon VARCHAR(200),
                SelgerType VARCHAR(50),
                Solgt TINYINT(1) DEFAULT 0,
                SolgtDato DATETIME NULL,
                SistSett DATETIME NULL,
                Opprettet DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_merke (Merke),
                INDEX idx_baattype (Baattype),
                INDEX idx_lengde (LengdeFot)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)
        conn.commit()

        for col, td in [
            ("Merke", "VARCHAR(100) NULL"), ("Baattype", "VARCHAR(100) NULL"),
            ("LengdeFot", "INT NULL"), ("BreddeCm", "INT NULL"),
            ("MotorType", "VARCHAR(50) NULL"), ("Drivstoff", "VARCHAR(50) NULL"),
            ("MotorHk", "INT NULL"), ("Motorfabrikant", "VARCHAR(150) NULL"),
            ("MotorInkludert", "VARCHAR(50) NULL"), ("Skrogmateriale", "VARCHAR(100) NULL"),
            ("Soveplasser", "INT NULL"), ("Sitteplasser", "INT NULL"),
            ("Regnr", "VARCHAR(30) NULL"), ("Farge", "VARCHAR(50) NULL"),
            ("Toppfart", "INT NULL"), ("Lat", "DOUBLE NULL"), ("Lon", "DOUBLE NULL"),
            ("Solgt", "TINYINT(1) DEFAULT 0"), ("SolgtDato", "DATETIME NULL"),
            ("SistSett", "DATETIME NULL"), ("Opprettet", "DATETIME DEFAULT CURRENT_TIMESTAMP"),
        ]:
            try:
                cur.execute(f"ALTER TABLE `{TABLE}` ADD COLUMN {col} {td}")
                logger.info("La til kolonne %s i %s.", col, TABLE)
            except Exception as e:
                if "Duplicate column" not in str(e) and "1060" not in str(e):
                    logger.error("Feil ved ALTER TABLE %s: %s", col, e)

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS `{PRISENDRINGER_TABLE}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                Finnkode BIGINT NOT NULL,
                Pris VARCHAR(100) NOT NULL,
                Tidspunkt DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_finnkode (Finnkode),
                INDEX idx_tidspunkt (Tidspunkt)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)
        try:
            cur.execute(f"ALTER TABLE `{PRISENDRINGER_TABLE}` ADD UNIQUE KEY uq_finnkode_pris (Finnkode, Pris(50))")
        except Exception as e:
            if "Duplicate key name" not in str(e) and "1061" not in str(e):
                logger.error("Feil ved UNIQUE KEY: %s", e)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS baat_bruker_data (
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


def update_database(ads: list, dry_run: bool = False) -> None:
    mode = "DRY RUN" if dry_run else "LIVE"
    logger.info("[%s] Starter databaseoppdatering for %d annonser.", mode, len(ads))

    conn = connect_to_database()
    if not conn:
        return

    try:
        cur = conn.cursor()
        nye = endret = uendret = 0
        prisfall = []

        collist = ", ".join(_COLS)
        placeholders = ", ".join(["%s"] * (len(_COLS) + 1))

        parts = []
        for c in _COLS:
            if c in _ALWAYS:
                parts.append(f"{c} = VALUES({c})")
            elif c == "PublisertDato":
                parts.append("PublisertDato = IF(PublisertDato IS NULL AND VALUES(PublisertDato) IS NOT NULL, VALUES(PublisertDato), PublisertDato)")
            elif c in _TEXTISH:
                parts.append(f"{c} = IF(VALUES({c}) != '' AND VALUES({c}) IS NOT NULL, VALUES({c}), {c})")
            else:
                parts.append(f"{c} = IF(VALUES({c}) IS NOT NULL, VALUES({c}), {c})")
        upserts = ",\n                        ".join(parts)

        for ad in ads:
            fk = ad["Finnkode"]
            ny_pris = normalize_price(ad["Pris"])
            if ny_pris is None:
                continue

            verdier = _build_nye_verdier(ad)

            cur.execute(f"SELECT Pris FROM `{TABLE}` WHERE Finnkode = %s", (fk,))
            row = cur.fetchone()

            if row:
                gammel = row[0]
                if gammel != ny_pris:
                    endret += 1
                    if gammel and ny_pris < gammel:
                        prisfall.append(f"{ad['Annonsenavn']}: {format_price(gammel)} -> {format_price(ny_pris)}")
                    if not dry_run:
                        cur.execute(f"INSERT INTO `{PRISENDRINGER_TABLE}` (Finnkode, Pris) VALUES (%s, %s)", (fk, ny_pris))
                else:
                    uendret += 1
            else:
                nye += 1
                logger.info("[%s] Ny: %s - %s", mode, fk, ad["Annonsenavn"])
                if not dry_run:
                    cur.execute(f"INSERT IGNORE INTO `{PRISENDRINGER_TABLE}` (Finnkode, Pris) VALUES (%s, %s)", (fk, ny_pris))

            if not dry_run:
                cur.execute(f"""
                    INSERT INTO `{TABLE}` (Finnkode, {collist})
                    VALUES ({placeholders})
                    ON DUPLICATE KEY UPDATE
                        {upserts}
                """, (fk, *verdier))

        if not dry_run:
            conn.commit()

        logger.info("[%s] %d nye, %d endret, %d uendret.", mode, nye, endret, uendret)

        if not dry_run and prisfall:
            send_ha_notification("Baat-prisfall", "**Prisfall:**\n" + "\n".join(f"- {t}" for t in prisfall[:10]))
    except Exception as e:
        logger.error("Feil i update_database: %s", e)
    finally:
        conn.close()


async def mark_removed_ads(current_ads, session=None, dry_run=False) -> None:
    mode = "DRY RUN" if dry_run else "LIVE"
    conn = connect_to_database()
    if not conn:
        return

    try:
        cur = conn.cursor()
        active = {a["Finnkode"] for a in current_ads}
        now = datetime.now()

        if not dry_run and active:
            cur.executemany(f"UPDATE `{TABLE}` SET SistSett = %s WHERE Finnkode = %s",
                            [(now, fk) for fk in active])

        cur.execute(
            f"SELECT Finnkode FROM `{TABLE}` WHERE (Solgt = 0 OR Solgt IS NULL) "
            "AND (SistSett IS NULL OR SistSett < %s)",
            (now - timedelta(hours=48),))
        stale = [r[0] for r in cur.fetchall() if r[0] not in active]

        if not stale:
            if not dry_run:
                conn.commit()
            return

        logger.info("[%s] %d kandidater ikke sett paa over 48t - dobbeltsjekker...", mode, len(stale))

        bekreftede = []
        sem = asyncio.Semaphore(3)

        async def sjekk(fk):
            async with sem:
                await asyncio.sleep(0.5)
                if session:
                    url = f"https://www.finn.no/mobility/item/{fk}"
                    try:
                        async with session.get(url, headers=HTTP_HEADERS, timeout=HTTP_TIMEOUT) as resp:
                            if resp.status == 404:
                                bekreftede.append(fk)
                                return
                            html = await resp.text()
                            if "ikke lenger tilgjengelig" in html.lower():
                                bekreftede.append(fk)
                    except Exception as e:
                        logger.warning("Feil ved verifisering av %s: %s", fk, e)
                else:
                    bekreftede.append(fk)

        await asyncio.gather(*(sjekk(fk) for fk in stale))

        for fk in bekreftede:
            logger.info("[%s] Markerer %s som Solgt/Fjernet.", mode, fk)
            if not dry_run:
                cur.execute(f"UPDATE `{TABLE}` SET Solgt = 1, SolgtDato = %s WHERE Finnkode = %s", (now, fk))
                try:
                    cur.execute(f"INSERT INTO `{PRISENDRINGER_TABLE}` (Finnkode, Pris) VALUES (%s, %s)",
                                (fk, "Solgt/Fjernet"))
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
            method="POST")
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        logger.warning("Kunne ikke sende HA-varsling: %s", e)


async def fetch_finn_ads(session):
    ads = await fetch_all_pages(session, LISTINGS_PAGE_URL)
    if not ads:
        logger.error("Ingen annonser hentet fra Finn.no API.")
        return []
    return list(await fetch_and_combine_data(session, ads))


async def main() -> None:
    logger.info("Starter baat-scraper...")
    if DRY_RUN:
        logger.info("*** DRY RUN MODUS ***")

    ensure_schema()

    async with aiohttp.ClientSession() as session:
        ads = await fetch_finn_ads(session)
        if ads:
            update_database(ads, dry_run=DRY_RUN)
            await mark_removed_ads(ads, session=session, dry_run=DRY_RUN)

    logger.info("Ferdig.")


def run_scraper():
    asyncio.run(main())


if __name__ == "__main__":
    run_scraper()
