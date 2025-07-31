#!/usr/bin/env python3
import os
import sys
import json
import re
import logging
import asyncio
import aiohttp
import subprocess
import mysql.connector
from datetime import datetime
from bs4 import BeautifulSoup
from tabulate import tabulate

# RUN_LOCALLY blir False hvis miljøvariabelen ikke er satt eller ikke finnes.
RUN_LOCALLY = os.getenv("RUN_LOCALLY", "false").lower() == "true"
logger = logging.getLogger()
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(console_handler)
else:
    # Remove duplicate handlers if any
    logger.handlers.clear()
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(console_handler)

if RUN_LOCALLY:
    # Kjører lokalt med hardkodede databaseverdier (ingen bruk av SUPERVISOR_OPTIONS)
    logger.info("Kjører lokalt med testkonfig.")

    options = {
        "databasehost": "192.168.1.66",       # Database host er satt til lokal IP
        "databaseusername": "homeassistant",  # Databasebruker
        "databasepassword": "FridaHenrik",    # Databasepassord
        "databasename": "finn_no",            # Databasenavn
        "databaseport": "3306"                # Databaseport
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

LISTINGS_PAGE_URL = "https://www.finn.no/mobility/search/api/search/SEARCH_ID_CAR_MOBILE_HOME?location=22042&location=20003&location=20007&location=22034&location=20061&location=20009&location=20008&location=20002&mileage_to=122000&mobile_home_segment=3&mobile_home_segment=1&mobile_home_segment=2&no_of_sleepers_from=4&price_from=300000&price_to=700000&sort=YEAR_DESC&stored-id=65468215&weight_to=3501&year_from=2006"
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
        conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=10, autocommit=True)
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
    """
    logger.info("Henter JSON fra FINN API...")
    try:
        async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as response:
            response.raise_for_status()
            return await response.json()
    except Exception as e:
        logger.error(f"Feil ved henting av JSON fra {url}: {e}")
        return None

async def fetch_all_pages(session: aiohttp.ClientSession, base_url: str) -> list[dict]:
    """
    Hent alle annonser fra FINN API ved å iterere over paginering (offset).
    """
    all_ads = []
    offset = 0
    page_size = 20  # FINN returnerer 20 annonser per side

    # Første kall for å hente metadata og total_matches
    initial_data = await fetch_json(session, f"{base_url}&offset={offset}")
    if not initial_data:
        logger.error("Kunne ikke hente første side.")
        return []

    total_matches = initial_data.get("metadata", {}).get("total_matches", 0)
    logger.info(f"Totalt antall annonser: {total_matches}")
    all_ads.extend(extract_info_from_json(initial_data))

    # Iterer over resten
    for offset in range(page_size, total_matches, page_size):
        await asyncio.sleep(0.2)  # For å unngå throttling
        paged_url = f"{base_url}&offset={offset}"
        logger.info(f"Henter annonser med offset={offset}")
        json_data = await fetch_json(session, paged_url)
        if not json_data:
            logger.warning(f"Ingen data hentet ved offset={offset}.")
            break
        ads = extract_info_from_json(json_data)
        if not ads:
            logger.info("Tom side – avslutter paginering.")
            break
        all_ads.extend(ads)

    logger.info(f"Totalt hentet {len(all_ads)} annonser.")
    return all_ads

def extract_info_from_json(json_data: dict) -> list[dict]:
    """
    Ekstraher relevante felter fra FINN JSON-data.
    """
    try:
        ads = json_data.get("docs", [])
        extracted_data = []
        for ad in ads:
            timestamp = ad.get("timestamp")
            formatted_date = datetime.fromtimestamp(timestamp / 1000).strftime(DATE_FORMAT) if timestamp else "Ukjent"
            extracted_data.append({
                "Finnkode": ad.get("id"),
                "Annonsenavn": ad.get("heading"),
                "Pris": ad.get("price", {}).get("amount"),
                "Modell": ad.get("year"),
                "Kilometerstand": ad.get("mileage"),
                "Oppdatert": formatted_date,
                "URL": ad.get("canonical_url"),
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

async def fetch_and_combine_data(session, ads):
    async def fetch_details(ad):
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

def display_ads(ads: list[dict]) -> None:
    """
    Vis annonser i tabellformat.
    """
    from textwrap import shorten
    table_data = [
        [
            ad["Finnkode"],
            ad["Annonsenavn"],
            normalize_and_format_price(ad["Pris"]),
            ad["Modell"],
            format_kilometerstand(ad["Kilometerstand"]),
            ad["Oppdatert"],
            ad["Detaljer"].get("Girkasse", ""),
            ad["Detaljer"].get("Type bobil", ""),
            ad["Detaljer"].get("Nyttelast", ""),
            shorten(ad["Detaljer"].get("Beskrivelse", ""), width=60, placeholder="...")
        ] for ad in ads
    ]
    headers = ["Finnkode", "Tittel", "Pris", "Modell", "Km", "Oppdatert", "Girkasse", "Type", "Nyttelast", "Beskrivelse"]
    print(tabulate(table_data, headers=headers, tablefmt="grid"))

def update_database(ads: list[dict]) -> None:
    """
    Oppdater database med annonser.
    Logger eksplisitt hvis noen felt endres.
    Pris lagres nå som int i databasen.
    Merk: For asynkron database, vurder aiomysql eller lignende bibliotek.
    """
    logger.info("Starter databaseoppdatering for %d annonser.", len(ads))
    try:
        conn = connect_to_database()
        if not conn:
            logger.error("Ingen tilkobling til databasen. Avbryter oppdatering.")
            return
        cursor = conn.cursor()
        if not ads:
            logger.warning("Ingen annonser å oppdatere i databasen.")
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
                    # Sammenlign som str for alle felt unntatt Pris (int)
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
                    logger.info(f"Endringer for Finnkode {finnkode}: {', '.join(endringer)}")
            else:
                logger.info(f"Ny annonse lagres med Finnkode {finnkode}")

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
            logger.debug(f"SQL: {query}")
            logger.debug(f"Data: {data}")
            try:
                cursor.execute(query, data)
            except Exception as e:
                logger.error(f"Feil ved lagring av annonse {finnkode}: {e}")
        logger.info(f"Lagret {len(ads)} annonser i databasen.")
        conn.commit()
        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        logger.error(f"Feil ved databaseoppdatering: {err}")
    except Exception as e:
        logger.error(f"Uventet feil ved databaseoppdatering: {e}")

async def main() -> None:
    """
    Hovedfunksjon for scriptet.
    """
    logger.info("Starter script...")
    async with aiohttp.ClientSession() as session:
        ads_data = await fetch_all_pages(session, LISTINGS_PAGE_URL)
        if not ads_data:
            logger.error("Ingen annonser hentet fra FINN API.")
            return

        ads_data = extract_info_from_json(json_data)
        if not ads_data:
            logger.error("Ingen annonser funnet i JSON-data.")
            return
        detailed_ads = await fetch_and_combine_data(session, ads_data)
        if not RUN_LOCALLY:
            # Databaseoppdatering skjer kun hvis RUN_LOCALLY er False.
            # For å deaktivere databaseoppdatering, kommenter ut linjen under:
            update_database(detailed_ads)
            #display_ads(detailed_ads)
        else:
            #display_ads(detailed_ads)
            update_database(detailed_ads)
    logger.info("Avslutter script...")

if __name__ == "__main__":
    asyncio.run(main())
