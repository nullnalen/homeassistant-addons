#!/usr/bin/env python3
import subprocess
import sys
import logging
import asyncio
import aiohttp
import json
from datetime import datetime
from bs4 import BeautifulSoup
from tabulate import tabulate
import re
import mysql.connector
import os

RUNNING_LOCALLY = __name__ == "__main__" and "SUPERVISOR_OPTIONS" not in os.environ

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.handlers = []
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

if RUNNING_LOCALLY:
    logger.info("Kjører lokalt med testkonfig.")
    options = {
        "databasehost": "localhost",
        "databaseusername": "user",
        "databasepassword": "pass",
        "databasename": "testdb",
        "databaseport": 3306
    }
else:
    try:
        options = json.loads(os.getenv("SUPERVISOR_OPTIONS", "{}"))
    except Exception as e:
        logger.error("Feil ved lasting av SUPERVISOR_OPTIONS: %s", e)
        sys.exit(1)
    if not options:
        logger.error("SUPERVISOR_OPTIONS mangler eller er feil formatert.")
        sys.exit(1)

LISTINGS_PAGE_URL = "https://www.finn.no/mobility/search/api/search/SEARCH_ID_CAR_MOBILE_HOME?location=22042&location=20003&location=20007&location=22034&location=20061&location=20009&location=20008&location=20002&mileage_to=122000&mobile_home_segment=3&mobile_home_segment=1&mobile_home_segment=2&no_of_sleepers_from=4&price_from=300000&price_to=600000&sort=YEAR_DESC&stored-id=65468215&weight_to=3501&year_from=2006"
DATE_FORMAT = "%d. %b. %Y %H:%M"

DB_CONFIG = {
    "host": options.get("databasehost", ""),
    "user": options.get("databaseusername", ""),
    "passwd": options.get("databasepassword", ""),
    "database": options.get("databasename", ""),
    "port": options.get("databaseport", 3306)
}

def connect_to_database():
    try:
        conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=10, autocommit=True)
        logger.info("Koblet til databasen.")
        return conn
    except mysql.connector.Error as err:
        logger.error(f"Feil ved tilkobling til databasen: {err}")
        return None

async def fetch_json(session, url):
    logger.info("Henter JSON fra FINN API...")
    try:
        async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as response:
            response.raise_for_status()
            return await response.json()
    except Exception as e:
        logger.error(f"Feil ved henting av JSON fra {url}: {e}")
        return None

def extract_info_from_json(json_data):
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
                "Beskrivelse": None,
                "Detaljer": {}
            })
        return extracted_data
    except Exception as e:
        logger.error(f"Feil ved ekstraksjon av JSON-data: {e}")
        return []

async def fetch_html(session, url):
    try:
        async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as response:
            response.raise_for_status()
            return await response.text()
    except Exception as e:
        logger.error(f"Feil ved henting av HTML fra {url}: {e}")
        return None

def extract_detailed_ad_info(html_content):
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
        info_dict["Beskrivelse"] = desc_tag['content'] if desc_tag else "Ikke tilgjengelig"

        if RUNNING_LOCALLY:
            logger.info("--- Detaljer fra annonse ---")
            for k, v in info_dict.items():
                logger.info(f"{k}: {v}")

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

def normalize_and_format_price(price, output_format=True):
    try:
        normalized = re.sub(r"[^\d]", "", str(price))
        price_as_int = int(normalized)
        return f"{price_as_int:,.0f} kr".replace(",", " ") if output_format else price_as_int
    except:
        return None

def format_kilometerstand(km):
    try:
        normalized = re.sub(r"[^\d]", "", str(km))
        return f"{int(normalized):,} km".replace(",", " ")
    except:
        return "Ukjent"

def display_ads(ads):
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
            ad["Detaljer"].get("Beskrivelse", "")[:60] + "..."
        ] for ad in ads
    ]
    headers = ["Finnkode", "Tittel", "Pris", "Modell", "Km", "Oppdatert", "Girkasse", "Type", "Nyttelast", "Beskrivelse"]
    print(tabulate(table_data, headers=headers, tablefmt="grid"))

def update_database(ads):
    try:
        conn = connect_to_database()
        if not conn:
            return
        cursor = conn.cursor()
        for ad in ads:
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
                ad["Finnkode"],
                ad["Annonsenavn"],
                ad["Modell"],
                format_kilometerstand(ad["Kilometerstand"]),
                ad["Detaljer"].get("Girkasse", "Ikke oppgitt"),
                ad["Detaljer"].get("Beskrivelse", "Ikke tilgjengelig"),
                ad["Detaljer"].get("Nyttelast", "Ikke oppgitt"),
                ad["Detaljer"].get("Type bobil", "Ikke oppgitt"),
                ad["Oppdatert"],
                ad["URL"],
                normalize_and_format_price(ad["Pris"], output_format=True)
            )
            cursor.execute(query, data)
        logger.info(f"Lagret {len(ads)} annonser i databasen.")
        conn.commit()
        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        logger.error(f"Feil ved databaseoppdatering: {err}")

async def main():
    logger.info("Starter script...")
    async with aiohttp.ClientSession() as session:
        json_data = await fetch_json(session, LISTINGS_PAGE_URL)
        if not json_data:
            return
        ads_data = extract_info_from_json(json_data)
        if not ads_data:
            return
        detailed_ads = await fetch_and_combine_data(session, ads_data)
        if RUNNING_LOCALLY:
            display_ads(detailed_ads)
        else:
            update_database(detailed_ads)
    logger.info("Avslutter script...")

if __name__ == "__main__":
    asyncio.run(main())
