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
import yaml
import os

# Bestemmer om scriptet kjører lokalt (f.eks. i VSCode) eller i Home Assistant
RUNNING_LOCALLY = __name__ == "__main__" and "SUPERVISOR_OPTIONS" not in os.environ

# Logging-konfigurasjon – logg ALT til stdout slik at det vises i Home Assistant-logg
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.handlers = []
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Last inn konfigurasjon fra miljøvariabler eller lokal testdata
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

# URL for å hente bobil-annonser via FINNs API og datoformat for presentasjon
LISTINGS_PAGE_URL = "https://www.finn.no/mobility/search/api/search/SEARCH_ID_CAR_MOBILE_HOME?location=22042&location=20003&location=20007&location=22034&location=20061&location=20009&location=20008&location=20002&mileage_to=122000&mobile_home_segment=3&mobile_home_segment=1&mobile_home_segment=2&no_of_sleepers_from=4&price_from=300000&price_to=600000&sort=YEAR_DESC&stored-id=65468215&weight_to=3501&year_from=2006"
DATE_FORMAT = "%d. %b. %Y %H:%M"

# Databasekonfigurasjon
DB_CONFIG = {
    "host": options.get("databasehost", ""),
    "user": options.get("databaseusername", ""),
    "passwd": options.get("databasepassword", ""),
    "database": options.get("databasename", ""),
    "port": options.get("databaseport", 3306)
}

# Oppretter databasenkobling med automatisk reconnect og timeout
def connect_to_database():
    try:
        conn = mysql.connector.connect(
            **DB_CONFIG,
            connection_timeout=10,
            autocommit=True
        )
        logger.info("Koblet til databasen.")
        return conn
    except mysql.connector.Error as err:
        logger.error(f"Feil ved tilkobling til databasen: {err}")
        return None

# Henter JSON-data direkte fra FINNs API
async def fetch_json(session, url):
    logger.info("Henter JSON fra FINN API...")
    try:
        async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as response:
            response.raise_for_status()
            return await response.json()
    except Exception as e:
        logger.error(f"Feil ved henting av JSON fra {url}: {e}")
        return None

# Parser JSON-responsen fra FINN og bygger liste over annonser
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
                "Detaljer": None
            })
        return extracted_data
    except Exception as e:
        logger.error(f"Feil ved ekstraksjon av JSON-data: {e}")
        return []

# Henter HTML-innhold fra enkeltannonser
async def fetch_html(session, url):
    try:
        async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as response:
            response.raise_for_status()
            return await response.text()
    except Exception as e:
        logger.error(f"Feil ved henting av HTML fra {url}: {e}")
        return None

# Ekstraherer spesifikasjoner og beskrivelse fra HTML-siden for en annonse
def extract_detailed_ad_info(html_content):
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        info_dict = {}
        spesifikasjoner = soup.find('dl', class_='list-descriptive')
        if spesifikasjoner:
            items = spesifikasjoner.find_all(['dt', 'dd'])
            for i in range(0, len(items), 2):
                key = items[i].text.strip()
                value = items[i+1].text.strip()
                info_dict[key] = value
        desc_tag = soup.find('meta', property='og:description')
        info_dict["Beskrivelse"] = desc_tag['content'] if desc_tag else "Ikke tilgjengelig"
        return info_dict
    except Exception as e:
        logger.error(f"Feil under detaljuttrekk: {e}")
        return {}

# Kombinerer grunndata og detaljer for hver annonse
async def fetch_and_combine_data(session, ads):
    async def fetch_details(ad):
        html = await fetch_html(session, ad["URL"])
        if html:
            ad["Detaljer"] = extract_detailed_ad_info(html)
        return ad
    return await asyncio.gather(*(fetch_details(ad) for ad in ads))

# Rens og formatter pris som tall eller tekst med tusenskille
def normalize_and_format_price(price, output_format=True):
    try:
        normalized = re.sub(r"[^\d]", "", str(price))
        price_as_int = int(normalized)
        return f"{price_as_int:,.0f} kr".replace(",", " ") if output_format else price_as_int
    except:
        return None

# Formatter kilometerstand med tusenskille
def format_kilometerstand(km):
    try:
        normalized = re.sub(r"[^\d]", "", str(km))
        return f"{int(normalized):,} km".replace(",", " ")
    except:
        return "Ukjent"

# Vis annonser i tabell (kun ved lokal kjøring)
def display_ads(ads):
    table_data = [
        [
            ad["Finnkode"],
            ad["Annonsenavn"],
            normalize_and_format_price(ad["Pris"]),
            ad["Modell"],
            format_kilometerstand(ad["Kilometerstand"]),
            ad["Oppdatert"],
            ad.get("Detaljer", {}).get("Beskrivelse", "")[:60] + "..."
        ] for ad in ads
    ]
    headers = ["Finnkode", "Tittel", "Pris", "Modell", "Km", "Oppdatert", "Beskrivelse"]
    print(tabulate(table_data, headers=headers, tablefmt="grid"))

# Hovedfunksjon: henter, parser og viser eller lagrer data
async def main():
    logger.info("Starter script...")

    if not RUNNING_LOCALLY:
        try:
            conn = connect_to_database()
            if not conn:
                return
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT Finnkode, Pris FROM bobil")
            existing_ads = {row["Finnkode"]: row["Pris"] for row in cursor.fetchall()}
            cursor.close()
            conn.close()
        except mysql.connector.Error as err:
            logger.error(f"Feil ved henting av eksisterende annonser: {err}")
            return
    else:
        existing_ads = {}

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
            logger.info("Produksjonsmodus: her kan du kalle compare_prices_and_save()")

    logger.info("Avslutter script...")

if __name__ == "__main__":
    asyncio.run(main())
