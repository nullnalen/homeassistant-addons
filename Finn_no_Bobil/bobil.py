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

# Logging-konfigurasjon
LOG_FILE = "/data/bobil_script.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


# Last inn konfigurasjon fra Home Assistant
options = json.loads(os.getenv("SUPERVISOR_OPTIONS", "{}"))

if not options:
    logger.error("SUPERVISOR_OPTIONS mangler eller er feil formatert.")
    sys.exit(1)

# URL og datoformat
LISTINGS_PAGE_URL = "https://www.finn.no/car/mobilehome/search.html?location=22042&location=20003&location=20007&location=22034&location=20061&location=20009&location=20008&location=20002&mileage_to=122000&mobile_home_segment=3&mobile_home_segment=1&mobile_home_segment=2&no_of_sleepers_from=4&price_from=300000&price_to=600000&sort=YEAR_DESC&stored-id=65468215&weight_to=3501&year_from=2006"
DATE_FORMAT = "%d. %b. %Y %H:%M"

DB_CONFIG = {
    "host": options.get("databasehost", ""),
    "user": options.get("databaseusername", ""),
    "passwd": options.get("databasepassword", ""),
    "database": options.get("databasename", ""),
    "port": options.get("databaseport", "")
}

# Mock-database for testing
mock_db = {}

def connect_to_database():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        logger.info("Koblet til databasen.")
        return conn
    except mysql.connector.Error as err:
        logger.error(f"Feil ved tilkobling til databasen: {err}")
        return None

async def fetch_html(session, url):
    logger.debug(f"Henter HTML fra: {url}")
    try:
        async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as response:
            response.raise_for_status()
            return await response.text()
    except Exception as e:
        logger.error(f"Feil ved henting av HTML fra {url}: {e}")
        return None

async def fetch_json(session, url):
    logger.info("Henter JSON fra nettsiden...")
    html_content = await fetch_html(session, url)
    if not html_content:
        return None

    try:
        json_start_identifier = '<script id="__NEXT_DATA__" type="application/json">'
        json_end_identifier = '</script>'
        start_index = html_content.find(json_start_identifier) + len(json_start_identifier)
        end_index = html_content.find(json_end_identifier, start_index)
        if start_index == -1 or end_index == -1:
            logger.warning("JSON ikke funnet i HTML.")
            return None
        return json.loads(html_content[start_index:end_index])
    except Exception as e:
        logger.error(f"Feil ved parsing av JSON: {e}")
        return None

def extract_info_from_json(json_data):
    try:
        ads = json_data.get('props', {}).get('pageProps', {}).get('search', {}).get('docs', [])
        if not ads:
            logger.warning("Ingen annonser funnet i JSON.")
            return []

        extracted_data = []
        for ad in ads:
            oppdatert = ad.get('timestamp', None)
            formatted_date = "Ikke tilgjengelig"
            if oppdatert:
                parsed_date = datetime.fromtimestamp(oppdatert / 1000)
                formatted_date = parsed_date.strftime(DATE_FORMAT)

            extracted_data.append({
                "Finnkode": ad.get('id'),
                "Annonsenavn": ad.get('heading', "Ingen tittel"),
                "Pris": ad.get('price', {}).get('amount', "Ukjent pris"),
                "Modell": ad.get('year', "Ukjent modell"),
                "Kilometerstand": ad.get('mileage', "Ukjent kilometerstand"),
                "Oppdatert": formatted_date,
                "URL": ad.get('canonical_url', "URL ikke tilgjengelig"),
                "Beskrivelse": None,
                "Detaljer": None
            })
        logger.info(f"Fant {len(extracted_data)} annonser i JSON.")
        return extracted_data
    except Exception as e:
        logger.error(f"Feil ved ekstraksjon av JSON-data: {e}")
        return []

def extract_detailed_ad_info(html_content):
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        info_dict = {}

        fields = [
            "Totalvekt", "Egenvekt", "Nyttelast", "Lengde",
            "Type seng", "Type bobil", "Reg. sitteplasser", "Soveplasser"
        ]

        spesifikasjoner = soup.find('dl', class_='list-descriptive')
        if spesifikasjoner:
            specification_items = spesifikasjoner.find_all(['dt', 'dd'])
            for index in range(0, len(specification_items), 2):
                key = specification_items[index].text.strip()
                value = specification_items[index + 1].text.strip()
                if key in fields:
                    info_dict[key] = value

        description_tag = soup.find('meta', property='og:description')
        if description_tag and description_tag.get('content'):
            info_dict["Beskrivelse"] = description_tag['content'].strip()
        else:
            info_dict["Beskrivelse"] = "Ikke tilgjengelig"

        return info_dict
    except Exception as e:
        logger.error(f"Feil under detaljuttrekk: {e}")
        return {}

async def fetch_and_combine_data(session, ads):
    async def fetch_details(ad):
        html_content = await fetch_html(session, ad["URL"])
        if html_content:
            detailed_info = extract_detailed_ad_info(html_content)
            ad.update({"Detaljer": detailed_info})
        return ad

    tasks = [fetch_details(ad) for ad in ads]
    return await asyncio.gather(*tasks)

def log_price_change(finnkode, ny_pris):
    try:
        conn = connect_to_database()
        if not conn:
            return

        cursor = conn.cursor()
        query = """
            INSERT INTO prisendringer (Finnkode, Tidspunkt, Pris)
            VALUES (%s, NOW(), %s)
        """
        data = (finnkode, ny_pris)
        cursor.execute(query, data)
        conn.commit()
        logger.info(f"Prisendring logget for Finnkode {finnkode}: {ny_pris}")
    except mysql.connector.Error as err:
        logger.error(f"Feil ved loggføring av prisendring for Finnkode {finnkode}: {err}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def update_bobil_table(ad):
    try:
        conn = connect_to_database()
        if not conn:
            return

        cursor = conn.cursor()
        query = """
            INSERT INTO bobil (Finnkode, Annonsenavn, Modell, Kilometerstand, Beskrivelse, Nyttelast, 
                               Typebobil, Oppdatert, URL, Pris)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                Annonsenavn = VALUES(Annonsenavn),
                Modell = VALUES(Modell),
                Kilometerstand = VALUES(Kilometerstand),
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
            ad["Detaljer"].get("Beskrivelse", "Ikke tilgjengelig"),
            ad["Detaljer"].get("Nyttelast", "Ikke tilgjengelig"),
            ad["Detaljer"].get("Type bobil", "Ikke tilgjengelig"),
            ad["Oppdatert"],
            ad["URL"],
            normalize_and_format_price(ad["Pris"], output_format=True),
        )

        # Logg SQL-spørringen for debugging
        debug_query = query % tuple(
            [repr(d) if isinstance(d, str) else d for d in data]
        )
        logger.debug(f"SQL-spørring: {debug_query}")

        cursor.execute(query, data)
        conn.commit()
        logger.info(f"Bobil-tabellen oppdatert for Finnkode {ad['Finnkode']}")
    except mysql.connector.Error as err:
        logger.error(f"Feil ved oppdatering av bobil-tabellen for Finnkode {ad['Finnkode']}: {err}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()


def normalize_and_format_price(price, output_format=True):
    """
    Normaliserer pris ved å fjerne valutasymboler og mellomrom,
    returnerer prisen som et heltall for sammenligning eller formaterer prisen for utskrift.
    """
    try:
        # Fjern alt unntatt tall
        normalized = re.sub(r"[^\d]", "", str(price))
        price_as_int = int(normalized)

        # Returner formatert pris som i databasen
        if output_format:
            return f"{price_as_int:,.0f} kr".replace(",", " ")
        return price_as_int
    except Exception as e:
        logger.warning(f"Kan ikke konvertere pris: {price}")
        return None

def format_kilometerstand(kilometerstand):
    """
    Formaterer kilometerstand for å matche eksisterende dataformat.
    Eksempel: 64500 -> "64 500 km"
    """
    try:
        # Fjern alle ikke-numeriske tegn (unntatt mellomrom)
        normalized = re.sub(r"[^\d]", "", str(kilometerstand))
        # Del opp med tusenskiller og legg til "km"
        formatted = f"{int(normalized):,}".replace(",", " ") + " km"
        return formatted
    except Exception as e:
        logger.warning(f"Kan ikke formatere kilometerstand: {kilometerstand}. Feil: {e}")
        return "Ikke tilgjengelig"  # Fallback for ugyldige verdier


def compare_prices_and_save(new_ads, existing_ads):
    logger.info("Sammenligner priser og oppdaterer databasen...")
    for ad in new_ads:
        finnkode = ad["Finnkode"]
        ny_pris = normalize_and_format_price(ad["Pris"])
        formatted_ny_pris = normalize_and_format_price(ad["Pris"], output_format=True)

        if ny_pris is None:
            logger.warning(f"Kan ikke normalisere ny pris for Finnkode {finnkode}. Hopper over.")
            continue

        if finnkode in existing_ads:
            gammel_pris = normalize_and_format_price(existing_ads[finnkode])
            formatted_gammel_pris = normalize_and_format_price(existing_ads[finnkode], output_format=True)

            if gammel_pris is None:
                logger.warning(f"Kan ikke normalisere gammel pris for Finnkode {finnkode}. Hopper over.")
                continue

            if gammel_pris != ny_pris:
                logger.info(f"Prisendring for Finnkode {finnkode}: Gammel pris: {formatted_gammel_pris}, Ny pris: {formatted_ny_pris}")
                log_price_change(finnkode, formatted_ny_pris)
                update_bobil_table(ad)
            else:
                logger.debug(f"Ingen prisendring for Finnkode {finnkode}.")
        else:
            logger.info(f"Ny annonse funnet: Finnkode {finnkode}, Pris: {formatted_ny_pris}")
            update_bobil_table(ad)


def display_mock_db(mock_db):
    if not mock_db:
        logger.info("Mock-databasen er tom.")
        print("Mock-databasen er tom.")
        return

    table_data = [
        [
            ad["Finnkode"],
            ad["Annonsenavn"],
            ad["Pris"],
            ad["Modell"],
            ad["Kilometerstand"],
            ad["Oppdatert"],
            ad.get("Detaljer", {}).get("Beskrivelse", "N/A")[:50] + "..."
        ]
        for ad in mock_db.values()
    ]

    headers = ["Finnkode", "Annonsenavn", "Pris", "Modell", "Kilometerstand", "Oppdatert", "Beskrivelse (kort)"]

    print(tabulate(table_data, headers=headers, tablefmt="grid"))


def update_last_run():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO bobil_script_status (last_run) VALUES (NOW()) ON DUPLICATE KEY UPDATE last_run=NOW();")
        conn.commit()
        logger.info("Oppdaterte tidspunkt for siste kjøring i databasen.")
    except mysql.connector.Error as err:
        logger.error(f"Feil ved oppdatering av siste kjøring: {err}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

async def main():
    logger.info("Starter script...")
    

    try:
        conn = connect_to_database()
        if not conn:
            return
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT Finnkode, Pris FROM bobil")
        existing_ads = {row["Finnkode"]: row["Pris"] for row in cursor.fetchall()}
        logger.info(f"Hentet {len(existing_ads)} eksisterende annonser fra databasen.")
    except mysql.connector.Error as err:
        logger.error(f"Feil ved henting av eksisterende annonser: {err}")
        return
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

    async with aiohttp.ClientSession() as session:
        json_data = await fetch_json(session, LISTINGS_PAGE_URL)
        if not json_data:
            logger.error("Ingen JSON-data funnet.")
            return

        ads_data = extract_info_from_json(json_data)
        if not ads_data:
            logger.warning("Ingen annonser funnet i JSON-data.")
            return

        detailed_ads = await fetch_and_combine_data(session, ads_data)
        compare_prices_and_save(detailed_ads, existing_ads)
    update_last_run()
    logger.info("Avslutter script...")




if __name__ == "__main__":
    asyncio.run(main())