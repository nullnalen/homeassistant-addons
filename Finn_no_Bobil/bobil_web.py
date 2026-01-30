#!/usr/bin/env python3
"""
Finn.no Bobil — Ingress Web UI
Flask-basert webgrensesnitt for å vise bobilannonser fra databasen.
"""
import os
import sys
import json
import re
import logging
import threading
from datetime import datetime

import mysql.connector
from mysql.connector import pooling
from flask import Flask, render_template_string, request, redirect, url_for, jsonify
from waitress import serve

# Logging
logger = logging.getLogger("bobil_web")
logger.setLevel(logging.INFO)
logger.handlers.clear()
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

# Konfigurasjon
try:
    options_str = os.getenv("SUPERVISOR_OPTIONS", "{}")
    options = json.loads(options_str)
except Exception as e:
    logger.error("Feil ved lasting av SUPERVISOR_OPTIONS: %s", e)
    options = {}

DB_CONFIG = {
    "host": options.get("databasehost", ""),
    "user": options.get("databaseusername", ""),
    "passwd": options.get("databasepassword", ""),
    "database": options.get("databasename", ""),
    "port": options.get("databaseport", 3306),
}

# Scraper-status
scraper_status = {
    "last_run": None,
    "running": False,
    "error": None,
}

# Norske måneder for datoparsing
NO_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "mai": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "okt": 10, "nov": 11, "des": 12,
}


def parse_norwegian_date(date_str):
    """Parse norsk datostreng som '25. jan. 2025 14:30' til datetime."""
    if not date_str or date_str == "Ukjent":
        return None
    try:
        # Fjern punktum etter måned og normaliser
        s = date_str.strip().lower()
        for no, num in NO_MONTHS.items():
            if no in s:
                s = re.sub(rf"\b{no}\.?\b", f"{num:02d}", s)
                break
        # Forventet format nå: "25. 01. 2025 14:30" eller "25. 01 2025 14:30"
        m = re.match(r"(\d{1,2})\.\s*(\d{2})\.?\s+(\d{4})\s+(\d{2}):(\d{2})", s)
        if m:
            return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)),
                            int(m.group(4)), int(m.group(5)))
    except Exception:
        pass
    return None


def parse_price(price_val):
    """Parse pris til int. Håndterer både int og streng-format."""
    if price_val is None:
        return None
    if isinstance(price_val, (int, float)):
        return int(price_val)
    s = str(price_val)
    if "solgt" in s.lower():
        return None
    try:
        return int(re.sub(r"[^\d]", "", s))
    except (ValueError, TypeError):
        return None


def parse_km(km_val):
    """Parse kilometerstand til int."""
    if km_val is None:
        return None
    if isinstance(km_val, (int, float)):
        return int(km_val)
    try:
        return int(re.sub(r"[^\d]", "", str(km_val)))
    except (ValueError, TypeError):
        return None


def format_price(price_int):
    """Formater int-pris til lesbar streng."""
    if not price_int:
        return "—"
    return f"{price_int:,.0f} kr".replace(",", " ")


def format_age(date_str):
    """Formater alder fra norsk datostreng til lesbar tekst, fargeklasse og sorteringsverdi."""
    dato = parse_norwegian_date(date_str)
    if not dato:
        return "Ukjent", "age-unknown", 99999
    delta = datetime.now() - dato
    dager = delta.days
    sort_val = dager
    if dager == 0:
        timer = delta.seconds // 3600
        if timer == 0:
            return "Nå", "age-fresh", 0
        return f"{timer}t siden", "age-fresh", 0
    if dager == 1:
        return "I går", "age-fresh", 1
    if dager < 7:
        return f"{dager} dager", "age-fresh", dager
    if dager < 30:
        return f"{dager} dager", "age-weeks", dager
    if dager < 365:
        mnd = dager // 30
        return f"{mnd} mnd", "age-old", dager
    return f"{dager // 365} år", "age-old", dager


_db_pool = None


def _get_pool():
    """Lazy-init connection pool."""
    global _db_pool
    if _db_pool is None:
        try:
            _db_pool = pooling.MySQLConnectionPool(
                pool_name="bobil_pool",
                pool_size=5,
                pool_reset_session=True,
                connection_timeout=10,
                **DB_CONFIG,
            )
            logger.info("DB connection pool opprettet (pool_size=5).")
        except Exception as e:
            logger.error("Kunne ikke opprette connection pool: %s", e)
            return None
    return _db_pool


def get_db():
    """Hent en tilkobling fra connection pool."""
    pool = _get_pool()
    if pool:
        try:
            return pool.get_connection()
        except Exception as e:
            logger.error("Kunne ikke hente tilkobling fra pool: %s", e)
    # Fallback til direkte tilkobling
    try:
        conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=10)
        return conn
    except Exception as e:
        logger.error("DB-tilkoblingsfeil: %s", e)
        return None


def ensure_db_columns():
    """Sørg for at nye kolonner og indekser finnes i databasen."""
    conn = get_db()
    if not conn:
        return
    try:
        cur = conn.cursor()
        # Nye kolonner
        for col, coltype in [("ImageURL", "TEXT"), ("Lokasjon", "VARCHAR(255)"), ("Solgt", "TINYINT(1) DEFAULT 0")]:
            try:
                cur.execute(f"ALTER TABLE bobil ADD COLUMN {col} {coltype}")
                logger.info("La til kolonne %s i bobil-tabellen.", col)
            except mysql.connector.Error as e:
                if e.errno == 1060:  # Duplicate column
                    pass
                else:
                    logger.error("Feil ved ALTER TABLE for %s: %s", col, e)
        # Migrer eksisterende solgt/fjernet-rader til Solgt=1
        try:
            cur.execute("UPDATE bobil SET Solgt = 1 WHERE Pris LIKE '%Solgt%' OR Pris LIKE '%Fjernet%'")
            if cur.rowcount > 0:
                logger.info("Migrerte %d rader med Solgt/Fjernet til Solgt=1.", cur.rowcount)
        except Exception as e:
            logger.error("Feil ved migrering av solgt-status: %s", e)

        # Indekser for raskere spørringer
        indexes = [
            ("idx_prisendringer_finnkode", "prisendringer", "Finnkode"),
            ("idx_prisendringer_tidspunkt", "prisendringer", "Tidspunkt"),
            ("idx_prisendringer_finnkode_tidspunkt", "prisendringer", "Finnkode, Tidspunkt"),
            ("idx_bobil_modell", "bobil", "Modell"),
            ("idx_bobil_pris", "bobil", "Pris(50)"),
        ]
        for idx_name, table, columns in indexes:
            try:
                cur.execute(f"CREATE INDEX {idx_name} ON {table} ({columns})")
                logger.info("Opprettet indeks %s på %s.", idx_name, table)
            except mysql.connector.Error as e:
                if e.errno == 1061:  # Duplicate key name
                    pass
                else:
                    logger.error("Feil ved opprettelse av indeks %s: %s", idx_name, e)
        conn.commit()
    except Exception as e:
        logger.error("Feil i ensure_db_columns: %s", e)
    finally:
        conn.close()


def get_total_count():
    """Hent totalt antall annonser i databasen."""
    conn = get_db()
    if not conn:
        return 0
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM bobil")
        return cur.fetchone()[0]
    except Exception:
        return 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# View-funksjoner
# ---------------------------------------------------------------------------

def get_prisendringer():
    """View 1: Annonser med prisendringer, sortert etter antall."""
    conn = get_db()
    if not conn:
        return []
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT b.Finnkode, b.Annonsenavn, b.Modell, b.Pris, b.Oppdatert,
                   COUNT(p.Pris) AS AntallEndringer,
                   MIN(NULLIF(CAST(REGEXP_REPLACE(p.Pris, '[^0-9]', '') AS UNSIGNED), 0)) AS LavestePris,
                   MAX(NULLIF(CAST(REGEXP_REPLACE(p.Pris, '[^0-9]', '') AS UNSIGNED), 0)) AS HoyestePris,
                   b.URL
            FROM bobil b
            JOIN prisendringer p ON b.Finnkode = p.Finnkode
            GROUP BY b.Finnkode, b.Annonsenavn, b.Modell, b.Pris, b.Oppdatert, b.URL
            ORDER BY AntallEndringer DESC
        """)
        rows = cur.fetchall()
        for r in rows:
            pris = parse_price(r["Pris"])
            laveste = parse_price(r["LavestePris"])
            hoyeste = parse_price(r["HoyestePris"])
            # Hvis nåværende pris mangler (f.eks. solgt), bruk siste kjente pris
            if not pris and laveste:
                pris = laveste
            r["NaaverendePris"] = format_price(pris)
            r["LavestePrisF"] = format_price(laveste)
            r["HoyestePrisF"] = format_price(hoyeste)
            r["FinnURL"] = f"https://www.finn.no/mobility/item/{r['Finnkode']}"
            r["Alder"], r["AlderClass"], r["AlderSort"] = format_age(r.get("Oppdatert", ""))
        return rows
    except Exception as e:
        logger.error("Feil i get_prisendringer: %s", e)
        return []
    finally:
        conn.close()


def get_kjopsscore():
    """View 2: Kjøpsscore — rangert liste."""
    conn = get_db()
    if not conn:
        return []
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT b.Finnkode, b.Annonsenavn, b.Modell, b.Pris, b.Kilometerstand,
                   b.Oppdatert, b.Beskrivelse,
                   COUNT(p.Pris) AS AntallEndringer,
                   MIN(NULLIF(CAST(REGEXP_REPLACE(p.Pris, '[^0-9]', '') AS UNSIGNED), 0)) AS LavestePris,
                   MAX(NULLIF(CAST(REGEXP_REPLACE(p.Pris, '[^0-9]', '') AS UNSIGNED), 0)) AS HoyestePris
            FROM bobil b
            LEFT JOIN prisendringer p ON b.Finnkode = p.Finnkode
            WHERE (b.Solgt = 0 OR b.Solgt IS NULL)
            GROUP BY b.Finnkode, b.Annonsenavn, b.Modell, b.Pris,
                     b.Kilometerstand, b.Oppdatert, b.Beskrivelse
        """)
        rows = cur.fetchall()

        results = []
        now = datetime.now()
        keywords = ["køye", "familie", "vendbare seter", "kapteinstoler"]

        for r in rows:
            pris = parse_price(r["Pris"])
            hoyeste = r["HoyestePris"]  # Allerede int fra CAST, eller None
            laveste = r["LavestePris"]
            dato = parse_norwegian_date(r["Oppdatert"])

            if not pris or not dato:
                continue

            dager = (now - dato).days
            if dager > 60:
                continue

            # Fallback til nåværende pris hvis ingen prishistorikk
            if not hoyeste:
                hoyeste = pris
            if not laveste:
                laveste = pris

            # Kjøpsscore: prisfall% * (dager+1) + 5 * antall endringer + dager
            prisfall_pct = 0
            if hoyeste > 0 and hoyeste > pris:
                prisfall_pct = ((hoyeste - pris) / hoyeste) * 100

            score = round(prisfall_pct * (dager + 1) + r["AntallEndringer"] * 5 + dager)

            # Søketreff
            tekst = f"{r['Annonsenavn']} {r.get('Beskrivelse', '')}".lower()
            treff = [kw for kw in keywords if kw.lower() in tekst]

            results.append({
                "Finnkode": r["Finnkode"],
                "Annonsenavn": r["Annonsenavn"],
                "Modell": r["Modell"],
                "NaaverendePris": format_price(pris),
                "LavestePris": format_price(laveste),
                "HoyestePris": format_price(hoyeste),
                "AntallEndringer": r["AntallEndringer"],
                "DagerPaaMarkedet": dager,
                "KjopsScore": score,
                "Soketreff": ", ".join(treff) if treff else "",
                "FinnURL": f"https://www.finn.no/mobility/item/{r['Finnkode']}",
                "ErNy": dager <= 1,
            })

        results.sort(key=lambda x: x["KjopsScore"], reverse=True)
        return results[:100]
    except Exception as e:
        logger.error("Feil i get_kjopsscore: %s", e)
        return []
    finally:
        conn.close()


def get_prisutvikling():
    """View 3: Gjennomsnittspris per modellår per måned."""
    conn = get_db()
    if not conn:
        return []
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT b.Modell,"
            " DATE_FORMAT(p.Tidspunkt, %s) AS Periode,"
            " ROUND(AVG(NULLIF(CAST(REGEXP_REPLACE(p.Pris, '[^0-9]', '') AS UNSIGNED), 0))) AS GjSnittPris,"
            " COUNT(*) AS Antall"
            " FROM prisendringer p"
            " JOIN bobil b ON p.Finnkode = b.Finnkode"
            " WHERE b.Modell IS NOT NULL"
            " AND p.Pris NOT LIKE %s"
            " GROUP BY b.Modell, DATE_FORMAT(p.Tidspunkt, %s)"
            " ORDER BY b.Modell DESC, Periode",
            ("%Y-%m", "%Solgt%", "%Y-%m")
        )
        rows = cur.fetchall()
        for r in rows:
            r["GjSnittPrisF"] = format_price(parse_price(r["GjSnittPris"]))
        return rows
    except Exception as e:
        logger.error("Feil i get_prisutvikling: %s", e)
        return []
    finally:
        conn.close()


def get_sokresultater(keywords_str):
    """View 4: Nøkkelord-søk i beskrivelse og annonsenavn."""
    if not keywords_str or not keywords_str.strip():
        return []
    conn = get_db()
    if not conn:
        return []
    try:
        terms = [t.strip() for t in keywords_str.split(",") if t.strip()]
        if not terms:
            return []

        conditions = " OR ".join(
            ["(b.Beskrivelse LIKE %s OR b.Annonsenavn LIKE %s)"] * len(terms)
        )
        params = []
        for t in terms:
            params.extend([f"%{t}%", f"%{t}%"])

        cur = conn.cursor(dictionary=True)
        cur.execute(f"""
            SELECT b.Finnkode, b.Annonsenavn, b.Beskrivelse, b.Modell,
                   b.Kilometerstand, b.Girkasse, b.Nyttelast, b.Typebobil,
                   b.Oppdatert, b.Pris,
                   COUNT(p.Pris) AS AntallEndringer,
                   MIN(NULLIF(CAST(REGEXP_REPLACE(p.Pris, '[^0-9]', '') AS UNSIGNED), 0)) AS LavestePris,
                   MAX(NULLIF(CAST(REGEXP_REPLACE(p.Pris, '[^0-9]', '') AS UNSIGNED), 0)) AS HoyestePris
            FROM bobil b
            LEFT JOIN prisendringer p ON b.Finnkode = p.Finnkode
            WHERE {conditions}
            GROUP BY b.Finnkode, b.Annonsenavn, b.Beskrivelse, b.Modell,
                     b.Kilometerstand, b.Girkasse, b.Nyttelast, b.Typebobil,
                     b.Oppdatert, b.Pris
            ORDER BY b.Oppdatert DESC
        """, params)
        rows = cur.fetchall()

        for r in rows:
            pris = parse_price(r["Pris"])
            laveste = parse_price(r["LavestePris"])
            hoyeste = parse_price(r["HoyestePris"])
            if not pris and laveste:
                pris = laveste
            if not laveste and pris:
                laveste = pris
            if not hoyeste and pris:
                hoyeste = pris
            r["NaaverendePris"] = format_price(pris)
            r["LavestePrisF"] = format_price(laveste)
            r["HoyestePrisF"] = format_price(hoyeste)
            r["FinnURL"] = f"https://www.finn.no/mobility/item/{r['Finnkode']}"
            r["Alder"], r["AlderClass"], r["AlderSort"] = format_age(r.get("Oppdatert", ""))
            # Finn hvilke termer som ga treff
            tekst = f"{r['Annonsenavn']} {r.get('Beskrivelse', '')}".lower()
            r["Soketreff"] = ", ".join(t for t in terms if t.lower() in tekst)
        return rows
    except Exception as e:
        logger.error("Feil i get_sokresultater: %s", e)
        return []
    finally:
        conn.close()


def get_filter_options():
    """Hent unike verdier for filterpanelet."""
    conn = get_db()
    if not conn:
        return {"modeller": [], "typer": [], "girkasser": []}
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT Modell FROM bobil WHERE Modell IS NOT NULL ORDER BY Modell DESC")
        modeller = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT DISTINCT Typebobil FROM bobil WHERE Typebobil IS NOT NULL AND Typebobil != 'Ikke oppgitt' ORDER BY Typebobil")
        typer = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT DISTINCT Girkasse FROM bobil WHERE Girkasse IS NOT NULL AND Girkasse != 'Ikke oppgitt' ORDER BY Girkasse")
        girkasser = [r[0] for r in cur.fetchall()]
        return {"modeller": modeller, "typer": typer, "girkasser": girkasser}
    except Exception:
        return {"modeller": [], "typer": [], "girkasser": []}
    finally:
        conn.close()


def get_detaljer(page=1, per_page=50, filters=None):
    """View 5: Detaljert oversikt med beregninger."""
    conn = get_db()
    if not conn:
        return [], 0
    try:
        cur = conn.cursor(dictionary=True)

        # Bygg WHERE-klausul basert på filtre
        where_parts = []
        params = []
        if filters:
            if filters.get("modell_fra"):
                where_parts.append("b.Modell >= %s")
                params.append(filters["modell_fra"])
            if filters.get("modell_til"):
                where_parts.append("b.Modell <= %s")
                params.append(filters["modell_til"])
            if filters.get("pris_fra"):
                where_parts.append("CAST(REGEXP_REPLACE(b.Pris, '[^0-9]', '') AS UNSIGNED) >= %s")
                params.append(int(filters["pris_fra"]))
            if filters.get("pris_til"):
                where_parts.append("CAST(REGEXP_REPLACE(b.Pris, '[^0-9]', '') AS UNSIGNED) <= %s")
                params.append(int(filters["pris_til"]))
            if filters.get("type"):
                where_parts.append("b.Typebobil = %s")
                params.append(filters["type"])
            if filters.get("girkasse"):
                where_parts.append("b.Girkasse = %s")
                params.append(filters["girkasse"])
            if filters.get("skjul_solgt"):
                where_parts.append("(b.Solgt = 0 OR b.Solgt IS NULL)")

        where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""

        # Totalt antall med filter
        cur.execute(f"SELECT COUNT(*) AS total FROM bobil b {where_clause}", params)
        total = cur.fetchone()["total"]

        offset = (page - 1) * per_page
        cur.execute(f"""
            SELECT b.Finnkode, b.Annonsenavn, b.Beskrivelse, b.Modell,
                   b.Kilometerstand, b.Girkasse, b.Nyttelast, b.Typebobil,
                   b.Oppdatert, b.Pris, b.URL, b.ImageURL, b.Lokasjon, b.Solgt,
                   COUNT(p.Pris) AS AntallEndringer,
                   MIN(NULLIF(CAST(REGEXP_REPLACE(p.Pris, '[^0-9]', '') AS UNSIGNED), 0)) AS LavestePris,
                   MAX(NULLIF(CAST(REGEXP_REPLACE(p.Pris, '[^0-9]', '') AS UNSIGNED), 0)) AS HoyestePris
            FROM bobil b
            LEFT JOIN prisendringer p ON b.Finnkode = p.Finnkode
            {where_clause}
            GROUP BY b.Finnkode, b.Annonsenavn, b.Beskrivelse, b.Modell,
                     b.Kilometerstand, b.Girkasse, b.Nyttelast, b.Typebobil,
                     b.Oppdatert, b.Pris, b.URL, b.ImageURL, b.Lokasjon, b.Solgt
            ORDER BY b.Modell DESC, CAST(REGEXP_REPLACE(b.Pris, '[^0-9]', '') AS UNSIGNED) ASC
            LIMIT %s OFFSET %s
        """, params + [per_page, offset])
        rows = cur.fetchall()

        now = datetime.now()
        for r in rows:
            pris = parse_price(r["Pris"])
            km = parse_km(r["Kilometerstand"])
            laveste = parse_price(r["LavestePris"])
            hoyeste = parse_price(r["HoyestePris"])
            # Fallback til nåværende pris hvis ingen prishistorikk
            if not laveste and pris:
                laveste = pris
            if not hoyeste and pris:
                hoyeste = pris
            r["NaaverendePris"] = format_price(pris)
            r["LavestePrisF"] = format_price(laveste)
            r["HoyestePrisF"] = format_price(hoyeste)
            r["FinnURL"] = f"https://www.finn.no/mobility/item/{r['Finnkode']}"

            # Sjekk om annonsen er ny (siste 24 timer)
            dato = parse_norwegian_date(r.get("Oppdatert", ""))
            r["ErNy"] = dato and (now - dato).total_seconds() < 86400
            r["Alder"], r["AlderClass"], r["AlderSort"] = format_age(r.get("Oppdatert", ""))

            # Pris per km
            if pris and km and km > 0:
                r["PrisPerKm"] = round(pris / km, 1)
            else:
                r["PrisPerKm"] = None

            # Prutet
            if pris:
                r["Prutet12"] = format_price(round(pris * 0.88))
                r["Prutet13"] = format_price(round(pris * 0.87))
            else:
                r["Prutet12"] = "—"
                r["Prutet13"] = "—"

            # Kjøpsscore: (pris/km) * (år - modell)
            modell = r.get("Modell")
            if r["PrisPerKm"] and modell:
                try:
                    alder = now.year - int(modell)
                    r["KjopsScore"] = round(r["PrisPerKm"] * alder, 1)
                except (ValueError, TypeError):
                    r["KjopsScore"] = None
            else:
                r["KjopsScore"] = None

        return rows, total
    except Exception as e:
        logger.error("Feil i get_detaljer: %s", e)
        return [], 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Scraper-integrasjon
# ---------------------------------------------------------------------------

def run_scraper_background():
    """Kjør scraperen i bakgrunnen."""
    if scraper_status["running"]:
        logger.info("Scraper kjører allerede.")
        return
    scraper_status["running"] = True
    try:
        # Importer her for å unngå sirkulær import ved modulnivå
        sys.path.insert(0, "/usr/bin")
        from bobil_v2 import run_scraper
        run_scraper()
        scraper_status["last_run"] = datetime.now()
        scraper_status["error"] = None
        logger.info("Scraping fullført.")
    except Exception as e:
        scraper_status["error"] = str(e)
        logger.error("Scraper feilet: %s", e)
    finally:
        scraper_status["running"] = False


def schedule_scraper(interval_hours=6):
    """Start periodisk scraping i bakgrunnstråd."""
    def loop():
        while True:
            logger.info("Starter planlagt scraping...")
            run_scraper_background()
            threading.Event().wait(interval_hours * 3600)

    t = threading.Thread(target=loop, daemon=True, name="scraper-scheduler")
    t.start()
    logger.info("Scraper planlagt til å kjøre hver %d. time.", interval_hours)


# ---------------------------------------------------------------------------
# Flask-app
# ---------------------------------------------------------------------------

app = Flask(__name__)

# HTML-mal
TEMPLATE = """
<!DOCTYPE html>
<html lang="no">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Bobil — Finn.no</title>
    <style>
        :root {
            --primary-color: #4caf50;
            --bg-color: #1c1c1c;
            --card-bg: #2d2d2d;
            --text-color: #e0e0e0;
            --text-muted: #9e9e9e;
            --border-color: #404040;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: var(--bg-color);
            color: var(--text-color);
            padding: 20px;
            line-height: 1.6;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 {
            color: var(--primary-color);
            margin-bottom: 15px;
            font-size: 1.5em;
        }
        .tabs {
            display: flex;
            gap: 4px;
            margin-bottom: 20px;
            flex-wrap: wrap;
        }
        .tab {
            padding: 8px 16px;
            background: var(--card-bg);
            color: var(--text-muted);
            text-decoration: none;
            border-radius: 8px 8px 0 0;
            border: 1px solid var(--border-color);
            border-bottom: none;
            font-size: 0.9em;
        }
        .tab:hover { color: var(--text-color); }
        .tab.active {
            background: var(--primary-color);
            color: #000;
            font-weight: 600;
            border-color: var(--primary-color);
        }
        .content {
            background: var(--card-bg);
            border-radius: 0 12px 12px 12px;
            padding: 20px;
            border: 1px solid var(--border-color);
            overflow-x: auto;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.85em;
        }
        th {
            background: rgba(0,0,0,0.3);
            color: var(--primary-color);
            padding: 10px 8px;
            text-align: left;
            position: sticky;
            top: 0;
            white-space: nowrap;
        }
        td {
            padding: 8px;
            border-bottom: 1px solid var(--border-color);
            vertical-align: top;
        }
        tr:hover { background: rgba(255,255,255,0.03); }
        a { color: var(--primary-color); text-decoration: none; }
        a:hover { text-decoration: underline; }
        .price-down { color: #4caf50; }
        .price-up { color: #f44336; }
        .score { font-weight: bold; color: var(--primary-color); }
        .keyword-tag {
            display: inline-block;
            background: rgba(76,175,80,0.2);
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 0.8em;
            margin: 1px;
        }
        .status-bar {
            margin-top: 20px;
            padding: 12px 16px;
            background: var(--card-bg);
            border-radius: 8px;
            border: 1px solid var(--border-color);
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 10px;
            font-size: 0.85em;
            color: var(--text-muted);
        }
        .btn {
            background: var(--primary-color);
            color: #000;
            border: none;
            padding: 6px 14px;
            border-radius: 6px;
            cursor: pointer;
            font-weight: 500;
            font-size: 0.85em;
        }
        .btn:hover { opacity: 0.9; }
        .btn:disabled { opacity: 0.5; cursor: not-allowed; }
        .search-form {
            display: flex;
            gap: 10px;
            margin-bottom: 15px;
        }
        .search-form input {
            flex: 1;
            padding: 8px 12px;
            border-radius: 6px;
            border: 1px solid var(--border-color);
            background: var(--bg-color);
            color: var(--text-color);
            font-size: 0.9em;
        }
        .pagination {
            display: flex;
            gap: 8px;
            margin-top: 15px;
            justify-content: center;
        }
        .pagination a, .pagination span {
            padding: 6px 12px;
            border-radius: 4px;
            border: 1px solid var(--border-color);
            color: var(--text-muted);
            text-decoration: none;
            font-size: 0.85em;
        }
        .pagination a:hover { background: rgba(255,255,255,0.05); }
        .pagination .current {
            background: var(--primary-color);
            color: #000;
            border-color: var(--primary-color);
            font-weight: 600;
        }
        .no-data {
            color: var(--text-muted);
            font-style: italic;
            text-align: center;
            padding: 40px;
        }
        .truncate {
            max-width: 300px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        th.sortable {
            cursor: pointer;
            user-select: none;
        }
        th.sortable:hover {
            color: var(--text-color);
        }
        th.sortable::after {
            content: ' ⇅';
            font-size: 0.7em;
            opacity: 0.4;
        }
        th.sort-asc::after {
            content: ' ▲';
            opacity: 0.8;
        }
        th.sort-desc::after {
            content: ' ▼';
            opacity: 0.8;
        }
        .new-badge {
            display: inline-block;
            background: #ff9800;
            color: #000;
            padding: 1px 6px;
            border-radius: 3px;
            font-size: 0.7em;
            font-weight: 600;
            margin-left: 4px;
            vertical-align: middle;
        }
        /* Filterpanel */
        .filter-panel {
            display: flex;
            gap: 12px;
            margin-bottom: 15px;
            flex-wrap: wrap;
            align-items: flex-end;
        }
        .filter-group {
            display: flex;
            flex-direction: column;
            gap: 4px;
        }
        .filter-group label {
            font-size: 0.75em;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        .filter-group select,
        .filter-group input[type="number"] {
            padding: 6px 10px;
            border-radius: 6px;
            border: 1px solid var(--border-color);
            background: var(--bg-color);
            color: var(--text-color);
            font-size: 0.85em;
            min-width: 120px;
        }
        /* Mobilvisning */
        @media (max-width: 768px) {
            body { padding: 10px; }
            .tabs { gap: 2px; }
            .tab { padding: 6px 10px; font-size: 0.8em; }
            .content { padding: 12px; }
            table { font-size: 0.75em; }
            th, td { padding: 6px 4px; }
            .filter-panel { flex-direction: column; }
            .filter-group { width: 100%; }
            .filter-group select,
            .filter-group input[type="number"] { width: 100%; }
            .status-bar { flex-direction: column; text-align: center; }
            /* Card layout for mobilvisning */
            .mobile-cards table { display: none; }
            .mobile-cards .card-list { display: block; }
        }
        @media (min-width: 769px) {
            .mobile-cards .card-list { display: none; }
        }
        .card-list {
            display: none;
        }
        .card {
            background: var(--bg-color);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 12px;
            margin-bottom: 10px;
        }
        .card-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 8px;
        }
        .card-header a {
            font-weight: 600;
            font-size: 0.95em;
        }
        .card-details {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 4px 12px;
            font-size: 0.8em;
        }
        .card-detail-label {
            color: var(--text-muted);
        }
        tr.sold {
            opacity: 0.5;
        }
        tr.sold td:first-child::before {
            content: '';
        }
        .thumb {
            width: 80px;
            height: 60px;
            object-fit: cover;
            border-radius: 4px;
            vertical-align: middle;
        }
        .detail-img {
            max-width: 480px;
            width: 100%;
            border-radius: 8px;
            margin-bottom: 20px;
        }
        .age-fresh { color: #4caf50; }
        .age-weeks { color: #ff9800; }
        .age-old { color: #f44336; }
        .age-unknown { color: var(--text-muted); }
        .sold-badge {
            display: inline-block;
            background: #f44336;
            color: #fff;
            padding: 1px 6px;
            border-radius: 3px;
            font-size: 0.7em;
            font-weight: 600;
            margin-left: 4px;
            vertical-align: middle;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Bobil — Finn.no Oversikt</h1>
        <nav class="tabs">
            <a href="{{ bp }}prisendringer" class="tab {{ 'active' if active_tab == 'prisendringer' }}">Prisendringer</a>
            <a href="{{ bp }}kjopsscore" class="tab {{ 'active' if active_tab == 'kjopsscore' }}">Kjøpsscore</a>
            <a href="{{ bp }}prisutvikling" class="tab {{ 'active' if active_tab == 'prisutvikling' }}">Prisutvikling</a>
            <a href="{{ bp }}sok" class="tab {{ 'active' if active_tab == 'sok' }}">Nøkkelord-søk</a>
            <a href="{{ bp }}detaljer" class="tab {{ 'active' if active_tab == 'detaljer' }}">Detaljert</a>
        </nav>

        <div class="content">
            {{ content|safe }}
        </div>

        <div class="status-bar">
            <span>
                {{ total_listings }} annonser i databasen
                {% if last_scrape %} | Sist oppdatert: {{ last_scrape }}{% endif %}
                {% if scraper_running %} | Scraping pågår...{% endif %}
            </span>
            <form method="POST" action="{{ bp }}scrape" style="display:inline">
                <button type="submit" class="btn" {{ 'disabled' if scraper_running }}>Oppdater nå</button>
            </form>
        </div>
    </div>
    <script>
        // Sortering av tabeller
        document.querySelectorAll('th.sortable').forEach(th => {
            th.addEventListener('click', () => {
                const table = th.closest('table');
                const tbody = table.querySelector('tbody');
                const idx = Array.from(th.parentNode.children).indexOf(th);
                const type = th.dataset.sort || 'string';
                const rows = Array.from(tbody.querySelectorAll('tr'));

                // Toggle sorteringsretning
                const isAsc = th.classList.contains('sort-asc');
                table.querySelectorAll('th').forEach(h => h.classList.remove('sort-asc', 'sort-desc'));
                th.classList.add(isAsc ? 'sort-desc' : 'sort-asc');
                const dir = isAsc ? -1 : 1;

                rows.sort((a, b) => {
                    const aCell = a.children[idx];
                    const bCell = b.children[idx];
                    let aVal = aCell?.textContent.trim() || '';
                    let bVal = bCell?.textContent.trim() || '';
                    if (type === 'number') {
                        const aNum = parseFloat(aCell?.dataset.sortValue ?? aVal.replace(/[^\d.-]/g, '')) || 0;
                        const bNum = parseFloat(bCell?.dataset.sortValue ?? bVal.replace(/[^\d.-]/g, '')) || 0;
                        return (aNum - bNum) * dir;
                    }
                    return aVal.localeCompare(bVal, 'no') * dir;
                });
                rows.forEach(row => tbody.appendChild(row));
            });
        });
    </script>
</body>
</html>
"""


def render_page(active_tab, content_html, base_path=""):
    """Render en side med felles layout."""
    last_scrape = None
    if scraper_status["last_run"]:
        last_scrape = scraper_status["last_run"].strftime("%d.%m.%Y %H:%M")
    return render_template_string(
        TEMPLATE,
        active_tab=active_tab,
        content=content_html,
        bp=base_path,
        total_listings=get_total_count(),
        last_scrape=last_scrape,
        scraper_running=scraper_status["running"],
    )


@app.route("/")
def index():
    return redirect("prisendringer")


@app.route("/prisendringer")
def view_prisendringer():
    rows = get_prisendringer()
    if not rows:
        return render_page("prisendringer", '<p class="no-data">Ingen prisendringer funnet.</p>')

    html = """
    <table>
        <thead>
            <tr>
                <th class="sortable" data-sort="number">Finnkode</th>
                <th class="sortable">Annonse</th>
                <th class="sortable" data-sort="number">Modell</th>
                <th class="sortable" data-sort="number">Pris</th>
                <th class="sortable" data-sort="number">Laveste</th>
                <th class="sortable" data-sort="number">Høyeste</th>
                <th class="sortable" data-sort="number">Endringer</th>
                <th class="sortable" data-sort="number">Sist sett</th>
            </tr>
        </thead>
        <tbody>
    """
    for r in rows:
        html += f"""
            <tr>
                <td><a href="annonse/{r['Finnkode']}">{r['Finnkode']}</a></td>
                <td class="truncate">{r['Annonsenavn'] or ''}</td>
                <td>{r['Modell'] or ''}</td>
                <td>{r['NaaverendePris']}</td>
                <td class="price-down">{r['LavestePrisF']}</td>
                <td class="price-up">{r['HoyestePrisF']}</td>
                <td><strong>{r['AntallEndringer']}</strong></td>
                <td class="{r['AlderClass']}" data-sort-value="{r['AlderSort']}">{r['Alder']}</td>
            </tr>
        """
    html += "</tbody></table>"
    return render_page("prisendringer", html)


@app.route("/kjopsscore")
def view_kjopsscore():
    rows = get_kjopsscore()
    if not rows:
        return render_page("kjopsscore", '<p class="no-data">Ingen aktive annonser med score.</p>')

    html = """
    <table>
        <thead>
            <tr>
                <th class="sortable" data-sort="number">Score</th>
                <th class="sortable" data-sort="number">Finnkode</th>
                <th class="sortable">Annonse</th>
                <th class="sortable" data-sort="number">Modell</th>
                <th class="sortable" data-sort="number">Pris</th>
                <th class="sortable" data-sort="number">Laveste</th>
                <th class="sortable" data-sort="number">Høyeste</th>
                <th class="sortable" data-sort="number">Endringer</th>
                <th class="sortable" data-sort="number">Dager</th>
                <th>Søketreff</th>
            </tr>
        </thead>
        <tbody>
    """
    for r in rows:
        treff_html = ""
        if r["Soketreff"]:
            for t in r["Soketreff"].split(", "):
                treff_html += f'<span class="keyword-tag">{t}</span>'
        ny_badge = '<span class="new-badge">NY</span>' if r.get("ErNy") else ""
        html += f"""
            <tr>
                <td class="score">{r['KjopsScore']}</td>
                <td><a href="annonse/{r['Finnkode']}">{r['Finnkode']}</a></td>
                <td class="truncate">{r['Annonsenavn'] or ''}{ny_badge}</td>
                <td>{r['Modell'] or ''}</td>
                <td>{r['NaaverendePris']}</td>
                <td class="price-down">{r['LavestePris']}</td>
                <td class="price-up">{r['HoyestePris']}</td>
                <td>{r['AntallEndringer']}</td>
                <td>{r['DagerPaaMarkedet']}</td>
                <td>{treff_html}</td>
            </tr>
        """
    html += "</tbody></table>"
    return render_page("kjopsscore", html)


@app.route("/prisutvikling")
def view_prisutvikling():
    rows = get_prisutvikling()
    if not rows:
        return render_page("prisutvikling", '<p class="no-data">Ingen prisdata funnet.</p>')

    html = """
    <table>
        <thead>
            <tr>
                <th class="sortable" data-sort="number">Modellår</th>
                <th class="sortable">Periode</th>
                <th class="sortable" data-sort="number">Gj.snittspris</th>
                <th class="sortable" data-sort="number">Datapunkter</th>
            </tr>
        </thead>
        <tbody>
    """
    prev_modell = None
    for r in rows:
        modell_display = r["Modell"] if r["Modell"] != prev_modell else ""
        style = ' style="border-top: 2px solid var(--border-color)"' if modell_display else ""
        html += f"""
            <tr{style}>
                <td><strong>{modell_display}</strong></td>
                <td>{r['Periode']}</td>
                <td>{r['GjSnittPrisF']}</td>
                <td>{r['Antall']}</td>
            </tr>
        """
        prev_modell = r["Modell"]
    html += "</tbody></table>"
    return render_page("prisutvikling", html)


@app.route("/sok")
def view_sok():
    keywords = request.args.get("q", "")
    rows = get_sokresultater(keywords) if keywords else []

    html = f"""
    <form class="search-form" method="GET" action="sok">
        <input type="text" name="q" value="{keywords}"
               placeholder="Søk etter nøkkelord (kommaseparert, f.eks: køye, familie, vendbare seter)">
        <button type="submit" class="btn">Søk</button>
    </form>
    """

    if keywords and not rows:
        html += '<p class="no-data">Ingen treff.</p>'
    elif rows:
        html += """
        <table>
            <thead>
                <tr>
                    <th class="sortable" data-sort="number">Finnkode</th>
                    <th class="sortable">Annonse</th>
                    <th class="sortable" data-sort="number">Modell</th>
                    <th class="sortable" data-sort="number">Pris</th>
                    <th class="sortable" data-sort="number">Km</th>
                    <th class="sortable">Type</th>
                    <th class="sortable" data-sort="number">Endringer</th>
                    <th class="sortable" data-sort="number">Laveste</th>
                    <th class="sortable" data-sort="number">Høyeste</th>
                    <th class="sortable" data-sort="number">Sist sett</th>
                    <th>Treff</th>
                </tr>
            </thead>
            <tbody>
        """
        for r in rows:
            treff_html = ""
            if r.get("Soketreff"):
                for t in r["Soketreff"].split(", "):
                    treff_html += f'<span class="keyword-tag">{t}</span>'
            html += f"""
                <tr>
                    <td><a href="annonse/{r['Finnkode']}">{r['Finnkode']}</a></td>
                    <td class="truncate">{r['Annonsenavn'] or ''}</td>
                    <td>{r['Modell'] or ''}</td>
                    <td>{r['NaaverendePris']}</td>
                    <td>{r.get('Kilometerstand', '')}</td>
                    <td>{r.get('Typebobil', '')}</td>
                    <td>{r['AntallEndringer']}</td>
                    <td class="price-down">{r['LavestePrisF']}</td>
                    <td class="price-up">{r['HoyestePrisF']}</td>
                    <td class="{r['AlderClass']}" data-sort-value="{r['AlderSort']}">{r['Alder']}</td>
                    <td>{treff_html}</td>
                </tr>
            """
        html += "</tbody></table>"

    return render_page("sok", html)


@app.route("/detaljer")
def view_detaljer():
    page = request.args.get("page", 1, type=int)
    per_page = 50
    filters = {
        "modell_fra": request.args.get("modell_fra", ""),
        "modell_til": request.args.get("modell_til", ""),
        "pris_fra": request.args.get("pris_fra", ""),
        "pris_til": request.args.get("pris_til", ""),
        "type": request.args.get("type", ""),
        "girkasse": request.args.get("girkasse", ""),
        "skjul_solgt": request.args.get("skjul_solgt", ""),
    }
    rows, total = get_detaljer(page, per_page, filters)

    if not rows and not any(filters.values()):
        return render_page("detaljer", '<p class="no-data">Ingen annonser funnet.</p>')

    # Hent filteralternativer
    filter_opts = get_filter_options()

    # Bygg filter-URL uten page-param
    def filter_qs():
        parts = []
        for k, v in filters.items():
            if v:
                parts.append(f"{k}={v}")
        return "&".join(parts)

    # Filterpanel
    type_options = "".join(
        f'<option value="{t}" {"selected" if filters.get("type") == t else ""}>{t}</option>'
        for t in filter_opts["typer"]
    )
    gir_options = "".join(
        f'<option value="{g}" {"selected" if filters.get("girkasse") == g else ""}>{g}</option>'
        for g in filter_opts["girkasser"]
    )
    skjul_checked = "checked" if filters.get("skjul_solgt") else ""

    html = f"""
    <form class="filter-panel" method="GET" action="detaljer">
        <div class="filter-group">
            <label>Modellår fra</label>
            <input type="number" name="modell_fra" value="{filters.get('modell_fra', '')}" placeholder="f.eks. 2010" min="1990" max="2030">
        </div>
        <div class="filter-group">
            <label>Modellår til</label>
            <input type="number" name="modell_til" value="{filters.get('modell_til', '')}" placeholder="f.eks. 2020" min="1990" max="2030">
        </div>
        <div class="filter-group">
            <label>Pris fra</label>
            <input type="number" name="pris_fra" value="{filters.get('pris_fra', '')}" placeholder="f.eks. 300000" step="50000">
        </div>
        <div class="filter-group">
            <label>Pris til</label>
            <input type="number" name="pris_til" value="{filters.get('pris_til', '')}" placeholder="f.eks. 700000" step="50000">
        </div>
        <div class="filter-group">
            <label>Type bobil</label>
            <select name="type">
                <option value="">Alle</option>
                {type_options}
            </select>
        </div>
        <div class="filter-group">
            <label>Girkasse</label>
            <select name="girkasse">
                <option value="">Alle</option>
                {gir_options}
            </select>
        </div>
        <div class="filter-group">
            <label>&nbsp;</label>
            <label style="font-size: 0.85em; text-transform: none; letter-spacing: normal; cursor: pointer;">
                <input type="checkbox" name="skjul_solgt" value="1" {skjul_checked}> Skjul solgt
            </label>
        </div>
        <div class="filter-group">
            <label>&nbsp;</label>
            <button type="submit" class="btn">Filtrer</button>
        </div>
    </form>
    """

    if not rows:
        html += '<p class="no-data">Ingen annonser matcher filtrene.</p>'
        return render_page("detaljer", html)

    html += """
    <table>
        <thead>
            <tr>
                <th></th>
                <th class="sortable">Annonse</th>
                <th class="sortable" data-sort="number">Modell</th>
                <th class="sortable" data-sort="number">Km</th>
                <th class="sortable" data-sort="number">Pris</th>
                <th class="sortable" data-sort="number">Laveste</th>
                <th class="sortable" data-sort="number">Høyeste</th>
                <th class="sortable" data-sort="number">Pris/km</th>
                <th class="sortable">Lokasjon</th>
                <th class="sortable" data-sort="number">Sist sett</th>
            </tr>
        </thead>
        <tbody>
    """
    for r in rows:
        priskm_html = f"{r['PrisPerKm']}" if r["PrisPerKm"] is not None else "—"
        is_sold = bool(r.get("Solgt")) or "solgt" in str(r.get("Pris", "")).lower()
        row_class = ' class="sold"' if is_sold else ""
        sold_badge = '<span class="sold-badge">Solgt</span>' if is_sold else ""
        ny_badge = '<span class="new-badge">NY</span>' if r.get("ErNy") and not is_sold else ""
        img_url = r.get("ImageURL", "") or ""
        thumb_html = f'<img src="{img_url}" class="thumb" alt="">' if img_url else ""
        lokasjon = r.get("Lokasjon", "") or ""
        html += f"""
            <tr{row_class}>
                <td>{thumb_html}</td>
                <td class="truncate"><a href="annonse/{r['Finnkode']}">{r['Annonsenavn'] or r['Finnkode']}</a>{sold_badge}{ny_badge}</td>
                <td>{r['Modell'] or ''}</td>
                <td>{r.get('Kilometerstand', '')}</td>
                <td>{r['NaaverendePris']}</td>
                <td class="price-down">{r['LavestePrisF']}</td>
                <td class="price-up">{r['HoyestePrisF']}</td>
                <td>{priskm_html}</td>
                <td>{lokasjon}</td>
                <td class="{r['AlderClass']}" data-sort-value="{r['AlderSort']}">{r['Alder']}</td>
            </tr>
        """
    html += "</tbody></table>"

    # Paginering med filter-params bevart
    total_pages = (total + per_page - 1) // per_page
    fqs = filter_qs()
    fqs_amp = f"&{fqs}" if fqs else ""
    if total_pages > 1:
        html += '<div class="pagination">'
        if page > 1:
            html += f'<a href="detaljer?page={page - 1}{fqs_amp}">Forrige</a>'
        for p in range(1, total_pages + 1):
            if p == page:
                html += f'<span class="current">{p}</span>'
            elif abs(p - page) <= 3 or p == 1 or p == total_pages:
                html += f'<a href="detaljer?page={p}{fqs_amp}">{p}</a>'
            elif abs(p - page) == 4:
                html += '<span>...</span>'
        if page < total_pages:
            html += f'<a href="detaljer?page={page + 1}{fqs_amp}">Neste</a>'
        html += '</div>'

    return render_page("detaljer", html)


@app.route("/annonse/<int:finnkode>")
def view_annonse(finnkode):
    """Detaljside for en enkelt annonse med prishistorikk-graf."""
    bp = "../"
    conn = get_db()
    if not conn:
        return render_page("detaljer", '<p class="no-data">Ingen databasetilkobling.</p>', base_path=bp)
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM bobil WHERE Finnkode = %s", (finnkode,))
        ad = cur.fetchone()
        if not ad:
            return render_page("detaljer", '<p class="no-data">Annonse ikke funnet.</p>', base_path=bp)

        # Hent prishistorikk
        cur.execute(
            "SELECT Tidspunkt, Pris FROM prisendringer WHERE Finnkode = %s ORDER BY Tidspunkt ASC",
            (finnkode,)
        )
        prishistorikk = cur.fetchall()

        pris = parse_price(ad["Pris"])
        km = parse_km(ad.get("Kilometerstand"))
        alder_txt, alder_cls, _ = format_age(ad.get("Oppdatert", ""))
        finn_url = f"https://www.finn.no/mobility/item/{finnkode}"

        # Bygg Chart.js data
        chart_labels = []
        chart_data = []
        for p in prishistorikk:
            ts = p["Tidspunkt"]
            if isinstance(ts, datetime):
                chart_labels.append(ts.strftime("%d.%m.%Y"))
            else:
                chart_labels.append(str(ts))
            pris_val = parse_price(p["Pris"])
            chart_data.append(pris_val if pris_val else 0)

        image_url = ad.get("ImageURL", "") or ""
        lokasjon = ad.get("Lokasjon", "") or ""
        img_html = f'<img src="{image_url}" class="detail-img" alt="">' if image_url else ""

        html = f"""
        <div style="margin-bottom: 15px;">
            <a href="../detaljer" style="font-size: 0.85em;">&larr; Tilbake</a>
        </div>
        <h2 style="margin-bottom: 10px; color: var(--text-color);">
            <a href="{finn_url}" target="_blank">{ad.get('Annonsenavn', finnkode)}</a>
        </h2>
        {img_html}
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px 24px; margin-bottom: 20px; font-size: 0.9em;">
            <div><span style="color: var(--text-muted);">Finnkode:</span> <a href="{finn_url}" target="_blank">{finnkode}</a></div>
            <div><span style="color: var(--text-muted);">Modell:</span> {ad.get('Modell', '—')}</div>
            <div><span style="color: var(--text-muted);">Pris:</span> {format_price(pris)}</div>
            <div><span style="color: var(--text-muted);">Km:</span> {ad.get('Kilometerstand', '—')}</div>
            <div><span style="color: var(--text-muted);">Type:</span> {ad.get('Typebobil', '—')}</div>
            <div><span style="color: var(--text-muted);">Girkasse:</span> {ad.get('Girkasse', '—')}</div>
            <div><span style="color: var(--text-muted);">Nyttelast:</span> {ad.get('Nyttelast', '—')}</div>
            <div><span style="color: var(--text-muted);">Lokasjon:</span> {lokasjon or '—'}</div>
            <div><span style="color: var(--text-muted);">Sist sett:</span> <span class="{alder_cls}">{alder_txt}</span></div>
        </div>
        <div style="color: var(--text-muted); font-size: 0.85em; margin-bottom: 20px;">
            {ad.get('Beskrivelse', '')}
        </div>
        """

        if chart_data and len(chart_data) > 1:
            import json as json_mod
            html += f"""
            <h3 style="color: var(--primary-color); margin-bottom: 10px;">Prishistorikk</h3>
            <div style="max-width: 700px; margin-bottom: 20px;">
                <canvas id="prisChart"></canvas>
            </div>
            <script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
            <script>
                new Chart(document.getElementById('prisChart'), {{
                    type: 'line',
                    data: {{
                        labels: {json_mod.dumps(chart_labels)},
                        datasets: [{{
                            label: 'Pris (kr)',
                            data: {json_mod.dumps(chart_data)},
                            borderColor: '#4caf50',
                            backgroundColor: 'rgba(76,175,80,0.1)',
                            fill: true,
                            tension: 0.3,
                            pointRadius: 4,
                            pointBackgroundColor: '#4caf50'
                        }}]
                    }},
                    options: {{
                        responsive: true,
                        plugins: {{
                            legend: {{ display: false }},
                            tooltip: {{
                                callbacks: {{
                                    label: ctx => ctx.parsed.y.toLocaleString('no-NO') + ' kr'
                                }}
                            }}
                        }},
                        scales: {{
                            y: {{
                                ticks: {{
                                    callback: v => (v/1000) + 'k',
                                    color: '#9e9e9e'
                                }},
                                grid: {{ color: 'rgba(255,255,255,0.05)' }}
                            }},
                            x: {{
                                ticks: {{ color: '#9e9e9e', maxRotation: 45 }},
                                grid: {{ color: 'rgba(255,255,255,0.05)' }}
                            }}
                        }}
                    }}
                }});
            </script>
            """
        elif prishistorikk:
            html += '<p style="color: var(--text-muted);">Kun ett datapunkt i prishistorikken.</p>'
        else:
            html += '<p style="color: var(--text-muted);">Ingen prishistorikk registrert.</p>'

        # Prisendringer-tabell
        if prishistorikk:
            html += """
            <h3 style="color: var(--primary-color); margin: 20px 0 10px;">Prisendringer</h3>
            <table style="max-width: 500px;">
                <thead><tr><th>Tidspunkt</th><th>Pris</th></tr></thead>
                <tbody>
            """
            for p in reversed(prishistorikk):
                ts = p["Tidspunkt"]
                if isinstance(ts, datetime):
                    ts_str = ts.strftime("%d.%m.%Y %H:%M")
                else:
                    ts_str = str(ts)
                pval = parse_price(p["Pris"])
                html += f"<tr><td>{ts_str}</td><td>{format_price(pval) if pval else p['Pris']}</td></tr>"
            html += "</tbody></table>"

        return render_page("detaljer", html, base_path=bp)
    except Exception as e:
        logger.error("Feil i view_annonse: %s", e)
        return render_page("detaljer", '<p class="no-data">Feil ved henting av annonse.</p>', base_path=bp)
    finally:
        conn.close()


@app.route("/scrape", methods=["POST"])
def trigger_scrape():
    if not scraper_status["running"]:
        t = threading.Thread(target=run_scraper_background, daemon=True)
        t.start()
    return redirect(request.referrer or "prisendringer")


@app.route("/api/status")
def api_status():
    return jsonify({
        "last_run": scraper_status["last_run"].isoformat() if scraper_status["last_run"] else None,
        "running": scraper_status["running"],
        "error": scraper_status["error"],
        "total_listings": get_total_count(),
    })


# ---------------------------------------------------------------------------
# Oppstart
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logger.info("Starter Bobil web UI på port 8100...")

    # Sørg for at nye kolonner finnes
    ensure_db_columns()

    # Start planlagt scraping i bakgrunnen
    scrape_interval = options.get("scrape_interval", 6)
    schedule_scraper(interval_hours=scrape_interval)

    # Start webserveren
    serve(app, host="0.0.0.0", port=8100)
