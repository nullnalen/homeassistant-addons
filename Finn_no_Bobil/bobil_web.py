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
    if price_int is None:
        return "—"
    return f"{price_int:,.0f} kr".replace(",", " ")


def get_db():
    """Opprett en ny DB-tilkobling per request."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=10)
        return conn
    except Exception as e:
        logger.error("DB-tilkoblingsfeil: %s", e)
        return None


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
            SELECT b.Finnkode, b.Annonsenavn, b.Modell, b.Pris,
                   COUNT(p.Pris) AS AntallEndringer,
                   MIN(p.Pris) AS LavestePris,
                   MAX(p.Pris) AS HoyestePris,
                   b.URL
            FROM bobil b
            JOIN prisendringer p ON b.Finnkode = p.Finnkode
            GROUP BY b.Finnkode, b.Annonsenavn, b.Modell, b.Pris, b.URL
            ORDER BY AntallEndringer DESC
        """)
        rows = cur.fetchall()
        for r in rows:
            r["NaaverendePris"] = format_price(parse_price(r["Pris"]))
            r["LavestePrisF"] = format_price(parse_price(r["LavestePris"]))
            r["HoyestePrisF"] = format_price(parse_price(r["HoyestePris"]))
            r["FinnURL"] = f"https://www.finn.no/mobility/item/{r['Finnkode']}"
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
                   b.Oppdatert, b.Beskrivelse, b.Annonsenavn AS Tittel,
                   COUNT(p.Pris) AS AntallEndringer,
                   MIN(p.Pris) AS LavestePris,
                   MAX(p.Pris) AS HoyestePris
            FROM bobil b
            LEFT JOIN prisendringer p ON b.Finnkode = p.Finnkode
            WHERE b.Pris NOT LIKE '%%Solgt%%'
            GROUP BY b.Finnkode, b.Annonsenavn, b.Modell, b.Pris,
                     b.Kilometerstand, b.Oppdatert, b.Beskrivelse
        """)
        rows = cur.fetchall()

        results = []
        now = datetime.now()
        keywords = ["køye", "familie", "vendbare seter", "kapteinstoler"]

        for r in rows:
            pris = parse_price(r["Pris"])
            hoyeste = parse_price(r["HoyestePris"])
            dato = parse_norwegian_date(r["Oppdatert"])

            if not pris or not dato:
                continue

            dager = (now - dato).days
            if dager > 60:
                continue

            # Kjøpsscore: prisfall% * dager + 5 * antall endringer
            prisfall_pct = 0
            if hoyeste and hoyeste > 0 and hoyeste > pris:
                prisfall_pct = ((hoyeste - pris) / hoyeste) * 100

            score = round(prisfall_pct * (dager + 1) + r["AntallEndringer"] * 5)

            # Søketreff
            tekst = f"{r['Annonsenavn']} {r.get('Beskrivelse', '')}".lower()
            treff = [kw for kw in keywords if kw.lower() in tekst]

            results.append({
                "Finnkode": r["Finnkode"],
                "Annonsenavn": r["Annonsenavn"],
                "Modell": r["Modell"],
                "NaaverendePris": format_price(pris),
                "LavestePris": format_price(parse_price(r["LavestePris"])),
                "HoyestePris": format_price(hoyeste),
                "AntallEndringer": r["AntallEndringer"],
                "DagerPaaMarkedet": dager,
                "KjopsScore": score,
                "Soketreff": ", ".join(treff) if treff else "",
                "FinnURL": f"https://www.finn.no/mobility/item/{r['Finnkode']}",
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
        cur.execute("""
            SELECT b.Modell,
                   DATE_FORMAT(p.Tidspunkt, '%%Y-%%m') AS Periode,
                   ROUND(AVG(p.Pris)) AS GjSnittPris,
                   COUNT(*) AS Antall
            FROM prisendringer p
            JOIN bobil b ON p.Finnkode = b.Finnkode
            WHERE b.Modell IS NOT NULL
              AND p.Pris NOT LIKE '%%Solgt%%'
            GROUP BY b.Modell, DATE_FORMAT(p.Tidspunkt, '%%Y-%%m')
            ORDER BY b.Modell DESC, Periode
        """)
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
                   MIN(p.Pris) AS LavestePris,
                   MAX(p.Pris) AS HoyestePris
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
            r["NaaverendePris"] = format_price(parse_price(r["Pris"]))
            r["LavestePrisF"] = format_price(parse_price(r["LavestePris"]))
            r["HoyestePrisF"] = format_price(parse_price(r["HoyestePris"]))
            r["FinnURL"] = f"https://www.finn.no/mobility/item/{r['Finnkode']}"
            # Finn hvilke termer som ga treff
            tekst = f"{r['Annonsenavn']} {r.get('Beskrivelse', '')}".lower()
            r["Soketreff"] = ", ".join(t for t in terms if t.lower() in tekst)
        return rows
    except Exception as e:
        logger.error("Feil i get_sokresultater: %s", e)
        return []
    finally:
        conn.close()


def get_detaljer(page=1, per_page=50):
    """View 5: Detaljert oversikt med beregninger."""
    conn = get_db()
    if not conn:
        return [], 0
    try:
        cur = conn.cursor(dictionary=True)

        # Totalt antall
        cur.execute("SELECT COUNT(*) AS total FROM bobil")
        total = cur.fetchone()["total"]

        offset = (page - 1) * per_page
        cur.execute(f"""
            SELECT b.Finnkode, b.Annonsenavn, b.Beskrivelse, b.Modell,
                   b.Kilometerstand, b.Girkasse, b.Nyttelast, b.Typebobil,
                   b.Oppdatert, b.Pris, b.URL,
                   COUNT(p.Pris) AS AntallEndringer,
                   MIN(p.Pris) AS LavestePris,
                   MAX(p.Pris) AS HoyestePris
            FROM bobil b
            LEFT JOIN prisendringer p ON b.Finnkode = p.Finnkode
            GROUP BY b.Finnkode, b.Annonsenavn, b.Beskrivelse, b.Modell,
                     b.Kilometerstand, b.Girkasse, b.Nyttelast, b.Typebobil,
                     b.Oppdatert, b.Pris, b.URL
            ORDER BY b.Modell DESC, b.Pris ASC
            LIMIT %s OFFSET %s
        """, (per_page, offset))
        rows = cur.fetchall()

        now = datetime.now()
        for r in rows:
            pris = parse_price(r["Pris"])
            km = parse_km(r["Kilometerstand"])
            r["NaaverendePris"] = format_price(pris)
            r["LavestePrisF"] = format_price(parse_price(r["LavestePris"]))
            r["HoyestePrisF"] = format_price(parse_price(r["HoyestePris"]))
            r["FinnURL"] = f"https://www.finn.no/mobility/item/{r['Finnkode']}"

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
    </style>
</head>
<body>
    <div class="container">
        <h1>Bobil — Finn.no Oversikt</h1>
        <nav class="tabs">
            <a href="prisendringer" class="tab {{ 'active' if active_tab == 'prisendringer' }}">Prisendringer</a>
            <a href="kjopsscore" class="tab {{ 'active' if active_tab == 'kjopsscore' }}">Kjøpsscore</a>
            <a href="prisutvikling" class="tab {{ 'active' if active_tab == 'prisutvikling' }}">Prisutvikling</a>
            <a href="sok" class="tab {{ 'active' if active_tab == 'sok' }}">Nøkkelord-søk</a>
            <a href="detaljer" class="tab {{ 'active' if active_tab == 'detaljer' }}">Detaljert</a>
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
            <form method="POST" action="scrape" style="display:inline">
                <button type="submit" class="btn" {{ 'disabled' if scraper_running }}>Oppdater nå</button>
            </form>
        </div>
    </div>
</body>
</html>
"""


def render_page(active_tab, content_html):
    """Render en side med felles layout."""
    last_scrape = None
    if scraper_status["last_run"]:
        last_scrape = scraper_status["last_run"].strftime("%d.%m.%Y %H:%M")
    return render_template_string(
        TEMPLATE,
        active_tab=active_tab,
        content=content_html,
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
                <th>Finnkode</th>
                <th>Annonse</th>
                <th>Modell</th>
                <th>Pris</th>
                <th>Laveste</th>
                <th>Høyeste</th>
                <th>Endringer</th>
            </tr>
        </thead>
        <tbody>
    """
    for r in rows:
        html += f"""
            <tr>
                <td><a href="{r['FinnURL']}" target="_blank">{r['Finnkode']}</a></td>
                <td class="truncate">{r['Annonsenavn'] or ''}</td>
                <td>{r['Modell'] or ''}</td>
                <td>{r['NaaverendePris']}</td>
                <td class="price-down">{r['LavestePrisF']}</td>
                <td class="price-up">{r['HoyestePrisF']}</td>
                <td><strong>{r['AntallEndringer']}</strong></td>
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
                <th>Score</th>
                <th>Finnkode</th>
                <th>Annonse</th>
                <th>Modell</th>
                <th>Pris</th>
                <th>Laveste</th>
                <th>Høyeste</th>
                <th>Endringer</th>
                <th>Dager</th>
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
        html += f"""
            <tr>
                <td class="score">{r['KjopsScore']}</td>
                <td><a href="{r['FinnURL']}" target="_blank">{r['Finnkode']}</a></td>
                <td class="truncate">{r['Annonsenavn'] or ''}</td>
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
                <th>Modellår</th>
                <th>Periode</th>
                <th>Gj.snittspris</th>
                <th>Datapunkter</th>
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
                    <th>Finnkode</th>
                    <th>Annonse</th>
                    <th>Modell</th>
                    <th>Pris</th>
                    <th>Km</th>
                    <th>Type</th>
                    <th>Endringer</th>
                    <th>Laveste</th>
                    <th>Høyeste</th>
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
                    <td><a href="{r['FinnURL']}" target="_blank">{r['Finnkode']}</a></td>
                    <td class="truncate">{r['Annonsenavn'] or ''}</td>
                    <td>{r['Modell'] or ''}</td>
                    <td>{r['NaaverendePris']}</td>
                    <td>{r.get('Kilometerstand', '')}</td>
                    <td>{r.get('Typebobil', '')}</td>
                    <td>{r['AntallEndringer']}</td>
                    <td class="price-down">{r['LavestePrisF']}</td>
                    <td class="price-up">{r['HoyestePrisF']}</td>
                    <td>{treff_html}</td>
                </tr>
            """
        html += "</tbody></table>"

    return render_page("sok", html)


@app.route("/detaljer")
def view_detaljer():
    page = request.args.get("page", 1, type=int)
    per_page = 50
    rows, total = get_detaljer(page, per_page)

    if not rows:
        return render_page("detaljer", '<p class="no-data">Ingen annonser funnet.</p>')

    html = """
    <table>
        <thead>
            <tr>
                <th>Finnkode</th>
                <th>Annonse</th>
                <th>Modell</th>
                <th>Km</th>
                <th>Pris</th>
                <th>Laveste</th>
                <th>Høyeste</th>
                <th>Pris/km</th>
                <th>Prutet 12%</th>
                <th>Prutet 13%</th>
                <th>Score</th>
                <th>Type</th>
                <th>Girkasse</th>
            </tr>
        </thead>
        <tbody>
    """
    for r in rows:
        score_html = f"{r['KjopsScore']}" if r["KjopsScore"] is not None else "—"
        priskm_html = f"{r['PrisPerKm']}" if r["PrisPerKm"] is not None else "—"
        html += f"""
            <tr>
                <td><a href="{r['FinnURL']}" target="_blank">{r['Finnkode']}</a></td>
                <td class="truncate">{r['Annonsenavn'] or ''}</td>
                <td>{r['Modell'] or ''}</td>
                <td>{r.get('Kilometerstand', '')}</td>
                <td>{r['NaaverendePris']}</td>
                <td class="price-down">{r['LavestePrisF']}</td>
                <td class="price-up">{r['HoyestePrisF']}</td>
                <td>{priskm_html}</td>
                <td>{r['Prutet12']}</td>
                <td>{r['Prutet13']}</td>
                <td class="score">{score_html}</td>
                <td>{r.get('Typebobil', '')}</td>
                <td>{r.get('Girkasse', '')}</td>
            </tr>
        """
    html += "</tbody></table>"

    # Paginering
    total_pages = (total + per_page - 1) // per_page
    if total_pages > 1:
        html += '<div class="pagination">'
        if page > 1:
            html += f'<a href="detaljer?page={page - 1}">Forrige</a>'
        for p in range(1, total_pages + 1):
            if p == page:
                html += f'<span class="current">{p}</span>'
            elif abs(p - page) <= 3 or p == 1 or p == total_pages:
                html += f'<a href="detaljer?page={p}">{p}</a>'
            elif abs(p - page) == 4:
                html += '<span>...</span>'
        if page < total_pages:
            html += f'<a href="detaljer?page={page + 1}">Neste</a>'
        html += '</div>'

    return render_page("detaljer", html)


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

    # Start planlagt scraping i bakgrunnen
    schedule_scraper(interval_hours=6)

    # Start webserveren
    serve(app, host="0.0.0.0", port=8100)
