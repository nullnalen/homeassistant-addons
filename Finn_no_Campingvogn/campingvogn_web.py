#!/usr/bin/env python3
"""
Campingvogn — Ingress Web UI
Flask-basert webgrensesnitt for campingvognannonser fra Finn.no.
"""
import os
import sys
import json
import re
import logging
import threading
import traceback
from datetime import datetime, timedelta

import mysql.connector
from mysql.connector import pooling
from flask import Flask, request, redirect, jsonify
from flask import render_template_string
from markupsafe import escape
from waitress import serve


def esc(val):
    if val is None:
        return ""
    return str(escape(val))


logger = logging.getLogger("campingvogn_web")
logger.setLevel(logging.INFO)
logger.handlers.clear()
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

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
    "database": options.get("databasename", "finn_no"),
    "port": options.get("databaseport", 3306),
}

TABLE = "campingvogn_elbil"
PRISENDRINGER_TABLE = "campingvogn_elbil_prisendringer"
BRUKER_TABLE = "campingvogn_bruker_data"

# Referansevogn — Dethleffs 480 QLK 2022 (vår nåværende vogn)
REFERANSEVOGN = {
    "navn":        "Dethleffs 480 QLK (2022)",
    "pris":        250995,
    "egenvekt":    990,
    "totalvekt":   1500,
    "nyttelast":   510,
    "lengde":      716,
    "bredde":      213,
    "soveplasser": 6,
    "aarsmodell":  2022,
}

scraper_status = {"last_run": None, "running": False, "error": None}

MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "mai": 5, "may": 5,
    "jun": 6, "jul": 7, "aug": 8, "sep": 9, "okt": 10, "oct": 10,
    "nov": 11, "des": 12, "dec": 12,
}


def parse_norwegian_date(date_str):
    if not date_str or date_str == "Ukjent":
        return None
    try:
        s = date_str.strip()
        if re.match(r"\d{4}-\d{2}-\d{2}", s):
            s_clean = re.sub(r"[TZ]", " ", s).strip()[:16]
            return datetime.strptime(s_clean, "%Y-%m-%d %H:%M")
        sl = s.lower()
        for name, num in MONTH_MAP.items():
            if name in sl:
                sl = re.sub(rf"\b{name}\.?\b", f"{num:02d}", sl)
                break
        m = re.match(r"(\d{1,2})\.\s*(\d{2})\.?\s+(\d{4})\s+(\d{2}):(\d{2})", sl)
        if m:
            return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)),
                            int(m.group(4)), int(m.group(5)))
    except Exception:
        pass
    return None


def parse_price(price_val):
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


def format_price(price_int):
    if not price_int:
        return "—"
    return f"{price_int:,.0f} kr".replace(",", " ")


def format_age(date_val):
    if not date_val:
        return "Ukjent", "age-unknown", 99999
    if isinstance(date_val, datetime):
        dato = date_val
    elif isinstance(date_val, str):
        if re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", date_val):
            try:
                dato = datetime.strptime(date_val, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return "Ukjent", "age-unknown", 99999
        else:
            dato = parse_norwegian_date(date_val)
    else:
        return "Ukjent", "age-unknown", 99999
    if not dato:
        return "Ukjent", "age-unknown", 99999
    delta = datetime.now() - dato
    dager = delta.days
    if dager == 0:
        timer = delta.seconds // 3600
        return (f"{timer}t siden", "age-fresh", 0) if timer else ("Nå", "age-fresh", 0)
    if dager == 1:
        return "I går", "age-fresh", 1
    if dager < 7:
        return f"{dager} dager", "age-fresh", dager
    if dager < 30:
        return f"{dager} dager", "age-weeks", dager
    if dager < 365:
        return f"{dager // 30} mnd", "age-old", dager
    return f"{dager // 365} år", "age-old", dager


def _forventet_pruting_pct(selgertype: str, dager: int) -> float:
    if selgertype == "Forhandler":
        if dager < 14:   return 6.0
        if dager < 30:   return 7.0
        if dager < 60:   return 8.0
        if dager < 90:   return 10.0
        return 13.0
    else:
        if dager < 14:   return 3.0
        if dager < 30:   return 4.0
        if dager < 60:   return 5.0
        return 7.0


def enrich_row_with_prices(r: dict, now: datetime) -> None:
    pris = parse_price(r.get("Pris"))
    startpris = parse_price(r.get("HoyestePris"))
    if not pris and startpris:
        pris = startpris
    if not startpris and pris:
        startpris = pris
    r["NaaverendePris"] = format_price(pris)
    if startpris and pris and startpris > pris:
        diff = startpris - pris
        pct = round(diff / startpris * 100, 1)
        diff_f = f"{diff:,.0f}".replace(",", " ")
        r["PrisfallHtml"] = (
            f'<span class="prisfall-cell">'
            f'<span class="prisfall-pil">↓</span>'
            f'<span class="prisfall-kr"> {diff_f} kr</span>'
            f'<span class="prisfall-pct">({pct}%)</span>'
            f'</span>'
        )
    else:
        r["PrisfallHtml"] = '<span class="note-secondary">—</span>'

    if not pris:
        r["AlleredeKuttetHtml"] = '<span class="note-secondary">—</span>'
        r["ForventetPrutingHtml"] = '<span class="note-secondary">—</span>'
        r["AntattKjopsprisHtml"] = '<span class="note-secondary">—</span>'
        r["AntattKjopsprisSort"] = 0
        return

    if startpris and startpris > pris:
        kuttet_kr = startpris - pris
        kuttet_pct = round(kuttet_kr / startpris * 100, 1)
        kuttet_kr_f = f"{kuttet_kr:,.0f}".replace(",", " ")
        r["AlleredeKuttetHtml"] = (
            f'<span class="prisfall-cell">'
            f'<span class="prisfall-pil">↓</span>'
            f'<span class="prisfall-kr"> {kuttet_kr_f} kr</span>'
            f'<span class="prisfall-pct">({kuttet_pct}%)</span>'
            f'</span>'
        )
    else:
        r["AlleredeKuttetHtml"] = '<span class="note-secondary">—</span>'

    publisert = r.get("PublisertDato")
    if publisert:
        if hasattr(publisert, "date"):
            dager = (now - publisert).days
        else:
            try:
                dager = (now - datetime.strptime(str(publisert)[:10], "%Y-%m-%d")).days
            except (ValueError, TypeError):
                dager = r.get("DagerPaaMarkedet") or 0
    else:
        dager = r.get("DagerPaaMarkedet") or 0

    selgertype = r.get("SelgerType") or ""
    pruting_pct = _forventet_pruting_pct(selgertype, dager)
    r["ForventetPrutingHtml"] = f'<span class="note-secondary">{pruting_pct:.0f}%</span>'

    antatt = round(pris * (1 - pruting_pct / 100))
    antatt_f = f"{antatt:,.0f}".replace(",", " ")
    r["AntattKjopsprisSort"] = antatt
    if startpris and startpris > antatt:
        total_pct = round((startpris - antatt) / startpris * 100, 1)
        r["AntattKjopsprisHtml"] = (
            f'<span class="antatt-kjopspris">'
            f'<strong>{antatt_f} kr</strong>'
            f'<span class="prisfall-pct"> (-{total_pct}% fra start)</span>'
            f'</span>'
        )
    else:
        r["AntattKjopsprisHtml"] = f'<strong>{antatt_f} kr</strong>'


def _diff_pill(label: str, val, ref_val, unit: str = "", lower_is_better: bool = False) -> str:
    """Lager en verdi-celle med fargekodet diff-pill mot referansevognen."""
    if val is None or ref_val is None:
        return f'<div class="ref-cell"><div class="ref-lbl">{label}</div><div class="ref-val">—</div></div>'
    try:
        v = float(val)
        r = float(ref_val)
    except (TypeError, ValueError):
        return f'<div class="ref-cell"><div class="ref-lbl">{label}</div><div class="ref-val">{val}{unit}</div></div>'

    diff = v - r
    if abs(diff) < 0.01:
        pill = '<span class="diff-pill diff-neutral">=</span>'
    else:
        pct = diff / r * 100 if r else 0
        sign = "+" if diff > 0 else ""
        if label in ("Pris", "Egenvekt", "Totalvekt"):
            better = diff < 0
        elif lower_is_better:
            better = diff < 0
        else:
            better = diff > 0
        cls = "diff-better" if better else "diff-worse"
        if unit in (" kr",):
            diff_f = f"{int(abs(diff)):,}".replace(",", " ")
            pill = f'<span class="diff-pill {cls}">{sign}{diff_f} kr ({sign}{pct:.0f}%)</span>'
        else:
            pill = f'<span class="diff-pill {cls}">{sign}{diff:.0f}{unit} ({sign}{pct:.0f}%)</span>'

    val_fmt = f"{int(v):,}".replace(",", " ") + unit if unit in (" kr",) else f"{v:.0f}{unit}" if v == int(v) else f"{v}{unit}"
    return f'<div class="ref-cell"><div class="ref-lbl">{label}</div><div class="ref-val">{val_fmt}{pill}</div></div>'


def build_ref_banner(ad: dict) -> str:
    ref = REFERANSEVOGN
    pris = parse_price(ad.get("Pris"))
    egenvekt = ad.get("Egenvekt") or ad.get("SvvEgenvekt")
    totalvekt = ad.get("Totalvekt") or ad.get("SvvTillattTotalvekt")
    nyttelast = ad.get("Nyttelast") or ad.get("SvvNyttelast")
    lengde = ad.get("Lengde") or ad.get("SvvLengde")
    bredde = ad.get("Bredde") or ad.get("SvvBredde")
    soveplasser = ad.get("Soveplasser")
    aarsmodell_raw = ad.get("Modell") or ad.get("SvvAarsmodell")
    try:
        aarsmodell = int(str(aarsmodell_raw)[:4]) if aarsmodell_raw else None
    except (TypeError, ValueError):
        aarsmodell = None

    cells = [
        _diff_pill("Pris", pris, ref["pris"], " kr", lower_is_better=True),
        _diff_pill("Egenvekt", egenvekt, ref["egenvekt"], " kg", lower_is_better=True),
        _diff_pill("Totalvekt", totalvekt, ref["totalvekt"], " kg", lower_is_better=True),
        _diff_pill("Nyttelast", nyttelast, ref["nyttelast"], " kg"),
        _diff_pill("Lengde", lengde, ref["lengde"], " cm"),
        _diff_pill("Bredde", bredde, ref["bredde"], " cm"),
        _diff_pill("Soveplasser", soveplasser, ref["soveplasser"]),
        _diff_pill("Årsmodell", aarsmodell, ref["aarsmodell"]),
    ]
    return (
        f'<div class="ref-banner">'
        f'<div class="ref-banner-title">Sammenliknet med referansevogn — {esc(ref["navn"])}</div>'
        f'<div class="ref-grid">{"".join(cells)}</div>'
        f'</div>'
    )


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

_db_pool = None


def _get_pool():
    global _db_pool
    if _db_pool is None:
        try:
            _db_pool = pooling.MySQLConnectionPool(
                pool_name="campingvogn", pool_size=5, **DB_CONFIG
            )
        except Exception as e:
            logger.error("Feil ved oppretting av DB-pool: %s", e)
    return _db_pool


def get_db():
    pool = _get_pool()
    if not pool:
        return None
    try:
        return pool.get_connection()
    except Exception as e:
        logger.error("Feil ved henting av DB-tilkobling: %s", e)
        return None


def get_total_count():
    conn = get_db()
    if not conn:
        return 0
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM `{TABLE}`")
        return cur.fetchone()[0]
    except Exception:
        return 0
    finally:
        conn.close()


def get_annonser() -> list[dict]:
    conn = get_db()
    if not conn:
        return []
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(f"""
            SELECT c.Finnkode, c.Annonsenavn, c.Modell, c.Pris, c.Oppdatert,
                   c.PublisertDato, c.Egenvekt, c.Lengde, c.Bredde, c.Soveplasser,
                   c.Nyttelast, c.Totalvekt, c.ImageURL, c.Lokasjon, c.Kjennemerke,
                   c.SelgerType, c.SelgerNavn, c.Solgt, c.URL,
                   c.SvvMerke, c.SvvAarsmodell, c.SvvEgenvekt, c.SvvNyttelast,
                   c.SvvTillattTotalvekt, c.SvvLengde, c.SvvBredde, c.SvvAntallAksler,
                   COALESCE(u.Favoritt, 0) AS Favoritt,
                   MAX(NULLIF(CAST(REGEXP_REPLACE(p.Pris, '[^0-9]', '') AS UNSIGNED), 0)) AS HoyestePris,
                   MAX(p.Tidspunkt) AS SistePrisendring,
                   COUNT(p.Pris) AS AntallEndringer
            FROM `{TABLE}` c
            LEFT JOIN `{PRISENDRINGER_TABLE}` p ON c.Finnkode = p.Finnkode
            LEFT JOIN `{BRUKER_TABLE}` u ON c.Finnkode = u.Finnkode
            WHERE (c.Solgt = 0 OR c.Solgt IS NULL)
            GROUP BY c.Finnkode
            ORDER BY COALESCE(MAX(p.Tidspunkt), c.PublisertDato, c.Oppdatert) DESC
        """)
        rows = cur.fetchall()
        now = datetime.now()
        for r in rows:
            enrich_row_with_prices(r, now)
            r["AdURL"] = r.get("URL") or f"https://www.finn.no/mobility/item/{r['Finnkode']}"
            alder_val = r.get("SistePrisendring") or r.get("PublisertDato") or r.get("Oppdatert") or ""
            r["Alder"], r["AlderClass"], r["AlderSort"] = format_age(alder_val)
            dato = parse_norwegian_date(r.get("Oppdatert") or "")
            r["DagerPaaMarkedet"] = (now - dato).days if dato else 0
            r["ErNy"] = r["DagerPaaMarkedet"] <= 1
        return rows
    except Exception as e:
        logger.error("Feil i get_annonser: %s\n%s", e, traceback.format_exc())
        return []
    finally:
        conn.close()


def get_bruker_data(finnkode: int) -> dict:
    conn = get_db()
    if not conn:
        return {"favoritt": False, "notat": "", "prisvarsel": None}
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(f"SELECT Favoritt, Notat, PrisVarsel FROM `{BRUKER_TABLE}` WHERE Finnkode = %s", (finnkode,))
        row = cur.fetchone()
        if row:
            return {"favoritt": bool(row["Favoritt"]), "notat": row["Notat"] or "", "prisvarsel": row["PrisVarsel"]}
        return {"favoritt": False, "notat": "", "prisvarsel": None}
    except Exception:
        return {"favoritt": False, "notat": "", "prisvarsel": None}
    finally:
        conn.close()


def get_prishistorikk(finnkode: int) -> list[dict]:
    conn = get_db()
    if not conn:
        return []
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            f"SELECT Pris, Tidspunkt FROM `{PRISENDRINGER_TABLE}` WHERE Finnkode = %s ORDER BY Tidspunkt ASC",
            (finnkode,)
        )
        return cur.fetchall()
    except Exception:
        return []
    finally:
        conn.close()


def ensure_db_columns() -> None:
    from campingvogn_v2 import ensure_schema
    ensure_schema()


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

def run_scraper_background():
    if scraper_status["running"]:
        return
    scraper_status["running"] = True
    try:
        sys.path.insert(0, "/usr/bin")
        from campingvogn_v2 import run_scraper
        run_scraper()
        scraper_status["last_run"] = datetime.now()
        scraper_status["error"] = None
    except Exception as e:
        scraper_status["error"] = str(e)
        logger.error("Scraper feilet: %s", e)
    finally:
        scraper_status["running"] = False


def schedule_scraper(interval_hours=6):
    import time

    def loop():
        while True:
            logger.info("Starter planlagt scraping...")
            run_scraper_background()
            time.sleep(interval_hours * 3600)

    t = threading.Thread(target=loop, daemon=True, name="scraper-scheduler")
    t.start()
    logger.info("Scraper planlagt til å kjøre hver %d. time.", interval_hours)


# ---------------------------------------------------------------------------
# Flask-app
# ---------------------------------------------------------------------------

app = Flask(__name__)

TEMPLATE = """
<!DOCTYPE html>
<html lang="no">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Campingvogn — Markedsplassoversikt</title>
    <style>
        :root {
            --accent:       #30D158;
            --accent-dim:   rgba(48,209,88,0.15);
            --bg:           #000000;
            --bg-elevated:  #1C1C1E;
            --bg-grouped:   #2C2C2E;
            --separator:    rgba(255,255,255,0.08);
            --separator-op: rgba(255,255,255,0.14);
            --label:        #FFFFFF;
            --label-sec:    rgba(235,235,245,0.60);
            --label-ter:    rgba(235,235,245,0.30);
            --fill:         rgba(120,120,128,0.36);
            --green:        #30D158;
            --orange:       #FF9F0A;
            --red:          #FF453A;
            --radius-sm:    8px;
            --radius-md:    12px;
            --radius-lg:    16px;
        }
        *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Text', 'Helvetica Neue', sans-serif; background: var(--bg); color: var(--label); line-height: 1.5; min-height: 100vh; -webkit-font-smoothing: antialiased; }
        .container { max-width: 1280px; margin: 0 auto; padding: 20px 16px 40px; }
        .app-header { display: flex; align-items: baseline; gap: 10px; margin-bottom: 24px; }
        .app-header h1 { font-size: 1.75rem; font-weight: 700; letter-spacing: -0.4px; }
        .app-header .subtitle { font-size: 0.85rem; color: var(--label-sec); }
        .tabs { display: flex; gap: 0; margin-bottom: 16px; background: var(--bg-grouped); border-radius: var(--radius-md); padding: 3px; overflow-x: auto; scrollbar-width: none; }
        .tab { flex: 1; min-width: max-content; padding: 7px 14px; background: transparent; color: var(--label-sec); text-decoration: none; border-radius: 9px; font-size: 0.82rem; font-weight: 500; text-align: center; transition: background 0.18s ease, color 0.18s ease; white-space: nowrap; }
        .tab:hover { color: var(--label); }
        .tab.active { background: var(--bg-elevated); color: var(--label); font-weight: 600; box-shadow: 0 1px 4px rgba(0,0,0,0.4), 0 0 0 0.5px var(--separator-op); }
        .content { background: var(--bg-elevated); border-radius: var(--radius-lg); overflow: hidden; border: 0.5px solid var(--separator-op); }
        .content-inner { padding: 16px 20px; overflow-x: auto; }
        table { width: 100%; border-collapse: collapse; font-size: 0.84rem; }
        thead { position: sticky; top: 0; z-index: 2; }
        th { background: var(--bg-elevated); color: var(--label-sec); padding: 10px 12px; text-align: left; font-weight: 600; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.4px; white-space: nowrap; border-bottom: 0.5px solid var(--separator-op); }
        td { padding: 10px 12px; border-bottom: 0.5px solid var(--separator); vertical-align: middle; color: var(--label); }
        tbody tr:last-child td { border-bottom: none; }
        tbody tr:hover td { background: var(--fill); }
        a { color: var(--accent); text-decoration: none; }
        a:hover { text-decoration: underline; }
        th.sortable { cursor: pointer; user-select: none; }
        th.sortable:hover { color: var(--label); }
        th.sortable::after { content: ' ⇅'; font-size: 0.65em; opacity: 0.3; }
        th.sort-asc::after { content: ' ▲'; opacity: 0.7; }
        th.sort-desc::after { content: ' ▼'; opacity: 0.7; }
        .badge { display: inline-flex; align-items: center; padding: 2px 7px; border-radius: 20px; font-size: 0.68rem; font-weight: 600; vertical-align: middle; margin-left: 4px; }
        .new-badge { background: var(--orange); color: #000; }
        .sold-badge { background: var(--red); color: #fff; }
        .age-fresh { color: var(--green); }
        .age-weeks { color: var(--orange); }
        .age-old { color: var(--red); }
        .age-unknown { color: var(--label-ter); }
        .thumb { width: 72px; height: 54px; object-fit: cover; border-radius: var(--radius-sm); vertical-align: middle; }
        .thumb-cell { width: 80px; padding: 6px 8px 6px 4px !important; }
        .detail-img { max-width: 520px; width: 100%; border-radius: var(--radius-md); margin-bottom: 20px; }
        .no-data { color: var(--label-sec); padding: 40px 20px; text-align: center; }
        .note-secondary { color: var(--label-ter); }
        .btn { padding: 7px 14px; border-radius: var(--radius-sm); background: var(--bg-grouped); color: var(--label); border: 0.5px solid var(--separator-op); cursor: pointer; font-size: 0.82rem; font-weight: 500; }
        .btn:hover { background: var(--fill); }
        .btn-primary { background: var(--accent); color: #000; border-color: transparent; }
        .btn-sm { padding: 5px 10px; font-size: 0.78rem; }
        .prisfall-cell { white-space: nowrap; }
        .prisfall-pil { color: var(--green); font-weight: 700; }
        .prisfall-kr { font-weight: 600; color: var(--green); }
        .prisfall-pct { font-size: 0.78em; color: var(--label-sec); margin-left: 3px; }
        .antatt-kjopspris { white-space: nowrap; }
        .antatt-kjopspris strong { color: var(--accent); }
        .fav-col { width: 28px; text-align: center; padding: 0 2px; }
        .fav-liste-btn { background: none; border: none; font-size: 1.1em; cursor: pointer; padding: 0; line-height: 1; opacity: 0.4; transition: opacity 0.15s, transform 0.15s; }
        .fav-liste-btn:hover { opacity: 1; transform: scale(1.2); }
        .fav-liste-btn-aktiv { opacity: 1; }
        .fav-btn { background: none; border: none; font-size: 1.5em; cursor: pointer; line-height: 1; padding: 0; transition: transform 0.15s; }
        .fav-btn:hover { transform: scale(1.2); }
        .selger-privat { background: rgba(48,209,88,0.15); color: #30d158; border-radius: 4px; padding: 1px 6px; font-size: 0.75rem; font-weight: 600; }
        .selger-forhandler { background: rgba(10,132,255,0.15); color: #0A84FF; border-radius: 4px; padding: 1px 6px; font-size: 0.75rem; font-weight: 600; }
        .info-panel { background: var(--bg-grouped); border: 0.5px solid var(--separator-op); border-radius: var(--radius-md); padding: 16px; margin-bottom: 16px; }
        .info-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 8px 24px; font-size: 0.9em; }
        .info-grid .lbl { color: var(--label-sec); font-size: 0.8em; }
        .svv-panel { background: var(--bg-grouped); border: 0.5px solid var(--separator-op); padding: 16px; border-radius: var(--radius-md); margin-bottom: 20px; }
        .svv-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px 24px; font-size: 0.9em; }
        .svv-grid .lbl { color: var(--label-sec); }
        .section-heading { color: var(--accent); margin: 20px 0 10px; font-size: 1.05rem; font-weight: 600; }
        .notat-section { margin-bottom: 20px; padding: 14px 16px; background: var(--bg-grouped); border: 0.5px solid var(--separator-op); border-radius: var(--radius-md); }
        .notat-label { font-size: 0.72rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; color: var(--label-sec); margin-bottom: 8px; }
        .notat-textarea { width: 100%; max-width: 600px; background: var(--bg-elevated); color: var(--label); border: 0.5px solid var(--separator-op); border-radius: var(--radius-sm); padding: 8px 10px; font-size: 0.9em; resize: vertical; font-family: inherit; }
        .notat-textarea:focus { outline: none; border-color: var(--accent); }
        .notat-save-row { display: flex; align-items: center; gap: 10px; margin-top: 8px; }
        .notat-status { font-size: 0.8em; color: var(--label-sec); }
        .kjennemerke-rediger-rad { display: flex; align-items: center; gap: 8px; margin-bottom: 4px; flex-wrap: wrap; }
        .kjennemerke-input { background: var(--bg-grouped); border: 1px solid var(--separator-op); border-radius: 6px; color: var(--label); padding: 3px 8px; font-size: 0.9rem; width: 100px; text-transform: uppercase; }
        .kjennemerke-status { font-size: 0.78rem; color: var(--label-sec); }
        .kjennemerke-hint { font-size: 0.85em; color: var(--label-sec); margin: 10px 0 20px; padding: 10px 16px; background: var(--bg-grouped); border: 0.5px solid var(--separator-op); border-radius: var(--radius-sm); }
        .detail-nav { display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; font-size: 0.85em; }
        .detail-title { display: flex; align-items: center; gap: 12px; margin-bottom: 10px; color: var(--label); }
        .prisvarsel-satt { color: var(--label-sec); font-size: 0.85rem; }
        .prisvarsel-utloest { color: var(--red); font-weight: 700; font-size: 0.85rem; }
        .prishistorikk-tabell { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
        .prishistorikk-tabell td { padding: 6px 10px; border-bottom: 0.5px solid var(--separator); }
        .stat-box { background: var(--bg-grouped); border: 0.5px solid var(--separator-op); border-radius: var(--radius-md); padding: 14px 18px; }
        .stat-num { font-size: 2rem; font-weight: 700; color: var(--accent); }
        .stat-lbl { font-size: 0.78rem; color: var(--label-sec); margin-top: 2px; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(150px, 1fr)); gap: 12px; margin-bottom: 20px; }
        .truncate { max-width: 280px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .ref-banner { background: var(--bg-grouped); border: 0.5px solid var(--accent); border-radius: var(--radius-md); padding: 14px 18px; margin-bottom: 20px; }
        .ref-banner-title { font-size: 0.72rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.6px; color: var(--accent); margin-bottom: 10px; }
        .ref-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(130px, 1fr)); gap: 8px 16px; }
        .ref-cell { font-size: 0.85em; }
        .ref-cell .ref-lbl { color: var(--label-sec); font-size: 0.75em; margin-bottom: 1px; }
        .ref-cell .ref-val { font-weight: 600; }
        .diff-pill { display: inline-block; padding: 1px 6px; border-radius: 10px; font-size: 0.72em; font-weight: 700; margin-left: 4px; vertical-align: middle; }
        .diff-better { background: rgba(48,209,88,0.18); color: #30D158; }
        .diff-worse  { background: rgba(255,69,58,0.18);  color: #FF453A; }
        .diff-neutral{ background: rgba(120,120,128,0.22); color: var(--label-sec); }
        .chart-bar-wrap { display: flex; flex-direction: column; gap: 6px; margin-top: 8px; }
        .chart-bar-row { display: flex; align-items: center; gap: 8px; font-size: 0.8em; }
        .chart-bar-lbl { width: 54px; text-align: right; color: var(--label-sec); flex-shrink: 0; }
        .chart-bar-track { flex: 1; background: var(--bg-grouped); border-radius: 4px; height: 18px; overflow: hidden; }
        .chart-bar-fill { height: 100%; background: var(--accent); border-radius: 4px; transition: width 0.3s; }
        .chart-bar-num { width: 80px; color: var(--label-sec); font-size: 0.78em; flex-shrink: 0; }
    </style>
</head>
<body>
<div class="container">
    <div class="app-header">
        <h1>Campingvogn</h1>
        <span class="subtitle">{{ total_listings }} annonser</span>
        {% if last_scrape %}<span class="subtitle">· Sist oppdatert {{ last_scrape }}</span>{% endif %}
        {% if scraper_running %}<span class="subtitle">· Scraper kjører...</span>{% endif %}
    </div>
    <nav class="tabs">
        <a href="{{ bp }}annonser" class="tab {% if active_tab == 'annonser' %}active{% endif %}">Annonser</a>
        <a href="{{ bp }}favoritter" class="tab tab-star {% if active_tab == 'favoritter' %}active{% endif %}">★ Favoritter</a>
        <a href="{{ bp }}statistikk" class="tab {% if active_tab == 'statistikk' %}active{% endif %}">Statistikk</a>
        <a href="{{ bp }}scrape" class="tab {% if active_tab == 'scrape' %}active{% endif %}">Oppdater</a>
    </nav>
    <div class="content">
        <div class="content-inner">
            {{ content | safe }}
        </div>
    </div>
</div>
<script>
// Sorterbar tabell
document.querySelectorAll('th.sortable').forEach(th => {
    th.addEventListener('click', () => {
        const table = th.closest('table');
        const tbody = table.querySelector('tbody');
        const idx = Array.from(th.parentElement.children).indexOf(th);
        const isNum = th.dataset.sort === 'number';
        const asc = th.classList.contains('sort-asc');
        table.querySelectorAll('th').forEach(h => h.classList.remove('sort-asc', 'sort-desc'));
        th.classList.add(asc ? 'sort-desc' : 'sort-asc');
        const rows = Array.from(tbody.querySelectorAll('tr'));
        rows.sort((a, b) => {
            const av = a.children[idx]?.dataset?.sortValue ?? a.children[idx]?.textContent ?? '';
            const bv = b.children[idx]?.dataset?.sortValue ?? b.children[idx]?.textContent ?? '';
            if (isNum) return (asc ? -1 : 1) * ((parseFloat(av) || 0) - (parseFloat(bv) || 0));
            return (asc ? -1 : 1) * av.localeCompare(bv, 'no');
        });
        rows.forEach(r => tbody.appendChild(r));
    });
});
// Sett standard sort-desc på kolonnen med sort-desc klasse
document.querySelectorAll('th.sort-desc').forEach(th => {
    const ev = new MouseEvent('click');
    th.dispatchEvent(ev);
    th.classList.remove('sort-asc');
    th.classList.add('sort-desc');
});
</script>
</body>
</html>
"""


def render_page(active_tab: str, content_html: str) -> str:
    bp = request.headers.get("X-Ingress-Path", "").rstrip("/") + "/"
    last_scrape = scraper_status["last_run"].strftime("%d.%m.%Y %H:%M") if scraper_status["last_run"] else None
    return render_template_string(
        TEMPLATE,
        active_tab=active_tab,
        content=content_html,
        bp=bp,
        total_listings=get_total_count(),
        last_scrape=last_scrape,
        scraper_running=scraper_status["running"],
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return redirect("annonser")


@app.route("/annonser")
def view_annonser():
    rows = get_annonser()
    if not rows:
        return render_page("annonser", '<p class="no-data">Ingen annonser funnet.</p>')

    bp = request.headers.get("X-Ingress-Path", "").rstrip("/") + "/"
    html = """
    <table>
        <thead>
            <tr>
                <th class="fav-col sortable" data-sort="number" title="Sorter på favoritter">★</th>
                <th class="thumb-cell"></th>
                <th class="sortable">Annonse</th>
                <th class="sortable" data-sort="number">Modell</th>
                <th class="sortable" data-sort="number">Pris</th>
                <th class="sortable" data-sort="number">Prisfall</th>
                <th class="sortable" data-sort="number" title="Allerede kuttet fra startpris">Kuttet</th>
                <th class="sortable" data-sort="number" title="Forventet ytterligere pruting">Pruting</th>
                <th class="sortable" data-sort="number" title="Antatt kjøpspris">Antatt kjøp</th>
                <th class="sortable" data-sort="number">Egenvekt</th>
                <th class="sortable" data-sort="number">Lengde</th>
                <th class="sortable" data-sort="number">Soveplasser</th>
                <th class="sortable" data-sort="number">Endringer</th>
                <th class="sortable" data-sort="number">Dager</th>
                <th class="sortable sort-desc" data-sort="number">Sist endret</th>
            </tr>
        </thead>
        <tbody>
    """
    for r in rows:
        ny_badge = '<span class="new-badge">NY</span>' if r.get("ErNy") else ""
        er_fav = bool(r.get("Favoritt"))
        fk = r["Finnkode"]
        fav_stjerne = "⭐" if er_fav else "☆"
        fav_val = 1 if er_fav else 0
        img_url = r.get("ImageURL", "") or ""
        thumb = f'<img src="{esc(img_url)}" class="thumb" alt="">' if img_url else ""
        egenvekt = f"{r['Egenvekt']} kg" if r.get("Egenvekt") else (f"{r['SvvEgenvekt']} kg" if r.get("SvvEgenvekt") else "—")
        lengde_cm = r.get("Lengde") or r.get("SvvLengde")
        lengde = f"{lengde_cm} cm" if lengde_cm else "—"
        soveplasser = str(r["Soveplasser"]) if r.get("Soveplasser") else "—"
        html += f"""
            <tr>
                <td class="fav-col" data-sort-value="{fav_val}">
                    <button class="fav-liste-btn{'  fav-liste-btn-aktiv' if er_fav else ''}"
                            onclick="toggleFav({esc(fk)}, this, '{esc(bp)}')"
                            title="{'Fjern favoritt' if er_fav else 'Legg til favoritt'}">{fav_stjerne}</button>
                </td>
                <td class="thumb-cell">{thumb}</td>
                <td class="truncate"><a href="annonse/{esc(fk)}">{esc(r['Annonsenavn'])}</a>{ny_badge}</td>
                <td>{esc(r.get('Modell') or '—')}</td>
                <td>{esc(r['NaaverendePris'])}</td>
                <td>{r.get('PrisfallHtml') or '<span class="note-secondary">—</span>'}</td>
                <td>{r.get('AlleredeKuttetHtml') or '<span class="note-secondary">—</span>'}</td>
                <td>{r.get('ForventetPrutingHtml') or '<span class="note-secondary">—</span>'}</td>
                <td data-sort-value="{r.get('AntattKjopsprisSort', 0)}">{r.get('AntattKjopsprisHtml') or '<span class="note-secondary">—</span>'}</td>
                <td>{egenvekt}</td>
                <td>{lengde}</td>
                <td>{soveplasser}</td>
                <td><strong>{esc(r['AntallEndringer'])}</strong></td>
                <td>{esc(r['DagerPaaMarkedet'])}</td>
                <td class="{esc(r['AlderClass'])}" data-sort-value="{esc(r['AlderSort'])}">{esc(r['Alder'])}</td>
            </tr>
        """
    html += """</tbody></table>
    <script>
    function toggleFav(fk, btn, bp) {
        fetch(bp + 'api/favoritt/' + fk, {method: 'POST'})
            .then(r => r.json())
            .then(d => {
                if (d.ok) {
                    btn.textContent = d.favoritt ? '⭐' : '☆';
                    btn.title = d.favoritt ? 'Fjern favoritt' : 'Legg til favoritt';
                    btn.classList.toggle('fav-liste-btn-aktiv', d.favoritt);
                    const td = btn.closest('td');
                    if (td) td.dataset.sortValue = d.favoritt ? '1' : '0';
                }
            });
    }
    </script>"""
    return render_page("annonser", html)


@app.route("/favoritter")
def view_favoritter():
    conn = get_db()
    if not conn:
        return render_page("favoritter", '<p class="no-data">Databasefeil.</p>')
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(f"""
            SELECT c.Finnkode, c.Annonsenavn, c.Modell, c.Pris, c.ImageURL,
                   c.Egenvekt, c.Lengde, c.Soveplasser, c.URL, c.Solgt,
                   u.Notat, u.PrisVarsel,
                   MAX(NULLIF(CAST(REGEXP_REPLACE(p.Pris, '[^0-9]', '') AS UNSIGNED), 0)) AS HoyestePris
            FROM `{BRUKER_TABLE}` u
            JOIN `{TABLE}` c ON u.Finnkode = c.Finnkode
            LEFT JOIN `{PRISENDRINGER_TABLE}` p ON c.Finnkode = p.Finnkode
            WHERE u.Favoritt = 1
            GROUP BY c.Finnkode
            ORDER BY u.Oppdatert DESC
        """)
        rows = cur.fetchall()
    except Exception as e:
        logger.error("Feil i view_favoritter: %s", e)
        rows = []
    finally:
        conn.close()

    if not rows:
        return render_page("favoritter", '<p class="no-data">Ingen favoritter ennå.</p>')

    now = datetime.now()
    html = '<table><thead><tr>'
    html += '<th class="thumb-cell"></th>'
    html += '<th class="sortable">Annonse</th>'
    html += '<th class="sortable" data-sort="number">Modell</th>'
    html += '<th class="sortable" data-sort="number">Pris</th>'
    html += '<th class="sortable" data-sort="number">Prisfall</th>'
    html += '<th class="sortable" data-sort="number">Antatt kjøp</th>'
    html += '<th class="sortable" data-sort="number">Egenvekt</th>'
    html += '<th class="sortable" data-sort="number">Lengde</th>'
    html += '<th>Prisvarsel</th><th>Notat</th><th>Lenke</th>'
    html += '</tr></thead><tbody>'

    for r in rows:
        enrich_row_with_prices(r, now)
        img_url = r.get("ImageURL", "") or ""
        thumb = f'<img src="{esc(img_url)}" class="thumb" alt="">' if img_url else ""
        solgt_badge = '<span class="sold-badge">Solgt</span>' if r.get("Solgt") else ""
        fk = r["Finnkode"]
        prisvarsel = r.get("PrisVarsel")
        naav_pris = parse_price(r.get("Pris"))
        utloest = prisvarsel and naav_pris and naav_pris <= prisvarsel
        pv_html = (
            f'<span class="prisvarsel-utloest" title="Pris er under varselterskelen!">🔔 {format_price(prisvarsel)}</span>'
            if utloest else
            (f'<span class="prisvarsel-satt">{format_price(prisvarsel)}</span>' if prisvarsel else "—")
        )
        egenvekt = f"{r['Egenvekt']} kg" if r.get("Egenvekt") else "—"
        lengde = f"{r['Lengde']} cm" if r.get("Lengde") else "—"
        html += f"""
            <tr>
                <td class="thumb-cell">{thumb}</td>
                <td><a href="annonse/{esc(fk)}">{esc(r['Annonsenavn'])}</a>{solgt_badge}</td>
                <td>{esc(r.get('Modell') or '—')}</td>
                <td>{esc(r['NaaverendePris'])}</td>
                <td>{r.get('PrisfallHtml') or '—'}</td>
                <td data-sort-value="{r.get('AntattKjopsprisSort', 0)}">{r.get('AntattKjopsprisHtml') or '—'}</td>
                <td>{egenvekt}</td>
                <td>{lengde}</td>
                <td>{pv_html}</td>
                <td><span style="color:var(--label-sec);font-size:0.82em">{esc(r.get('Notat') or '')}</span></td>
                <td><a href="{esc(r.get('URL') or '')}" target="_blank" rel="noopener">Finn ↗</a></td>
            </tr>
        """
    html += '</tbody></table>'
    return render_page("favoritter", html)


@app.route("/statistikk")
def view_statistikk():
    conn = get_db()
    if not conn:
        return render_page("statistikk", '<p class="no-data">Databasefeil.</p>')
    try:
        cur = conn.cursor(dictionary=True)

        # --- Topptall ---
        cur.execute(f"SELECT COUNT(*) AS antall FROM `{TABLE}` WHERE Solgt = 0 OR Solgt IS NULL")
        antall_aktive = cur.fetchone()["antall"]
        cur.execute(f"SELECT COUNT(*) AS antall FROM `{TABLE}` WHERE Solgt = 1")
        antall_solgte = cur.fetchone()["antall"]
        cur.execute(f"SELECT ROUND(AVG(Pris)) AS snitt, MIN(Pris) AS minpris, MAX(Pris) AS makspris FROM `{TABLE}` WHERE Pris > 0 AND (Solgt = 0 OR Solgt IS NULL)")
        pris_row = cur.fetchone()
        snitt_pris = parse_price(pris_row["snitt"])
        min_pris = parse_price(pris_row["minpris"])
        maks_pris = parse_price(pris_row["makspris"])

        # --- Prisfordeling per årsmodell ---
        cur.execute(f"""
            SELECT
                COALESCE(SvvAarsmodell, CAST(LEFT(Modell,4) AS UNSIGNED)) AS Aar,
                COUNT(*) AS Antall,
                ROUND(MIN(Pris)) AS MinPris,
                ROUND(AVG(Pris)) AS SnittPris,
                ROUND(MAX(Pris)) AS MaksPris
            FROM `{TABLE}`
            WHERE Pris > 0 AND (Solgt = 0 OR Solgt IS NULL)
              AND (SvvAarsmodell >= 2010 OR CAST(LEFT(Modell,4) AS UNSIGNED) >= 2010)
            GROUP BY Aar
            HAVING Aar >= 2010 AND Aar <= YEAR(NOW())
            ORDER BY Aar DESC
            LIMIT 15
        """)
        aarsmodell_priser = cur.fetchall()

        # --- Tid på markedet (histogram-buckets i dager) ---
        cur.execute(f"""
            SELECT
                CASE
                    WHEN DATEDIFF(NOW(), COALESCE(PublisertDato, Opprettet)) < 7   THEN '0-7 dager'
                    WHEN DATEDIFF(NOW(), COALESCE(PublisertDato, Opprettet)) < 14  THEN '7-14 dager'
                    WHEN DATEDIFF(NOW(), COALESCE(PublisertDato, Opprettet)) < 30  THEN '14-30 dager'
                    WHEN DATEDIFF(NOW(), COALESCE(PublisertDato, Opprettet)) < 60  THEN '30-60 dager'
                    WHEN DATEDIFF(NOW(), COALESCE(PublisertDato, Opprettet)) < 90  THEN '60-90 dager'
                    ELSE '90+ dager'
                END AS Bucket,
                COUNT(*) AS Antall
            FROM `{TABLE}`
            WHERE (Solgt = 0 OR Solgt IS NULL) AND COALESCE(PublisertDato, Opprettet) IS NOT NULL
            GROUP BY Bucket
            ORDER BY MIN(DATEDIFF(NOW(), COALESCE(PublisertDato, Opprettet)))
        """)
        tid_buckets = cur.fetchall()

        # --- Prisfall-analyse: andel med prisfall, gjennomsnittlig kutt ---
        cur.execute(f"""
            SELECT
                COUNT(*) AS TotaltMedHistorikk,
                SUM(CASE WHEN HoyestePris > c.Pris THEN 1 ELSE 0 END) AS AntallMedKutt,
                ROUND(AVG(CASE WHEN HoyestePris > c.Pris THEN (HoyestePris - c.Pris) / HoyestePris * 100 END), 1) AS SnittKuttPct,
                ROUND(AVG(CASE WHEN HoyestePris > c.Pris THEN HoyestePris - c.Pris END)) AS SnittKuttKr
            FROM `{TABLE}` c
            JOIN (
                SELECT Finnkode,
                       MAX(NULLIF(CAST(REGEXP_REPLACE(Pris,'[^0-9]','') AS UNSIGNED),0)) AS HoyestePris
                FROM `{PRISENDRINGER_TABLE}`
                GROUP BY Finnkode
            ) p ON c.Finnkode = p.Finnkode
            WHERE (c.Solgt = 0 OR c.Solgt IS NULL) AND c.Pris > 0
        """)
        prisfall_row = cur.fetchone()

        # --- Markedsaktivitet: nye + solgte per uke siste 12 uker ---
        cur.execute(f"""
            SELECT
                DATE_FORMAT(DATE(COALESCE(PublisertDato, Opprettet)) - INTERVAL WEEKDAY(COALESCE(PublisertDato, Opprettet)) DAY, '%Y-%m-%d') AS Uke,
                COUNT(*) AS NyeAnnonser
            FROM `{TABLE}`
            WHERE COALESCE(PublisertDato, Opprettet) >= NOW() - INTERVAL 12 WEEK
            GROUP BY Uke
            ORDER BY Uke ASC
        """)
        ukentlig_nye = cur.fetchall()

        cur.execute(f"""
            SELECT
                DATE_FORMAT(DATE(COALESCE(PublisertDato, Opprettet)) - INTERVAL WEEKDAY(COALESCE(PublisertDato, Opprettet)) DAY, '%Y-%m-%d') AS Uke,
                COUNT(*) AS SolgteAnnonser
            FROM `{TABLE}`
            WHERE Solgt = 1 AND COALESCE(PublisertDato, Opprettet) >= NOW() - INTERVAL 12 WEEK
            GROUP BY Uke
            ORDER BY Uke ASC
        """)
        ukentlig_solgte = cur.fetchall()

        # --- Merker topp 10 ---
        cur.execute(f"""
            SELECT SvvMerke AS Merke, COUNT(*) AS Antall, ROUND(AVG(Pris)) AS SnittPris
            FROM `{TABLE}` WHERE SvvMerke IS NOT NULL AND (Solgt = 0 OR Solgt IS NULL)
            GROUP BY SvvMerke ORDER BY Antall DESC LIMIT 10
        """)
        merker = cur.fetchall()

        # --- Salgsanalyse: sammenlignbare vogner (ref ±2 år, ±15% lengde, ±2 soveplasser) ---
        ref = REFERANSEVOGN
        aar_fra = ref["aarsmodell"] - 2
        aar_til = ref["aarsmodell"] + 2
        lengde_fra = int(ref["lengde"] * 0.85)
        lengde_til = int(ref["lengde"] * 1.15)
        sov_fra = ref["soveplasser"] - 2
        sov_til = ref["soveplasser"] + 2
        cur.execute(f"""
            SELECT
                COUNT(*) AS Antall,
                ROUND(MIN(CAST(REGEXP_REPLACE(c.Pris,'[^0-9]','') AS UNSIGNED))) AS MinPris,
                ROUND(AVG(CAST(REGEXP_REPLACE(c.Pris,'[^0-9]','') AS UNSIGNED))) AS SnittPris,
                ROUND(MAX(CAST(REGEXP_REPLACE(c.Pris,'[^0-9]','') AS UNSIGNED))) AS MaksPris,
                ROUND(
                    AVG(CASE WHEN hp.HoyestePris > CAST(REGEXP_REPLACE(c.Pris,'[^0-9]','') AS UNSIGNED)
                        THEN (hp.HoyestePris - CAST(REGEXP_REPLACE(c.Pris,'[^0-9]','') AS UNSIGNED)) / hp.HoyestePris * 100
                        END), 1
                ) AS SnittFallPct,
                ROUND(AVG(DATEDIFF(NOW(), COALESCE(c.PublisertDato, c.Opprettet)))) AS SnittDager
            FROM `{TABLE}` c
            LEFT JOIN (
                SELECT Finnkode,
                       MAX(NULLIF(CAST(REGEXP_REPLACE(Pris,'[^0-9]','') AS UNSIGNED),0)) AS HoyestePris
                FROM `{PRISENDRINGER_TABLE}` GROUP BY Finnkode
            ) hp ON c.Finnkode = hp.Finnkode
            WHERE (c.Solgt = 0 OR c.Solgt IS NULL)
              AND CAST(REGEXP_REPLACE(c.Pris,'[^0-9]','') AS UNSIGNED) > 0
              AND COALESCE(c.SvvAarsmodell, CAST(LEFT(c.Modell,4) AS UNSIGNED)) BETWEEN %s AND %s
              AND CAST(REGEXP_REPLACE(c.Lengde,'[^0-9]','') AS UNSIGNED) BETWEEN %s AND %s
              AND CAST(REGEXP_REPLACE(c.Soveplasser,'[^0-9]','') AS UNSIGNED) BETWEEN %s AND %s
        """, (aar_fra, aar_til, lengde_fra, lengde_til, sov_fra, sov_til))
        salg_sammenlignbare = cur.fetchone()

        # Antall solgte sammenlignbare siste 6 mnd
        cur.execute(f"""
            SELECT COUNT(*) AS AntallSolgte,
                   ROUND(AVG(CAST(REGEXP_REPLACE(Pris,'[^0-9]','') AS UNSIGNED))) AS SnittSolgtPris
            FROM `{TABLE}`
            WHERE Solgt = 1
              AND COALESCE(PublisertDato, Opprettet) >= NOW() - INTERVAL 6 MONTH
              AND CAST(REGEXP_REPLACE(Pris,'[^0-9]','') AS UNSIGNED) > 0
              AND COALESCE(SvvAarsmodell, CAST(LEFT(Modell,4) AS UNSIGNED)) BETWEEN %s AND %s
              AND CAST(REGEXP_REPLACE(Lengde,'[^0-9]','') AS UNSIGNED) BETWEEN %s AND %s
              AND CAST(REGEXP_REPLACE(Soveplasser,'[^0-9]','') AS UNSIGNED) BETWEEN %s AND %s
        """, (aar_fra, aar_til, lengde_fra, lengde_til, sov_fra, sov_til))
        salg_historikk = cur.fetchone()

        # --- Sesong: snitt-pris og antall annonser per måned (alle år) ---
        # Bruker COALESCE(PublisertDato, Opprettet) — Oppdatert er varchar med norsk datoformat
        cur.execute(f"""
            SELECT
                MONTH(COALESCE(PublisertDato, Opprettet)) AS Maaned,
                COUNT(*) AS Antall,
                ROUND(AVG(CAST(REGEXP_REPLACE(Pris,'[^0-9]','') AS UNSIGNED))) AS SnittPris
            FROM `{TABLE}`
            WHERE CAST(REGEXP_REPLACE(Pris,'[^0-9]','') AS UNSIGNED) > 0
              AND COALESCE(PublisertDato, Opprettet) IS NOT NULL
            GROUP BY Maaned
            ORDER BY Maaned
        """)
        sesong_alle = cur.fetchall()

        # Sesong for solgte: gjennomsnittlig liggetid og pris per måned de ble solgt
        cur.execute(f"""
            SELECT
                MONTH(COALESCE(PublisertDato, Opprettet)) AS Maaned,
                COUNT(*) AS AntallSolgte,
                ROUND(AVG(CAST(REGEXP_REPLACE(Pris,'[^0-9]','') AS UNSIGNED))) AS SnittPris
            FROM `{TABLE}`
            WHERE Solgt = 1
              AND CAST(REGEXP_REPLACE(Pris,'[^0-9]','') AS UNSIGNED) > 0
              AND COALESCE(PublisertDato, Opprettet) IS NOT NULL
            GROUP BY Maaned
            ORDER BY Maaned
        """)
        sesong_solgte = cur.fetchall()

    except Exception as e:
        logger.error("Feil i statistikk: %s\n%s", e, traceback.format_exc())
        antall_aktive = antall_solgte = snitt_pris = min_pris = maks_pris = 0
        aarsmodell_priser = tid_buckets = merker = ukentlig_nye = ukentlig_solgte = []
        prisfall_row = {}
        salg_sammenlignbare = salg_historikk = None
        sesong_alle = sesong_solgte = []
    finally:
        conn.close()

    # --- Bygg HTML ---
    html = f"""
    <div class="stats-grid">
        <div class="stat-box"><div class="stat-num">{antall_aktive}</div><div class="stat-lbl">Aktive annonser</div></div>
        <div class="stat-box"><div class="stat-num">{antall_solgte}</div><div class="stat-lbl">Solgte (historikk)</div></div>
        <div class="stat-box"><div class="stat-num">{format_price(snitt_pris)}</div><div class="stat-lbl">Snittspris aktive</div></div>
        <div class="stat-box"><div class="stat-num">{format_price(min_pris)}</div><div class="stat-lbl">Laveste pris</div></div>
        <div class="stat-box"><div class="stat-num">{format_price(maks_pris)}</div><div class="stat-lbl">Høyeste pris</div></div>
    </div>
    """

    # Prisfordeling per årsmodell
    if aarsmodell_priser:
        html += '<h3 class="section-heading">Prisfordeling per årsmodell</h3>'
        html += '<table><thead><tr><th>Årsmodell</th><th class="sortable" data-sort="number">Antall</th><th class="sortable" data-sort="number">Min</th><th class="sortable" data-sort="number">Snitt</th><th class="sortable" data-sort="number">Maks</th></tr></thead><tbody>'
        for row in aarsmodell_priser:
            snitt = parse_price(row["SnittPris"])
            # Marker hvis snitt er nær referansevognens pris
            ref_mark = ""
            if row.get("Aar") == REFERANSEVOGN["aarsmodell"]:
                ref_mark = ' <span class="diff-pill diff-neutral">ref</span>'
            html += (
                f'<tr>'
                f'<td><strong>{esc(row["Aar"])}</strong>{ref_mark}</td>'
                f'<td>{row["Antall"]}</td>'
                f'<td>{format_price(parse_price(row["MinPris"]))}</td>'
                f'<td>{format_price(snitt)}</td>'
                f'<td>{format_price(parse_price(row["MaksPris"]))}</td>'
                f'</tr>'
            )
        html += '</tbody></table>'

    # Prisfall-analyse
    if prisfall_row and prisfall_row.get("TotaltMedHistorikk"):
        totalt = prisfall_row["TotaltMedHistorikk"] or 0
        med_kutt = prisfall_row["AntallMedKutt"] or 0
        andel = round(med_kutt / totalt * 100) if totalt else 0
        snitt_pct = prisfall_row["SnittKuttPct"] or 0
        snitt_kr = parse_price(prisfall_row["SnittKuttKr"])
        html += '<h3 class="section-heading">Prisfall-analyse</h3>'
        html += f'''
        <div class="stats-grid" style="margin-bottom:12px">
            <div class="stat-box">
                <div class="stat-num">{andel}%</div>
                <div class="stat-lbl">Andel annonser med prisfall ({med_kutt} av {totalt})</div>
            </div>
            <div class="stat-box">
                <div class="stat-num">{snitt_pct:.1f}%</div>
                <div class="stat-lbl">Gjennomsnittlig kutt (%)</div>
            </div>
            <div class="stat-box">
                <div class="stat-num">{format_price(snitt_kr)}</div>
                <div class="stat-lbl">Gjennomsnittlig kutt (kr)</div>
            </div>
        </div>
        '''

    # Tid på markedet
    if tid_buckets:
        max_antall = max(b["Antall"] for b in tid_buckets) or 1
        html += '<h3 class="section-heading">Tid på markedet (aktive annonser)</h3>'
        html += '<div class="chart-bar-wrap">'
        for b in tid_buckets:
            pct = b["Antall"] / max_antall * 100
            html += (
                f'<div class="chart-bar-row">'
                f'<div class="chart-bar-lbl">{esc(b["Bucket"])}</div>'
                f'<div class="chart-bar-track"><div class="chart-bar-fill" style="width:{pct:.0f}%"></div></div>'
                f'<div class="chart-bar-num">{b["Antall"]} annonser</div>'
                f'</div>'
            )
        html += '</div>'

    # Markedsaktivitet per uke
    if ukentlig_nye or ukentlig_solgte:
        # Bygg en felles uke-dict
        uke_data = {}
        for r in ukentlig_nye:
            uke_data.setdefault(r["Uke"], {"nye": 0, "solgte": 0})["nye"] = r["NyeAnnonser"]
        for r in ukentlig_solgte:
            uke_data.setdefault(r["Uke"], {"nye": 0, "solgte": 0})["solgte"] = r["SolgteAnnonser"]
        uker = sorted(uke_data.keys())
        max_val = max((max(v["nye"], v["solgte"]) for v in uke_data.values()), default=1) or 1

        html += '<h3 class="section-heading">Markedsaktivitet per uke (siste 12 uker)</h3>'
        html += '<table><thead><tr><th>Uke</th><th>Nye annonser</th><th>Solgte</th></tr></thead><tbody>'
        for uke in reversed(uker):
            d = uke_data[uke]
            nye_bar = f'<div style="display:inline-block;width:{int(d["nye"]/max_val*80)}px;height:10px;background:var(--accent);border-radius:2px;margin-right:4px;vertical-align:middle"></div>'
            solgt_bar = f'<div style="display:inline-block;width:{int(d["solgte"]/max_val*80)}px;height:10px;background:var(--red);border-radius:2px;margin-right:4px;vertical-align:middle"></div>'
            html += f'<tr><td>{esc(uke)}</td><td>{nye_bar}{d["nye"]}</td><td>{solgt_bar}{d["solgte"]}</td></tr>'
        html += '</tbody></table>'

    # Merker
    if merker:
        html += '<h3 class="section-heading">Merker (topp 10)</h3>'
        max_m = max(m["Antall"] for m in merker) or 1
        html += '<div class="chart-bar-wrap" style="margin-bottom:20px">'
        for m in merker:
            pct = m["Antall"] / max_m * 100
            html += (
                f'<div class="chart-bar-row">'
                f'<div class="chart-bar-lbl" style="width:80px">{esc(m["Merke"])}</div>'
                f'<div class="chart-bar-track"><div class="chart-bar-fill" style="width:{pct:.0f}%"></div></div>'
                f'<div class="chart-bar-num">{m["Antall"]} · {format_price(parse_price(m["SnittPris"]))}</div>'
                f'</div>'
            )
        html += '</div>'

    # -----------------------------------------------------------------------
    # SALGSANALYSE — Hva kan jeg selge min vogn for?
    # -----------------------------------------------------------------------
    ref = REFERANSEVOGN
    html += f'<h2 class="section-heading" style="font-size:1.2rem;margin-top:28px;border-top:0.5px solid var(--separator-op);padding-top:20px">Din vogn — salgsanalyse</h2>'
    html += f'<p style="color:var(--label-sec);font-size:0.85em;margin-bottom:16px">Basert på sammenlignbare annonser: årsmodell {ref["aarsmodell"]-2}–{ref["aarsmodell"]+2}, lengde {int(ref["lengde"]*0.85)}–{int(ref["lengde"]*1.15)} cm, {ref["soveplasser"]-2}–{ref["soveplasser"]+2} soveplasser.</p>'

    if salg_sammenlignbare and salg_sammenlignbare.get("Antall"):
        s = salg_sammenlignbare
        antall_s = s["Antall"] or 0
        min_s = parse_price(s["MinPris"])
        snitt_s = parse_price(s["SnittPris"])
        maks_s = parse_price(s["MaksPris"])
        fall_pct = float(s["SnittFallPct"] or 0)
        snitt_dager = int(s["SnittDager"] or 0)

        # Beregn forventet startpris og realistisk pris basert på ref-pris
        forventet_start = ref["pris"]
        realistisk_lav = round(snitt_s * 0.93) if snitt_s else None
        realistisk_hoy = round(snitt_s * 1.05) if snitt_s else None

        html += f'''
        <div class="stats-grid" style="margin-bottom:16px">
            <div class="stat-box">
                <div class="stat-num">{antall_s}</div>
                <div class="stat-lbl">Aktive sammenlignbare annonser</div>
            </div>
            <div class="stat-box">
                <div class="stat-num">{format_price(snitt_s)}</div>
                <div class="stat-lbl">Snittspris sammenlignbare</div>
            </div>
            <div class="stat-box">
                <div class="stat-num">{format_price(min_s)} – {format_price(maks_s)}</div>
                <div class="stat-lbl">Prisspenn i markedet</div>
            </div>
            <div class="stat-box">
                <div class="stat-num">{snitt_dager} dager</div>
                <div class="stat-lbl">Snitt liggetid aktive</div>
            </div>
        </div>
        '''

        # Prisanbefaling
        if snitt_s and realistisk_lav and realistisk_hoy:
            kjoper_betaler = round(snitt_s * (1 - fall_pct / 100)) if fall_pct else snitt_s
            html += f'''
            <div class="ref-banner" style="margin-bottom:16px">
                <div class="ref-banner-title">Prisanbefaling for din Dethleffs 480 QLK (2022)</div>
                <div class="ref-grid" style="gap:12px 24px">
                    <div class="ref-cell">
                        <div class="ref-lbl">Anbefalt annonseringspris</div>
                        <div class="ref-val" style="font-size:1.1em">{format_price(realistisk_lav)} – {format_price(realistisk_hoy)}</div>
                    </div>
                    <div class="ref-cell">
                        <div class="ref-lbl">Hva kjøper trolig betaler</div>
                        <div class="ref-val" style="font-size:1.1em;color:var(--accent)">{format_price(kjoper_betaler)}</div>
                    </div>
                    <div class="ref-cell">
                        <div class="ref-lbl">Typisk prisfall før salg</div>
                        <div class="ref-val">{fall_pct:.1f}%</div>
                    </div>
                    <div class="ref-cell">
                        <div class="ref-lbl">Kjøpspris din vogn (2024)</div>
                        <div class="ref-val note-secondary">{format_price(ref["pris"])}</div>
                    </div>
                </div>
            </div>
            '''

        if salg_historikk and salg_historikk.get("AntallSolgte"):
            sh = salg_historikk
            html += f'''
            <div class="info-panel" style="margin-bottom:20px">
                <div class="info-grid">
                    <div><div class="lbl">Solgte (siste 6 mnd)</div><div><strong>{sh["AntallSolgte"]}</strong></div></div>
                    <div><div class="lbl">Snittspris solgte</div><div><strong>{format_price(parse_price(sh["SnittSolgtPris"]))}</strong></div></div>
                </div>
            </div>
            '''
    else:
        html += '<p class="note-secondary" style="margin-bottom:20px">Ikke nok sammenlignbare annonser i databasen ennå.</p>'

    # -----------------------------------------------------------------------
    # SESONG — Når bør jeg selge?
    # -----------------------------------------------------------------------
    MAANED_NAVN = ["", "Jan", "Feb", "Mar", "Apr", "Mai", "Jun", "Jul", "Aug", "Sep", "Okt", "Nov", "Des"]

    if sesong_alle:
        sesong_map = {r["Maaned"]: r for r in sesong_alle}
        solgt_map  = {r["Maaned"]: r for r in sesong_solgte}

        max_antall = max((r["Antall"] for r in sesong_alle), default=1) or 1
        max_pris_m = max((parse_price(r["SnittPris"]) or 0 for r in sesong_alle), default=1) or 1

        html += '<h3 class="section-heading" style="margin-top:24px">Sesong — når bør du selge?</h3>'
        html += '<p style="color:var(--label-sec);font-size:0.85em;margin-bottom:12px">Basert på alle annonser i databasen. Høy aktivitet + høy snittspris = godt tidspunkt å legge ut.</p>'

        html += '<table style="margin-bottom:20px"><thead><tr>'
        html += '<th>Måned</th><th>Aktivitet</th><th>Snittspris aktive</th><th>Solgte</th><th>Snittspris solgte</th>'
        html += '</tr></thead><tbody>'

        # Finn beste måned (høyest kombinert score: normalisert antall * normalisert pris)
        scores = {}
        for mnd in range(1, 13):
            r = sesong_map.get(mnd, {})
            antall_n = (r.get("Antall") or 0) / max_antall
            pris_n = (parse_price(r.get("SnittPris")) or 0) / max_pris_m
            scores[mnd] = antall_n * 0.5 + pris_n * 0.5
        beste_mnd = max(scores, key=scores.get) if scores else None

        for mnd in range(1, 13):
            r = sesong_map.get(mnd, {})
            rs = solgt_map.get(mnd, {})
            antall_m = r.get("Antall") or 0
            snitt_m  = parse_price(r.get("SnittPris"))
            bar_w = int(antall_m / max_antall * 80) if max_antall else 0
            bar = f'<div style="display:inline-block;width:{bar_w}px;height:10px;background:var(--accent);border-radius:2px;margin-right:4px;vertical-align:middle"></div>'
            beste_mark = ' <span class="diff-pill diff-better">★ best</span>' if mnd == beste_mnd else ''
            html += (
                f'<tr>'
                f'<td><strong>{MAANED_NAVN[mnd]}</strong>{beste_mark}</td>'
                f'<td>{bar}{antall_m}</td>'
                f'<td>{format_price(snitt_m) if snitt_m else "—"}</td>'
                f'<td>{rs.get("AntallSolgte") or "—"}</td>'
                f'<td>{format_price(parse_price(rs.get("SnittPris"))) if rs.get("SnittPris") else "—"}</td>'
                f'</tr>'
            )
        html += '</tbody></table>'

        # Tekstlig anbefaling
        beste_navn = MAANED_NAVN[beste_mnd] if beste_mnd else "ukjent"
        topp3 = sorted(scores, key=scores.get, reverse=True)[:3]
        topp3_navn = ", ".join(MAANED_NAVN[m] for m in sorted(topp3))
        html += f'''
        <div class="ref-banner">
            <div class="ref-banner-title">Anbefaling basert på sesongdata</div>
            <p style="font-size:0.9em;color:var(--label);margin-bottom:6px">
                Beste måneder å legge ut for salg: <strong style="color:var(--accent)">{topp3_navn}</strong>
            </p>
            <p style="font-size:0.85em;color:var(--label-sec)">
                Høy markedsaktivitet kombinert med høy snittspris gir best utgangspunkt.
                Campingvogner selges typisk best tidlig vår (folk planlegger sesong) og svakest sent høst/vinter.
            </p>
        </div>
        '''

    return render_page("statistikk", html)


@app.route("/annonse/<finnkode>")
def view_annonse(finnkode):
    try:
        fk = int(finnkode)
    except (TypeError, ValueError):
        return "Ugyldig Finnkode", 400

    conn = get_db()
    if not conn:
        return render_page("annonser", '<p class="no-data">Databasefeil.</p>')
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(f"SELECT * FROM `{TABLE}` WHERE Finnkode = %s", (fk,))
        ad = cur.fetchone()
    except Exception as e:
        logger.error("Feil ved henting av annonse %s: %s", fk, e)
        ad = None
    finally:
        conn.close()

    if not ad:
        return render_page("annonser", '<p class="no-data">Annonse ikke funnet.</p>')

    bruker = get_bruker_data(fk)
    prishistorikk = get_prishistorikk(fk)
    now = datetime.now()
    bp = request.headers.get("X-Ingress-Path", "").rstrip("/") + "/"

    er_fav = bruker["favoritt"]
    img_url = ad.get("ImageURL", "") or ""
    img_tag = f'<img src="{esc(img_url)}" class="detail-img" alt="">' if img_url else ""
    pris = parse_price(ad.get("Pris"))
    finn_url = ad.get("URL") or f"https://www.finn.no/mobility/item/{fk}"
    kjennemerke = (ad.get("Kjennemerke") or "").strip()
    selger_type = ad.get("SelgerType") or ""
    selger_html = (
        '<span class="selger-privat">Privat</span>' if selger_type == "Privat"
        else '<span class="selger-forhandler">Forhandler</span>' if selger_type
        else "—"
    )
    prisvarsel = bruker.get("prisvarsel")
    utloest = prisvarsel and pris and pris <= prisvarsel
    pv_html = (
        f'<span class="prisvarsel-utloest">🔔 Utløst! Pris {format_price(pris)} ≤ {format_price(prisvarsel)}</span>'
        if utloest else
        (f'<span class="prisvarsel-satt">Varsel satt til {format_price(prisvarsel)}</span>' if prisvarsel else "")
    )

    # SVV-panel
    svv_html = ""
    har_svv = any(ad.get(f"Svv{x}") for x in ["Merke", "Aarsmodell", "Egenvekt", "Lengde"])
    if har_svv:
        svv_html = '<div class="svv-panel"><div class="svv-grid">'
        svv_felter = [
            ("Merke", ad.get("SvvMerke")),
            ("Årsmodell", ad.get("SvvAarsmodell")),
            ("Førstegangsr.", ad.get("SvvForstegangNorge")),
            ("Status", ad.get("SvvRegistreringsstatus")),
            ("Egenvekt", f"{ad.get('SvvEgenvekt')} kg" if ad.get("SvvEgenvekt") else None),
            ("Nyttelast", f"{ad.get('SvvNyttelast')} kg" if ad.get("SvvNyttelast") else None),
            ("Tillatt totalvekt", f"{ad.get('SvvTillattTotalvekt')} kg" if ad.get("SvvTillattTotalvekt") else None),
            ("Lengde", f"{ad.get('SvvLengde')} cm" if ad.get("SvvLengde") else None),
            ("Bredde", f"{ad.get('SvvBredde')} cm" if ad.get("SvvBredde") else None),
            ("Antall aksler", ad.get("SvvAntallAksler")),
        ]
        for lbl, val in svv_felter:
            if val:
                svv_html += f'<div><div class="lbl">{lbl}</div><div>{esc(val)}</div></div>'
        svv_html += '</div></div>'

    # Prishistorikk
    ph_html = ""
    if prishistorikk:
        ph_html = '<table class="prishistorikk-tabell"><thead><tr><th>Tidspunkt</th><th>Pris</th></tr></thead><tbody>'
        for p in reversed(prishistorikk):
            pris_str = format_price(parse_price(p["Pris"])) if p["Pris"] != "Solgt/Fjernet" else '<span class="sold-badge">Solgt</span>'
            ph_html += f'<tr><td>{esc(str(p["Tidspunkt"])[:16])}</td><td>{pris_str}</td></tr>'
        ph_html += '</tbody></table>'

    html = f"""
    <div class="detail-nav">
        <a href="{esc(bp)}annonser">← Tilbake</a>
        <a href="{esc(finn_url)}" target="_blank" rel="noopener">Åpne på Finn.no ↗</a>
    </div>
    <div class="detail-title">
        <button class="fav-btn" id="fav-btn" onclick="toggleFavDetail({fk}, '{esc(bp)}')"
                title="{'Fjern favoritt' if er_fav else 'Legg til favoritt'}">{'⭐' if er_fav else '☆'}</button>
        <h2>{esc(ad.get('Annonsenavn', ''))}</h2>
    </div>
    {img_tag}
    <div class="info-panel">
        <div class="info-grid">
            <div><div class="lbl">Pris</div><div><strong>{format_price(pris)}</strong></div></div>
            <div><div class="lbl">Årsmodell</div><div>{esc(ad.get('Modell') or '—')}</div></div>
            <div><div class="lbl">Egenvekt</div><div>{f"{ad.get('Egenvekt')} kg" if ad.get('Egenvekt') else '—'}</div></div>
            <div><div class="lbl">Lengde</div><div>{f"{ad.get('Lengde')} cm" if ad.get('Lengde') else '—'}</div></div>
            <div><div class="lbl">Bredde</div><div>{f"{ad.get('Bredde')} cm" if ad.get('Bredde') else '—'}</div></div>
            <div><div class="lbl">Soveplasser</div><div>{ad.get('Soveplasser') or '—'}</div></div>
            <div><div class="lbl">Nyttelast</div><div>{f"{ad.get('Nyttelast')} kg" if ad.get('Nyttelast') else '—'}</div></div>
            <div><div class="lbl">Totalvekt</div><div>{f"{ad.get('Totalvekt')} kg" if ad.get('Totalvekt') else '—'}</div></div>
            <div><div class="lbl">Selger</div><div>{selger_html}</div></div>
            <div><div class="lbl">Lokasjon</div><div>{esc(ad.get('Lokasjon') or '—')}</div></div>
        </div>
    </div>

    {build_ref_banner(ad)}

    <h3 class="section-heading">Kjennemerke / SVV-data</h3>
    <div class="kjennemerke-hint">
        Campingvogner oppgir sjelden skiltnummer i annonsen. Legg inn kjennemerke manuelt for å hente SVV-data.
    </div>
    <div class="kjennemerke-rediger-rad">
        <input type="text" class="kjennemerke-input" id="kjennemerke-input"
               value="{esc(kjennemerke)}" placeholder="AB1234" maxlength="8">
        <button class="btn btn-sm" onclick="lagreKjennemerke({fk}, '{esc(bp)}')">Lagre</button>
        <button class="btn btn-sm btn-primary" onclick="hentSvv({fk}, '{esc(bp)}')">Hent SVV-data</button>
        <span class="kjennemerke-status" id="kjennemerke-status"></span>
    </div>
    {svv_html}

    <h3 class="section-heading">Prishistorikk</h3>
    {ph_html if ph_html else '<p class="note-secondary">Ingen prishistorikk.</p>'}

    <h3 class="section-heading">Prisvarsel</h3>
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:16px">
        <input type="number" id="prisvarsel-input" placeholder="F.eks. 150000"
               value="{prisvarsel or ''}"
               style="width:120px;padding:6px 9px;border-radius:6px;border:1px solid var(--separator-op);background:var(--bg-grouped);color:var(--label);font-size:0.9rem">
        <button class="btn btn-sm" onclick="lagrePrisvarsel({fk}, '{esc(bp)}')">Sett varsel</button>
        {pv_html}
    </div>

    <div class="notat-section">
        <div class="notat-label">Notat</div>
        <textarea class="notat-textarea" id="notat-input" rows="4">{esc(bruker.get('notat', ''))}</textarea>
        <div class="notat-save-row">
            <button class="btn btn-sm btn-primary" onclick="lagreNotat({fk}, '{esc(bp)}')">Lagre notat</button>
            <span class="notat-status" id="notat-status"></span>
        </div>
    </div>

    <script>
    function toggleFavDetail(fk, bp) {{
        fetch(bp + 'api/favoritt/' + fk, {{method: 'POST'}})
            .then(r => r.json())
            .then(d => {{
                if (d.ok) {{
                    const btn = document.getElementById('fav-btn');
                    btn.textContent = d.favoritt ? '⭐' : '☆';
                    btn.title = d.favoritt ? 'Fjern favoritt' : 'Legg til favoritt';
                }}
            }});
    }}
    function lagreNotat(fk, bp) {{
        const notat = document.getElementById('notat-input').value;
        fetch(bp + 'api/notat/' + fk, {{
            method: 'POST', headers: {{'Content-Type': 'application/json'}},
            body: JSON.stringify({{notat}})
        }}).then(r => r.json()).then(d => {{
            document.getElementById('notat-status').textContent = d.ok ? 'Lagret!' : 'Feil';
            setTimeout(() => document.getElementById('notat-status').textContent = '', 2000);
        }});
    }}
    function lagrePrisvarsel(fk, bp) {{
        const pris = parseInt(document.getElementById('prisvarsel-input').value) || null;
        fetch(bp + 'api/prisvarsel/' + fk, {{
            method: 'POST', headers: {{'Content-Type': 'application/json'}},
            body: JSON.stringify({{prisvarsel: pris}})
        }}).then(r => r.json()).then(d => {{
            if (d.ok) location.reload();
        }});
    }}
    function lagreKjennemerke(fk, bp) {{
        const kjennemerke = document.getElementById('kjennemerke-input').value.trim().toUpperCase();
        fetch(bp + 'api/kjennemerke/' + fk, {{
            method: 'POST', headers: {{'Content-Type': 'application/json'}},
            body: JSON.stringify({{kjennemerke}})
        }}).then(r => r.json()).then(d => {{
            document.getElementById('kjennemerke-status').textContent = d.ok ? 'Lagret!' : (d.error || 'Feil');
            setTimeout(() => document.getElementById('kjennemerke-status').textContent = '', 3000);
        }});
    }}
    function hentSvv(fk, bp) {{
        document.getElementById('kjennemerke-status').textContent = 'Henter SVV-data...';
        fetch(bp + 'api/hent_svv/' + fk, {{method: 'POST'}})
            .then(r => r.json())
            .then(d => {{
                if (d.ok) {{
                    document.getElementById('kjennemerke-status').textContent = 'SVV-data hentet!';
                    setTimeout(() => location.reload(), 1000);
                }} else {{
                    document.getElementById('kjennemerke-status').textContent = d.error || 'Feil';
                }}
            }});
    }}
    </script>
    """
    return render_page("annonser", html)


@app.route("/scrape")
def view_scrape():
    html = f"""
    <p style="margin-bottom:16px;color:var(--label-sec)">
        Starter en manuell scraping av Finn.no. Dette kan ta noen minutter.
    </p>
    <form method="post" action="api/scrape">
        <button type="submit" class="btn btn-primary">Start scraping nå</button>
    </form>
    {'<p style="color:var(--red);margin-top:12px">Feil: ' + esc(scraper_status["error"]) + '</p>' if scraper_status["error"] else ''}
    {'<p style="color:var(--green);margin-top:12px">Scraper kjører...</p>' if scraper_status["running"] else ''}
    """
    return render_page("scrape", html)


# ---------------------------------------------------------------------------
# API-endepunkter
# ---------------------------------------------------------------------------

@app.route("/api/scrape", methods=["POST"])
def api_scrape():
    t = threading.Thread(target=run_scraper_background, daemon=True)
    t.start()
    return redirect("scrape")


@app.route("/api/favoritt/<finnkode>", methods=["POST"])
def api_favoritt(finnkode):
    try:
        fk = int(finnkode)
    except (TypeError, ValueError):
        return jsonify({"ok": False}), 400
    conn = get_db()
    if not conn:
        return jsonify({"ok": False}), 500
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(f"SELECT Favoritt FROM `{BRUKER_TABLE}` WHERE Finnkode = %s", (fk,))
        row = cur.fetchone()
        ny_verdi = 0 if (row and row["Favoritt"]) else 1
        cur.execute(f"""
            INSERT INTO `{BRUKER_TABLE}` (Finnkode, Favoritt)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE Favoritt = %s
        """, (fk, ny_verdi, ny_verdi))
        conn.commit()
        return jsonify({"ok": True, "favoritt": bool(ny_verdi)})
    except Exception as e:
        logger.error("Feil i api_favoritt: %s", e)
        return jsonify({"ok": False}), 500
    finally:
        conn.close()


@app.route("/api/notat/<finnkode>", methods=["POST"])
def api_notat(finnkode):
    try:
        fk = int(finnkode)
    except (TypeError, ValueError):
        return jsonify({"ok": False}), 400
    data = request.get_json(silent=True) or {}
    notat = str(data.get("notat", ""))[:2000]
    conn = get_db()
    if not conn:
        return jsonify({"ok": False}), 500
    try:
        cur = conn.cursor()
        cur.execute(f"""
            INSERT INTO `{BRUKER_TABLE}` (Finnkode, Notat)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE Notat = %s
        """, (fk, notat, notat))
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("Feil i api_notat: %s", e)
        return jsonify({"ok": False}), 500
    finally:
        conn.close()


@app.route("/api/prisvarsel/<finnkode>", methods=["POST"])
def api_prisvarsel(finnkode):
    try:
        fk = int(finnkode)
    except (TypeError, ValueError):
        return jsonify({"ok": False}), 400
    data = request.get_json(silent=True) or {}
    prisvarsel = data.get("prisvarsel")
    if prisvarsel is not None:
        try:
            prisvarsel = int(prisvarsel)
        except (TypeError, ValueError):
            prisvarsel = None
    conn = get_db()
    if not conn:
        return jsonify({"ok": False}), 500
    try:
        cur = conn.cursor()
        cur.execute(f"""
            INSERT INTO `{BRUKER_TABLE}` (Finnkode, PrisVarsel)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE PrisVarsel = %s
        """, (fk, prisvarsel, prisvarsel))
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("Feil i api_prisvarsel: %s", e)
        return jsonify({"ok": False}), 500
    finally:
        conn.close()


@app.route("/api/kjennemerke/<finnkode>", methods=["POST"])
def api_kjennemerke(finnkode):
    try:
        fk = int(finnkode)
    except (TypeError, ValueError):
        return jsonify({"ok": False}), 400
    data = request.get_json(silent=True) or {}
    kjennemerke = (data.get("kjennemerke") or "").strip().upper().replace(" ", "")
    conn = get_db()
    if not conn:
        return jsonify({"ok": False}), 500
    try:
        cur = conn.cursor()
        cur.execute(f"UPDATE `{TABLE}` SET Kjennemerke = %s WHERE Finnkode = %s", (kjennemerke, fk))
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("Feil i api_kjennemerke: %s", e)
        return jsonify({"ok": False}), 500
    finally:
        conn.close()


@app.route("/api/hent_svv/<finnkode>", methods=["POST"])
def api_hent_svv(finnkode):
    try:
        fk = int(finnkode)
    except (TypeError, ValueError):
        return jsonify({"ok": False}), 400
    conn = get_db()
    if not conn:
        return jsonify({"ok": False}), 500
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(f"SELECT Kjennemerke FROM `{TABLE}` WHERE Finnkode = %s", (fk,))
        rad = cur.fetchone()
        if not rad:
            return jsonify({"ok": False, "error": "Annonse ikke funnet"}), 404
        kjennemerke = (rad.get("Kjennemerke") or "").strip().upper().replace(" ", "")
        if not kjennemerke:
            return jsonify({"ok": False, "error": "Kjennemerke mangler — legg det inn først"})

        from campingvogn_v2 import fetch_svv_data, parse_vegvesen_data, _SVV_COLS
        import asyncio, aiohttp
        api_key = options.get("vegvesen_api_key", "")
        if not api_key:
            return jsonify({"ok": False, "error": "Vegvesen API-nøkkel ikke konfigurert"})

        async def _fetch():
            async with aiohttp.ClientSession() as session:
                return await fetch_svv_data(session, kjennemerke, api_key)

        svv = asyncio.run(_fetch())
        if not svv:
            return jsonify({"ok": False, "error": "Ingen data fra Vegvesen"})

        svv_key_map = {
            "SvvMerke": "svv_merke", "SvvAarsmodell": "svv_aarsmodell",
            "SvvForstegangNorge": "svv_forstegang_norge", "SvvRegistreringsstatus": "svv_registreringsstatus",
            "SvvEgenvekt": "svv_egenvekt", "SvvNyttelast": "svv_nyttelast",
            "SvvTillattTotalvekt": "svv_tillatt_totalvekt", "SvvLengde": "svv_lengde",
            "SvvBredde": "svv_bredde", "SvvAntallAksler": "svv_antall_aksler",
        }
        vals = [svv.get(svv_key_map[col]) for col in _SVV_COLS]
        set_clause = ", ".join(f"{col} = %s" for col in _SVV_COLS)
        cur2 = conn.cursor()
        cur2.execute(f"UPDATE `{TABLE}` SET {set_clause} WHERE Finnkode = %s", vals + [fk])
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("Feil i api_hent_svv: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/dbdiag")
def api_dbdiag():
    conn = get_db()
    if not conn:
        return jsonify({"ok": False, "error": "Ingen DB-tilkobling"})
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM `{TABLE}`")
        n = cur.fetchone()[0]
        return jsonify({"ok": True, "antall": n})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Oppstart
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logger.info("Starter Campingvogn web UI på port 8101...")
    ensure_db_columns()
    scrape_interval = options.get("scrape_interval", 6)
    schedule_scraper(interval_hours=scrape_interval)
    serve(app, host="0.0.0.0", port=8101)
