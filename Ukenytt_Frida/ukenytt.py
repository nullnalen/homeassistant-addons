#!/usr/bin/env python3
"""
Ukenytt Add-on for Home Assistant.

Mottar ukenytt-PDF-filer via HTTP og konverterer dem til Home Assistant sensorer.
"""

import json
import logging
import os
import re
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pdfplumber
import requests
import tabula
from flask import Flask, jsonify, request, render_template_string

# Konfigurer logging til stdout for S6-overlay
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# Konfigurasjon fra miljøvariabler (satt av S6/bashio)
API_KEY = os.getenv("UKENYTT_API_KEY", "")
HA_URL = os.getenv("UKENYTT_HA_URL", "http://supervisor/core")
HA_TOKEN = os.getenv("UKENYTT_HA_TOKEN") or os.getenv("SUPERVISOR_TOKEN", "")

# Parse barn fra JSON miljøvariabel
_children_json = os.getenv("UKENYTT_CHILDREN", "")
try:
    if not _children_json:
        raise ValueError("UKENYTT_CHILDREN er tom")
    _children_list = json.loads(_children_json)
    if not isinstance(_children_list, list) or not _children_list:
        raise ValueError("UKENYTT_CHILDREN er ikke en liste eller er tom")
    CHILDREN = [child.get("name", "Barn") for child in _children_list]
except (json.JSONDecodeError, TypeError, ValueError) as _cfg_err:
    logging.warning(
        "Kunne ikke parse UKENYTT_CHILDREN ('%s'): %s — bruker fallback ['Barn1']",
        _children_json, _cfg_err
    )
    CHILDREN = ["Barn1"]

# Versjon satt av Dockerfile via ADDON_VERSION env-var, fallback til hardkodet
# (synkroniseres med config.yaml ved hvert release via Dockerfile LABEL)
ADDON_VERSION = os.getenv("ADDON_VERSION", "1.0.25")

# Konstanter
MAX_INFO_LENGTH = 500
MAX_PDF_SIZE = 10 * 1024 * 1024  # 10 MB
WEEKDAYS = ["Mandag", "Tirsdag", "Onsdag", "Torsdag", "Fredag"]
WEEKDAYS_LOWER = ["mandag", "tirsdag", "onsdag", "torsdag", "fredag", "lørdag", "søndag"]

# Mappe for lagring av PDF-filer
DATA_DIR = Path("/data/ukenytt")
DATA_DIR.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)


def _safe_sensor_name(child_name: str) -> str:
    """Genererer sensornavn-vennlig streng (kun alfanumerisk og _)."""
    return "".join(c for c in child_name.lower() if c.isalnum() or c in "_")


def _safe_file_name(child_name: str) -> str:
    """Genererer filnavn-vennlig streng (alfanumerisk, - og _)."""
    return "".join(c for c in child_name.lower() if c.isalnum() or c in "-_")


# HTML-mal for Ingress-visning
INGRESS_TEMPLATE = """
<!DOCTYPE html>
<html lang="no">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Ukenytt</title>
    <style>
        :root {
            --primary-color: #03a9f4;
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
        .container { max-width: 900px; margin: 0 auto; }
        h1 {
            color: var(--primary-color);
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        h1 svg { width: 32px; height: 32px; fill: var(--primary-color); }
        .child-section {
            background: var(--card-bg);
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
            border: 1px solid var(--border-color);
        }
        .child-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 1px solid var(--border-color);
        }
        .child-name {
            font-size: 1.4em;
            font-weight: 600;
            color: var(--primary-color);
        }
        .week-badge {
            background: var(--primary-color);
            color: #000;
            padding: 5px 15px;
            border-radius: 20px;
            font-weight: 600;
        }
        .weekday {
            margin-bottom: 15px;
        }
        .weekday-name {
            font-weight: 600;
            color: var(--primary-color);
            margin-bottom: 5px;
            font-size: 1.1em;
        }
        .weekday-items {
            padding-left: 20px;
        }
        .weekday-items li {
            margin-bottom: 4px;
            color: var(--text-color);
        }
        .info-section {
            margin-top: 20px;
            padding-top: 15px;
            border-top: 1px solid var(--border-color);
        }
        .info-title {
            font-weight: 600;
            color: var(--text-muted);
            margin-bottom: 10px;
            font-size: 0.9em;
            text-transform: uppercase;
        }
        .info-content {
            white-space: pre-wrap;
            color: var(--text-color);
            background: rgba(0,0,0,0.2);
            padding: 15px;
            border-radius: 8px;
            font-size: 0.95em;
        }
        .no-data {
            color: var(--text-muted);
            font-style: italic;
            text-align: center;
            padding: 40px;
        }
        .api-info {
            margin-top: 30px;
            padding: 15px;
            background: rgba(3, 169, 244, 0.1);
            border-radius: 8px;
            border: 1px solid var(--primary-color);
        }
        .api-info h3 {
            color: var(--primary-color);
            margin-bottom: 10px;
        }
        .api-info code {
            background: rgba(0,0,0,0.3);
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 0.9em;
        }
        .refresh-btn {
            background: var(--primary-color);
            color: #000;
            border: none;
            padding: 8px 16px;
            border-radius: 6px;
            cursor: pointer;
            font-weight: 500;
        }
        .refresh-btn:hover { opacity: 0.9; }
    </style>
</head>
<body>
    <div class="container">
        <h1>
            <svg viewBox="0 0 24 24"><path d="M19,19H5V8H19M16,1V3H8V1H6V3H5C3.89,3 3,3.89 3,5V19A2,2 0 0,0 5,21H19A2,2 0 0,0 21,19V5C21,3.89 20.1,3 19,3H18V1M17,12H12V17H17V12Z"/></svg>
            Ukenytt
        </h1>

        {% for child in children_data %}
        <div class="child-section">
            <div class="child-header">
                <span class="child-name">{{ child.name }}</span>
                {% if child.week %}
                <span class="week-badge">Uke {{ child.week }}</span>
                {% endif %}
            </div>

            {% if child.ukeplan %}
                {% for day in ['Mandag', 'Tirsdag', 'Onsdag', 'Torsdag', 'Fredag'] %}
                    {% if day in child.ukeplan %}
                    <div class="weekday">
                        <div class="weekday-name">{{ day }}</div>
                        <ul class="weekday-items">
                            {% for item in child.ukeplan[day] %}
                            <li>{{ item }}</li>
                            {% endfor %}
                        </ul>
                    </div>
                    {% endif %}
                {% endfor %}
            {% else %}
                <p class="no-data">Ingen ukeplan lastet opp enn&aring;</p>
            {% endif %}

            {% if child.info %}
            <div class="info-section">
                <div class="info-title">Informasjon</div>
                <div class="info-content">{{ child.info }}</div>
            </div>
            {% endif %}
        </div>
        {% endfor %}

        <div class="api-info">
            <h3>API-endepunkter</h3>
            <p><code>POST /upload?child=navn</code> - Last opp PDF</p>
            <p><code>POST /refresh</code> - Oppdater idag/imorgen-sensorer</p>
            <p><code>GET /info/navn</code> - Hent full info-tekst (JSON)</p>
            <p><code>GET /api</code> - JSON API-status</p>
        </div>
    </div>
</body>
</html>
"""


def get_child_data(child_name: str) -> dict:
    """Henter data for et barn — lokal state først, HA-sensor som sekundær kilde."""
    data = {"name": child_name, "week": None, "ukeplan": None, "info": None}

    # Les lokal state først (rask, ingen nettverkskall)
    local_state = load_sensor_state(child_name)
    if local_state:
        data["week"] = local_state.get("state")
        attrs = local_state.get("attributes", {})
        data["ukeplan"] = attrs.get("ukeplan")
        data["info"] = attrs.get("info")

    # Hent full info fra fil (har alltid prioritet over truncert sensor-info)
    info_path = DATA_DIR / f"{_safe_file_name(child_name)}_info.txt"
    if info_path.exists():
        data["info"] = info_path.read_text(encoding="utf-8")

    return data


@app.route("/", methods=["GET"])
def index():
    """Root-endepunkt - viser Ingress HTML-side."""
    # Sjekk om det er en Ingress-forespørsel (HTML) eller API (JSON)
    accept = request.headers.get("Accept", "")
    if "text/html" in accept or not accept:
        # Hent data for alle barn
        children_data = [get_child_data(child) for child in CHILDREN]
        return render_template_string(INGRESS_TEMPLATE, children_data=children_data)

    # Fallback til JSON for API-kall
    return jsonify({
        "addon": "Ukenytt",
        "status": "running",
        "children": CHILDREN,
        "endpoints": {
            "upload": "POST /upload?child=<name>",
            "health": "GET /health",
            "status": "GET /status",
            "process": "POST /process",
            "info": "GET /info/<child_name>",
            "api": "GET /api"
        }
    })


@app.route("/api", methods=["GET"])
def api_index():
    """API-endepunkt - returnerer alltid JSON."""
    return jsonify({
        "addon": "Ukenytt",
        "version": ADDON_VERSION,
        "status": "running",
        "children": CHILDREN,
        "endpoints": {
            "upload": "POST /upload?child=<name>",
            "process": "POST /process",
            "refresh": "POST /refresh",
            "health": "GET /health",
            "status": "GET /status",
            "info": "GET /info/<child_name>"
        }
    })


def get_pdf_path(child_name: str) -> Path:
    """Returnerer stien til PDF-filen for et barn."""
    return DATA_DIR / f"{_safe_file_name(child_name)}.pdf"


def _get_original_filename_path(child_name: str) -> Path:
    """Returnerer stien til filen som lagrer det originale filnavnet."""
    return DATA_DIR / f"{_safe_file_name(child_name)}_filename.txt"


def save_original_filename(child_name: str, filename: str) -> None:
    """Lagrer originalt filnavn for bruk ved reprocessing."""
    _get_original_filename_path(child_name).write_text(filename, encoding="utf-8")


def get_original_filename(child_name: str) -> str | None:
    """Henter lagret originalt filnavn."""
    path = _get_original_filename_path(child_name)
    if path.exists():
        return path.read_text(encoding="utf-8").strip()
    return None


def parse_pdf(file_path: Path) -> tuple[dict, list]:
    """Parser PDF og returnerer ukeplan som dictionary og rå tabeller."""
    logger.info("Parser PDF: %s", file_path)

    pd.options.mode.chained_assignment = None

    try:
        # Bruk java_options for å unngå JPype (subprocess-modus)
        tables = tabula.read_pdf(
            str(file_path), pages=1, multiple_tables=True, stream=True,
            java_options=["-Xmx256m"]
        )
    except Exception as e:
        logger.error("Feil ved lesing av PDF: %s", e)
        raise ValueError(f"Kunne ikke lese PDF: {e}") from e

    if not tables or not isinstance(tables[0], pd.DataFrame):
        raise ValueError("Ingen gyldige tabeller funnet i PDF-en")

    df = tables[0].fillna("")
    if df.empty:
        raise ValueError("Tabellen i PDF-en er tom")

    logger.info("Tabell funnet med %d rader og %d kolonner", len(df), len(df.columns))

    # Valider at tabellen har nok kolonner (forventer minst 3: dag, tom, aktiviteter)
    if len(df.columns) < 3:
        col_preview = df.to_string(max_rows=5, max_cols=10)
        logger.error(
            "Uventet tabellstruktur: kun %d kolonne(r). Forventet minst 3.\nTabellinnhold (5 første rader):\n%s",
            len(df.columns), col_preview
        )
        raise ValueError(
            f"Uventet tabellstruktur: {len(df.columns)} kolonne(r), forventet minst 3. "
            "PDF-malen kan ha endret seg."
        )

    ordered_weekdays = WEEKDAYS
    indices = {day: df[df.iloc[:, 0] == day].index.min() for day in ordered_weekdays}
    found_days = [day for day in ordered_weekdays if pd.notna(indices[day])]

    if not found_days:
        col0_values = df.iloc[:, 0].unique().tolist()[:10]
        logger.error(
            "Ingen ukedager funnet i kolonne 0. Innhold i kolonne 0 (inntil 10 unike): %s",
            col0_values
        )
        raise ValueError(
            f"Ingen ukedager (Mandag–Fredag) funnet i PDF-tabellen. "
            f"Kolonne 0 inneholder: {col0_values}. PDF-malen kan ha endret seg."
        )

    logger.info("Fant ukedager i PDF: %s", found_days)
    last_index = len(df) - 1

    output = {}
    for i, day in enumerate(ordered_weekdays):
        if pd.notna(indices[day]):
            start = max(0, indices[day] - 1)
            next_day_idx = (
                indices[ordered_weekdays[i + 1]]
                if i + 1 < len(ordered_weekdays)
                else None
            )
            end = (next_day_idx - 2) if pd.notna(next_day_idx) else last_index
            end = max(start, end)

            todo_list = df.iloc[start : end + 1, 2].tolist()
            todo_list = [item for item in todo_list if item and str(item).strip()]
            if todo_list:
                output[day] = todo_list

    if not output:
        logger.warning("Parsing fullført, men ingen aktiviteter ble funnet. Ukedager: %s", found_days)

    return output, tables


def extract_pdf_text(file_path: Path) -> str:
    """Leser all tekst fra PDF-en (inkludert overskrifter og tekst utenfor tabeller)."""
    try:
        with pdfplumber.open(str(file_path)) as pdf:
            text = ""
            for page in pdf.pages:
                text += page.extract_text() or ""
            return text
    except Exception as e:
        logger.warning("Kunne ikke lese PDF-tekst med pdfplumber for %s: %s", file_path.name, e, exc_info=True)
        return ""


def extract_week_number(file_path: Path, pdf_tables: list = None, pdf_text: str = None) -> str:
    """Ekstraherer ukenummer fra filnavn eller PDF-innhold.

    Prøver først filnavn (f.eks. 'uke 4.pdf', 'uke4.pdf', 'Ukenytt_uke_5.pdf'),
    deretter søker i PDF-teksten (overskrifter) etter 'Uke XX' mønster.
    """

    logger.info("Ekstraherer ukenummer fra fil: %s", file_path.name)

    # Prøv filnavn først - søk etter "uke" etterfulgt av tall
    filename = file_path.stem.lower()
    match = re.search(r'uke\s*(\d{1,2})', filename)
    if match:
        logger.info("Fant ukenummer i filnavn: %s", match.group(1))
        return match.group(1)

    # Søk i PDF-teksten (overskrifter etc) etter "Uke XX"
    if pdf_text:
        match = re.search(r'[Uu]ke\s*(\d{1,2})', pdf_text)
        if match:
            logger.info("Fant ukenummer i PDF-tekst: %s", match.group(1))
            return match.group(1)

    # Fallback: søk i tabellene
    if pdf_tables:
        for table in pdf_tables:
            if hasattr(table, 'to_string'):
                table_text = table.to_string()
                match = re.search(r'[Uu]ke\s*(\d{1,2})', table_text)
                if match:
                    logger.info("Fant ukenummer i tabell: %s", match.group(1))
                    return match.group(1)

    logger.warning("Kunne ikke finne ukenummer i filnavn '%s' eller PDF-innhold", file_path.name)
    return "0"


def extract_extra_text(pdf_text: str) -> str:
    """Ekstraherer tekst som ikke er del av ukeplan-tabellen (beskjeder, info etc).

    Strategi: finn siste forekomst av en ukedag i teksten og ta alt etter det.
    Fallback: filtrer bort alle linjer som kun inneholder ukedager.
    """
    if not pdf_text:
        return ""

    lines = pdf_text.split('\n')

    # Finn indeksen til den siste linjen som inneholder en ukedag
    last_weekday_line = -1
    for i, line in enumerate(lines):
        line_lower = line.lower().strip()
        if any(day in line_lower for day in WEEKDAYS_LOWER):
            last_weekday_line = i

    if last_weekday_line >= 0:
        # Ta alt etter siste ukedag-linje
        candidate_lines = lines[last_weekday_line + 1:]
    else:
        # Ingen ukedager funnet — filtrer bort kortlinjer som kun er ukedagnavn
        candidate_lines = lines

    extra_lines = [
        line.strip() for line in candidate_lines
        if line.strip() and not any(line.lower().strip() == day for day in WEEKDAYS_LOWER)
    ]

    return '\n'.join(extra_lines).strip()


def save_info_file(child_name: str, info_text: str) -> Path:
    """Lagrer full info-tekst til fil for et barn."""
    info_path = DATA_DIR / f"{_safe_file_name(child_name)}_info.txt"
    info_path.write_text(info_text, encoding="utf-8")
    logger.info("Lagret info-tekst til %s (%d tegn)", info_path, len(info_text))
    return info_path


def _get_sensor_state_path(child_name: str) -> Path:
    """Returnerer stien til JSON-filen som lagrer sensor-state for et barn."""
    return DATA_DIR / f"{_safe_file_name(child_name)}_sensor.json"


def save_sensor_state(child_name: str, payload: dict) -> None:
    """Lagrer sensor-payload til disk for persistens over restarter."""
    state_path = _get_sensor_state_path(child_name)
    state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Lagret sensor-state til %s", state_path)


def load_sensor_state(child_name: str) -> dict | None:
    """Laster lagret sensor-state fra disk."""
    state_path = _get_sensor_state_path(child_name)
    if not state_path.exists():
        return None
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, IOError) as e:
        logger.warning("Kunne ikke laste sensor-state for %s: %s", child_name, e)
        return None


def truncate_text(text: str, max_length: int = 500) -> str:
    """Truncerer tekst til maks lengde med '...' hvis nødvendig."""
    if not text or len(text) <= max_length:
        return text
    return text[:max_length - 3].rsplit(' ', 1)[0] + "..."


DAYS_NO = ["Mandag", "Tirsdag", "Onsdag", "Torsdag", "Fredag", "Lørdag", "Søndag"]
WEEKEND = {"Lørdag", "Søndag"}
OPENEPAPERLINK_WIDTH = 20


def _format_day_plan(ukeplan: dict, day: str) -> str:
    """Returnerer dagens/morgendagens plan som lesbar tekst."""
    if day in WEEKEND:
        return "Ingen skole i helgen"
    plan = ukeplan.get(day, [])
    if plan:
        return "\n".join(plan)[:240]
    return f"Ingen planer for {day}"


def _wordwrap_openepaperlink(text: str, max_width: int = OPENEPAPERLINK_WIDTH) -> str:
    """Bryter tekst til linjer på maks max_width tegn, skilt med '#'."""
    lines = []
    for paragraph in text.split("\n"):
        current = ""
        for word in paragraph.split():
            candidate = f"{current} {word}".strip() if current else word
            if len(candidate) <= max_width:
                current = candidate
            else:
                if current:
                    lines.append(current)
                current = word
        if current:
            lines.append(current)
    return "#".join(lines)


def _post_ha_sensor(url: str, headers: dict, payload: dict, retries: int = 3, delay: float = 2.0) -> bool:
    """Sender POST til HA API med retry ved midlertidige feil."""
    for attempt in range(1, retries + 1):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=10)
            if response.status_code in (200, 201):
                return True
            # 4xx-feil er permanente (feil token, ugyldig payload etc) - ikke retry
            if 400 <= response.status_code < 500:
                logger.error("HA API returnerte %s (permanent feil): %s", response.status_code, response.text)
                return False
            logger.warning(
                "HA API feil %s (forsøk %d/%d): %s",
                response.status_code, attempt, retries, response.text
            )
        except requests.Timeout:
            logger.warning("HA API timeout (forsøk %d/%d)", attempt, retries)
        except requests.RequestException as e:
            logger.warning("HA API nettverksfeil (forsøk %d/%d): %s", attempt, retries, e)

        if attempt < retries:
            time.sleep(delay)

    logger.error("HA API utilgjengelig etter %d forsøk: %s", retries, url)
    return False


def update_home_assistant_sensor(
    child_name: str, data: dict, week_number: str, extra_text: str = ""
) -> bool:
    """Oppdaterer Home Assistant sensor for et barn."""
    sensor_name = f"sensor.{_safe_sensor_name(child_name)}_ukenytt_tabell"
    url = f"{HA_URL}/api/states/{sensor_name}"

    headers = {
        "Authorization": f"Bearer {HA_TOKEN}",
        "Content-Type": "application/json",
    }

    # Lagre full info-tekst til fil uansett lengde (for persistens)
    info_truncated = None
    has_full_info = False
    if extra_text:
        save_info_file(child_name, extra_text)
        if len(extra_text) > MAX_INFO_LENGTH:
            info_truncated = truncate_text(extra_text, MAX_INFO_LENGTH)
            has_full_info = True
        else:
            info_truncated = extra_text

    payload = {
        "state": int(week_number) if week_number.isdigit() else 0,
        "attributes": {
            "barn": child_name,
            "ukeplan": data,
            "info": info_truncated if info_truncated else None,
            "info_full_available": has_full_info,
            "last_updated": datetime.now(tz=timezone.utc).isoformat(),
            "friendly_name": f"{child_name} Ukenytt",
            "icon": "mdi:calendar-week",
        },
    }

    # Fjern None-verdier fra attributter
    payload["attributes"] = {k: v for k, v in payload["attributes"].items() if v is not None}

    if _post_ha_sensor(url, headers, payload):
        logger.info("Sensor '%s' oppdatert med uke %s", sensor_name, week_number)
        save_sensor_state(child_name, payload)
        _update_derived_sensors(child_name, data, headers)
        return True
    return False


def _update_derived_sensors(child_name: str, ukeplan: dict, headers: dict) -> None:
    """Publiserer idag/imorgen-sensorer og OpenEpaperLink-varianter."""
    today_idx = datetime.now().weekday()
    today = DAYS_NO[today_idx]
    tomorrow = DAYS_NO[(today_idx + 1) % 7]

    safe = _safe_sensor_name(child_name)

    sensors = {
        f"sensor.{safe}_ukenytt_idag_addon": {
            "state": _format_day_plan(ukeplan, today),
            "attributes": {
                "dag": today,
                "friendly_name": f"{child_name} Ukenytt i dag",
                "icon": "mdi:calendar-today",
            },
        },
        f"sensor.{safe}_ukenytt_imorgen_addon": {
            "state": _format_day_plan(ukeplan, tomorrow),
            "attributes": {
                "dag": tomorrow,
                "friendly_name": f"{child_name} Ukenytt i morgen",
                "icon": "mdi:calendar-arrow-right",
            },
        },
    }

    # OpenEpaperLink-varianter avledes fra tekst-sensorene
    for base_key, eink_key in [
        (f"sensor.{safe}_ukenytt_idag_addon", f"sensor.{safe}_ukenytt_idag_openepaperlink_addon"),
        (f"sensor.{safe}_ukenytt_imorgen_addon", f"sensor.{safe}_ukenytt_imorgen_openepaperlink_addon"),
    ]:
        wrapped = _wordwrap_openepaperlink(sensors[base_key]["state"])
        sensors[eink_key] = {
            "state": wrapped,
            "attributes": {
                "friendly_name": f"{child_name} Ukenytt {base_key.split('_')[-2]} (OpenEpaperLink)",
                "icon": "mdi:image-text",
            },
        }

    for sensor_name, payload in sensors.items():
        url = f"{HA_URL}/api/states/{sensor_name}"
        if _post_ha_sensor(url, headers, payload, retries=2, delay=0.5):
            logger.info("Avledet sensor '%s' oppdatert", sensor_name)
        else:
            logger.warning("Kunne ikke oppdatere avledet sensor '%s'", sensor_name)


def process_pdf_for_child(
    child_name: str, original_filename: str = None, pdf_override: Path = None
) -> tuple[bool, str]:
    """Prosesserer PDF for et barn og oppdaterer sensor.

    pdf_override: bruk denne filen i stedet for den permanente PDF-stien (ved atomic upload).
    """
    pdf_path = pdf_override or get_pdf_path(child_name)

    if not pdf_path.exists():
        return False, f"Ingen PDF funnet for {child_name}"

    try:
        data, tables = parse_pdf(pdf_path)

        # Les all tekst fra PDF (for overskrift og ekstra info)
        pdf_text = extract_pdf_text(pdf_path)

        # Bruk originalt filnavn for ukenummer hvis tilgjengelig
        effective_filename = original_filename or get_original_filename(child_name)
        if effective_filename:
            week_number = extract_week_number(Path(effective_filename), tables, pdf_text)
        else:
            week_number = extract_week_number(pdf_path, tables, pdf_text)

        # Hent ekstra tekst (beskjeder etc)
        extra_text = extract_extra_text(pdf_text)

        if update_home_assistant_sensor(child_name, data, week_number, extra_text):
            return True, f"Sensor oppdatert for {child_name}, uke {week_number}"
        return False, f"Kunne ikke oppdatere sensor for {child_name}"
    except ValueError as e:
        return False, str(e)


@app.route("/health", methods=["GET"])
def health_check():
    """Helsesjekk-endepunkt."""
    return jsonify({"status": "ok", "children": CHILDREN})


@app.route("/upload", methods=["POST"])
def upload_pdf():
    """
    Mottar PDF-fil via HTTP POST.

    Query-parametere:
        child: Navn på barnet (påkrevd)
        api_key: API-nøkkel for autentisering (påkrevd hvis konfigurert)
    """
    # Sjekk API-nøkkel hvis konfigurert
    if API_KEY:
        provided_key = request.args.get("api_key") or request.headers.get("X-API-Key")
        if provided_key != API_KEY:
            logger.warning("Ugyldig API-nøkkel forsøk fra %s", request.remote_addr)
            return jsonify({"error": "Ugyldig API-nøkkel"}), 401

    # Hent barnenavn
    child_name = request.args.get("child", "").strip()
    if not child_name:
        return jsonify({"error": "Mangler 'child' parameter"}), 400

    # Sjekk at barnet er konfigurert (case-insensitive)
    children_lower = [c.lower() for c in CHILDREN]
    if child_name.lower() not in children_lower:
        return (
            jsonify(
                {
                    "error": f"Ukjent barn: {child_name}",
                    "configured_children": CHILDREN,
                }
            ),
            400,
        )

    # Finn riktig navn med korrekt casing
    child_index = children_lower.index(child_name.lower())
    child_name = CHILDREN[child_index]

    # Sjekk at fil ble sendt
    original_filename = None
    if "file" not in request.files:
        if request.content_type and "pdf" in request.content_type.lower():
            file_data = request.get_data()
        else:
            return (
                jsonify(
                    {
                        "error": "Ingen fil mottatt. Send som 'file' i multipart/form-data eller som raw PDF body"
                    }
                ),
                400,
            )
    else:
        file = request.files["file"]
        if file.filename == "":
            return jsonify({"error": "Ingen fil valgt"}), 400
        original_filename = file.filename
        logger.info("Mottatt fil med originalnavn: %s", original_filename)
        file_data = file.read()

    # Valider filstørrelse
    if len(file_data) > MAX_PDF_SIZE:
        return jsonify({"error": f"Filen er for stor ({len(file_data)} bytes). Maks {MAX_PDF_SIZE} bytes."}), 400

    # Valider at det er en PDF
    if not file_data.startswith(b"%PDF"):
        return jsonify({"error": "Ugyldig filformat - må være PDF"}), 400

    pdf_path = get_pdf_path(child_name)
    old_existed = pdf_path.exists()
    tmp_path = pdf_path.with_suffix(".tmp")

    # Skriv til temp-fil først — beskytt eksisterende PDF ved parsing-feil
    try:
        tmp_path.write_bytes(file_data)
        logger.info("PDF midlertidig lagret for %s: %s", child_name, tmp_path)
    except IOError as e:
        logger.error("Kunne ikke lagre temp-fil: %s", e)
        return jsonify({"error": "Kunne ikke lagre fil"}), 500

    # Prosesser temp-filen (med originalt filnavn for ukenummer)
    success, message = process_pdf_for_child(child_name, original_filename, pdf_override=tmp_path)

    if success:
        # Parsing OK — erstatt eksisterende PDF atomisk
        try:
            tmp_path.replace(pdf_path)
            if original_filename:
                save_original_filename(child_name, original_filename)
            logger.info("PDF aktivert for %s: %s", child_name, pdf_path)
        except IOError as e:
            logger.error("Kunne ikke aktivere PDF: %s", e)
            tmp_path.unlink(missing_ok=True)
            return jsonify({"error": "Kunne ikke aktivere fil etter parsing"}), 500
    else:
        # Parsing feilet — behold forrige PDF, slett temp
        tmp_path.unlink(missing_ok=True)
        logger.warning("PDF forkastet for %s (parsing feilet): %s", child_name, message)

    return (
        jsonify(
            {
                "success": success,
                "message": message,
                "child": child_name,
                "replaced_existing": old_existed,
            }
        ),
        200 if success else 422,
    )


@app.route("/process", methods=["POST"])
def process_existing():
    """
    Prosesserer eksisterende PDF-filer på nytt.

    Query-parametere:
        child: Navn på barnet (valgfritt - prosesserer alle hvis ikke angitt)
        api_key: API-nøkkel for autentisering (påkrevd hvis konfigurert)
    """
    # Sjekk API-nøkkel hvis konfigurert
    if API_KEY:
        provided_key = request.args.get("api_key") or request.headers.get("X-API-Key")
        if provided_key != API_KEY:
            logger.warning("Ugyldig API-nøkkel forsøk på /process fra %s", request.remote_addr)
            return jsonify({"error": "Ugyldig API-nøkkel"}), 401

    child_name = request.args.get("child", "").strip()

    if child_name:
        children_lower = [c.lower() for c in CHILDREN]
        if child_name.lower() not in children_lower:
            return jsonify({"error": f"Ukjent barn: {child_name}"}), 400
        child_index = children_lower.index(child_name.lower())
        children_to_process = [CHILDREN[child_index]]
    else:
        children_to_process = CHILDREN

    results = {}
    for child in children_to_process:
        success, message = process_pdf_for_child(child)
        results[child] = {"success": success, "message": message}

    overall_success = all(r["success"] for r in results.values())
    return (
        jsonify({"success": overall_success, "results": results}),
        200 if overall_success else 207,
    )


@app.route("/status", methods=["GET"])
def status():
    """Viser status for alle barn."""
    status_info = {}
    for child in CHILDREN:
        pdf_path = get_pdf_path(child)
        info_path = DATA_DIR / f"{_safe_file_name(child)}_info.txt"
        state_path = _get_sensor_state_path(child)

        pdf_uploaded_at = None
        if pdf_path.exists():
            mtime = pdf_path.stat().st_mtime
            pdf_uploaded_at = datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()

        original_filename = get_original_filename(child)

        status_info[child] = {
            "has_pdf": pdf_path.exists(),
            "pdf_size": pdf_path.stat().st_size if pdf_path.exists() else None,
            "pdf_uploaded_at": pdf_uploaded_at,
            "original_filename": original_filename,
            "has_info_file": info_path.exists(),
            "has_sensor_state": state_path.exists(),
        }

    return jsonify({"children": status_info, "data_directory": str(DATA_DIR)})


@app.route("/info/<child_name>", methods=["GET"])
def get_info(child_name: str):
    """Henter full info-tekst for et barn."""
    # Finn riktig navn med korrekt casing
    children_lower = [c.lower() for c in CHILDREN]
    if child_name.lower() not in children_lower:
        return jsonify({"error": f"Ukjent barn: {child_name}"}), 404

    child_index = children_lower.index(child_name.lower())
    child_name = CHILDREN[child_index]

    info_path = DATA_DIR / f"{_safe_file_name(child_name)}_info.txt"

    if not info_path.exists():
        return jsonify({"error": f"Ingen info-fil funnet for {child_name}"}), 404

    info_text = info_path.read_text(encoding="utf-8")
    return jsonify({
        "child": child_name,
        "info": info_text,
        "length": len(info_text)
    })


def _refresh_derived_sensors() -> None:
    """Oppdaterer idag/imorgen-sensorer fra lagret state for alle barn."""
    headers = {
        "Authorization": f"Bearer {HA_TOKEN}",
        "Content-Type": "application/json",
    }
    for child in CHILDREN:
        state = load_sensor_state(child)
        if state:
            ukeplan = state.get("attributes", {}).get("ukeplan", {})
            _update_derived_sensors(child, ukeplan, headers)


def _midnight_refresh_loop() -> None:
    """Bakgrunnstråd som oppdaterer idag/imorgen-sensorer ved midnatt."""
    while True:
        now = datetime.now()
        # Sekunder til neste midnatt + 5s buffer
        seconds_until_midnight = (
            (23 - now.hour) * 3600 + (59 - now.minute) * 60 + (60 - now.second) + 5
        )
        time.sleep(seconds_until_midnight)
        logger.info("Midnatt-oppdatering: oppdaterer idag/imorgen-sensorer")
        _refresh_derived_sensors()


def restore_sensor_from_state(child_name: str) -> bool:
    """Gjenoppretter sensor fra lagret JSON-state (raskere enn PDF-reprosessering)."""
    payload = load_sensor_state(child_name)
    if not payload:
        return False

    sensor_name = f"sensor.{_safe_sensor_name(child_name)}_ukenytt_tabell"
    url = f"{HA_URL}/api/states/{sensor_name}"
    headers = {
        "Authorization": f"Bearer {HA_TOKEN}",
        "Content-Type": "application/json",
    }

    if _post_ha_sensor(url, headers, payload, delay=0.5):
        logger.info("Sensor '%s' gjenopprettet fra lagret state", sensor_name)
        ukeplan = payload.get("attributes", {}).get("ukeplan", {})
        _update_derived_sensors(child_name, ukeplan, headers)
        return True
    logger.warning("Kunne ikke gjenopprette sensor '%s' fra lagret state", sensor_name)
    return False


@app.route("/refresh", methods=["POST"])
def refresh_derived():
    """Oppdaterer idag/imorgen-sensorer fra lagret state (uten å reparsere PDF)."""
    if API_KEY:
        provided_key = request.args.get("api_key") or request.headers.get("X-API-Key")
        if provided_key != API_KEY:
            return jsonify({"error": "Ugyldig API-nøkkel"}), 401
    _refresh_derived_sensors()
    return jsonify({"success": True, "message": "Idag/imorgen-sensorer oppdatert"})


def startup_process():
    """Kjører ved oppstart - gjenoppretter sensorer fra lagret state eller reprosesserer PDFer."""
    logger.info("Starter Ukenytt add-on v%s", ADDON_VERSION)
    logger.info("Konfigurerte barn: %s", CHILDREN)
    logger.info("Data-mappe: %s", DATA_DIR)
    logger.info("Home Assistant URL: %s", HA_URL)

    for child in CHILDREN:
        # Prøv rask gjenoppretting fra lagret sensor-state først
        if restore_sensor_from_state(child):
            logger.info("Sensor for %s gjenopprettet fra lagret state", child)
            continue

        # Fallback: reprosesser PDF hvis state-fil mangler
        pdf_path = get_pdf_path(child)
        if pdf_path.exists():
            logger.info("Fant eksisterende PDF for %s, prosesserer...", child)
            success, message = process_pdf_for_child(child)
            logger.info("  -> %s", message)
        else:
            logger.info("Ingen PDF eller lagret state funnet for %s", child)


if __name__ == "__main__":
    startup_process()

    # Start bakgrunnstråd for daglig oppdatering av idag/imorgen-sensorer
    t = threading.Thread(target=_midnight_refresh_loop, daemon=True, name="midnight-refresh")
    t.start()
    logger.info("Midnatt-oppdatering aktivert")

    port = int(os.getenv("PORT", "8099"))
    logger.info("Starter webserver på port %d", port)

    try:
        from waitress import serve

        serve(app, host="0.0.0.0", port=port)
    except ImportError:
        app.run(host="0.0.0.0", port=port, debug=False)
