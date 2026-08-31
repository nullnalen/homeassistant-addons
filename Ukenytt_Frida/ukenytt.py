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

import pdfplumber
import requests
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
ADDON_VERSION = os.getenv("ADDON_VERSION", "1.0.32")

# Konstanter
MAX_INFO_LENGTH = 500
MAX_PDF_SIZE = 10 * 1024 * 1024  # 10 MB
WEEKDAYS = ["Mandag", "Tirsdag", "Onsdag", "Torsdag", "Fredag"]
WEEKDAYS_LOWER = ["mandag", "tirsdag", "onsdag", "torsdag", "fredag", "lørdag", "søndag"]
DAYS_NO = ["Mandag", "Tirsdag", "Onsdag", "Torsdag", "Fredag", "Lørdag", "Søndag"]
WEEKEND = {"Lørdag", "Søndag"}
OPENEPAPERLINK_WIDTH = 20

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


def _format_day_lines(day_data) -> list[str]:
    """Formatterer gammel eller ny dagsstruktur til visningslinjer."""
    if isinstance(day_data, dict):
        lines = []
        if day_data.get("tid"):
            lines.append(day_data["tid"])
        fag = day_data.get("fag") or []
        lekser = day_data.get("lekser") or []
        if fag:
            lines.append("På skolen: " + ", ".join(fag))
        if lekser:
            lines.append("Lekser: " + " ".join(lekser))
        return lines
    if isinstance(day_data, list):
        return day_data
    if day_data:
        return [str(day_data)]
    return []


def _format_ukeplan_for_display(ukeplan: dict | None) -> dict:
    """Lager en enkel dag -> linjer-struktur til HTML og avledede tekstsensorer."""
    if not isinstance(ukeplan, dict):
        return {}
    return {
        day: _format_day_lines(ukeplan.get(day))
        for day in DAYS_NO
        if _format_day_lines(ukeplan.get(day))
    }


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
            <p><code>GET /ukenytt/navn</code> - Hent rik ukeplan, lekser, ord og lærerbrev (JSON)</p>
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
        data["ukeplan"] = _format_ukeplan_for_display(attrs.get("ukeplan"))
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
            "ukenytt": "GET /ukenytt/<child_name>",
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
            "info": "GET /info/<child_name>",
            "ukenytt": "GET /ukenytt/<child_name>"
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


def _extract_time(cell: str) -> str | None:
    """Trekker ut tidspunkt på formen '08.30 – 14.10' fra en celle."""
    parsed = _extract_time_range(cell)
    return parsed["tid"] if parsed else None


def _extract_time_range(cell: str) -> dict | None:
    """Trekker ut samlet tid, start og slutt fra en celle."""
    match = re.search(
        r'(?P<start>\d{1,2}[.:]\d{2})\s*[–\-]\s*(?P<end>\d{1,2}[.:]\d{2})',
        str(cell)
    )
    if not match:
        return None
    start = match.group("start").replace(":", ".")
    end = match.group("end").replace(":", ".")
    return {
        "tid": f"{start} – {end}",
        "start_tid": start,
        "slutt_tid": end,
    }


def _split_items(cell) -> list[str]:
    """Deler celleinnhold (linjeskift eller newline) til rensede enkeltposter."""
    if not cell or not str(cell).strip():
        return []
    return [item.strip() for item in str(cell).split("\n") if item.strip()]



_LEKSE_NOISE = {"God helg!", "God helg", "Lykke til!", "PÅ SKOLEN", "HJEMME"}
_DAY_RE = re.compile(r'(Mandag|Tirsdag|Onsdag|Torsdag|Fredag)\b')
_TIME_RE = re.compile(r'\d{1,2}[.:]\d{2}')


def _find_day_in_row(cells: list[str]) -> tuple[str | None, str | None]:
    """Returnerer (dagsnavn, celle-med-tid-eller-None) fra en rad."""
    for cell in cells:
        m = _DAY_RE.match(cell)
        if m:
            return m.group(1), cell
    return None, None


def _parse_ukeplan_table(rows: list[list]) -> dict:
    """Parser ukeplan fra pdfplumber-rader (tabell 0 på side 1).

    Kirkevoll-malen finnes i to varianter som begge følger samme kolonnestruktur:
      kolonne 4      : fag på skolen (konsekvent i begge varianter)
      kolonne 6 eller 7: hjemmelekser (lengste tekstcell til høyre for fag)

    Dagsnavn kan stå i kolonne 0 (med tidspunkt på samme linje) eller kolonne 1
    (med tidspunkt på neste rad i samme kolonne). Vi søker dagsnavn i alle celler.
    Fag og lekser akkumuleres per dag over alle påfølgende rader frem til neste dag.
    """
    output = {}
    current_day = None

    # Første pass: finn hvilken kolonne fag konsekvent sitter i.
    # Vi teller hvilken kolonne som oftest inneholder korte tekster som ikke er dag/tid/lekse.
    col_scores = {}
    for row in rows:
        cells = [(c or "").strip() for c in row]
        for ci, val in enumerate(cells):
            if not val or _DAY_RE.search(val) or _TIME_RE.search(val):
                continue
            lines = [l for l in _split_items(val) if l and len(l) < 40 and l not in _LEKSE_NOISE]
            if lines:
                col_scores[ci] = col_scores.get(ci, 0) + len(lines)

    # Fag-kolonnen er den med høyest score ekskl. de siste kolonnene (lekser)
    n_cols = max((len(r) for r in rows), default=9)
    lekse_start_col = max(n_cols - 3, 2)
    fag_col = max(
        (ci for ci in col_scores if ci < lekse_start_col),
        key=lambda ci: col_scores[ci],
        default=4
    )
    logger.info("Fag-kolonne bestemt til: %d (scores: %s)", fag_col, col_scores)

    # Normaliser alle rader én gang
    norm_rows = [[(c or "").strip() for c in row] for row in rows]

    def _extract_fag(cells):
        result = []
        if fag_col < len(cells):
            for item in _split_items(cells[fag_col]):
                if (item and item not in WEEKDAYS and not _TIME_RE.search(item)
                        and item not in _LEKSE_NOISE and len(item) < 40):
                    result.append(item)
        return result

    def _extract_lekser(cells):
        # Kolonne 6 (fag_col+2) inneholder alltid utruncert tekst i Kirkevoll-malen.
        # Newlines i kolonne 6 er linjeskift inni én sammenhengende lekse-celle — join til én streng.
        preferred_col = fag_col + 2
        if preferred_col < len(cells) and cells[preferred_col] and not _DAY_RE.search(cells[preferred_col]) and not _TIME_RE.search(cells[preferred_col]):
            raw = cells[preferred_col]
            # Slå linjeskift inni cellen sammen til ett mellomrom, deretter splitt på linjeskift
            # som skiller faktisk separate lekser (i Frida-varianten kan cellen ha "Lekse1\nLekse2")
            # Vi antar at en ny lekse starter med stor bokstav og forrige avsluttes med punktum.
            lines = [l.strip() for l in raw.split("\n") if l.strip()]
            merged = []
            for line in lines:
                if line in _LEKSE_NOISE:
                    continue
                if (merged and not merged[-1].endswith((".", "!", "?"))
                        and not re.match(r'^[A-ZÆØÅ][a-zæøå]+:', line)):
                    merged[-1] = merged[-1] + " " + line
                else:
                    merged.append(line)
            if merged:
                return merged
        lekse_text = ""
        for ci in range(fag_col + 1, len(cells)):
            val = cells[ci]
            if val and len(val) > len(lekse_text) and not _DAY_RE.search(val) and not _TIME_RE.search(val):
                lekse_text = val
        return [item for item in _split_items(lekse_text) if item and item not in _LEKSE_NOISE]

    # Detect PDF-variant: Henrik-varianten har dagsnavn i kolonne 1 (col0 er tom på dag-raden).
    # Frida-varianten har dag+tid i kolonne 0.
    # Vi teller hvor mange dag-rader som har dagsnavn KUN i kolonne 1 (ikke kolonne 0).
    day_in_col1_only = sum(
        1 for cells in norm_rows
        if _DAY_RE.match(cells[1] if len(cells) > 1 else "")
        and not _DAY_RE.match(cells[0] if cells else "")
    )
    is_henrik_variant = day_in_col1_only >= 2
    logger.info("PDF-variant: %s (dag-i-col1-only: %d)", "Henrik" if is_henrik_variant else "Frida", day_in_col1_only)

    current_day = None
    buffered_fag: list[str] = []
    buffered_lekser: list[str] = []

    for i, cells in enumerate(norm_rows):
        day, day_cell = _find_day_in_row(cells)

        if day:
            if day not in output:
                # Første gang vi ser denne dagen — initialiser
                current_day = day
                tid_data = _extract_time_range(day_cell) or {"tid": None, "start_tid": None, "slutt_tid": None}
                output[current_day] = {**tid_data, "fag": list(buffered_fag), "lekser": list(buffered_lekser)}
                buffered_fag.clear()
                buffered_lekser.clear()
            else:
                # Duplikat-dag-rad (f.eks. col1 gjentar dagsnavn) — bare akkumler
                current_day = day
            for item in _extract_fag(cells):
                if item not in output[current_day]["fag"]:
                    output[current_day]["fag"].append(item)
            lekser = output[current_day]["lekser"]
            prev_trunc = bool(lekser and not lekser[-1].endswith((".", "!", "?")))
            for item in _extract_lekser(cells):
                if item in _LEKSE_NOISE:
                    continue
                is_new_cat = bool(re.match(r'^[A-ZÆØÅ][a-zæøå]+:', item))
                if prev_trunc and not is_new_cat and not any(item in l for l in lekser):
                    lekser[-1] = lekser[-1] + " " + item
                elif item not in lekser and not any(item in l for l in lekser):
                    lekser.append(item)
                prev_trunc = bool(lekser and not lekser[-1].endswith((".", "!", "?")))
            continue

        # Ingen dag funnet på denne raden
        if is_henrik_variant:
            # Henrik-variant: rader uten dagsnavn som kommer FØR neste dag-rad tilhører
            # den kommende dagen. Vi akkumulerer over flere slike rader (lekser kan strekke seg over 3-4 rader).
            next_has_day = (
                i + 1 < len(norm_rows)
                and _find_day_in_row(norm_rows[i + 1])[0] is not None
            )
            if next_has_day:
                buffered_fag = _extract_fag(cells)
                buffered_lekser = _extract_lekser(cells)
                continue
            # Rader som tilhører en kommende dag men ikke er direkte før den — akkumler i buffer
            # Sjekk om vi er i en sekvens av rader som buffer mot neste dag
            future_has_day = any(
                _find_day_in_row(norm_rows[j])[0] is not None
                for j in range(i + 1, min(i + 6, len(norm_rows)))
            )
            if future_has_day and current_day is None:
                for item in _extract_fag(cells):
                    if item not in buffered_fag:
                        buffered_fag.append(item)
                for item in _extract_lekser(cells):
                    if item not in buffered_lekser:
                        buffered_lekser.append(item)
                continue

        if current_day is None:
            continue

        # Plukk opp tid fra rader uten dagsnavn der en celle kun inneholder tidspunkt
        if output[current_day]["tid"] is None:
            for cell in cells:
                if cell and _TIME_RE.search(cell) and not _DAY_RE.search(cell) and len(cell) < 20:
                    tid_data = _extract_time_range(cell)
                    if tid_data:
                        output[current_day].update(tid_data)
                        break

        # Akkumler fag og lekser for current_day
        new_fag = _extract_fag(cells)
        for item in new_fag:
            if item not in output[current_day]["fag"]:
                output[current_day]["fag"].append(item)
        lekser = output[current_day]["lekser"]
        new_lekser = _extract_lekser(cells)
        # Detekter tekstfortsettelsesrader: siste lekse er truncert (slutter ikke med . ! ?)
        # og ny item er ikke en ny selvstendig kategori (starter med "Norsk:", "Matte:" e.l.)
        prev_lekse_truncated = bool(lekser and not lekser[-1].endswith((".", "!", "?")))
        for item in new_lekser:
            if item in _LEKSE_NOISE:
                continue
            is_new_category = bool(re.match(r'^[A-ZÆØÅ][a-zæøå]+:', item))
            # Slå sammen hvis forrige lekse er truncert og item ikke er en ny kategori.
            # Gjelder uavhengig av om raden også har fag (Henrik-varianten har fag på continuation-rader).
            if (prev_lekse_truncated and len(new_lekser) == 1 and not is_new_category):
                lekser[-1] = lekser[-1] + " " + item
            elif item not in lekser and not any(item in l for l in lekser):
                lekser.append(item)
            prev_lekse_truncated = bool(lekser and not lekser[-1].endswith((".", "!", "?")))

    # Fredag-lekser flyttes til torsdag: lekser som skal gjøres hjemme og leveres fredag
    # må være ferdige senest torsdag kveld.
    if "Fredag" in output and "Torsdag" in output:
        fredag_lekser = output["Fredag"].get("lekser", [])
        if fredag_lekser:
            torsdag_lekser = output["Torsdag"].setdefault("lekser", [])
            for item in fredag_lekser:
                if item not in torsdag_lekser and not any(item in l for l in torsdag_lekser):
                    torsdag_lekser.append(item)
            output["Fredag"]["lekser"] = []

    return output


def _extract_emoji_map(page) -> dict[float, str]:
    """Leser emoji-ikoner fra PDF-siden ved å analysere SegoeUIEmoji-chars og farger.

    Kirkevoll-malen bruker to emoji-varianter som begge rendres med SegoeUIEmoji-font:
    - 🌜 halvmåne: har en lilla/mørk farge (R<0.3, B>0.2) blant sine tegn-farger
    - ☀️ sol: kun oransje/gule farger (R>0.8, G>0.4, B<0.3)

    Returnerer dict av {top_posisjon: emoji_streng} for emoji-chars i lekse-kolonnen (x0 > 195).
    """
    emoji_by_top: dict[float, list] = {}
    for c in page.chars:
        if "Emoji" not in c.get("fontname", ""):
            continue
        if c.get("x0", 0) < 195:
            continue
        top = round(c["top"])
        color = c.get("non_stroking_color") or c.get("stroking_color") or (0, 0, 0)
        emoji_by_top.setdefault(top, []).append(color)

    result = {}
    for top, colors in emoji_by_top.items():
        has_purple = any(
            len(c) == 3 and c[0] < 0.4 and c[2] > 0.15
            for c in colors
        )
        result[top] = "🌜" if has_purple else "☀️"

    logger.info("Emoji-kart fra PDF: %s", result)
    return result


def _apply_emoji_to_ukeplan(ukeplan: dict, page) -> dict:
    """Setter inn emoji-prefiks på lekser som pdfplumber ikke klarte å lese.

    Bruker y-posisjoner fra emoji-kartet og ord-posisjoner fra siden til å matche
    hvilken lekse-linje som har et emoji-ikon foran seg.
    """
    emoji_map = _extract_emoji_map(page)
    if not emoji_map:
        return ukeplan

    # Bygg map: top-posisjon -> hele linjetekst (for lekse-kolonnen, x0 > 195)
    words = page.extract_words(x_tolerance=3, y_tolerance=3)
    line_text: dict[int, str] = {}  # top -> samlet tekst på den linjen
    for w in words:
        if w["x0"] < 195:
            continue
        text = w["text"].strip()
        if not text or text in ("HJEMME", "PÅ SKOLEN"):
            continue
        top = round(w["top"])
        line_text[top] = (line_text.get(top, "") + " " + text).strip()

    # For hvert emoji-ikon, finn leksen med nærmeste top-posisjon (innen 8 px)
    # Hopp over lekser som allerede starter med emoji (Unicode-emojier fra pdfplumber)
    emoji_chars = set("🌜☀️🌛🌙⭐🌟💫✨🔥")
    for day_data in ukeplan.values():
        lekser = day_data.get("lekser", [])
        for idx, lekse in enumerate(lekser):
            if lekse and lekse[0] in emoji_chars:
                continue  # allerede har emoji
            # Finn top-posisjon ved å matche lekseteksten mot linje-tekst
            clean_lekse = lekse.strip()
            lekse_top = next(
                (top for top, line in line_text.items()
                 if clean_lekse[:20] in line or line[:20] in clean_lekse[:25]),
                None
            )
            if lekse_top is None:
                continue
            closest = min(emoji_map.keys(), key=lambda t: abs(t - lekse_top), default=None)
            if closest is not None and abs(closest - lekse_top) < 8:
                lekser[idx] = emoji_map[closest] + lekse

    return ukeplan


def parse_pdf(file_path: Path) -> tuple[dict, list]:
    """Parser PDF med pdfplumber og returnerer rik ukeplan-dict.

    Returnert dict har formen:
      {
        "Mandag": {
          "tid": "08.30 – 14.10",
          "start_tid": "08.30",
          "slutt_tid": "14.10",
          "fag": ["Lek", "Norsk", ...],
          "lekser": ["Les s. 14-15 ...", ...]
        },
        ...
      }
    """
    logger.info("Parser PDF: %s", file_path)

    try:
        with pdfplumber.open(str(file_path)) as pdf:
            if not pdf.pages:
                raise ValueError("PDF-en er tom")
            page1 = pdf.pages[0]
            tables = page1.extract_tables()

            if not tables:
                raise ValueError("Ingen tabeller funnet i PDF-en")

            ukeplan_rows = tables[0]
            logger.info("Ukeplan-tabell: %d rader, %d kolonner", len(ukeplan_rows), len(ukeplan_rows[0]) if ukeplan_rows else 0)

            output = _parse_ukeplan_table(ukeplan_rows)

            if not output:
                raise ValueError("Ingen ukedager funnet i PDF-tabellen. PDF-malen kan ha endret seg.")

            # Legg til emoji-prefiks på lekser (🌜/☀️) som pdfplumber ikke klarte å lese som tekst
            output = _apply_emoji_to_ukeplan(output, page1)

    except ValueError:
        raise
    except Exception as e:
        logger.error("Feil ved lesing av PDF: %s", e)
        raise ValueError(f"Kunne ikke lese PDF: {e}") from e

    logger.info("Fant ukedager: %s", list(output.keys()))
    return output, tables


def extract_pdf_text(file_path: Path) -> str:
    """Leser all tekst fra PDF-en (inkludert overskrifter og tekst utenfor tabeller)."""
    return "\n".join(extract_pdf_page_texts(file_path))


def extract_pdf_page_texts(file_path: Path) -> list[str]:
    """Leser tekst fra PDF-en side for side."""
    try:
        with pdfplumber.open(str(file_path)) as pdf:
            return [page.extract_text() or "" for page in pdf.pages]
    except Exception as e:
        logger.warning("Kunne ikke lese PDF-tekst med pdfplumber for %s: %s", file_path.name, e, exc_info=True)
        return []


def _clean_section_lines(text: str) -> list[str]:
    """Returnerer ikke-tomme, trimmede linjer."""
    return [line.strip() for line in text.splitlines() if line.strip()]


def _extract_between_markers(text: str, start_pattern: str, end_patterns: list[str]) -> str:
    """Henter tekst mellom en startmarkør og første etterfølgende sluttmarkør."""
    start = re.search(start_pattern, text, flags=re.IGNORECASE)
    if not start:
        return ""

    section = text[start.end():]
    end_positions = []
    for pattern in end_patterns:
        end = re.search(pattern, section, flags=re.IGNORECASE)
        if end:
            end_positions.append(end.start())

    if end_positions:
        section = section[:min(end_positions)]
    return section.strip()


def extract_learning_goals(page1_text: str) -> dict[str, list[str]]:
    """Parser FAG/TEMA -> LÆRINGSMÅL fra første side."""
    section = _extract_between_markers(page1_text, r'FAG/TEMA\s+LÆRINGSMÅL', [r'\bØVEORD\b'])
    if not section:
        return {}

    lines = [
        line for line in _clean_section_lines(section)
        if "noen mål jobbes" not in line.lower()
    ]

    goals = {}
    current_subject = None
    subject_pattern = re.compile(r'^(NORSK|MATEMATIKK|SOSIAL KOMPETANSE|ENGELSK|NATURFAG|SAMFUNNSFAG|KRLE|MUSIKK)\b\s*(.*)$', re.IGNORECASE)

    for line in lines:
        match = subject_pattern.match(line)
        if match:
            current_subject = match.group(1).upper()
            goals.setdefault(current_subject, [])
            remainder = match.group(2).strip()
            if remainder:
                goals[current_subject].append(remainder)
            continue

        if current_subject:
            if line.startswith("Å ") or line.startswith("å "):
                goals[current_subject].append(line)
            elif goals[current_subject]:
                goals[current_subject][-1] = f"{goals[current_subject][-1]} {line}"
            else:
                goals[current_subject].append(line)

    return {subject: items for subject, items in goals.items() if items}


def extract_word_sections(page1_text: str) -> tuple[list[str], list[dict[str, str]]]:
    """Parser øveord og gloser fra første side."""
    section = _extract_between_markers(page1_text, r'\bØVEORD\s+GLOSER\b', [])
    if not section:
        return [], []

    practice_words = []
    glossary = []

    for line in _clean_section_lines(section):
        parts = re.split(r'\s+[–-]\s+', line, maxsplit=1)
        if len(parts) == 2:
            left, right = parts[0].strip(), parts[1].strip()
            left_words = left.split()
            if len(left_words) > 1:
                practice_words.extend(left_words[:-1])
                left = left_words[-1]
            glossary.append({"engelsk": left, "norsk": right})
        else:
            practice_words.extend(line.split())

    return practice_words, glossary


def extract_teacher_letter(page_texts: list[str]) -> str:
    """Returnerer fritekstbrevet fra lærerne, normalt hele side 2."""
    if len(page_texts) < 2:
        return ""
    return page_texts[1].strip()


def extract_ukenytt_content(
    file_path: Path, ukeplan: dict, week_number: str, pdf_text: str, page_texts: list[str],
    source_filename: str | None = None
) -> dict:
    """Bygger rik JSON-struktur for /ukenytt-endepunktet."""
    page1_text = page_texts[0] if page_texts else pdf_text
    oveord, gloser = extract_word_sections(page1_text)

    return {
        "week": int(week_number) if str(week_number).isdigit() else week_number,
        "source_file": source_filename or file_path.name,
        "ukeplan": ukeplan,
        "laeringsmaal": extract_learning_goals(page1_text),
        "oveord": oveord,
        "gloser": gloser,
        "laererbrev": extract_teacher_letter(page_texts),
        "last_updated": datetime.now(tz=timezone.utc).isoformat(),
    }


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


def _get_ukenytt_data_path(child_name: str) -> Path:
    """Returnerer stien til rik ukenytt-JSON for et barn."""
    return DATA_DIR / f"{_safe_file_name(child_name)}_ukenytt.json"


def save_ukenytt_data(child_name: str, payload: dict) -> None:
    """Lagrer rik ukenytt-struktur til disk."""
    path = _get_ukenytt_data_path(child_name)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Lagret rik ukenytt-data til %s", path)


def load_ukenytt_data(child_name: str) -> dict | None:
    """Laster rik ukenytt-struktur fra disk."""
    path = _get_ukenytt_data_path(child_name)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, IOError) as e:
        logger.warning("Kunne ikke laste rik ukenytt-data for %s: %s", child_name, e)
        return None


def _sensor_state_has_ukenytt_data(child_name: str) -> bool:
    """Returnerer om sensor-state har nok data til /ukenytt fallback-respons."""
    state = load_sensor_state(child_name)
    if not state:
        return False
    attrs = state.get("attributes", {})
    return bool(attrs.get("ukeplan"))


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


def _format_day_plan(ukeplan: dict, day: str) -> str:
    """Returnerer dagens/morgendagens plan som lesbar tekst."""
    if day in WEEKEND:
        return "Ingen skole i helgen"
    lines = _format_day_lines(ukeplan.get(day))
    if lines:
        return "\n".join(lines)[:240]
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
    child_name: str, data: dict, week_number: str, extra_text: str = "", rich_data: dict | None = None
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
            "laeringsmaal": rich_data.get("laeringsmaal") if rich_data else None,
            "oveord": rich_data.get("oveord") if rich_data else None,
            "gloser": rich_data.get("gloser") if rich_data else None,
            "laererbrev": truncate_text(rich_data.get("laererbrev"), MAX_INFO_LENGTH) if rich_data else None,
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


def build_ukenytt_data_from_pdf(
    child_name: str, original_filename: str = None, pdf_override: Path = None
) -> dict:
    """Parser PDF og bygger rik ukenytt-data uten å oppdatere HA-sensorer."""
    pdf_path = pdf_override or get_pdf_path(child_name)
    if not pdf_path.exists():
        raise ValueError(f"Ingen PDF funnet for {child_name}")

    data, tables = parse_pdf(pdf_path)
    page_texts = extract_pdf_page_texts(pdf_path)
    pdf_text = "\n".join(page_texts)

    effective_filename = original_filename or get_original_filename(child_name)
    if effective_filename:
        week_number = extract_week_number(Path(effective_filename), tables, pdf_text)
    else:
        week_number = extract_week_number(pdf_path, tables, pdf_text)

    return extract_ukenytt_content(
        pdf_path, data, week_number, pdf_text, page_texts, effective_filename
    )


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
        rich_data = build_ukenytt_data_from_pdf(child_name, original_filename, pdf_path)
        data = rich_data["ukeplan"]
        week_number = str(rich_data["week"])
        page_texts = extract_pdf_page_texts(pdf_path)
        pdf_text = "\n".join(page_texts)
        extra_text = rich_data.get("laererbrev") or extract_extra_text(pdf_text)

        if update_home_assistant_sensor(child_name, data, week_number, extra_text, rich_data):
            save_ukenytt_data(child_name, rich_data)
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
        ukenytt_path = _get_ukenytt_data_path(child)

        pdf_uploaded_at = None
        if pdf_path.exists():
            mtime = pdf_path.stat().st_mtime
            pdf_uploaded_at = datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()

        original_filename = get_original_filename(child)
        has_ukenytt_json = ukenytt_path.exists()
        has_ukenytt_fallback = (not has_ukenytt_json) and _sensor_state_has_ukenytt_data(child)

        status_info[child] = {
            "has_pdf": pdf_path.exists(),
            "pdf_size": pdf_path.stat().st_size if pdf_path.exists() else None,
            "pdf_uploaded_at": pdf_uploaded_at,
            "original_filename": original_filename,
            "has_info_file": info_path.exists(),
            "has_sensor_state": state_path.exists(),
            "has_ukenytt_data": has_ukenytt_json or has_ukenytt_fallback,
            "has_ukenytt_json": has_ukenytt_json,
            "has_ukenytt_fallback": has_ukenytt_fallback,
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


@app.route("/ukenytt/<child_name>", methods=["GET"])
def get_ukenytt(child_name: str):
    """Henter rik ukenytt-struktur for et barn."""
    children_lower = [c.lower() for c in CHILDREN]
    if child_name.lower() not in children_lower:
        return jsonify({"error": f"Ukjent barn: {child_name}"}), 404

    child_index = children_lower.index(child_name.lower())
    child_name = CHILDREN[child_index]

    data = load_ukenytt_data(child_name)
    if not data:
        pdf_path = get_pdf_path(child_name)
        if pdf_path.exists():
            try:
                data = build_ukenytt_data_from_pdf(child_name)
                save_ukenytt_data(child_name, data)
            except ValueError as e:
                logger.warning("Kunne ikke bygge rik ukenytt-data for %s fra PDF: %s", child_name, e)

    if not data:
        state = load_sensor_state(child_name)
        if state:
            attrs = state.get("attributes", {})
            data = {
                "week": state.get("state"),
                "ukeplan": attrs.get("ukeplan", {}),
                "laeringsmaal": attrs.get("laeringsmaal", {}),
                "oveord": attrs.get("oveord", []),
                "gloser": attrs.get("gloser", []),
                "laererbrev": attrs.get("laererbrev") or attrs.get("info", ""),
                "last_updated": attrs.get("last_updated"),
            }

    if not data:
        return jsonify({"error": f"Ingen ukenytt-data funnet for {child_name}"}), 404

    response = {"child": child_name}
    response.update(data)
    return jsonify(response)


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
