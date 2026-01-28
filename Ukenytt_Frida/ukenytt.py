#!/usr/bin/env python3
"""
Ukenytt Add-on for Home Assistant.

Mottar ukenytt-PDF-filer via HTTP og konverterer dem til Home Assistant sensorer.
"""

import json
import logging
import os
import sys
from pathlib import Path

import pandas as pd
import requests
import tabula
from flask import Flask, jsonify, request

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
_children_json = os.getenv("UKENYTT_CHILDREN", '[{"name": "Barn1"}]')
try:
    _children_list = json.loads(_children_json) if _children_json else []
    CHILDREN = [child.get("name", "Barn") for child in _children_list]
except (json.JSONDecodeError, TypeError):
    CHILDREN = ["Barn1"]

# Mappe for lagring av PDF-filer
DATA_DIR = Path("/data/ukenytt")
DATA_DIR.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)


@app.route("/", methods=["GET"])
def index():
    """Root-endepunkt - viser status."""
    return jsonify({
        "addon": "Ukenytt",
        "status": "running",
        "children": CHILDREN,
        "endpoints": {
            "upload": "POST /upload?child=<name>",
            "health": "GET /health",
            "status": "GET /status",
            "process": "POST /process",
            "info": "GET /info/<child_name>"
        }
    })


def get_pdf_path(child_name: str) -> Path:
    """Returnerer stien til PDF-filen for et barn."""
    safe_name = "".join(c for c in child_name.lower() if c.isalnum() or c in "-_")
    return DATA_DIR / f"{safe_name}.pdf"


def parse_pdf(file_path: Path) -> tuple[dict, list]:
    """Parser PDF og returnerer ukeplan som dictionary og rå tabeller."""
    logger.info("Parser PDF: %s", file_path)

    pd.options.mode.chained_assignment = None

    try:
        # Bruk java_options for å unngå JPype (subprocess-modus)
        tables = tabula.read_pdf(
            str(file_path), pages=1, multiple_tables=True, stream=True,
            java_options=None  # Tvinger subprocess-modus
        )
    except Exception as e:
        logger.error("Feil ved lesing av PDF: %s", e)
        raise ValueError(f"Kunne ikke lese PDF: {e}") from e

    if not tables or not isinstance(tables[0], pd.DataFrame):
        raise ValueError("Ingen gyldige tabeller funnet i PDF-en")

    df = tables[0].fillna("")
    if df.empty:
        raise ValueError("Tabellen i PDF-en er tom")

    ordered_weekdays = ["Mandag", "Tirsdag", "Onsdag", "Torsdag", "Fredag"]
    indices = {day: df[df.iloc[:, 0] == day].index.min() for day in ordered_weekdays}
    last_index = len(df) - 1

    output = {}
    for i, day in enumerate(ordered_weekdays):
        if pd.notna(indices[day]):
            start = indices[day] - 1
            next_day_idx = (
                indices[ordered_weekdays[i + 1]]
                if i + 1 < len(ordered_weekdays)
                else None
            )
            end = (next_day_idx - 2) if pd.notna(next_day_idx) else last_index

            todo_list = df.iloc[start : end + 1, 2].tolist()
            todo_list = [item for item in todo_list if item and str(item).strip()]
            if todo_list:
                output[day] = todo_list

    return output, tables


def extract_pdf_text(file_path: Path) -> str:
    """Leser all tekst fra PDF-en (inkludert overskrifter og tekst utenfor tabeller)."""
    try:
        import pdfplumber
        with pdfplumber.open(str(file_path)) as pdf:
            text = ""
            for page in pdf.pages:
                text += page.extract_text() or ""
            return text
    except Exception as e:
        logger.warning("Kunne ikke lese PDF-tekst med pdfplumber: %s", e)
        return ""


def extract_week_number(file_path: Path, pdf_tables: list = None, pdf_text: str = None) -> str:
    """Ekstraherer ukenummer fra filnavn eller PDF-innhold.

    Prøver først filnavn (f.eks. 'uke 4.pdf', 'uke4.pdf', 'Ukenytt_uke_5.pdf'),
    deretter søker i PDF-teksten (overskrifter) etter 'Uke XX' mønster.
    """
    import re

    logger.info("Ekstraherer ukenummer fra fil: %s", file_path.name)

    # Prøv filnavn først - søk etter "uke" etterfulgt av tall
    filename = file_path.stem.lower()
    match = re.search(r'uke\s*(\d{1,2})', filename)
    if match:
        logger.info("Fant ukenummer i filnavn: %s", match.group(1))
        return match.group(1)

    # Fallback: bare tall i filnavnet
    digits = "".join(filter(str.isdigit, filename))
    if digits:
        week = digits.lstrip('0') or '0'
        logger.info("Fant tall i filnavn: %s", week)
        return week

    # Søk i PDF-teksten (overskrifter etc) etter "Uke XX"
    if pdf_text:
        match = re.search(r'[Uu]ke\s*(\d{1,2})', pdf_text)
        if match:
            logger.info("Fant ukenummer i PDF-tekst: %s", match.group(1))
            return match.group(1)

    # Fallback: søk i tabellene
    if pdf_tables:
        for i, table in enumerate(pdf_tables):
            if hasattr(table, 'to_string'):
                table_text = table.to_string()
                match = re.search(r'[Uu]ke\s*(\d{1,2})', table_text)
                if match:
                    logger.info("Fant ukenummer i tabell: %s", match.group(1))
                    return match.group(1)

    logger.warning("Kunne ikke finne ukenummer")
    return "0"


def extract_extra_text(pdf_text: str) -> str:
    """Ekstraherer tekst som kommer etter tabellen (informasjon, beskjeder etc)."""
    import re

    if not pdf_text:
        return ""

    # Fjern ukeplan-delen (linjer med ukedager)
    lines = pdf_text.split('\n')
    extra_lines = []
    past_table = False

    weekdays = ['mandag', 'tirsdag', 'onsdag', 'torsdag', 'fredag', 'lørdag', 'søndag']

    for line in lines:
        line_lower = line.lower().strip()

        # Sjekk om vi er forbi tabellen (etter fredag)
        if 'fredag' in line_lower:
            past_table = True
            continue

        if past_table and line.strip():
            # Hopp over linjer som bare er ukedager
            if not any(day in line_lower for day in weekdays):
                extra_lines.append(line.strip())

    extra_text = '\n'.join(extra_lines).strip()
    return extra_text


def save_info_file(child_name: str, info_text: str) -> Path:
    """Lagrer full info-tekst til fil for et barn."""
    safe_name = "".join(c for c in child_name.lower() if c.isalnum() or c in "-_")
    info_path = DATA_DIR / f"{safe_name}_info.txt"
    info_path.write_text(info_text, encoding="utf-8")
    logger.info("Lagret info-tekst til %s (%d tegn)", info_path, len(info_text))
    return info_path


def truncate_text(text: str, max_length: int = 500) -> str:
    """Truncerer tekst til maks lengde med '...' hvis nødvendig."""
    if not text or len(text) <= max_length:
        return text
    return text[:max_length - 3].rsplit(' ', 1)[0] + "..."


def update_home_assistant_sensor(
    child_name: str, data: dict, week_number: str, extra_text: str = ""
) -> bool:
    """Oppdaterer Home Assistant sensor for et barn."""
    safe_name = "".join(c for c in child_name.lower() if c.isalnum() or c in "_")
    sensor_name = f"sensor.{safe_name}_ukenytt_tabell"
    url = f"{HA_URL}/api/states/{sensor_name}"

    headers = {
        "Authorization": f"Bearer {HA_TOKEN}",
        "Content-Type": "application/json",
    }

    # Lagre full info-tekst til fil hvis den er lang
    info_truncated = None
    has_full_info = False
    if extra_text:
        if len(extra_text) > 500:
            # Lagre full tekst til fil
            save_info_file(child_name, extra_text)
            info_truncated = truncate_text(extra_text, 500)
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
            "friendly_name": f"{child_name} Ukenytt",
            "icon": "mdi:calendar-week",
        },
    }

    # Fjern None-verdier fra attributter
    payload["attributes"] = {k: v for k, v in payload["attributes"].items() if v is not None}

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        if response.status_code in (200, 201):
            logger.info("Sensor '%s' oppdatert med uke %s", sensor_name, week_number)
            return True
        logger.error(
            "Feil ved oppdatering av sensor: %s - %s",
            response.status_code,
            response.text,
        )
        return False
    except requests.RequestException as e:
        logger.error("Nettverksfeil ved oppdatering av sensor: %s", e)
        return False


def process_pdf_for_child(child_name: str, original_filename: str = None) -> tuple[bool, str]:
    """Prosesserer PDF for et barn og oppdaterer sensor."""
    pdf_path = get_pdf_path(child_name)

    if not pdf_path.exists():
        return False, f"Ingen PDF funnet for {child_name}"

    try:
        data, tables = parse_pdf(pdf_path)

        # Les all tekst fra PDF (for overskrift og ekstra info)
        pdf_text = extract_pdf_text(pdf_path)

        # Bruk originalt filnavn for ukenummer hvis tilgjengelig
        if original_filename:
            from pathlib import Path as P
            week_number = extract_week_number(P(original_filename), tables, pdf_text)
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

    # Valider at det er en PDF
    if not file_data.startswith(b"%PDF"):
        return jsonify({"error": "Ugyldig filformat - må være PDF"}), 400

    # Lagre filen (overskriver eksisterende)
    pdf_path = get_pdf_path(child_name)
    old_existed = pdf_path.exists()

    try:
        pdf_path.write_bytes(file_data)
        logger.info("PDF lagret for %s: %s", child_name, pdf_path)
    except IOError as e:
        logger.error("Kunne ikke lagre fil: %s", e)
        return jsonify({"error": "Kunne ikke lagre fil"}), 500

    # Prosesser PDF og oppdater sensor (med originalt filnavn for ukenummer)
    success, message = process_pdf_for_child(child_name, original_filename)

    return (
        jsonify(
            {
                "success": success,
                "message": message,
                "child": child_name,
                "replaced_existing": old_existed,
            }
        ),
        200 if success else 500,
    )


@app.route("/process", methods=["POST"])
def process_existing():
    """
    Prosesserer eksisterende PDF-filer på nytt.

    Query-parametere:
        child: Navn på barnet (valgfritt - prosesserer alle hvis ikke angitt)
    """
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
        safe_name = "".join(c for c in child.lower() if c.isalnum() or c in "-_")
        info_path = DATA_DIR / f"{safe_name}_info.txt"
        status_info[child] = {
            "has_pdf": pdf_path.exists(),
            "pdf_size": pdf_path.stat().st_size if pdf_path.exists() else None,
            "has_info_file": info_path.exists(),
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

    safe_name = "".join(c for c in child_name.lower() if c.isalnum() or c in "-_")
    info_path = DATA_DIR / f"{safe_name}_info.txt"

    if not info_path.exists():
        return jsonify({"error": f"Ingen info-fil funnet for {child_name}"}), 404

    info_text = info_path.read_text(encoding="utf-8")
    return jsonify({
        "child": child_name,
        "info": info_text,
        "length": len(info_text)
    })


def startup_process():
    """Kjører ved oppstart - prosesserer eksisterende PDFer."""
    logger.info("Starter Ukenytt add-on v1.0.0")
    logger.info("Konfigurerte barn: %s", CHILDREN)
    logger.info("Data-mappe: %s", DATA_DIR)
    logger.info("Home Assistant URL: %s", HA_URL)

    for child in CHILDREN:
        pdf_path = get_pdf_path(child)
        if pdf_path.exists():
            logger.info("Fant eksisterende PDF for %s, prosesserer...", child)
            success, message = process_pdf_for_child(child)
            logger.info("  -> %s", message)
        else:
            logger.info("Ingen PDF funnet for %s", child)


if __name__ == "__main__":
    startup_process()

    port = int(os.getenv("PORT", "8099"))
    logger.info("Starter webserver på port %d", port)

    try:
        from waitress import serve

        serve(app, host="0.0.0.0", port=port)
    except ImportError:
        app.run(host="0.0.0.0", port=port, debug=False)
