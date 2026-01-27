import os
import json
import logging
from pathlib import Path

import tabula
import pandas as pd
import requests
from flask import Flask, request, jsonify

# Konfigurer logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Last konfigurasjon
options = json.loads(os.getenv("SUPERVISOR_OPTIONS", "{}"))

# Home Assistant konfigurasjon
HOME_ASSISTANT_URL = options.get("homeassistant_url", "http://supervisor/core")
HA_TOKEN = options.get("homeassistant_long_lived_access_token", "")

# API-nøkkel for opplasting (enkel autentisering)
API_KEY = options.get("api_key", "")

# Konfigurerbare barn
CHILDREN = options.get("children", ["frida"])

# Mappe for lagring av PDF-filer
DATA_DIR = Path("/data/ukenytt")
DATA_DIR.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)


def get_pdf_path(child_name: str) -> Path:
    """Returnerer stien til PDF-filen for et barn."""
    return DATA_DIR / f"{child_name.lower()}.pdf"


def parse_pdf(file_path: Path) -> dict:
    """Parser PDF og returnerer ukeplan som dictionary."""
    logger.info(f"Parser PDF: {file_path}")

    pd.options.mode.chained_assignment = None

    try:
        tables = tabula.read_pdf(str(file_path), pages=1, multiple_tables=True, stream=True)
    except Exception as e:
        logger.error(f"Feil ved lesing av PDF: {e}")
        raise ValueError(f"Kunne ikke lese PDF: {e}")

    if not tables or not isinstance(tables[0], pd.DataFrame):
        raise ValueError("Ingen gyldige tabeller funnet i PDF-en")

    df = tables[0].fillna('')
    if df.empty:
        raise ValueError("Tabellen i PDF-en er tom")

    ordered_weekdays = ['Mandag', 'Tirsdag', 'Onsdag', 'Torsdag', 'Fredag']
    indices = {day: df[df.iloc[:, 0] == day].index.min() for day in ordered_weekdays}
    last_index = len(df) - 1

    output = {}
    for i, day in enumerate(ordered_weekdays):
        if pd.notna(indices[day]):
            start = indices[day] - 1
            next_day_idx = indices[ordered_weekdays[i + 1]] if i + 1 < len(ordered_weekdays) else None
            end = (next_day_idx - 2) if pd.notna(next_day_idx) else last_index

            todo_list = df.iloc[start:end + 1, 2].tolist()
            # Filtrer ut tomme verdier
            todo_list = [item for item in todo_list if item and str(item).strip()]
            if todo_list:
                output[day] = todo_list

    return output


def extract_week_number(file_path: Path) -> str:
    """Ekstraherer ukenummer fra filnavn."""
    digits = ''.join(filter(str.isdigit, file_path.stem))
    if len(digits) >= 2:
        return digits[-2:]
    return "00"


def update_home_assistant_sensor(child_name: str, data: dict, week_number: str):
    """Oppdaterer Home Assistant sensor for et barn."""
    sensor_name = f"sensor.{child_name.lower()}_ukenytt_tabell"
    url = f"{HOME_ASSISTANT_URL}/api/states/{sensor_name}"

    headers = {
        "Authorization": f"Bearer {HA_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {
        "state": int(week_number) if week_number.isdigit() else 0,
        "attributes": {
            "barn": child_name,
            "ukeplan": data,
            "friendly_name": f"{child_name.capitalize()} Ukenytt",
        },
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        if response.status_code in (200, 201):
            logger.info(f"Sensor '{sensor_name}' oppdatert med uke {week_number}")
            return True
        else:
            logger.error(f"Feil ved oppdatering av sensor: {response.status_code} - {response.text}")
            return False
    except requests.RequestException as e:
        logger.error(f"Nettverksfeil ved oppdatering av sensor: {e}")
        return False


def process_pdf_for_child(child_name: str) -> tuple[bool, str]:
    """Prosesserer PDF for et barn og oppdaterer sensor."""
    pdf_path = get_pdf_path(child_name)

    if not pdf_path.exists():
        return False, f"Ingen PDF funnet for {child_name}"

    try:
        data = parse_pdf(pdf_path)
        week_number = extract_week_number(pdf_path)

        if update_home_assistant_sensor(child_name, data, week_number):
            return True, f"Sensor oppdatert for {child_name}, uke {week_number}"
        else:
            return False, f"Kunne ikke oppdatere sensor for {child_name}"
    except ValueError as e:
        return False, str(e)


@app.route('/health', methods=['GET'])
def health_check():
    """Helsesjekk-endepunkt."""
    return jsonify({"status": "ok", "children": CHILDREN})


@app.route('/upload', methods=['POST'])
def upload_pdf():
    """
    Mottar PDF-fil via HTTP POST.

    Query-parametere:
        child: Navn på barnet (påkrevd)
        api_key: API-nøkkel for autentisering (påkrevd hvis konfigurert)
    """
    # Sjekk API-nøkkel hvis konfigurert
    if API_KEY:
        provided_key = request.args.get('api_key') or request.headers.get('X-API-Key')
        if provided_key != API_KEY:
            logger.warning("Ugyldig API-nøkkel forsøk")
            return jsonify({"error": "Ugyldig API-nøkkel"}), 401

    # Hent barnenavn
    child_name = request.args.get('child', '').lower()
    if not child_name:
        return jsonify({"error": "Mangler 'child' parameter"}), 400

    if child_name not in [c.lower() for c in CHILDREN]:
        return jsonify({
            "error": f"Ukjent barn: {child_name}",
            "configured_children": CHILDREN
        }), 400

    # Sjekk at fil ble sendt
    if 'file' not in request.files:
        # Prøv å lese direkte fra request body
        if request.content_type and 'pdf' in request.content_type.lower():
            file_data = request.get_data()
        else:
            return jsonify({"error": "Ingen fil mottatt. Send som 'file' i multipart/form-data eller som raw PDF body"}), 400
    else:
        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "Ingen fil valgt"}), 400
        file_data = file.read()

    # Valider at det er en PDF
    if not file_data.startswith(b'%PDF'):
        return jsonify({"error": "Ugyldig filformat - må være PDF"}), 400

    # Lagre filen (overskriver eksisterende)
    pdf_path = get_pdf_path(child_name)
    old_existed = pdf_path.exists()

    try:
        pdf_path.write_bytes(file_data)
        logger.info(f"PDF lagret for {child_name}: {pdf_path}")
    except IOError as e:
        logger.error(f"Kunne ikke lagre fil: {e}")
        return jsonify({"error": "Kunne ikke lagre fil"}), 500

    # Prosesser PDF og oppdater sensor
    success, message = process_pdf_for_child(child_name)

    return jsonify({
        "success": success,
        "message": message,
        "child": child_name,
        "replaced_existing": old_existed,
        "file_path": str(pdf_path)
    }), 200 if success else 500


@app.route('/process', methods=['POST'])
def process_existing():
    """
    Prosesserer eksisterende PDF-filer på nytt.

    Query-parametere:
        child: Navn på barnet (valgfritt - prosesserer alle hvis ikke angitt)
    """
    child_name = request.args.get('child', '').lower()

    if child_name:
        if child_name not in [c.lower() for c in CHILDREN]:
            return jsonify({"error": f"Ukjent barn: {child_name}"}), 400
        children_to_process = [child_name]
    else:
        children_to_process = [c.lower() for c in CHILDREN]

    results = {}
    for child in children_to_process:
        success, message = process_pdf_for_child(child)
        results[child] = {"success": success, "message": message}

    overall_success = all(r["success"] for r in results.values())
    return jsonify({
        "success": overall_success,
        "results": results
    }), 200 if overall_success else 207  # 207 Multi-Status


@app.route('/status', methods=['GET'])
def status():
    """Viser status for alle barn."""
    status_info = {}
    for child in CHILDREN:
        child_lower = child.lower()
        pdf_path = get_pdf_path(child_lower)
        status_info[child_lower] = {
            "has_pdf": pdf_path.exists(),
            "pdf_path": str(pdf_path) if pdf_path.exists() else None,
            "pdf_size": pdf_path.stat().st_size if pdf_path.exists() else None,
        }

    return jsonify({
        "children": status_info,
        "data_directory": str(DATA_DIR)
    })


def startup_process():
    """Kjører ved oppstart - prosesserer eksisterende PDFer."""
    logger.info("Starter Ukenytt add-on...")
    logger.info(f"Konfigurerte barn: {CHILDREN}")
    logger.info(f"Data-mappe: {DATA_DIR}")

    for child in CHILDREN:
        child_lower = child.lower()
        pdf_path = get_pdf_path(child_lower)
        if pdf_path.exists():
            logger.info(f"Fant eksisterende PDF for {child}, prosesserer...")
            success, message = process_pdf_for_child(child_lower)
            logger.info(f"  -> {message}")
        else:
            logger.info(f"Ingen PDF funnet for {child}")


if __name__ == "__main__":
    startup_process()

    port = int(os.getenv("PORT", "8099"))
    logger.info(f"Starter webserver på port {port}")

    # Bruk waitress for produksjon hvis tilgjengelig, ellers Flask dev server
    try:
        from waitress import serve
        serve(app, host='0.0.0.0', port=port)
    except ImportError:
        app.run(host='0.0.0.0', port=port, debug=False)
