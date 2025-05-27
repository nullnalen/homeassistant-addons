from flask import Flask, request, jsonify
import requests
import os

app = Flask(__name__)

HA_TOKEN = os.getenv("SUPERVISOR_TOKEN")
HA_API_BASE = "http://supervisor/core/api"

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Ingen data"}), 400

        responses = []

        for key, value in data.items():
            entity_id = f"sensor.{key}_ekstern"
            payload = {
                "state": value,
                "attributes": {"friendly_name": key.replace("_", " ").capitalize()}
            }

            resp = requests.post(
                f"{HA_API_BASE}/states/{entity_id}",
                headers={"Authorization": f"Bearer {HA_TOKEN}", "Content-Type": "application/json"},
                json=payload,
                timeout=5
            )
            responses.append((entity_id, resp.status_code))

        return jsonify({"updated": responses}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

app.run(host="0.0.0.0", port=80)
