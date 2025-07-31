import subprocess
import sys
import os
import json
import tabula
import pandas as pd
import requests
options = json.loads(os.getenv("SUPERVISOR_OPTIONS", "{}"))

# Retrieve Bitbucket username from options or use an empty string if not provided
BITBUCKET_USERNAME = options.get("bitbucket_username", "")

# Retrieve Bitbucket app password from options or use an empty string if not provided
BITBUCKET_APP_PASSWORD = options.get("bitbucket_app_password", "")

# Retrieve Bitbucket repository owner from options or use an empty string if not provided
REPO_OWNER = options.get("bitbucket_repo_owner", "")

# Retrieve Bitbucket repository slug from options or use an empty string if not provided
REPO_SLUG = options.get("bitbucket_repo_slug", "")

# Retrieve Bitbucket branch from options or use 'main' if not provided
BRANCH = options.get("bitbucket_branch", "main")

# Retrieve Bitbucket folder path from options or use '/addons' if not provided
FOLDER_PATH = options.get("bitbucket_folder_path", "/addons")

# Retrieve Home Assistant URL from options or use 'http://localhost:8123' if not provided
home_assistant_url = options.get("homeassistant_url", "http://localhost:8123")

# Retrieve Home Assistant long-lived access token from options or use an empty string if not provided
long_lived_access_token = options.get("homeassistant_long_lived_access_token", "")

def download_latest_file():
    url = f"https://api.bitbucket.org/2.0/repositories/{REPO_OWNER}/{REPO_SLUG}/src/{BRANCH}/{FOLDER_PATH}"
    auth = (BITBUCKET_USERNAME, BITBUCKET_APP_PASSWORD)
    response = requests.get(url, auth=auth)

    if response.status_code != 200:
        print(f"Feil ved henting av fil fra Bitbucket: {response.status_code} - {response.json()}")
        return None

    files = response.json().get("values", [])
    pdf_files = [file["path"] for file in files if file["path"].lower().endswith(".pdf")]

    if not pdf_files:
        print("Ingen PDF-filer funnet i Bitbucket.")
        return None

    latest_file = sorted(pdf_files, key=lambda x: int(''.join(filter(str.isdigit, os.path.basename(x)))[-2:]))[-1]
    print(f"Nyeste fil funnet: {latest_file}")

    file_url = f"https://api.bitbucket.org/2.0/repositories/{REPO_OWNER}/{REPO_SLUG}/src/{BRANCH}/{latest_file}"
    file_response = requests.get(file_url, auth=auth)
    if file_response.status_code == 200:
        local_path = os.path.join("/data/", os.path.basename(latest_file))
        with open(local_path, "wb") as f:
            f.write(file_response.content)
        print(f"Fil lastet ned til: {local_path}")
        return local_path
    else:
        print(f"Feil ved nedlasting av fil: {file_response.status_code}")
        return None

def update_home_assistant_sensor(data, file_name):
    try:
        week_number = ''.join(filter(str.isdigit, file_name))[-2:]
        if not week_number.isdigit():
            raise ValueError(f"Ugyldig ukenummer hentet fra filnavn: {file_name}")

        headers = {
            "Authorization": f"Bearer {long_lived_access_token}",
            "Content-Type": "application/json",
        }
        payload = {
            "state": int(week_number),
            "attributes": data,
        }

        response = requests.post(home_assistant_url, headers=headers, json=payload)

        if response.status_code == 200:
            print(f"Sensor 'sensor.frida_ukenytt_tabell' oppdatert i Home Assistant med ukenummer {week_number}.")
        else:
            print(f"Feil ved oppdatering av sensor: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Feil ved oppdatering av Home Assistant-sensor: {e}")

def main():
    file_path = download_latest_file()
    if not file_path:
        print("Ingen fil å prosessere.")
        return
    
    file_name = os.path.basename(file_path)

    pd.options.mode.chained_assignment = None
    try:
        first_table = tabula.read_pdf(file_path, pages=1, multiple_tables=True, stream=True)
    except Exception as e:
        print(f"Feil ved lesing av PDF: {e}")
        return

    if not first_table or not isinstance(first_table[0], pd.DataFrame):
        print("Ingen gyldige tabeller funnet i PDF-en.")
        return

    df = first_table[0].fillna('')
    if df.empty:
        print("DataFrame er tom.")
        return

    ordered_weekdays = ['Mandag', 'Tirsdag', 'Onsdag', 'Torsdag', 'Fredag']
    indices = {day: df[df.iloc[:, 0] == day].index.min() for day in ordered_weekdays}
    last_index = len(df) - 1

    output_json = {}
    for i, day in enumerate(ordered_weekdays):
        if indices[day] is not None:
            start = indices[day] - 1
            end = indices[ordered_weekdays[i + 1]] - 2 if i + 1 < len(ordered_weekdays) and indices[ordered_weekdays[i + 1]] else last_index
            todo_list = df.iloc[start:end + 1, 2].tolist()
            if any(todo_list):
                output_json[day] = todo_list

    update_home_assistant_sensor(output_json, file_name)

if __name__ == "__main__":
    main()