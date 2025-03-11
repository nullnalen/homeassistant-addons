import subprocess
import sys
import os
import json
import tabula
import pandas as pd
import warnings
import requests
import yaml

# Last inn konfigurasjon fra Home Assistant
options = json.loads(os.getenv("SUPERVISOR_OPTIONS", "{}"))

# Hent innstillinger
BITBUCKET_USERNAME = options.get("bitbucket_username", "")
BITBUCKET_APP_PASSWORD = options.get("bitbucket_app_password", "")
REPO_OWNER = options.get("bitbucket_repo_owner", "")
REPO_SLUG = options.get("bitbucket_repo_slug", "")
BRANCH = options.get("bitbucket_branch", "main")
FOLDER_PATH = options.get("bitbucket_folder_path", "/addons")
home_assistant_url = options.get("homeassistant_url", "http://localhost:8123")
long_lived_access_token = options.get("homeassistant_long_lived_access_token", "")

# Laster ned fil fra Bitbucket
def download_latest_file():
    url = f"https://api.bitbucket.org/2.0/repositories/{REPO_OWNER}/{REPO_SLUG}/src/{BRANCH}/{FOLDER_PATH}"
    auth = (BITBUCKET_USERNAME, BITBUCKET_APP_PASSWORD)
    response = requests.get(url, auth=auth)

    if response.status_code != 200:
        print(f"Feil ved henting av fil fra Bitbucket: {response.status_code} - {response.json()}")
        return None

    # Parse responsen for å finne PDF-filer
    files = response.json().get("values", [])
    pdf_files = [file["path"] for file in files if file["path"].lower().endswith(".pdf")]

    if not pdf_files:
        print("Ingen PDF-filer funnet i Bitbucket.")
        return None

    # Finn filen med høyest nummer
    latest_file = sorted(pdf_files, key=lambda x: int(''.join(filter(str.isdigit, os.path.basename(x)))[-2:]))[-1]
    print(f"Nyeste fil funnet: {latest_file}")

    # Last ned filen
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
        # Hent ukenummer fra filnavnet
        week_number = ''.join(filter(str.isdigit, file_name))[-2:]
        if not week_number.isdigit():
            raise ValueError(f"Ugyldig ukenummer hentet fra filnavn: {file_name}")



        # Payload for oppdatering av sensor
        headers = {
            "Authorization": f"Bearer {long_lived_access_token}",
            "Content-Type": "application/json",
        }
        payload = {
            "state": int(week_number),
            "attributes": data,
        }

        # Send forespørselen til Home Assistant
        response = requests.post(home_assistant_url, headers=headers, json=payload)

        if response.status_code == 200:
            print(f"Sensor 'sensor.frida_ukenytt_tabell' oppdatert i Home Assistant med ukenummer {week_number}.")
        else:
            print(f"Feil ved oppdatering av sensor: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Feil ved oppdatering av Home Assistant-sensor: {e}")

def main():
    # Last ned nyeste fil fra Bitbucket
    file_path = download_latest_file()
    if not file_path:
        print("Ingen fil å prosessere.")
        return
    
    # Hent filnavnet
    file_name = os.path.basename(file_path)

    # Prosesser PDF-filen
    warnings.filterwarnings("ignore", category=UserWarning, module='tabula')
    try:
        first_table = tabula.read_pdf(file_path, pages=1, multiple_tables=True)
    except Exception as e:
        print(f"Feil ved lesing av PDF: {e}")
        return

    if not first_table or not isinstance(first_table[0], pd.DataFrame):
        print("Ingen gyldige tabeller funnet i PDF-en.")
        return

    # Rens og prosesser DataFrame
    df = first_table[0]
    df_cleaned = df.fillna('')
    if df_cleaned.empty:
        print("DataFrame er tom.")
        return

    # Ordne ukedager
    ordered_weekdays = ['Mandag', 'Tirsdag', 'Onsdag', 'Torsdag', 'Fredag']
    weekday_indices = {weekday: None for weekday in ordered_weekdays}
    last_index = len(df_cleaned) - 1

    # Finn indekser for hver ukedag
    for weekday in ordered_weekdays:
        indices = df_cleaned[df_cleaned.iloc[:, 0] == weekday].index.tolist()
        if indices:
            weekday_indices[weekday] = indices[0]

    # Logikkfunksjoner for hver ukedag
    def logic_weekday(start_weekday, next_weekday):
        start_row = weekday_indices[start_weekday] - 1
        end_row = weekday_indices[next_weekday] - 2 if weekday_indices[next_weekday] else last_index
        return start_row, end_row

    # To-do-lister for hver ukedag
    todo_lists = {weekday: [] for weekday in ordered_weekdays}
    for i, weekday in enumerate(ordered_weekdays):
        if weekday_indices[weekday] is not None:
            next_weekday = ordered_weekdays[i + 1] if i + 1 < len(ordered_weekdays) else None
            start_row, end_row = logic_weekday(weekday, next_weekday) if next_weekday else (weekday_indices[weekday] - 1, last_index)
            todo_list = df_cleaned.iloc[start_row:end_row + 1, 2].tolist()
            if any(todo_list):
                todo_lists[weekday] = todo_list

    # Lag JSON-utdata
    output_json = {weekday: todo_list for weekday, todo_list in todo_lists.items() if todo_list}
    json_output = json.dumps(output_json, indent=4, ensure_ascii=False).replace('\u2013', '-')

    # Skriv ut JSON-utdata til konsollen
    #print("\nJSON-utdata:")
    print(json_output)

    # Oppdater Home Assistant sensor
    update_home_assistant_sensor(output_json, file_name)


if __name__ == "__main__":
    main()