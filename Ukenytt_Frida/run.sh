#!/bin/bash
set -e

echo "Starter Ukenytt add-on..."

# Eksporter konfigurasjonen fra options.json som miljøvariabler
if [ -f /data/options.json ]; then
    export SUPERVISOR_OPTIONS=$(cat /data/options.json)
else
    echo "ADVARSEL: /data/options.json ikke funnet, bruker standardverdier"
    export SUPERVISOR_OPTIONS="{}"
fi

# Opprett data-mappe hvis den ikke finnes
mkdir -p /data/ukenytt

# Kjør Python-applikasjonen
exec python3 /ukenytt.py
