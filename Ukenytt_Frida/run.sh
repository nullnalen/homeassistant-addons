#!/bin/bash
echo "Starter addon..."

# Eksporter konfigurasjonen fra options.json som miljøvariabler
export SUPERVISOR_OPTIONS=$(</data/options.json)

# Kjør Python-scriptet
python3 /ukenytt.py
