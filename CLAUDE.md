# Claude Code - Prosjektnotater

## Viktige påminnelser ved commits

### Ukenytt_Frida
Ved hvert release må versjonsnummeret oppdateres **tre steder** (alle må være like):
1. `Ukenytt_Frida/config.yaml` — linje 3: `version:`
2. `Ukenytt_Frida/Dockerfile` — `ENV ADDON_VERSION=` og `io.hass.version` label
3. `Ukenytt_Frida/ukenytt.py` — fallback i `os.getenv("ADDON_VERSION", "x.x.x")`

Merk: Python leser versjonen fra `ADDON_VERSION` env-var satt av Dockerfile. Fallback i Python er kun sikkerhetsnett.
