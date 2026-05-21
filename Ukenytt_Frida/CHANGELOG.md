# Changelog

Alle vesentlige endringer i dette prosjektet vil bli dokumentert i denne filen.

Formatet er basert på [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
og prosjektet følger [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.24] - 2026-05-21

### Lagt til
- **Avledede sensorer fra addon** (erstatter Jinja2-hjelpere i HA):
  - `sensor.{barn}_ukenytt_idag` — dagens plan som tekst
  - `sensor.{barn}_ukenytt_imorgen` — morgendagens plan som tekst
  - `sensor.{barn}_ukenytt_idag_openepaperlink` — today med `#`-linjeskift for e-ink
  - `sensor.{barn}_ukenytt_imorgen_openepaperlink` — tomorrow med `#`-linjeskift for e-ink
- Alle fire oppdateres automatisk ved midnatt (bakgrunnstråd) og ved ny PDF
- `POST /refresh` — oppdaterer idag/imorgen-sensorer uten å reparsere PDF
- Versjon vises nå i `/api`-responsen

## [1.0.23] - 2026-05-21

### Fikset
- **Atomic PDF-opplasting**: ny PDF lagres til temp-fil og aktiveres kun ved vellykket parsing — feil PDF sletter ikke lenger forrige ukes plan
- Negativ radindeks i `parse_pdf` ved ukedager på første tabellrad (Python `iloc[-1]` gir siste rad — stille feil)
- `extract_extra_text` stopper ikke lenger alltid ved «fredag» — bruker nå siste ukedagslinje i teksten, fungerer selv om fredag mangler i planen

### Forbedret
- `get_child_data` leser nå lokal state først (ingen nettverkskall) — raskere sidevisning i Ingress
- `last_updated` (ISO 8601 UTC) lagt til i sensor-attributter — kan brukes i automations for å varsle ved gammel plan
- Kortere retry-delay (0,5s) ved oppstart vs. live upload (2s) — raskere addon-start

## [1.0.22] - 2026-05-21

### Fikset
- **Ukeplan vises ikke etter restart** — `get_child_data()` bruker nå lokal sensor-state som fallback når HA-API ikke svarer eller mangler data
- Ukenummer-fallback bruker ikke lenger alle siffer i filnavnet (f.eks. `2025-01-15.pdf` ga feil uke); nå kreves "uke"-prefiks i filnavnet eller tekst i PDF
- Config-advarsel: tydelig loggmelding hvis `UKENYTT_CHILDREN` ikke kan parses (tidligere stille fallback)

### Forbedret
- PDF-parsing gir nå diagnostisk feilmelding ved uventet tabellstruktur: logger antall kolonner og innholdet i kolonne 0
- Retry-logikk ved HA API-feil: prøver 3 ganger med 2 sekunders pause ved 5xx/nettverksfeil (4xx avbrytes umiddelbart)
- `/status`-endepunktet viser nå opplastningstidspunkt (ISO 8601), originalt filnavn og om sensor-state-fil finnes
- Versjonsnummer leses fra `ADDON_VERSION` env-var satt i Dockerfile, slik at kun Dockerfile og config.yaml trenger oppdatering

## [1.0.18] - 2026-01-29

### Forbedret
- Versjonsnummer vises nå korrekt i oppstartslogg (var hardkodet til v1.0.0)
- Refaktorert duplisert safe_name-logikk til hjelpefunksjoner
- Flyttet alle import-setninger til toppen av filen
- Fjernet unødvendig re-import av Path i process_pdf_for_child
- Bedre feillogging i extract_pdf_text() — inkluderer filnavn og full stack trace

### Lagt til
- Filstørrelsevalidering ved PDF-opplasting (maks 10 MB)
- API-nøkkelsjekk på /process-endepunktet (manglet tidligere)
- JVM-minnegrense (-Xmx256m) for tabula-py
- Originalt filnavn lagres til disk — ukenummer beholdes ved reprocessing
- pdfplumber eksplisitt i requirements.txt
- Konstanter for magiske verdier (MAX_INFO_LENGTH, MAX_PDF_SIZE, WEEKDAYS)

## [1.0.0] - 2026-01-27

### Endret
- **BREAKING:** Fullstendig omskriving av add-on arkitektur
- Fjernet Bitbucket-integrasjon
- Ny HTTP API-basert opplasting via Flask
- Støtte for flere barn med separate sensorer
- Migrert til Home Assistant best practices
- Bruker nå Home Assistant API proxy i stedet for direkte token

### Lagt til
- HTTP API på port 8099 med endepunkter:
  - `POST /upload` - Last opp PDF
  - `GET /status` - Se status for alle barn
  - `POST /process` - Re-prosesser eksisterende PDFer
  - `GET /health` - Helsesjekk
- API-nøkkel autentisering (valgfritt)
- Strukturert logging
- Flerspråklig støtte (norsk og engelsk)
- Komplett dokumentasjon (DOCS.md)
- S6-overlay for prosesshåndtering

### Fjernet
- Bitbucket username/password konfigurasjon
- Bitbucket repository konfigurasjon
- Automatisk nedlasting fra Bitbucket

## [0.2.2] - 2025-07-31

### Fikset
- Diverse feilrettinger

## [0.2.0] - 2025-06-06

### Lagt til
- Første fungerende versjon med Bitbucket-integrasjon
- PDF-parsing med tabula-py
- Home Assistant sensor-oppdatering
