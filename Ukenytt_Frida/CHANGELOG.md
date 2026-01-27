# Changelog

Alle vesentlige endringer i dette prosjektet vil bli dokumentert i denne filen.

Formatet er basert på [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
og prosjektet følger [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
