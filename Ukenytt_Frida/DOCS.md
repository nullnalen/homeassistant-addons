# Ukenytt Add-on

Denne add-on-en lar deg laste opp ukenytt-PDF-filer (f.eks. fra barnehage eller skole) og konverterer dem automatisk til Home Assistant sensorer.

## Funksjoner

- **HTTP API** for opplasting av PDF-filer
- **Støtte for flere barn** - hver får sin egen sensor
- **Automatisk parsing** av ukeplan-tabeller
- **Siri Shortcuts-kompatibel** - last opp direkte fra iPhone
- **Persistente data** - PDF-filer lagres lokalt

## Konfigurasjon

### API-nøkkel (valgfritt)

En hemmelig nøkkel som kreves for å laste opp filer. Hvis den er tom, kreves ingen autentisering.

```yaml
api_key: "min-hemmelige-nøkkel"
```

### Barn

Liste over barn å spore ukeplaner for. Hvert barn får sin egen sensor.

```yaml
children:
  - name: "Frida"
  - name: "Emma"
```

Dette oppretter sensorene:
- `sensor.frida_ukenytt_tabell`
- `sensor.emma_ukenytt_tabell`

## API-endepunkter

Add-on-en kjører en HTTP-server på port **8099**.

### POST /upload

Last opp en PDF-fil for et barn.

**Query-parametere:**
- `child` (påkrevd): Barnets navn (må matche konfigurasjon)
- `api_key` (valgfritt): API-nøkkel hvis konfigurert

**Body:** PDF-fil som `multipart/form-data` eller rå bytes

**Eksempel med curl:**
```bash
curl -X POST "http://homeassistant.local:8099/upload?child=frida&api_key=min-nøkkel" \
  -F "file=@ukenytt.pdf"
```

**Respons:**
```json
{
  "success": true,
  "message": "Sensor oppdatert for frida, uke 23",
  "child": "frida",
  "replaced_existing": true
}
```

### GET /status

Viser status for alle konfigurerte barn.

**Eksempel:**
```bash
curl "http://homeassistant.local:8099/status"
```

**Respons:**
```json
{
  "children": {
    "frida": {
      "has_pdf": true,
      "pdf_size": 123456
    },
    "emma": {
      "has_pdf": false
    }
  }
}
```

### POST /process

Re-prosesserer eksisterende PDF-filer.

**Query-parametere:**
- `child` (valgfritt): Spesifikt barn, eller alle hvis ikke angitt

### GET /health

Helsesjekk for add-on-en.

## Siri Shortcuts oppsett

For å laste opp PDF direkte fra iPhone:

1. **Opprett en ny snarvei** i Snarveier-appen
2. **Legg til handling:** "Del ark" → Motta PDF
3. **Legg til handling:** "Hent innhold fra URL"
   - URL: `http://DIN_HA_IP:8099/upload?child=frida&api_key=DIN_NØKKEL`
   - Metode: POST
   - Forespørselstekst: Fil
   - Innhold: Snarveisinndata
4. **Lagre snarveien**

Lag én snarvei per barn med forskjellig `child`-parameter.

## Sensor-attributter

Hver sensor har følgende attributter:

| Attributt | Beskrivelse |
|-----------|-------------|
| `state` | Ukenummer (f.eks. 23) |
| `barn` | Barnets navn |
| `ukeplan` | Dictionary med ukedager og oppgaver |
| `friendly_name` | Visningsnavn |

**Eksempel på ukeplan-attributt:**
```json
{
  "Mandag": ["Gymtøy", "Matpakke"],
  "Tirsdag": ["Tur i skogen"],
  "Onsdag": ["Baking"],
  "Torsdag": ["Bibliotek"],
  "Fredag": ["Fredagskos"]
}
```

## Lovelace-eksempel

```yaml
type: markdown
title: Ukenytt Frida
content: |
  **Uke {{ states('sensor.frida_ukenytt_tabell') }}**

  {% for dag, aktiviteter in state_attr('sensor.frida_ukenytt_tabell', 'ukeplan').items() %}
  **{{ dag }}:**
  {% for aktivitet in aktiviteter %}
  - {{ aktivitet }}
  {% endfor %}
  {% endfor %}
```

## Feilsøking

### PDF-en parses ikke riktig

Add-on-en forventer PDF-er med en tabell der:
- Første kolonne inneholder ukedager (Mandag, Tirsdag, osv.)
- Tredje kolonne inneholder aktiviteter/oppgaver

Hvis PDF-formatet er annerledes, vil parsingen ikke fungere korrekt.

### Sensor oppdateres ikke

1. Sjekk at add-on-en kjører i Supervisor
2. Verifiser at porten 8099 er tilgjengelig
3. Sjekk loggene for feilmeldinger

### API-nøkkel fungerer ikke

Sørg for at API-nøkkelen sendes enten som:
- Query-parameter: `?api_key=din-nøkkel`
- HTTP-header: `X-API-Key: din-nøkkel`

## Support

Rapporter problemer på [GitHub Issues](https://github.com/nullnalen/ukenytt_addon/issues).
