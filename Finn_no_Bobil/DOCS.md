# Finn.no Bobil — Dokumentasjon

Dette addonet søker automatisk etter bobiler på finn.no og lagrer annonsedata i en MySQL-database. Prisendringer og andre feltendringer logges ved hver kjøring.

## Installasjon

1. Gå til **Innstillinger** → **Tillegg** → **Tilleggsbutikk**.
2. Legg til dette repositoryet som et eksternt repository.
3. Finn **Finn no Bobil** og klikk **INSTALLER**.
4. Konfigurer databasetilkobling og søkeparametere under **Konfigurasjon**-fanen.
5. Start addonet.

## Forutsetninger

Addonet krever en MySQL/MariaDB-database med en tabell `bobil`. Tabellen må ha følgende struktur:

```sql
CREATE TABLE bobil (
    Finnkode BIGINT PRIMARY KEY,
    Annonsenavn VARCHAR(255),
    Modell VARCHAR(50),
    Kilometerstand VARCHAR(50),
    Girkasse VARCHAR(50),
    Beskrivelse TEXT,
    Nyttelast VARCHAR(50),
    Typebobil VARCHAR(50),
    Oppdatert VARCHAR(50),
    URL VARCHAR(500),
    Pris INT
);
```

## Konfigurasjon

```yaml
databasehost: "192.168.1.x"
databaseusername: "homeassistant"
databasepassword: "ditt-passord"
databasename: "finn_no"
databaseport: "3306"
dry_run: false
price_from: 300000
price_to: 700000
mileage_to: 122000
year_from: 2006
no_of_sleepers_from: 4
weight_to: 3501
locations:
  - "22042"
  - "20003"
mobile_home_segments:
  - 1
  - 2
  - 3
sort: "YEAR_DESC"
```

### Database

| Option | Beskrivelse |
|--------|-------------|
| `databasehost` | Hostname eller IP til MySQL-serveren |
| `databaseusername` | Brukernavn for databasetilkobling |
| `databasepassword` | Passord for databasetilkobling |
| `databasename` | Navn på databasen som inneholder `bobil`-tabellen |
| `databaseport` | Port for MySQL-tilkobling (standard: `3306`) |

### Option: `dry_run`

Sett til `true` for å kjøre addonet uten å skrive til databasen. Nyttig for å verifisere at søk og datahenting fungerer korrekt, og for å se hva som *ville* blitt endret uten å risikere eksisterende data.

I dry run-modus vil addonet:
- Hente alle annonser fra finn.no som normalt
- Koble til databasen og lese eksisterende data
- Sammenligne og logge hva som er nytt, endret og uendret
- **Ikke skrive noe til databasen**

### Søkeparametere

| Option | Beskrivelse |
|--------|-------------|
| `price_from` | Minimum pris i kr |
| `price_to` | Maksimum pris i kr |
| `mileage_to` | Maksimum kilometerstand |
| `year_from` | Tidligste årsmodell |
| `no_of_sleepers_from` | Minimum antall soveplasser |
| `weight_to` | Maksimum totalvekt i kg |
| `sort` | Sortering (`YEAR_DESC`, `PRICE_ASC`, `PRICE_DESC` osv.) |

### Option: `locations`

Liste med finn.no-lokasjonskoder (region-ID-er). Hver kode representerer en norsk region:

| Kode | Region |
|------|--------|
| `22042` | Agder |
| `20003` | Akershus |
| `20007` | Buskerud |
| `22034` | Innlandet |
| `20061` | Oslo |
| `20009` | Telemark |
| `20008` | Vestfold |
| `20002` | Østfold |
| `20010` | Rogaland |
| `22046` | Vestland |
| `20016` | Møre og Romsdal |
| `22035` | Trøndelag |
| `20018` | Nordland |
| `22030` | Troms |
| `20020` | Finnmark |

For å søke i hele Norge, legg til alle regionskodene. Fjern regioner du ikke er interessert i for raskere søk.

### Option: `mobile_home_segments`

Liste med bobiltyper å søke etter:

| Verdi | Type |
|-------|------|
| `1` | Integrert |
| `2` | Delintegrert |
| `3` | Alkove |

## Hvordan det fungerer

1. Addonet bygger en søke-URL basert på konfigurerte parametere.
2. Henter alle annonser fra finn.no sitt API med paginering.
3. For hver annonse hentes detaljert informasjon (girkasse, nyttelast, type bobil, beskrivelse).
4. Sammenligner med eksisterende data i databasen og logger endringer.
5. Lagrer nye og oppdaterte annonser i `bobil`-tabellen.

## Feilsøking

Sjekk addon-loggen for detaljerte meldinger. Vanlige problemer:

- **"Ingen tilkobling til databasen"** — sjekk at MySQL-serveren kjører og at host/brukernavn/passord er riktig.
- **"FINN API returnerte HTTP 403"** — finn.no kan ha blokkert forespørselen. Prøv igjen senere.
- **"FINN API-responsen mangler 'docs'-feltet"** — finn.no kan ha endret API-strukturen. Sjekk loggen for detaljer om hva som ble returnert.
