#!/usr/bin/env python3
"""
Bobil — Ingress Web UI
Flask-basert webgrensesnitt for å vise bobilannonser (Finn.no + autodb) fra databasen.
"""
import os
import sys
import json
import re
import logging
import threading
import traceback
from datetime import datetime, timedelta

import mysql.connector
from mysql.connector import pooling
from flask import Flask, render_template_string, request, redirect, url_for, jsonify
from markupsafe import escape
from waitress import serve


def esc(val):
    """HTML-escape en verdi for trygg innbygging i HTML. Returnerer tom streng for None."""
    if val is None:
        return ""
    return str(escape(val))

# Logging
logger = logging.getLogger("bobil_web")
logger.setLevel(logging.INFO)
logger.handlers.clear()
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

# Konfigurasjon
try:
    options_str = os.getenv("SUPERVISOR_OPTIONS", "{}")
    options = json.loads(options_str)
except Exception as e:
    logger.error("Feil ved lasting av SUPERVISOR_OPTIONS: %s", e)
    options = {}

DB_CONFIG = {
    "host": options.get("databasehost", ""),
    "user": options.get("databaseusername", ""),
    "passwd": options.get("databasepassword", ""),
    "database": options.get("databasename", ""),
    "port": options.get("databaseport", 3306),
}

# Scraper-status
scraper_status = {
    "last_run": None,
    "running": False,
    "error": None,
}

# Månedsnavn til tall — norsk og engelsk
MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "mai": 5, "may": 5,
    "jun": 6, "jul": 7, "aug": 8, "sep": 9, "okt": 10, "oct": 10,
    "nov": 11, "des": 12, "dec": 12,
}


def parse_norwegian_date(date_str):
    """Parse datostreng til datetime. Støtter norsk format og ISO 8601."""
    if not date_str or date_str == "Ukjent":
        return None
    try:
        s = date_str.strip()
        # ISO 8601 fallback: "2026-05-26T03:01:32..." eller "2026-05-26 03:01"
        if re.match(r"\d{4}-\d{2}-\d{2}", s):
            s_clean = re.sub(r"[TZ]", " ", s).strip()[:16]
            return datetime.strptime(s_clean, "%Y-%m-%d %H:%M")
        sl = s.lower()
        for name, num in MONTH_MAP.items():
            if name in sl:
                sl = re.sub(rf"\b{name}\.?\b", f"{num:02d}", sl)
                break
        # Forventet format: "25. 05. 2026 14:31"
        m = re.match(r"(\d{1,2})\.\s*(\d{2})\.?\s+(\d{4})\s+(\d{2}):(\d{2})", sl)
        if m:
            return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)),
                            int(m.group(4)), int(m.group(5)))
    except Exception:
        pass
    return None


def parse_price(price_val):
    """Parse pris til int. Håndterer både int og streng-format."""
    if price_val is None:
        return None
    if isinstance(price_val, (int, float)):
        return int(price_val)
    s = str(price_val)
    if "solgt" in s.lower():
        return None
    try:
        return int(re.sub(r"[^\d]", "", s))
    except (ValueError, TypeError):
        return None


def parse_km(km_val):
    """Parse kilometerstand til int."""
    if km_val is None:
        return None
    if isinstance(km_val, (int, float)):
        return int(km_val)
    try:
        return int(re.sub(r"[^\d]", "", str(km_val)))
    except (ValueError, TypeError):
        return None


def format_price(price_int):
    """Formater int-pris til lesbar streng."""
    if not price_int:
        return "—"
    return f"{price_int:,.0f} kr".replace(",", " ")


def format_age(date_val):
    """Formater alder fra norsk datostreng, ISO-streng eller datetime til (tekst, css-klasse, sorteringsverdi)."""
    if not date_val:
        return "Ukjent", "age-unknown", 99999
    if isinstance(date_val, datetime):
        dato = date_val
    elif isinstance(date_val, str):
        if re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", date_val):
            try:
                dato = datetime.strptime(date_val, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return "Ukjent", "age-unknown", 99999
        else:
            dato = parse_norwegian_date(date_val)
    else:
        return "Ukjent", "age-unknown", 99999
    if not dato:
        return "Ukjent", "age-unknown", 99999
    delta = datetime.now() - dato
    dager = delta.days
    if dager == 0:
        timer = delta.seconds // 3600
        if timer == 0:
            return "Nå", "age-fresh", 0
        return f"{timer}t siden", "age-fresh", 0
    if dager == 1:
        return "I går", "age-fresh", 1
    if dager < 7:
        return f"{dager} dager", "age-fresh", dager
    if dager < 30:
        return f"{dager} dager", "age-weeks", dager
    if dager < 365:
        mnd = dager // 30
        return f"{mnd} mnd", "age-old", dager
    return f"{dager // 365} år", "age-old", dager


def safe_int(val) -> int | None:
    """Parse en verdi til int, returnerer None ved feil."""
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def enrich_row_with_prices(r: dict) -> None:
    """Berik én rad med formaterte prisfelter (NaaverendePris, LavestePrisF, HoyestePrisF, Prisfall)."""
    pris = parse_price(r.get("Pris"))
    laveste = parse_price(r.get("LavestePris"))
    hoyeste = parse_price(r.get("HoyestePris"))
    if not pris and laveste:
        pris = laveste
    if not laveste and pris:
        laveste = pris
    if not hoyeste and pris:
        hoyeste = pris
    r["NaaverendePris"] = format_price(pris)
    r["LavestePrisF"] = format_price(laveste)
    r["HoyestePrisF"] = format_price(hoyeste)
    if hoyeste and pris and hoyeste > pris:
        diff = hoyeste - pris
        pct = round(diff / hoyeste * 100, 1)
        diff_f = f"{diff:,.0f}".replace(",", " ")
        r["Prisfall"] = f"-{pct}%"
        r["PrisfallHtml"] = (
            f'<span class="prisfall-cell">'
            f'<span class="prisfall-pil">↓</span>'
            f'<span class="prisfall-kr"> {diff_f} kr</span>'
            f'<span class="prisfall-pct">({pct}%)</span>'
            f'</span>'
        )
    else:
        r["Prisfall"] = None
        r["PrisfallHtml"] = '<span class="note-secondary">—</span>'


_db_pool = None


def _get_pool():
    """Lazy-init connection pool."""
    global _db_pool
    if _db_pool is None:
        try:
            _db_pool = pooling.MySQLConnectionPool(
                pool_name="bobil_pool",
                pool_size=5,
                pool_reset_session=True,
                connection_timeout=10,
                **DB_CONFIG,
            )
            logger.info("DB connection pool opprettet (pool_size=5).")
        except Exception as e:
            logger.error("Kunne ikke opprette connection pool: %s", e)
            return None
    return _db_pool


def get_db():
    """Hent en tilkobling fra connection pool."""
    pool = _get_pool()
    if pool:
        try:
            return pool.get_connection()
        except Exception as e:
            logger.error("Kunne ikke hente tilkobling fra pool: %s", e)
    # Fallback til direkte tilkobling
    try:
        conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=10)
        return conn
    except Exception as e:
        logger.error("DB-tilkoblingsfeil: %s", e)
        return None


def ensure_db_columns():
    """Sørg for at nye kolonner og indekser finnes i databasen."""
    conn = get_db()
    if not conn:
        return
    try:
        cur = conn.cursor()
        # Nye kolonner
        for col, coltype in [
            ("ImageURL", "TEXT"),
            ("Lokasjon", "VARCHAR(255)"),
            ("Solgt", "TINYINT(1) DEFAULT 0"),
            ("Kjennemerke", "VARCHAR(20)"),
            ("SvvMerke", "VARCHAR(100)"),
            ("SvvHandelsbetegnelse", "VARCHAR(100)"),
            ("SvvAarsmodell", "INT"),
            ("SvvFarge", "TEXT"),
            ("SvvDrivstoff", "VARCHAR(50)"),
            ("SvvMotorvolum", "INT"),
            ("SvvMotoreffekt", "FLOAT"),
            ("SvvTypebetegnelse", "VARCHAR(100)"),
            ("SvvForstegangNorge", "VARCHAR(20)"),
            ("SvvRegistreringsstatus", "VARCHAR(50)"),
            ("SvvEuKontrollfrist", "VARCHAR(20)"),
            ("SvvEuSistGodkjent", "VARCHAR(20)"),
            ("SvvKarosseritype", "TEXT"),
            ("SvvAntallDorer", "INT"),
            ("SvvAntallSylindre", "INT"),
            ("SvvGirkassetype", "VARCHAR(50)"),
            ("SvvAntallGir", "INT"),
            ("SvvMaksHastighet", "INT"),
            ("SvvElektrisk", "TINYINT(1)"),
            ("SvvLengde", "INT"),
            ("SvvBredde", "INT"),
            ("SvvHoyde", "INT"),
            ("SvvEgenvekt", "INT"),
            ("SvvNyttelast", "INT"),
            ("SvvTotalvekt", "INT"),
            ("SvvTillattTotalvekt", "INT"),
            ("SvvTilhengervektMedBrems", "INT"),
            ("SvvTilhengervektUtenBrems", "INT"),
            ("SvvVertikalKoplingslast", "INT"),
            ("SvvEuroKlasse", "VARCHAR(10)"),
            ("SvvSitteplasser", "INT"),
            ("SvvKjoretoytype", "TEXT"),
            ("Sengelayout", "VARCHAR(50)"),
            ("VendbareForerstoler", "TINYINT(1)"),
            ("Heftelser", "TINYINT UNSIGNED"),
            ("HeftelseSjekket", "DATETIME"),
            ("HeftelserDetaljer", "TEXT"),
            ("AutodbId", "INT"),
            ("Kilde", "VARCHAR(20) DEFAULT 'finn'"),
        ]:
            try:
                cur.execute(f"ALTER TABLE bobil ADD COLUMN {col} {coltype}")
                logger.info("La til kolonne %s i bobil-tabellen.", col)
            except mysql.connector.Error as e:
                if e.errno == 1060:  # Duplicate column
                    pass
                else:
                    logger.error("Feil ved ALTER TABLE for %s: %s", col, e)
        # Utvid kolonner som kan ha vært for korte
        for col, coltype in [("SvvKarosseritype", "TEXT"), ("SvvKjoretoytype", "TEXT"), ("SvvFarge", "TEXT")]:
            try:
                cur.execute(f"ALTER TABLE bobil MODIFY COLUMN {col} {coltype}")
            except Exception:
                pass
        # Migrer eksisterende solgt/fjernet-rader til Solgt=1
        try:
            cur.execute("UPDATE bobil SET Solgt = 1 WHERE Pris LIKE '%Solgt%' OR Pris LIKE '%Fjernet%'")
            if cur.rowcount > 0:
                logger.info("Migrerte %d rader med Solgt/Fjernet til Solgt=1.", cur.rowcount)
        except Exception as e:
            logger.error("Feil ved migrering av solgt-status: %s", e)

        # Dedupliser prisendringer: behold kun første rad per (Finnkode, Pris)
        # så MAX(Tidspunkt) reflekterer første gang en pris ble sett, ikke siste scrape
        try:
            cur.execute("""
                DELETE p FROM prisendringer p
                INNER JOIN prisendringer p2
                    ON p.Finnkode = p2.Finnkode
                    AND LEFT(p.Pris, 50) = LEFT(p2.Pris, 50)
                    AND p.Tidspunkt > p2.Tidspunkt
            """)
            if cur.rowcount > 0:
                logger.info("Slettet %d duplikate prisendring-rader.", cur.rowcount)
        except Exception as e:
            logger.error("Feil ved deduplisering av prisendringer: %s", e)

        # UNIQUE-nøkkel på prisendringer(Finnkode, Pris) slik at INSERT IGNORE
        # faktisk ignorerer duplikater og ikke skriver ny timestamp ved uendret pris
        try:
            cur.execute(
                "ALTER TABLE prisendringer ADD UNIQUE KEY uq_finnkode_pris (Finnkode, Pris(50))"
            )
            logger.info("La til UNIQUE KEY uq_finnkode_pris på prisendringer.")
        except mysql.connector.Error as e:
            if e.errno not in (1061, 1062):  # 1061=dup key name, 1062=dup entry
                logger.error("Feil ved ALTER TABLE prisendringer UNIQUE: %s", e)

        # Indekser for raskere spørringer
        indexes = [
            ("idx_prisendringer_finnkode", "prisendringer", "Finnkode"),
            ("idx_prisendringer_tidspunkt", "prisendringer", "Tidspunkt"),
            ("idx_prisendringer_finnkode_tidspunkt", "prisendringer", "Finnkode, Tidspunkt"),
            ("idx_bobil_modell", "bobil", "Modell"),
            ("idx_bobil_pris", "bobil", "Pris(50)"),
        ]
        for idx_name, table, columns in indexes:
            try:
                cur.execute(f"CREATE INDEX {idx_name} ON {table} ({columns})")
                logger.info("Opprettet indeks %s på %s.", idx_name, table)
            except mysql.connector.Error as e:
                if e.errno == 1061:  # Duplicate key name
                    pass
                else:
                    logger.error("Feil ved opprettelse av indeks %s: %s", idx_name, e)
        conn.commit()

        # SolgtDato: legg til kolonne og bakfyll fra prisendringer om nødvendig
        try:
            cur.execute("ALTER TABLE bobil ADD COLUMN SolgtDato DATETIME NULL")
            logger.info("La til kolonne SolgtDato i bobil-tabellen.")
            conn.commit()
        except mysql.connector.Error as e:
            if e.errno != 1060:
                logger.error("Feil ved ALTER TABLE SolgtDato: %s", e)
        try:
            cur.execute("""
                UPDATE bobil b
                JOIN (
                    SELECT Finnkode, MAX(Tidspunkt) AS SolgtTidspunkt
                    FROM prisendringer
                    WHERE Pris = 'Solgt/Fjernet'
                    GROUP BY Finnkode
                ) p ON b.Finnkode = p.Finnkode
                SET b.SolgtDato = p.SolgtTidspunkt
                WHERE b.SolgtDato IS NULL
            """)
            if cur.rowcount > 0:
                logger.info("Bakfylte SolgtDato for %d annonser.", cur.rowcount)
            conn.commit()
        except Exception as e:
            logger.error("Feil ved bakfylling av SolgtDato: %s", e)

        # bruker_data: favoritter og notater per annonse
        try:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bruker_data (
                    Finnkode INT PRIMARY KEY,
                    Favoritt TINYINT(1) DEFAULT 0,
                    Notat TEXT,
                    Oppdatert DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
        except Exception as e:
            logger.error("Feil ved oppretting av bruker_data: %s", e)

    except Exception as e:
        logger.error("Feil i ensure_db_columns: %s", e)
    finally:
        conn.close()


def get_bruker_data(finnkode: int) -> dict:
    """Hent favoritt-status og notat for en annonse."""
    conn = get_db()
    if not conn:
        return {"favoritt": False, "notat": ""}
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT Favoritt, Notat FROM bruker_data WHERE Finnkode = %s", (finnkode,))
        row = cur.fetchone()
        if row:
            return {"favoritt": bool(row["Favoritt"]), "notat": row["Notat"] or ""}
        return {"favoritt": False, "notat": ""}
    except Exception:
        return {"favoritt": False, "notat": ""}
    finally:
        conn.close()


def get_alle_favoritter() -> list[dict]:
    """Hent alle favorittmerkede biler med brukernotat og bobildata."""
    conn = get_db()
    if not conn:
        return []
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT b.Finnkode, b.AutodbId, b.Kilde, b.Annonsenavn, b.Modell, b.Pris, b.Kilometerstand,
                   b.Lokasjon, b.ImageURL, b.SvvNyttelast, b.SvvLengde,
                   b.SvvTilhengervektMedBrems, b.SvvEuKontrollfrist,
                   b.Sengelayout, b.Heftelser, b.HeftelserDetaljer, b.Solgt,
                   u.Favoritt, u.Notat, u.Oppdatert AS BrukerOppdatert,
                   MAX(NULLIF(CAST(REGEXP_REPLACE(p.Pris, '[^0-9]', '') AS UNSIGNED), 0)) AS HoyestePris,
                   MIN(NULLIF(CAST(REGEXP_REPLACE(p.Pris, '[^0-9]', '') AS UNSIGNED), 0)) AS LavestePris
            FROM bruker_data u
            JOIN bobil b ON u.Finnkode = b.Finnkode
            LEFT JOIN prisendringer p ON b.Finnkode = p.Finnkode
            WHERE u.Favoritt = 1
            GROUP BY b.Finnkode, b.AutodbId, b.Kilde, b.Annonsenavn, b.Modell, b.Pris, b.Kilometerstand,
                     b.Lokasjon, b.ImageURL, b.SvvNyttelast, b.SvvLengde,
                     b.SvvTilhengervektMedBrems, b.SvvEuKontrollfrist,
                     b.Sengelayout, b.Heftelser, b.HeftelserDetaljer, b.Solgt,
                     u.Favoritt, u.Notat, u.Oppdatert
            ORDER BY u.Oppdatert DESC
        """)
        rows = cur.fetchall()
        for r in rows:
            enrich_row_with_prices(r)
            r["AdURL"] = _ad_url(r)
        return rows
    except Exception as e:
        logger.error("Feil i get_alle_favoritter: %s\n%s", e, traceback.format_exc())
        return []
    finally:
        conn.close()


def get_total_count():
    """Hent totalt antall annonser i databasen."""
    conn = get_db()
    if not conn:
        return 0
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM bobil")
        return cur.fetchone()[0]
    except Exception:
        return 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# View-funksjoner
# ---------------------------------------------------------------------------

def get_annonser():
    """Alle annonser (Finn + autodb) med prishistorikk og kjøpsscore, sortert etter siste endring."""
    conn = get_db()
    if not conn:
        return []
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT b.Finnkode, b.AutodbId, b.Kilde, b.Annonsenavn, b.Modell, b.Pris, b.Oppdatert,
                   b.Opprettet, b.SistSett, b.AutodbSistEndret, b.Kilometerstand, b.Beskrivelse, b.Sengelayout,
                   b.SvvNyttelast, b.SvvTilhengervektMedBrems,
                   b.SvvEuKontrollfrist, b.SvvEuSistGodkjent, b.SvvAarsmodell, b.SvvMerke,
                   COUNT(p.Pris) AS AntallEndringer,
                   MIN(NULLIF(CAST(REGEXP_REPLACE(p.Pris, '[^0-9]', '') AS UNSIGNED), 0)) AS LavestePris,
                   MAX(NULLIF(CAST(REGEXP_REPLACE(p.Pris, '[^0-9]', '') AS UNSIGNED), 0)) AS HoyestePris,
                   MAX(p.Tidspunkt) AS SistePrisendring,
                   b.URL
            FROM bobil b
            LEFT JOIN prisendringer p ON b.Finnkode = p.Finnkode
            WHERE (b.Solgt = 0 OR b.Solgt IS NULL)
            GROUP BY b.Finnkode, b.AutodbId, b.Kilde, b.Annonsenavn, b.Modell, b.Pris,
                     b.Oppdatert, b.Opprettet, b.SistSett, b.AutodbSistEndret, b.Kilometerstand, b.Beskrivelse, b.Sengelayout,
                     b.SvvNyttelast, b.SvvTilhengervektMedBrems,
                     b.SvvEuKontrollfrist, b.SvvEuSistGodkjent, b.SvvAarsmodell, b.SvvMerke, b.URL
            ORDER BY COALESCE(MAX(p.Tidspunkt), b.AutodbSistEndret, b.Opprettet) DESC
        """)
        rows = cur.fetchall()
        now = datetime.now()
        keywords = ["køye", "senkeseng", "familie", "vendbare seter", "kapteinstoler", "alkove"]
        for r in rows:
            enrich_row_with_prices(r)
            r["AdURL"] = _ad_url(r)
            # Sorteringsrekkefølge: siste prisendring > sist endret autodb (monoton) > opprettet i DB
            alder_val = r.get("SistePrisendring") or r.get("AutodbSistEndret") or r.get("Opprettet") or ""
            if not alder_val:
                r["Alder"], r["AlderClass"], r["AlderSort"] = "—", "age-unknown", 99999
            else:
                r["Alder"], r["AlderClass"], r["AlderSort"] = format_age(alder_val)
            dato = parse_norwegian_date(r.get("Oppdatert") or "")
            r["DagerPaaMarkedet"] = (now - dato).days if dato else 0
            r["ErNy"] = r["DagerPaaMarkedet"] <= 1
            tekst = f"{r.get('Annonsenavn', '')} {r.get('Beskrivelse', '')}".lower()
            r["Soketreff"] = ", ".join(kw for kw in keywords if kw in tekst)
            if not r.get("HoyestePris"):
                r["HoyestePris"] = parse_price(r.get("Pris"))
            r["KjopsScore"] = beregn_kjopsscore(r, now)
        return rows
    except Exception as e:
        logger.error("Feil i get_annonser: %s\n%s", e, traceback.format_exc())
        return []
    finally:
        conn.close()


FORUM_RISIKO = {
    'Sunlight': 0, 'Rimor': 0, 'Carado': 0, 'Challenger': 5,
    'Dethleffs': 10, 'Knaus': 10, 'Bürstner': 15,
    'Hymer': 20, 'Adria': 20,
}


def beregn_kjopsscore(r: dict, now: datetime) -> int:
    """
    Full scoring-algoritme basert på EU-frist, km/år, nyttelast, årsmodell og merkerisiko.
    """
    s = 0

    # EU-kontrollfrist
    eu_frist = r.get("SvvEuKontrollfrist") or ""
    eu_sist = r.get("SvvEuSistGodkjent") or ""
    mnd_til_eu = None
    mnd_siden_eu = None
    try:
        if eu_frist:
            frist_dato = datetime.strptime(eu_frist[:10], "%Y-%m-%d")
            mnd_til_eu = max(0, (frist_dato - now).days // 30)
        if eu_sist:
            sist_dato = datetime.strptime(eu_sist[:10], "%Y-%m-%d")
            mnd_siden_eu = max(0, (now - sist_dato).days // 30)
    except (ValueError, TypeError):
        pass

    if mnd_til_eu is not None:
        if mnd_til_eu > 24:
            s += 25
        elif mnd_til_eu > 12:
            s += 15
        elif mnd_til_eu > 6:
            s += 5
        else:
            s -= 10

    if mnd_siden_eu is not None:
        if mnd_siden_eu < 6:
            s += 20
        elif mnd_siden_eu < 12:
            s += 10
        elif mnd_siden_eu < 24:
            s += 5
        else:
            s -= 5

    # Nyttelast (SVV)
    nyttelast = r.get("SvvNyttelast") or 0
    if nyttelast >= 700:
        s += 20
    elif nyttelast >= 550:
        s += 15
    elif nyttelast >= 450:
        s += 10
    else:
        s += 3

    # Km per år
    km = parse_km(r.get("Kilometerstand"))
    try:
        aar = int(r.get("SvvAarsmodell") or r.get("Modell") or 0)
    except (ValueError, TypeError):
        aar = 0
    if km and aar and aar > 2000:
        alder_aar = max(1, now.year - aar)
        km_aar = km / alder_aar
        if km_aar < 7000:
            s += 20
        elif km_aar < 10000:
            s += 10
        elif km_aar < 13000:
            s += 5
        else:
            s -= 5

    # Årsmodell
    if aar >= 2019:
        s += 10
    elif aar >= 2017:
        s += 7
    elif aar >= 2015:
        s += 4

    # Merke-risiko (trekk)
    merke = r.get("SvvMerke") or r.get("Annonsenavn", "").split()[0]
    s -= FORUM_RISIKO.get(merke, 5)

    # Prisfall-bonus: belønner annonser med dokumentert prisfall
    pris = parse_price(r.get("Pris"))
    hoyeste = r.get("HoyestePris")
    if pris and hoyeste and hoyeste > pris:
        prisfall_pct = (hoyeste - pris) / hoyeste * 100
        s += min(20, int(prisfall_pct * 2))

    return min(100, max(0, s))


def get_kjopsscore():
    """Returnerer annonser sortert etter kjøpsscore (synkende)."""
    rows = get_annonser()
    rows = [r for r in rows if parse_price(r.get("Pris"))]
    rows.sort(key=lambda x: x["KjopsScore"], reverse=True)
    return rows[:100]


def get_prisutvikling():
    """View 3: Gjennomsnittspris per modellår per måned."""
    conn = get_db()
    if not conn:
        return []
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT b.Modell,"
            " DATE_FORMAT(p.Tidspunkt, %s) AS Periode,"
            " ROUND(AVG(NULLIF(CAST(REGEXP_REPLACE(p.Pris, '[^0-9]', '') AS UNSIGNED), 0))) AS GjSnittPris,"
            " COUNT(*) AS Antall"
            " FROM prisendringer p"
            " JOIN bobil b ON p.Finnkode = b.Finnkode"
            " WHERE b.Modell IS NOT NULL"
            " AND p.Pris NOT LIKE %s"
            " GROUP BY b.Modell, DATE_FORMAT(p.Tidspunkt, %s)"
            " ORDER BY b.Modell DESC, Periode",
            ("%Y-%m", "%Solgt%", "%Y-%m")
        )
        rows = cur.fetchall()
        for r in rows:
            r["GjSnittPrisF"] = format_price(parse_price(r["GjSnittPris"]))
        return rows
    except Exception as e:
        logger.error("Feil i get_prisutvikling: %s\n%s", e, traceback.format_exc())
        return []
    finally:
        conn.close()


def get_liggetid_statistikk():
    """Aggreger median liggetid (dager) for solgte annonser per merke, type og prisklasse."""
    conn = get_db()
    if not conn:
        return {"per_merke": [], "per_type": [], "per_prisklasse": [], "totalt": None}
    try:
        cur = conn.cursor(dictionary=True)

        # Felles CTE: beregn liggetid for solgte annonser.
        # Bruker MIN(prisendringer.Tidspunkt) som "første sett"-dato — mer pålitelig enn
        # Oppdatert-kolonnen som overskrives ved hver scrape-kjøring.
        liggetid_cte = """
            WITH solgt_dato AS (
                SELECT Finnkode, MAX(Tidspunkt) AS SolgtTidspunkt
                FROM prisendringer
                WHERE Pris = 'Solgt/Fjernet'
                GROUP BY Finnkode
            ),
            forste_sett AS (
                SELECT Finnkode, MIN(Tidspunkt) AS ErstSett
                FROM prisendringer
                WHERE Pris REGEXP '^[0-9]+$'
                GROUP BY Finnkode
            ),
            liggetid AS (
                SELECT
                    b.Finnkode,
                    b.SvvMerke,
                    b.Typebobil,
                    CAST(REGEXP_REPLACE(b.Pris, '[^0-9]', '') AS UNSIGNED) AS PrisNum,
                    DATEDIFF(
                        COALESCE(b.SolgtDato, sd.SolgtTidspunkt),
                        fs.ErstSett
                    ) AS Liggetid
                FROM bobil b
                LEFT JOIN solgt_dato sd ON b.Finnkode = sd.Finnkode
                JOIN forste_sett fs ON b.Finnkode = fs.Finnkode
                WHERE b.Solgt = 1
                  AND COALESCE(b.SolgtDato, sd.SolgtTidspunkt) IS NOT NULL
                  AND DATEDIFF(
                      COALESCE(b.SolgtDato, sd.SolgtTidspunkt),
                      fs.ErstSett
                  ) BETWEEN 0 AND 730
            )
        """

        # MySQL-kompatibel median via rownumber-trick
        median_merke_sql = liggetid_cte + """
            SELECT SvvMerke AS Gruppe,
                   COUNT(*) AS Antall,
                   ROUND(AVG(Liggetid)) AS SnittDager,
                   CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(
                       GROUP_CONCAT(Liggetid ORDER BY Liggetid SEPARATOR ','),
                       ',', FLOOR((COUNT(*)+1)/2)
                   ), ',', -1) AS UNSIGNED) AS MedianDager
            FROM liggetid
            WHERE SvvMerke IS NOT NULL AND SvvMerke != ''
            GROUP BY SvvMerke
            HAVING Antall >= 2
            ORDER BY MedianDager ASC
        """
        cur.execute(median_merke_sql)
        per_merke = cur.fetchall()

        cur.execute(liggetid_cte + """
            SELECT Typebobil AS Gruppe,
                   COUNT(*) AS Antall,
                   ROUND(AVG(Liggetid)) AS SnittDager,
                   CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(
                       GROUP_CONCAT(Liggetid ORDER BY Liggetid SEPARATOR ','),
                       ',', FLOOR((COUNT(*)+1)/2)
                   ), ',', -1) AS UNSIGNED) AS MedianDager
            FROM liggetid
            WHERE Typebobil IS NOT NULL AND Typebobil NOT IN ('', 'Ikke oppgitt')
            GROUP BY Typebobil
            HAVING Antall >= 2
            ORDER BY MedianDager ASC
        """)
        per_type = cur.fetchall()

        cur.execute(liggetid_cte + """
            SELECT
                CASE
                    WHEN PrisNum < 200000  THEN 'Under 200k'
                    WHEN PrisNum < 300000  THEN '200–300k'
                    WHEN PrisNum < 400000  THEN '300–400k'
                    WHEN PrisNum < 500000  THEN '400–500k'
                    WHEN PrisNum < 700000  THEN '500–700k'
                    WHEN PrisNum < 1000000 THEN '700k–1M'
                    ELSE 'Over 1M'
                END AS Gruppe,
                CASE
                    WHEN PrisNum < 200000  THEN 1
                    WHEN PrisNum < 300000  THEN 2
                    WHEN PrisNum < 400000  THEN 3
                    WHEN PrisNum < 500000  THEN 4
                    WHEN PrisNum < 700000  THEN 5
                    WHEN PrisNum < 1000000 THEN 6
                    ELSE 7
                END AS SortKey,
                COUNT(*) AS Antall,
                ROUND(AVG(Liggetid)) AS SnittDager,
                CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(
                    GROUP_CONCAT(Liggetid ORDER BY Liggetid SEPARATOR ','),
                    ',', FLOOR((COUNT(*)+1)/2)
                ), ',', -1) AS UNSIGNED) AS MedianDager
            FROM liggetid
            WHERE PrisNum > 0
            GROUP BY Gruppe, SortKey
            HAVING Antall >= 2
            ORDER BY SortKey
        """)
        per_prisklasse = cur.fetchall()

        # Totalt antall solgte med liggetid-data
        cur.execute(liggetid_cte + """
            SELECT COUNT(*) AS Antall, ROUND(AVG(Liggetid)) AS SnittDager
            FROM liggetid
        """)
        totalt = cur.fetchone()

        return {
            "per_merke": per_merke,
            "per_type": per_type,
            "per_prisklasse": per_prisklasse,
            "totalt": totalt,
        }
    except Exception as e:
        logger.error("Feil i get_liggetid_statistikk: %s\n%s", e, traceback.format_exc())
        return {"per_merke": [], "per_type": [], "per_prisklasse": [], "totalt": None}
    finally:
        conn.close()


def get_liggetid_for_annonse(finnkode: int) -> dict | None:
    """Returner median liggetid for annonser i samme segment som gitt finnkode."""
    conn = get_db()
    if not conn:
        return None
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT SvvMerke, Typebobil, CAST(REGEXP_REPLACE(Pris, '[^0-9]', '') AS UNSIGNED) AS PrisNum "
            "FROM bobil WHERE Finnkode = %s",
            (finnkode,)
        )
        ad = cur.fetchone()
        if not ad:
            return None

        merke = ad.get("SvvMerke")
        typebobil = ad.get("Typebobil")
        pris = ad.get("PrisNum") or 0

        prisklasse_cond = (
            "PrisNum < 200000" if pris < 200000 else
            "PrisNum < 300000" if pris < 300000 else
            "PrisNum < 400000" if pris < 400000 else
            "PrisNum < 500000" if pris < 500000 else
            "PrisNum < 700000" if pris < 700000 else
            "PrisNum < 1000000" if pris < 1000000 else
            "PrisNum >= 1000000"
        )

        liggetid_base = """
            SELECT DATEDIFF(
                       COALESCE(b.SolgtDato, sd.SolgtTidspunkt),
                       fs.ErstSett
                   ) AS Liggetid,
                   CAST(REGEXP_REPLACE(b.Pris, '[^0-9]', '') AS UNSIGNED) AS PrisNum,
                   b.SvvMerke, b.Typebobil
            FROM bobil b
            LEFT JOIN (
                SELECT Finnkode, MAX(Tidspunkt) AS SolgtTidspunkt
                FROM prisendringer WHERE Pris = 'Solgt/Fjernet' GROUP BY Finnkode
            ) sd ON b.Finnkode = sd.Finnkode
            JOIN (
                SELECT Finnkode, MIN(Tidspunkt) AS ErstSett
                FROM prisendringer WHERE Pris REGEXP '^[0-9]+$' GROUP BY Finnkode
            ) fs ON b.Finnkode = fs.Finnkode
            WHERE b.Solgt = 1
              AND COALESCE(b.SolgtDato, sd.SolgtTidspunkt) IS NOT NULL
              AND DATEDIFF(
                  COALESCE(b.SolgtDato, sd.SolgtTidspunkt),
                  fs.ErstSett
              ) BETWEEN 0 AND 730
        """

        result = {}

        # Snitt for samme merke
        if merke:
            cur.execute(
                f"SELECT COUNT(*) AS Antall, ROUND(AVG(Liggetid)) AS SnittDager "
                f"FROM ({liggetid_base}) AS t WHERE SvvMerke = %s",
                (merke,)
            )
            row = cur.fetchone()
            if row and row["Antall"] >= 2:
                result["merke"] = {"navn": merke, **row}

        # Snitt for samme type
        if typebobil and typebobil not in ("", "Ikke oppgitt"):
            cur.execute(
                f"SELECT COUNT(*) AS Antall, ROUND(AVG(Liggetid)) AS SnittDager "
                f"FROM ({liggetid_base}) AS t WHERE Typebobil = %s",
                (typebobil,)
            )
            row = cur.fetchone()
            if row and row["Antall"] >= 2:
                result["type"] = {"navn": typebobil, **row}

        # Snitt for samme prisklasse
        if pris > 0:
            cur.execute(
                f"SELECT COUNT(*) AS Antall, ROUND(AVG(Liggetid)) AS SnittDager "
                f"FROM ({liggetid_base}) AS t WHERE {prisklasse_cond}"
            )
            row = cur.fetchone()
            if row and row["Antall"] >= 2:
                result["prisklasse"] = {"navn": _prisklasse_navn(pris), **row}

        return result if result else None
    except Exception as e:
        logger.error("Feil i get_liggetid_for_annonse: %s\n%s", e, traceback.format_exc())
        return None
    finally:
        conn.close()


def _prisklasse_navn(pris: int) -> str:
    if pris < 200000:  return "under 200k"
    if pris < 300000:  return "200–300k"
    if pris < 400000:  return "300–400k"
    if pris < 500000:  return "400–500k"
    if pris < 700000:  return "500–700k"
    if pris < 1000000: return "700k–1M"
    return "over 1M"


def get_sokresultater(keywords_str):
    """View 4: Nøkkelord-søk i beskrivelse og annonsenavn."""
    if not keywords_str or not keywords_str.strip():
        return []
    conn = get_db()
    if not conn:
        return []
    try:
        terms = [t.strip() for t in keywords_str.split(",") if t.strip()]
        if not terms:
            return []

        conditions = " OR ".join(
            ["(b.Beskrivelse LIKE %s OR b.Annonsenavn LIKE %s)"] * len(terms)
        )
        params = []
        for t in terms:
            params.extend([f"%{t}%", f"%{t}%"])

        cur = conn.cursor(dictionary=True)
        cur.execute(f"""
            SELECT b.Finnkode, b.AutodbId, b.Kilde, b.Annonsenavn, b.Beskrivelse, b.Modell,
                   b.Kilometerstand, b.Girkasse, b.Nyttelast, b.Typebobil,
                   b.Oppdatert, b.Pris,
                   COUNT(p.Pris) AS AntallEndringer,
                   MIN(NULLIF(CAST(REGEXP_REPLACE(p.Pris, '[^0-9]', '') AS UNSIGNED), 0)) AS LavestePris,
                   MAX(NULLIF(CAST(REGEXP_REPLACE(p.Pris, '[^0-9]', '') AS UNSIGNED), 0)) AS HoyestePris
            FROM bobil b
            LEFT JOIN prisendringer p ON b.Finnkode = p.Finnkode
            WHERE {conditions}
            GROUP BY b.Finnkode, b.AutodbId, b.Kilde, b.Annonsenavn, b.Beskrivelse, b.Modell,
                     b.Kilometerstand, b.Girkasse, b.Nyttelast, b.Typebobil,
                     b.Oppdatert, b.Pris
            ORDER BY STR_TO_DATE(b.Oppdatert, '%d. %m. %Y %H:%i') DESC
        """, params)
        rows = cur.fetchall()

        for r in rows:
            enrich_row_with_prices(r)
            r["AdURL"] = _ad_url(r)
            r["Alder"], r["AlderClass"], r["AlderSort"] = format_age(r.get("Oppdatert", ""))
            tekst = f"{r['Annonsenavn']} {r.get('Beskrivelse', '')}".lower()
            r["Soketreff"] = ", ".join(t for t in terms if t.lower() in tekst)
        return rows
    except Exception as e:
        logger.error("Feil i get_sokresultater: %s\n%s", e, traceback.format_exc())
        return []
    finally:
        conn.close()


def get_filter_options():
    """Hent unike verdier for filterpanelet."""
    conn = get_db()
    if not conn:
        return {"modeller": [], "typer": [], "girkasser": [], "merker": []}
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT Modell FROM bobil WHERE Modell IS NOT NULL ORDER BY Modell DESC")
        modeller = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT DISTINCT Typebobil FROM bobil WHERE Typebobil IS NOT NULL AND Typebobil != 'Ikke oppgitt' ORDER BY Typebobil")
        typer = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT DISTINCT Girkasse FROM bobil WHERE Girkasse IS NOT NULL AND Girkasse != 'Ikke oppgitt' ORDER BY Girkasse")
        girkasser = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT Merke, COUNT(*) as n FROM bobil WHERE Merke IS NOT NULL AND (Solgt=0 OR Solgt IS NULL) GROUP BY Merke ORDER BY n DESC, Merke")
        merker = [r[0] for r in cur.fetchall()]
        return {"modeller": modeller, "typer": typer, "girkasser": girkasser, "merker": merker}
    except Exception:
        return {"modeller": [], "typer": [], "girkasser": [], "merker": []}
    finally:
        conn.close()


def get_detaljer(page=1, per_page=50, filters=None):
    """View 5: Detaljert oversikt med beregninger."""
    conn = get_db()
    if not conn:
        return [], 0
    try:
        cur = conn.cursor(dictionary=True)

        # Bygg WHERE-klausul basert på filtre
        where_parts = []
        params = []
        if filters:
            if filters.get("modell_fra"):
                where_parts.append("b.Modell >= %s")
                params.append(filters["modell_fra"])
            if filters.get("modell_til"):
                where_parts.append("b.Modell <= %s")
                params.append(filters["modell_til"])

            pris_fra = safe_int(filters.get("pris_fra"))
            if pris_fra is not None:
                where_parts.append("CAST(REGEXP_REPLACE(b.Pris, '[^0-9]', '') AS UNSIGNED) >= %s")
                params.append(pris_fra)
            pris_til = safe_int(filters.get("pris_til"))
            if pris_til is not None:
                where_parts.append("CAST(REGEXP_REPLACE(b.Pris, '[^0-9]', '') AS UNSIGNED) <= %s")
                params.append(pris_til)
            if filters.get("type"):
                where_parts.append("b.Typebobil = %s")
                params.append(filters["type"])
            if filters.get("girkasse"):
                where_parts.append("b.Girkasse = %s")
                params.append(filters["girkasse"])
            solgt_filter = filters.get("solgt_filter", "aktive")
            if solgt_filter == "aktive":
                where_parts.append("(b.Solgt = 0 OR b.Solgt IS NULL)")
            elif solgt_filter == "solgte":
                where_parts.append("b.Solgt = 1")
            min_nyttelast = safe_int(filters.get("min_nyttelast"))
            if min_nyttelast is not None:
                where_parts.append("b.SvvNyttelast >= %s")
                params.append(min_nyttelast)
            min_lengde = safe_int(filters.get("min_lengde"))
            if min_lengde is not None:
                where_parts.append("b.SvvLengde >= %s")
                params.append(min_lengde)
            max_lengde = safe_int(filters.get("max_lengde"))
            if max_lengde is not None:
                where_parts.append("b.SvvLengde <= %s")
                params.append(max_lengde)
            min_tilhengervekt = safe_int(filters.get("min_tilhengervekt"))
            if min_tilhengervekt is not None:
                where_parts.append("b.SvvTilhengervektMedBrems >= %s")
                params.append(min_tilhengervekt)
            if filters.get("sengelayout"):
                where_parts.append("b.Sengelayout = %s")
                params.append(filters["sengelayout"])
            merker_valgt = [m for m in filters.get("merker", []) if m]
            if merker_valgt:
                placeholders = ",".join(["%s"] * len(merker_valgt))
                where_parts.append(f"b.Merke IN ({placeholders})")
                params.extend(merker_valgt)

        where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""

        # Totalt antall med filter
        cur.execute(f"SELECT COUNT(*) AS total FROM bobil b {where_clause}", params)
        total = cur.fetchone()["total"]

        offset = (page - 1) * per_page
        cur.execute(f"""
            SELECT b.Finnkode, b.AutodbId, b.Kilde, b.Annonsenavn, b.Beskrivelse, b.Modell,
                   b.Kilometerstand, b.Girkasse, b.Nyttelast, b.Typebobil,
                   b.Oppdatert, b.Pris, b.URL, b.ImageURL, b.Lokasjon, b.Solgt, b.SistSett,
                   b.Sengelayout, b.Heftelser, b.HeftelseSjekket, b.HeftelserDetaljer,
                   COUNT(p.Pris) AS AntallEndringer,
                   MIN(NULLIF(CAST(REGEXP_REPLACE(p.Pris, '[^0-9]', '') AS UNSIGNED), 0)) AS LavestePris,
                   MAX(NULLIF(CAST(REGEXP_REPLACE(p.Pris, '[^0-9]', '') AS UNSIGNED), 0)) AS HoyestePris
            FROM bobil b
            LEFT JOIN prisendringer p ON b.Finnkode = p.Finnkode
            {where_clause}
            GROUP BY b.Finnkode, b.AutodbId, b.Kilde, b.Annonsenavn, b.Beskrivelse, b.Modell,
                     b.Kilometerstand, b.Girkasse, b.Nyttelast, b.Typebobil,
                     b.Oppdatert, b.Pris, b.URL, b.ImageURL, b.Lokasjon, b.Solgt, b.SistSett,
                     b.Sengelayout, b.Heftelser, b.HeftelseSjekket, b.HeftelserDetaljer
            ORDER BY STR_TO_DATE(b.Oppdatert, '%d. %m. %Y %H:%i') DESC
            LIMIT %s OFFSET %s
        """, params + [per_page, offset])
        rows = cur.fetchall()

        now = datetime.now()
        for r in rows:
            enrich_row_with_prices(r)
            pris = parse_price(r.get("Pris"))
            km = parse_km(r["Kilometerstand"])
            r["AdURL"] = _ad_url(r)

            # Sjekk om annonsen er ny (siste 24 timer)
            dato = parse_norwegian_date(r.get("Oppdatert", ""))
            r["ErNy"] = dato and (now - dato).total_seconds() < 86400
            r["Alder"], r["AlderClass"], r["AlderSort"] = format_age(r.get("Oppdatert", ""))

            # Pris per km
            if pris and km and km > 0:
                r["PrisPerKm"] = round(pris / km, 1)
            else:
                r["PrisPerKm"] = None

        return rows, total
    except Exception as e:
        logger.error("Feil i get_detaljer: %s\n%s", e, traceback.format_exc())
        return [], 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Scraper-integrasjon
# ---------------------------------------------------------------------------

def run_scraper_background():
    """Kjør scraperen i bakgrunnen."""
    if scraper_status["running"]:
        logger.info("Scraper kjører allerede.")
        return
    scraper_status["running"] = True
    try:
        # Importer her for å unngå sirkulær import ved modulnivå
        sys.path.insert(0, "/usr/bin")
        from bobil_v2 import run_scraper
        run_scraper()
        scraper_status["last_run"] = datetime.now()
        scraper_status["error"] = None
        logger.info("Scraping fullført.")
    except Exception as e:
        scraper_status["error"] = str(e)
        logger.error("Scraper feilet: %s", e)
    finally:
        scraper_status["running"] = False


def schedule_scraper(interval_hours=6):
    """Start periodisk scraping i bakgrunnstråd."""
    import time

    def loop():
        while True:
            logger.info("Starter planlagt scraping...")
            run_scraper_background()
            time.sleep(interval_hours * 3600)

    t = threading.Thread(target=loop, daemon=True, name="scraper-scheduler")
    t.start()
    logger.info("Scraper planlagt til å kjøre hver %d. time.", interval_hours)


# ---------------------------------------------------------------------------
# Flask-app
# ---------------------------------------------------------------------------

app = Flask(__name__)

# HTML-mal
TEMPLATE = """
<!DOCTYPE html>
<html lang="no">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Bobil — Markedsplassoversikt</title>
    <style>
        :root {
            --accent:       #0A84FF;
            --accent-dim:   rgba(10,132,255,0.15);
            --accent-mid:   rgba(10,132,255,0.35);
            --bg:           #000000;
            --bg-elevated:  #1C1C1E;
            --bg-grouped:   #2C2C2E;
            --separator:    rgba(255,255,255,0.08);
            --separator-op: rgba(255,255,255,0.14);
            --label:        #FFFFFF;
            --label-sec:    rgba(235,235,245,0.60);
            --label-ter:    rgba(235,235,245,0.30);
            --fill:         rgba(120,120,128,0.36);
            --green:        #30D158;
            --orange:       #FF9F0A;
            --red:          #FF453A;
            --radius-sm:    8px;
            --radius-md:    12px;
            --radius-lg:    16px;
        }
        *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
        html { -webkit-text-size-adjust: 100%; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Text', 'Helvetica Neue', sans-serif;
            background: var(--bg);
            color: var(--label);
            line-height: 1.5;
            min-height: 100vh;
            -webkit-font-smoothing: antialiased;
        }

        /* ── Layout ── */
        .container { max-width: 1280px; margin: 0 auto; padding: 20px 16px 40px; }

        /* ── Header ── */
        .app-header {
            display: flex;
            align-items: baseline;
            gap: 10px;
            margin-bottom: 24px;
        }
        .app-header h1 {
            font-size: 1.75rem;
            font-weight: 700;
            letter-spacing: -0.4px;
            color: var(--label);
        }
        .app-header .subtitle {
            font-size: 0.85rem;
            color: var(--label-sec);
            font-weight: 400;
        }

        /* ── Segmented control (tabs) ── */
        .tabs {
            display: flex;
            gap: 0;
            margin-bottom: 16px;
            background: var(--bg-grouped);
            border-radius: var(--radius-md);
            padding: 3px;
            overflow-x: auto;
            scrollbar-width: none;
            -ms-overflow-style: none;
        }
        .tabs::-webkit-scrollbar { display: none; }
        .tab {
            flex: 1;
            min-width: max-content;
            padding: 7px 14px;
            background: transparent;
            color: var(--label-sec);
            text-decoration: none;
            border-radius: 9px;
            font-size: 0.82rem;
            font-weight: 500;
            text-align: center;
            transition: background 0.18s ease, color 0.18s ease;
            white-space: nowrap;
            letter-spacing: -0.1px;
        }
        .tab:hover { color: var(--label); }
        .tab.active {
            background: var(--bg-elevated);
            color: var(--label);
            font-weight: 600;
            box-shadow: 0 1px 4px rgba(0,0,0,0.4), 0 0 0 0.5px var(--separator-op);
        }
        .tab-star {
            color: var(--orange);
        }
        .tab-star.active { color: var(--orange); }

        /* ── Content card ── */
        .content {
            background: var(--bg-elevated);
            border-radius: var(--radius-lg);
            padding: 0;
            overflow: hidden;
            border: 0.5px solid var(--separator-op);
        }
        .content-inner {
            padding: 16px 20px;
            overflow-x: auto;
        }

        /* ── Tables ── */
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.84rem;
        }
        thead { position: sticky; top: 0; z-index: 2; }
        th {
            background: var(--bg-elevated);
            color: var(--label-sec);
            padding: 10px 12px;
            text-align: left;
            font-weight: 600;
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.4px;
            white-space: nowrap;
            border-bottom: 0.5px solid var(--separator-op);
        }
        td {
            padding: 10px 12px;
            border-bottom: 0.5px solid var(--separator);
            vertical-align: middle;
            color: var(--label);
        }
        tbody tr:last-child td { border-bottom: none; }
        tbody tr:hover td { background: var(--fill); }
        a { color: var(--accent); text-decoration: none; }
        a:hover { text-decoration: underline; }

        /* ── Sortable headers ── */
        th.sortable { cursor: pointer; user-select: none; }
        th.sortable:hover { color: var(--label); }
        th.sortable::after { content: ' ⇅'; font-size: 0.65em; opacity: 0.3; }
        th.sort-asc::after  { content: ' ▲'; opacity: 0.7; }
        th.sort-desc::after { content: ' ▼'; opacity: 0.7; }

        /* ── Badges ── */
        .badge {
            display: inline-flex;
            align-items: center;
            padding: 2px 7px;
            border-radius: 20px;
            font-size: 0.68rem;
            font-weight: 600;
            letter-spacing: 0.2px;
            vertical-align: middle;
            margin-left: 4px;
        }
        .new-badge  { background: var(--orange);  color: #000; }
        .sold-badge { background: var(--red);      color: #fff; }
        .kilde-badge { border-radius: 5px; }
        .kilde-finn   { background: rgba(255,69,58,0.20);  color: #FF6961; border: 0.5px solid rgba(255,69,58,0.35); }
        .kilde-autodb { background: rgba(10,132,255,0.18); color: #409CFF; border: 0.5px solid rgba(10,132,255,0.35); }
        .kilde-both   { background: rgba(191,90,242,0.18); color: #DA8FFF; border: 0.5px solid rgba(191,90,242,0.35); }

        /* Source link badges */
        a.kilde-badge {
            display: inline-flex;
            align-items: center;
            gap: 3px;
            padding: 3px 8px;
            border-radius: 6px;
            font-size: 0.72rem;
            font-weight: 600;
            text-decoration: none;
            transition: opacity 0.15s;
        }
        a.kilde-badge:hover { opacity: 0.75; text-decoration: none; }

        /* ── Price colors ── */
        .price-down { color: var(--green); }
        .price-up   { color: var(--red); }
        .score      { font-weight: 700; display: inline-block; min-width: 2.2em; text-align: center; border-radius: 4px; padding: 1px 5px; }
        .score-high { background: #d4edda; color: #155724; }
        .score-mid  { background: #fff3cd; color: #856404; }
        .score-low  { background: #f8d7da; color: #721c24; }

        /* ── Age colors ── */
        .age-fresh   { color: var(--green); }
        .age-weeks   { color: var(--orange); }
        .age-old     { color: var(--red); }
        .age-unknown { color: var(--label-ter); }

        /* ── Keyword tags ── */
        .keyword-tag {
            display: inline-block;
            background: var(--accent-dim);
            color: var(--accent);
            padding: 2px 8px;
            border-radius: 20px;
            font-size: 0.75rem;
            margin: 1px 2px;
            font-weight: 500;
        }

        /* ── Thumbnails ── */
        .thumb {
            width: 72px;
            height: 54px;
            object-fit: cover;
            border-radius: var(--radius-sm);
            vertical-align: middle;
            background: var(--bg-grouped);
        }
        .detail-img {
            max-width: 520px;
            width: 100%;
            border-radius: var(--radius-md);
            margin-bottom: 20px;
        }

        /* ── Sold rows ── */
        tr.sold td { opacity: 0.4; }

        /* ── Filter panel ── */
        .filter-panel {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(130px, 1fr));
            gap: 8px 10px;
            margin-bottom: 16px;
            align-items: end;
        }
        .filter-group {
            display: flex;
            flex-direction: column;
            gap: 4px;
        }
        .filter-group label {
            font-size: 0.68rem;
            color: var(--label-sec);
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        .filter-group select,
        .filter-group input[type="number"],
        .search-form input {
            padding: 7px 10px;
            border-radius: var(--radius-sm);
            border: 0.5px solid var(--separator-op);
            background: var(--bg-grouped);
            color: var(--label);
            font-size: 0.83rem;
            font-family: inherit;
            width: 100%;
            -webkit-appearance: none;
            appearance: none;
            transition: border-color 0.15s;
        }
        .filter-group select:focus,
        .filter-group input:focus,
        .search-form input:focus {
            outline: none;
            border-color: var(--accent);
        }
        .filter-checkbox-label {
            display: flex;
            align-items: center;
            gap: 6px;
            font-size: 0.83rem;
            color: var(--label-sec);
            cursor: pointer;
            padding: 7px 0;
        }

        .filter-radio-group {
            display: flex;
            flex-direction: column;
            gap: 4px;
            padding: 4px 0;
        }
        .filter-radio-group label {
            display: flex;
            align-items: center;
            gap: 6px;
            font-size: 0.83rem;
            color: var(--label-sec);
            cursor: pointer;
        }
        .filter-group-merker { min-width: 160px; }
        .merke-cb-list {
            display: flex;
            flex-direction: column;
            gap: 3px;
            padding: 4px 0;
            max-height: 200px;
            overflow-y: auto;
        }
        .merke-cb-label {
            display: flex;
            align-items: center;
            gap: 6px;
            font-size: 0.83rem;
            color: var(--label-sec);
            cursor: pointer;
            white-space: nowrap;
        }

        /* ── Search ── */
        .search-form {
            display: flex;
            gap: 10px;
            margin-bottom: 16px;
        }
        .search-form input { flex: 1; }

        /* ── Buttons ── */
        .btn {
            background: var(--accent);
            color: #fff;
            border: none;
            padding: 8px 18px;
            border-radius: var(--radius-sm);
            cursor: pointer;
            font-weight: 600;
            font-size: 0.85rem;
            font-family: inherit;
            letter-spacing: -0.1px;
            transition: opacity 0.15s;
            -webkit-appearance: none;
        }
        .btn:hover   { opacity: 0.85; }
        .btn:active  { opacity: 0.7; }
        .btn:disabled { opacity: 0.35; cursor: not-allowed; }

        /* ── Pagination ── */
        .pagination {
            display: flex;
            gap: 6px;
            margin-top: 16px;
            justify-content: center;
        }
        .pagination a, .pagination span {
            padding: 6px 13px;
            border-radius: var(--radius-sm);
            border: 0.5px solid var(--separator-op);
            color: var(--accent);
            text-decoration: none;
            font-size: 0.83rem;
            font-weight: 500;
            background: var(--bg-grouped);
            transition: background 0.15s;
        }
        .pagination a:hover { background: var(--accent-dim); text-decoration: none; }
        .pagination .current {
            background: var(--accent);
            color: #fff;
            border-color: var(--accent);
            font-weight: 700;
        }

        /* ── No data ── */
        .no-data {
            color: var(--label-ter);
            font-style: italic;
            text-align: center;
            padding: 48px 20px;
            font-size: 0.9rem;
        }

        /* ── Statistikk-panel ── */
        .stat-header { margin-bottom: 24px; }
        .stat-header h2 { margin-bottom: 6px; }
        .stat-ingress { color: var(--label-pri); margin-bottom: 4px; }
        .stat-note { color: var(--label-ter); font-size: 0.82rem; font-style: italic; }
        .stat-section { margin-bottom: 32px; }
        .stat-section h3 { font-size: 1rem; font-weight: 600; margin-bottom: 8px; color: var(--label-sec); }
        .stat-table { width: 100%; border-collapse: collapse; font-size: 0.88rem; }
        .stat-table th, .stat-table td { padding: 6px 10px; border-bottom: 1px solid var(--sep); text-align: left; }
        .stat-table th { font-weight: 600; color: var(--label-sec); }
        .stat-table td.num { text-align: right; font-variant-numeric: tabular-nums; }
        .stat-empty { padding: 48px 20px; text-align: center; }
        .liggetid-hint { font-size: 0.8rem; color: var(--label-ter); margin-top: 4px; }
        .liggetid-hint strong { color: var(--label-sec); }
        .liggetid-box { background: var(--card-bg); border: 1px solid var(--sep); border-radius: 10px; padding: 12px 16px; margin: 12px 0; }
        .liggetid-box .lbl { display: block; font-size: 0.78rem; font-weight: 600; color: var(--label-ter); text-transform: uppercase; letter-spacing: 0.04em; margin-bottom: 6px; }
        .liggetid-list { margin: 0 0 6px 0; padding-left: 18px; font-size: 0.88rem; color: var(--label-pri); }
        .liggetid-list li { margin-bottom: 2px; }
        .liggetid-note { font-size: 0.78rem; color: var(--label-ter); }

        /* ── Truncate / nowrap ── */
        .truncate {
            max-width: 280px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .nowrap { white-space: nowrap; }
        .inline-form { display: inline; }
        .row-divider td { border-top: 2px solid var(--separator-op) !important; }
        .mt-4 { margin-top: 4px; }

        /* ── Status bar ── */
        .status-bar {
            margin-top: 14px;
            padding: 12px 18px;
            background: var(--bg-elevated);
            border-radius: var(--radius-md);
            border: 0.5px solid var(--separator-op);
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 10px;
            font-size: 0.8rem;
            color: var(--label-sec);
        }

        /* ── Section headers inside content ── */
        .section-header {
            padding: 14px 20px 8px;
            font-size: 0.72rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            color: var(--label-sec);
            border-bottom: 0.5px solid var(--separator);
        }

        /* ── Detail page ── */
        .detail-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 0;
        }
        .detail-row {
            display: contents;
        }
        .detail-row dt,
        .detail-row dd {
            padding: 9px 20px;
            border-bottom: 0.5px solid var(--separator);
            font-size: 0.875rem;
        }
        .detail-row dt { color: var(--label-sec); font-weight: 500; }
        .detail-row dd { color: var(--label); }

        .info-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 8px 24px;
            margin-bottom: 20px;
            font-size: 0.9em;
        }
        .info-grid .lbl { color: var(--label-sec); }

        .svv-panel {
            background: var(--bg-grouped);
            border: 0.5px solid var(--separator-op);
            padding: 16px;
            border-radius: var(--radius-md);
            margin-bottom: 20px;
        }
        .svv-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 8px 24px;
            font-size: 0.9em;
        }
        .svv-grid .lbl { color: var(--label-sec); }

        .section-heading {
            color: var(--accent);
            margin: 20px 0 10px;
            font-size: 1.05rem;
            font-weight: 600;
            letter-spacing: -0.2px;
        }

        .kjennemerke-hint {
            font-size: 0.85em;
            color: var(--label-sec);
            margin: 10px 0 20px;
            padding: 10px 16px;
            background: var(--bg-grouped);
            border: 0.5px solid var(--separator-op);
            border-radius: var(--radius-sm);
        }

        /* ── Detail-side navigasjon og layout ── */
        .detail-nav {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
            font-size: 0.85em;
        }
        .detail-title {
            display: flex;
            align-items: center;
            gap: 12px;
            margin-bottom: 10px;
            color: var(--label);
        }
        .fav-btn {
            background: none;
            border: none;
            font-size: 1.5em;
            cursor: pointer;
            line-height: 1;
            padding: 0;
            transition: transform 0.15s;
        }
        .fav-btn:hover { transform: scale(1.2); }
        .notat-section {
            margin-bottom: 20px;
            padding: 14px 16px;
            background: var(--bg-grouped);
            border: 0.5px solid var(--separator-op);
            border-radius: var(--radius-md);
        }
        .notat-label {
            font-size: 0.72rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            color: var(--label-sec);
            margin-bottom: 8px;
        }
        .notat-textarea {
            width: 100%;
            max-width: 600px;
            background: var(--bg-elevated);
            color: var(--label);
            border: 0.5px solid var(--separator-op);
            border-radius: var(--radius-sm);
            padding: 8px 10px;
            font-size: 0.9em;
            resize: vertical;
            font-family: inherit;
            transition: border-color 0.15s;
        }
        .notat-textarea:focus { outline: none; border-color: var(--accent); }
        .notat-save-row {
            display: flex;
            align-items: center;
            gap: 10px;
            margin-top: 8px;
        }
        .notat-status {
            font-size: 0.8em;
            color: var(--label-sec);
        }
        .beskrivelse {
            color: var(--label-sec);
            font-size: 0.85em;
            line-height: 1.6;
            margin-bottom: 20px;
            white-space: pre-wrap;
        }
        .chart-container {
            max-width: 700px;
            margin-bottom: 20px;
        }
        .prishistorikk-tabell {
            max-width: 500px;
        }
        .note-secondary {
            color: var(--label-sec);
            font-size: 0.85em;
            font-style: italic;
        }

        /* ── Heftelse pills ── */
        .heft-pill {
            display: inline-flex;
            align-items: center;
            gap: 4px;
            padding: 2px 8px;
            border-radius: 20px;
            font-size: 0.75rem;
            font-weight: 600;
        }
        .heft-ok    { background: rgba(48,209,88,0.15);  color: var(--green);  }
        .heft-warn  { background: rgba(255,159,10,0.15); color: var(--orange); }
        .heft-high  { background: rgba(255,69,58,0.18);  color: var(--red);    }
        .heft-none  { color: var(--label-ter); font-size: 0.8rem; }

        /* ── Heftelse detail items ── */
        .heft-item { margin-bottom: 10px; padding: 10px 12px; border-radius: var(--radius-sm); border-left: 3px solid; }
        .heft-item-high   { background: rgba(255,69,58,0.08);  border-color: var(--red); }
        .heft-item-medium { background: rgba(255,159,10,0.08); border-color: var(--orange); }
        .heft-item-low    { background: rgba(120,120,128,0.1); border-color: var(--separator-op); }
        .heft-item-type   { font-weight: 600; font-size: 0.9em; }
        .heft-type-high   { color: var(--red); }
        .heft-type-medium { color: var(--orange); }
        .heft-type-low    { color: var(--label-sec); }
        .heft-item-meta   { font-size: 0.8em; color: var(--label-sec); margin-top: 2px; }
        .heft-item-krav        { font-size: 0.82em; color: var(--label); margin-top: 2px; }
        .heft-item-salgspant   { font-size: 0.82em; color: #155724; background: rgba(40,167,69,0.1); border-radius: 4px; padding: 3px 6px; margin-top: 4px; }
        .salgspant-hint        { font-size: 0.78em; color: #155724; background: rgba(40,167,69,0.12); border-radius: 4px; padding: 1px 5px; margin-left: 4px; white-space: nowrap; }
        .selger-privat         { font-size: 0.78em; background: rgba(120,120,128,0.12); color: var(--label-sec); border-radius: 4px; padding: 1px 6px; }
        .selger-forhandler     { font-size: 0.78em; background: rgba(10,132,255,0.1); color: var(--blue); border-radius: 4px; padding: 1px 6px; }
        .salgspris-box         { background: rgba(10,132,255,0.06); border: 1px solid rgba(10,132,255,0.2); border-radius: var(--radius-sm); padding: 10px 14px; margin-top: 8px; }
        .salgspris-row         { display: flex; gap: 20px; flex-wrap: wrap; margin-top: 4px; }
        .salgspris-item        { display: flex; flex-direction: column; }
        .salgspris-label       { font-size: 0.75em; color: var(--label-sec); text-transform: uppercase; letter-spacing: 0.04em; }
        .salgspris-value       { font-size: 1.05em; font-weight: 600; color: var(--label); }
        .salgspris-note        { font-size: 0.75em; color: var(--label-sec); margin-top: 6px; }

        /* ── Prisfall indikator ── */
        .prisfall-cell { white-space: nowrap; }
        .prisfall-pil  { color: var(--green); font-weight: 700; }
        .prisfall-kr   { font-weight: 600; color: var(--green); }
        .prisfall-pct  { font-size: 0.78em; color: var(--label-sec); margin-left: 3px; }

        /* ── Thumb-kolonne ── */
        .thumb-cell { width: 56px; padding: 6px 8px 6px 4px !important; }
        .thumb {
            width: 52px;
            height: 39px;
            object-fit: cover;
            border-radius: 6px;
            display: block;
            background: var(--bg-grouped);
        }

        /* ── Inline notat-felt i Mine biler ── */
        .notat-inline-textarea {
            width: 200px;
            background: var(--bg);
            color: var(--label);
            border: 0.5px solid var(--separator-op);
            border-radius: var(--radius-sm);
            padding: 4px 6px;
            font-size: 0.83em;
            font-family: inherit;
            resize: vertical;
        }
        .notat-vis {
            cursor: pointer;
            color: var(--label-sec);
            font-size: 0.85em;
        }
        .notat-vis:hover { color: var(--label); }

        /* ── Ekstern lenke-rad på detaljside ── */
        .ext-links { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 12px; }

        /* ── Btn variants ── */
        .btn-sm   { padding: 5px 12px; font-size: 0.78rem; }
        .btn-danger { background: var(--red); }
        .btn-ghost {
            background: transparent;
            color: var(--accent);
            border: 0.5px solid var(--accent);
        }
        .btn-ghost:hover { background: var(--accent-dim); opacity: 1; }

        /* ── Mobile ── */
        @media (max-width: 680px) {
            .container { padding: 12px 10px 32px; }
            .app-header h1 { font-size: 1.4rem; }
            .tabs { border-radius: var(--radius-sm); }
            .tab { padding: 6px 10px; font-size: 0.78rem; }
            table { font-size: 0.78rem; }
            th { padding: 8px 8px; font-size: 0.68rem; }
            td { padding: 8px 8px; }
            .filter-panel { flex-direction: column; gap: 8px; }
            .filter-group { width: 100%; }
            .filter-group select,
            .filter-group input[type="number"] { width: 100%; }
            .status-bar { flex-direction: column; text-align: center; }
            .thumb { width: 56px; height: 42px; }
            .truncate { max-width: 160px; }
            .mobile-cards table { display: none; }
            .mobile-cards .card-list { display: block; }
        }
        @media (min-width: 681px) {
            .mobile-cards .card-list { display: none; }
        }

        /* ── Mobile cards ── */
        .card-list { display: none; }
        .card {
            background: var(--bg-grouped);
            border: 0.5px solid var(--separator-op);
            border-radius: var(--radius-md);
            padding: 14px;
            margin-bottom: 8px;
        }
        .card-header {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 8px;
            gap: 8px;
        }
        .card-header a {
            font-weight: 600;
            font-size: 0.92rem;
            line-height: 1.3;
            flex: 1;
        }
        .card-details {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 4px 16px;
            font-size: 0.8rem;
        }
        .card-detail-label { color: var(--label-sec); }
    </style>
</head>
<body>
    <div class="container">
        <div class="app-header">
            <h1>Bobil</h1>
            <span class="subtitle">Finn.no · AutoDB oversikt</span>
        </div>
        <nav class="tabs">
            <a href="{{ bp }}annonser" class="tab {{ 'active' if active_tab == 'annonser' }}">Annonser</a>
            <a href="{{ bp }}prisutvikling" class="tab {{ 'active' if active_tab == 'prisutvikling' }}">Prisutvikling</a>
            <a href="{{ bp }}sok" class="tab {{ 'active' if active_tab == 'sok' }}">Søk</a>
            <a href="{{ bp }}detaljer" class="tab {{ 'active' if active_tab == 'detaljer' }}">Detaljert</a>
            <a href="{{ bp }}statistikk" class="tab {{ 'active' if active_tab == 'statistikk' }}">Markedsdata</a>
            <a href="{{ bp }}mine-biler" class="tab tab-star {{ 'active' if active_tab == 'mine-biler' }}">★ Mine biler</a>
        </nav>

        <div class="content">
            <div class="content-inner">
                {{ content|safe }}
            </div>
        </div>

        <div class="status-bar">
            <span>
                {{ total_listings }} annonser
                {% if last_scrape %}&nbsp;·&nbsp;Oppdatert {{ last_scrape }}{% endif %}
                {% if scraper_running %}&nbsp;·&nbsp;Scraping pågår…{% endif %}
            </span>
            <form method="POST" action="{{ bp }}scrape" class="inline-form">
                <button type="submit" class="btn" {{ 'disabled' if scraper_running }}>Oppdater nå</button>
            </form>
        </div>
    </div>
    <script>
        document.querySelectorAll('th.sortable').forEach(th => {
            th.addEventListener('click', () => {
                const table = th.closest('table');
                const tbody = table.querySelector('tbody');
                const idx = Array.from(th.parentNode.children).indexOf(th);
                const type = th.dataset.sort || 'string';
                const rows = Array.from(tbody.querySelectorAll('tr'));
                const isAsc = th.classList.contains('sort-asc');
                table.querySelectorAll('th').forEach(h => h.classList.remove('sort-asc', 'sort-desc'));
                th.classList.add(isAsc ? 'sort-desc' : 'sort-asc');
                const dir = isAsc ? -1 : 1;
                rows.sort((a, b) => {
                    const aCell = a.children[idx];
                    const bCell = b.children[idx];
                    let aVal = aCell?.textContent.trim() || '';
                    let bVal = bCell?.textContent.trim() || '';
                    if (type === 'number') {
                        const aNum = parseFloat(aCell?.dataset.sortValue ?? aVal.replace(/[^\\d.-]/g, '')) || 0;
                        const bNum = parseFloat(bCell?.dataset.sortValue ?? bVal.replace(/[^\\d.-]/g, '')) || 0;
                        return (aNum - bNum) * dir;
                    }
                    return aVal.localeCompare(bVal, 'no') * dir;
                });
                rows.forEach(row => tbody.appendChild(row));
            });
        });
    </script>
</body>
</html>
"""


def _eu_kontroll_html(frist_str: str, sist_str: str) -> str:
    """Returner HTML for EU-kontroll-raden med fremheving basert på gjenstående tid."""
    now = datetime.now().date()
    frist_html = frist_str or "—"
    sist_html = sist_str or "—"
    stil = ""
    merknad = ""
    if frist_str:
        try:
            frist = datetime.strptime(frist_str[:10], "%Y-%m-%d").date()
            mnd = (frist.year - now.year) * 12 + (frist.month - now.month)
            if mnd < 0:
                stil = "color:#c0392b;font-weight:bold"
                merknad = " &#x26A0; Utløpt!"
            elif mnd < 3:
                stil = "color:#c0392b;font-weight:bold"
                merknad = f" &#x26A0; Om {mnd} mnd"
            elif mnd < 12:
                stil = "color:#e67e22;font-weight:600"
                merknad = f" — om {mnd} mnd"
            else:
                ar = mnd // 12
                rest = mnd % 12
                stil = "color:#27ae60"
                merknad = f" — om {ar} år" if rest == 0 else f" — om ca. {ar} år {rest} mnd"
            frist_html = f'<span style="{stil}">{frist_str}{merknad}</span>'
        except (ValueError, TypeError):
            pass
    if sist_str:
        try:
            sist = datetime.strptime(sist_str[:10], "%Y-%m-%d").date()
            mnd_siden = (now.year - sist.year) * 12 + (now.month - sist.month)
            sist_html = f"{sist_str} (for {mnd_siden} mnd siden)"
        except (ValueError, TypeError):
            pass
    return frist_html, sist_html


_HEFTELSE_RISIKO_TYPER = {
    "rettsstiftelsestype.utp": "high",   # Utleggspant (namsfogd/tvang)
    "rettsstiftelsestype.sap": "medium", # Salgspant (vanlig bilfinansiering)
    "rettsstiftelsestype.lea": "medium", # Leasing
}


def _salgspant_alder_tekst(dato_str: str) -> str:
    """Returner menneskelig alder-tekst for en salgspant-dato, eller tom streng."""
    if not dato_str:
        return ""
    try:
        dato = datetime.strptime(dato_str[:10], "%Y-%m-%d").date()
        today = datetime.now().date()
        maneder = (today.year - dato.year) * 12 + (today.month - dato.month)
        if maneder < 1:
            return "denne måneden"
        if maneder == 1:
            return "for 1 mnd siden"
        if maneder < 24:
            return f"for {maneder} mnd siden"
        ar = round(maneder / 12)
        return f"for {ar} år siden"
    except (ValueError, TypeError):
        return ""


def _heftelse_badge(antall, detaljer_json=None) -> str:
    """Kompakt pill-badge for tabellvisning med risikofarge."""
    if antall is None:
        return '<span class="heft-none">— ikke sjekket</span>'
    if antall == 0:
        return '<span class="heft-pill heft-ok">✓ Ingen</span>'

    detaljer = []
    if detaljer_json:
        try:
            detaljer = json.loads(detaljer_json) if isinstance(detaljer_json, str) else detaljer_json
        except (ValueError, TypeError):
            pass

    har_utlegg = any(rs.get("type_kode") == "rettsstiftelsestype.utp" for rs in detaljer)
    label = f"⚠ {antall}" if har_utlegg else f"△ {antall}"
    cls = "heft-high" if har_utlegg else "heft-warn"
    enhet = "heftelse" if antall == 1 else "heftelser"
    badge = f'<span class="heft-pill {cls}">{label} {enhet}</span>'

    # Vis salgspant-hint om det finnes en nylig registrert salgspant (< 36 mnd)
    salgspant = [rs for rs in detaljer if rs.get("type_kode") == "rettsstiftelsestype.sap"]
    if salgspant:
        nyeste = sorted(salgspant, key=lambda r: r.get("dato", ""), reverse=True)[0]
        dato = nyeste.get("dato", "")
        alder = _salgspant_alder_tekst(dato)
        belop_liste = nyeste.get("belop", [])
        belop_str = ""
        if belop_liste:
            b = belop_liste[0]
            belop_str = f" · ~{int(b['belop']):,} kr".replace(",", " ")
        if alder and dato >= (datetime.now().date() - timedelta(days=365 * 3)).isoformat():
            badge += f' <span class="salgspant-hint" title="Salgspant registrert {dato}">🔑 Kjøpt {alder}{belop_str}</span>'

    return badge


def _heftelse_html(antall, sjekket_dato, detaljer_json=None) -> str:
    """Formater heftelsesresultat for detaljside — viser full tinglysningsliste."""
    if antall is None:
        return '<span class="heft-none">Ikke sjekket ennå</span>'
    if antall == 0:
        return '<span class="heft-pill heft-ok">✓ Ingen heftelser</span>'

    detaljer = []
    if detaljer_json:
        try:
            detaljer = json.loads(detaljer_json) if isinstance(detaljer_json, str) else detaljer_json
        except (ValueError, TypeError):
            pass

    if not detaljer:
        enhet = "heftelse" if antall == 1 else "heftelser"
        return f'<span class="heft-pill heft-high">⚠ {antall} {enhet}</span>'

    lines = []
    for rs in detaljer:
        risiko = _HEFTELSE_RISIKO_TYPER.get(rs.get("type_kode", ""), "low")
        if risiko == "high":
            ikon = "🚨"
            item_cls = "heft-item-high"
            type_color = "var(--red)"
        elif risiko == "medium":
            ikon = "⚠️"
            item_cls = "heft-item-medium"
            type_color = "var(--orange)"
        else:
            ikon = "ℹ️"
            item_cls = "heft-item-low"
            type_color = "var(--label-sec)"

        roller_tekst = " · ".join(
            f"{r['rolle']}: {r['navn']}" for r in rs.get("roller", [])
        )
        belop_tekst = " / ".join(
            f"{b['belop']:,.0f} {b['valuta']}".replace(",", " ")
            for b in rs.get("belop", [])
        )
        type_cls = {"high": "heft-type-high", "medium": "heft-type-medium", "low": "heft-type-low"}.get(risiko, "heft-type-low")
        krav_html = f'<div class="heft-item-krav">Krav: {esc(belop_tekst)}</div>' if belop_tekst else ""

        # Salgspant-signal: vis alder og beløp som klartekst kjøpsindikator
        salgspant_signal = ""
        if rs.get("type_kode") == "rettsstiftelsestype.sap":
            alder = _salgspant_alder_tekst(rs.get("dato", ""))
            belop_liste = rs.get("belop", [])
            if alder and belop_liste:
                kjopesum = f"{int(belop_liste[0]['belop']):,} kr".replace(",", " ")
                salgspant_signal = (
                    f'<div class="heft-item-salgspant">'
                    f'🔑 Registrert {alder} — indikerer sannsynlig kjøpesum: ~{kjopesum}'
                    f'</div>'
                )
            elif alder:
                salgspant_signal = (
                    f'<div class="heft-item-salgspant">'
                    f'🔑 Registrert {alder} — indikerer nylig kjøp'
                    f'</div>'
                )

        lines.append(
            f'<div class="heft-item {item_cls}">'
            f'<div class="heft-item-type {type_cls}">{ikon} {esc(rs["type"])}</div>'
            f'<div class="heft-item-meta">Dok.nr {esc(rs["dok"])} · {esc(rs["dato"])}</div>'
            f'<div class="heft-item-meta">{esc(roller_tekst)}</div>'
            f'{krav_html}'
            f'{salgspant_signal}'
            f'</div>'
        )
    return "\n".join(lines)


_RABATT_PER_MODELLAAR = {
    # Fra analyse av 78 solgte biler med prishistorikk
    2019: 0.093,
    2018: 0.093,
    2017: 0.055,
    2016: 0.055,
    2015: 0.055,
}
_RABATT_SNITT = 0.059
_RABATT_AGGRESSIV = 0.10


def beregn_forventet_salgspris(pris: int | None, modell: int | None) -> dict | None:
    """Returner estimert salgsintervall basert på kalibrerte rabattsatser."""
    if not pris or pris < 10000:
        return None
    ar = int(modell) if modell else None
    if ar:
        snitt_rabatt = _RABATT_PER_MODELLAAR.get(ar, _RABATT_SNITT)
    else:
        snitt_rabatt = _RABATT_SNITT
    forsiktig = round(pris * (1 - snitt_rabatt / 2) / 1000) * 1000
    realistisk = round(pris * (1 - snitt_rabatt) / 1000) * 1000
    aggressivt = round(pris * (1 - _RABATT_AGGRESSIV) / 1000) * 1000
    return {
        "forsiktig": forsiktig,
        "realistisk": realistisk,
        "aggressivt": aggressivt,
        "snitt_rabatt_pct": round(snitt_rabatt * 100, 1),
        "ar_kalibrert": ar in _RABATT_PER_MODELLAAR if ar else False,
    }


def _liggetid_html(data: dict | None) -> str:
    """Render markedssammenligning-blokk for annonsedetalj."""
    if not data:
        return ""
    linjer = []
    if "merke" in data:
        d = data["merke"]
        linjer.append(f"<strong>{esc(d['navn'])}</strong>: snitt {d['SnittDager']} dager ({d['Antall']} solgte)")
    if "type" in data:
        d = data["type"]
        linjer.append(f"Type <strong>{esc(d['navn'])}</strong>: snitt {d['SnittDager']} dager ({d['Antall']} solgte)")
    if "prisklasse" in data:
        d = data["prisklasse"]
        linjer.append(f"Prisklasse <strong>{esc(d['navn'])}</strong>: snitt {d['SnittDager']} dager ({d['Antall']} solgte)")
    if not linjer:
        return ""
    items = "".join(f"<li>{l}</li>" for l in linjer)
    return f"""
    <div class="liggetid-box">
        <span class="lbl">Liggetid — sammenlignbare biler</span>
        <ul class="liggetid-list">{items}</ul>
        <div class="liggetid-note">Basert på solgte annonser vi har sporet. <a href="../statistikk">Se full markedsstatistikk →</a></div>
    </div>
    """


def _selger_html(ad: dict) -> str:
    """Formater selger-info for detaljside."""
    kilde = ad.get("Kilde") or "finn"
    navn = ad.get("SelgerNavn") or ""
    stype = ad.get("SelgerType") or ""
    org_id = ad.get("SelgerOrgId") or ""

    if not navn and not stype and not org_id:
        return "—"

    er_privat = stype.lower() == "privat"
    type_badge = (
        '<span class="selger-privat">Privat</span>' if er_privat
        else '<span class="selger-forhandler">Forhandler</span>' if stype
        else ""
    )

    if navn:
        if kilde == "autodb" and org_id:
            lenke = f'<a href="https://www.autodb.no/forhandler/{org_id}" target="_blank" rel="noopener">{esc(navn)}</a>'
        elif kilde in ("finn", "finn+autodb") and org_id and not er_privat:
            lenke = f'<a href="https://www.finn.no/shops/{org_id}" target="_blank" rel="noopener">{esc(navn) or "Finn-forhandler"}</a>'
        else:
            lenke = esc(navn)
        return f'{lenke} {type_badge}'.strip()

    if not er_privat and org_id and kilde in ("finn", "finn+autodb"):
        lenke = f'<a href="https://www.finn.no/shops/{org_id}" target="_blank" rel="noopener">Se forhandler på Finn</a>'
        return f'{lenke} {type_badge}'.strip()

    return type_badge or "—"


def _kilde_badge(kilde):
    """Render kilde-badge: [F] for finn.no, [A] for autodb, [F+A] for begge."""
    if not kilde or kilde == "finn":
        return '<span class="kilde-badge kilde-finn">F</span>'
    if kilde == "autodb":
        return '<span class="kilde-badge kilde-autodb">A</span>'
    if kilde in ("finn+autodb", "autodb+finn"):
        return '<span class="kilde-badge kilde-both">F+A</span>'
    return ""


def _ad_url(row):
    """Primær ekstern lenke for en annonse — finn.no eller autodb."""
    kilde = row.get("Kilde") or "finn"
    try:
        finnkode = int(row.get("Finnkode") or 0)
    except (TypeError, ValueError):
        finnkode = 0
    try:
        autodb_id = int(row.get("AutodbId") or 0)
    except (TypeError, ValueError):
        autodb_id = 0
    if kilde == "autodb" and autodb_id:
        return f"https://www.autodb.no/view/{autodb_id}"
    if finnkode > 0:
        return f"https://www.finn.no/mobility/item/{finnkode}"
    if autodb_id:
        return f"https://www.autodb.no/view/{autodb_id}"
    return "#"


def _kilde_lenker(row):
    """HTML med én eller to eksterne lenker avhengig av kilde."""
    kilde = row.get("Kilde") or "finn"
    try:
        finnkode = int(row.get("Finnkode") or 0)
    except (TypeError, ValueError):
        finnkode = 0
    try:
        autodb_id = int(row.get("AutodbId") or 0)
    except (TypeError, ValueError):
        autodb_id = 0

    finn_lenke = ""
    autodb_lenke = ""
    if finnkode > 0:
        finn_lenke = f'<a href="https://www.finn.no/mobility/item/{finnkode}" target="_blank" class="kilde-badge kilde-finn">finn.no ↗</a>'
    if autodb_id:
        autodb_lenke = f'<a href="https://www.autodb.no/view/{autodb_id}" target="_blank" class="kilde-badge kilde-autodb">autodb ↗</a>'

    if kilde == "autodb":
        return autodb_lenke
    if kilde == "finn+autodb":
        return finn_lenke + " " + autodb_lenke
    return finn_lenke


def render_page(active_tab, content_html, base_path=""):
    """Render en side med felles layout."""
    last_scrape = None
    if scraper_status["last_run"]:
        last_scrape = scraper_status["last_run"].strftime("%d.%m.%Y %H:%M")
    return render_template_string(
        TEMPLATE,
        active_tab=active_tab,
        content=content_html,
        bp=base_path,
        total_listings=get_total_count(),
        last_scrape=last_scrape,
        scraper_running=scraper_status["running"],
    )


@app.route("/")
def index():
    return redirect("annonser")


@app.route("/prisendringer")
@app.route("/kjopsscore")
@app.route("/annonser")
def view_annonser():
    rows = get_annonser()
    if not rows:
        return render_page("annonser", '<p class="no-data">Ingen annonser funnet.</p>')

    html = """
    <table>
        <thead>
            <tr>
                <th class="sortable" data-sort="number">Score</th>
                <th class="sortable">Annonse</th>
                <th class="sortable" data-sort="number">Modell</th>
                <th class="sortable" data-sort="number">Pris</th>
                <th class="sortable" data-sort="number">Prisfall</th>
                <th class="sortable" data-sort="number">Nyttelast</th>
                <th class="sortable" data-sort="number">Endringer</th>
                <th class="sortable" data-sort="number">Dager</th>
                <th class="sortable sort-desc" data-sort="number">Sist endret</th>
            </tr>
        </thead>
        <tbody>
    """
    for r in rows:
        ny_badge = '<span class="new-badge">NY</span>' if r.get("ErNy") else ""
        score = r.get("KjopsScore", 0)
        score_cls = "score-high" if score >= 70 else ("score-mid" if score >= 40 else "score-low")
        nyttelast = f"{r['SvvNyttelast']} kg" if r.get('SvvNyttelast') else '—'
        html += f"""
            <tr>
                <td><span class="score {score_cls}">{score}</span></td>
                <td class="truncate"><a href="annonse/{esc(r['Finnkode'])}">{esc(r['Annonsenavn'])}</a>{ny_badge}{_kilde_badge(r.get('Kilde'))}</td>
                <td>{esc(r['Modell'])}</td>
                <td>{esc(r['NaaverendePris'])}</td>
                <td>{r.get('PrisfallHtml') or '<span class="note-secondary">—</span>'}</td>
                <td>{nyttelast}</td>
                <td><strong>{esc(r['AntallEndringer'])}</strong></td>
                <td>{esc(r['DagerPaaMarkedet'])}</td>
                <td class="{esc(r['AlderClass'])}" data-sort-value="{esc(r['AlderSort'])}">{esc(r['Alder'])}</td>
            </tr>
        """
    html += "</tbody></table>"
    return render_page("annonser", html)


@app.route("/prisutvikling")
def view_prisutvikling():
    rows = get_prisutvikling()
    if not rows:
        return render_page("prisutvikling", '<p class="no-data">Ingen prisdata funnet.</p>')

    html = """
    <table>
        <thead>
            <tr>
                <th class="sortable" data-sort="number">Modellår</th>
                <th class="sortable">Periode</th>
                <th class="sortable" data-sort="number">Gj.snittspris</th>
                <th class="sortable" data-sort="number">Datapunkter</th>
            </tr>
        </thead>
        <tbody>
    """
    prev_modell = None
    for r in rows:
        modell_display = r["Modell"] if r["Modell"] != prev_modell else ""
        row_cls = ' class="row-divider"' if modell_display else ""
        html += f"""
            <tr{row_cls}>
                <td><strong>{esc(modell_display)}</strong></td>
                <td>{esc(r['Periode'])}</td>
                <td>{esc(r['GjSnittPrisF'])}</td>
                <td>{esc(r['Antall'])}</td>
            </tr>
        """
        prev_modell = r["Modell"]
    html += "</tbody></table>"
    return render_page("prisutvikling", html)


@app.route("/statistikk")
def view_statistikk():
    data = get_liggetid_statistikk()
    totalt = data.get("totalt") or {}
    per_merke = data.get("per_merke", [])
    per_type = data.get("per_type", [])
    per_prisklasse = data.get("per_prisklasse", [])

    antall_totalt = totalt.get("Antall", 0)
    snitt_totalt = totalt.get("SnittDager")

    if antall_totalt == 0:
        return render_page("statistikk", """
            <div class="stat-empty">
                <h2>Markedsdata — liggetid</h2>
                <p class="no-data">Ikke nok data ennå. Statistikken bygges opp etter hvert som annonser blir solgt og fjernet fra markedet.</p>
            </div>
        """)

    def _tabell(tittel, rader, gruppe_label):
        if not rader:
            return ""
        rows_html = "".join(
            f"<tr>"
            f"<td>{esc(r['Gruppe'])}</td>"
            f"<td class='num'>{r['MedianDager']}</td>"
            f"<td class='num'>{r['SnittDager']}</td>"
            f"<td class='num'>{r['Antall']}</td>"
            f"</tr>"
            for r in rader
        )
        return f"""
        <div class="stat-section">
            <h3>{tittel}</h3>
            <table class="stat-table">
                <thead><tr>
                    <th class="sortable">{gruppe_label}</th>
                    <th class="sortable" data-sort="number">Median dager</th>
                    <th class="sortable" data-sort="number">Snitt dager</th>
                    <th class="sortable" data-sort="number">Antall solgte</th>
                </tr></thead>
                <tbody>{rows_html}</tbody>
            </table>
        </div>
        """

    snitt_txt = f"{snitt_totalt} dager" if snitt_totalt else "—"
    html = f"""
    <div class="stat-header">
        <h2>Markedsdata — liggetid</h2>
        <p class="stat-ingress">
            Basert på <strong>{antall_totalt}</strong> solgte annonser der vi har registrert
            både publiseringsdato og salgsdato.
            Gjennomsnittlig liggetid totalt: <strong>{snitt_txt}</strong>.
        </p>
        <p class="stat-note">Liggetid = antall dager fra annonsens publiseringsdato til den ble fjernet fra markedet.</p>
    </div>
    {_tabell("Per merke", per_merke, "Merke")}
    {_tabell("Per bobil-type", per_type, "Type")}
    {_tabell("Per prisklasse", per_prisklasse, "Prisklasse")}
    """
    return render_page("statistikk", html)


@app.route("/sok")
def view_sok():
    keywords = request.args.get("q", "")
    rows = get_sokresultater(keywords) if keywords else []

    html = f"""
    <form class="search-form" method="GET" action="sok">
        <input type="text" name="q" value="{esc(keywords)}"
               placeholder="Søk etter nøkkelord (kommaseparert, f.eks: køye, familie, vendbare seter)">
        <button type="submit" class="btn">Søk</button>
    </form>
    """

    if keywords and not rows:
        html += '<p class="no-data">Ingen treff.</p>'
    elif rows:
        html += """
        <table>
            <thead>
                <tr>
                    <th class="sortable">Annonse</th>
                    <th class="sortable" data-sort="number">Modell</th>
                    <th class="sortable" data-sort="number">Pris</th>
                    <th class="sortable" data-sort="number">Km</th>
                    <th class="sortable">Type</th>
                    <th class="sortable" data-sort="number">Endringer</th>
                    <th class="sortable" data-sort="number">Laveste</th>
                    <th class="sortable" data-sort="number">Høyeste</th>
                    <th class="sortable" data-sort="number">Sist sett</th>
                    <th>Lenke</th>
                    <th>Treff</th>
                </tr>
            </thead>
            <tbody>
        """
        for r in rows:
            treff_html = ""
            if r.get("Soketreff"):
                for t in r["Soketreff"].split(", "):
                    treff_html += f'<span class="keyword-tag">{esc(t)}</span>'
            html += f"""
                <tr>
                    <td class="truncate"><a href="annonse/{esc(r['Finnkode'])}">{esc(r['Annonsenavn'])}</a>{_kilde_badge(r.get('Kilde'))}</td>
                    <td>{esc(r['Modell'])}</td>
                    <td>{esc(r['NaaverendePris'])}</td>
                    <td>{esc(r.get('Kilometerstand'))}</td>
                    <td>{esc(r.get('Typebobil'))}</td>
                    <td>{esc(r['AntallEndringer'])}</td>
                    <td class="price-down">{esc(r['LavestePrisF'])}</td>
                    <td class="price-up">{esc(r['HoyestePrisF'])}</td>
                    <td class="{esc(r['AlderClass'])}" data-sort-value="{esc(r['AlderSort'])}">{esc(r['Alder'])}</td>
                    <td class="nowrap">{_kilde_lenker(r)}</td>
                    <td>{treff_html}</td>
                </tr>
            """
        html += "</tbody></table>"

    return render_page("sok", html)


@app.route("/detaljer")
def view_detaljer():
    page = request.args.get("page", 1, type=int)
    per_page = 50
    filters = {
        "modell_fra": request.args.get("modell_fra", ""),
        "modell_til": request.args.get("modell_til", ""),
        "pris_fra": request.args.get("pris_fra", ""),
        "pris_til": request.args.get("pris_til", ""),
        "type": request.args.get("type", ""),
        "girkasse": request.args.get("girkasse", ""),
        "solgt_filter": request.args.get("solgt_filter", "aktive"),
        "min_nyttelast": request.args.get("min_nyttelast", ""),
        "min_lengde": request.args.get("min_lengde", ""),
        "max_lengde": request.args.get("max_lengde", ""),
        "min_tilhengervekt": request.args.get("min_tilhengervekt", ""),
        "sengelayout": request.args.get("sengelayout", ""),
        "merker": request.args.getlist("merker"),
    }
    rows, total = get_detaljer(page, per_page, filters)

    if not rows and not any(filters.values()):
        return render_page("detaljer", '<p class="no-data">Ingen annonser funnet.</p>')

    # Hent filteralternativer
    filter_opts = get_filter_options()

    # Bygg filter-URL uten page-param
    def filter_qs():
        parts = []
        for k, v in filters.items():
            if k == "merker":
                for m in v:
                    if m:
                        parts.append(f"merker={m}")
            elif v:
                parts.append(f"{k}={v}")
        return "&".join(parts)

    # Filterpanel
    type_options = "".join(
        f'<option value="{t}" {"selected" if filters.get("type") == t else ""}>{t}</option>'
        for t in filter_opts["typer"]
    )
    gir_options = "".join(
        f'<option value="{g}" {"selected" if filters.get("girkasse") == g else ""}>{g}</option>'
        for g in filter_opts["girkasser"]
    )
    solgt_filter_val = filters.get("solgt_filter", "aktive")
    senge_valg = filters.get("sengelayout", "")
    senge_options = "".join(
        f'<option value="{s}" {"selected" if senge_valg == s else ""}>{s}</option>'
        for s in ["senkeseng", "køyer", "alkove", "enkelsenger", "queenbed", "dobbeltseng"]
    )
    merker_valgt = set(filters.get("merker", []))
    merke_checkboxes = "".join(
        f'<label class="merke-cb-label"><input type="checkbox" name="merker" value="{esc(m)}"'
        f'{"checked" if m in merker_valgt else ""}> {esc(m)}</label>'
        for m in filter_opts["merker"]
    )

    html = f"""
    <form class="filter-panel" method="GET" action="detaljer">
        <div class="filter-group">
            <label>Modellår fra</label>
            <input type="number" name="modell_fra" value="{filters.get('modell_fra', '')}" placeholder="f.eks. 2017" min="1990" max="2030">
        </div>
        <div class="filter-group">
            <label>Modellår til</label>
            <input type="number" name="modell_til" value="{filters.get('modell_til', '')}" placeholder="f.eks. 2023" min="1990" max="2030">
        </div>
        <div class="filter-group">
            <label>Pris fra</label>
            <input type="number" name="pris_fra" value="{filters.get('pris_fra', '')}" placeholder="f.eks. 300000" step="50000">
        </div>
        <div class="filter-group">
            <label>Pris til</label>
            <input type="number" name="pris_til" value="{filters.get('pris_til', '')}" placeholder="f.eks. 660000" step="50000">
        </div>
        <div class="filter-group">
            <label>Min nyttelast (kg)</label>
            <input type="number" name="min_nyttelast" value="{filters.get('min_nyttelast', '')}" placeholder="f.eks. 550" step="50">
        </div>
        <div class="filter-group">
            <label>Lengde fra (cm)</label>
            <input type="number" name="min_lengde" value="{filters.get('min_lengde', '')}" placeholder="f.eks. 600" step="10">
        </div>
        <div class="filter-group">
            <label>Lengde til (cm)</label>
            <input type="number" name="max_lengde" value="{filters.get('max_lengde', '')}" placeholder="f.eks. 800" step="10">
        </div>
        <div class="filter-group">
            <label>Min tilhengervekt (kg)</label>
            <input type="number" name="min_tilhengervekt" value="{filters.get('min_tilhengervekt', '')}" placeholder="f.eks. 2000" step="100">
        </div>
        <div class="filter-group">
            <label>Sengelayout</label>
            <select name="sengelayout">
                <option value="">Alle</option>
                {senge_options}
            </select>
        </div>
        <div class="filter-group filter-group-merker">
            <label>Merke</label>
            <div class="merke-cb-list">{merke_checkboxes}</div>
        </div>
        <div class="filter-group">
            <label>Type bobil</label>
            <select name="type">
                <option value="">Alle</option>
                {type_options}
            </select>
        </div>
        <div class="filter-group">
            <label>Girkasse</label>
            <select name="girkasse">
                <option value="">Alle</option>
                {gir_options}
            </select>
        </div>
        <div class="filter-group">
            <label>Annonser</label>
            <div class="filter-radio-group">
                <label><input type="radio" name="solgt_filter" value="aktive" {"checked" if solgt_filter_val == "aktive" else ""}> Bare aktive</label>
                <label><input type="radio" name="solgt_filter" value="alle" {"checked" if solgt_filter_val == "alle" else ""}> Alle</label>
                <label><input type="radio" name="solgt_filter" value="solgte" {"checked" if solgt_filter_val == "solgte" else ""}> Bare solgte</label>
            </div>
        </div>
        <div class="filter-group">
            <label>&nbsp;</label>
            <button type="submit" class="btn">Filtrer</button>
        </div>
        <div class="filter-group">
            <label>&nbsp;</label>
            <a href="detaljer?modell_fra=2017&pris_til=660000&min_nyttelast=550&min_lengde=600&max_lengde=800&min_tilhengervekt=2000&solgt_filter=aktive"
               class="btn btn-ghost">Familie-filter</a>
        </div>
    </form>
    """

    if not rows:
        html += '<p class="no-data">Ingen annonser matcher filtrene.</p>'
        return render_page("detaljer", html)

    vis_solgte = solgt_filter_val == "solgte"
    solgt_th = '<th class="sortable" data-sort="number">Sist sett</th>' if vis_solgte else '<th class="sortable">Heftelser</th>'
    html += f"""
    <table>
        <thead>
            <tr>
                <th class="thumb-cell"></th>
                <th class="sortable">Annonse</th>
                <th class="sortable" data-sort="number">Modell</th>
                <th class="sortable" data-sort="number">Km</th>
                <th class="sortable" data-sort="number">Pris</th>
                <th class="sortable" data-sort="number">Prisfall</th>
                <th class="sortable" data-sort="number">Nyttelast</th>
                <th class="sortable">Seng</th>
                {solgt_th}
                <th class="sortable">Lokasjon</th>
                <th>Lenke</th>
                <th class="sortable" data-sort="number">{"Fjernet" if vis_solgte else "Sist sett"}</th>
            </tr>
        </thead>
        <tbody>
    """
    for r in rows:
        is_sold = bool(r.get("Solgt")) or "solgt" in str(r.get("Pris", "")).lower()
        row_class = ' class="sold"' if is_sold else ""
        sold_badge = '<span class="sold-badge">Solgt</span>' if is_sold else ""
        ny_badge = '<span class="new-badge">NY</span>' if r.get("ErNy") and not is_sold else ""
        img_url = r.get("ImageURL", "") or ""
        thumb_html = f'<img src="{esc(img_url)}" class="thumb" alt="">' if img_url else ""
        lokasjon = r.get("Lokasjon", "") or ""
        nyttelast = f"{r['SvvNyttelast']} kg" if r.get("SvvNyttelast") else "—"
        if vis_solgte:
            sist_sett_raw = r.get("SistSett")
            if sist_sett_raw:
                try:
                    ss_dt = datetime.strptime(str(sist_sett_raw)[:19], "%Y-%m-%d %H:%M:%S")
                    sist_sett_td = f'<td class="note-secondary">{ss_dt.strftime("%-d. %b %Y")}</td>'
                except (ValueError, TypeError):
                    sist_sett_td = f'<td class="note-secondary">{esc(str(sist_sett_raw)[:10])}</td>'
            else:
                sist_sett_td = '<td class="note-secondary">—</td>'
            ekstra_col = sist_sett_td
            alder_col = f'<td class="{esc(r["AlderClass"])}" data-sort-value="{esc(r["AlderSort"])}">{esc(r["Alder"])}</td>'
        else:
            ekstra_col = f'<td>{_heftelse_badge(r.get("Heftelser"), r.get("HeftelserDetaljer"))}</td>'
            alder_col = f'<td class="{esc(r["AlderClass"])}" data-sort-value="{esc(r["AlderSort"])}">{esc(r["Alder"])}</td>'
        html += f"""
            <tr{row_class}>
                <td class="thumb-cell">{thumb_html}</td>
                <td class="truncate"><a href="annonse/{esc(r['Finnkode'])}">{esc(r['Annonsenavn'] or r['Finnkode'])}</a>{sold_badge}{ny_badge}{_kilde_badge(r.get('Kilde'))}</td>
                <td>{esc(r['Modell'])}</td>
                <td>{esc(r.get('Kilometerstand'))}</td>
                <td>{esc(r['NaaverendePris'])}</td>
                <td>{r.get('PrisfallHtml') or '<span class="note-secondary">—</span>'}</td>
                <td>{nyttelast}</td>
                <td>{esc(r.get('Sengelayout')) or '—'}</td>
                {ekstra_col}
                <td>{esc(lokasjon)}</td>
                <td class="nowrap">{_kilde_lenker(r)}</td>
                {alder_col}
            </tr>
        """
    html += "</tbody></table>"

    # Paginering med filter-params bevart
    total_pages = (total + per_page - 1) // per_page
    fqs = filter_qs()
    fqs_amp = f"&{fqs}" if fqs else ""
    if total_pages > 1:
        html += '<div class="pagination">'
        if page > 1:
            html += f'<a href="detaljer?page={page - 1}{fqs_amp}">Forrige</a>'
        for p in range(1, total_pages + 1):
            if p == page:
                html += f'<span class="current">{p}</span>'
            elif abs(p - page) <= 3 or p == 1 or p == total_pages:
                html += f'<a href="detaljer?page={p}{fqs_amp}">{p}</a>'
            elif abs(p - page) == 4:
                html += '<span>...</span>'
        if page < total_pages:
            html += f'<a href="detaljer?page={page + 1}{fqs_amp}">Neste</a>'
        html += '</div>'

    return render_page("detaljer", html)


@app.route("/annonse/<finnkode>")
def view_annonse(finnkode):
    """Detaljside for en enkelt annonse med prishistorikk-graf."""
    bp = "../"
    try:
        finnkode = int(finnkode)
    except (TypeError, ValueError):
        return render_page("detaljer", '<p class="no-data">Ugyldig annonsekode.</p>', base_path=bp)
    conn = get_db()
    if not conn:
        return render_page("detaljer", '<p class="no-data">Ingen databasetilkobling.</p>', base_path=bp)
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM bobil WHERE Finnkode = %s", (finnkode,))
        ad = cur.fetchone()
        if not ad:
            return render_page("detaljer", '<p class="no-data">Annonse ikke funnet.</p>', base_path=bp)

        # Hent prishistorikk
        cur.execute(
            "SELECT Tidspunkt, Pris FROM prisendringer WHERE Finnkode = %s ORDER BY Tidspunkt ASC",
            (finnkode,)
        )
        prishistorikk = cur.fetchall()

        pris = parse_price(ad["Pris"])
        km = parse_km(ad.get("Kilometerstand"))
        alder_txt, alder_cls, _ = format_age(ad.get("Oppdatert", ""))
        ad_url = _ad_url(ad)

        # Bygg Chart.js data
        chart_labels = []
        chart_data = []
        for p in prishistorikk:
            ts = p["Tidspunkt"]
            if isinstance(ts, datetime):
                chart_labels.append(ts.strftime("%d.%m.%Y"))
            else:
                chart_labels.append(str(ts))
            pris_val = parse_price(p["Pris"])
            chart_data.append(pris_val if pris_val else 0)

        image_url = ad.get("ImageURL", "") or ""
        lokasjon = ad.get("Lokasjon", "") or ""
        kjennemerke = ad.get("Kjennemerke", "") or ""
        img_html = f'<img src="{esc(image_url)}" class="detail-img" alt="">' if image_url else ""

        bruker = get_bruker_data(finnkode)
        er_favoritt = bruker["favoritt"]
        notat_verdi = esc(bruker["notat"])

        # Vegvesen-data
        def v(key): return ad.get(key)
        def vs(key): return esc(ad.get(key)) or "—"
        har_svv = any(ad.get(k) for k in ["SvvMerke", "SvvHandelsbetegnelse", "SvvFarge", "SvvDrivstoff"])

        def kg(val): return f"{val} kg" if val else "—"
        def cm(val): return f"{val} cm" if val else "—"
        def kw(val): return f"{round(val)} kW / {round(val * 1.36)} hk" if val else "—"
        def liter(val): return f"{val / 1000:.1f} L" if val else "—"

        eu_frist_html, eu_sist_html = _eu_kontroll_html(
            ad.get("SvvEuKontrollfrist") or "", ad.get("SvvEuSistGodkjent") or ""
        )

        svv_block = ""
        if har_svv:
            svv_block = f"""
        <h3 class="section-heading">Kjøretøydata fra Statens vegvesen</h3>
        <div class="svv-panel">
        <div class="svv-grid">
            <div><span class="lbl">Kjennemerke:</span> <strong>{esc(kjennemerke) or '—'}</strong></div>
            <div><span class="lbl">Kjøretøytype:</span> {vs('SvvKjoretoytype')}</div>
            <div><span class="lbl">Merke (SVV):</span> {vs('SvvMerke')}</div>
            <div><span class="lbl">Handelsbetegnelse:</span> {vs('SvvHandelsbetegnelse')}</div>
            <div><span class="lbl">Typebetegnelse:</span> {vs('SvvTypebetegnelse')}</div>
            <div><span class="lbl">Årsmodell (SVV):</span> {vs('SvvAarsmodell')}</div>
            <div><span class="lbl">1. gang reg. Norge:</span> {vs('SvvForstegangNorge')}</div>
            <div><span class="lbl">Registreringsstatus:</span> {vs('SvvRegistreringsstatus')}</div>
            <div><span class="lbl">EU-kontroll frist:</span> {eu_frist_html}</div>
            <div><span class="lbl">EU-kontroll sist:</span> {eu_sist_html}</div>
            <div><span class="lbl">Farge:</span> {vs('SvvFarge')}</div>
            <div><span class="lbl">Karosseritype:</span> {vs('SvvKarosseritype')}</div>
            <div><span class="lbl">Antall dører:</span> {vs('SvvAntallDorer')}</div>
            <div><span class="lbl">Drivstoff (SVV):</span> {vs('SvvDrivstoff')}</div>
            <div><span class="lbl">Motorvolum:</span> {liter(v('SvvMotorvolum'))}</div>
            <div><span class="lbl">Motoreffekt:</span> {kw(v('SvvMotoreffekt'))}</div>
            <div><span class="lbl">Antall sylindre:</span> {vs('SvvAntallSylindre')}</div>
            <div><span class="lbl">Girkasse (SVV):</span> {vs('SvvGirkassetype')}</div>
            <div><span class="lbl">Antall gir:</span> {vs('SvvAntallGir')}</div>
            <div><span class="lbl">Maks hastighet:</span> {"—" if not v('SvvMaksHastighet') else f"{v('SvvMaksHastighet')} km/t"}</div>
            <div><span class="lbl">Elektrisk/hybrid:</span> {"Ja" if v('SvvElektrisk') else "—"}</div>
            <div><span class="lbl">Euro-klasse:</span> {vs('SvvEuroKlasse')}</div>
            <div><span class="lbl">Lengde (SVV):</span> {cm(v('SvvLengde'))}</div>
            <div><span class="lbl">Bredde (SVV):</span> {cm(v('SvvBredde'))}</div>
            <div><span class="lbl">Høyde (SVV):</span> {cm(v('SvvHoyde'))}</div>
            <div><span class="lbl">Egenvekt:</span> {kg(v('SvvEgenvekt'))}</div>
            <div><span class="lbl">Nyttelast (SVV):</span> {kg(v('SvvNyttelast'))}</div>
            <div><span class="lbl">Teknisk tillatt totalvekt:</span> {kg(v('SvvTotalvekt'))}</div>
            <div><span class="lbl">Tillatt totalvekt:</span> {kg(v('SvvTillattTotalvekt'))}</div>
            <div><span class="lbl">Tilhengervekt m/brems:</span> {kg(v('SvvTilhengervektMedBrems'))}</div>
            <div><span class="lbl">Tilhengervekt u/brems:</span> {kg(v('SvvTilhengervektUtenBrems'))}</div>
            <div><span class="lbl">Vertikal koplingslast:</span> {kg(v('SvvVertikalKoplingslast'))}</div>
            <div><span class="lbl">Sitteplasser (SVV):</span> {vs('SvvSitteplasser')}</div>
        </div>
        </div>
        """
        elif kjennemerke:
            svv_block = f"""
        <div class="kjennemerke-hint">
            Kjennemerke: <strong>{esc(kjennemerke)}</strong> — ingen Vegvesen-data hentet ennå.
        </div>
        """

        kilde = ad.get("Kilde") or "finn"
        autodb_id = ad.get("AutodbId")

        stjerne = "⭐" if er_favoritt else "☆"
        stjerne_title = "Fjern fra favoritter" if er_favoritt else "Legg til i favoritter"

        ext_links_html = _kilde_lenker(ad)

        html = f"""
        <div class="detail-nav">
            <a href="javascript:history.back()">&larr; Tilbake</a>
            <a href="../mine-biler">⭐ Mine biler</a>
        </div>
        <h2 class="detail-title">
            {esc(ad.get('Annonsenavn', finnkode))}
            {_kilde_badge(kilde)}
            <button id="fav-btn" class="fav-btn" onclick="toggleFavoritt({esc(finnkode)})"
                    title="{esc(stjerne_title)}">{stjerne}</button>
        </h2>
        <div class="ext-links">{ext_links_html}</div>
        {img_html}
        <div class="notat-section">
            <div class="notat-label">Notat</div>
            <textarea id="notat-felt" class="notat-textarea" rows="3"
                      placeholder="Skriv ditt notat om denne bilen her...">{notat_verdi}</textarea>
            <div class="notat-save-row">
                <button class="btn btn-sm" onclick="lagreNotat({esc(finnkode)})">Lagre notat</button>
                <span id="notat-status" class="notat-status"></span>
            </div>
        </div>
        <script>
        const _apiBase = '{esc(bp)}';
        function toggleFavoritt(fk) {{
            fetch(_apiBase + 'api/favoritt/' + fk, {{method: 'POST'}})
                .then(r => r.json())
                .then(d => {{
                    if (d.ok) {{
                        const btn = document.getElementById('fav-btn');
                        btn.textContent = d.favoritt ? '⭐' : '☆';
                        btn.title = d.favoritt ? 'Fjern fra favoritter' : 'Legg til i favoritter';
                    }}
                }});
        }}
        function lagreNotat(fk) {{
            const txt = document.getElementById('notat-felt').value;
            fetch(_apiBase + 'api/notat/' + fk, {{
                method: 'POST',
                headers: {{'Content-Type': 'application/json'}},
                body: JSON.stringify({{notat: txt}})
            }}).then(r => r.json()).then(d => {{
                const s = document.getElementById('notat-status');
                s.textContent = d.ok ? 'Lagret ✓' : 'Feil ved lagring';
                setTimeout(() => s.textContent = '', 3000);
            }});
        }}
        </script>
        """
        selger_html = _selger_html(ad)
        liggetid_data = get_liggetid_for_annonse(finnkode)
        salgspris_est = beregn_forventet_salgspris(pris, ad.get("Modell"))
        if salgspris_est:
            kalibrert_note = (
                f"Kalibrert mot {salgspris_est['snitt_rabatt_pct']}% snittrabatt for {ad.get('Modell')}-modeller"
                if salgspris_est["ar_kalibrert"]
                else f"Snittrabatt alle årsmodeller: {salgspris_est['snitt_rabatt_pct']}%"
            )
            sp_f = f"~{salgspris_est['forsiktig']:,} kr".replace(",", " ")
            sp_r = f"~{salgspris_est['realistisk']:,} kr".replace(",", " ")
            sp_a = f"~{salgspris_est['aggressivt']:,} kr".replace(",", " ")
            salgspris_block = (
                '<div class="salgspris-box">'
                '<span class="lbl">Estimert salgspris</span>'
                '<div class="salgspris-row">'
                f'<div class="salgspris-item"><span class="salgspris-label">Forsiktig bud</span><span class="salgspris-value">{sp_f}</span></div>'
                f'<div class="salgspris-item"><span class="salgspris-label">Realistisk landing</span><span class="salgspris-value">{sp_r}</span></div>'
                f'<div class="salgspris-item"><span class="salgspris-label">Aggressivt åpningsbud</span><span class="salgspris-value">{sp_a}</span></div>'
                '</div>'
                f'<div class="salgspris-note">{esc(kalibrert_note)} · Basert på 78 historiske salg</div>'
                '</div>'
            )
        else:
            salgspris_block = ""

        id_label = "AutodbId" if kilde == "autodb" else "Finnkode"
        id_value = esc(ad.get("AutodbId") if kilde == "autodb" else finnkode)
        vendbare = "Ja" if ad.get("VendbareForerstoler") == 1 else ("Nei" if ad.get("VendbareForerstoler") == 0 else "—")
        html += f"""
        <div class="info-grid">
            <div><span class="lbl">{id_label}:</span> <a href="{esc(ad_url)}" target="_blank">{id_value}</a></div>
            <div><span class="lbl">Modell:</span> {esc(ad.get('Modell')) or '—'}</div>
            <div><span class="lbl">Pris:</span> {esc(format_price(pris))}</div>
            <div><span class="lbl">Km:</span> {esc(ad.get('Kilometerstand')) or '—'}</div>
            <div><span class="lbl">Type:</span> {esc(ad.get('Typebobil')) or '—'}</div>
            <div><span class="lbl">Girkasse:</span> {esc(ad.get('Girkasse')) or '—'}</div>
            <div><span class="lbl">Nyttelast (annonse):</span> {esc(ad.get('Nyttelast')) or '—'}</div>
            <div><span class="lbl">Nyttelast (SVV):</span> {kg(v('SvvNyttelast'))}</div>
            <div><span class="lbl">Tilhengervekt m/brems:</span> {kg(v('SvvTilhengervektMedBrems'))}</div>
            <div><span class="lbl">Lokasjon:</span> {esc(lokasjon) or '—'}</div>
            <div><span class="lbl">Sist sett:</span> <span class="{esc(alder_cls)}">{esc(alder_txt)}</span></div>
            <div><span class="lbl">Sengelayout:</span> {esc(ad.get('Sengelayout')) or '—'}</div>
            <div><span class="lbl">Vendbare forseter:</span> {vendbare}</div>
            <div><span class="lbl">Selger:</span> {selger_html}</div>
            <div><span class="lbl">Heftelser (Brreg):</span> {_heftelse_html(ad.get('Heftelser'), ad.get('HeftelseSjekket'), ad.get('HeftelserDetaljer'))}</div>
        </div>
        {salgspris_block}
        {_liggetid_html(liggetid_data)}
        {svv_block}
        <div class="beskrivelse">
            {esc(ad.get('Beskrivelse', ''))}
        </div>
        """

        if chart_data and len(chart_data) > 1:
            html += f"""
            <h3 class="section-heading">Prishistorikk</h3>
            <div class="chart-container">
                <canvas id="prisChart"></canvas>
            </div>
            <script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
            <script>
                new Chart(document.getElementById('prisChart'), {{
                    type: 'line',
                    data: {{
                        labels: {json.dumps(chart_labels)},
                        datasets: [{{
                            label: 'Pris (kr)',
                            data: {json.dumps(chart_data)},
                            borderColor: '#0A84FF',
                            backgroundColor: 'rgba(10,132,255,0.1)',
                            fill: true,
                            tension: 0.3,
                            pointRadius: 4,
                            pointBackgroundColor: '#0A84FF'
                        }}]
                    }},
                    options: {{
                        responsive: true,
                        plugins: {{
                            legend: {{ display: false }},
                            tooltip: {{
                                callbacks: {{
                                    label: ctx => ctx.parsed.y.toLocaleString('no-NO') + ' kr'
                                }}
                            }}
                        }},
                        scales: {{
                            y: {{
                                ticks: {{
                                    callback: v => (v/1000) + 'k',
                                    color: 'rgba(235,235,245,0.6)'
                                }},
                                grid: {{ color: 'rgba(255,255,255,0.05)' }}
                            }},
                            x: {{
                                ticks: {{ color: 'rgba(235,235,245,0.6)', maxRotation: 45 }},
                                grid: {{ color: 'rgba(255,255,255,0.05)' }}
                            }}
                        }}
                    }}
                }});
            </script>
            """
        elif prishistorikk:
            html += '<p class="note-secondary">Kun ett datapunkt i prishistorikken.</p>'
        else:
            html += '<p class="note-secondary">Ingen prishistorikk registrert.</p>'

        # Prisendringer-tabell
        if prishistorikk:
            html += """
            <h3 class="section-heading">Prisendringer</h3>
            <table class="prishistorikk-tabell">
                <thead><tr><th>Tidspunkt</th><th>Pris</th></tr></thead>
                <tbody>
            """
            for p in reversed(prishistorikk):
                ts = p["Tidspunkt"]
                if isinstance(ts, datetime):
                    ts_str = ts.strftime("%d.%m.%Y %H:%M")
                else:
                    ts_str = str(ts)
                pval = parse_price(p["Pris"])
                pris_str = format_price(pval) if pval else p["Pris"]
                html += f"<tr><td>{esc(ts_str)}</td><td>{esc(pris_str)}</td></tr>"
            html += "</tbody></table>"

        return render_page("detaljer", html, base_path=bp)
    except Exception as e:
        logger.error("Feil i view_annonse: %s\n%s", e, traceback.format_exc())
        return render_page("detaljer", '<p class="no-data">Feil ved henting av annonse.</p>', base_path=bp)
    finally:
        conn.close()


@app.route("/scrape", methods=["POST"])
def trigger_scrape():
    if not scraper_status["running"]:
        t = threading.Thread(target=run_scraper_background, daemon=True)
        t.start()
    return redirect(request.referrer or "annonser")


@app.route("/api/favoritt/<int:finnkode>", methods=["POST"])
def api_toggle_favoritt(finnkode):
    """Toggle favoritt-status for en annonse."""
    conn = get_db()
    if not conn:
        return jsonify({"ok": False}), 500
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT Favoritt FROM bruker_data WHERE Finnkode = %s", (finnkode,))
        row = cur.fetchone()
        ny_verdi = 0 if (row and row["Favoritt"]) else 1
        if row:
            cur.execute("UPDATE bruker_data SET Favoritt = %s WHERE Finnkode = %s", (ny_verdi, finnkode))
        else:
            cur.execute("INSERT INTO bruker_data (Finnkode, Favoritt) VALUES (%s, %s)", (finnkode, ny_verdi))
        conn.commit()
        return jsonify({"ok": True, "favoritt": bool(ny_verdi)})
    except Exception as e:
        logger.error("Feil i api_toggle_favoritt: %s", e)
        return jsonify({"ok": False}), 500
    finally:
        conn.close()


@app.route("/api/notat/<int:finnkode>", methods=["POST"])
def api_lagre_notat(finnkode):
    """Lagre notat for en annonse."""
    notat = request.json.get("notat", "") if request.is_json else request.form.get("notat", "")
    conn = get_db()
    if not conn:
        return jsonify({"ok": False}), 500
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT Finnkode FROM bruker_data WHERE Finnkode = %s", (finnkode,))
        if cur.fetchone():
            cur.execute("UPDATE bruker_data SET Notat = %s WHERE Finnkode = %s", (notat, finnkode))
        else:
            cur.execute("INSERT INTO bruker_data (Finnkode, Notat) VALUES (%s, %s)", (finnkode, notat))
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("Feil i api_lagre_notat: %s", e)
        return jsonify({"ok": False}), 500
    finally:
        conn.close()


@app.route("/mine-biler")
def view_mine_biler():
    rows = get_alle_favoritter()

    if not rows:
        return render_page("mine-biler", '<p class="no-data">Ingen favoritter ennå — klikk stjernen på en annonse for å legge til.</p>')

    html = '<table><thead><tr>'
    html += '<th class="thumb-cell"></th>'
    html += '<th class="sortable">Annonse</th>'
    html += '<th class="sortable" data-sort="number">Modell</th>'
    html += '<th class="sortable" data-sort="number">Pris</th>'
    html += '<th class="sortable" data-sort="number">Prisfall</th>'
    html += '<th class="sortable" data-sort="number">Nyttelast</th>'
    html += '<th class="sortable">Seng</th>'
    html += '<th class="sortable">EU-frist</th>'
    html += '<th class="sortable">Heftelser</th>'
    html += '<th>Lenke</th>'
    html += '<th>Notat</th>'
    html += '<th></th>'
    html += '</tr></thead><tbody>'

    for r in rows:
        img_url = r.get("ImageURL", "") or ""
        thumb = f'<img src="{esc(img_url)}" class="thumb" alt="">' if img_url else ""
        solgt_badge = '<span class="sold-badge">Solgt</span>' if r.get("Solgt") else ""
        nyttelast = f"{r['SvvNyttelast']} kg" if r.get("SvvNyttelast") else "—"
        eu_frist = esc(r.get("SvvEuKontrollfrist") or "—")
        prisfall = r.get("PrisfallHtml") or '<span class="note-secondary">—</span>'
        notat_tekst = esc(r.get("Notat") or "")
        finnkode = r["Finnkode"]

        html += f"""
        <tr>
            <td class="thumb-cell">{thumb}</td>
            <td class="truncate">
                <a href="annonse/{esc(finnkode)}">{esc(r['Annonsenavn'])}</a>{solgt_badge}{_kilde_badge(r.get('Kilde'))}
            </td>
            <td>{esc(r['Modell'])}</td>
            <td>{esc(r['NaaverendePris'])}</td>
            <td>{prisfall}</td>
            <td>{nyttelast}</td>
            <td>{esc(r.get('Sengelayout')) or '—'}</td>
            <td>{eu_frist}</td>
            <td>{_heftelse_badge(r.get('Heftelser'), r.get('HeftelserDetaljer'))}</td>
            <td class="nowrap">{_kilde_lenker(r)}</td>
            <td>
                <span class="notat-vis" data-fk="{esc(finnkode)}"
                      onclick="toggleNotat({esc(finnkode)})">{notat_tekst or '<em>Legg til notat...</em>'}</span>
                <div id="notat-form-{esc(finnkode)}" style="display:none; margin-top:4px;">
                    <textarea id="notat-txt-{esc(finnkode)}" rows="2"
                              class="notat-inline-textarea">{notat_tekst}</textarea>
                    <br>
                    <button class="btn btn-sm mt-4"
                            onclick="lagreNotat({esc(finnkode)})">Lagre</button>
                </div>
            </td>
            <td>
                <button class="btn btn-sm btn-danger"
                        onclick="fjernFavoritt({esc(finnkode)}, this)">&#x2715;</button>
            </td>
        </tr>
        """

    html += "</tbody></table>"

    html += """
    <script>
    function toggleNotat(fk) {
        const form = document.getElementById('notat-form-' + fk);
        form.style.display = form.style.display === 'none' ? 'block' : 'none';
    }
    function lagreNotat(fk) {
        const txt = document.getElementById('notat-txt-' + fk).value;
        fetch('api/notat/' + fk, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({notat: txt})
        }).then(r => r.json()).then(d => {
            if (d.ok) {
                const vis = document.querySelector('.notat-vis[data-fk="' + fk + '"]');
                if (vis) vis.innerHTML = txt || '<em>Legg til notat...</em>';
                document.getElementById('notat-form-' + fk).style.display = 'none';
            }
        });
    }
    function fjernFavoritt(fk, btn) {
        fetch('api/favoritt/' + fk, {method: 'POST'})
            .then(r => r.json())
            .then(d => { if (d.ok && !d.favoritt) btn.closest('tr').remove(); });
    }
    </script>
    """

    return render_page("mine-biler", html)


@app.route("/api/dbdiag")
def api_dbdiag():
    conn = get_db()
    if not conn:
        return jsonify({"error": "no db"})
    try:
        cur = conn.cursor()
        results = {}
        cur.execute("SELECT COUNT(*) FROM bobil WHERE Solgt=1")
        results["solgt_totalt"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM bobil WHERE Solgt=1 AND SolgtDato IS NOT NULL")
        results["har_solgt_dato"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT Finnkode) FROM prisendringer WHERE Pris='Solgt/Fjernet'")
        results["prisendringer_solgt"] = cur.fetchone()[0]
        # Hvor mange solgte har minst én prisrad i prisendringer?
        cur.execute("""
            SELECT COUNT(*) FROM bobil b
            JOIN (SELECT DISTINCT Finnkode FROM prisendringer WHERE Pris REGEXP '^[0-9]+$') p
            ON b.Finnkode = p.Finnkode WHERE b.Solgt=1
        """)
        results["solgte_med_prisrad"] = cur.fetchone()[0]
        # Liggetid-distribusjon med ny logikk (forste_sett fra prisendringer)
        cur.execute("""
            SELECT
                SUM(CASE WHEN liggetid < 0 THEN 1 ELSE 0 END) AS negativ,
                SUM(CASE WHEN liggetid = 0 THEN 1 ELSE 0 END) AS null_dager,
                SUM(CASE WHEN liggetid BETWEEN 1 AND 30 THEN 1 ELSE 0 END) AS en_til_30,
                SUM(CASE WHEN liggetid BETWEEN 31 AND 730 THEN 1 ELSE 0 END) AS trettien_til_730,
                SUM(CASE WHEN liggetid > 730 THEN 1 ELSE 0 END) AS over_730,
                COUNT(*) AS totalt
            FROM (
                SELECT DATEDIFF(COALESCE(b.SolgtDato, sd.SolgtTidspunkt), fs.ErstSett) AS liggetid
                FROM bobil b
                LEFT JOIN (
                    SELECT Finnkode, MAX(Tidspunkt) AS SolgtTidspunkt
                    FROM prisendringer WHERE Pris='Solgt/Fjernet' GROUP BY Finnkode
                ) sd ON b.Finnkode = sd.Finnkode
                JOIN (
                    SELECT Finnkode, MIN(Tidspunkt) AS ErstSett
                    FROM prisendringer WHERE Pris REGEXP '^[0-9]+$' GROUP BY Finnkode
                ) fs ON b.Finnkode = fs.Finnkode
                WHERE b.Solgt=1 AND COALESCE(b.SolgtDato, sd.SolgtTidspunkt) IS NOT NULL
            ) t
        """)
        r = cur.fetchone()
        results["liggetid_dist"] = {
            "negativ": r[0], "null_dager": r[1], "en_til_30": r[2],
            "trettien_til_730": r[3], "over_730": r[4], "totalt": r[5]
        }
        # 5 eksempler med liggetid-detaljer
        cur.execute("""
            SELECT b.Finnkode, b.SolgtDato, fs.ErstSett,
                   DATEDIFF(COALESCE(b.SolgtDato, sd.SolgtTidspunkt), fs.ErstSett) AS liggetid
            FROM bobil b
            LEFT JOIN (
                SELECT Finnkode, MAX(Tidspunkt) AS SolgtTidspunkt
                FROM prisendringer WHERE Pris='Solgt/Fjernet' GROUP BY Finnkode
            ) sd ON b.Finnkode = sd.Finnkode
            JOIN (
                SELECT Finnkode, MIN(Tidspunkt) AS ErstSett
                FROM prisendringer WHERE Pris REGEXP '^[0-9]+$' GROUP BY Finnkode
            ) fs ON b.Finnkode = fs.Finnkode
            WHERE b.Solgt=1 AND COALESCE(b.SolgtDato, sd.SolgtTidspunkt) IS NOT NULL
            LIMIT 10
        """)
        results["eksempler"] = [
            {"fk": r[0], "solgt_dato": str(r[1]), "erst_sett": str(r[2]), "liggetid": r[3]}
            for r in cur.fetchall()
        ]
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        conn.close()


@app.route("/api/status")
def api_status():
    return jsonify({
        "last_run": scraper_status["last_run"].isoformat() if scraper_status["last_run"] else None,
        "running": scraper_status["running"],
        "error": scraper_status["error"],
        "total_listings": get_total_count(),
    })


# ---------------------------------------------------------------------------
# Oppstart
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logger.info("Starter Bobil web UI på port 8100...")

    # Sørg for at nye kolonner finnes
    ensure_db_columns()

    # Start planlagt scraping i bakgrunnen
    scrape_interval = options.get("scrape_interval", 6)
    schedule_scraper(interval_hours=scrape_interval)

    # Start webserveren
    serve(app, host="0.0.0.0", port=8100)
