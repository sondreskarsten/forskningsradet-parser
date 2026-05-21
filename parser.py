"""Parse Forskningsradet CSVs. Orgnr is directly in both datasets — no resolution needed.

bevilgningereu: semicolon-delimited, key='Organisasjonsnummer', EU grants only (H2020)
soknader2: comma-delimited, key='organisasjonsnummer', all applications
"""

import hashlib
import re

ORGNR_RE = re.compile(r"^\d{9}$")


def content_hash_eu(row):
    tracked = [
        row.get("Prosjektnr", ""),
        row.get("Organisasjonsnummer", ""),
        row.get("Organisasjon rolle", ""),
        row.get("Kontraktstatus", ""),
        str(row.get("Innvilget belop organisasjon EUR", "")),
    ]
    return hashlib.sha256("|".join(tracked).encode()).hexdigest()[:16]


def content_hash_sok(row):
    tracked = [
        row.get("prosjektnummer", ""),
        row.get("organisasjonsnummer", ""),
        row.get("prosjektfase", ""),
        str(row.get("tildelt_belop", "")),
    ]
    return hashlib.sha256("|".join(tracked).encode()).hexdigest()[:16]


def parse_bevilgningereu(rows):
    """Parse EU grants CSV. Returns list of dicts with orgnr populated."""
    parsed = []
    for row in rows:
        orgnr = (row.get("Organisasjonsnummer") or "").strip()
        if not ORGNR_RE.match(orgnr):
            continue
        parsed.append({
            "orgnr": orgnr,
            "dataset": "bevilgningereu",
            "project_nr": row.get("Prosjektnr", ""),
            "project_acronym": row.get("Prosjektkortnavn", ""),
            "project_name": row.get("Prosjektnavn", ""),
            "org_name": row.get("Organisasjonsnavn", ""),
            "org_role": row.get("Organisasjon rolle", ""),
            "contract_status": row.get("Kontraktstatus", ""),
            "contract_start": row.get("Kontrakt startdato", ""),
            "contract_end": row.get("Kontrakt sluttdato", ""),
            "programme": row.get("Rammeprogram", ""),
            "programme_area": row.get("Programomraade (H2020)", ""),
            "amount_eur": row.get("Innvilget belop organisasjon EUR", ""),
            "kommune": row.get("Adresse kommune", ""),
            "fylke": row.get("Adresse fylke", ""),
            "content_hash": content_hash_eu(row),
        })
    return parsed


def parse_soknader2(rows):
    """Parse all-applications CSV. Returns list of dicts with orgnr."""
    parsed = []
    for row in rows:
        orgnr = (row.get("organisasjonsnummer") or "").strip()
        if not ORGNR_RE.match(orgnr):
            continue
        parsed.append({
            "orgnr": orgnr,
            "dataset": "soknader2",
            "project_nr": row.get("prosjektnummer", ""),
            "project_title": row.get("prosjekttittel", ""),
            "org_name": row.get("prosjektansvarlig_navn", ""),
            "project_phase": row.get("prosjektfase", ""),
            "project_type": row.get("prosjekttype", ""),
            "aktivitet": row.get("aktivitet", ""),
            "virkemiddel": row.get("virkemiddel", ""),
            "applied_amount": row.get("sokt_belop", ""),
            "granted_amount": row.get("tildelt_belop", ""),
            "project_start": row.get("prosjektstart", ""),
            "project_end": row.get("prosjektslutt", ""),
            "kommune": row.get("kommune", ""),
            "fylke": row.get("fylke", ""),
            "content_hash": content_hash_sok(row),
        })
    return parsed
