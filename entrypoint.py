"""Forskningsradet parser entrypoint. Reads CSVs, emits 12-col CDC. Orgnr is native."""

import os, sys
from datetime import date
from reader import GCSReader
from parser import parse_bevilgningereu, parse_soknader2
from cdc import ForskningsradetCDC

GCS_BUCKET = os.environ.get("GCS_BUCKET", "sondre_brreg_data")
GCS_PREFIX = os.environ.get("GCS_PREFIX", "forskningsradet")
RUN_MODE = os.environ.get("RUN_MODE", "daily")
SNAPSHOT_DATE = os.environ.get("SNAPSHOT_DATE", "")


def main():
    print(f"{'='*60}\n  forskningsradet-parser — mode: {RUN_MODE}\n  {date.today().isoformat()}\n  GCS: gs://{GCS_BUCKET}/{GCS_PREFIX}/\n{'='*60}", flush=True)

    reader = GCSReader(GCS_BUCKET, GCS_PREFIX)
    snapshot_dates = reader.list_snapshot_dates()
    if not snapshot_dates:
        print("  No snapshots. Run forskningsradet-collector first.", flush=True)
        sys.exit(1)

    snapshot = SNAPSHOT_DATE if SNAPSHOT_DATE else snapshot_dates[-1]
    print(f"  Using snapshot: {snapshot}", flush=True)

    all_parsed = []

    eu_rows = reader.read_csv(snapshot, "bevilgningereu")
    if eu_rows:
        parsed_eu = parse_bevilgningereu(eu_rows)
        print(f"  bevilgningereu: {len(eu_rows):,} raw -> {len(parsed_eu):,} with orgnr", flush=True)
        all_parsed.extend(parsed_eu)

    sok_rows = reader.read_csv(snapshot, "soknader2")
    if sok_rows:
        parsed_sok = parse_soknader2(sok_rows)
        print(f"  soknader2: {len(sok_rows):,} raw -> {len(parsed_sok):,} with orgnr", flush=True)
        all_parsed.extend(parsed_sok)

    print(f"\n  Total parsed: {len(all_parsed):,}", flush=True)

    cdc = ForskningsradetCDC(GCS_BUCKET, GCS_PREFIX)
    run_mode = "bootstrap" if RUN_MODE == "bootstrap" else "daily"
    stats = cdc.run(all_parsed, date.today().isoformat(), run_mode=run_mode)
    print(f"  CDC: {stats}", flush=True)


if __name__ == "__main__":
    main()
