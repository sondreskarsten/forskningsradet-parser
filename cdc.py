"""CDC for Forskningsradet parser.

Two datasets, two LUASes:
    bevilgningereu: (project_nr, orgnr) — one EU grant participation
    soknader2: (project_nr, orgnr) — one application/award
"""

import io, json, uuid
from datetime import datetime, timezone
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage as gcs_lib
import re

def parse_date_to_iso(date_str):
    if not date_str:
        return None
    m = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", str(date_str))
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    if re.match(r"\d{4}-\d{2}-\d{2}", str(date_str)):
        return str(date_str)[:10]
    return str(date_str)

CHANGELOG_SCHEMA = pa.schema([
    ("orgnr", pa.string()), ("document_id", pa.string()), ("data_source", pa.string()),
    ("event_type", pa.string()), ("event_subtype", pa.string()), ("summary", pa.string()),
    ("changed_fields", pa.string()), ("valid_time", pa.string()), ("detected_time", pa.string()),
    ("details_json", pa.string()), ("source_run_mode", pa.string()), ("run_id", pa.string()),
])

SNAPSHOT_SCHEMA = pa.schema([
    ("dataset", pa.string()), ("project_nr", pa.string()), ("orgnr", pa.string()), ("content_hash", pa.string()),
])

POOL_SCHEMA = pa.schema([
    ("orgnr", pa.string()), ("first_seen", pa.string()), ("last_seen", pa.string()), ("n_entries", pa.int32()),
])


class ForskningsradetCDC:

    def __init__(self, bucket_name, prefix="forskningsradet"):
        self._client = gcs_lib.Client()
        self._bucket = self._client.bucket(bucket_name)
        self._prefix = prefix.rstrip("/")

    def _gcs_path(self, *parts):
        return "/".join([self._prefix] + list(parts))

    def _read_parquet(self, path):
        blob = self._bucket.blob(path)
        if not blob.exists():
            return None
        return pq.read_table(io.BytesIO(blob.download_as_bytes()))

    def _write_parquet(self, table, path):
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="zstd")
        buf.seek(0)
        self._bucket.blob(path).upload_from_file(buf, content_type="application/octet-stream")

    def _load_snapshots(self):
        t = self._read_parquet(self._gcs_path("cdc", "snapshots.parquet"))
        if t is None:
            return {}
        d = t.to_pydict()
        return {(d["dataset"][i], d["project_nr"][i], d["orgnr"][i]): d["content_hash"][i] for i in range(t.num_rows)}

    def _load_pool(self):
        t = self._read_parquet(self._gcs_path("cdc", "pool.parquet"))
        if t is None:
            return {}
        d = t.to_pydict()
        return {d["orgnr"][i]: {"first_seen": d["first_seen"][i], "last_seen": d["last_seen"][i],
                                "n_entries": d["n_entries"][i]} for i in range(t.num_rows)}

    def run(self, parsed_rows, run_date, run_mode="daily"):
        run_id = str(uuid.uuid4())[:8]
        detected_time = datetime.now(timezone.utc).isoformat()
        old_snaps = self._load_snapshots()
        pool = self._load_pool()
        changelog_rows = []
        new_count = 0
        mod_count = 0
        new_snaps = {}

        for row in parsed_rows:
            ds = row["dataset"]
            key = (ds, row["project_nr"], row["orgnr"])
            h = row["content_hash"]
            old_h = old_snaps.get(key)
            new_snaps[key] = {"dataset": ds, "project_nr": row["project_nr"], "orgnr": row["orgnr"], "content_hash": h}

            if run_mode == "bootstrap" or old_h is None:
                event_type = "new"
                new_count += 1
            elif old_h != h:
                event_type = "modified"
                mod_count += 1
            else:
                continue

            name = row.get("org_name") or row.get("project_acronym") or row.get("project_title") or ""
            amt = row.get("amount_eur") or row.get("granted_amount") or ""
            summary = " — ".join(filter(None, [ds, name[:50], f"project {row['project_nr']}", str(amt) if amt else None]))

            details = {k: v for k, v in row.items() if k != "content_hash"}
            changelog_rows.append({
                "orgnr": row["orgnr"],
                "document_id": f"nfr-{ds}-{row['project_nr']}-{row['orgnr']}",
                "data_source": "forskningsradet",
                "event_type": event_type,
                "event_subtype": f"nfr_{ds}",
                "summary": summary,
                "changed_fields": None if event_type == "new" else json.dumps(["content_hash"]),
                "valid_time": parse_date_to_iso(row.get("contract_start") or row.get("project_start")) or run_date,
                "detected_time": detected_time,
                "details_json": json.dumps(details, ensure_ascii=False),
                "source_run_mode": run_mode,
                "run_id": run_id,
            })

            orgnr = row["orgnr"]
            if orgnr in pool:
                pool[orgnr]["last_seen"] = run_date
                pool[orgnr]["n_entries"] += 1
            else:
                pool[orgnr] = {"first_seen": run_date, "last_seen": run_date, "n_entries": 1}

        if run_mode != "bootstrap":
            for key, old_h in old_snaps.items():
                if key not in new_snaps:
                    changelog_rows.append({
                        "orgnr": key[2], "document_id": f"nfr-{key[0]}-{key[1]}-{key[2]}",
                        "data_source": "forskningsradet", "event_type": "disappeared",
                        "event_subtype": f"nfr_{key[0]}_ended", "summary": f"Disappeared from {key[0]}: project {key[1]}",
                        "changed_fields": None, "valid_time": run_date, "detected_time": detected_time,
                        "details_json": None, "source_run_mode": run_mode, "run_id": run_id,
                    })

        if changelog_rows:
            self._write_parquet(pa.Table.from_pylist(changelog_rows, schema=CHANGELOG_SCHEMA),
                               self._gcs_path("cdc", "changelog", f"{run_date}.parquet"))
        snap_rows = list(new_snaps.values())
        if snap_rows:
            self._write_parquet(pa.Table.from_pylist(snap_rows, schema=SNAPSHOT_SCHEMA),
                               self._gcs_path("cdc", "snapshots.parquet"))
        if pool:
            self._write_parquet(pa.Table.from_pylist([{"orgnr": k, **v} for k, v in pool.items()], schema=POOL_SCHEMA),
                               self._gcs_path("cdc", "pool.parquet"))

        return {"new": new_count, "modified": mod_count, "changelog_rows": len(changelog_rows),
                "pool_size": len(pool), "snapshot_size": len(new_snaps)}
