"""GCS reader for Forskningsradet parser."""

import io, json, csv
from google.cloud import storage as gcs_lib


class GCSReader:

    def __init__(self, bucket_name, prefix="forskningsradet"):
        self._client = gcs_lib.Client()
        self._bucket = self._client.bucket(bucket_name)
        self._prefix = prefix.rstrip("/")

    def list_snapshot_dates(self):
        prefix = f"{self._prefix}/raw/"
        dates = set()
        iterator = self._bucket.list_blobs(prefix=prefix, delimiter="/")
        for page in iterator.pages:
            for p in page.prefixes:
                d = p.rstrip("/").split("/")[-1]
                if len(d) == 10:
                    dates.add(d)
        return sorted(dates)

    def load_manifest(self, snapshot_date):
        blob = self._bucket.blob(f"{self._prefix}/raw/{snapshot_date}/manifest.json")
        if not blob.exists():
            return None
        return json.loads(blob.download_as_text())

    def read_csv(self, snapshot_date, dataset_name):
        path = f"{self._prefix}/raw/{snapshot_date}/{dataset_name}.csv"
        blob = self._bucket.blob(path)
        if not blob.exists():
            return []
        text = blob.download_as_text(encoding="utf-8")
        delimiter = ";" if dataset_name == "bevilgningereu" else ","
        return list(csv.DictReader(io.StringIO(text), delimiter=delimiter))
