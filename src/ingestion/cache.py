import hashlib
import json
from pathlib import Path

import pandas as pd

from .models import IngestionUnit


class DataFrameCache:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for(self, unit: IngestionUnit):
        digest = hashlib.sha256(unit.unit_id.encode("utf-8")).hexdigest()[:16]
        return self.root / f"{unit.ingestion_type}_{digest}.pkl"

    def load(self, unit):
        path = self.path_for(unit)
        if not path.exists():
            return None
        return pd.read_pickle(path)

    def save(self, unit, dataframe):
        path = self.path_for(unit)
        temporary_path = path.with_suffix(".tmp")
        dataframe.to_pickle(temporary_path)
        temporary_path.replace(path)
        return str(path)


class JsonlIngestionLog:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _records(self):
        if not self.path.exists():
            return
        with self.path.open() as log_file:
            for line in log_file:
                if line.strip():
                    yield json.loads(line)

    def latest(self):
        records = {}
        for record in self._records():
            records[record["unit_id"]] = record
        return records

    def status(self, unit):
        return self.latest().get(unit.unit_id, {}).get("status")

    def record(self, unit, status, **details):
        record = {
            "unit_id": unit.unit_id,
            "status": status,
            **unit.as_metadata(),
            **details,
        }
        with self.path.open("a") as log_file:
            log_file.write(json.dumps(record, default=str) + "\n")