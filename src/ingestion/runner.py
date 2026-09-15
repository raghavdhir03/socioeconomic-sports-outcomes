import time
import uuid

from .adapters import ingest_unit, table_name_for_unit
from .cache import DataFrameCache, JsonlIngestionLog
from .config import IngestionConfig
from .maxpreps_client import MaxPrepsClient


class IngestionRunner:
    def __init__(self, config: IngestionConfig, writer, client=None, cache=None, log=None, sleeper=time.sleep):
        config.validate()
        self.config = config
        self.writer = writer
        self.client = client or MaxPrepsClient(
            max_retries=config.max_retries,
            initial_backoff_seconds=config.initial_backoff_seconds,
            request_delay_seconds=config.request_delay_seconds,
            requests_per_second=config.requests_per_second,
            min_completeness_ratio=config.min_completeness_ratio,
            school_retry_attempts=config.school_retry_attempts,
            school_retry_delay_seconds=config.school_retry_delay_seconds,
            sleeper=sleeper,
        )
        self.cache = cache or DataFrameCache(config.cache_dir)
        self.log = log or JsonlIngestionLog(config.log_path)
        self.sleeper = sleeper

    def run(self):
        run_id = uuid.uuid4().hex
        results = []
        for unit in self.config.units():
            # "skipped" is itself evidence of a prior success (that's the only
            # reason a unit is ever logged that way) — treating only the
            # literal word "succeeded" as done means a unit's status flips to
            # "skipped" the first time it's skipped, and every run after that
            # stops recognizing it as done, silently redoing already-finished
            # work on every subsequent resume.
            if self.log.status(unit) in ("succeeded", "skipped") and self.cache.load(unit) is not None:
                self.log.record(unit, "skipped", run_id=run_id, reason="already_succeeded")
                results.append((unit.unit_id, "skipped"))
                continue

            self.log.record(unit, "running", run_id=run_id)
            try:
                dataframe = self.cache.load(unit)
                if dataframe is None:
                    dataframe = ingest_unit(self.client, unit, self.config.cities)
                    cache_path = self.cache.save(unit, dataframe)
                else:
                    cache_path = str(self.cache.path_for(unit))
                row_count = self.writer.write(dataframe, table_name_for_unit(unit))
                failed_schools = dataframe.attrs.get("failed_schools")
                self.log.record(
                    unit,
                    "succeeded",
                    run_id=run_id,
                    row_count=row_count,
                    cache_path=cache_path,
                    failed_schools=failed_schools,
                )
                results.append((unit.unit_id, "succeeded"))
            except Exception as error:
                self.log.record(unit, "failed", run_id=run_id, error_message=str(error))
                results.append((unit.unit_id, "failed"))
            self.sleeper(self.config.request_delay_seconds)
        return results