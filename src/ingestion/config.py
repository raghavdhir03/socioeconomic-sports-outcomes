import json
from dataclasses import dataclass, field
from itertools import product
from pathlib import Path

from .models import IngestionUnit


@dataclass
class IngestionConfig:
    states: list[str] = field(default_factory=list)
    sports: list[str] = field(default_factory=lambda: ["basketball"])
    seasons: list[str] = field(default_factory=list)
    genders: list[str] = field(default_factory=lambda: ["boys"])
    ingestion_types: list[str] = field(default_factory=lambda: ["contests"])
    cities: list[str] | None = None
    max_retries: int = 4
    initial_backoff_seconds: float = 2.0
    request_delay_seconds: float = 1.0
    requests_per_second: float | None = None
    min_completeness_ratio: float = 0.9
    school_retry_attempts: int = 0
    school_retry_delay_seconds: float = 5.0
    cache_dir: str = ".cache/maxpreps"
    log_path: str = ".cache/maxpreps/ingestion.jsonl"
    snowflake_chunk_size: int = 5000

    @classmethod
    def from_json(cls, path):
        with Path(path).open() as config_file:
            return cls(**json.load(config_file))

    def units(self):
        for ingestion_type, state, sport, gender, season in product(
            self.ingestion_types,
            self.states,
            self.sports,
            self.genders,
            self.seasons,
        ):
            if gender not in {"boys", "girls"}:
                raise ValueError("genders must contain only 'boys' or 'girls'")
            yield IngestionUnit(
                ingestion_type=ingestion_type,
                state=state.lower(),
                sport=sport.lower(),
                season=season,
                boys=gender == "boys",
            )

    def validate(self):
        if not self.states or not self.seasons:
            raise ValueError("states and seasons must contain at least one value")
        if self.max_retries < 1:
            raise ValueError("max_retries must be at least 1")
        if self.initial_backoff_seconds < 0 or self.request_delay_seconds < 0:
            raise ValueError("backoff and request delay values cannot be negative")
        if self.snowflake_chunk_size < 1:
            raise ValueError("snowflake_chunk_size must be positive")
        if self.requests_per_second is not None and self.requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        if not 0 < self.min_completeness_ratio <= 1:
            raise ValueError("min_completeness_ratio must be between 0 (exclusive) and 1")
        if self.school_retry_attempts < 0:
            raise ValueError("school_retry_attempts cannot be negative")
        if self.school_retry_delay_seconds < 0:
            raise ValueError("school_retry_delay_seconds cannot be negative")
        list(self.units())