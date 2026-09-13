from tempfile import TemporaryDirectory

import pandas as pd

from src.ingestion.adapters import add_metadata, table_name_for_unit
from src.ingestion.cache import DataFrameCache, JsonlIngestionLog
from src.ingestion.config import IngestionConfig
from src.ingestion.maxpreps_client import MaxPrepsClient
from src.ingestion.maxpreps_client import EmptyScrapeResultError
from src.ingestion.models import IngestionUnit
from src.ingestion.runner import IngestionRunner
from src.ingestion.writers import NoopWriter


class FakeScraper:
    def get_rankings(self, **kwargs):
        return pd.DataFrame({"Team": ["A"], "Rating": [1.0]})


class FailingThenWorkingScraper:
    def __init__(self):
        self.calls = 0

    def get_rankings(self, **kwargs):
        self.calls += 1
        if self.calls == 1:
            raise ConnectionError("temporary failure")
        return pd.DataFrame({"Team": ["A"]})


class EmptyScraper:
    def get_rankings(self, **kwargs):
        return pd.DataFrame()


class FakeContestsScraper:
    def get_contests(self, **kwargs):
        return pd.DataFrame({"Team 1": ["A"], "Team 2": ["B"]})


class IncompleteThenCompleteScraper:
    """Simulates a state scrape that's badly incomplete once, then clean."""

    def __init__(self):
        self.calls = 0

    def get_contests(self, **kwargs):
        self.calls += 1
        dataframe = pd.DataFrame({"Team 1": ["A"], "Team 2": ["B"]})
        dataframe.attrs["schools_discovered"] = 10
        dataframe.attrs["schools_scraped"] = 5 if self.calls == 1 else 10
        return dataframe


class FailedSchoolsScraper:
    """Simulates a state scrape that's above the completeness threshold but
    still has a specific, known-missing school worth logging."""

    def get_contests(self, **kwargs):
        dataframe = pd.DataFrame({"Team 1": ["A"], "Team 2": ["B"]})
        dataframe.attrs["schools_discovered"] = 2
        dataframe.attrs["schools_scraped"] = 1
        dataframe.attrs["failed_schools"] = [{"school": "Ghost High", "url": "/ghost/schedule/"}]
        return dataframe


def test_config_expands_small_units():
    config = IngestionConfig(
        states=["tx", "ca"],
        sports=["basketball"],
        seasons=["23-24"],
        genders=["boys", "girls"],
        ingestion_types=["rankings"],
    )
    assert [unit.unit_id for unit in config.units()] == [
        "rankings:tx:basketball:boys:23-24",
        "rankings:tx:basketball:girls:23-24",
        "rankings:ca:basketball:boys:23-24",
        "rankings:ca:basketball:girls:23-24",
    ]


def test_client_retries_with_exponential_backoff():
    scraper = FailingThenWorkingScraper()
    delays = []
    client = MaxPrepsClient(scraper, max_retries=2, initial_backoff_seconds=2, sleeper=delays.append)

    result = client.fetch("rankings", "tx", "basketball", "23-24")

    assert len(result) == 1
    assert scraper.calls == 2
    assert delays == [2]


def test_client_rejects_empty_results():
    client = MaxPrepsClient(EmptyScraper(), max_retries=1, sleeper=lambda _: None)
    try:
        client.fetch("rankings", "tx", "basketball", "23-24")
    except EmptyScrapeResultError as error:
        assert "returned no rows" in str(error)
    else:
        raise AssertionError("empty scraper result should fail the ingestion")


def test_metadata_row_key_ignores_scrape_timestamp():
    unit = IngestionUnit("rankings", "tx", "basketball", "23-24")
    first = add_metadata(pd.DataFrame({"Team": ["A"]}), unit)
    second = add_metadata(pd.DataFrame({"Team": ["A"]}), unit)
    assert first.loc[0, "_ROW_KEY"] == second.loc[0, "_ROW_KEY"]


def test_runner_reuses_cache_and_resumes_successful_units():
    config = IngestionConfig(
        states=["tx"], sports=["basketball"], seasons=["23-24"],
        genders=["boys"], ingestion_types=["rankings"],
        request_delay_seconds=0,
    )
    with TemporaryDirectory() as directory:
        config.cache_dir = directory + "/cache"
        config.log_path = directory + "/ingestion.jsonl"
        writer = NoopWriter()
        scraper = FakeScraper()
        runner = IngestionRunner(
            config,
            writer,
            client=MaxPrepsClient(scraper, request_delay_seconds=0),
            cache=DataFrameCache(config.cache_dir),
            log=JsonlIngestionLog(config.log_path),
            sleeper=lambda _: None,
        )

        assert runner.run() == [("rankings:tx:basketball:boys:23-24", "succeeded")]
        assert runner.run() == [("rankings:tx:basketball:boys:23-24", "skipped")]
        assert len(writer.writes) == 1


def test_table_name_for_unit_adds_sport_and_season_for_contests():
    unit = IngestionUnit("contests", "tx", "basketball", "19-20")
    assert table_name_for_unit(unit) == "RAW_MAXPREPS_CONTESTS_BASKETBALL_19_20"


def test_table_name_for_unit_rankings_and_districts_unchanged():
    rankings_unit = IngestionUnit("rankings", "tx", "basketball", "23-24")
    districts_unit = IngestionUnit("districts", "tx", "basketball", "23-24")
    assert table_name_for_unit(rankings_unit) == "RAW_MAXPREPS_RANKINGS"
    assert table_name_for_unit(districts_unit) == "RAW_MAXPREPS_DISTRICTS"


def test_runner_writes_contests_to_per_season_tables():
    config = IngestionConfig(
        states=["tx"], sports=["basketball"], seasons=["19-20", "20-21"],
        genders=["boys"], ingestion_types=["contests"],
        request_delay_seconds=0,
    )
    with TemporaryDirectory() as directory:
        config.cache_dir = directory + "/cache"
        config.log_path = directory + "/ingestion.jsonl"
        writer = NoopWriter()
        runner = IngestionRunner(
            config,
            writer,
            client=MaxPrepsClient(FakeContestsScraper(), request_delay_seconds=0),
            cache=DataFrameCache(config.cache_dir),
            log=JsonlIngestionLog(config.log_path),
            sleeper=lambda _: None,
        )

        runner.run()

        assert [table for table, _ in writer.writes] == [
            "RAW_MAXPREPS_CONTESTS_BASKETBALL_19_20",
            "RAW_MAXPREPS_CONTESTS_BASKETBALL_20_21",
        ]


def test_client_retries_on_incomplete_contests_scrape():
    scraper = IncompleteThenCompleteScraper()
    delays = []
    client = MaxPrepsClient(scraper, max_retries=2, initial_backoff_seconds=1, sleeper=delays.append)

    result = client.fetch("contests", "tx", "basketball", "23-24")

    assert scraper.calls == 2
    assert delays == [1]
    assert result.attrs["schools_scraped"] == 10


def test_client_forwards_rate_limit_kwargs_to_default_scraper(monkeypatch):
    captured = {}

    class RecordingScraper:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(
        "src.ingestion.maxpreps_client._load_scraper_class",
        lambda: RecordingScraper,
    )

    MaxPrepsClient(
        max_retries=3,
        initial_backoff_seconds=1.5,
        requests_per_second=2.0,
        school_retry_attempts=2,
        school_retry_delay_seconds=10,
    )

    assert captured == {
        "requests_per_second": 2.0,
        "max_retries": 3,
        "backoff_factor": 1.5,
        "school_retry_attempts": 2,
        "school_retry_delay_seconds": 10,
    }


def test_runner_logs_failed_schools_from_contests_scrape():
    config = IngestionConfig(
        states=["tx"], sports=["basketball"], seasons=["23-24"],
        genders=["boys"], ingestion_types=["contests"],
        request_delay_seconds=0,
    )
    with TemporaryDirectory() as directory:
        config.cache_dir = directory + "/cache"
        config.log_path = directory + "/ingestion.jsonl"
        log = JsonlIngestionLog(config.log_path)
        runner = IngestionRunner(
            config,
            NoopWriter(),
            client=MaxPrepsClient(FailedSchoolsScraper(), request_delay_seconds=0, min_completeness_ratio=0.1),
            cache=DataFrameCache(config.cache_dir),
            log=log,
            sleeper=lambda _: None,
        )

        runner.run()

        unit = next(config.units())
        record = log.latest()[unit.unit_id]
        assert record["failed_schools"] == [{"school": "Ghost High", "url": "/ghost/schedule/"}]