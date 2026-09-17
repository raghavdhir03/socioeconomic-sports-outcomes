import hashlib
import json

import pandas as pd

from .maxpreps_client import MaxPrepsClient
from .models import IngestionUnit


TABLES = {
    "contests": "RAW_MAXPREPS_CONTESTS",
    "rankings": "RAW_MAXPREPS_RANKINGS",
    "districts": "RAW_MAXPREPS_DISTRICTS",
}


def _row_key(row):
    values = {
        str(key): str(value)
        for key, value in row.items()
        if key not in {"_ROW_KEY", "SCRAPED_AT"}
    }
    return hashlib.sha256(
        json.dumps(values, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def _stringify_mixed_object_columns(dataframe):
    """Force object columns with mixed Python types (e.g. pd.read_html turning
    '+3' into a str but '-1' into an int within the same column) to a single
    string dtype, since Snowflake's write_pandas cannot infer an Arrow type
    for a column holding more than one Python type."""
    result = dataframe.copy()
    for column in result.columns:
        if result[column].dtype != "object":
            continue
        non_null = result[column].dropna()
        if non_null.map(type).nunique() > 1:
            result[column] = result[column].where(result[column].isna(), result[column].astype(str))
    return result


def table_name_for_unit(unit: IngestionUnit) -> str:
    """Snowflake table to write a unit's data into.

    Contests get one physical table per sport/season (e.g.
    RAW_MAXPREPS_CONTESTS_BASKETBALL_19_20) so all states for a given
    sport/season accumulate together; rankings/districts keep the single
    shared table they already use.

    Boys' table names carry no gender segment at all — they're the
    existing, already-ingested tables, and changing their names now would
    require migrating real data and break anything already querying them.
    Any other gender (currently just "girls") gets its own explicitly
    labeled set of tables instead of sharing boys' table.
    """
    base = TABLES[unit.ingestion_type]
    if unit.ingestion_type == "contests":
        season = unit.season.replace("-", "_")
        gender_segment = "" if unit.boys else f"_{unit.gender.upper()}"
        return f"{base}_{unit.sport.upper()}{gender_segment}_{season}"
    return base


def add_metadata(dataframe, unit: IngestionUnit):
    result = _stringify_mixed_object_columns(dataframe)
    for key, value in unit.as_metadata().items():
        result[key] = value
    result["SCRAPED_AT"] = pd.Timestamp.utcnow()
    result["_ROW_KEY"] = result.apply(_row_key, axis=1)

    # A _ROW_KEY collision means two rows are identical in every tracked
    # column (same game, same score, etc.) — the scraper can produce this
    # (e.g. a school appearing twice in a state's rankings pagination) and
    # Snowflake's MERGE can't handle two source rows matching one target key,
    # so it fails outright rather than picking one. Drop to the first
    # occurrence rather than let that reach the writer.
    attrs = result.attrs
    result = result.drop_duplicates(subset="_ROW_KEY", keep="first").reset_index(drop=True)
    result.attrs = attrs
    return result


def ingest_unit(client: MaxPrepsClient, unit: IngestionUnit, cities=None):
    dataframe = client.fetch(
        unit.ingestion_type,
        state=unit.state,
        sport=unit.sport,
        season=unit.season,
        boys=unit.boys,
        cities=cities,
    )
    if dataframe is None:
        dataframe = pd.DataFrame()
    return add_metadata(dataframe, unit)