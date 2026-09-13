import re
import uuid

import pandas as pd


def _identifier(value):
    if not re.fullmatch(r"[A-Za-z0-9_]+", value):
        raise ValueError(f"Unsafe Snowflake identifier: {value}")
    return value.upper()


class SnowflakeWriter:
    def __init__(self, connection_factory, chunk_size=5000, schema=None):
        self.connection_factory = connection_factory
        self.chunk_size = chunk_size
        self.schema = schema

    def write(self, dataframe: pd.DataFrame, table_name: str):
        if dataframe.empty:
            return 0
        # write_pandas auto-creates the staging table using the dataframe's own
        # column names verbatim (as quoted, case-sensitive identifiers). The
        # MERGE SQL below references columns as column.upper(), so the frame's
        # columns must be uppercased first or the two will not match.
        dataframe = dataframe.rename(columns=str.upper)
        table_name = _identifier(table_name)
        connection = self.connection_factory()
        stage_name = f"{table_name}__STAGE_{uuid.uuid4().hex[:12]}".upper()
        try:
            from snowflake.connector.pandas_tools import write_pandas

            first_chunk = True
            row_count = 0
            for start in range(0, len(dataframe), self.chunk_size):
                chunk = dataframe.iloc[start : start + self.chunk_size]
                success, _, rows, _ = write_pandas(
                    conn=connection,
                    df=chunk,
                    table_name=stage_name,
                    schema=self.schema.upper() if self.schema else None,
                    auto_create_table=True,
                    overwrite=first_chunk,
                )
                if not success:
                    raise RuntimeError(f"Failed to write Snowflake staging table {stage_name}")
                first_chunk = False
                row_count += rows

            qualified_stage = self._qualified(stage_name)
            qualified_target = self._qualified(table_name)
            columns = [f'"{column.upper()}"' for column in dataframe.columns]
            update_columns = [column for column in columns if column != '"_ROW_KEY"']
            update_sql = ", ".join(f"target.{column} = source.{column}" for column in update_columns)
            insert_columns = ", ".join(columns)
            insert_values = ", ".join(f"source.{column}" for column in columns)
            connection.cursor().execute(
                f"CREATE TABLE IF NOT EXISTS {qualified_target} LIKE {qualified_stage}"
            )
            connection.cursor().execute(
                f"MERGE INTO {qualified_target} target USING {qualified_stage} source "
                f"ON target.\"_ROW_KEY\" = source.\"_ROW_KEY\" "
                f"WHEN MATCHED THEN UPDATE SET {update_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
            )
            connection.cursor().execute(f"DROP TABLE IF EXISTS {qualified_stage}")
            connection.commit()
            return row_count
        finally:
            connection.close()

    def _qualified(self, table_name):
        table_name = _identifier(table_name)
        return f'"{self.schema.upper()}"."{table_name}"' if self.schema else f'"{table_name}"'


class NoopWriter:
    """Writer used for dry runs and local validation."""

    def __init__(self):
        self.writes = []

    def write(self, dataframe, table_name):
        self.writes.append((table_name, dataframe.copy()))
        return len(dataframe)