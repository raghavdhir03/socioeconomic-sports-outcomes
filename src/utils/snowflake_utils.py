# src/utils/snowflake_utils.py

import os
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()


def get_snowflake_connection():
    connection_args = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
    }
    if os.getenv("SNOWFLAKE_AUTHENTICATOR"):
        connection_args["authenticator"] = os.getenv("SNOWFLAKE_AUTHENTICATOR")
    if os.getenv("SNOWFLAKE_PASSWORD"):
        connection_args["password"] = os.getenv("SNOWFLAKE_PASSWORD")
    if os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"):
        connection_args["private_key_file"] = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
        connection_args["private_key_file_pwd"] = os.getenv("SNOWFLAKE_PRIVATE_KEY")
    return snowflake.connector.connect(**connection_args)


def write_dataframe(df, table_name, schema=None):
    conn = get_snowflake_connection()

    try:
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name.upper(),
            schema=schema.upper() if schema else None,
            auto_create_table=True,
            overwrite=False,
        )

        if not success:
            raise RuntimeError("Failed to write dataframe to Snowflake.")

        print(f"Uploaded {nrows} rows to {table_name}")

    finally:
        conn.close()