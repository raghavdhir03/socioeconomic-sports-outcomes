import os
from dotenv import load_dotenv
import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

# Sample dataframe
df = pd.DataFrame({
    "TEAM": ["A", "B", "C"],
    "WINS": [10, 8, 12],
    "AVG_INCOME": [72000, 54000, 91000]
})

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    authenticator="SNOWFLAKE_JWT",
    private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"),
    private_key_file_pwd=os.getenv("SNOWFLAKE_PRIVATE_KEY"),
    role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

try:
    success, nchunks, nrows, output = write_pandas(
        conn,
        df,
        table_name="TEST_TEAMS",
        auto_create_table=True,
        overwrite=True
    )

    print("Success:", success)
    print("Rows uploaded:", nrows)

finally:
    conn.close()