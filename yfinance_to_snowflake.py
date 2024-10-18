from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import yfinance as yf
from datetime import datetime, timedelta
import logging

# Snowflake credentials
SNOWFLAKE_CONFIG = {
    "account": "svpfiiy-awb47013",
    "user": "pranavss722",
    "password": "AntKrumpMatteo2001!",
    "warehouse": "COMPUTE_WH",
    "database": "NAV1",
    "schema": "PUBLIC"
}

def get_next_day(date_str: str) -> str:
    """Returns the next day for a given date string in 'YYYY-MM-DD' format."""
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    next_day = date_obj + timedelta(days=1)
    return next_day.strftime("%Y-%m-%d")

def return_snowflake_conn():
    """Creates and returns a Snowflake connection using SnowflakeHook."""
    hook = SnowflakeHook(
        account=SNOWFLAKE_CONFIG["account"],
        user=SNOWFLAKE_CONFIG["user"],
        password=SNOWFLAKE_CONFIG["password"],
        warehouse=SNOWFLAKE_CONFIG["warehouse"],
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"]
    )
    return hook.get_conn().cursor()

def get_logical_date() -> str:
    """Retrieves the logical execution date from the Airflow context."""
    context = get_current_context()
    return str(context['logical_date'])[:10]

@task
def extract(symbol: str) -> dict:
    """Extracts stock data from Yahoo Finance for a specific symbol and date."""
    date = get_logical_date()
    end_date = get_next_day(date)

    try:
        data = yf.download(symbol, start=date, end=end_date)

        if data.empty:
            logging.warning(f"No data found for {symbol} on {date}. Check if the market is open or if the symbol is correct.")
            return {}

        return data.to_dict(orient="list")
    except Exception as e:
        logging.error(f"Error fetching data for {symbol} on {date}: {e}")
        return {}

@task
def load(data: dict, symbol: str, target_table: str):
    """Loads stock data into a Snowflake table. Creates the table if it doesn't exist."""
    date = get_logical_date()
    cur = return_snowflake_conn()

    if not data:
        logging.error(f"No data to load for {symbol} on {date}. Skipping load.")
        return

    try:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                date DATE,
                open FLOAT,
                close FLOAT,
                high FLOAT,
                low FLOAT,
                volume INT,
                symbol VARCHAR
            )
        """)

        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {target_table} WHERE date='{date}' AND symbol='{symbol}'")

        insert_query = f"""
            INSERT INTO {target_table} (date, open, close, high, low, volume, symbol)
            VALUES (
                '{date}',
                {data['Open'][0]}, {data['Close'][0]},
                {data['High'][0]}, {data['Low'][0]},
                {data['Volume'][0]}, '{symbol}'
            )
        """
        logging.info(f"Executing query: {insert_query}")
        cur.execute(insert_query)
        cur.execute("COMMIT;")
        logging.info(f"Data for {symbol} on {date} loaded successfully.")
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error(f"Error during Snowflake operation: {e}")
        raise
    finally:
        cur.close()

with DAG(
    dag_id='YfinanceToSnowflake',
    start_date=datetime(2024, 10, 2),
    schedule='30 2 * * *',
    catchup=False,
    tags=['ETL'],
) as dag:
    target_table = "STOCK_PRICES"
    symbol = "AAPL"

    data = extract(symbol)
    load(data, symbol, target_table)
