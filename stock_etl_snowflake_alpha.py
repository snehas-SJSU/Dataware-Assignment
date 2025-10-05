# -----------------------------
# Import Libraries and Snowflake Connections
# -----------------------------
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import pandas as pd
import requests
import snowflake.connector

# -----------------------------
# Snowflake Helper Function
# -----------------------------
# This function establishes a connection to Snowflake using Airflow's SnowflakeHook
# It returns a cursor object that can be used to execute SQL queries
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn().cursor()


# -----------------------------
# Step 1: Extraction Task
# -----------------------------
# This task fetches daily stock price data from Alpha Vantage API
# Returns the "Time Series (Daily)" portion as a dictionary
@task
def extract_stock_data(symbol, api_key):
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
    response = requests.get(url)
    data = response.json()
    time_series = data.get("Time Series (Daily)", {})
    print(f"Extracted {len(time_series)} rows for symbol {symbol}")
    return time_series


# -----------------------------
# Step 2: Transformation Task
# -----------------------------
# Converts the JSON from API into a clean pandas DataFrame
# Adds symbol column, renames columns, sorts by date, keeps last 90 trading days
@task
def transform_stock_data(time_series, symbol):
    records = []
    for date_str, daily_data in time_series.items():
        daily_data['date'] = date_str
        records.append(daily_data)

    df_stock = pd.DataFrame(records)
    df_stock.rename(columns={
        '1. open': 'open',
        '2. high': 'high',
        '3. low': 'low',
        '4. close': 'close',
        '5. volume': 'volume'
    }, inplace=True)

    df_stock['symbol'] = symbol
    df_stock = df_stock[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']]
    df_stock['date'] = pd.to_datetime(df_stock['date'])
    df_stock.sort_values('date', inplace=True)
    df_stock.reset_index(drop=True, inplace=True)

    # Keep only last 90 trading days
    df_stock = df_stock.tail(90)

    print(f"Transformed data for {symbol}, showing last 5 rows:")
    print(df_stock.tail(5))
    return df_stock


# -----------------------------
# Step 3: Load Task
# -----------------------------
# Loads the cleaned DataFrame into Snowflake
# Performs full-refresh:
#   - Creates table if not exists
#   - Deletes old records
#   - Inserts new records transactionally
@task
def load_stock_data(df, table_name="raw.stock_prices_raw"):
    df['date'] = pd.to_datetime(df['date'])
    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")

        # Create table if not exists
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            symbol VARCHAR,
            date DATE,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            PRIMARY KEY(symbol, date)
        );
        """)

        # Delete old records (full refresh)
        cur.execute(f"DELETE FROM {table_name};")
        print(f"Deleted old records from {table_name}.")

        # Insert new records
        for r in df.to_dict(orient='records'):
            symbol_safe = r['symbol'].replace("'", "''")
            cur.execute(f"""
                INSERT INTO {table_name} (symbol, date, open, high, low, close, volume)
                VALUES (
                    '{symbol_safe}', 
                    '{r['date'].strftime('%Y-%m-%d')}', 
                    {r['open']}, {r['high']}, {r['low']}, {r['close']}, {r['volume']}
                );
            """)

        cur.execute("COMMIT;")
        print(f"Loaded {len(df)} rows into {table_name} successfully.")

    except Exception as e:
        cur.execute("ROLLBACK;")
        print("Error during load, transaction rolled back:", e)
        raise e
    finally:
        cur.close()


# -----------------------------
# Step 4: DAG Definition
# -----------------------------
# This DAG executes the ETL pipeline:
#   1. Extract stock data from Alpha Vantage
#   2. Transform data to clean format
#   3. Load data into Snowflake table
with DAG(
    dag_id='stock_data_etl_v1',          # Unique DAG ID
    start_date=datetime(2025, 10, 1),    # Start date
    catchup=False,                        # Do not backfill
    schedule_interval='0 2 * * *',        # Daily at 2:00 AM
    tags=['ETL', 'Stock'],                # Tags for Airflow UI
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:

    # Fetch Airflow Variables
    api_key = Variable.get("alphavantage_api_key")
    symbol = Variable.get("stock_symbol", default_var="AAPL")  # Can be parameterized

    # Define task dependencies
    extracted_data = extract_stock_data(symbol, api_key)
    transformed_data = transform_stock_data(extracted_data, symbol)
    load_stock_data(transformed_data)
