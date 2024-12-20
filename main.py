import os
import requests
import pandas as pd
import duckdb
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(
    filename="etl_errors.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv()

API_KEY = os.getenv("CURRENCYBEACON_API_KEY")
if not API_KEY:
    raise ValueError("CURRENCYBEACON_API_KEY environment variable not set.")

DB_PATH = "data/currencywarehouse.duckdb"
HISTORICAL_URL = "https://api.currencybeacon.com/v1/historical"
BASE_CURRENCY = "USD"
INITIAL_START_DATE = date(1996, 1, 1)  # If no data is present, start from here.

def validate_api_key():
    url = "https://api.currencybeacon.com/v1/currencies"
    params = {"api_key": API_KEY}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        print("API key validated successfully.")
    except requests.exceptions.HTTPError as e:
        print("Invalid API key or network issue:", e)
        exit(1)

def get_connection():
    os.makedirs("data", exist_ok=True)
    return duckdb.connect(DB_PATH)

def create_schema_if_not_exists():
    con = get_connection()
    schema_sql = open("schema.sql", "r").read()
    con.execute(schema_sql)
    con.close()

def fetch_currencies():
    url = "https://api.currencybeacon.com/v1/currencies"
    params = {"api_key": API_KEY}
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    # Extract response key
    currencies = data.get("response", [])
    df = pd.DataFrame(currencies)

    print("Fetched DataFrame from API:")
    print(df.head())  # Debug: print the first few rows
    print(f"DataFrame shape: {df.shape}")  # Debug: Check row and column counts
    return df

def update_dim_currency_schema(new_columns, default_type="VARCHAR"):
    con = get_connection()
    existing_columns = con.execute("PRAGMA table_info('dim_currency')").df()['name'].tolist()
    for col in new_columns:
        if col not in existing_columns:
            print(f"Adding new column '{col}' to dim_currency...")
            con.execute(f"ALTER TABLE dim_currency ADD COLUMN {col} {default_type}")
    con.close()

def insert_currency_dimensions():
    # Fetch data from the API
    df = fetch_currencies()

    # Ensure required columns exist
    needed_cols = ["id", "name", "short_code", "code", "precision", "subunit", "symbol", "symbol_first", "decimal_mark", "thousands_separator"]
    for col in needed_cols:
        if col not in df.columns:
            print(f"Missing column: {col}")
            df[col] = None

    print("Cleaned currencies DataFrame:")
    print(df.head())

    con = get_connection()
    try:
        if not df.empty:
            # Register the DataFrame for operations
            con.register("currency_df", df[needed_cols])

            # Delete stale records
            print("Deleting stale records from dim_currency...")
            con.execute("""
                DELETE FROM dim_currency
                WHERE currency_id IN (SELECT id FROM currency_df)
            """)

            # Insert updated records
            print("Inserting new records into dim_currency...")
            con.execute("""
                INSERT INTO dim_currency (currency_id, name, short_code, code, precision, subunit, symbol, symbol_first, decimal_mark, thousands_separator)
                SELECT id, name, short_code, code, precision, subunit, symbol, symbol_first, decimal_mark, thousands_separator
                FROM currency_df
            """)
            print("dim_currency updated successfully.")
        else:
            print("currency_df is empty. Skipping update.")
    except Exception as e:
        logging.error(f"Error during dim_currency insertion: {e}")
        print("Error during dim_currency insertion. Check logs for details.")
    finally:
        con.close()


def transform_data(data):
    print("API Response:", data)
    base_currency = data.get("base")
    rates = data.get("rates", {})
    date_str = data.get("date")
    timestamp = data.get("timestamp")

    # Validate rates structure
    if not isinstance(rates, dict):
        print("Unexpected rates structure. Skipping transformation.")
        return pd.DataFrame()

    # Transform rates into a DataFrame
    if rates:
        df = pd.DataFrame(list(rates.items()), columns=["currency_code", "rate"])
    else:
        print(f"No rates available for {date_str}. Skipping.")
        return pd.DataFrame()

    # Add base currency and date information
    df["base_currency"] = base_currency
    df["date"] = pd.to_datetime(date_str) if date_str else pd.to_datetime(datetime.now())
    df["timestamp"] = timestamp or pd.Timestamp.now().timestamp()

    print("Transformed DataFrame:", df.head())  # Debug print to check the result
    return df

def load_data(df):
    try:
        if df.empty:
            print("No data to load. Skipping.")
            return

        con = get_connection()

        # Insert date dimension
        date_df = pd.DataFrame(df["date"].unique(), columns=["date"])
        date_df["year"] = date_df["date"].dt.year
        date_df["month"] = date_df["date"].dt.month
        date_df["day"] = date_df["date"].dt.day

        if date_df.empty:  # Check for empty date_df
            print("No new dates to insert into dim_date. Skipping.")
            return

        existing_max_id = con.execute("SELECT COALESCE(MAX(date_id), 0) FROM dim_date").fetchone()[0]
        date_df.insert(0, "date_id", range(existing_max_id + 1, existing_max_id + 1 + len(date_df)))

        con.register("date_df", date_df)
        con.execute("""
            INSERT INTO dim_date (date_id, date, year, month, day)
            SELECT date_id, date, year, month, day FROM date_df
        """)

        # Fetch valid currencies
        dim_currency = con.execute("SELECT currency_id, short_code FROM dim_currency").df()
        valid_currencies = dim_currency["short_code"].unique()
        unmatched_currencies = df[~df["currency_code"].isin(valid_currencies)]
        if not unmatched_currencies.empty:
            print(f"Unmatched currencies: {unmatched_currencies['currency_code'].unique()}")

        df = df[df["currency_code"].isin(valid_currencies)]

        # Map currency_code to currency_id
        merged = df.merge(dim_currency, left_on="currency_code", right_on="short_code", how="left")

        # Map date dimension
        dim_date = con.execute("SELECT date_id, date FROM dim_date").df()
        merged = merged.merge(dim_date, on="date", how="left")

        # Generate fact_id manually
        existing_fact_id = con.execute("SELECT COALESCE(MAX(fact_id), 0) FROM fact_exchange_rate").fetchone()[0]
        merged.insert(0, "fact_id", range(existing_fact_id + 1, existing_fact_id + 1 + len(merged)))

        # Insert into fact_exchange_rate
        merged = merged[["fact_id", "date_id", "currency_id", "rate", "base_currency", "timestamp"]]
        con.register("merged_df", merged)
        con.execute("""
            INSERT INTO fact_exchange_rate (fact_id, date_id, currency_id, rate, base_currency, timestamp)
            SELECT fact_id, date_id, currency_id, rate, base_currency, timestamp FROM merged_df
        """)

        print("fact_exchange_rate updated successfully.")
        con.close()

    except Exception as e:
        logging.error(f"Error in load_data: {e}")
        print("An error occurred during data loading. Check the logs for details.")



def get_max_loaded_date():
    con = get_connection()
    result = con.execute("SELECT MAX(date) FROM dim_date").fetchone()
    print("MAX(date) fetched:", result)  # Debugging
    con.close()
    return result[0] if result and result[0] else None

def extract_historical_data(fetch_date):
    params = {
        "api_key": API_KEY,
        "date": fetch_date.strftime("%Y-%m-%d"),
        "base": BASE_CURRENCY
    }
    response = requests.get(HISTORICAL_URL, params=params)
    response.raise_for_status()
    data = response.json()
    return data

def load_incremental_data(start_date):
    today = date.today()
    end_date = today - timedelta(days=1)

    if start_date > end_date:
        print("No new data to load. Already up to date.")
        return

    current_date = start_date
    while current_date <= end_date:
        try:
            print(f"Fetching historical data for {current_date}...")
            data = extract_historical_data(current_date)  # Ensure this matches the function name
            df = transform_data(data)
            if not df.empty:
                load_data(df)
        except Exception as e:
            print(f"Error processing data for {current_date}: {e}")
        current_date += timedelta(days=1)


def main():
    #validate API key
    validate_api_key()

    # Create schema if not exists
    create_schema_if_not_exists()
    
    # Insert currency dimensions only if dim_currency is empty
    con = get_connection()
    dim_currency_count = con.execute("SELECT COUNT(*) FROM dim_currency").fetchone()[0]
    con.close()

    if dim_currency_count == 0:
        print("dim_currency is empty, loading currency dimensions...")
        insert_currency_dimensions()
    else:
        print("dim_currency already populated. Skipping currency dimensions load.")

    # Get the maximum date from dim_date
    max_date = get_max_loaded_date()
    if max_date is None:
        print("No existing data found. Starting from the initial start date.")
        start_date = INITIAL_START_DATE
    else:
        print(f"Resuming data load from {max_date + timedelta(days=1)}.")
        start_date = max_date + timedelta(days=1)

    # Load data incrementally
    load_incremental_data(start_date)

    print("Incremental ETL process complete.")

if __name__ == "__main__":
    main()