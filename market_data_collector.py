# market_data_collector.py

import yfinance as yf
import pandas as pd  # pandas is crucial for DataFrame operations
# FIX: Explicitly import datetime class as dt_class, date class as date_class, and timedelta
from datetime import datetime as dt_class, date as date_class, timedelta
import pymongo
from pymongo.errors import ConnectionFailure, DuplicateKeyError
import time  # For time.sleep
import requests  # requests might be indirectly used by yfinance, good to have it


# --- MongoDB Connection for Market Data ---
def connect_to_market_data_mongodb(host='localhost', port=27017, db_name='indian_market_scanner_db',
                                   collection_name='historical_market_data'):
    """
    Establishes a connection to MongoDB and returns the market data collection object.
    Creates a unique compound index on symbol and date.
    """
    try:
        client = pymongo.MongoClient(host, port, serverSelectionTimeoutMS=5000)
        client.admin.command('ismaster')
        print(f"Successfully connected to MongoDB for market data at {host}:{port}")
        db = client[db_name]
        collection = db[collection_name]

        collection.create_index([("symbol", pymongo.ASCENDING), ("date", pymongo.ASCENDING)], unique=True)
        print(f"Ensured unique compound index on 'symbol' and 'date' for collection '{collection_name}'")

        return collection
    except ConnectionFailure as e:
        print(f"Could not connect to MongoDB for market data: {e}. Please ensure MongoDB server is running.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during MongoDB market data connection: {e}")
        return None


# --- Helper Function for Caching Market Data ---
def get_latest_market_data_date(mongo_collection, symbol):
    """
    Retrieves the latest date for a given stock symbol from MongoDB.
    Returns datetime object or None if no data found.
    """
    if mongo_collection is None:
        return None

    latest_record = mongo_collection.find(
        {"symbol": symbol, "date": {"$ne": None}}
    ).sort("date", pymongo.DESCENDING).limit(1)

    try:
        latest = latest_record.next()
        # Use dt_class for datetime.datetime type check
        if isinstance(latest.get('date'), dt_class):
            return latest['date']
        return None
    except StopIteration:
        return None


# --- Fetch Historical Market Data Function ---
def fetch_historical_market_data(tickers, start_date_str, end_date_str, mongo_collection):
    """
    Fetches historical OHLCV data for given tickers using yfinance
    and inserts/updates it into the MongoDB collection.
    """
    print("\n--- Phase 4: Fetching Historical Market Data ---")
    if mongo_collection is None:
        print("MongoDB market data collection not available. Aborting market data fetch.")
        return 0

    total_inserted_count = 0

    for ticker in tickers:
        # Determine start date for smart fetching
        latest_date_in_db = get_latest_market_data_date(mongo_collection, ticker)

        # Ensure latest_date_in_db is a datetime object before using timedelta/strftime
        if latest_date_in_db and isinstance(latest_date_in_db, dt_class):  # Use dt_class for isinstance
            fetch_start_date_obj = latest_date_in_db + timedelta(days=1)
            fetch_start_date_str = fetch_start_date_obj.strftime('%Y-%m-%d')
            print(
                f"Latest data for {ticker} in DB is {latest_date_in_db.strftime('%Y-%m-%d')}. Fetching from {fetch_start_date_str}...")
        else:
            fetch_start_date_str = start_date_str
            print(f"No data for {ticker} in DB. Fetching from {fetch_start_date_str}...")

        # Use dt_class for datetime.strptime and dt_class.now()
        if dt_class.strptime(fetch_start_date_str, '%Y-%m-%d').date() > dt_class.now().date():
            print(f"Skipping {ticker}: Fetch start date {fetch_start_date_str} is in the future.")
            continue

        print(f"Fetching data for {ticker} from {fetch_start_date_str} to {end_date_str}...")
        try:
            data = yf.download(ticker, start=fetch_start_date_str, end=end_date_str)

            if data.empty:
                print(
                    f"No new data found for {ticker} in the specified date range ({fetch_start_date_str} to {end_date_str}).")
                continue

            data.reset_index(inplace=True)

            # Flatten the MultiIndex columns if they exist.
            new_columns = []
            for col in data.columns:
                if isinstance(col, tuple):
                    new_columns.append(col[0])  # Take the first element of the tuple, e.g., 'Date', 'Close'
                else:
                    new_columns.append(col)  # For regular column names

            data.columns = new_columns

            # Convert the 'Date' column to native Python datetime.date objects.
            # This is robust via pd.to_datetime and .dt.date
            if 'Date' in data.columns:
                data['Date'] = pd.to_datetime(data['Date']).dt.date
            else:
                raise ValueError("Expected 'Date' column not found after flattening and preprocessing.")

            # Ensure other expected columns are present and correctly named (strings)
            expected_price_volume_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            for col in expected_price_volume_cols:
                if col not in data.columns:
                    raise ValueError(
                        f"Missing expected column '{col}' in yfinance data for {ticker} after preprocessing.")

            # Convert the DataFrame to a list of dictionaries.
            records_to_insert = data.to_dict('records')

            # --- DEBUG STEP 1: Verify columns and data types AFTER all preprocessing ---
            if records_to_insert:  # Only print if there's data to show
                print(f"DEBUG (Market Data): Columns after all preprocessing: {data.columns.tolist()}")
                print(f"DEBUG (Market Data): First 3 rows of data:\n{data.head(3)}")
                print(
                    f"DEBUG (Market Data): Dtypes of processed columns:\n{data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']].dtypes}")
                print(f"DEBUG (Market Data): First record keys: {records_to_insert[0].keys()}")
                print(f"DEBUG (Market Data): First record: {records_to_insert[0]}")
            # --- END DEBUG ---

            inserted_count_for_ticker = 0

            for record in records_to_insert:  # Iterate through the list of dictionaries
                record_date_dt_date = record['Date']  # This is a datetime.date object

                # --- FIX: Convert datetime.date object to datetime.datetime (midnight) for MongoDB ---
                record_date_for_mongo = dt_class.combine(record_date_dt_date, dt_class.min.time())
                # --- END FIX ---

                market_record = {
                    'symbol': ticker,
                    'date': record_date_for_mongo,  # Use the datetime.datetime object for MongoDB
                    'open': record['Open'],
                    'high': record['High'],
                    'low': record['Low'],
                    'close': record['Close'],
                    'volume': record['Volume']
                }

                try:
                    result = mongo_collection.update_one(
                        {'symbol': ticker, 'date': record_date_for_mongo},  # Use for query too
                        {'$set': market_record},
                        upsert=True
                    )

                    if result.upserted_id:
                        inserted_count_for_ticker += 1
                        total_inserted_count += 1

                except DuplicateKeyError:
                    date_for_print = record_date_dt_date.strftime(
                        '%Y-%m-%d')  # Print original datetime.date for message
                    print(f"  Duplicate record for {ticker} on {date_for_print}. Skipped by unique index.")
                except Exception as e:
                    date_for_print = record_date_dt_date.strftime(
                        '%Y-%m-%d')  # Print original datetime.date for message
                    # FIX: Use dt_class and date_class explicitly here in isinstance
                    print(f"  Error inserting/updating {ticker} on {date_for_print}: {type(e).__name__}: {e}")

            print(f"Successfully processed {inserted_count_for_ticker} new/updated records for {ticker}.")
            time.sleep(1)

        except Exception as e:
            if isinstance(e, requests.exceptions.ConnectionError) or \
                    (hasattr(e, 'args') and isinstance(e.args[0], ConnectionResetError)):
                print(f"Failed to fetch data for {ticker} due to a network connection error: {e}")
            else:
                print(f"Failed to fetch or process data for {ticker}: {type(e).__name__}: {e}")

    print(f"\nHistorical market data collection complete. Total new/updated records: {total_inserted_count}.")
    return total_inserted_count


# --- Test Execution Block for market_data_collector.py ---
if __name__ == "__main__":
    print("--- Running Market Data Collector Separately for Testing ---")

    mongo_market_data_collection = connect_to_market_data_mongodb(db_name='indian_market_scanner_db',
                                                                  collection_name='historical_market_data')

    if mongo_market_data_collection is not None:
        nifty_index_tickers = [
            '^NSEI',  # Nifty 50
            '^NSEBANK',  # Nifty Bank
            '^CNXIT',  # Nifty IT
            '^CNXAUTO',  # Nifty Auto
            # '^CNXPHARM',# Removed as it was causing 404 errors
            '^CNXFMCG',  # Nifty FMCG
            '^CNXMETAL',  # Nifty Metal
            '^CNXMEDIA',  # Nifty Media
            '^CNXREALTY',  # Nifty Realty
            # Add more as needed
        ]

        today_date = dt_class.now()  # FIX: Use dt_class.now()
        start_date_hist = (today_date - timedelta(days=5 * 365)).strftime('%Y-%m-%d')
        end_date_hist = today_date.strftime('%Y-%m-%d')

        total_market_data_records = fetch_historical_market_data(
            tickers=nifty_index_tickers,
            start_date_str=start_date_hist,
            end_date_str=end_date_hist,
            mongo_collection=mongo_market_data_collection
        )
        if total_market_data_records > 0:
            print(f"\nSuccessfully collected {total_market_data_records} new/updated records.")
        else:
            print("\nNo new market data records collected or an error occurred during market data collection.")

    else:
        print("\nFailed to connect to MongoDB for market data. Aborting market data collection.")

    print("\nMarket Data Collector test execution complete.")
    print("Check your MongoDB: indian_market_scanner_db -> historical_market_data.")
    print(
        "Example mongosh query: `use indian_market_scanner_db; db.historical_market_data.find({'symbol': '^NSEI'}).sort({'date': -1}).limit(5).pretty()`")