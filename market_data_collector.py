# market_data_collector.py

import yfinance as yf
import pandas as pd  # pandas is crucial for DataFrame operations
from datetime import datetime, timedelta
import pymongo
from pymongo.errors import ConnectionFailure, DuplicateKeyError
import time  # For time.sleep
import requests

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
        if isinstance(latest.get('date'), datetime):
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
        if latest_date_in_db and isinstance(latest_date_in_db, datetime):
            # Fetch from one day after the latest date in DB
            fetch_start_date_obj = latest_date_in_db + timedelta(days=1)
            fetch_start_date_str = fetch_start_date_obj.strftime('%Y-%m-%d')
            print(
                f"Latest data for {ticker} in DB is {latest_date_in_db.strftime('%Y-%m-%d')}. Fetching from {fetch_start_date_str}...")
        else:
            # If no data in DB, fetch from the provided start_date_str
            fetch_start_date_str = start_date_str
            print(f"No data for {ticker} in DB. Fetching from {fetch_start_date_str}...")

        # Ensure fetch_start_date_str is not in the future
        if datetime.strptime(fetch_start_date_str, '%Y-%m-%d').date() > datetime.now().date():
            print(f"Skipping {ticker}: Fetch start date {fetch_start_date_str} is in the future.")
            continue

        print(f"Fetching data for {ticker} from {fetch_start_date_str} to {end_date_str}...")
        try:
            data = yf.download(ticker, start=fetch_start_date_str, end=end_date_str)

            if data.empty:
                print(
                    f"No new data found for {ticker} in the specified date range ({fetch_start_date_str} to {end_date_str}).")
                continue

            # --- ULTIMATE FIX FOR PANDAS DATAFRAME TO DICT CONVERSION ---
            # Step 1: Convert original index (Date) to a column named 'Date'
            # This is robust regardless of whether the index was named or not.
            data.reset_index(inplace=True)

            # Step 2: Ensure the 'Date' column is converted to native Python datetime.date objects.
            # This is done robustly via pd.to_datetime and .dt.date
            # The .dt.date accessor explicitly gets the date object from a pandas Timestamp.
            data['Date'] = pd.to_datetime(data['Date']).dt.date

            # Step 3: Convert the DataFrame to a list of dictionaries.
            # This is where all values become scalar, native Python types.
            # It completely bypasses issues with 'Series' objects from .iterrows().
            records_to_insert = data.to_dict('records')
            # --- END ULTIMATE FIX ---

            inserted_count_for_ticker = 0

            for record in records_to_insert:  # Iterate through the list of dictionaries
                # All values in 'record' are now direct Python scalars (datetime.date, float, int)
                record_date = record['Date']  # This is now guaranteed to be a native datetime.date object

                market_record = {
                    'symbol': ticker,
                    'date': record_date,  # Stored as datetime.date object
                    'open': record['Open'],
                    'high': record['High'],
                    'low': record['Low'],
                    'close': record['Close'],
                    'volume': record['Volume']
                }

                try:
                    result = mongo_collection.update_one(
                        {'symbol': ticker, 'date': record_date},
                        {'$set': market_record},
                        upsert=True
                    )

                    if result.upserted_id:
                        inserted_count_for_ticker += 1
                        total_inserted_count += 1
                    # No explicit print for existing/modified for brevity here

                except DuplicateKeyError:
                    # The record_date is now reliably a datetime.date object here too.
                    date_for_print = record_date.strftime('%Y-%m-%d')
                    print(f"  Duplicate record for {ticker} on {date_for_print}. Skipped by unique index.")
                except Exception as e:
                    # The record_date is now reliably a datetime.date object here too.
                    date_for_print = record_date.strftime('%Y-%m-%d')
                    print(f"  Error inserting/updating {ticker} on {date_for_print}: {e}")

            print(f"Successfully processed {inserted_count_for_ticker} new/updated records for {ticker}.")
            time.sleep(1)  # Be polite to Yahoo Finance API

        except Exception as e:
            # More descriptive error handling for fetching/processing
            if isinstance(e, requests.exceptions.ConnectionError) or \
                    (hasattr(e, 'args') and isinstance(e.args[0], ConnectionResetError)):
                print(f"Failed to fetch data for {ticker} due to a network connection error: {e}")
            else:
                print(f"Failed to fetch or process data for {ticker}: {e}")

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

        # Fetch data for the last 5 years from today (as a base, will update incrementally)
        today_date = datetime.now()  # Use a distinct variable name
        start_date_hist = (today_date - timedelta(days=5 * 365)).strftime('%Y-%m-%d')
        end_date_hist = today_date.strftime('%Y-%m-%d')

        total_market_data_records = fetch_historical_market_data(
            tickers=nifty_index_tickers,
            start_date_str=start_date_hist,
            end_date_str=end_date_hist,
            mongo_collection=mongo_market_data_collection
        )
        if total_market_data_records > 0:
            print(f"\nSuccessfully collected {total_market_data_records} new/updated market data records.")
        else:
            print("\nNo new market data records collected or an error occurred during market data collection.")

    else:
        print("\nFailed to connect to MongoDB for market data. Aborting market data collection.")

    print("\nMarket Data Collector test execution complete.")
    print("Check your MongoDB: indian_market_scanner_db -> historical_market_data.")
    print(
        "Example mongosh query: `use indian_market_scanner_db; db.historical_market_data.find({'symbol': '^NSEI'}).sort({'date': -1}).limit(5).pretty()`")