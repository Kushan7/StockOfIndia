import yfinance as yf
from datetime import datetime, timedelta
import pymongo
from pymongo.errors import InvalidDocument, BulkWriteError
import pandas as pd
import logging
import time

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# --- MongoDB Connection ---
def connect_to_mongodb(host='localhost', port=27017, db_name='indian_market_scanner_db',
                       collection_name='historical_market_data'):
    """
    Establishes a connection to MongoDB and returns the historical data collection.
    """
    try:
        client = pymongo.MongoClient(host, port, serverSelectionTimeoutMS=5000)
        client.admin.command('ismaster')
        logging.info(f"Successfully connected to MongoDB at {host}:{port}")
        db = client[db_name]
        collection = db[collection_name]

        collection.create_index([('symbol', pymongo.ASCENDING), ('date', pymongo.ASCENDING)], unique=True)
        logging.info("Ensured unique compound index on 'symbol' and 'date' for market data collection.")

        return collection
    except pymongo.errors.ConnectionFailure as e:
        logging.error(f"Failed to connect to MongoDB: {e}")
        return None


# --- Market Data Fetching ---
def fetch_historical_market_data(mongo_collection):
    """
    Fetches historical market data for key Indian sector indices and stores it in MongoDB.
    """
    logging.info("--- Phase 4: Fetching Historical Market Data ---")

    if mongo_collection is None:
        logging.error("MongoDB collection for market data is not available. Aborting.")
        return 0

    tickers = {
        '^NSEI': 'Nifty 50',
        '^NSEBANK': 'Nifty Bank',
        '^CNXIT': 'Nifty IT',
        '^CNXAUTO': 'Nifty Auto',
        '^CNXFMCG': 'Nifty FMCG',
        '^CNXMETAL': 'Nifty Metal',
        '^CNXMEDIA': 'Nifty Media',
        '^CNXREALTY': 'Nifty Realty'
    }

    inserted_count = 0

    for symbol in tickers:
        logging.info(f"Processing historical data for {symbol}...")

        latest_record = mongo_collection.find_one({'symbol': symbol}, sort=[('date', pymongo.DESCENDING)])

        if latest_record is None:
            start_date = (datetime.now() - timedelta(days=5 * 365)).strftime('%Y-%m-%d')
            logging.info(f"No data found for {symbol}. Fetching full 5-year history from {start_date}...")
        else:
            latest_date_in_db = latest_record['date']
            start_date = (latest_date_in_db + timedelta(days=1)).strftime('%Y-%m-%d')
            logging.info(
                f"Latest data for {symbol} in DB is {latest_date_in_db.strftime('%Y-%m-%d')}. Fetching from {start_date}...")

        end_date = datetime.now().strftime('%Y-%m-%d')

        if start_date >= end_date:
            logging.info(f"No new data to fetch for {symbol} in the specified date range ({start_date} to {end_date}).")
            continue

        try:
            data = yf.download(symbol, start=start_date, end=end_date, auto_adjust=True)

            # --- FIX: Check if DataFrame is empty using .empty attribute ---
            if data.empty:
                logging.warning(f"yfinance returned no data for {symbol}. Skipping to the next symbol.")
                continue

            records = []
            for date, row in data.iterrows():
                if pd.isna(row['Close']):
                    logging.warning(
                        f"Skipping record for {symbol} on {date.strftime('%Y-%m-%d')} due to missing closing price.")
                    continue

                record = {
                    'symbol': symbol,
                    'sector': tickers[symbol],
                    'date': date.to_pydatetime(),
                    'open': row['Open'],
                    'high': row['High'],
                    'low': row['Low'],
                    'close': row['Close'],
                    'volume': row['Volume']
                }
                records.append(record)

            if records:
                try:
                    mongo_collection.insert_many(records, ordered=False)
                    inserted_count += len(records)
                    logging.info(f"Successfully inserted {len(records)} new records for {symbol}.")
                except BulkWriteError as bwe:
                    errors = bwe.details['writeErrors']
                    duplicate_errors = [err for err in errors if err['code'] == 11000]
                    if duplicate_errors:
                        logging.warning(
                            f"Skipped {len(duplicate_errors)} duplicate records for {symbol}. {len(records) - len(duplicate_errors)} records were inserted.")
                        inserted_count += (len(records) - len(duplicate_errors))
                    else:
                        logging.error(f"Error inserting records for {symbol}: {bwe}")

            time.sleep(2)

        except Exception as e:
            logging.error(f"Failed to fetch or process data for {symbol}: {e}")

    logging.info(f"Historical market data collection complete. Total new/updated records: {inserted_count}.")
    return inserted_count


if __name__ == '__main__':
    logging.info("--- Running Market Data Collector Separately for Testing ---")
    mongo_market_data_collection_test = connect_to_mongodb()
    if mongo_market_data_collection_test:
        fetch_historical_market_data(mongo_market_data_collection_test)