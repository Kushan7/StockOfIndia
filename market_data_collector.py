import pandas as pd
import yfinance as yf
import datetime
import logging

# Configure logging for better error tracking
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def fetch_historical_data(symbol, start_date=None, end_date=None):
    """
    Fetches historical market data for a given symbol, with robust error handling.
    """
    try:
        logging.info(f"Fetching data for {symbol}...")

        # Use auto_adjust=True to handle splits and dividends, and explicitly
        # provide start/end dates to prevent fetching unnecessary data.
        data = yf.download(symbol, start=start_date, end=end_date, auto_adjust=True)

        if data.empty:
            logging.warning(f"No data found for {symbol} in the specified date range. Check the symbol and dates.")
            return None

        # The error "The truth value of a Series is ambiguous" is likely coming from
        # a condition in your script that looks something like 'if data.Close > 0:'.
        # This revised script ensures that we have data before we proceed.

        # Perform any necessary calculations here
        # Example: Calculate moving averages
        data['SMA_20'] = data['Close'].rolling(window=20).mean()

        return data

    except Exception as e:
        logging.error(f"Failed to fetch or process data for {symbol}: {e}")
        return None


def get_market_data(symbols_list, last_fetch_date):
    """
    Main function to orchestrate the data fetching process.
    """
    all_market_data = []

    start_date = (last_fetch_date + datetime.timedelta(
        days=1)) if last_fetch_date else datetime.datetime.now() - datetime.timedelta(days=365 * 5)
    end_date = datetime.datetime.now().date()

    for symbol in symbols_list:
        data = fetch_historical_data(symbol, start_date=start_date, end_date=end_date)
        if data is not None:
            data['symbol'] = symbol
            all_market_data.append(data)

    return pd.concat(all_market_data) if all_market_data else pd.DataFrame()


if __name__ == '__main__':
    # This is a sample usage. Your database_manager.py would call this function.
    symbols = ['^NSEI', '^NSEBANK', '^CNXIT', '^CNXFMCG']
    last_date = datetime.date(2025, 8, 9)
    market_df = get_market_data(symbols, last_date)

    if not market_df.empty:
        logging.info(f"Successfully fetched {len(market_df)} records.")
    else:
        logging.warning("Failed to fetch any market data.")