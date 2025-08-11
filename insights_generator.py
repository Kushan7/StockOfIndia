import pandas as pd
from pymongo import MongoClient
import numpy as np
from datetime import datetime, timedelta
import logging

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# --- MongoDB Connection ---
def connect_to_mongodb():
    """Establishes and returns a connection to MongoDB."""
    try:
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
        client.admin.command('ismaster')
        return client
    except Exception as e:
        logging.error(f"Failed to connect to MongoDB: {e}")
        return None


# --- Main Insights Generator ---
def generate_and_store_insights(news_collection, market_collection, insights_collection):
    """
    Generates market insights by combining news sentiment and historical data.
    """
    logging.info("--- Phase 5: Generating Correlation and Insights ---")

    if news_collection is None or market_collection is None or insights_collection is None:
        logging.error("MongoDB collections are not available. Aborting insights generation.")
        return

    # Fetch news and market data
    try:
        logging.info("Fetching news articles and market data from MongoDB...")
        news_df = pd.DataFrame(list(news_collection.find({})))
        market_df = pd.DataFrame(list(market_collection.find({})))

        # CRITICAL FIX: Only drop '_id' if the DataFrame is not empty
        if not news_df.empty:
            news_df = news_df.drop(columns=['_id'], errors='ignore')
        if not market_df.empty:
            market_df = market_df.drop(columns=['_id'], errors='ignore')

        # Ensure date columns are in datetime format and timezone-naive
        if not news_df.empty and 'publishedAt' in news_df.columns:
            news_df['publishedAt'] = pd.to_datetime(news_df['publishedAt']).dt.tz_localize(None)
        if not market_df.empty and 'date' in market_df.columns:
            market_df['date'] = pd.to_datetime(market_df['date']).dt.tz_localize(None)


    except Exception as e:
        logging.error(f"An unexpected error occurred during insights generation: {e}")
        return

    if news_df.empty or market_df.empty:
        logging.warning("News or market data is empty. Cannot generate insights.")
        return

    # --- Data Processing and Insight Generation ---

    # Aggregate sentiment by symbol and date
    if 'symbol' in news_df.columns and 'sentiment_score' in news_df.columns and 'publishedAt' in news_df.columns:
        news_df['date'] = news_df['publishedAt'].dt.floor('d')
        daily_sentiment = news_df.groupby(['symbol', 'date']).agg(
            avg_sentiment=('sentiment_score', 'mean'),
            num_articles=('sentiment_score', 'count')
        ).reset_index()
        logging.info("Aggregated sentiment by sector and date.")
    else:
        logging.warning("Skipping sentiment aggregation due to missing columns in news data.")
        daily_sentiment = pd.DataFrame(columns=['symbol', 'date', 'avg_sentiment', 'num_articles'])

    # Create a full time series for all symbols in the market data
    if 'symbol' in market_df.columns and 'date' in market_df.columns:
        all_dates = pd.to_datetime(market_df['date'].unique())
        all_symbols = market_df['symbol'].unique()
        full_index = pd.MultiIndex.from_product([all_symbols, all_dates], names=['symbol', 'date'])
        full_df = pd.DataFrame(index=full_index).reset_index()

        # Join market data with the full time series
        insights_df = pd.merge(full_df, market_df, on=['symbol', 'date'], how='left')
        logging.info("Creating a full time series and calculating trends...")
    else:
        logging.warning("Skipping time series creation due to missing columns in market data.")
        insights_df = pd.DataFrame(columns=['symbol', 'date'])

    # Calculate beta for each sector/symbol
    if 'close' in insights_df.columns:
        insights_df['close'].fillna(method='ffill', inplace=True)
        insights_df['daily_return'] = insights_df.groupby('symbol')['close'].pct_change()
        market_return = insights_df[insights_df['symbol'] == '^NSEI']['daily_return'].rename('market_return')
        insights_df = insights_df.merge(market_return, on='date', how='left')

        def calculate_beta(group):
            if len(group) > 20 and group['market_return'].var() > 0:
                covariance = group['daily_return'].cov(group['market_return'])
                market_variance = group['market_return'].var()
                return covariance / market_variance
            return np.nan

        insights_df['beta'] = insights_df.groupby('symbol').apply(calculate_beta).reset_index(level=0, drop=True)
        logging.info("Calculating Beta for each sector.")

        insights_df['sma_20'] = insights_df.groupby('symbol')['close'].rolling(window=20).mean().reset_index(level=0,
                                                                                                             drop=True)
        insights_df['price_to_sma_20_ratio'] = insights_df['close'] / insights_df['sma_20']

    # Join sentiment data with the full market data
    if not daily_sentiment.empty:
        insights_df = pd.merge(insights_df, daily_sentiment, on=['symbol', 'date'], how='left')
        logging.info("Joining sentiment data with full market data.")
    else:
        insights_df['avg_sentiment'] = np.nan
        insights_df['num_articles'] = np.nan

    if 'avg_sentiment' in insights_df.columns:
        insights_df['avg_sentiment'] = insights_df['avg_sentiment'].fillna(0.5)
    if 'num_articles' in insights_df.columns:
        insights_df['num_articles'] = insights_df['num_articles'].fillna(0)

    # Store final insights in MongoDB
    if not insights_df.empty:
        insights_records = insights_df.to_dict('records')
        try:
            insights_collection.insert_many(insights_records, ordered=False)
            logging.info(f"Successfully inserted {len(insights_records)} insight records.")
        except Exception as e:
            logging.error(f"Error storing final insights in MongoDB: {e}")
    else:
        logging.warning("No insights generated to store in MongoDB.")


if __name__ == '__main__':
    logging.info("--- Running Insights Generator Separately for Testing ---")
    client = connect_to_mongodb()
    if client:
        db = client['indian_market_scanner_db']
        generate_and_store_insights(db['news_articles'], db['historical_market_data'], db['insights'])
