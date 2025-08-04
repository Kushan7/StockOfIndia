# insights_generator.py

import pandas as pd
from datetime import datetime, timedelta
import pymongo
import numpy as np

from pymongo.errors import InvalidDocument


# We'll need a MongoDB connection function to run this independently,
# but when called from database_manager.py, we'll pass the collection objects.
def connect_to_insights_mongodb(host='localhost', port=27017, db_name='indian_market_scanner_db',
                                collection_name='insights'):
    """
    Establishes a connection to MongoDB and returns the insights collection.
    """
    try:
        client = pymongo.MongoClient(host, port, serverSelectionTimeoutMS=5000)
        client.admin.command('ismaster')
        print(f"Successfully connected to MongoDB for insights at {host}:{port}")
        db = client[db_name]
        collection = db[collection_name]
        return collection
    except pymongo.errors.ConnectionFailure as e:
        print(f"Failed to connect to MongoDB for insights: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during MongoDB connection for insights: {e}")
        return None


def generate_and_store_insights(news_collection, market_data_collection, insights_collection):
    """
    Generates and stores market insights based on aggregated news sentiment
    and historical market data.
    """
    print("\n--- Phase 5: Generating Correlation and Insights ---")

    if news_collection is None or market_data_collection is None or insights_collection is None:
        print("Required MongoDB collections are not available. Aborting insight generation.")
        return 0

    # 1. Fetch data from MongoDB
    print("Fetching news articles and market data from MongoDB...")
    news_data = list(news_collection.find({'sentiment_score': {'$ne': None}}))
    market_data = list(market_data_collection.find())

    news_df = pd.DataFrame(news_data)
    market_df = pd.DataFrame(market_data)

    if news_df.empty or market_df.empty:
        print("Not enough data to generate insights. Please run the data collectors first.")
        return 0

    # Clean up dataframes
    news_df['publication_date'] = pd.to_datetime(news_df['publication_date']).dt.date
    market_df['date'] = pd.to_datetime(market_df['date'])

    # 2. Aggregate sentiment by sector and date
    print("Aggregating sentiment by sector and date...")
    sentiment_by_sector = news_df.explode('sectors_mentioned').groupby(
        ['publication_date', 'sectors_mentioned']
    ).agg(
        avg_sentiment=('sentiment_score', 'mean'),
        num_articles=('sentiment_score', 'count')
    ).reset_index()

    # Rename columns for clarity
    sentiment_by_sector.rename(columns={'publication_date': 'date', 'sectors_mentioned': 'sector'}, inplace=True)
    sentiment_by_sector['date'] = pd.to_datetime(sentiment_by_sector['date']).dt.date

    # 3. Join sentiment with market data
    print("Joining sentiment data with market data...")
    sector_to_ticker_mapping = {
        'Banking & Financial Services': '^NSEBANK',
        'Information Technology': '^CNXIT',
        'Automobile': '^CNXAUTO',
        'Healthcare & Pharma': '^CNXPHARM',
        'FMCG': '^CNXFMCG',
        'Metals & Mining': '^CNXMETAL',
        'Media & Entertainment': '^CNXMEDIA',
        'Real Estate': '^CNXREALTY'
    }

    ticker_to_sector_mapping = {v: k for k, v in sector_to_ticker_mapping.items()}

    market_df['sector'] = market_df['symbol'].map(ticker_to_sector_mapping)

    insights_df = pd.merge(
        sentiment_by_sector,
        market_df,
        on=['date', 'sector'],
        how='inner'
    )

    if insights_df.empty:
        print("No matching news and market data found for the same date/sector. Aborting.")
        return 0

    # 4. Calculate market trends (SMAs)
    print("Calculating market trends and signals...")
    insights_df.sort_values(['sector', 'date'], inplace=True)
    insights_df['sma_20'] = insights_df.groupby('sector')['Close'].transform(lambda x: x.rolling(window=20).mean())
    insights_df['sma_50'] = insights_df.groupby('sector')['Close'].transform(lambda x: x.rolling(window=50).mean())

    # 5. Generate signals
    def generate_signal(row):
        if row['avg_sentiment'] > 0.65 and row['Close'] > row['sma_20'] and row['sma_20'] > row['sma_50']:
            return 'Buy'
        if row['avg_sentiment'] < 0.35 and row['Close'] < row['sma_20'] and row['sma_20'] < row['sma_50']:
            return 'Sell'
        return 'Neutral'

    insights_df['signal'] = insights_df.apply(generate_signal, axis=1)

    # 6. Store final insights in a new collection
    print("Storing final insights in MongoDB...")

    insights_collection.delete_many({})  # Drop existing insights for a clean update

    records_to_insert = insights_df.to_dict('records')
    inserted_count = 0
    if records_to_insert:
        try:
            insights_collection.insert_many(records_to_insert)
            inserted_count = len(records_to_insert)
            print(f"Successfully inserted {inserted_count} insight records.")
        except InvalidDocument as e:
            print(f"Error inserting insights into DB: InvalidDocument: {e}. Check data types.")
        except Exception as e:
            print(f"Error inserting insights into DB: {e}")

    return inserted_count


# --- Test execution block (run independently) ---
if __name__ == '__main__':
    print("--- Running Insights Generator Separately for Testing ---")
    print("NOTE: This requires 'news_articles' and 'historical_market_data' collections to be pre-populated.")


    def mock_connect_to_db(db_name, collection_name):
        try:
            client = pymongo.MongoClient("mongodb://localhost:27017/")
            db = client[db_name]
            return db[collection_name]
        except Exception as e:
            print(f"Mock DB connection failed: {e}")
            return None


    mongo_news_collection_test = mock_connect_to_db("indian_market_scanner_db", "news_articles")
    mongo_market_data_collection_test = mock_connect_to_db("indian_market_scanner_db", "historical_market_data")
    mongo_insights_collection_test = mock_connect_to_db("indian_market_scanner_db", "insights")

    # FIX: Check against None instead of bool()
    if mongo_news_collection_test is not None and mongo_market_data_collection_test is not None and mongo_insights_collection_test is not None:
        total_insights_generated = generate_and_store_insights(
            mongo_news_collection_test,
            mongo_market_data_collection_test,
            mongo_insights_collection_test
        )
        print(f"Total insights generated and stored: {total_insights_generated}")
    else:
        print("Test collections not available. Aborting.")