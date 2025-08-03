# database_manager.py

# --- All Imports at the Top ---
import requests  # Still needed for Finnhub/Marketaux
import time
import re
# FIX: Consistent import of datetime and date classes
from datetime import datetime as dt_class, date as date_class, timedelta
import pymongo
from pymongo.errors import ConnectionFailure, DuplicateKeyError

import os
from dotenv import load_dotenv

from nlp_processor import process_and_update_sentiment, process_and_update_entities

# NEW: Import market data functions from market_data_collector.py
from market_data_collector import connect_to_market_data_mongodb, fetch_historical_market_data

# NEW: Import ET scraping function from et_news_scraper.py
from et_news_scraper import scrape_economic_times_headlines  # Only need to import the main function

# --- Load Environment Variables ---
load_dotenv()

# --- API Keys Configuration ---
FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY')
MARKETAUX_API_KEY = os.getenv('MARKETAUX_API_KEY')

# --- API Base URLs ---
FINNHUB_NEWS_BASE_URL = 'https://finnhub.io/api/v1/news'
MARKETAUX_NEWS_BASE_URL = 'https://api.marketaux.com/v1/news/all'


# --- Helper Functions for Caching/Smart Fetching (for news APIs) ---
# This remains in database_manager.py as it's general for news sources (ET, Finnhub, Marketaux)
def get_latest_news_date(mongo_collection, source_name):
    """
    Retrieves the latest publication_date for a given source from MongoDB.
    Returns datetime object or None if no data found.
    """
    if mongo_collection is None:
        return None

    latest_article = mongo_collection.find(
        {"source": source_name, "publication_date": {"$ne": None}}
    ).sort("publication_date", pymongo.DESCENDING).limit(1)

    try:
        latest = latest_article.next()
        # FIX: Use dt_class explicitly
        if isinstance(latest.get('publication_date'), dt_class):
            return latest['publication_date']
        elif isinstance(latest.get('publication_date'), str):
            try:
                # FIX: Use dt_class.strptime
                return dt_class.strptime(latest['publication_date'], '%Y-%m-%d')
            except ValueError:
                return None
        return None
    except StopIteration:
        return None


# --- MongoDB Insertion Function (Core DB utility) ---
def insert_article_into_mongodb(collection, article_data):
    """
    Inserts a single news article document into the MongoDB collection.
    Uses update_one with upsert=True to insert if not exists, or update if exists.
    """
    if collection is None:
        print("MongoDB collection not available. Skipping insertion.")
        return False

    # FIX: Use dt_class.strptime for parsing dates if they come as strings
    if 'date' in article_data and isinstance(article_data['date'], str):
        try:
            parsed_date = dt_class.strptime(article_data['date'], '%Y-%m-%d')
            article_data['publication_date'] = parsed_date
        except ValueError:
            print(f"Warning: Could not parse date '{article_data['date']}' into datetime object. Storing as string.")
            article_data['publication_date'] = article_data['date']
        article_data.pop('date', None)

    article_data.setdefault('sentiment_score', None)
    article_data.setdefault('companies_mentioned', [])
    article_data.setdefault('sectors_mentioned', [])

    try:
        result = collection.update_one(
            {'url': article_data['url']},
            {
                '$set': {
                    'title': article_data.get('title'),
                    'content': article_data.get('content'),
                    'publication_date': article_data.get('publication_date'),
                    'source': article_data.get('source'),
                    'sentiment_score': article_data.get('sentiment_score'),
                    'companies_mentioned': article_data.get('companies_mentioned'),
                    'sectors_mentioned': article_data.get('sectors_mentioned')
                }
            },
            upsert=True
        )

        if result.upserted_id:
            print(f"Inserted new article: {article_data['title'][:50]}... (ID: {result.upserted_id})")
            return True
        elif result.matched_count > 0 and result.modified_count == 0:
            print(f"Article already exists (URL: {article_data['url']}). No new insert or update needed.")
            return False
        elif result.matched_count > 0 and result.modified_count > 0:
            print(f"Existing article (URL: {article_data['url']}) updated.")
            return True
        else:
            print(f"MongoDB operation for URL {article_data['url']} neither inserted nor updated. Check logic.")
            return False

    except DuplicateKeyError:
        print(f"Duplicate key error for URL: {article_data['url']}. Article already exists.")
        return False
    except Exception as e:
        print(f"Error inserting/updating article {article_data.get('url', 'N/A')}: {e}")
        return False


# --- News API Fetching Functions ---
def fetch_news_from_finnhub(api_key, mongo_collection, num_articles_limit=15):
    """
    Fetches financial news from Finnhub API, filters for Indian context,
    and inserts them into MongoDB.
    """
    print("\n--- Data Collection via Finnhub API ---")
    if mongo_collection is None:
        print("MongoDB collection not available for Finnhub news. Aborting.")
        return []

    latest_finnhub_date_in_db = get_latest_news_date(mongo_collection, "Finnhub")
    # FIX: Use dt_class.strftime and dt_class.now()
    from_date_str = (latest_finnhub_date_in_db + timedelta(days=1)).strftime(
        '%Y-%m-%d') if latest_finnhub_date_in_db else None
    to_date_str = dt_class.now().strftime('%Y-%m-%d')

    if latest_finnhub_date_in_db:
        print(
            f"Latest Finnhub article in DB is from: {latest_finnhub_date_in_db.strftime('%Y-%m-%d')}. Fetching newer news.")
    else:
        print("No Finnhub articles found in DB. Fetching recent news.")

    # FIX: Use dt_class.strptime and dt_class.now()
    if from_date_str and dt_class.strptime(from_date_str, '%Y-%m-%d').date() > dt_class.now().date():
        print(f"Skipping Finnhub fetch: From date {from_date_str} is in the future.")
        return []

    categories = ['general']
    all_fetched_articles = []
    processed_count = 0

    indian_keywords = ['india', 'indian', 'nifty', 'sensex', 'rbi', 'nse', 'bse', 'mumbai', 'delhi', 'adani',
                       'reliance', 'tata', 'infosys', 'sbi', 'icici', 'hdfc']

    for category in categories:
        params = {
            'category': category,
            'token': api_key,
            # FIX: Use dt_class.now()
            'from': from_date_str if from_date_str else (dt_class.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
            'to': to_date_str
        }

        response = None
        try:
            print(f"Fetching {category} news from Finnhub (from {params['from']} to {params['to']})...")
            time.sleep(0.5)
            response = requests.get(FINNHUB_NEWS_BASE_URL, params=params, timeout=15)
            response.raise_for_status()
            news_items = response.json()
            print(f"Fetched {len(news_items)} news items from Finnhub for category '{category}'.")

            for item in news_items:
                title_lower = item.get('headline', '').lower()
                summary_lower = item.get('summary', '').lower()

                is_indian_context = False
                for keyword in indian_keywords:
                    if keyword in title_lower or keyword in summary_lower:
                        is_indian_context = True
                        break

                if not is_indian_context:
                    continue

                article_data = {
                    'title': item.get('headline'),
                    'content': item.get('summary'),
                    'url': item.get('url'),
                    # FIX: Use dt_class.fromtimestamp
                    'publication_date': dt_class.fromtimestamp(item.get('datetime', 0)).strftime('%Y-%m-%d'),
                    'source': "Finnhub",
                    'sentiment_score': None,
                    'companies_mentioned': [],
                    'sectors_mentioned': []
                }

                if not article_data['url'] or not article_data['title'] or not article_data['content']:
                    print(
                        f"Skipping article due to missing crucial data from Finnhub: {article_data.get('url', 'N/A')}")
                    continue

                inserted = insert_article_into_mongodb(mongo_collection, article_data)
                if inserted:
                    all_fetched_articles.append(article_data)
                    processed_count += 1

                if processed_count >= num_articles_limit:
                    print(f"Reached article limit ({num_articles_limit}) for Finnhub news, stopping.")
                    return all_fetched_articles

            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            error_response_content = response.text[:200] if response is not None and hasattr(response,
                                                                                             'text') else 'N/A'
            print(f"Error fetching news from Finnhub API for category '{category}': {e}")
            print(f"Response content: {error_response_content}")
        except Exception as e:
            print(f"An unexpected error occurred processing Finnhub news for category '{category}': {e}")

    print(f"Finnhub news collection complete. Inserted {processed_count} new/updated articles.")
    return all_fetched_articles


def fetch_news_from_marketaux(api_key, mongo_collection, num_articles_limit=15):
    """
    Fetches financial news from Marketaux API, filters for Indian context,
    and inserts them into MongoDB.
    Marketaux often has a 'countries' filter which is very useful.
    """
    print("\n--- Data Collection via Marketaux API ---")
    if mongo_collection is None:
        print("MongoDB collection not available for Marketaux news. Aborting.")
        return []

    latest_marketaux_date_in_db = get_latest_news_date(mongo_collection, "Marketaux")
    # FIX: Use dt_class and timedelta.isoformat()
    published_after_date_str = (
            latest_marketaux_date_in_db + timedelta(days=1)).isoformat() if latest_marketaux_date_in_db else None

    if latest_marketaux_date_in_db:
        print(
            f"Latest Marketaux article in DB is from: {latest_marketaux_date_in_db.strftime('%Y-%m-%d')}. Fetching newer news.")
    else:
        print("No Marketaux articles found in DB. Fetching recent news.")

    # FIX: Use dt_class.fromisoformat and dt_class.now()
    if published_after_date_str and dt_class.fromisoformat(
            published_after_date_str.replace('Z', '+00:00')).date() > dt_class.now().date():
        print(f"Skipping Marketaux fetch: Published after date {published_after_date_str} is in the future.")
        return []

    all_fetched_articles = []
    processed_count = 0

    params = {
        'api_token': api_key,
        'countries': 'in',
        'limit': 100,  # Max number of articles per request (adjust based on your plan)
        'sort': 'published_desc',
        'published_after': published_after_date_str  # Use 'published_after' for smart fetching
    }

    if not published_after_date_str:
        # FIX: Use dt_class.now()
        params['published_after'] = (dt_class.now() - timedelta(days=7)).isoformat()

    response = None
    try:
        print(
            f"Fetching news from Marketaux (published after {params['published_after'] if 'published_after' in params else 'start'})...")
        time.sleep(0.5)
        response = requests.get(MARKETAUX_NEWS_BASE_URL, params=params, timeout=15)
        response.raise_for_status()

        json_data = response.json()
        news_items = json_data.get('data', [])

        print(f"Fetched {len(news_items)} news items from Marketaux.")

        for item in news_items:
            published_date_str = None
            if item.get('published_at'):
                try:
                    # FIX: Use dt_class.fromisoformat
                    dt_object = dt_class.fromisoformat(item['published_at'].replace('Z', '+00:00'))
                    published_date_str = dt_object.strftime('%Y-%m-%d')
                except ValueError:
                    print(f"Warning: Could not parse Marketaux date '{item['published_at']}'. Storing as raw string.")
                    published_date_str = item['published_at']

            article_data = {
                'title': item.get('title'),
                'content': item.get('description'),
                'url': item.get('url'),
                'publication_date': published_date_str,
                'source': "Marketaux",
                'sentiment_score': None,
                'companies_mentioned': [],
                'sectors_mentioned': []
            }

            if not article_data['url'] or not article_data['title'] or not article_data['content']:
                print(f"Skipping article due to missing crucial data from Marketaux: {article_data.get('url', 'N/A')}")
                continue

            inserted = insert_article_into_mongodb(mongo_collection, article_data)
            if inserted:
                all_fetched_articles.append(article_data)
                processed_count += 1

            if processed_count >= num_articles_limit:
                print(f"Reached article limit ({num_articles_limit}) for Marketaux news, stopping.")
                return all_fetched_articles

        time.sleep(0.5)

    except requests.exceptions.RequestException as e:
        error_response_content = response.text[:200] if response is not None and hasattr(response, 'text') else 'N/A'
        print(f"Error fetching news from Marketaux API: {e}")
        print(f"Response content: {error_response_content}")
    except Exception as e:
        print(f"An unexpected error occurred processing Marketaux news: {e}")

    print(f"Marketaux news collection complete. Inserted {processed_count} new/updated articles.")
    return all_fetched_articles


# --- MongoDB Connection Functions ---
def connect_to_mongodb(host='localhost', port=27017, db_name='indian_market_scanner_db',
                       collection_name='news_articles'):
    """
    Establishes a connection to MongoDB and returns the news articles collection object.
    """
    try:
        client = pymongo.MongoClient(host, port, serverSelectionTimeoutMS=5000)
        client.admin.command('ismaster')
        print(f"Successfully connected to MongoDB for news articles at {host}:{port}")
        db = client[db_name]
        collection = db[collection_name]

        collection.create_index([("url", pymongo.ASCENDING)], unique=True)
        print(f"Ensured unique index on 'url' for collection '{collection_name}'")

        return collection
    except ConnectionFailure as e:
        print(f"Could not connect to MongoDB for news articles: {e}. Please ensure MongoDB server is running.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during MongoDB news articles connection: {e}")
        return None


def insert_article_into_mongodb(collection, article_data):
    """
    Inserts a single news article document into the MongoDB collection.
    Uses update_one with upsert=True to insert if not exists, or update if exists.
    """
    if collection is None:
        print("MongoDB collection not available. Skipping insertion.")
        return False

    # FIX: Use dt_class.strptime for parsing
    if 'date' in article_data and isinstance(article_data['date'], str):
        try:
            parsed_date = dt_class.strptime(article_data['date'], '%Y-%m-%d')
            article_data['publication_date'] = parsed_date
        except ValueError:
            print(f"Warning: Could not parse date '{article_data['date']}' into datetime object. Storing as string.")
            article_data['publication_date'] = article_data['date']
        article_data.pop('date', None)

    article_data.setdefault('sentiment_score', None)
    article_data.setdefault('companies_mentioned', [])
    article_data.setdefault('sectors_mentioned', [])

    try:
        result = collection.update_one(
            {'url': article_data['url']},
            {
                '$set': {
                    'title': article_data.get('title'),
                    'content': article_data.get('content'),
                    'publication_date': article_data.get('publication_date'),
                    'source': article_data.get('source'),
                    'sentiment_score': article_data.get('sentiment_score'),
                    'companies_mentioned': article_data.get('companies_mentioned'),
                    'sectors_mentioned': article_data.get('sectors_mentioned')
                }
            },
            upsert=True
        )

        if result.upserted_id:
            print(f"Inserted new article: {article_data['title'][:50]}... (ID: {result.upserted_id})")
            return True
        elif result.matched_count > 0 and result.modified_count == 0:
            print(f"Article already exists (URL: {article_data['url']}). No new insert or update needed.")
            return False
        elif result.matched_count > 0 and result.modified_count > 0:
            print(f"Existing article (URL: {article_data['url']}) updated.")
            return True
        else:
            print(f"MongoDB operation for URL {article_data['url']} neither inserted nor updated. Check logic.")
            return False

    except DuplicateKeyError:
        print(f"Duplicate key error for URL: {article_data['url']}. Article already exists.")
        return False
    except Exception as e:
        print(f"Error inserting/updating article {article_data.get('url', 'N/A')}: {e}")
        return False


# --- Main Execution Block ---
if __name__ == "__main__":
    print("Starting news and market data processing pipeline...")

    # --- API Key Checks ---
    if not FINNHUB_API_KEY:
        print("Error: FINNHUB_API_KEY not found in .env file or environment variables.")
        print("Please ensure you have FINNHUB_API_KEY=YOUR_KEY_HERE in your .env file.")
        exit(1)
    if not MARKETAUX_API_KEY:
        print("Error: MARKETAUX_API_KEY not found in .env file or environment variables.")
        print("Please ensure you have MARKETAUX_API_KEY=YOUR_KEY_HERE in your .env file.")
        exit(1)

    # --- MongoDB Connections ---
    mongo_news_collection = connect_to_mongodb(db_name='indian_market_scanner_db', collection_name='news_articles')
    mongo_market_data_collection = connect_to_market_data_mongodb(db_name='indian_market_scanner_db',
                                                                  collection_name='historical_market_data')

    if mongo_news_collection is not None and mongo_market_data_collection is not None:
        print("\nAll MongoDB connections established. Proceeding with data collection and processing.")

        # --- Phase 1 & 2: Data Collection ---

        # 1. Economic Times Web Scraping (As requested, kept active for specific news redirection)
        print("\n--- Phase 1 & 2: Data Collection via Economic Times Scraper ---")
        et_scraped_summary = scrape_economic_times_headlines(
            num_articles_limit=15,
            mongo_collection=mongo_news_collection
        )
        if et_scraped_summary:
            print(f"\nEconomic Times scraping complete. Processed {len(et_scraped_summary)} articles.")
        else:
            print("No new articles scraped from Economic Times or an error occurred.")

        # 2. Finnhub API News
        finnhub_news_summary = fetch_news_from_finnhub(
            api_key=FINNHUB_API_KEY,
            mongo_collection=mongo_news_collection,
            num_articles_limit=20
        )
        if finnhub_news_summary:
            print(f"\nFinnhub news collection complete. Processed {len(finnhub_news_summary)} articles.")
        else:
            print("No new articles fetched from Finnhub or an error occurred during collection.")

        # 3. Marketaux API News
        marketaux_news_summary = fetch_news_from_marketaux(
            api_key=MARKETAUX_API_KEY,
            mongo_collection=mongo_news_collection,
            num_articles_limit=20
        )
        if marketaux_news_summary:
            print(f"\nMarketaux news collection complete. Processed {len(marketaux_news_summary)} articles.")
        else:
            print("No new articles fetched from Marketaux or an error occurred during collection.")

        # --- Phase 3: NLP Processing (Sentiment & Entity Recognition) ---
        print("\n--- Phase 3a: Sentiment Analysis ---")
        process_and_update_sentiment(mongo_news_collection)

        print("\n--- Phase 3b: Entity and Sector Recognition ---")
        process_and_update_entities(mongo_news_collection)

        # --- Phase 4: Historical Market Data ---
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

        # Fetch data for the last 5 years from today
        today_date = dt_class.now()
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
            print("\nNo new market data records collected or an error occurred.")

    else:
        print("\nFailed to connect to MongoDB. All processing aborted.")

    print("\nProject execution complete. Check your MongoDB for:")
    print("  - News Articles (indian_market_scanner_db -> news_articles)")
    print("  - Historical Market Data (indian_market_scanner_db -> historical_market_data)")
    print(
        "\nExample mongosh query for news: `use indian_market_scanner_db; db.news_articles.find({'sentiment_score': {'$ne': null}}).limit(1).pretty()`")
    print(
        "Example mongosh query for market data: `use indian_market_scanner_db; db.historical_market_data.find({'symbol': '^NSEI'}).sort({'date': -1}).limit(5).pretty()`")