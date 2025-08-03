# database_manager.py

# --- All Imports at the Top ---
import requests
from bs4 import BeautifulSoup
import time
import re
from datetime import datetime, timedelta
import pymongo
from pymongo.errors import ConnectionFailure, DuplicateKeyError
import yfinance as yf

import os
from dotenv import load_dotenv

from nlp_processor import process_and_update_sentiment, process_and_update_entities

# --- Load Environment Variables ---
load_dotenv()

# --- API Keys Configuration ---
FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY')
MARKETAUX_API_KEY = os.getenv('MARKETAUX_API_KEY')

# --- API Base URLs ---
FINNHUB_NEWS_BASE_URL = 'https://finnhub.io/api/v1/news'
MARKETAUX_NEWS_BASE_URL = 'https://api.marketaux.com/v1/news/all'


# --- Helper Functions for Caching/Smart Fetching ---

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
        if isinstance(latest.get('publication_date'), datetime):
            return latest['publication_date']
        elif isinstance(latest.get('publication_date'), str):
            try:
                return datetime.strptime(latest['publication_date'], '%Y-%m-%d')
            except ValueError:
                return None
        return None
    except StopIteration:
        return None


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


# --- Web Scraping Functions (Economic Times) ---
def get_html_content(url, retries=3, delay=2):
    """
    Fetches the HTML content of a given URL with retries and delays.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    for i in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.content
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            if i < retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"Failed to fetch {url} after {retries} attempts.")
                return None
    return None


def parse_article_page(article_url):
    """
    Parses a single Economic Times article page to extract title, date, and content.
    """
    print(f"Scraping article: {article_url}")
    html_content = get_html_content(article_url)
    if not html_content:
        return None

    soup = BeautifulSoup(html_content, 'lxml')

    title = ""
    date = ""

    # --- Extract Title ---
    title_element = soup.find('h1', {'class': 'artTitle'})
    if not title_element:
        title_element = soup.find('h1', {'class': 'article_title'})
    if not title_element:
        title_element = soup.find('h1')

    if title_element:
        title = title_element.get_text(strip=True)

    # --- Extract Date ---
    date_element = soup.find('time', {'class': 'publishedAt'})
    if not date_element:
        date_element = soup.find('div', {'class': 'publish_on'})
    if not date_element:
        date_element = soup.find('span', {'class': 'byline_data'})

    if date_element:
        date = date_element.get_text(strip=True)
        match = re.search(r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}', date)
        if match:
            date = match.group(0)
        else:
            match = re.search(r'\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}', date)
            if match:
                date = match.group(0)

    # --- Extract Article Content (REFINED STRATEGY - Broadened Search) ---
    full_content_parts = []

    article_data_container = soup.find('div', class_=lambda x: x and 'artdata' in x.split())

    if not article_data_container:
        article_data_container = soup.find('div', id='pagecontent')
        if article_data_container:
            article_data_container = article_data_container.find('div', class_='pagecontent_fit')

    if article_data_container:
        text_containing_elements = article_data_container.find_all(['div', 'p'])

        for element in text_containing_elements:
            classes = element.get('class', [])
            if 'arttextmedium' in classes or 'arttext' in classes or element.name == 'p':
                text_chunk = element.get_text(separator=' ', strip=True)

                if len(text_chunk) > 50 and \
                        not text_chunk.lower().startswith("read more:") and \
                        not text_chunk.lower().startswith("also read:") and \
                        not text_chunk.lower().startswith("download the economic times app") and \
                        not text_chunk.lower().startswith("by downloading the app") and \
                        not text_chunk.lower().startswith("follow us on") and \
                        not text_chunk.lower().startswith("join us on") and \
                        not text_chunk.lower().startswith("view more") and \
                        not text_chunk.lower().startswith("watch now") and \
                        not text_chunk.lower().startswith("trending now"):
                    full_content_parts.append(text_chunk)

    full_content_str = "\n".join(full_content_parts)

    if not title and not full_content_str:
        print(f"Warning: Could not extract significant content from {article_url}. Title and content are empty.")
        return None

    return {
        'title': title,
        'date': date,
        'content': full_content_str,
        'url': article_url,
        'source': 'Economic Times'
    }


def scrape_economic_times_headlines(num_articles_limit=10, mongo_collection=None):
    """
    Scrapes headlines and article URLs from Economic Times listing pages
    and optionally inserts them into the provided MongoDB collection.
    It will try to only process articles newer than the latest found in DB.
    """
    all_articles_data = []
    seen_urls = set()

    # Get the latest date for ET from DB for smart fetching
    latest_et_date_in_db = get_latest_news_date(mongo_collection, "Economic Times")
    if latest_et_date_in_db:
        print(
            f"Latest Economic Times article in DB is from: {latest_et_date_in_db.strftime('%Y-%m-%d')}. Fetching newer news.")
    else:
        print("No Economic Times articles found in DB. Fetching recent news.")

    urls_to_scrape = [
        'https://economictimes.indiatimes.com/news/latest-news',
        'https://economictimes.indiatimes.com/markets/stocks/news',
        # Removed 'https://economictimes.indiatimes.com/markets/et-markets/real-time-news' due to 404
        # Add more specific ET URLs here if needed
    ]

    for page_url in urls_to_scrape:
        print(f"Fetching news from listing page: {page_url}")
        html_content = get_html_content(page_url)
        if not html_content:
            continue

        soup = BeautifulSoup(html_content, 'lxml')

        news_list_container = soup.find('ul', class_='data')

        if not news_list_container:
            print(f"Could not find news list container on {page_url}. Please re-check HTML structure.")
            continue

        news_items = news_list_container.find_all('li', itemprop='itemListElement')

        if not news_items:
            print(f"No news items found within the container on {page_url}. Please re-check LI structure.")
            continue

        for item in news_items:
            link_tag = item.find('a', href=True)
            timestamp_tag = item.find('span', class_='timestamp', attrs={'data-time': True})

            if link_tag and timestamp_tag:
                title = link_tag.get_text(strip=True)
                article_url = link_tag['href']

                date_str = timestamp_tag['data-time']
                formatted_date_str = None
                article_date_obj = None  # Store as datetime for comparison

                try:
                    parsed_date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    formatted_date_str = parsed_date_obj.strftime('%Y-%m-%d')
                    article_date_obj = parsed_date_obj  # Use this for comparison
                except ValueError:
                    display_date_text = timestamp_tag.get_text(strip=True)
                    match = re.search(r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}',
                                      display_date_text)
                    if match:
                        formatted_date_str = match.group(0)
                        try:  # Attempt to parse the matched string into datetime for comparison
                            article_date_obj = datetime.strptime(formatted_date_str, '%b %d, %Y')
                        except ValueError:
                            pass
                    else:
                        match = re.search(r'\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}',
                                          display_date_text)
                        if match:
                            formatted_date_str = match.group(0)
                            try:  # Attempt to parse for comparison
                                article_date_obj = datetime.strptime(formatted_date_str, '%d %b %Y')
                            except ValueError:
                                pass
                        else:
                            formatted_date_str = display_date_text

                # Smart Fetching for ET: Skip if article is older than latest in DB
                # Only compare if we successfully parsed the article_date_obj and have a latest_et_date_in_db
                if latest_et_date_in_db and article_date_obj and article_date_obj.date() <= latest_et_date_in_db.date():
                    print(
                        f"Skipping {article_url} (older than latest in DB: {latest_et_date_in_db.strftime('%Y-%m-%d')}).")
                    continue

                if not article_url.startswith('http'):
                    article_url = f"https://economictimes.indiatimes.com{article_url}"

                if "/articleshow/" in article_url and "economictimes.indiatimes.com" in article_url and article_url not in seen_urls:
                    print(f"Attempting to process article: {article_url}")

                    article_details = parse_article_page(article_url)

                    if article_details:
                        article_details['title'] = title
                        article_details['date'] = formatted_date_str
                        article_details['source'] = "Economic Times"  # Explicitly set source

                        if mongo_collection is not None:
                            inserted_successfully = insert_article_into_mongodb(mongo_collection, article_details)
                            if inserted_successfully:
                                all_articles_data.append(article_details)
                        else:
                            all_articles_data.append(article_details)

                        seen_urls.add(article_url)
                        time.sleep(1.5)
                    else:
                        basic_article_data = {
                            'title': title,
                            'date': formatted_date_str,
                            'content': 'Failed to scrape full content from article page',
                            'url': article_url,
                            'source': 'Economic Times',  # Explicitly set source
                            'sentiment_score': None,
                            'companies_mentioned': [],
                            'sectors_mentioned': []
                        }
                        if mongo_collection is not None:
                            insert_article_into_mongodb(mongo_collection, basic_article_data)

                        all_articles_data.append(basic_article_data)
                        seen_urls.add(article_url)

                if len(all_articles_data) >= num_articles_limit:
                    print(f"Reached article limit ({num_articles_limit}) for testing, stopping.")
                    break

        if len(all_articles_data) >= num_articles_limit:
            break

    return all_articles_data


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
    # For Finnhub, 'from' parameter expects YYYY-MM-DD. Use latest_date_in_db + 1 day
    from_date_str = (latest_finnhub_date_in_db + timedelta(days=1)).strftime(
        '%Y-%m-%d') if latest_finnhub_date_in_db else None
    to_date_str = datetime.now().strftime('%Y-%m-%d')  # Fetch up to today

    if latest_finnhub_date_in_db:
        print(
            f"Latest Finnhub article in DB is from: {latest_finnhub_date_in_db.strftime('%Y-%m-%d')}. Fetching newer news.")
    else:
        print("No Finnhub articles found in DB. Fetching recent news.")

    # Ensure from_date_str is not in the future for Finnhub API call
    if from_date_str and datetime.strptime(from_date_str, '%Y-%m-%d').date() > datetime.now().date():
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
            # Use 'from' and 'to' for smart fetching
            'from': from_date_str if from_date_str else (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
            # Default to last 7 days if no existing data
            'to': to_date_str
        }

        response = None  # Initialize response outside try block
        try:
            print(f"Fetching {category} news from Finnhub (from {params['from']} to {params['to']})...")
            time.sleep(0.5)  # Small delay before request
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
                    'publication_date': datetime.fromtimestamp(item.get('datetime', 0)).strftime('%Y-%m-%d'),
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
            # Safely access response.text here
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
    # Marketaux 'published_after' expects ISO format string
    published_after_date_str = (
                latest_marketaux_date_in_db + timedelta(days=1)).isoformat() if latest_marketaux_date_in_db else None

    if latest_marketaux_date_in_db:
        print(
            f"Latest Marketaux article in DB is from: {latest_marketaux_date_in_db.strftime('%Y-%m-%d')}. Fetching newer news.")
    else:
        print("No Marketaux articles found in DB. Fetching recent news.")

    # Ensure published_after_date_str is not in the future
    if published_after_date_str and datetime.fromisoformat(
            published_after_date_str.replace('Z', '+00:00')).date() > datetime.now().date():
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

    # If no articles found, default to fetching last 7 days to populate initially
    if not published_after_date_str:
        params['published_after'] = (datetime.now() - timedelta(days=7)).isoformat()

    response = None  # Initialize response outside try block
    try:
        print(
            f"Fetching news from Marketaux (published after {params['published_after'] if 'published_after' in params else 'start'})...")
        time.sleep(0.5)  # Small delay before request
        response = requests.get(MARKETAUX_NEWS_BASE_URL, params=params, timeout=15)
        response.raise_for_status()

        json_data = response.json()
        news_items = json_data.get('data', [])

        print(f"Fetched {len(news_items)} news items from Marketaux.")

        for item in news_items:
            published_date_str = None
            if item.get('published_at'):
                try:
                    dt_object = datetime.fromisoformat(item['published_at'].replace('Z', '+00:00'))
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


def insert_article_into_mongodb(collection, article_data):
    """
    Inserts a single news article document into the MongoDB collection.
    Uses update_one with upsert=True to insert if not exists, or do nothing if exists (due to unique index).
    """
    if collection is None:
        print("MongoDB collection not available. Skipping insertion.")
        return False

    # Ensure publication_date is a datetime object for better MongoDB querying
    if 'date' in article_data and isinstance(article_data['date'], str):
        try:
            parsed_date = datetime.strptime(article_data['date'], '%Y-%m-%d')
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

            data.reset_index(inplace=True)

            # --- FIX: Convert Pandas Timestamps to native Python datetimes after reset_index ---
            data['Date'] = data['Date'].apply(lambda x: x.to_pydatetime())
            # --- END FIX ---

            # In your database_manager.py file, locate the fetch_historical_market_data function

            # ... (code before the loop: data.reset_index(inplace=True), data['Date'] = ... ) ...

            inserted_count_for_ticker = 0

            for index, row in data.iterrows():
                # FIX: Explicitly convert to a datetime.date object
                # This ensures it's a native Python date object, not a Pandas Series or Timestamp with complexities
                record_date = row['Date'].date()

                market_record = {
                    'symbol': ticker,
                    'date': record_date,  # Stored as datetime.date object
                    'open': row['Open'].item(),
                    'high': row['High'].item(),
                    'low': row['Low'].item(),
                    'close': row['Close'].item(),
                    'volume': row['Volume'].item()
                }

                try:
                    # The 'date' field in the query and $set now uses the native datetime.date object
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
                    date_for_print = record_date.strftime('%Y-%m-%d') if isinstance(record_date, datetime) else str(
                        record_date)
                    print(f"  Duplicate record for {ticker} on {date_for_print}. Skipped by unique index.")
                except Exception as e:
                    date_for_print = record_date.strftime('%Y-%m-%d') if isinstance(record_date, datetime) else str(
                        record_date)
                    print(f"  Error inserting/updating {ticker} on {date_for_print}: {e}")

            print(f"Successfully processed {inserted_count_for_ticker} new/updated records for {ticker}.")
            time.sleep(1)

        # ... (rest of the fetch_historical_market_data function and the file) ...
        except Exception as e:
            print(f"Failed to fetch or process data for {ticker}: {e}")

    print(f"\nHistorical market data collection complete. Total new/updated records: {total_inserted_count}.")
    return total_inserted_count


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
            num_articles_limit=15,  # Adjust limit as desired
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

        # Fetch data for the last 5 years from today (as a base, will update incrementally)
        today_date = datetime.now()  # Use a distinct variable name
        start_date_hist = (today_date - timedelta(days=5 * 365)).strftime('%Y-%m-%d')  # Base start date for new symbols
        end_date_hist = today_date.strftime('%Y-%m-%d')  # Always fetch up to today

        total_market_data_records = fetch_historical_market_data(
            tickers=nifty_index_tickers,
            start_date_str=start_date_hist,  # Pass as string
            end_date_str=end_date_hist,  # Pass as string
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