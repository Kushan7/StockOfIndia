# All imports for database_manager.py
import requests
from bs4 import BeautifulSoup
import time
import re
from datetime import datetime
import pymongo
from pymongo.errors import ConnectionFailure, DuplicateKeyError

# NEW: Import NLP processing functions from the new file
from nlp_processor import process_and_update_sentiment, process_and_update_entities


# --- Web Scraping Functions ---
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


# In your database_manager.py file

# ... (rest of parse_article_page function before content extraction, including imports) ...

# In your database_manager.py file

# ... (keep all imports at the top: requests, BeautifulSoup, time, re, datetime, pymongo, etc.) ...

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

    # Look for a common parent container that holds all variations of article text divs
    # 'artdata' class seems to be a good candidate as seen in previous inspections/paths
    article_data_container = soup.find('div', class_=lambda x: x and 'artdata' in x.split())

    # Fallback to pagecontent which seems to hold the entire main content area
    if not article_data_container:
        article_data_container = soup.find('div', id='pagecontent')
        if article_data_container:  # If pagecontent exists, look for its direct content wrapper
            article_data_container = article_data_container.find('div', class_='pagecontent_fit')

    if article_data_container:
        # Find all divs and p tags within this broader container that might hold content.
        # This will capture both 'arttextmedium', 'arttext', and any direct paragraphs.
        text_containing_elements = article_data_container.find_all(['div', 'p'])

        for element in text_containing_elements:
            # Check for specific classes that typically indicate main content blocks
            classes = element.get('class', [])
            if 'arttextmedium' in classes or 'arttext' in classes or element.name == 'p':
                # Get all text within this specific element, separated by spaces for readability
                text_chunk = element.get_text(separator=' ', strip=True)

                # Apply robust filtering to clean out non-content text
                if len(text_chunk) > 50 and \
                        not text_chunk.lower().startswith("read more:") and \
                        not text_chunk.lower().startswith("also read:") and \
                        not text_chunk.lower().startswith("download the economic times app") and \
                        not text_chunk.lower().startswith("by downloading the app") and \
                        not text_chunk.lower().startswith("follow us on") and \
                        not text_chunk.lower().startswith("join us on") and \
                        not text_chunk.lower().startswith("view more") and \
                        not text_chunk.lower().startswith("watch now") and \
                        not text_chunk.lower().startswith("trending now"):  # Added more common junk filters

                    full_content_parts.append(text_chunk)

    # Join the collected parts into a single string
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

# --- MongoDB Integration Functions ---

def connect_to_mongodb(host='localhost', port=27017, db_name='indian_market_scanner_db',
                       collection_name='news_articles'):
    """
    Establishes a connection to MongoDB and returns the collection object.
    """
    try:
        client = pymongo.MongoClient(host, port, serverSelectionTimeoutMS=5000)
        client.admin.command('ismaster')
        print(f"Successfully connected to MongoDB at {host}:{port}")
        db = client[db_name]
        collection = db[collection_name]

        collection.create_index([("url", pymongo.ASCENDING)], unique=True)
        print(f"Ensured unique index on 'url' for collection '{collection_name}'")

        return collection
    except ConnectionFailure as e:
        print(f"Could not connect to MongoDB: {e}. Please ensure MongoDB server is running.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during MongoDB connection: {e}")
        return None


def insert_article_into_mongodb(collection, article_data):
    """
    Inserts a single news article document into the MongoDB collection.
    Uses update_one with upsert=True to insert if not exists, or do nothing if exists (due to unique index).
    """
    if collection is None:
        print("MongoDB collection not available. Skipping insertion.")
        return False

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
    article_data.setdefault('sectors_mentioned', [])  # Ensure this is also defaulted

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
                    'sectors_mentioned': article_data.get('sectors_mentioned')  # Also ensure this is set
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


# --- Scraper Function (Modified to use insert_article_into_mongodb with full defaults) ---

def scrape_economic_times_headlines(base_url, num_articles_limit=10, mongo_collection=None):
    """
    Scrapes headlines and article URLs from Economic Times listing pages
    and optionally inserts them into the provided MongoDB collection.
    """
    all_articles_data = []
    seen_urls = set()

    urls_to_scrape = [
        'https://economictimes.indiatimes.com/news/latest-news',
        'https://economictimes.indiatimes.com/markets/stocks/news',
        'https://economictimes.indiatimes.com/markets/et-markets/real-time-news'
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
                formatted_date = None
                try:
                    parsed_date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    formatted_date = parsed_date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    display_date_text = timestamp_tag.get_text(strip=True)
                    match = re.search(r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}',
                                      display_date_text)
                    if match:
                        formatted_date = match.group(0)
                    else:
                        match = re.search(r'\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}',
                                          display_date_text)
                        if match:
                            formatted_date = match.group(0)
                        else:
                            formatted_date = display_date_text

                if not article_url.startswith('http'):
                    article_url = f"https://economictimes.indiatimes.com{article_url}"

                if "/articleshow/" in article_url and "economictimes.indiatimes.com" in article_url and article_url not in seen_urls:
                    print(f"Attempting to process article: {article_url}")

                    article_details = parse_article_page(article_url)

                    if article_details:
                        article_details['title'] = title
                        article_details['date'] = formatted_date

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
                            'date': formatted_date,
                            'content': 'Failed to scrape full content from article page',
                            'url': article_url,
                            'source': 'Economic Times',
                            'sentiment_score': None,  # Default for NLP later
                            'companies_mentioned': [],  # Default for NER later
                            'sectors_mentioned': []  # Default for NER later
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


# NEW: Import NLP processing functions from nlp_processor.py
from nlp_processor import process_and_update_sentiment, process_and_update_entities

# database_manager.py

# All imports for database_manager.py
import requests
# BeautifulSoup is likely only needed if you still have parse_article_page for old data.
# If you fully switch to APIs, you can remove it.
from bs4 import BeautifulSoup
import time
import re
from datetime import datetime
import pymongo
from pymongo.errors import ConnectionFailure, DuplicateKeyError

import os
from dotenv import load_dotenv

load_dotenv()

# --- API Keys Configuration ---
FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY')
MARKETAUX_API_KEY = os.getenv('MARKETAUX_API_KEY')  # NEW: Marketaux API Key

FINNHUB_NEWS_BASE_URL = 'https://finnhub.io/api/v1/news'
# NEW: Marketaux API Base URL
MARKETAUX_NEWS_BASE_URL = 'https://api.marketaux.com/v1/news/all'


# --- Existing: Finnhub News Fetching Function (keep as is) ---
def fetch_news_from_finnhub(api_key, mongo_collection, num_articles_limit=15):
    # ... (content of this function remains exactly the same as before) ...
    """
    Fetches financial news from Finnhub API, filters for Indian context,
    and inserts them into MongoDB.
    """
    print("\n--- Data Collection via Finnhub API ---")  # Simplified print
    if mongo_collection is None:
        print("MongoDB collection not available for Finnhub news. Aborting.")
        return []

    categories = ['general']
    all_fetched_articles = []
    processed_count = 0

    indian_keywords = ['india', 'indian', 'nifty', 'sensex', 'rbi', 'nse', 'bse', 'mumbai', 'delhi', 'adani',
                       'reliance', 'tata', 'infosys', 'sbi', 'icici', 'hdfc']

    for category in categories:
        params = {
            'category': category,
            'token': api_key
        }

        try:
            print(f"Fetching {category} news from Finnhub...")
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
                    'source': item.get('source'),
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
            print(f"Error fetching news from Finnhub API for category '{category}': {e}")
            print(f"Response content: {response.text[:200] if response else 'N/A'}")
        except Exception as e:
            print(f"An unexpected error occurred processing Finnhub news for category '{category}': {e}")

    print(f"Finnhub news collection complete. Inserted {processed_count} new/updated articles.")
    return all_fetched_articles


# --- NEW: Marketaux News Fetching Function ---
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

    all_fetched_articles = []
    processed_count = 0

    # Marketaux allows filtering by country directly!
    # Common financial keywords might still be useful to narrow down,
    # or you can use their 'industries' or 'sectors' parameters if available.

    # We will use 'search' parameter for keywords like Nifty/Sensex
    # And 'countries' for 'in' (India)
    params = {
        'api_token': api_key,
        'countries': 'in',  # Direct filter for India
        'limit': 100,  # Max number of articles per request (adjust based on your plan)
        'sort': 'published_desc'  # Sort by latest
        # Marketaux also has 'published_before', 'published_after' for date range
        # and 'offset' for pagination to get more articles.
    }

    try:
        print("Fetching news from Marketaux...")
        response = requests.get(MARKETAUX_NEWS_BASE_URL, params=params, timeout=15)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

        json_data = response.json()
        news_items = json_data.get('data', [])  # Marketaux returns data under 'data' key

        print(f"Fetched {len(news_items)} news items from Marketaux.")

        for item in news_items:
            # Marketaux fields: 'title', 'description', 'url', 'published_at', 'source', etc.
            # 'description' is usually a good candidate for content.

            # Convert 'published_at' (ISO format string) to YYYY-MM-DD
            published_date_str = None
            if item.get('published_at'):
                try:
                    # Example: "2023-10-27T10:00:00.000000Z"
                    dt_object = datetime.fromisoformat(item['published_at'].replace('Z', '+00:00'))
                    published_date_str = dt_object.strftime('%Y-%m-%d')
                except ValueError:
                    print(f"Warning: Could not parse Marketaux date '{item['published_at']}'. Storing as raw string.")
                    published_date_str = item['published_at']

            article_data = {
                'title': item.get('title'),
                'content': item.get('description'),  # Using 'description' as main content
                'url': item.get('url'),
                'publication_date': published_date_str,
                'source': item.get('source'),
                'sentiment_score': None,
                'companies_mentioned': [],
                'sectors_mentioned': []
            }

            if not article_data['url'] or not article_data['title'] or not article_data['content']:
                print(f"Skipping article due to missing crucial data from Marketaux: {article_data.get('url', 'N/A')}")
                continue

            inserted = insert_article_into_mongodb(mongo_collection, article_data)
            if inserted:
                all_fetched_articles.append(article_data)  # Add to list if newly inserted
                processed_count += 1

            if processed_count >= num_articles_limit:
                print(f"Reached article limit ({num_articles_limit}) for Marketaux news, stopping.")
                return all_fetched_articles

        time.sleep(0.5)  # Be polite to API

    except requests.exceptions.RequestException as e:
        print(f"Error fetching news from Marketaux API: {e}")
        print(f"Response content: {response.text[:200] if response else 'N/A'}")
    except Exception as e:
        print(f"An unexpected error occurred processing Marketaux news: {e}")

    print(f"Marketaux news collection complete. Inserted {processed_count} new/updated articles.")
    return all_fetched_articles


# --- Web Scraping Functions (Keep these for now, if you wish to retain the scraper, otherwise remove them) ---
# (get_html_content, parse_article_page, scrape_economic_times_headlines)
# ...

# --- MongoDB Integration Functions (connect_to_mongodb, insert_article_into_mongodb) ---
# ... (these remain unchanged) ...

# --- Main Execution Block (Modified to call both Finnhub and Marketaux) ---
if __name__ == "__main__":
    print("Starting news processing pipeline with Finnhub/Marketaux APIs and MongoDB integration...")

    # Check if API keys are loaded
    if not FINNHUB_API_KEY:
        print("Error: FINNHUB_API_KEY not found in .env file or environment variables.")
        print("Please ensure you have FINNHUB_API_KEY=YOUR_KEY_HERE in your .env file.")
        exit(1)
    if not MARKETAUX_API_KEY:  # NEW: Check Marketaux API key
        print("Error: MARKETAUX_API_KEY not found in .env file or environment variables.")
        print("Please ensure you have MARKETAUX_API_KEY=YOUR_KEY_HERE in your .env file.")
        exit(1)

    mongo_collection = connect_to_mongodb(db_name='indian_market_scanner_db', collection_name='news_articles')

    if mongo_collection is not None:
        print("\nMongoDB connection established for all processes.")

        # Phase 1 & 2: Data Collection via Finnhub API
        finnhub_news_summary = fetch_news_from_finnhub(
            api_key=FINNHUB_API_KEY,
            mongo_collection=mongo_collection,
            num_articles_limit=20  # Adjust limit for each source as per your free tier/needs
        )
        if finnhub_news_summary:
            print(f"\nFinnhub news collection complete. Processed {len(finnhub_news_summary)} articles.")
        else:
            print("No new articles fetched from Finnhub or an error occurred during collection.")

        # NEW: Phase 1 & 2: Data Collection via Marketaux API
        marketaux_news_summary = fetch_news_from_marketaux(
            api_key=MARKETAUX_API_KEY,
            mongo_collection=mongo_collection,
            num_articles_limit=20  # Adjust limit
        )
        if marketaux_news_summary:
            print(f"\nMarketaux news collection complete. Processed {len(marketaux_news_summary)} articles.")
        else:
            print("No new articles fetched from Marketaux or an error occurred during collection.")

        # Phase 3a: Sentiment Analysis
        print("\n--- Phase 3a: Sentiment Analysis ---")
        process_and_update_sentiment(mongo_collection)

        # Phase 3b: Entity and Sector Recognition
        print("\n--- Phase 3b: Entity and Sector Recognition ---")
        process_and_update_entities(mongo_collection)

    else:
        print("\nFailed to connect to MongoDB. All processing aborted.")

    print(
        "\nProject execution complete. Check your MongoDB for updated sentiment scores and identified entities/sectors.")
    print(
        "To view data in MongoDB Compass: connect to localhost:27017, then navigate to indian_market_scanner_db > news_articles.")
    print(
        "To view data in mongosh: `use indian_market_scanner_db` then `db.news_articles.find({'sentiment_score': {'$ne': null}}).pretty()`")
# --- Main Execution Block ---
if __name__ == "__main__":
    print("Starting news scraping, sentiment analysis, and entity recognition with MongoDB integration...")

    mongo_collection = connect_to_mongodb(db_name='indian_market_scanner_db', collection_name='news_articles')

    if mongo_collection is not None:
        print("\nMongoDB connection established for all processes.")

        # Phase 1 & 2: Data Scraping (Optional: Comment out if you have enough data)
        print("\n--- Phase 1 & 2: Data Scraping ---")
        scraped_news_summary = scrape_economic_times_headlines(
            base_url='https://economictimes.indiatimes.com/news/latest-news',
            num_articles_limit=15,
            mongo_collection=mongo_collection  # Pass the same collection object
        )
        if scraped_news_summary:
            print(f"\nScraping complete. Processed {len(scraped_news_summary)} articles for database insertion/update.")
        else:
            print("No new articles were scraped or processed for insertion during this run.")

        # Phase 3a: Sentiment Analysis
        print("\n--- Phase 3a: Sentiment Analysis ---")
        process_and_update_sentiment(mongo_collection)  # Pass the collection object

        # Phase 3b: Entity and Sector Recognition
        print("\n--- Phase 3b: Entity and Sector Recognition ---")
        process_and_update_entities(mongo_collection)  # Pass the collection object

    else:
        print("\nFailed to connect to MongoDB. All processing aborted.")

    print(
        "\nProject execution complete. Check your MongoDB for updated sentiment scores and identified entities/sectors.")
    print(
        "To view data in MongoDB Compass: connect to localhost:27017, then navigate to indian_market_scanner_db > news_articles.")
    print(
        "To view data in mongosh: `use indian_market_scanner_db` then `db.news_articles.find({'sectors_mentioned': {'$ne': []}}).pretty()`")