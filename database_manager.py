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

# --- Finnhub API Configuration ---
# REPLACE 'YOUR_FINNHUB_API_KEY' WITH YOUR ACTUAL FINNHUB API KEY
import os
from dotenv import load_dotenv

# NEW: Load environment variables from .env file
load_dotenv()

# NEW: Access API Key from environment variables
# REMOVE THE OLD HARDCODED FINNHUB_API_KEY = 'YOUR_FINNHUB_API_KEY' line
FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY')
FINNHUB_NEWS_BASE_URL = 'https://finnhub.io/api/v1/news'


# --- NEW: Finnhub News Fetching Function ---
def fetch_news_from_finnhub(api_key, mongo_collection, num_articles_limit=15):
    """
    Fetches financial news from Finnhub API, filters for Indian context,
    and inserts them into MongoDB.
    """
    print("\n--- Phase 1 & 2: Data Collection via Finnhub API ---")
    if mongo_collection is None:
        print("MongoDB collection not available for Finnhub news. Aborting.")
        return []

    # Categories to fetch. 'general' covers most news, but you can also try 'forex', 'crypto' if relevant.
    # Finnhub general news doesn't have a direct country filter, so we'll filter keywords.
    categories = ['general']  # You can extend this, e.g., ['general', 'forex', 'crypto']

    all_fetched_articles = []
    processed_count = 0

    # Keywords to filter for Indian market context
    indian_keywords = ['india', 'indian', 'nifty', 'sensex', 'rbi', 'nse', 'bse', 'mumbai', 'delhi', 'adani',
                       'reliance', 'tata', 'infosys', 'sbi', 'icici', 'hdfc']

    for category in categories:
        params = {
            'category': category,
            'token': api_key
            # You can use 'minId' here to fetch news newer than a certain ID for incremental updates
            # 'minId': latest_news_id_from_db # For subsequent runs
        }

        try:
            print(f"Fetching {category} news from Finnhub...")
            response = requests.get(FINNHUB_NEWS_BASE_URL, params=params, timeout=15)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            news_items = response.json()
            print(f"Fetched {len(news_items)} news items from Finnhub for category '{category}'.")

            for item in news_items:
                # Basic filtering for Indian context based on title or summary
                # Lowercase for case-insensitive matching
                title_lower = item.get('headline', '').lower()
                summary_lower = item.get('summary', '').lower()

                is_indian_context = False
                for keyword in indian_keywords:
                    if keyword in title_lower or keyword in summary_lower:
                        is_indian_context = True
                        break

                if not is_indian_context:
                    continue  # Skip if no Indian keyword found

                # Transform Finnhub item into our article_data format
                article_data = {
                    'title': item.get('headline'),
                    'content': item.get('summary'),  # Finnhub 'summary' is often good enough as content
                    'url': item.get('url'),
                    'publication_date': datetime.fromtimestamp(item.get('datetime', 0)).strftime('%Y-%m-%d'),
                    # Convert Unix timestamp to YYYY-MM-DD
                    'source': item.get('source'),
                    'sentiment_score': None,  # To be filled by NLP
                    'companies_mentioned': [],  # To be filled by NLP
                    'sectors_mentioned': []  # To be filled by NLP
                }

                # Check for mandatory fields before attempting insert
                if not article_data['url'] or not article_data['title'] or not article_data['content']:
                    print(
                        f"Skipping article due to missing crucial data from Finnhub: {article_data.get('url', 'N/A')}")
                    continue

                inserted = insert_article_into_mongodb(mongo_collection, article_data)
                if inserted:
                    all_fetched_articles.append(article_data)  # Add to list if newly inserted
                    processed_count += 1

                if processed_count >= num_articles_limit:
                    print(f"Reached article limit ({num_articles_limit}) for Finnhub news, stopping.")
                    return all_fetched_articles  # Return as soon as limit is met

            time.sleep(0.5)  # Be polite to API, especially if fetching multiple categories

        except requests.exceptions.RequestException as e:
            print(f"Error fetching news from Finnhub API for category '{category}': {e}")
            print(f"Response content: {response.text[:200] if response else 'N/A'}")
        except Exception as e:
            print(f"An unexpected error occurred processing Finnhub news for category '{category}': {e}")

    print(f"Finnhub news collection complete. Inserted {processed_count} new/updated articles.")
    return all_fetched_articles


# --- Web Scraping Functions (Keep for now, but will be removed from main execution) ---
# (Keep get_html_content, parse_article_page, scrape_economic_times_headlines as is for now)
# (You can remove scrape_economic_times_headlines entirely once you're confident in Finnhub data)

# --- MongoDB Integration Functions (Keep as is) ---
# (Keep connect_to_mongodb, insert_article_into_mongodb as is)

# --- Main Execution Block (Modified for Finnhub) ---
if __name__ == "__main__":
    print("Starting news processing pipeline with Finnhub API and MongoDB integration...")

    mongo_collection = connect_to_mongodb(db_name='indian_market_scanner_db', collection_name='news_articles')

    if mongo_collection is not None:
        print("\nMongoDB connection established for all processes.")

        # Phase 1 & 2: Data Collection via Finnhub API
        # Replacing the old scraper with the new Finnhub function
        finnhub_news_summary = fetch_news_from_finnhub(
            api_key=FINNHUB_API_KEY,
            mongo_collection=mongo_collection,
            num_articles_limit=30  # Increased limit to get more Finnhub data for testing
        )
        if finnhub_news_summary:
            print(
                f"\nFinnhub news collection complete. Processed {len(finnhub_news_summary)} articles for database insertion/update.")
        else:
            print("No new articles fetched from Finnhub or an error occurred during collection.")

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