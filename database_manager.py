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
    content = []

    title_element = soup.find('h1', {'class': 'artTitle'})
    if not title_element:
        title_element = soup.find('h1', {'class': 'article_title'})
    if not title_element:
        title_element = soup.find('h1')

    if title_element:
        title = title_element.get_text(strip=True)

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

    article_body = soup.find('div', {'class': 'artcontent'})
    if not article_body:
        article_body = soup.find('div', {'class': 'Normal'})
    if not article_body:
        article_body = soup.find('div', {'class': 'article_body'})
    if not article_body:
        article_body = soup.find('section', {'itemprop': 'articleBody'})

    if article_body:
        paragraphs = article_body.find_all('p')
        for p in paragraphs:
            paragraph_text = p.get_text(strip=True)
            if len(paragraph_text) > 50 and not paragraph_text.startswith(
                    "Also Read:") and not paragraph_text.lower().startswith("read more:"):
                content.append(paragraph_text)

    full_content = "\n".join(content)

    if not title and not full_content:
        print(f"Warning: Could not extract significant content from {article_url}")
        return None

    return {
        'title': title,
        'date': date,
        'content': full_content,
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