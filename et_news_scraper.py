# et_news_scraper.py

import requests
from bs4 import BeautifulSoup
import time
import re
from datetime import datetime as dt_class, date as date_class, timedelta
import pymongo
from pymongo.errors import ConnectionFailure, DuplicateKeyError


# --- Helper Functions for Caching/Insertion (Local to this file) ---
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
        if isinstance(latest.get('publication_date'), dt_class):
            return latest['publication_date']
        elif isinstance(latest.get('publication_date'), str):
            try:
                return dt_class.strptime(latest['publication_date'], '%Y-%m-%d')
            except ValueError:
                return None
        return None
    except StopIteration:
        return None


def insert_article_into_mongodb(collection, article_data):
    """
    Inserts a single news article document into the MongoDB collection.
    Uses update_one with upsert=True to insert if not exists, or update if exists.
    """
    if collection is None:
        print("MongoDB collection not available. Skipping insertion.")
        return False

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


# --- Web Scraping Helper Functions (Specific to ET structure) ---
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
    print(f"Scraping article content: {article_url}")
    html_content = get_html_content(article_url)
    if not html_content:
        print(f"Failed to fetch HTML for {article_url}.")
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

    # --- FINAL STRATEGY FOR ET: ONLY EXTRACT METADATA ---
    return {
        'title': title,
        'date': date,
        'content': "",
        'url': article_url,
        'source': 'Economic Times'
    }


def scrape_economic_times_headlines(mongo_collection, num_articles_limit=10):
    """
    Scrapes headlines and article URLs from Economic Times listing pages
    and inserts them into the provided MongoDB collection.
    It will only fetch metadata (title, URL, date) for ET articles.
    """
    if mongo_collection is None:
        print("Error: DB collection not provided. Aborting ET scraper.")
        return []

    all_articles_data = []
    seen_urls = set()

    latest_et_date_in_db = get_latest_news_date(mongo_collection, "Economic Times")
    if latest_et_date_in_db:
        print(
            f"Latest Economic Times article in DB is from: {latest_et_date_in_db.strftime('%Y-%m-%d')}. Fetching newer news.")
    else:
        print("No Economic Times articles found in DB. Fetching recent news.")

    urls_to_scrape = [
        'https://economictimes.indiatimes.com/news/latest-news',
        'https://economictimes.indiatimes.com/markets/stocks/news',
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
                article_date_obj = None

                try:
                    parsed_date_obj = dt_class.fromisoformat(date_str.replace('Z', '+00:00'))
                    formatted_date_str = parsed_date_obj.strftime('%Y-%m-%d')
                    article_date_obj = parsed_date_obj
                except ValueError:
                    display_date_text = timestamp_tag.get_text(strip=True)
                    match = re.search(r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}',
                                      display_date_text)
                    if match:
                        formatted_date_str = match.group(0)
                        try:
                            article_date_obj = dt_class.strptime(formatted_date_str, '%b %d, %Y')
                        except ValueError:
                            pass
                    else:
                        match = re.search(r'\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}',
                                          display_date_text)
                        if match:
                            formatted_date_str = match.group(0)
                            try:
                                article_date_obj = dt_class.strptime(formatted_date_str, '%d %b %Y')
                            except ValueError:
                                pass
                        else:
                            formatted_date_str = display_date_text

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
                        article_details['source'] = "Economic Times"

                        inserted_successfully = insert_article_into_mongodb(mongo_collection, article_details)
                        if inserted_successfully:
                            all_articles_data.append(article_details)
                        else:
                            all_articles_data.append(article_details)
                    else:
                        print(f"Warning: Missing title or URL for article: {article_url}. Skipping.")
                        continue  # Skip to next article if metadata extraction failed

                    seen_urls.add(article_url)
                    time.sleep(1.5)

                if len(all_articles_data) >= num_articles_limit:
                    print(f"Reached article limit ({num_articles_limit}) for testing, stopping.")
                    break

        if len(all_articles_data) >= num_articles_limit:
            break

    return all_articles_data