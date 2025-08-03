# et_news_scraper.py

import requests
from bs4 import BeautifulSoup
import time
import re
from datetime import datetime, timedelta  # Need datetime for date parsing/comparison


# Note: This file does NOT directly import pymongo or load_dotenv.
# It receives functions for DB interaction from the main script.


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


# et_news_scraper.py

# ... (imports: requests, BeautifulSoup, time, re, datetime, etc. should be at the top) ...




def parse_article_page(article_url):
    """
    Parses a single Economic Times article page to extract title, date, and content.
    Includes enhanced filtering for common non-content fragments.
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

    # --- Extract Article Content (REFINED STRATEGY - Broadened Search & Enhanced Filtering) ---
    full_content_parts = []

    article_data_container = soup.find('div', class_=lambda x: x and 'artdata' in x.split())

    if not article_data_container:
        article_data_container = soup.find('div', id='pagecontent')
        if article_data_container:
            article_data_container = article_data_container.find('div', class_='pagecontent_fit')

    if article_data_container:
        # Find all divs and p tags within this broader container that might hold content.
        text_containing_elements = article_data_container.find_all(
            ['div', 'p', 'span', 'strong'])  # Added span and strong

        # Compile common non-content phrases for more efficient filtering
        # These are case-insensitive
        non_content_starters = (
            "read more:", "also read:", "download the economic times app",
            "by downloading the app", "follow us on", "join us on",
            "view more", "watch now", "trending now",
            "et prime", "to see how", "track live", "go to the", "for full story",
            "terms of use", "privacy policy", "cookie policy", "disclaimer",
            "all rights reserved", "copyright"
        )
        non_content_exact = ("et", "live", "more", "full story", "download", "app")  # Exact matches for short junk

        for element in text_containing_elements:
            text_chunk = element.get_text(separator=' ', strip=True)  # Get text and clean whitespace

            # Basic length filter (can be adjusted)
            if len(text_chunk) < 50:  # Too short for meaningful content
                # Allow very short text if it looks like a headline (e.g., from an a tag, but not generic like price)
                if len(text_chunk) > 10 and element.name in ['h1', 'h2', 'h3',
                                                             'a']:  # Might be a sub-headline or link title
                    pass  # Don't filter out yet, let other filters handle
                else:
                    continue  # Skip very short snippets

            # Filter out common non-content phrases (case-insensitive)
            text_chunk_lower = text_chunk.lower()
            if any(text_chunk_lower.startswith(phrase) for phrase in non_content_starters) or \
                    any(text_chunk_lower == phrase for phrase in non_content_exact) or \
                    (len(text_chunk) < 100 and (text_chunk_lower.endswith(
                        "ist") or "am ist" in text_chunk_lower or "pm ist" in text_chunk_lower)):  # Often timestamps/bylines
                continue  # Skip if it matches a known non-content pattern

            # Filter out known ad/widget indicators if they sneak in
            if "ad-slot" in element.get('class', []) or "widget" in element.get('class', []) or \
                    "sponsored" in text_chunk_lower or "advertisement" in text_chunk_lower:
                continue

            # If it passes all filters, add to list
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


# --- Main ET Scraping Function ---
# This function's content remains as provided in the last full fixed code
# It relies on parse_article_page from this file, and get_latest_news_date_func/insert_article_func
# passed from database_manager.py.
def scrape_economic_times_headlines(num_articles_limit=10, get_latest_news_date_func=None, insert_article_func=None):
    """
    Scrapes headlines and article URLs from Economic Times listing pages
    and optionally inserts them into the provided MongoDB collection via insert_article_func.
    It will try to only process articles newer than the latest found in DB.
    """
    if get_latest_news_date_func is None or insert_article_func is None:
        print("Error: DB helper functions not provided to scrape_economic_times_headlines. Aborting.")
        return []

    all_articles_data = []
    seen_urls = set()

    # Get the latest date for ET from DB for smart fetching
    latest_et_date_in_db = get_latest_news_date_func("Economic Times")
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
                    # FIX: Use datetime.fromisoformat, assuming datetime is imported correctly here
                    parsed_date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    formatted_date_str = parsed_date_obj.strftime('%Y-%m-%d')
                    article_date_obj = parsed_date_obj
                except ValueError:
                    display_date_text = timestamp_tag.get_text(strip=True)
                    match = re.search(r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}',
                                      display_date_text)
                    if match:
                        formatted_date_str = match.group(0)
                        try:
                            article_date_obj = datetime.strptime(formatted_date_str, '%b %d, %Y')
                        except ValueError:
                            pass
                    else:
                        match = re.search(r'\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}',
                                          display_date_text)
                        if match:
                            formatted_date_str = match.group(0)
                            try:
                                article_date_obj = datetime.strptime(formatted_date_str, '%d %b %Y')
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

                        # Call the passed insert_article_func
                        inserted_successfully = insert_article_func(article_details)
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
                            'source': 'Economic Times',
                            'sentiment_score': None,
                            'companies_mentioned': [],
                            'sectors_mentioned': []
                        }
                        # Call the passed insert_article_func
                        insert_article_func(basic_article_data)
                        all_articles_data.append(basic_article_data)
                        seen_urls.add(article_url)

                if len(all_articles_data) >= num_articles_limit:
                    print(f"Reached article limit ({num_articles_limit}) for testing, stopping.")
                    break

        if len(all_articles_data) >= num_articles_limit:
            break

    return all_articles_data


# --- Test Execution Block for et_news_scraper.py ---
# This block is here for testing this file in isolation.
# It uses mock database functions.
if __name__ == "__main__":
    print("--- Running Economic Times Scraper Separately for Testing ---")


    # Simple Mock DB for testing this file in isolation:
    class MockNewsCollection:
        def __init__(self):
            self.data = {}

        def insert_one(self, doc):
            print(f"Mock insert: {doc.get('title', 'N/A')}")
            self.data[doc.get('url')] = doc
            return type('obj', (object,), {'inserted_id': 'mock_id'})()  # Mimic result

        def update_one(self, query, update, upsert):
            url = query.get('url')
            if url in self.data:
                self.data[url].update(update.get('$set', {}))
                print(f"Mock update: {url}")
                return type('obj', (object,), {'matched_count': 1, 'modified_count': 1, 'upserted_id': None})()
            elif upsert:
                self.data[url] = update.get('$set', {})
                print(f"Mock upsert: {url}")
                return type('obj', (object,), {'matched_count': 0, 'modified_count': 0, 'upserted_id': 'mock_id'})()
            return type('obj', (object,), {'matched_count': 0, 'modified_count': 0, 'upserted_id': None})()

        def find(self, query=None):
            # Simplified mock for get_latest_news_date
            if query and query.get("source") == "Economic Times":
                if self.data:
                    # Find latest from mock data (not robust for real world)
                    latest = max((d for d in self.data.values() if
                                  d.get('source') == "Economic Times" and isinstance(d.get('publication_date'),
                                                                                     datetime)),
                                 key=lambda x: x.get('publication_date', datetime.min), default=None)
                    if latest:
                        # Return an iterator-like object
                        return type('obj', (object,), {'next': lambda: latest})()
            return type('obj', (object,), {'next': lambda: (_ for _ in ()).throw(StopIteration)})()  # Empty iterator


    mock_db_collection = MockNewsCollection()


    # Mock versions of the functions scrape_economic_times_headlines needs
    def mock_get_latest_news_date_func(source_name):
        # Directly use mock_db_collection
        latest_article = mock_db_collection.find(
            {"source": source_name, "publication_date": {"$ne": None}}
        )
        try:
            latest = latest_article.next()
            return latest.get('publication_date')
        except StopIteration:
            return None


    def mock_insert_article_func(article_data):
        return mock_db_collection.update_one({'url': article_data['url']}, {'$set': article_data}, upsert=True)


    et_scraped_summary = scrape_economic_times_headlines(
        num_articles_limit=5,  # Small limit for quick test
        get_latest_news_date_func=mock_get_latest_news_date_func,
        insert_article_func=mock_insert_article_func
    )
    print(f"\nET Scraper Test complete. Processed {len(et_scraped_summary)} articles.")
    print("Mock DB content (first 2):", list(mock_db_collection.data.values())[:2])