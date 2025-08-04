# et_news_scraper.py

import requests
from bs4 import BeautifulSoup
import time
import re
from datetime import datetime as dt_class, date as date_class, timedelta
import pymongo
from pymongo.errors import ConnectionFailure, DuplicateKeyError

# Import core MongoDB insertion utility from the main file
from database_manager import get_latest_news_date, insert_article_into_mongodb


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
    """
    all_articles_data = []
    seen_urls = set()

    # Call the imported helper functions
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

                    # Since ET scraping is for metadata only, we can simplify this even more
                    article_details = parse_article_page(article_url)

                    if article_details and article_details['title'] and article_details['url']:
                        inserted_successfully = insert_article_into_mongodb(mongo_collection, article_details)
                        if inserted_successfully:
                            all_articles_data.append(article_details)
                        else:
                            all_articles_data.append(article_details)
                    else:
                        print(f"Warning: Missing title or URL for article: {article_url}. Skipping.")

                    seen_urls.add(article_url)
                    time.sleep(1.5)

                if len(all_articles_data) >= num_articles_limit:
                    print(f"Reached article limit ({num_articles_limit}) for testing, stopping.")
                    break

        if len(all_articles_data) >= num_articles_limit:
            break

    return all_articles_data

# --- Main Execution Block (removed from here, will be in database_manager.py) ---