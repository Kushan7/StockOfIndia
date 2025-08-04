# et_news_scraper.py

import requests
from bs4 import BeautifulSoup
import time
import re
from datetime import datetime, timedelta

# Note: This file needs these imports because the functions within it use them directly.
import pymongo


# For the test execution block, we will use a Mock DB, so these imports are not strictly needed
# for the main functions, but they are here for the test harness.


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

    # --- Extract Article Content (REFINED STRATEGY - Broadened Search & Aggressive Filtering) ---
    all_content_chunks = []
    content_container_selectors = [
        {'name': 'div', 'attrs': {'data-articlebody': '1'}},
        {'name': 'div', 'attrs': {'class': 'arttextmedium'}},
        {'name': 'div', 'attrs': {'class': 'arttext'}},
        {'name': 'div', 'attrs': {'class': lambda x: x and 'artdata' in x.split()}},
        {'name': 'div', 'attrs': {'id': 'pagecontent'}},
        {'name': 'section', 'attrs': {'itemprop': 'articleBody'}},
        {'name': 'div', 'attrs': {'class': 'Normal'}}
    ]
    main_content_area = None
    for selector in content_container_selectors:
        main_content_area = soup.find(selector['name'], selector['attrs'])
        if main_content_area:
            break
    if not main_content_area:
        print(f"Warning: Main content container not found for {article_url}. Falling back to generic paragraphs.")
        paragraphs = soup.find_all('p')
        if paragraphs:
            for p in paragraphs:
                all_content_chunks.append(p.get_text(separator=' ', strip=True))
    else:
        text_containing_elements = main_content_area.find_all(
            ['div', 'p', 'span', 'strong', 'em', 'li', 'h1', 'h2', 'h3', 'h4', 'a'])
        non_content_filters = (
            "read more:", "also read:", "download the economic times app",
            "by downloading the app", "follow us on", "join us on",
            "view more", "watch now", "trending now",
            "et prime", "to see how", "track live", "go to the", "for full story",
            "terms of use", "privacy policy", "cookie policy", "disclaimer",
            "all rights reserved", "copyright", "subscribe", "newsletter", "sign in",
            "log in", "photo gallery", "in pics", "videos", "podcast", "topics", "agencies",
            "pti", "ani", "ap", "reuters", "bloomberg", "et now", "et wealth", "et auto",
            "et government", "et retail", "et bfsi", "et markets", "et hrworld", "et energy",
            "et telecom", "et panache", "et realty", "et healthworld", "et tech", "et travel"
        )
        non_content_exact = (
            "et", "live", "more", "full story", "download", "app", "prime", "english", "hindi",
            "subscribe", "read", "share", "connect"
        )
        non_content_contains = (
            "stock market", "budget 2025", "latest news updates", "breaking news",
            "stories you might be interested in", "hot on web", "in case you missed it",
            "top searched companies", "top calculators", "top definitions", "top commodities",
            "top slideshow", "private companies", "top prime articles", "top story listing",
            "follow us on", "find this comment offensive", "reason for reporting",
            "your reason has been reported", "log in/connect with", "will be displayed",
            "will not be displayed", "worry not", "et prime membership", "offer exclusively for you",
            "save up to rs", "get 1 year free", "with 1 and 2-year et prime membership",
            "get flat", "on etprime", "then ₹", "for 1 month", "what’s included with",
            "grow your wealth", "buy low & sell high", "access to stock score",
            "upside potential", "market bulls are investing", "stock analyzer",
            "check the score based on", "fundamentals, solvency, growth", "risk & ownership",
            "market mood", "analyze the market sentiments", "trend reversal",
            "stock talk live", "ask your stock queries", "get assured replies by",
            "sebi registered experts", "epaper - print view", "read the pdf version",
            "download & access it offline", "epaper - digital view", "read your daily newspaper",
            "wealth edition", "manage your money efficiently", "toi epaper",
            "deep explainers", "health+ stories", "personal finance+ stories",
            "new york times exclusives", "timesprime subscription", "access 20+ premium subscriptions",
            "docubay subscription", "stream new documentaries", "leadership | entrepreneurship",
            "people | culture", "from new delhi", "PTI", "ANI", "AP", "REUTERS", "BLOOMBERG"
        )
        non_content_ends = (
            "ist", "comments", "shares", "update", "more", "story"
        )
        regex_filters = [
            r"^\s*\d{1,2}:\d{2}\s+(am|pm)\s+ist\s*$",
            r"^\s*\w{3,4}\s+\d{1,2},\s+\d{4},\s+\d{1,2}:\d{2}\s+(am|pm)\s+ist\s*$",
            r"^\s*\d{1,3}k\s+shares\s*$",
            r"^\s*-\s*[A-Za-z\s]+?\s*-\s*$",
            r"^\s*\(pti\)\s*$",
            r"^\d{1,}\s+comments?$",
            r"^\s*follow us on telegram",
            r"read more news on", "read more business news"
        ]
        compiled_regexes = [re.compile(pattern, re.IGNORECASE) for pattern in regex_filters]

        for element in text_containing_elements:
            text_chunk = element.get_text(separator=' ', strip=True)
            if not text_chunk:
                continue
            text_chunk_lower = text_chunk.lower()
            if any(text_chunk_lower.startswith(phrase) for phrase in non_content_starters) or \
                    any(text_chunk_lower.endswith(phrase) for phrase in non_content_ends) or \
                    any(text_chunk_lower == phrase for phrase in non_content_exact) or \
                    any(phrase in text_chunk_lower for phrase in non_content_contains) or \
                    any(regex.search(text_chunk_lower) for regex in compiled_regexes) or \
                    (element.get('class') and any(
                        indicator in class_name.lower() for class_name in element.get('class', []) for indicator in
                        ["ad", "widget", "sponsored", "promo", "related"])) or \
                    (element.get('id') and any(
                        indicator in element.get('id').lower() for indicator in ["ad", "widget", "promo", "related"])):
                continue

            if len(text_chunk) < 70:
                if element.name not in ['h1', 'h2', 'h3', 'h4', 'a', 'strong', 'em']:
                    continue

            all_content_chunks.append(text_chunk)

    full_content_str = "\n".join(all_content_chunks)
    if not title or len(full_content_str) < 150:
        print(
            f"Warning: Significant content (>=150 chars) not extracted for {article_url}. Title: '{title[:50]}...' Content len: {len(full_content_str)}")
        return None

    return {
        'title': title,
        'date': date,
        'content': full_content_str,
        'url': article_url,
        'source': 'Economic Times'
    }


# --- Main ET Scraping Function ---
# FIX: Change signature to accept mongo_collection directly
def scrape_economic_times_headlines(mongo_collection, num_articles_limit=10):
    """
    Scrapes headlines and article URLs from Economic Times listing pages
    and optionally inserts them into the provided MongoDB collection.
    """
    # ... (function content below remains the same, it will now use 'mongo_collection' directly) ...
    pass  # Placeholder for content, see next code block