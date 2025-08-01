import requests
from bs4 import BeautifulSoup
import time
import re


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
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
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

    # --- Extract Title ---
    # Common selectors for titles on ET article pages
    title_element = soup.find('h1', {'class': 'artTitle'})  # Main title class
    if not title_element:
        title_element = soup.find('h1', {'class': 'article_title'})  # Another possible title class
    if not title_element:
        title_element = soup.find('h1')  # Fallback to any h1

    if title_element:
        title = title_element.get_text(strip=True)

    # --- Extract Date ---
    # Dates are often in span/div with specific classes, or within meta tags
    date_element = soup.find('time', {'class': 'publishedAt'})
    if not date_element:
        date_element = soup.find('div', {'class': 'publish_on'})
    if not date_element:
        date_element = soup.find('span', {'class': 'byline_data'})  # Another common class

    if date_element:
        date = date_element.get_text(strip=True)
        # Often date strings need cleaning, e.g., "Updated: Aug 1, 2025, 08:45 AM IST"
        # We can use regex to extract just the date part if needed
        match = re.search(r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}', date)
        if match:
            date = match.group(0)
        else:
            match = re.search(r'\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}', date)
            if match:
                date = match.group(0)

    # --- Extract Article Content ---
    # The main article content is usually within a specific div/article tag.
    # We need to find the container that holds the main body of the text
    # and then extract all paragraph tags within it.

    # Common article body container classes on ET
    article_body = soup.find('div', {'class': 'artcontent'})
    if not article_body:
        article_body = soup.find('div', {'class': 'Normal'})  # Older or different layout
    if not article_body:
        article_body = soup.find('div', {'class': 'article_body'})  # Another possible class
    if not article_body:  # Fallback for some structures
        article_body = soup.find('section', {'itemprop': 'articleBody'})

    if article_body:
        paragraphs = article_body.find_all('p')
        for p in paragraphs:
            # Filter out short paragraphs that might be captions, ads, or junk
            paragraph_text = p.get_text(strip=True)
            if len(paragraph_text) > 50 and not paragraph_text.startswith(
                    "Also Read:") and not paragraph_text.lower().startswith("read more:"):
                content.append(paragraph_text)

    full_content = "\n".join(content)

    if not title and not full_content:  # If both are empty, it's likely a bad scrape
        print(f"Warning: Could not extract significant content from {article_url}")
        return None

    return {
        'title': title,
        'date': date,
        'content': full_content,
        'url': article_url,
        'source': 'Economic Times'
    }


def scrape_economic_times_headlines(base_url, num_pages=1):
    """
    Scrapes headlines and article URLs from Economic Times listing pages.
    Then, for each article URL, scrapes the full article content.
    """
    all_articles_data = []

    # Iterate through pages (Economic Times often uses pagination, though some sections are endless scroll)
    # For simplicity, we'll assume a basic URL structure with page numbers for now.
    # You might need to adjust this URL or use Selenium for truly dynamic loading.

    # Example base_url: 'https://economictimes.indiatimes.com/latest-news'
    # ET often loads more content dynamically. For initial scrape, let's target the main
    # sections which often have navigable page numbers or a clear list structure.
    # Let's try to get articles from the main landing page or a specific news category.
    # For demonstration, we'll try a common pattern for listing articles.

    # A more robust approach for ET would involve inspecting the network requests
    # to find the API calls that load more news, or using Selenium.
    # For now, let's target the visible links on a main news section.

    # Example URL for a news listing page. This might require adjustment.
    # Often, the main page or category pages don't have explicit page numbers in URL like ?page=X.
    # For demonstration, let's use a URL that lists many articles.
    # If the site loads content dynamically, this will only get the initially loaded articles.

    urls_to_scrape = [
        # You might need to try different sections, e.g., 'markets', 'latest-news', 'industry'
        # 'https://economictimes.indiatimes.com/markets/stocks/news', # Good for stock specific news
        'https://economictimes.indiatimes.com/news/latest-news',  # General latest news
        'https://economictimes.indiatimes.com/markets',  # Market news overview
    ]

    seen_urls = set()  # To avoid scraping the same article multiple times if it appears on different lists

    for page_url in urls_to_scrape:
        print(f"Fetching news from listing page: {page_url}")
        html_content = get_html_content(page_url)
        if not html_content:
            continue

        soup = BeautifulSoup(html_content, 'lxml')

        # Find all article links on the listing page
        # This part is highly dependent on the current HTML structure of Economic Times.
        # Common patterns: div with a specific class for news cards, then an anchor tag inside.

        # Example 1: A very common pattern for news items on ET:
        # div with class 'eachStory' or similar, containing an 'a' tag.
        news_items = soup.find_all('div', class_=lambda x: x and ('eachStory' in x or '_3Y-96' in x))

        if not news_items:
            # Try another common pattern if the first one doesn't yield results
            news_items = soup.find_all('a', class_=lambda x: x and ('newsHdng' in x or 'story_title' in x))
            # Or find specific h2/h3 tags containing links
            if not news_items:
                h2_links = soup.find_all('h2')
                for h2 in h2_links:
                    link = h2.find('a', href=True)
                    if link and 'economictimes.indiatimes.com' in link['href']:
                        news_items.append(link)  # Add the link object directly

        for item in news_items:
            link = None
            if item.name == 'a':  # If item itself is an anchor tag
                link = item
            else:  # If it's a div containing an anchor tag
                link = item.find('a', href=True)

            if link and link.get('href'):
                article_url = link['href']

                # Ensure the URL is absolute
                if not article_url.startswith('http'):
                    article_url = f"https://economictimes.indiatimes.com{article_url}"

                # Filter out non-article links (e.g., videos, photo galleries, specific sections)
                # and avoid duplicates
                if "/news/" in article_url and "articleshow" in article_url and article_url not in seen_urls:
                    article_data = parse_article_page(article_url)
                    if article_data:
                        all_articles_data.append(article_data)
                        seen_urls.add(article_url)
                        time.sleep(1)  # Be polite, add a delay between requests

            # Limit the number of articles for a quick test run
            if len(all_articles_data) >= 10:  # Adjust this limit as needed
                break
        if len(all_articles_data) >= 10:
            break

    return all_articles_data


# ... (rest of your code above remains the same)

if __name__ == "__main__":
    print("Starting Economic Times news scraping...")
    # Fix: Pass a base_url argument. We'll pick one of the main listing pages.
    # The function internally iterates through a list, but this fixes the signature error.
    main_et_news_url = 'https://economictimes.indiatimes.com/news/latest-news'
    scraped_news = scrape_economic_times_headlines(main_et_news_url, num_pages=1)

    if scraped_news:
        print(f"\nScraped {len(scraped_news)} articles.")
        for i, article in enumerate(scraped_news):
            print(f"\n--- Article {i + 1} ---")
            print(f"Title: {article.get('title', 'N/A')}")
            print(f"Date: {article.get('date', 'N/A')}")
            print(f"URL: {article.get('url', 'N/A')}")
            print(f"Content (first 200 chars): {article.get('content', 'N/A')[:200]}...")
    else:
        print("No articles scraped or an error occurred.")