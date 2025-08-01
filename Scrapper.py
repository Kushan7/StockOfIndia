import requests
from bs4 import BeautifulSoup
import time
import re
from datetime import datetime


# (Keep get_html_content function as is)
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


# (Keep parse_article_page function as is for now, we'll confirm its effectiveness after this part)
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


def scrape_economic_times_headlines(base_url, num_articles_limit=10):
    """
    Scrapes headlines and article URLs from Economic Times listing pages
    based on the latest HTML structure provided in the screenshot.
    Then, for each article URL, scrapes the full article content.
    """
    all_articles_data = []
    seen_urls = set()  # To avoid scraping the same article multiple times

    # The base_url argument is just for the function signature,
    # we'll use a hardcoded list of relevant ET news URLs for broader coverage.
    urls_to_scrape = [
        'https://economictimes.indiatimes.com/news/latest-news',
        'https://economictimes.indiatimes.com/markets/stocks/news',  # Specific stock news
        'https://economictimes.indiatimes.com/markets/et-markets/real-time-news'  # Another real-time news section
        # You can add more category pages here.
    ]

    for page_url in urls_to_scrape:
        print(f"Fetching news from listing page: {page_url}")
        html_content = get_html_content(page_url)
        if not html_content:
            continue

        soup = BeautifulSoup(html_content, 'lxml')

        # Target the <ul> with class="data" as the main container
        news_list_container = soup.find('ul', class_='data')

        if not news_list_container:
            print(f"Could not find news list container on {page_url}. Check HTML structure again.")
            continue  # Move to the next URL if container is not found

        # Find all <li> elements within this container
        news_items = news_list_container.find_all('li', itemprop='itemListElement')

        if not news_items:
            print(f"No news items found within the container on {page_url}. Check LI structure.")
            continue

        for item in news_items:
            # Extract the <a> tag which contains the title and URL
            link_tag = item.find('a', href=True)

            # Extract the <span> tag for the timestamp
            timestamp_tag = item.find('span', class_='timestamp', attrs={'data-time': True})

            if link_tag and timestamp_tag:
                title = link_tag.get_text(strip=True)
                article_url = link_tag['href']

                # Extract date from data-time attribute for better accuracy
                date_str = timestamp_tag['data-time']
                # The format is 'YYYY-MM-DDTHH:MM:SS+HH:MM' or similar. We want just the date.
                try:
                    # Example: 2025-08-01T19:43:00Z -> 2025-08-01
                    # Or '2025-08-02, 01:13 AM IST' -> 2025-08-02 (from get_text)
                    # Let's prioritize data-time as it's cleaner
                    parsed_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    formatted_date = parsed_date.strftime('%Y-%m-%d')
                except ValueError:
                    # Fallback to get_text if data-time is not a standard ISO format
                    # or if we prefer the displayed text for some reason
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
                            formatted_date = display_date_text  # Keep raw if unable to parse

                # Ensure the URL is absolute
                if not article_url.startswith('http'):
                    article_url = f"https://economictimes.indiatimes.com{article_url}"

                # Filter out non-article links and avoid duplicates
                if "/articleshow/" in article_url and "economictimes.indiatimes.com" in article_url and article_url not in seen_urls:
                    print(f"Found article link: {article_url}")  # Debugging line

                    # Call parse_article_page to get full content
                    article_details = parse_article_page(article_url)

                    if article_details:
                        # Override title and date if we got better ones from listing page
                        article_details['title'] = title
                        article_details['date'] = formatted_date  # Use the clean date from listing page
                        all_articles_data.append(article_details)
                        seen_urls.add(article_url)
                        time.sleep(1.5)  # Be polite, add a delay between requests
                    else:
                        # If parsing the full article page failed, just add what we got from listing
                        all_articles_data.append({
                            'title': title,
                            'date': formatted_date,
                            'content': 'Failed to scrape full content',
                            'url': article_url,
                            'source': 'Economic Times'
                        })
                        seen_urls.add(article_url)  # Still mark as seen

                # Limit the number of articles for a quick test run
                if len(all_articles_data) >= num_articles_limit:  # Use the passed limit
                    print(f"Reached article limit ({num_articles_limit}) for testing, stopping.")
                    break

        if len(all_articles_data) >= num_articles_limit:
            break  # Break out of the page_url loop too

    return all_articles_data


if __name__ == "__main__":
    print("Starting Economic Times news scraping...")

    # We will pass a dummy base_url as the function uses an internal list of URLs
    # The num_articles_limit parameter is useful for controlled testing.
    scraped_news = scrape_economic_times_headlines(base_url='https://economictimes.indiatimes.com/news/latest-news',
                                                   num_articles_limit=10)

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