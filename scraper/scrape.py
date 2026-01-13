import pandas as pd
import requests
import time
import random
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse
from tqdm import tqdm
from bs4 import BeautifulSoup
from typing import Dict, Any, Optional


DEFAULT_USER_AGENT = 'ItalawResearchBot/1.0 (academic research)'
DEFAULT_DELAY_RANGE = (0.5, 1.5)


def get_robots_parser(base_url: str, user_agent: str, timeout: int = 10) -> RobotFileParser:
    """
    Fetch and parse robots.txt for a given base URL.

    Parameters:
        base_url: The base URL (e.g., 'https://www.italaw.com')
        user_agent: User agent string for the request
        timeout: Request timeout in seconds

    Returns:
        RobotFileParser instance (will allow all if robots.txt not found)
    """
    rp = RobotFileParser()
    robots_url = f"{base_url.rstrip('/')}/robots.txt"

    try:
        response = requests.get(robots_url, timeout=timeout, headers={'User-Agent': user_agent})
        if response.status_code == 200:
            rp.parse(response.text.splitlines())
        else:
            # No robots.txt or error - allow all by default
            rp.parse([])
    except requests.RequestException:
        # Can't fetch robots.txt - allow all by default
        rp.parse([])

    return rp


def is_url_allowed(url: str, robots_cache: dict, user_agent: str, timeout: int = 10) -> bool:
    """
    Check if a URL is allowed by the site's robots.txt.
    Caches robots.txt parsers per domain to avoid repeated fetches.

    Parameters:
        url: The URL to check
        robots_cache: Dict mapping base URLs to RobotFileParser instances
        user_agent: User agent string
        timeout: Request timeout for fetching robots.txt

    Returns:
        True if allowed, False if disallowed
    """
    parsed = urlparse(url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"

    if base_url not in robots_cache:
        robots_cache[base_url] = get_robots_parser(base_url, user_agent, timeout)

    return robots_cache[base_url].can_fetch(user_agent, url)


def fetch_html_for_urls(df, url_col='url', html_col='html', timeout=10,
                        user_agent=DEFAULT_USER_AGENT,
                        delay_range=DEFAULT_DELAY_RANGE,
                        respect_robots=True):
    """
    Fetch HTML for each non-missing URL in the DataFrame.

    Parameters:
        df (pd.DataFrame): The DataFrame containing the URLs.
        url_col (str): Name of the column with URLs.
        html_col (str): Name of the column where HTML content will be stored.
        timeout (int): Timeout for each request in seconds.
        user_agent (str): User agent string to identify the scraper.
        delay_range (tuple): Min and max seconds to wait between requests.
        respect_robots (bool): If True, check robots.txt before fetching.

    Returns:
        pd.DataFrame: DataFrame with an additional column for HTML.
    """
    # Initialize the HTML column with None
    df[html_col] = None

    # Create a session for connection reuse
    session = requests.Session()
    session.headers.update({'User-Agent': user_agent})

    # Cache for robots.txt parsers (one per domain)
    robots_cache = {}

    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Fetching HTML"):
        url = row[url_col]
        if pd.notna(url) and url != "Not available":
            # Check robots.txt if enabled
            if respect_robots and not is_url_allowed(url, robots_cache, user_agent, timeout):
                print(f"Blocked by robots.txt: {url}")
                df.at[idx, html_col] = None
                continue

            try:
                response = session.get(url, timeout=timeout)
                response.raise_for_status()
                df.at[idx, html_col] = response.text
            except requests.RequestException as e:
                print(f"Failed to fetch {url}: {e}")
                df.at[idx, html_col] = None

            # Polite delay between requests
            time.sleep(random.uniform(*delay_range))
        else:
            df.at[idx, html_col] = None

    return df

def extract_titles(df, html_col='italaw_html', title_col='italaw_title'):
    """
    Extracts the <title> string from HTML content and adds it to a new column.

    Parameters:
        df (pd.DataFrame): DataFrame with HTML content.
        html_col (str): Name of the column containing HTML.
        title_col (str): Name of the column to store the extracted title.

    Returns:
        pd.DataFrame: DataFrame with an additional column for the title.
    """
    df[title_col] = None

    for idx, html in df[html_col].items():
        if pd.notna(html):
            try:
                soup = BeautifulSoup(html, 'html.parser')
                df.at[idx, title_col] = soup.title.string.strip() if soup.title and soup.title.string else None
            except Exception as e:
                print(f"Error parsing HTML at index {idx}: {e}")
                df.at[idx, title_col] = None
        else:
            df.at[idx, title_col] = None

    return df

def extract_case_metadata(df, html_col='html', fields=None):
    """
    Extracts case metadata fields from HTML and adds them to the DataFrame.

    Fields extracted:
        - case_type
        - arbitration_rules
        - investment_treaty
        - legal_instruments
        - economic_sector

    Parameters:
        df (pd.DataFrame): DataFrame with HTML content.
        html_col (str): Name of the column containing HTML.

    Returns:
        pd.DataFrame: DataFrame with new columns for each metadata field.
    """
    
    for field_name, _ in fields:
        df[field_name] = None

    for idx, html in df[html_col].items():
        if pd.isna(html):
            continue

        try:
            soup = BeautifulSoup(html, 'html.parser')

            for field_name, field_class in fields:
                container = soup.find('div', class_=field_class)
                if container:
                    content = container.find('div', class_='field-content')
                    if content:
                        text = content.get_text(strip=True)
                        df.at[idx, field_name] = text
        except Exception as e:
            print(f"Error parsing metadata at index {idx}: {e}")

    return df

def parse_case_document(row_soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
    """
    Parses a single case document entry.
    Handles both linked and unlinked documents.
    Returns None if all key fields are missing and details is empty.
    
    Returns:
        dict with keys: 'date', 'doc_name', 'doc_link', and 'details' (a nested dict),
        or None if the entry is empty.
    """
    result = {
        'date': None,
        'doc_name': None,
        'doc_link': None,
        'details': {}
    }

    # --- Date ---
    date_div = row_soup.find('div', class_='views-field-field-case-document-date')
    if date_div:
        date_span = date_div.find('span', class_='date-display-single')
        if date_span:
            result['date'] = date_span.get('content')

    # --- Document Name & Link ---
    file_div = row_soup.find('div', class_='views-field-field-case-doc-file')
    if file_div:
        ul = file_div.find('ul')
        if ul:
            li = ul.find('li')
            if li:
                a_tag = li.find('a')
                if a_tag:
                    result['doc_name'] = a_tag.get_text(strip=True)
                    result['doc_link'] = a_tag.get('href')

    # --- Fallback: Unlinked document names ---
    if not result['doc_name']:
        no_pdf_div = row_soup.find('div', class_='views-field-field-case-document-no-pdf-')
        if no_pdf_div:
            field_content = no_pdf_div.find('div', class_='field-content')
            if field_content:
                result['doc_name'] = field_content.get_text(strip=True)

    # --- Dynamic Details ---
    details_div = row_soup.find('div', class_='views-field-nothing-1')
    if details_div:
        for item in details_div.find_all('div', class_='views-field'):
            label_tag = item.find('span', class_='views-label')
            content_tag = item.find('div', class_='field-content')
            if label_tag and content_tag:
                key = label_tag.get_text(strip=True).rstrip(':')
                value = content_tag.get_text(strip=True)
                if value != "":
                    result['details'][key] = value
    else:
        result['details'] = {}

    # --- Final filter: return None if the row is "empty" ---
    if all([
        result['date'] is None,
        result['doc_name'] is None,
        result['doc_link'] is None,
        result['details'] == {}
    ]):
        return None

    return result


def attach_documents_to_data_list(data_list):
    """
    Adds a 'documents' key to each item in data_list,
    containing parsed documents using parse_case_document.
    """
    for row in data_list:
        html = row.get('italaw_html')
        documents = []
        
        if html and isinstance(html, str):
            soup = BeautifulSoup(html, 'html.parser')
            doc_rows = soup.find_all('div', class_='views-row')
            
            for doc_row in doc_rows:
                parsed_doc = parse_case_document(doc_row)
                if parsed_doc:
                    documents.append(parsed_doc)

        row['documents'] = documents  # Will be an empty list if no documents found

    return data_list
