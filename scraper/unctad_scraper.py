import pandas as pd
import requests
import time
import random
from tqdm import tqdm
from bs4 import BeautifulSoup
from typing import Optional, Dict, Any, List


BASE_URL = "https://investmentpolicy.unctad.org/investment-dispute-settlement/cases/{case_id}/1"
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
}


def fetch_unctad_case(case_id: int, session: requests.Session = None,
                      timeout: int = 15, max_retries: int = 3) -> Optional[str]:
    """
    Fetch HTML for a single UNCTAD case page.

    Parameters:
        case_id: The UNCTAD case ID (1-1500+)
        session: Optional requests.Session for connection pooling
        timeout: Request timeout in seconds
        max_retries: Number of retry attempts with exponential backoff

    Returns:
        HTML content as string, or None if fetch failed
    """
    url = BASE_URL.format(case_id=case_id)
    requester = session if session else requests

    for attempt in range(max_retries):
        try:
            response = requester.get(url, headers=DEFAULT_HEADERS, timeout=timeout)

            if response.status_code == 404:
                return None  # Case doesn't exist

            response.raise_for_status()
            return response.text

        except requests.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                time.sleep(wait_time)
            else:
                print(f"Failed to fetch case {case_id} after {max_retries} attempts: {e}")
                return None

    return None


def extract_italaw_link(soup: BeautifulSoup) -> Optional[str]:
    """
    Extract ITA Law link from the italaw-link-content div.

    Returns:
        ITA Law URL string, or None if not present
    """
    italaw_div = soup.find('div', id='italaw-link-content')
    if italaw_div:
        link = italaw_div.find('a')
        if link:
            return link.get('href')
        # Check if it just says "None"
        text = italaw_div.get_text(strip=True)
        if text.lower() == 'none':
            return None
    return None


def extract_case_name(soup: BeautifulSoup) -> Optional[str]:
    """Extract the case name from the page title or header."""
    # Try the page title first
    title = soup.find('title')
    if title:
        title_text = title.get_text(strip=True)
        # Remove common suffixes like " | UNCTAD Investment Policy Hub"
        if '|' in title_text:
            title_text = title_text.split('|')[0].strip()
        return title_text

    # Fallback to h1 header
    h1 = soup.find('h1')
    if h1:
        return h1.get_text(strip=True)

    return None


def extract_field_from_section(soup: BeautifulSoup, section_id: str) -> Optional[str]:
    """
    Extract text content from a collapsible section by its ID.

    The UNCTAD site uses accordion-style sections with IDs like:
    - applicable-iia-content
    - nationality-content
    - economic-sector-content
    etc.
    """
    section = soup.find('div', id=section_id)
    if section:
        text = section.get_text(separator=' ', strip=True)
        if text and text.lower() != 'none':
            return text
    return None


def extract_labeled_values(section_soup: BeautifulSoup) -> Dict[str, str]:
    """
    Extract label-value pairs from a section that uses label/value structure.

    UNCTAD sections use patterns like:
    <span class="label">Label</span><span class="value">Value</span>
    or just text with labels embedded.
    """
    result = {}
    if not section_soup:
        return result

    # Try to find label/value span pairs
    labels = section_soup.find_all('span', class_='label')
    for label in labels:
        label_text = label.get_text(strip=True).rstrip(':')
        # Value is usually the next sibling or nearby element
        value_span = label.find_next_sibling('span', class_='value')
        if value_span:
            result[label_text] = value_span.get_text(strip=True)

    return result


def extract_unctad_metadata(html: str, case_id: int) -> Dict[str, Any]:
    """
    Extract all available metadata from a UNCTAD case page.

    Parameters:
        html: Raw HTML content of the page
        case_id: The UNCTAD case ID

    Returns:
        Dictionary with extracted metadata fields
    """
    soup = BeautifulSoup(html, 'html.parser')

    result = {
        'unctad_case_id': case_id,
        'case_name': extract_case_name(soup),
        'italaw_link': extract_italaw_link(soup),
        'applicable_iia': None,
        'respondent_state': None,
        'investor_nationality': None,
        'investment_summary': None,
        'economic_sector': None,
        'arbitral_rules': None,
        'administering_institution': None,
        'case_status': None,
        'decisions': None,
        'amount_claimed_usd': None,
        'amount_awarded_usd': None,
        'breaches_alleged': None,
        'breaches_found': None,
        'follow_on_proceedings': None,
    }

    # Simple text extractions
    result['applicable_iia'] = extract_field_from_section(soup, 'applicable-iia-content')
    result['case_status'] = extract_field_from_section(soup, 'status-content')
    result['decisions'] = extract_field_from_section(soup, 'decisions-content')
    result['follow_on_proceedings'] = extract_field_from_section(soup, 'follow-ups-content')

    # Parties section - contains respondent and investor home state
    parties_section = soup.find('div', id='parties-content')
    if parties_section:
        text = parties_section.get_text(strip=True)
        # Format: "Respondent State(s)XHome State(s) of investorY"
        if 'Respondent State' in text:
            parts = text.split('Home State')
            if len(parts) >= 1:
                respondent_part = parts[0].replace('Respondent State(s)', '').strip()
                result['respondent_state'] = respondent_part
            if len(parts) >= 2:
                investor_part = parts[1].replace('(s) of investor', '').strip()
                result['investor_nationality'] = investor_part

    # Summary section
    summary_section = soup.find('div', id='summary-content')
    if summary_section:
        result['investment_summary'] = summary_section.get_text(separator=' ', strip=True)

    # Economic sector - clean up the text
    econ_section = soup.find('div', id='economic-sector-content')
    if econ_section:
        text = econ_section.get_text(separator=' | ', strip=True)
        result['economic_sector'] = text

    # Rules and Institution section
    rules_section = soup.find('div', id='rules-institution-content')
    if rules_section:
        text = rules_section.get_text(strip=True)
        # Format: "Arbitration RulesXAdministering institutionY"
        if 'Arbitration Rules' in text:
            parts = text.split('Administering institution')
            if len(parts) >= 1:
                rules_part = parts[0].replace('Arbitration Rules', '').strip()
                result['arbitral_rules'] = rules_part
            if len(parts) >= 2:
                result['administering_institution'] = parts[1].strip()

    # Amounts section
    amounts_section = soup.find('div', id='amounts-content')
    if amounts_section:
        text = amounts_section.get_text(strip=True)
        # Format: "Claimed by investorX mln USDAwarded by tribunalY mln USD"
        if 'Claimed by investor' in text:
            parts = text.split('Awarded by tribunal')
            if len(parts) >= 1:
                claimed = parts[0].replace('Claimed by investor', '').strip()
                result['amount_claimed_usd'] = claimed
            if len(parts) >= 2:
                result['amount_awarded_usd'] = parts[1].strip()

    # Breaches section - contains both alleged and found
    breaches_section = soup.find('div', id='breaches-content')
    if breaches_section:
        text = breaches_section.get_text(strip=True)
        if 'IIA breaches alleged' in text:
            parts = text.split('IIA breaches found')
            if len(parts) >= 1:
                alleged = parts[0].replace('IIA breaches alleged', '').strip()
                result['breaches_alleged'] = alleged
            if len(parts) >= 2:
                result['breaches_found'] = parts[1].strip()

    return result


def scrape_all_cases(start_id: int = 1, end_id: int = 1500,
                     delay: float = 1.0, save_interval: int = 100,
                     output_path: str = None) -> pd.DataFrame:
    """
    Iterate through UNCTAD case IDs and build a dataset.

    Parameters:
        start_id: First case ID to scrape
        end_id: Last case ID to scrape (inclusive)
        delay: Seconds to wait between requests (be polite!)
        save_interval: Save intermediate results every N cases
        output_path: Path to save intermediate/final CSV results

    Returns:
        DataFrame with scraped case metadata
    """
    results = []
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    not_found_count = 0
    consecutive_not_found = 0

    for case_id in tqdm(range(start_id, end_id + 1), desc="Scraping UNCTAD cases"):
        html = fetch_unctad_case(case_id, session=session)

        if html is None:
            not_found_count += 1
            consecutive_not_found += 1

            # If we hit 50 consecutive 404s, we've likely passed the last case
            if consecutive_not_found >= 50:
                print(f"\n50 consecutive missing cases at ID {case_id}. Stopping early.")
                break

            # Still add a placeholder row so we know we tried this ID
            results.append({
                'unctad_case_id': case_id,
                'case_name': None,
                'italaw_link': None,
                'fetch_status': 'not_found'
            })
        else:
            consecutive_not_found = 0
            metadata = extract_unctad_metadata(html, case_id)
            metadata['fetch_status'] = 'success'
            results.append(metadata)

        # Save intermediate results
        if output_path and len(results) % save_interval == 0:
            pd.DataFrame(results).to_csv(output_path, index=False)

        # Polite delay
        time.sleep(delay + random.uniform(0, 0.5))

    df = pd.DataFrame(results)

    # Final save
    if output_path:
        df.to_csv(output_path, index=False)
        print(f"\nSaved {len(df)} cases to {output_path}")

    # Summary stats
    success_count = len(df[df['fetch_status'] == 'success'])
    has_italaw = df['italaw_link'].notna().sum()
    print(f"\nScraping complete:")
    print(f"  Total cases attempted: {len(df)}")
    print(f"  Successfully fetched: {success_count}")
    print(f"  Cases with ITA Law link: {has_italaw}")
    print(f"  Cases without ITA Law link: {success_count - has_italaw}")

    return df


def scrape_single_case(case_id: int) -> Dict[str, Any]:
    """
    Convenience function to scrape a single case for testing.

    Parameters:
        case_id: The UNCTAD case ID

    Returns:
        Dictionary with case metadata, or empty dict if not found
    """
    html = fetch_unctad_case(case_id)
    if html:
        return extract_unctad_metadata(html, case_id)
    return {'unctad_case_id': case_id, 'fetch_status': 'not_found'}
