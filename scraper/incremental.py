"""
Incremental scraping utilities for ITA Law.

This module provides functions to:
1. Compare newly scraped documents against existing data
2. Identify new documents and metadata changes
3. Merge updates into the existing dataset
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DOCUMENTS_DIR

import pandas as pd
from typing import Dict, List, Set, Tuple, Optional
from tqdm import tqdm

from scraper.scrape import (
    fetch_html_for_urls,
    attach_documents_to_data_list,
    extract_titles,
    extract_case_metadata,
    DEFAULT_USER_AGENT,
    DEFAULT_DELAY_RANGE
)
from utility.cleaning import to_snake_case, flatten_to_document_level


# Metadata fields to compare for detecting changes
# These are the fields that come from scraping, not computed later
METADATA_FIELDS = [
    'doc_date',
    'doc_name',
    'doc_link',
]

# Detail fields are dynamic (detail_Claimant appointee, etc.)
# We'll compare all detail_* columns


def load_existing_documents(csv_path: str) -> pd.DataFrame:
    """
    Load existing document-level data from CSV.

    Parameters:
        csv_path: Path to unctad_document_level_data.csv

    Returns:
        DataFrame with existing document data
    """
    return pd.read_csv(csv_path, low_memory=False)


def get_existing_doc_ids(df: pd.DataFrame) -> Set[str]:
    """Extract set of existing doc_ids from dataframe."""
    return set(df['doc_id'].dropna().unique())


def get_existing_case_urls(df: pd.DataFrame) -> Set[str]:
    """Extract set of existing case page URLs."""
    return set(df['link_to_italaws_case_page'].dropna().unique())


def generate_doc_id(arbitration_id: str, doc_name: str) -> str:
    """Generate a doc_id from arbitration_id and document name."""
    if not doc_name:
        return None
    doc_name_clean = to_snake_case(doc_name)
    return f"{arbitration_id}_{doc_name_clean}"


def generate_arbitration_id(year: int, case_name: str) -> str:
    """Generate an arbitration_id from year and case name."""
    case_name_clean = to_snake_case(case_name)
    return f"{int(year)}_{case_name_clean}"


def extract_detail_columns(df: pd.DataFrame) -> List[str]:
    """Get list of all detail_* columns in a dataframe."""
    return [col for col in df.columns if col.startswith('detail_')]


def compare_metadata(existing_row: pd.Series, new_doc: dict, detail_cols: List[str]) -> bool:
    """
    Compare metadata between existing row and newly scraped document.

    Returns:
        True if metadata differs, False if identical
    """
    # Compare core metadata fields
    for field in METADATA_FIELDS:
        existing_val = existing_row.get(field)
        new_val = new_doc.get(field)

        # Normalize None/NaN
        if pd.isna(existing_val):
            existing_val = None
        if pd.isna(new_val):
            new_val = None

        if existing_val != new_val:
            return True

    # Compare detail fields
    new_details = new_doc.get('details', {})
    for col in detail_cols:
        detail_key = col.replace('detail_', '')
        existing_val = existing_row.get(col)
        new_val = new_details.get(detail_key)

        if pd.isna(existing_val):
            existing_val = None
        if pd.isna(new_val):
            new_val = None

        if existing_val != new_val:
            return True

    return False


def scrape_case_documents(case_urls_df: pd.DataFrame,
                          url_col: str = 'link_to_italaws_case_page',
                          delay_range: tuple = DEFAULT_DELAY_RANGE,
                          user_agent: str = DEFAULT_USER_AGENT) -> List[dict]:
    """
    Scrape all case pages and extract documents.

    Parameters:
        case_urls_df: DataFrame with case URLs
        url_col: Column name containing URLs
        delay_range: Delay range between requests
        user_agent: User agent string

    Returns:
        List of case dicts with 'documents' key containing parsed documents
    """
    # Fetch HTML for all case pages
    df = case_urls_df.copy()
    df = fetch_html_for_urls(
        df,
        url_col=url_col,
        html_col='italaw_html',
        delay_range=delay_range,
        user_agent=user_agent
    )

    # Extract titles and metadata
    df = extract_titles(df, html_col='italaw_html', title_col='italaw_title')

    metadata_fields = [
        ('italaw_case_type', 'field-case-type'),
        ('italaw_arbitration_rules', 'field-arbitration-rules'),
        ('italaw_investment_treaty', 'field-investment-treaty'),
        ('italaw_legal_instruments', 'field-legal-instruments'),
        ('italaw_economic_sector', 'field-economic-sector'),
    ]
    df = extract_case_metadata(df, html_col='italaw_html', fields=metadata_fields)

    # Convert to list of dicts and attach documents
    data_list = df.to_dict('records')
    data_list = attach_documents_to_data_list(data_list)

    return data_list


def compare_documents(existing_df: pd.DataFrame,
                      scraped_cases: List[dict]) -> Dict[str, List]:
    """
    Compare newly scraped documents against existing data.

    Parameters:
        existing_df: Existing document-level DataFrame
        scraped_cases: List of case dicts from scrape_case_documents()

    Returns:
        {
            'new': List of new document dicts (with case metadata attached),
            'updated': List of (doc_id, updated_fields_dict) tuples,
            'unchanged': List of doc_ids that haven't changed,
            'new_cases': List of case URLs that are entirely new
        }
    """
    existing_doc_ids = get_existing_doc_ids(existing_df)
    existing_case_urls = get_existing_case_urls(existing_df)
    detail_cols = extract_detail_columns(existing_df)

    # Index existing data by doc_id for fast lookup
    existing_by_doc_id = existing_df.set_index('doc_id')

    new_docs = []
    updated_docs = []
    unchanged_doc_ids = []
    new_cases = []

    for case in tqdm(scraped_cases, desc="Comparing documents"):
        case_url = case.get('link_to_italaws_case_page')
        year = case.get('year_of_initiation')
        case_name = case.get('short_case_name')

        if not year or not case_name:
            continue

        arbitration_id = generate_arbitration_id(year, case_name)

        # Check if this is an entirely new case
        if case_url and case_url not in existing_case_urls:
            new_cases.append(case_url)

        documents = case.get('documents', [])
        for doc in documents:
            doc_name = doc.get('doc_name')
            if not doc_name:
                continue

            doc_id = generate_doc_id(arbitration_id, doc_name)
            if not doc_id:
                continue

            if doc_id not in existing_doc_ids:
                # New document - attach case metadata
                new_doc = {
                    **{k: v for k, v in case.items() if k != 'documents'},
                    'doc_date': doc.get('date'),
                    'doc_name': doc.get('doc_name'),
                    'doc_link': doc.get('doc_link'),
                    'details': doc.get('details', {}),
                    'arbitration_id': arbitration_id,
                    'doc_id': doc_id,
                }
                new_docs.append(new_doc)
            else:
                # Existing document - check for metadata changes
                existing_row = existing_by_doc_id.loc[doc_id]

                # Handle case where doc_id might have duplicates
                if isinstance(existing_row, pd.DataFrame):
                    existing_row = existing_row.iloc[0]

                doc_dict = {
                    'doc_date': doc.get('date'),
                    'doc_name': doc.get('doc_name'),
                    'doc_link': doc.get('doc_link'),
                    'details': doc.get('details', {}),
                }

                if compare_metadata(existing_row, doc_dict, detail_cols):
                    updated_docs.append((doc_id, doc_dict))
                else:
                    unchanged_doc_ids.append(doc_id)

    return {
        'new': new_docs,
        'updated': updated_docs,
        'unchanged': unchanged_doc_ids,
        'new_cases': new_cases,
    }


def merge_updates(existing_df: pd.DataFrame,
                  comparison_result: Dict[str, List]) -> pd.DataFrame:
    """
    Merge new and updated documents into existing dataframe.

    Parameters:
        existing_df: Existing document-level DataFrame
        comparison_result: Result from compare_documents()

    Returns:
        Updated DataFrame with new documents appended and metadata updated
    """
    df = existing_df.copy()
    new_docs = comparison_result['new']
    updated_docs = comparison_result['updated']

    # Apply updates to existing documents
    if updated_docs:
        df = df.set_index('doc_id')
        for doc_id, updates in tqdm(updated_docs, desc="Updating metadata"):
            if doc_id in df.index:
                # Update core fields
                for field in ['doc_date', 'doc_name', 'doc_link']:
                    if field in updates:
                        df.loc[doc_id, field] = updates[field]

                # Update detail fields
                details = updates.get('details', {})
                for key, value in details.items():
                    col = f'detail_{key}'
                    if col not in df.columns:
                        df[col] = None
                    df.loc[doc_id, col] = value

        df = df.reset_index()

    # Append new documents
    if new_docs:
        new_rows = []
        for doc in new_docs:
            row = {k: v for k, v in doc.items() if k != 'details'}
            row['short_case_name_clean'] = to_snake_case(doc.get('short_case_name', ''))
            row['doc_name_clean'] = to_snake_case(doc.get('doc_name', ''))

            # Add detail columns
            details = doc.get('details', {})
            for key, value in details.items():
                row[f'detail_{key}'] = value

            # Initialize page count columns as None (to be filled later)
            row['page_count'] = None
            row['adjusted_page_count'] = None

            new_rows.append(row)

        new_df = pd.DataFrame(new_rows)

        # Ensure all columns exist in both dataframes
        for col in df.columns:
            if col not in new_df.columns:
                new_df[col] = None
        for col in new_df.columns:
            if col not in df.columns:
                df[col] = None

        df = pd.concat([df, new_df], ignore_index=True)

    return df


def get_missing_pdfs(df: pd.DataFrame, documents_dir: str = None) -> pd.DataFrame:
    """
    Filter to documents that don't have PDFs downloaded yet.

    Parameters:
        df: Document-level DataFrame
        documents_dir: Directory where PDFs are stored (defaults to DOCUMENTS_DIR from config)

    Returns:
        DataFrame filtered to documents missing PDFs
    """
    if documents_dir is None:
        documents_dir = DOCUMENTS_DIR
    def pdf_exists(doc_id):
        if pd.isna(doc_id):
            return True  # Skip rows without doc_id
        pdf_path = os.path.join(documents_dir, f"{doc_id}.pdf")
        return os.path.exists(pdf_path)

    mask = ~df['doc_id'].apply(pdf_exists)
    return df[mask & df['doc_link'].notna()]


def run_incremental_update(existing_csv: str,
                           case_urls_df: pd.DataFrame,
                           output_csv: str = None,
                           delay_range: tuple = DEFAULT_DELAY_RANGE,
                           documents_dir: str = None) -> Dict:
    """
    Run a full incremental update.

    Parameters:
        existing_csv: Path to existing document-level CSV
        case_urls_df: DataFrame with case URLs to scrape
        output_csv: Path to save updated CSV (defaults to existing_csv)
        delay_range: Delay range between requests
        documents_dir: Directory where PDFs are stored (defaults to DOCUMENTS_DIR from config)

    Returns:
        Summary dict with counts and list of documents needing download
    """
    if output_csv is None:
        output_csv = existing_csv
    if documents_dir is None:
        documents_dir = DOCUMENTS_DIR

    print("Loading existing data...")
    existing_df = load_existing_documents(existing_csv)
    print(f"  Existing documents: {len(existing_df)}")

    print("\nScraping case pages...")
    scraped_cases = scrape_case_documents(case_urls_df, delay_range=delay_range)
    print(f"  Cases scraped: {len(scraped_cases)}")

    print("\nComparing documents...")
    comparison = compare_documents(existing_df, scraped_cases)
    print(f"  New documents: {len(comparison['new'])}")
    print(f"  Updated documents: {len(comparison['updated'])}")
    print(f"  Unchanged documents: {len(comparison['unchanged'])}")
    print(f"  New cases: {len(comparison['new_cases'])}")

    print("\nMerging updates...")
    updated_df = merge_updates(existing_df, comparison)
    print(f"  Total documents after merge: {len(updated_df)}")

    print(f"\nSaving to {output_csv}...")
    updated_df.to_csv(output_csv, index=False)

    print("\nChecking for missing PDFs...")
    missing_pdfs = get_missing_pdfs(updated_df, documents_dir)
    print(f"  Documents needing download: {len(missing_pdfs)}")

    return {
        'existing_count': len(existing_df),
        'new_count': len(comparison['new']),
        'updated_count': len(comparison['updated']),
        'unchanged_count': len(comparison['unchanged']),
        'new_cases_count': len(comparison['new_cases']),
        'total_count': len(updated_df),
        'missing_pdfs': missing_pdfs,
        'new_cases': comparison['new_cases'],
    }
