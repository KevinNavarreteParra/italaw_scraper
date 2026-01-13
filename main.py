#!/usr/bin/env python3
"""
ITA Law Scraper - Main Pipeline

Usage:
    python main.py                    # Full pipeline from UNCTAD scrape
    python main.py --incremental      # Only update with new documents
    python main.py --unctad           # Scrape UNCTAD for new links, then run pipeline
    python main.py --png-only         # Only run PNG conversion (smart skip)
    python main.py --png-only --force # Force reconvert all PNGs from scratch
    python main.py --test             # Test mode (5 cases only)
    python main.py --skip-download    # Skip PDF download step
    python main.py --skip-png         # Skip PNG conversion step
    python main.py --reserve-cores 2  # Reserve 2 cores (use n-2 for PNG conversion)
"""

import argparse
import json
import os
import sys
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def print_step(step_num, total_steps, message):
    """Print a formatted step message."""
    print(f"\n{'='*60}")
    print(f"Step {step_num}/{total_steps}: {message}")
    print('='*60)


def run_full_pipeline(args):
    """Run the full scraping pipeline from UNCTAD."""
    # Lazy imports to allow --help without all dependencies
    import pandas as pd
    from tqdm import tqdm
    from config import DOCUMENTS_DIR, IMAGES_DIR
    from scraper.unctad_scraper import scrape_all_cases
    from scraper.scrape import (
        fetch_html_for_urls,
        extract_titles,
        extract_case_metadata,
        attach_documents_to_data_list
    )
    from utility.cleaning import flatten_to_document_level, to_snake_case
    from doc_download.download_docs import parallel_download_pdfs
    from utility.pdf_parser import get_page_count, get_adjusted_page_count
    from png_conversion.png_converter import convert_pdf_to_images

    total_steps = 8
    if args.skip_download:
        total_steps -= 1
    if args.skip_png:
        total_steps -= 1

    current_step = 0

    # Step 1: Scrape UNCTAD
    current_step += 1
    print_step(current_step, total_steps, "Scraping UNCTAD for case metadata and ITA Law links")

    end_id = 10 if args.test else 1500
    df = scrape_all_cases(
        start_id=1,
        end_id=end_id,
        delay=1.0,
        save_interval=100,
        output_path='data/unctad_cases.csv'
    )

    print(f"  Total cases fetched: {len(df)}")
    print(f"  Successfully fetched: {(df['fetch_status'] == 'success').sum()}")
    print(f"  Cases with ITA Law link: {df['italaw_link'].notna().sum()}")

    # Step 2: Clean data
    current_step += 1
    print_step(current_step, total_steps, "Cleaning UNCTAD data")

    # Filter to successful fetches and rename columns
    data = df[df['fetch_status'] == 'success'].drop(columns=['fetch_status'])
    data = data.rename(columns={'italaw_link': 'link_to_italaws_case_page'})
    data.to_csv('data/unctad_clean.csv', index=False)

    print(f"  Cleaned cases: {len(data)}")
    print(f"  Cases with ITA Law link: {data['link_to_italaws_case_page'].notna().sum()}")

    # Step 3: Fetch ITA Law HTML
    current_step += 1
    print_step(current_step, total_steps, "Fetching HTML from ITA Law case pages")

    data = fetch_html_for_urls(
        data,
        url_col='link_to_italaws_case_page',
        html_col='italaw_html',
        delay_range=(0.5, 1.5)
    )
    data.to_csv('data/unctad_clean_with_html.csv', index=False)

    print(f"  Pages fetched: {data['italaw_html'].notna().sum()}")

    # Step 4: Extract metadata and documents
    current_step += 1
    print_step(current_step, total_steps, "Extracting case metadata and documents from HTML")

    data = extract_titles(data, html_col='italaw_html', title_col='italaw_title')

    fields = [
        ('italaw_case_type', 'views-field-field-case-type'),
        ('italaw_arbitration_rules', 'views-field-field-arbitration-rules'),
        ('italaw_investment_treaty', 'views-field-field-case-treaties'),
        ('italaw_legal_instruments', 'views-field-field-case-law-text'),
        ('italaw_economic_sector', 'views-field-field-economic-sector')
    ]
    data = extract_case_metadata(data, html_col='italaw_html', fields=fields)

    data_list = data.to_dict(orient='records')
    data_list = attach_documents_to_data_list(data_list)

    with open('data/unctad_clean_with_metadata.json', 'w') as f:
        json.dump(data_list, f, indent=4, ensure_ascii=False)

    total_docs = sum(len(case.get('documents', [])) for case in data_list)
    print(f"  Cases processed: {len(data_list)}")
    print(f"  Total documents found: {total_docs}")

    # Step 5: Flatten to document level
    current_step += 1
    print_step(current_step, total_steps, "Flattening data to document level")

    doc_df = flatten_to_document_level(data_list)

    # Remove HTML column
    if 'italaw_html' in doc_df.columns:
        del doc_df['italaw_html']

    # Create IDs
    doc_df['short_case_name_clean'] = doc_df['short_case_name'].apply(
        lambda x: to_snake_case(x) if pd.notna(x) else None
    )
    doc_df['arbitration_id'] = doc_df.apply(
        lambda row: f"{int(row['year_of_initiation'])}_{row['short_case_name_clean']}"
        if pd.notna(row['year_of_initiation']) and pd.notna(row['short_case_name_clean'])
        else None,
        axis=1
    )
    doc_df['doc_name_clean'] = doc_df['doc_name'].apply(
        lambda x: to_snake_case(x) if pd.notna(x) else None
    )
    doc_df['doc_id'] = doc_df.apply(
        lambda row: f"{row['arbitration_id']}_{row['doc_name_clean']}"
        if pd.notna(row['arbitration_id']) and pd.notna(row['doc_name_clean'])
        else None,
        axis=1
    )

    doc_df.to_csv('data/unctad_document_level_data.csv', index=False, encoding='utf-8-sig')

    print(f"  Total document rows: {len(doc_df)}")
    print(f"  Documents with links: {doc_df['doc_link'].notna().sum()}")

    # Step 6: Download PDFs
    if not args.skip_download:
        current_step += 1
        print_step(current_step, total_steps, "Downloading PDFs")

        os.makedirs(DOCUMENTS_DIR, exist_ok=True)

        results = parallel_download_pdfs(doc_df)

        # Save download results
        with open('data/download_results.json', 'w') as f:
            json.dump(results, f, indent=2)

        from collections import Counter
        status_counts = Counter(r['status'].split(' - ')[0] for r in results)
        for status, count in status_counts.items():
            print(f"  {status}: {count}")

    # Step 7: Extract page counts
    current_step += 1
    print_step(current_step, total_steps, "Extracting PDF page counts")

    doc_df = pd.read_csv('data/unctad_document_level_data.csv')

    print("  Calculating page counts...")
    doc_df['page_count'] = doc_df['doc_id'].apply(get_page_count)
    doc_df['adjusted_page_count'] = doc_df['doc_id'].apply(get_adjusted_page_count)

    doc_df.to_csv('data/unctad_document_level_data.csv', index=False)

    # Create arbitration-level aggregates
    grouped = doc_df.groupby('arbitration_id')

    adj_page_stats = grouped['adjusted_page_count'].agg(
        adj_page_sum='sum',
        adj_page_mean='mean',
        adj_page_var='var',
        adj_page_sd='std',
        adj_page_min='min',
        adj_page_max='max'
    ).reset_index()

    page_stats = grouped['page_count'].agg(
        page_sum='sum',
        page_mean='mean',
        page_var='var',
        page_sd='std',
        page_min='min',
        page_max='max'
    ).reset_index()

    italaw_vars = [col for col in doc_df.columns if col.startswith('italaw_')]
    italaw_info = doc_df.groupby('arbitration_id')[italaw_vars].first().reset_index()

    num_known_docs = doc_df.dropna(subset=['doc_name']).groupby('arbitration_id')['doc_name'].count().reset_index(name='num_known_docs')
    num_avail_docs = doc_df.dropna(subset=['doc_link']).groupby('arbitration_id')['doc_link'].count().reset_index(name='num_avail_docs')

    arb_df = pd.merge(adj_page_stats, italaw_info, on='arbitration_id')
    arb_df = pd.merge(arb_df, page_stats, on='arbitration_id')
    arb_df = pd.merge(arb_df, num_known_docs, on='arbitration_id')
    arb_df = pd.merge(arb_df, num_avail_docs, on='arbitration_id')
    arb_df['pct_avail_docs'] = arb_df['num_avail_docs'] / arb_df['num_known_docs']

    arb_df.to_csv('data/arbitration_level_document_metadata.csv', index=False)

    print(f"  Documents with page counts: {doc_df['page_count'].notna().sum()}")
    print(f"  Arbitration-level records: {len(arb_df)}")

    # Step 8: Convert PDFs to PNG
    if not args.skip_png:
        current_step += 1
        print_step(current_step, total_steps, "Converting PDFs to PNG images")

        os.makedirs(IMAGES_DIR, exist_ok=True)

        pdf_files = [f for f in os.listdir(DOCUMENTS_DIR) if f.lower().endswith('.pdf')]

        if args.test:
            pdf_files = pdf_files[:5]

        num_workers = max(multiprocessing.cpu_count() - args.reserve_cores, 1)
        print(f"  Using {num_workers} workers to process {len(pdf_files)} PDFs")

        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(convert_pdf_to_images, pdf) for pdf in pdf_files]
            for future in tqdm(as_completed(futures), total=len(futures), desc="  Converting"):
                pass  # Results are printed by the function

        print("  PNG conversion complete")

    print("\n" + "="*60)
    print("PIPELINE COMPLETE")
    print("="*60)
    print("  Document-level data: data/unctad_document_level_data.csv")
    print("  Arbitration-level data: data/arbitration_level_document_metadata.csv")
    print(f"  PDFs: {DOCUMENTS_DIR}")
    print(f"  Images: {IMAGES_DIR}")


def run_incremental_pipeline(args):
    """Run the incremental update pipeline."""
    # Lazy imports to allow --help without all dependencies
    import pandas as pd
    from tqdm import tqdm
    from config import DOCUMENTS_DIR, IMAGES_DIR
    from scraper.incremental import (
        load_existing_documents,
        scrape_case_documents,
        compare_documents,
        merge_updates,
        get_missing_pdfs
    )
    from doc_download.download_docs import parallel_download_pdfs
    from utility.pdf_parser import get_page_count, get_adjusted_page_count
    from png_conversion.png_converter import convert_pdf_to_images

    total_steps = 6
    if args.skip_download:
        total_steps -= 1
    if args.skip_png:
        total_steps -= 1

    current_step = 0

    # Step 1: Load existing data
    current_step += 1
    print_step(current_step, total_steps, "Loading existing document data")

    existing_df = load_existing_documents('data/unctad_document_level_data.csv')
    print(f"  Existing documents: {len(existing_df):,}")
    print(f"  Existing cases: {existing_df['arbitration_id'].nunique():,}")

    # Step 2: Get case URLs to scrape
    current_step += 1
    print_step(current_step, total_steps, "Preparing case URLs for scraping")

    case_cols = [
        'year_of_initiation', 'short_case_name', 'full_case_name',
        'link_to_italaws_case_page', 'respondent_state', 'home_state_of_investor'
    ]
    case_urls_df = existing_df[case_cols].drop_duplicates(subset=['link_to_italaws_case_page'])
    case_urls_df = case_urls_df[case_urls_df['link_to_italaws_case_page'].notna()]

    if args.test:
        case_urls_df = case_urls_df.head(5)

    print(f"  Case URLs to scrape: {len(case_urls_df):,}")

    # Step 3: Scrape ITA Law case pages
    current_step += 1
    print_step(current_step, total_steps, "Scraping ITA Law case pages")

    scraped_cases = scrape_case_documents(case_urls_df, delay_range=(0.5, 1.5))
    print(f"  Cases scraped: {len(scraped_cases)}")

    # Step 4: Compare and merge
    current_step += 1
    print_step(current_step, total_steps, "Comparing documents and merging updates")

    comparison = compare_documents(existing_df, scraped_cases)
    print(f"  New documents: {len(comparison['new'])}")
    print(f"  Updated documents: {len(comparison['updated'])}")
    print(f"  Unchanged documents: {len(comparison['unchanged'])}")
    print(f"  New cases: {len(comparison['new_cases'])}")

    updated_df = merge_updates(existing_df, comparison)
    updated_df.to_csv('data/unctad_document_level_data.csv', index=False)
    print(f"  Total documents after merge: {len(updated_df)}")

    # Step 5: Download new PDFs
    if not args.skip_download:
        current_step += 1
        print_step(current_step, total_steps, "Downloading new PDFs")

        os.makedirs(DOCUMENTS_DIR, exist_ok=True)

        missing_pdfs = get_missing_pdfs(updated_df)
        print(f"  PDFs to download: {len(missing_pdfs)}")

        if len(missing_pdfs) > 0:
            results = parallel_download_pdfs(missing_pdfs)

            with open('data/download_results_incremental.json', 'w') as f:
                json.dump(results, f, indent=2)

            from collections import Counter
            status_counts = Counter(r['status'].split(' - ')[0] for r in results)
            for status, count in status_counts.items():
                print(f"  {status}: {count}")
        else:
            print("  No new PDFs to download")

        # Update page counts for new documents
        print("\n  Updating page counts for new documents...")
        updated_df = pd.read_csv('data/unctad_document_level_data.csv')

        # Only update rows missing page counts
        mask = updated_df['page_count'].isna()
        if mask.any():
            updated_df.loc[mask, 'page_count'] = updated_df.loc[mask, 'doc_id'].apply(get_page_count)
            updated_df.loc[mask, 'adjusted_page_count'] = updated_df.loc[mask, 'doc_id'].apply(get_adjusted_page_count)
            updated_df.to_csv('data/unctad_document_level_data.csv', index=False)
            print(f"  Updated page counts for {mask.sum()} documents")

    # Step 6: Convert new PDFs to PNG
    if not args.skip_png:
        current_step += 1
        print_step(current_step, total_steps, "Converting new PDFs to PNG")

        os.makedirs(IMAGES_DIR, exist_ok=True)

        # Get list of PDFs that don't have image folders yet
        pdf_files = [f for f in os.listdir(DOCUMENTS_DIR) if f.lower().endswith('.pdf')]
        new_pdfs = []
        for pdf in pdf_files:
            doc_id = os.path.splitext(pdf)[0]
            if not os.path.exists(os.path.join(IMAGES_DIR, doc_id)):
                new_pdfs.append(pdf)

        if args.test:
            new_pdfs = new_pdfs[:5]

        print(f"  New PDFs to convert: {len(new_pdfs)}")

        if new_pdfs:
            num_workers = max(multiprocessing.cpu_count() - args.reserve_cores, 1)
            print(f"  Using {num_workers} workers")

            with ProcessPoolExecutor(max_workers=num_workers) as executor:
                futures = [executor.submit(convert_pdf_to_images, pdf) for pdf in new_pdfs]
                for future in tqdm(as_completed(futures), total=len(futures), desc="  Converting"):
                    pass
        else:
            print("  No new PDFs to convert")

    print("\n" + "="*60)
    print("INCREMENTAL UPDATE COMPLETE")
    print("="*60)
    print(f"  New documents added: {len(comparison['new'])}")
    print(f"  Metadata updated: {len(comparison['updated'])}")


def run_unctad_pipeline(args):
    """Run pipeline starting from fresh UNCTAD scrape, filtering to new ITA Law links."""
    # Lazy imports to allow --help without all dependencies
    import pandas as pd
    from tqdm import tqdm
    from config import DOCUMENTS_DIR, IMAGES_DIR
    from scraper.unctad_scraper import scrape_all_cases
    from scraper.incremental import (
        load_existing_documents,
        scrape_case_documents,
        compare_documents,
        merge_updates,
        get_missing_pdfs
    )
    from doc_download.download_docs import parallel_download_pdfs
    from utility.pdf_parser import get_page_count, get_adjusted_page_count
    from png_conversion.png_converter import convert_pdf_to_images

    total_steps = 7
    if args.skip_download:
        total_steps -= 1
    if args.skip_png:
        total_steps -= 1

    current_step = 0

    # Step 1: Load existing data to find already-scraped URLs
    current_step += 1
    print_step(current_step, total_steps, "Loading existing document data")

    existing_csv = 'data/unctad_document_level_data.csv'
    if os.path.exists(existing_csv):
        existing_df = load_existing_documents(existing_csv)
        existing_urls = set(existing_df['link_to_italaws_case_page'].dropna().unique())
        print(f"  Existing documents: {len(existing_df):,}")
        print(f"  Existing ITA Law URLs: {len(existing_urls):,}")
    else:
        existing_df = pd.DataFrame()
        existing_urls = set()
        print("  No existing data found - will scrape all links")

    # Step 2: Scrape UNCTAD for fresh case data
    current_step += 1
    print_step(current_step, total_steps, "Scraping UNCTAD for case metadata and ITA Law links")

    end_id = 10 if args.test else 1500
    unctad_df = scrape_all_cases(
        start_id=1,
        end_id=end_id,
        delay=1.0,
        save_interval=100,
        output_path='data/unctad_cases.csv'
    )

    print(f"  Total cases fetched: {len(unctad_df)}")
    print(f"  Successfully fetched: {(unctad_df['fetch_status'] == 'success').sum()}")
    print(f"  Cases with ITA Law link: {unctad_df['italaw_link'].notna().sum()}")

    # Step 3: Filter to new ITA Law links only
    current_step += 1
    print_step(current_step, total_steps, "Filtering to new ITA Law links")

    # Clean UNCTAD data
    unctad_clean = unctad_df[unctad_df['fetch_status'] == 'success'].drop(columns=['fetch_status'])
    unctad_clean = unctad_clean.rename(columns={'italaw_link': 'link_to_italaws_case_page'})

    # Filter to cases with ITA Law links that haven't been scraped yet
    has_italaw_link = unctad_clean['link_to_italaws_case_page'].notna()
    is_new_url = ~unctad_clean['link_to_italaws_case_page'].isin(existing_urls)
    new_cases_df = unctad_clean[has_italaw_link & is_new_url].copy()

    # Also need case metadata for scraping - rename investor_nationality to home_state_of_investor
    if 'investor_nationality' in new_cases_df.columns:
        new_cases_df = new_cases_df.rename(columns={'investor_nationality': 'home_state_of_investor'})

    if args.test:
        new_cases_df = new_cases_df.head(5)

    print(f"  Total ITA Law links from UNCTAD: {has_italaw_link.sum()}")
    print(f"  Already scraped: {(has_italaw_link & ~is_new_url).sum()}")
    print(f"  New links to scrape: {len(new_cases_df)}")

    if len(new_cases_df) == 0:
        print("\n" + "="*60)
        print("UNCTAD PIPELINE COMPLETE - NO NEW LINKS")
        print("="*60)
        print("  No new ITA Law links found in UNCTAD data.")
        return

    # Step 4: Scrape ITA Law case pages for new links
    current_step += 1
    print_step(current_step, total_steps, "Scraping ITA Law case pages")

    scraped_cases = scrape_case_documents(new_cases_df, delay_range=(0.5, 1.5))
    print(f"  Cases scraped: {len(scraped_cases)}")

    # Step 5: Compare and merge
    current_step += 1
    print_step(current_step, total_steps, "Comparing documents and merging updates")

    if len(existing_df) > 0:
        comparison = compare_documents(existing_df, scraped_cases)
        print(f"  New documents: {len(comparison['new'])}")
        print(f"  Updated documents: {len(comparison['updated'])}")
        print(f"  Unchanged documents: {len(comparison['unchanged'])}")
        print(f"  New cases: {len(comparison['new_cases'])}")

        updated_df = merge_updates(existing_df, comparison)
    else:
        # No existing data - flatten scraped cases directly
        from utility.cleaning import flatten_to_document_level, to_snake_case
        updated_df = flatten_to_document_level(scraped_cases)

        # Remove HTML column
        if 'italaw_html' in updated_df.columns:
            del updated_df['italaw_html']

        # Create IDs
        updated_df['short_case_name_clean'] = updated_df['short_case_name'].apply(
            lambda x: to_snake_case(x) if pd.notna(x) else None
        )
        updated_df['arbitration_id'] = updated_df.apply(
            lambda row: f"{int(row['year_of_initiation'])}_{row['short_case_name_clean']}"
            if pd.notna(row['year_of_initiation']) and pd.notna(row['short_case_name_clean'])
            else None,
            axis=1
        )
        updated_df['doc_name_clean'] = updated_df['doc_name'].apply(
            lambda x: to_snake_case(x) if pd.notna(x) else None
        )
        updated_df['doc_id'] = updated_df.apply(
            lambda row: f"{row['arbitration_id']}_{row['doc_name_clean']}"
            if pd.notna(row['arbitration_id']) and pd.notna(row['doc_name_clean'])
            else None,
            axis=1
        )
        comparison = {'new': updated_df.to_dict('records'), 'updated': []}

    updated_df.to_csv('data/unctad_document_level_data.csv', index=False)
    print(f"  Total documents after merge: {len(updated_df)}")

    # Step 6: Download new PDFs
    if not args.skip_download:
        current_step += 1
        print_step(current_step, total_steps, "Downloading new PDFs")

        os.makedirs(DOCUMENTS_DIR, exist_ok=True)

        missing_pdfs = get_missing_pdfs(updated_df)
        print(f"  PDFs to download: {len(missing_pdfs)}")

        if len(missing_pdfs) > 0:
            results = parallel_download_pdfs(missing_pdfs)

            with open('data/download_results_unctad.json', 'w') as f:
                json.dump(results, f, indent=2)

            from collections import Counter
            status_counts = Counter(r['status'].split(' - ')[0] for r in results)
            for status, count in status_counts.items():
                print(f"  {status}: {count}")
        else:
            print("  No new PDFs to download")

        # Update page counts for new documents
        print("\n  Updating page counts for new documents...")
        updated_df = pd.read_csv('data/unctad_document_level_data.csv')

        # Only update rows missing page counts
        mask = updated_df['page_count'].isna()
        if mask.any():
            updated_df.loc[mask, 'page_count'] = updated_df.loc[mask, 'doc_id'].apply(get_page_count)
            updated_df.loc[mask, 'adjusted_page_count'] = updated_df.loc[mask, 'doc_id'].apply(get_adjusted_page_count)
            updated_df.to_csv('data/unctad_document_level_data.csv', index=False)
            print(f"  Updated page counts for {mask.sum()} documents")

    # Step 7: Convert new PDFs to PNG
    if not args.skip_png:
        current_step += 1
        print_step(current_step, total_steps, "Converting new PDFs to PNG")

        os.makedirs(IMAGES_DIR, exist_ok=True)

        # Get list of PDFs that don't have image folders yet
        pdf_files = [f for f in os.listdir(DOCUMENTS_DIR) if f.lower().endswith('.pdf')]
        new_pdfs = []
        for pdf in pdf_files:
            doc_id = os.path.splitext(pdf)[0]
            if not os.path.exists(os.path.join(IMAGES_DIR, doc_id)):
                new_pdfs.append(pdf)

        if args.test:
            new_pdfs = new_pdfs[:5]

        print(f"  New PDFs to convert: {len(new_pdfs)}")

        if new_pdfs:
            num_workers = max(multiprocessing.cpu_count() - args.reserve_cores, 1)
            print(f"  Using {num_workers} workers")

            with ProcessPoolExecutor(max_workers=num_workers) as executor:
                futures = [executor.submit(convert_pdf_to_images, pdf) for pdf in new_pdfs]
                for future in tqdm(as_completed(futures), total=len(futures), desc="  Converting"):
                    pass
        else:
            print("  No new PDFs to convert")

    print("\n" + "="*60)
    print("UNCTAD PIPELINE COMPLETE")
    print("="*60)
    print(f"  New documents added: {len(comparison['new'])}")
    print(f"  Metadata updated: {len(comparison['updated'])}")


def get_conversion_status(doc_id, images_dir, page_count):
    """
    Check if a document needs PNG conversion.

    Returns: 'skip', 'convert', or 'reconvert'
    """
    folder = os.path.join(images_dir, doc_id)
    if not os.path.exists(folder):
        return 'convert'

    # Count PNG files in folder
    png_count = len([f for f in os.listdir(folder) if f.endswith('.png')])

    if page_count is not None and png_count == int(page_count):
        return 'skip'
    else:
        return 'reconvert'  # Partial or mismatched


def run_png_only_pipeline(args):
    """Run only the PNG conversion step with smart skip/reconvert logic."""
    import shutil
    import pandas as pd
    import math
    from tqdm import tqdm
    from config import DOCUMENTS_DIR, IMAGES_DIR
    from png_conversion.png_converter import convert_pdf_to_images
    from utility.pdf_parser import get_page_count

    # Get all PDFs
    os.makedirs(IMAGES_DIR, exist_ok=True)
    pdf_files = [f for f in os.listdir(DOCUMENTS_DIR) if f.lower().endswith('.pdf')]

    if args.test:
        pdf_files = pdf_files[:5]

    # Force mode: skip all checking, convert everything
    if args.force:
        print_step(1, 3, "Force mode: preparing to reconvert all PDFs")
        print(f"  Total PDFs found: {len(pdf_files)}")

        to_skip = []
        to_convert = []
        to_reconvert = pdf_files  # Mark all as reconvert

        print(f"\n  Force mode: will reconvert all {len(pdf_files)} PDFs")

    else:
        print_step(1, 3, "Analyzing documents for PNG conversion")

        # Load page counts from CSV
        csv_path = 'data/unctad_document_level_data.csv'
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path, usecols=['doc_id', 'page_count'])
            page_counts = dict(zip(df['doc_id'], df['page_count']))
            print(f"  Loaded page counts for {len(page_counts):,} documents from CSV")
        else:
            page_counts = {}
            print("  Warning: CSV not found, will calculate page counts from PDFs")

        print(f"  Total PDFs found: {len(pdf_files)}")

        # Categorize each PDF
        to_skip = []
        to_convert = []
        to_reconvert = []

        for pdf_file in tqdm(pdf_files, desc="  Checking status"):
            doc_id = os.path.splitext(pdf_file)[0]
            page_count = page_counts.get(doc_id)

            # Fallback: calculate from PDF if CSV value is missing/NaN
            if page_count is None or (isinstance(page_count, float) and math.isnan(page_count)):
                page_count = get_page_count(doc_id)

            status = get_conversion_status(doc_id, IMAGES_DIR, page_count)

            if status == 'skip':
                to_skip.append(pdf_file)
            elif status == 'convert':
                to_convert.append(pdf_file)
            else:  # reconvert
                to_reconvert.append(pdf_file)

        print(f"\n  Already complete (skipping): {len(to_skip)}")
        print(f"  New (need conversion): {len(to_convert)}")
        print(f"  Partial/mismatched (need reconversion): {len(to_reconvert)}")

    # Delete folders for reconversion
    if to_reconvert:
        print_step(2, 3, "Cleaning up partial conversions")
        for pdf_file in tqdm(to_reconvert, desc="  Removing folders"):
            doc_id = os.path.splitext(pdf_file)[0]
            folder = os.path.join(IMAGES_DIR, doc_id)
            if os.path.exists(folder):
                shutil.rmtree(folder)
        print(f"  Removed {len(to_reconvert)} incomplete folders")
    else:
        print_step(2, 3, "No partial conversions to clean up")

    # Combine for conversion
    pdfs_to_process = to_convert + to_reconvert

    if not pdfs_to_process:
        print_step(3, 3, "No PDFs to convert")
        print("\n" + "="*60)
        print("PNG ONLY PIPELINE COMPLETE")
        print("="*60)
        print(f"  Skipped (already complete): {len(to_skip)}")
        print(f"  Converted: 0")
        print(f"  Reconverted: 0")
        return

    print_step(3, 3, f"Converting {len(pdfs_to_process)} PDFs to PNG")

    num_workers = max(multiprocessing.cpu_count() - args.reserve_cores, 1)
    print(f"  Using {num_workers} workers ({multiprocessing.cpu_count()} cores - {args.reserve_cores} reserved)")

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(convert_pdf_to_images, pdf) for pdf in pdfs_to_process]
        for future in tqdm(as_completed(futures), total=len(futures), desc="  Converting"):
            pass

    print("\n" + "="*60)
    print("PNG ONLY PIPELINE COMPLETE")
    print("="*60)
    print(f"  Skipped (already complete): {len(to_skip)}")
    print(f"  Converted (new): {len(to_convert)}")
    print(f"  Reconverted (was partial): {len(to_reconvert)}")


def main():
    parser = argparse.ArgumentParser(
        description='ITA Law Scraper - Full pipeline or incremental updates',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python main.py                    # Full pipeline from UNCTAD scrape
    python main.py --incremental      # Update with new documents only
    python main.py --unctad           # Scrape UNCTAD for new links, then run pipeline
    python main.py --png-only         # Only run PNG conversion (smart skip)
    python main.py --png-only --force # Force reconvert all PNGs from scratch
    python main.py --png-only --force --reserve-cores 2  # Full reconvert with 2 cores reserved
    python main.py --test             # Test mode (5 cases)
    python main.py --skip-download    # Skip PDF download
    python main.py --skip-png         # Skip PNG conversion
        """
    )

    parser.add_argument(
        '--incremental', '-i',
        action='store_true',
        help='Run incremental update instead of full pipeline'
    )
    parser.add_argument(
        '--unctad', '-u',
        action='store_true',
        help='Scrape UNCTAD for new ITA Law links, then run pipeline on new links only'
    )
    parser.add_argument(
        '--test', '-t',
        action='store_true',
        help='Test mode: only process 5 cases'
    )
    parser.add_argument(
        '--skip-download',
        action='store_true',
        help='Skip PDF download step'
    )
    parser.add_argument(
        '--skip-png',
        action='store_true',
        help='Skip PNG conversion step'
    )
    parser.add_argument(
        '--png-only',
        action='store_true',
        help='Only run PNG conversion step (skips scraping and downloads)'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force full reconversion (use with --png-only to remake all PNGs from scratch)'
    )
    parser.add_argument(
        '--reserve-cores',
        type=int,
        default=1,
        metavar='K',
        help='Number of CPU cores to reserve (uses n-k cores). Default: 1'
    )

    args = parser.parse_args()

    # Check for mutually exclusive flags
    mode_flags = sum([args.incremental, args.unctad, args.png_only])
    if mode_flags > 1:
        parser.error("--incremental, --unctad, and --png-only cannot be used together")

    # Import config after parsing args (lazy import for --help)
    from config import DOCUMENTS_DIR, IMAGES_DIR

    # Ensure data directory exists
    os.makedirs('data', exist_ok=True)

    # Determine mode string
    if args.png_only:
        mode_str = 'PNG Only'
    elif args.unctad:
        mode_str = 'UNCTAD (new links only)'
    elif args.incremental:
        mode_str = 'Incremental'
    else:
        mode_str = 'Full Pipeline'

    print("="*60)
    print("ITA LAW SCRAPER")
    print("="*60)
    print(f"Mode: {mode_str}")
    print(f"Test mode: {args.test}")
    print(f"Skip download: {args.skip_download}")
    print(f"Skip PNG: {args.skip_png}")
    print(f"Reserve cores: {args.reserve_cores}")
    print(f"Documents dir: {DOCUMENTS_DIR}")
    print(f"Images dir: {IMAGES_DIR}")

    try:
        if args.png_only:
            run_png_only_pipeline(args)
        elif args.unctad:
            run_unctad_pipeline(args)
        elif args.incremental:
            run_incremental_pipeline(args)
        else:
            run_full_pipeline(args)
    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nError: {e}")
        raise


if __name__ == '__main__':
    main()
