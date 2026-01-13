# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a scraper for ITA Law (Investment Treaty Arbitration) that extracts ISDS (Investor-State Dispute Settlement) case documents and metadata. The scraper uses the UNCTAD dataset as its source of ITA Law URLs.

## Running the Pipeline

The scraper is run through numbered Jupyter notebooks that must be executed sequentially:

```bash
source .venv/bin/activate  # Activate the virtual environment first
```

1. `01_clean_data.ipynb` - Clean the raw UNCTAD Excel data
2. `02_scrape_ita_links.ipynb` - Fetch HTML from ITA Law URLs
3. `03_pull_data_from_pages.ipynb` - Extract metadata from HTML
4. `04_make_long_data.ipynb` - Flatten to document-level data
5. `05_download_docs.ipynb` - Download PDF documents
6. `06_get_pdf_metadata.ipynb` - Extract page counts from PDFs
7. `07_convert_pdf_to_png.ipynb` - Convert PDFs to PNG images

## Architecture

### Directory Structure
- `scraper/` - HTML fetching and parsing (`scrape.py`)
- `utility/` - Data cleaning (`cleaning.py`), PDF parsing (`pdf_parser.py`), plotting (`plot_utility.py`)
- `doc_download/` - Parallel PDF downloader (`download_docs.py`)
- `png_conversion/` - PDF to PNG conversion (`png_converter.py`)
- `data/` - Input/output data files
- `documents/` - Downloaded PDFs (gitignored)
- `images/` - Converted PNGs (gitignored)

### Key Dependencies
- `requests` + `BeautifulSoup` for scraping
- `pandas` for data manipulation
- `PyMuPDF` (`fitz`) for PDF metadata extraction
- `pdf2image` for PDF to PNG conversion
- `tqdm` for progress bars

### Data Flow
1. UNCTAD Excel (`data/UNCTAD-*.xlsx`) contains ITA Law URLs
2. HTML is fetched and stored in CSV/JSON with the data
3. Documents are parsed into arbitration-level then flattened to document-level
4. PDFs are downloaded in parallel with polite delays
5. Page counts calculated (with adjusted count treating landscape pages as 2)

### Configuration
`config.py` defines `DROPBOX_ROOT` and `LOCAL_CACHE` paths. This file is gitignored.

### Output Files
- `unctad_document_level_data.csv` - One row per document (primary output)
- `arbitration_level_document_metadata.csv` - One row per case
- `download_results.json` - Download status log
