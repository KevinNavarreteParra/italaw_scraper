# ITA Law Scraper

This repository contains a scraper for ITA Law, designed to extract the following information:

- **All Known Documents**: All known documents related to Investor-State Dispute Settlement (ISDS) cases. This includes all documents that are available for public download from ITA Law as well as those that are listed but not available for download. Note that there are some cases for which no documents are known to be available. It is also highly likely that there are cases for which unknown documents exist. As such, the documents listed here should be considered a convenience sample of the full set of known documents available from ITA Law.
- **Document Metadata**: Metadata for each document, including the official document name, the date ITA Law lists for the document[^1], and additional details included in the *details* dropdown for the given document.
- **Document Links**: Direct links to the documents hosted on ITA Law, where available.
- **Case Metadata**: Metadata for each case, including the case name, parties involved, and other relevant details provided by ITA Law. In many cases, this data already exists in the UNCTAD data set, but it is included here for completeness.

## A Note on the Collection Process

The scraper was designed to pull the URLs from the UNCTAD dataset, which contains a variable with a link to the ITA Law page for each arbitration. There's a few possible issues with this approach, however. Most importantly, the UNCTAD dataset could be incomplete, leaving out some cases for which ITA Law has documents. In this case, the scraper will systematically miss those cases' documents. Additionally, there were a few broken links in the UNCTAD dataset, and it is unclear whether these are due to issues with UNCTAD or ITA Law. The scraper will skip these cases. In the end, only about four or five cases were skipped due to broken links, but it is nevertheless worth noting and checking down the line to see if these missing cases can be recovered.

You can find the UNCTAD dataset I used in the `data/` directory of this repository. The file is the only XLSX file in that directory. The data in that file is up to date as of December 31st 2023, meaning that it captures the universe of known ISDS arbitrations from 1987 to the end of 2023. As such, the scraper will only scrape documents for cases that were initiated before January 1st, 2024. Updating the scraper to scrape cases initiated after that date is as simple as adding the new ITA Law URLs to the UNCTAD dataset and re-running the scraper.

## Data Format

I've provided the data in a few useful formats. First, PDFs and images are saved to a configurable location (see [Configuration](#configuration) below). Within that location, the `documents/` directory contains all the PDF documents scraped from ITA Law, and the `images/` directory contains PNG conversions of those PDFs. Notice that each document name follows the format `year-of-initiation_case-name_document-name.pdf`. This way, all documents are easily identifiable by their case and document name. This also gets around any issues associated with arbitrations that have the same name but are from different years. This naming convention is also self-sorting, so you'll find that all documents are sorted by year and then alphabetically by case name and document name by default.

The `data/` directory contains two salient files:

- `unctad_document_level_data.csv`: This file is at the arbitration-document level of analysis, meaning that each row corresponds to a single document related to a specific arbitration. Notably, this dataset also includes a row for each arbitration that has no known documents, with the document variables are set to missing. This allows for easy aggregation and merging with datasets at the arbitration level of analysis. Note that this file includes page counts for all publicly available documents, which were calculated in two ways. First, I calculated the page count based on the number of pages in the PDF file itself. Second, I calculated an adjusted page count, treating all landscape pages as two pages and all portrait pages as one page to account for the fact that landscape pages typically contain two "pages" worth of content.
- `arbitration_level_document_metadata.csv`: This file is at the arbitration level of analysis, meaning that each row corresponds to a specific arbitration and includes metadata for all known documents related to that arbitration. A major thing to note is that all of the variables relating to page counts have two versions: a regular and adjusted version. The regular version is the raw page count from the PDF file, while the adjusted version treats landscape pages as two pages and portrait pages as one page.

The remaining files in the `data/` directory can be ignored for most purposes. They're typically intermediate files used in the scraping process. The only exception is the `download_results.json` file, which is a log of the download process, including any documents that failed to download. This file will be useful down the line to see which documents were not successfully downloaded and to retry those downloads if necessary.

## Setup

### Prerequisites

- Python 3.9 or higher
- [uv](https://docs.astral.sh/uv/) - A fast Python package manager

### Installing uv

If you don't have `uv` installed, you can install it with:

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows (PowerShell)
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# Or via pip
pip install uv
```

### Setting Up the Project

1. **Clone the repository:**

```bash
git clone https://github.com/KevinNavarreteParra/italaw_scraper.git
cd italaw_scraper
```

2. **Create a virtual environment and install dependencies:**

```bash
uv venv
```

This creates a `.venv` directory with a fresh Python virtual environment.

3. **Activate the virtual environment:**

```bash
# macOS/Linux
source .venv/bin/activate

# Windows (PowerShell)
.venv\Scripts\activate.ps1

# Windows (Command Prompt)
.venv\Scripts\activate.bat
```

4. **Install dependencies:**

```bash
uv sync
```

This installs all required packages from `pyproject.toml`:
- pandas
- requests
- tqdm
- beautifulsoup4
- pdf2image
- pillow
- seaborn
- matplotlib
- pymupdf

### Additional System Dependency

The `pdf2image` package requires `poppler` to be installed on your system:

```bash
# macOS (Homebrew)
brew install poppler

# Ubuntu/Debian
sudo apt-get install poppler-utils

# Windows
# Download from: https://github.com/osber/poppler-windows/releases
# Add the bin/ folder to your PATH
```

## Running the Scraper

Once setup is complete, you can run the full pipeline:

```bash
python main.py
```

Or with options:

```bash
python main.py --test           # Test mode (5 cases only)
python main.py --incremental    # Only update with new documents
python main.py --skip-download  # Skip PDF download step
python main.py --skip-png       # Skip PNG conversion step
```

Alternatively, you can run the scraper by executing each Jupyter notebook in the `notebooks/` directory in numerical order. The notebooks are designed to be run sequentially and automatically deposit the scraped data in the appropriate directories.

## Configuration

The scraper saves PDF documents and PNG images to a location specified in `config.py`. By default, this is set to a Dropbox folder, but you can change it to any valid directory path.

To configure the output location, edit `config.py` and set the `DROPBOX_ROOT` variable:

```python
DROPBOX_ROOT = '/path/to/your/output/directory'
```

The scraper will automatically create `documents/` and `images/` subdirectories within this path. The path is validated at import time to ensure it is a valid string.

[^1]: At the time of writing, it is not clear what the date listed by ITA Law represents. It could be the date the document was added to the website, the date the document was filed at the relevant tribunal, or some other date. It's worth noting that not all documents have a corresponding date listed.
