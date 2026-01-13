# import requests
# import os
# import pandas as pd

# def download_documents(df, link_col='doc_link', id_col='doc_id', folder='documents'):
#     """
#     Downloads PDFs from doc_link and saves them using doc_id as the filename.

#     Parameters:
#         df (pd.DataFrame): The DataFrame containing doc_link and doc_id.
#         link_col (str): Name of the column with the PDF URLs.
#         id_col (str): Name of the column with the unique document IDs.
#         folder (str): Folder where the PDFs will be saved.

#     Returns:
#         None
#     """
#     os.makedirs(folder, exist_ok=True)
    
#     for idx, row in df.iterrows():
#         doc_link = row.get(link_col)
#         doc_id = row.get(id_col)

#         if pd.isna(doc_link) or pd.isna(doc_id):
#             continue

#         filename = f"{folder}/{doc_id}.pdf"
        
#         if os.path.exists(filename):
#             continue  # Skip if already downloaded

#         try:
#             response = requests.get(doc_link, timeout=30)
#             if response.status_code == 200 and 'application/pdf' in response.headers.get('Content-Type', ''):
#                 with open(filename, 'wb') as f:
#                     f.write(response.content)
#                 print(f"Downloaded: {filename}")
#             else:
#                 print(f"Skipped (non-PDF or error): {doc_link}")
#         except Exception as e:
#             print(f"Error downloading {doc_link}: {e}")

import os
import requests
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import cpu_count
from tqdm import tqdm
import pandas as pd

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DOCUMENTS_DIR

# Make sure the output folder exists
os.makedirs(DOCUMENTS_DIR, exist_ok=True)

def download_pdf(doc):
    doc_id = doc.get("doc_id")
    url = doc.get("doc_link")

    if not doc_id or not url or pd.isna(url):
        return {"doc_id": doc_id, "status": "skipped - missing"}

    filename = os.path.join(DOCUMENTS_DIR, f"{doc_id}.pdf")

    try:
        headers = {
            "User-Agent": "Mozilla/5.0"
        }
        response = requests.get(url, headers=headers, timeout=15)

        if response.status_code == 200 and response.headers.get("Content-Type", "").lower().startswith("application/pdf"):
            with open(filename, "wb") as f:
                f.write(response.content)
            # polite delay
            time.sleep(random.uniform(0.5, 1.5))
            return {"doc_id": doc_id, "status": "downloaded"}
        else:
            return {"doc_id": doc_id, "status": f"failed - status {response.status_code}"}

    except Exception as e:
        return {"doc_id": doc_id, "status": f"error - {str(e)}"}

def parallel_download_pdfs(flat_df):
    documents = flat_df[["doc_id", "doc_link"]].dropna(subset=["doc_link"]).to_dict(orient="records")

    max_workers = max(cpu_count() - 1, 1)
    print(f"Downloading PDFs with {max_workers} threads...")

    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(download_pdf, doc) for doc in documents]

        for future in tqdm(as_completed(futures), total=len(futures)):
            results.append(future.result())

    return results
