import fitz
import os

def get_adjusted_page_count(doc_id):
    pdf_path = f"documents/{doc_id}.pdf"
    if not os.path.exists(pdf_path):
        return None

    try:
        doc = fitz.open(pdf_path)
        count = 0
        for page in doc:
            width, height = page.rect.width, page.rect.height
            count += 2 if width > height else 1
        return count
    except Exception:
        return None

def get_page_count(doc_id):
    pdf_path = f"documents/{doc_id}.pdf"
    if not os.path.exists(pdf_path):
        return None

    try:
        doc = fitz.open(pdf_path)
        return doc.page_count
    except Exception:
        return None

def is_machine_readable(pdf_path, check_pages=5):
    doc = fitz.open(pdf_path)
    doc_length = fitz.page_count(doc)
    for page in doc[:check_pages]:
        text = page.get_text()
        if text and text.strip():
            return True
    return False

