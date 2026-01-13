import os
from pdf2image import convert_from_path
from PIL import Image

def convert_pdf_to_images(pdf_file, pdf_dir="documents", output_dir="images"):
    """
    Convert a single PDF to PNGs and save them in a subfolder.
    """
    doc_id = os.path.splitext(pdf_file)[0]
    pdf_path = os.path.join(pdf_dir, pdf_file)
    output_folder = os.path.join(output_dir, doc_id)
    os.makedirs(output_folder, exist_ok=True)

    try:
        pages = convert_from_path(pdf_path)
        for i, page in enumerate(pages, start=1):
            page_number = f"{i:03}"
            output_path = os.path.join(output_folder, f"{doc_id}_{page_number}.png")
            page.save(output_path, "PNG")
        return f"Converted {pdf_file}."
    except Exception as e:
        return f"Error processing {pdf_file}: {e}"