import pdfplumber

pdf_path = "Reports/test.pdf"

with pdfplumber.open(pdf_path) as pdf:
    for i, page in enumerate(pdf.pages):
        print(f"--- page {i} ---")
        print(page.extract_text())