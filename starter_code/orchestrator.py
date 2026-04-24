import json
import time
import os
from datetime import datetime

# Robust path handling
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "raw_data")

# Import role-specific modules
from schema import UnifiedDocument
from process_pdf import extract_pdf_data
from process_transcript import clean_transcript
from process_html import parse_html_catalog
from process_csv import process_sales_csv
from process_legacy_code import extract_logic_from_code
from quality_check import run_quality_gate

# ==========================================
# ROLE 4: DEVOPS & INTEGRATION SPECIALIST
# ==========================================

def serialize_doc(doc: dict) -> dict:
    """Convert datetime objects to ISO strings for JSON serialization."""
    result = {}
    for k, v in doc.items():
        if isinstance(v, datetime):
            result[k] = v.isoformat()
        elif isinstance(v, dict):
            result[k] = serialize_doc(v)
        else:
            result[k] = v
    return result

def main():
    start_time = time.time()
    final_kb = []

    # --- FILE PATHS ---
    pdf_path  = os.path.join(RAW_DATA_DIR, "lecture_notes.pdf")
    trans_path = os.path.join(RAW_DATA_DIR, "demo_transcript.txt")
    html_path  = os.path.join(RAW_DATA_DIR, "product_catalog.html")
    csv_path   = os.path.join(RAW_DATA_DIR, "sales_records.csv")
    code_path  = os.path.join(RAW_DATA_DIR, "legacy_pipeline.py")
    output_path = os.path.join(os.path.dirname(SCRIPT_DIR), "processed_knowledge_base.json")

    # --- 1. PDF (Gemini API) ---
    print("[1/5] Processing PDF...")
    pdf_doc = extract_pdf_data(pdf_path)
    if pdf_doc and run_quality_gate(pdf_doc):
        final_kb.append(pdf_doc)
        print(f"  -> Added: {pdf_doc['document_id']}")

    # --- 2. Transcript ---
    print("[2/5] Processing Transcript...")
    trans_doc = clean_transcript(trans_path)
    if trans_doc and run_quality_gate(trans_doc):
        final_kb.append(trans_doc)
        print(f"  -> Added: {trans_doc['document_id']}")

    # --- 3. HTML ---
    print("[3/5] Processing HTML...")
    html_docs = parse_html_catalog(html_path)
    for doc in html_docs:
        if run_quality_gate(doc):
            final_kb.append(doc)
    print(f"  -> Added {len([d for d in html_docs if run_quality_gate(d)])} HTML products")

    # --- 4. CSV ---
    print("[4/5] Processing CSV...")
    csv_docs = process_sales_csv(csv_path)
    added_csv = 0
    for doc in csv_docs:
        if run_quality_gate(doc):
            final_kb.append(doc)
            added_csv += 1
    print(f"  -> Added {added_csv} CSV records")

    # --- 5. Legacy Code ---
    print("[5/5] Processing Legacy Code...")
    code_doc = extract_logic_from_code(code_path)
    if code_doc and run_quality_gate(code_doc):
        final_kb.append(code_doc)
        print(f"  -> Added: {code_doc['document_id']}")

    # --- Save output ---
    serialized = [serialize_doc(d) for d in final_kb]
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(serialized, f, ensure_ascii=False, indent=2)

    end_time = time.time()
    elapsed = end_time - start_time
    print(f"\nPipeline finished in {elapsed:.2f} seconds.")
    print(f"Total valid documents stored: {len(final_kb)}")
    print(f"Output saved to: {output_path}")

    # SLA check
    if elapsed > 60:
        print("WARNING: Pipeline exceeded 60s SLA!")


if __name__ == "__main__":
    main()
