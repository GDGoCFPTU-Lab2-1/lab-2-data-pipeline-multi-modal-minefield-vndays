import pandas as pd
from datetime import datetime
from typing import List
import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================

VIET_PRICE_MAP = {
    "liên hệ": None,
    "n/a": None,
    "null": None,
}

def _parse_price(raw) -> float | None:
    """Convert messy price strings to float. Returns None if unparseable."""
    if pd.isna(raw):
        return None
    s = str(raw).strip().lower()
    if s in VIET_PRICE_MAP:
        return None
    # Remove currency symbols and commas
    s = s.replace('$', '').replace(',', '').strip()
    # Handle written-out English numbers
    word_map = {
        'zero': 0, 'one': 1, 'two': 2, 'three': 3, 'four': 4,
        'five': 5, 'six': 6, 'seven': 7, 'eight': 8, 'nine': 9,
        'ten': 10, 'eleven': 11, 'twelve': 12,
    }
    if s in word_map:
        return float(word_map[s])
    try:
        val = float(s)
        # Reject negative prices
        return val if val >= 0 else None
    except ValueError:
        return None

def _parse_date(raw) -> str | None:
    """Normalize various date formats to YYYY-MM-DD."""
    if pd.isna(raw):
        return None
    s = str(raw).strip()
    formats = [
        '%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d',
        '%B %dth %Y', '%B %dst %Y', '%B %dnd %Y', '%B %drd %Y',
        '%d %b %Y', '%B %d %Y',
    ]
    # Normalize ordinal suffixes: "January 16th 2026" -> "January 16 2026"
    s_clean = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', s)
    for fmt in formats:
        try:
            return datetime.strptime(s_clean, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return s  # Return as-is if unparseable

def process_sales_csv(file_path: str) -> List[dict]:
    df = pd.read_csv(file_path)

    # Remove duplicate rows based on 'id' (keep first occurrence)
    df = df.drop_duplicates(subset=['id'], keep='first')

    results = []
    for _, row in df.iterrows():
        price = _parse_price(row.get('price'))
        date  = _parse_date(row.get('date_of_sale'))

        # Stock: handle empty/NaN
        try:
            stock = int(row['stock_quantity']) if not pd.isna(row.get('stock_quantity')) else None
        except (ValueError, TypeError):
            stock = None

        content = (
            f"Sale record: {row.get('product_name', '')} | "
            f"Category: {row.get('category', '')} | "
            f"Price: {price} {row.get('currency', '')} | "
            f"Date: {date} | Seller: {row.get('seller_id', '')}"
        )

        results.append({
            "document_id": f"csv-{int(row['id'])}",
            "content": content,
            "source_type": "CSV",
            "author": row.get('seller_id', 'Unknown'),
            "timestamp": datetime.now(),
            "source_metadata": {
                "product_name": row.get('product_name'),
                "category": row.get('category'),
                "price": price,
                "currency": row.get('currency'),
                "date_of_sale": date,
                "seller_id": row.get('seller_id'),
                "stock_quantity": stock,
            }
        })

    return results
