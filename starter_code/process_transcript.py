import re
from datetime import datetime
from typing import Optional

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================

# Map Vietnamese number words to integers
VIET_NUMBER_MAP = {
    "không": 0, "một": 1, "hai": 2, "ba": 3, "bốn": 4, "năm": 5,
    "sáu": 6, "bảy": 7, "tám": 8, "chín": 9, "mười": 10,
    "mươi": 10, "trăm": 100, "nghìn": 1000, "ngàn": 1000,
    "triệu": 1_000_000, "tỷ": 1_000_000_000,
}

def parse_viet_number(text: str) -> Optional[int]:
    """Parse Vietnamese number words like 'năm trăm nghìn' -> 500000."""
    text = text.lower().strip()
    tokens = text.split()
    result = 0
    current = 0
    for token in tokens:
        val = VIET_NUMBER_MAP.get(token)
        if val is None:
            continue
        if val >= 1000:
            current = current if current > 0 else 1
            result += current * val
            current = 0
        elif val >= 100:
            current = current if current > 0 else 1
            current *= val
        else:
            current += val
    result += current
    return result if result > 0 else None

def clean_transcript(file_path: str) -> dict:
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()

    # Remove timestamps like [00:00:00]
    cleaned = re.sub(r'\[\d{2}:\d{2}:\d{2}\]', '', text)

    # Remove noise tokens: [Music starts], [Music ends], [inaudible], [Laughter], etc.
    cleaned = re.sub(r'\[(?:Music[^\]]*|inaudible|Laughter|Applause|noise[^\]]*)\]', '', cleaned, flags=re.IGNORECASE)

    # Remove speaker labels like [Speaker 1]:
    cleaned = re.sub(r'\[Speaker \d+\]:', '', cleaned)

    # Collapse extra whitespace / blank lines
    cleaned = re.sub(r'\n{2,}', '\n', cleaned)
    cleaned = re.sub(r'[ \t]+', ' ', cleaned).strip()

    # Detect price in Vietnamese words
    # Pattern: "năm trăm nghìn VND" or similar
    price_pattern = re.search(
        r'((?:(?:không|một|hai|ba|bốn|năm|sáu|bảy|tám|chín|mười|mươi|trăm|nghìn|ngàn|triệu|tỷ)\s*)+)VND',
        text, flags=re.IGNORECASE
    )
    detected_price = None
    if price_pattern:
        number_str = price_pattern.group(1).strip()
        detected_price = parse_viet_number(number_str)

    return {
        "document_id": "transcript-001",
        "content": cleaned,
        "source_type": "Video",
        "author": "Unknown",
        "timestamp": datetime.now(),
        "source_metadata": {
            "original_file": file_path,
            "detected_price_vnd": detected_price,
        }
    }
