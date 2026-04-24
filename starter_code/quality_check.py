# ==========================================
# ROLE 3: OBSERVABILITY & QA ENGINEER
# ==========================================

import re

# ==========================================
# ROLE 3: OBSERVABILITY & QA ENGINEER
# ==========================================

# Toxic patterns — use word boundaries to avoid false positives (e.g. "Da Nang")
TOXIC_PATTERNS = [
    r'\bnull pointer exception\b',
    r'\bsegmentation fault\b',
    r'\btraceback \(most recent call last\)',
    r'\bfatal error\b',
    r'\bundefined reference\b',
    r'\baccess violation\b',
    r'\bstack overflow\b',
    r'\bout of memory\b',
    r'\bnan\b',   # standalone NaN only
    r'\binf\b',   # standalone Inf only
]

def run_quality_gate(document_dict: dict) -> bool:
    """Returns True if document passes all quality checks, False otherwise."""
    content = document_dict.get("content", "")

    # Gate 1: Minimum content length
    if len(content.strip()) < 20:
        print(f"  [QA REJECT] document_id={document_dict.get('document_id')} — content too short ({len(content)} chars)")
        return False

    # Gate 2: Toxic / error strings (word-boundary aware)
    content_lower = content.lower()
    for pattern in TOXIC_PATTERNS:
        if re.search(pattern, content_lower):
            print(f"  [QA REJECT] document_id={document_dict.get('document_id')} — toxic pattern: '{pattern}'")
            return False

    # Gate 3: source_type must be set
    if not document_dict.get("source_type"):
        print(f"  [QA REJECT] document_id={document_dict.get('document_id')} — missing source_type")
        return False

    # Gate 4: Flag discrepancies from legacy code (log only, don't reject)
    discrepancies = document_dict.get("source_metadata", {}).get("discrepancies", [])
    for d in discrepancies:
        print(f"  [QA FLAG] {document_dict.get('document_id')}: {d}")

    return True
