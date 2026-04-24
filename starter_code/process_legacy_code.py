import ast
import re
from datetime import datetime
from typing import List

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================

def extract_logic_from_code(file_path: str) -> dict:
    with open(file_path, 'r', encoding='utf-8') as f:
        source_code = f.read()

    tree = ast.parse(source_code)

    rules = []
    discrepancies = []

    # Extract module-level docstring
    module_doc = ast.get_docstring(tree)
    if module_doc:
        rules.append(f"[MODULE] {module_doc.strip()}")

    # Walk all function definitions
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            doc = ast.get_docstring(node)
            if doc:
                rules.append(f"[FUNCTION: {node.name}] {doc.strip()}")

    # Detect discrepancy in legacy_tax_calc via regex on raw source
    # Comment says "code says it does 8%" but actual tax_rate = 0.10
    tax_func_match = re.search(
        r'def legacy_tax_calc.*?(?=\ndef |\Z)', source_code, re.DOTALL
    )
    if tax_func_match:
        func_src = tax_func_match.group(0)
        # Find the percentage mentioned in comments (e.g. "does 8%")
        comment_pct = re.search(r'does\s+(\d+)%', func_src)
        code_rate   = re.search(r'tax_rate\s*=\s*([\d.]+)', func_src)
        if comment_pct and code_rate:
            comment_val = float(comment_pct.group(1)) / 100
            code_val    = float(code_rate.group(1))
            if abs(comment_val - code_val) > 0.001:
                discrepancies.append(
                    f"DISCREPANCY in legacy_tax_calc: comment says {comment_pct.group(1)}% "
                    f"but code uses {code_val*100:.0f}%"
                )

    content = "\n\n".join(rules)
    if discrepancies:
        content += "\n\n[DISCREPANCIES DETECTED]\n" + "\n".join(discrepancies)

    return {
        "document_id": "code-legacy-pipeline",
        "content": content,
        "source_type": "Code",
        "author": "Senior Dev (retired)",
        "timestamp": datetime.now(),
        "source_metadata": {
            "file": file_path,
            "rules_extracted": len(rules),
            "discrepancies": discrepancies,
        }
    }
