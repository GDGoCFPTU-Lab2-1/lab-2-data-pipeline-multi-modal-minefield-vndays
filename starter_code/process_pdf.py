import os
import time
from datetime import datetime

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================

def extract_pdf_data(file_path: str) -> dict | None:
    if not os.path.exists(file_path):
        print(f"Warning: PDF not found at {file_path}. Skipping PDF extraction.")
        return None

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("Warning: GEMINI_API_KEY not set. Skipping PDF extraction.")
        return None

    try:
        from google import genai
        from google.genai import types
    except ImportError:
        import google.generativeai as genai_legacy
        return _extract_with_legacy(genai_legacy, file_path, api_key)

    client = genai.Client(api_key=api_key)

    prompt = (
        "You are a document parser. Extract the following from this PDF and respond in plain text:\n"
        "1. Title\n"
        "2. Author\n"
        "3. Main Topics (bullet list)\n"
        "4. Any tables found (describe briefly)\n"
        "5. A 3-sentence summary\n"
        "Format your response clearly with these section headers."
    )

    max_retries = 5
    for attempt in range(max_retries):
        try:
            with open(file_path, 'rb') as f:
                pdf_bytes = f.read()

            response = client.models.generate_content(
                model="gemini-1.5-flash",
                contents=[
                    types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"),
                    prompt,
                ]
            )
            extracted_text = response.text
            break
        except Exception as e:
            err_str = str(e)
            if '429' in err_str or 'quota' in err_str.lower():
                wait = (2 ** attempt) + 1
                print(f"Rate limit hit. Retrying in {wait}s... (attempt {attempt+1}/{max_retries})")
                time.sleep(wait)
                if attempt == max_retries - 1:
                    print("Max retries reached for PDF extraction.")
                    return None
            else:
                print(f"Gemini API error: {e}")
                return None

    return {
        "document_id": "pdf-lecture-notes",
        "content": extracted_text,
        "source_type": "PDF",
        "author": "Unknown",
        "timestamp": datetime.now(),
        "source_metadata": {
            "file": file_path,
            "extraction_method": "gemini-1.5-flash",
        }
    }


def _extract_with_legacy(genai, file_path: str, api_key: str) -> dict | None:
    """Fallback using deprecated google-generativeai package."""
    import time
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-1.5-flash")
    prompt = (
        "Extract Title, Author, Main Topics, any Tables, and a 3-sentence summary "
        "from this PDF. Use clear section headers."
    )
    max_retries = 5
    for attempt in range(max_retries):
        try:
            pdf_file = genai.upload_file(path=file_path)
            response = model.generate_content([pdf_file, prompt])
            return {
                "document_id": "pdf-lecture-notes",
                "content": response.text,
                "source_type": "PDF",
                "author": "Unknown",
                "timestamp": datetime.now(),
                "source_metadata": {"file": file_path, "extraction_method": "gemini-legacy"},
            }
        except Exception as e:
            if '429' in str(e):
                wait = (2 ** attempt) + 1
                time.sleep(wait)
            else:
                print(f"Gemini legacy error: {e}")
                return None
    return None
