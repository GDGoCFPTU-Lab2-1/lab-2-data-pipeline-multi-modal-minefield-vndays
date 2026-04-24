# Lab 2: Data Pipeline Multi-Modal Minefield

## Team Members

| Họ và Tên | MSV | Email | GitHub Username | Vai trò |
|-----------|-----|-------|-----------------|---------|
| Nguyễn Văn Lĩnh | 2A202600412 | linhnv.4dev@gmail.com | @linhnv4dev | Full Stack (Role 1 + 2 + 3 + 4) |

---

## Cách chạy

```bash
# 1. Cài dependencies
pip install -r requirements.txt

# 2. Set API key
export GEMINI_API_KEY="your_key_here"

# 3. Chạy pipeline
python starter_code/orchestrator.py

# 4. Chạy forensic agent để chấm điểm
python forensic_agent/agent_forensic.py
```

---

## Cấu trúc dự án

```
├── raw_data/               # Dữ liệu nguồn (không chỉnh sửa)
│   ├── lecture_notes.pdf
│   ├── demo_transcript.txt
│   ├── product_catalog.html
│   ├── sales_records.csv
│   └── legacy_pipeline.py
├── starter_code/           # Code pipeline
│   ├── schema.py           # Role 1: Data Contract (UnifiedDocument)
│   ├── process_pdf.py      # Role 2: Gemini API PDF extraction
│   ├── process_transcript.py # Role 2: Transcript cleaning
│   ├── process_html.py     # Role 2: HTML table parsing
│   ├── process_csv.py      # Role 2: CSV cleaning
│   ├── process_legacy_code.py # Role 2: AST docstring extraction
│   ├── quality_check.py    # Role 3: Semantic quality gates
│   └── orchestrator.py     # Role 4: DAG orchestration
├── forensic_agent/
│   └── agent_forensic.py   # Automated scoring
├── processed_knowledge_base.json  # Output (generated)
└── requirements.txt
```

---

## Ghi chú kỹ thuật

- **CSV**: Loại bỏ duplicate theo `id`, chuẩn hóa giá (xử lý `$1200`, `five dollars`, `Liên hệ`), chuẩn hóa ngày về `YYYY-MM-DD`.
- **Transcript**: Xóa timestamp `[00:00:00]`, noise tokens `[Music]`, `[inaudible]`. Phát hiện giá tiếng Việt "năm trăm nghìn" → `500000`.
- **HTML**: Chỉ lấy bảng `#main-catalog`, bỏ qua nav/footer/ads.
- **Legacy Code**: Dùng `ast` để extract docstrings, regex để phát hiện discrepancy thuế (comment 8% vs code 10%).
- **PDF**: Gemini API với exponential backoff cho lỗi 429.
- **Quality Gate**: Reject content < 20 ký tự, reject toxic strings (`Null pointer exception`, v.v.).
