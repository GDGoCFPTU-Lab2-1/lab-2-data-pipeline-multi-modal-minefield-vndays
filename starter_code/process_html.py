from bs4 import BeautifulSoup
from datetime import datetime
from typing import List

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================

def parse_html_catalog(file_path: str) -> List[dict]:
    with open(file_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')

    table = soup.find('table', id='main-catalog')
    if not table:
        print("Warning: #main-catalog table not found in HTML.")
        return []

    headers = [th.get_text(strip=True) for th in table.find('thead').find_all('th')]
    results = []

    for i, row in enumerate(table.find('tbody').find_all('tr')):
        cells = [td.get_text(strip=True) for td in row.find_all('td')]
        if not cells:
            continue

        row_data = dict(zip(headers, cells))

        # Map Vietnamese headers to English keys
        product_id   = row_data.get('Mã SP', f'SP-{i+1:03d}')
        product_name = row_data.get('Tên sản phẩm', '')
        category     = row_data.get('Danh mục', '')
        raw_price    = row_data.get('Giá niêm yết', '')
        stock        = row_data.get('Tồn kho', '')
        rating       = row_data.get('Đánh giá', '')

        # Normalize price: "28,500,000 VND" -> float, "N/A" / "Liên hệ" -> None
        price_value = None
        currency = None
        if raw_price not in ('N/A', 'Liên hệ', '', None):
            price_str = raw_price.replace(',', '').replace('.', '')
            # Remove currency suffix
            price_str = price_str.replace('VND', '').replace('USD', '').strip()
            try:
                price_value = float(price_str)
                currency = 'VND' if 'VND' in raw_price else 'USD'
            except ValueError:
                price_value = None

        # Normalize stock: negative stock is suspicious but keep it
        try:
            stock_int = int(stock)
        except (ValueError, TypeError):
            stock_int = None

        content = (
            f"Product: {product_name} | Category: {category} | "
            f"Price: {raw_price} | Stock: {stock} | Rating: {rating}"
        )

        results.append({
            "document_id": f"html-{product_id}",
            "content": content,
            "source_type": "HTML",
            "author": "VinShop",
            "timestamp": datetime.now(),
            "source_metadata": {
                "product_id": product_id,
                "product_name": product_name,
                "category": category,
                "price_raw": raw_price,
                "price_value": price_value,
                "currency": currency,
                "stock": stock_int,
                "rating": rating,
            }
        })

    return results
