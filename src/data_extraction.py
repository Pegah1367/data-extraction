# src/data_extraction.py
from __future__ import annotations
from typing import Any, Dict, Optional, Union
import json
import pandas as pd
import requests

# --- CSV ---
def load_csv(path: str, **read_csv_kwargs) -> pd.DataFrame:
    return pd.read_csv(path, **read_csv_kwargs)

# --- Excel ---
def load_excel(path: str, sheet_name: Union[int, str] = 0, **read_excel_kwargs) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=sheet_name, **read_excel_kwargs)

# --- API ---
def load_api(url: str, params: Optional[Dict[str, Any]] = None,
             headers: Optional[Dict[str, str]] = None, timeout: int = 30) -> pd.DataFrame:
    r = requests.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    return pd.json_normalize(data) if isinstance(data, (list, dict)) else pd.DataFrame(data)

# --- JSON file ---
def load_json(path: str) -> pd.DataFrame:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return pd.json_normalize(data) if isinstance(data, (list, dict)) else pd.DataFrame(data)

# --- MongoDB ---
def load_mongodb(uri: str, db_name: str, collection_name: str,
                 query: Optional[Dict[str, Any]] = None,
                 projection: Optional[Dict[str, int]] = None,
                 limit: Optional[int] = None) -> pd.DataFrame:
    from pymongo import MongoClient
    client = MongoClient(uri)
    try:
        cur = client[db_name][collection_name].find(query or {}, projection or {})
        if limit: cur = cur.limit(int(limit))
        docs = list(cur)
        if docs and "_id" in docs[0]:
            for d in docs: d["_id"] = str(d["_id"])
        return pd.DataFrame(docs)
    finally:
        client.close()

# --- MySQL ---
def load_mysql(host: str, user: str, password: str, database: str,
               query: str, port: int = 3306) -> pd.DataFrame:
    import mysql.connector
    conn = mysql.connector.connect(host=host, user=user, password=password,
                                   database=database, port=port)
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()

# --- Web scraping (WooCommerce-like demo) ---
def scrape_shop(base_url: str, product_selector: str = ".product",
                name_selector: str = ".woocommerce-loop-product__title",
                price_selector: str = ".price") -> pd.DataFrame:
    from bs4 import BeautifulSoup
    r = requests.get(base_url, timeout=30); r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    items = []
    for p in soup.select(product_selector):
        n = p.select_one(name_selector); pr = p.select_one(price_selector)
        if not n or not pr: continue
        items.append({"name": n.get_text(strip=True), "price": pr.get_text(strip=True)})
    return pd.DataFrame(items)
