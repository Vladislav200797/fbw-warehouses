#!/usr/bin/env python3
import os, sys, time
from typing import List, Dict, Any
import requests
from supabase import create_client, Client

WB_SUPPLIES_TOKEN = os.getenv("WB_SUPPLIES_TOKEN")          # HeaderApiKey (Authorization)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")    # service_role
SCHEMA = os.getenv("SUPABASE_SCHEMA", "public")
TABLE = os.getenv("SUPABASE_TABLE", "fbw_warehouses")

API_URL = "https://supplies-api.wildberries.ru/api/v1/warehouses"
HEADERS = {"Authorization": WB_SUPPLIES_TOKEN}

def fail(msg: str, code: int = 1):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)

def fetch_warehouses() -> List[Dict[str, Any]]:
    backoffs = [0, 3, 7]  # простая защита от 429
    for i, wait in enumerate(backoffs):
        if wait:
            time.sleep(wait)
        resp = requests.get(API_URL, headers=HEADERS, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            if not isinstance(data, list):
                fail(f"Unexpected response: {data}")
            # Нормализация ключей (acceptsQR vs acceptsQr)
            norm = []
            for row in data:
                norm.append({
                    "id": row.get("ID") or row.get("id"),
                    "name": row.get("name"),
                    "address": row.get("address"),
                    "work_time": row.get("workTime"),
                    "accepts_qr": row.get("acceptsQR", row.get("acceptsQr")),
                    "is_active": row.get("isActive"),
                    "is_transit_active": row.get("isTransitActive"),
                })
            return norm
        elif resp.status_code == 429 and i < len(backoffs) - 1:
            continue
        else:
            fail(f"WB API {resp.status_code}: {resp.text}")
    return []

def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def main():
    if not WB_SUPPLIES_TOKEN:
        fail("WB_SUPPLIES_TOKEN is empty")
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        fail("Supabase URL or SERVICE KEY is empty")

    sb: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    data = fetch_warehouses()
    print(f"Fetched {len(data)} warehouses from WB")

    # 1) Удаляем все строки (условие по id >= 0 — требуется фильтр)
    sb.schema(SCHEMA).table(TABLE).delete().gte("id", 0).execute()

    # 2) Вставляем пачками
    for batch in chunked(data, 500):
        sb.schema(SCHEMA).table(TABLE).insert(batch).execute()

    print("Sync completed")

if __name__ == "__main__":
    main()
