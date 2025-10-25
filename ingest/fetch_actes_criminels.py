import argparse
import datetime as dt
import json
import os
import sys
import time
from typing import List, Dict

import boto3
import requests

API_BASE = "https://donnees.montreal.ca/api/3/action/datastore_search"
RESOURCE_ID = "c6f482bf-bf0f-4960-8b2f-9982c211addd"

def fetch_chunk(limit: int, offset: int, filters: Dict = None) -> List[Dict]:
    params = {
        "resource_id": RESOURCE_ID,
        "limit": limit,
        "offset": offset,
    }
    if filters:
        params["filters"] = json.dumps(filters, ensure_ascii=False)

    r = requests.get(API_BASE, params=params, timeout=60)
    r.raise_for_status()
    payload = r.json()
    if not payload.get("success"):
        raise RuntimeError(f"API returned success=false: {payload}")
    return payload["result"].get("records", [])

def month_folder_for(date_obj: dt.date) -> str:
    return f"{date_obj.strftime('%Y-%m')}"

def main():
    parser = argparse.ArgumentParser(description="Fetch Actes criminels and upload to S3 as NDJSON.")
    parser.add_argument("--bucket", required=True, help="S3 bucket name, e.g., ug1-travel-safe-bucket")
    parser.add_argument("--prefix", default="raw/crime", help="S3 prefix, default raw/crime")
    parser.add_argument("--limit", type=int, default=1000, help="Page size (CKAN limit)")
    parser.add_argument("--max-rows", type=int, default=2000000, help="Safety cap to avoid infinite loops")
    parser.add_argument("--from-date", help="Filter DATE >= this (YYYY-MM-DD)")
    parser.add_argument("--to-date", help="Filter DATE <= this (YYYY-MM-DD)")
    parser.add_argument("--outdir", default="out", help="Local temp output dir")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    # Build optional filters by date (field name is "DATE" in the dataset)
    use_client_filter = bool(args.from_date or args.to_date)
    from_date = dt.date.min
    to_date = dt.date.max
    if args.from_date:
        from_date = dt.date.fromisoformat(args.from_date)
    if args.to_date:
        to_date = dt.date.fromisoformat(args.to_date)

    collected = 0
    offset = 0
    ndjson_path = os.path.join(
        args.outdir,
        f"actes_criminels-{dt.date.today().isoformat()}.ndjson"
    )

    print(f"[INFO] Starting fetch. Writing NDJSON to {ndjson_path}")
    with open(ndjson_path, "w", encoding="utf-8") as f:
        while collected < args.max_rows:
            rows = fetch_chunk(limit=args.limit, offset=offset)
            if not rows:
                break

            if use_client_filter:
                filtered = []
                for r in rows:
                    try:
                        d = dt.date.fromisoformat(str(r.get("DATE", ""))[:10])
                    except Exception:
                        continue
                    if from_date <= d <= to_date:
                        filtered.append(r)
                rows_to_write = filtered
            else:
                rows_to_write = rows

            for r in rows_to_write:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
                collected += 1

            offset += args.limit
            print(f"[INFO] Fetched {collected} rows so far (offset now {offset})")
            time.sleep(0.1)

    print(f"[INFO] Done. Total rows written: {collected}")

    # Decide S3 key using the month folder of "today" (or choose from_date if provided)
    month_for_path = month_folder_for(from_date if args.from_date else dt.date.today())
    s3_key = f"{args.prefix}/{month_for_path}/actes_criminels-{dt.date.today().isoformat()}.ndjson"

    print(f"[INFO] Uploading to s3://{args.bucket}/{s3_key}")
    s3 = boto3.client("s3")
    s3.upload_file(ndjson_path, args.bucket, s3_key)
    print("[INFO] Upload complete.")

try:
    main()
except Exception as e:
    print(f"[ERROR] {e}", file=sys.stderr)
    sys.exit(1)
