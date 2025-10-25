import argparse, datetime as dt, json, os, sys, time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlsplit, unquote

import boto3
import requests
from requests import HTTPError


def fetch_chunk(api_base: str, resource_id: str, limit: int, offset: int, filters: Optional[Dict] = None) -> List[Dict]:
    """Call CKAN datastore_search with pagination."""
    params = {"resource_id": resource_id, "limit": limit, "offset": offset}
    if filters:
        params["filters"] = json.dumps(filters, ensure_ascii=False)
    url = api_base.rstrip("/") + "/datastore_search"
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    payload = r.json()
    if not payload.get("success"):
        raise RuntimeError(f"datastore_search returned success=false: {payload}")
    return payload["result"].get("records", [])


def resource_show(api_base: str, resource_id: str) -> Dict:
    """Ask CKAN for resource metadata (to get direct file URL, datastore_active flag, etc.)."""
    url = api_base.rstrip("/") + "/resource_show"
    r = requests.get(url, params={"id": resource_id}, timeout=60)
    r.raise_for_status()
    payload = r.json()
    if not payload.get("success"):
        raise RuntimeError(f"resource_show returned success=false: {payload}")
    return payload["result"]


def month_folder_for(date_obj: dt.date) -> str:
    return date_obj.strftime("%Y-%m")


def filename_from_url(u: str) -> str:
    """Best-effort get a filename from a resource URL."""
    path = urlsplit(u).path or ""
    base = os.path.basename(path)
    return unquote(base) if base else "downloaded_resource"


def download_to_file(url: str, dest_path: str) -> None:
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def try_datastore_to_ndjson(args, filters_dict: Dict[str, str]) -> Tuple[Optional[str], int]:
    """
    Attempt to pull from datastore_search into NDJSON.
    Returns (outfile_path, total_rows). If None, caller should fallback to file download.
    """
    today = dt.date.today()
    outfile = os.path.join(args.outdir, f"{args.dataset_name}-{today.isoformat()}.ndjson")

    collected = 0
    offset = 0
    with open(outfile, "w", encoding="utf-8") as f:
        while collected < args.max_rows:
            rows = fetch_chunk(
                api_base=args.api_base,
                resource_id=args.resource_id,
                limit=args.limit,
                offset=offset,
                filters=(filters_dict or None),
            )
            if not rows:
                break
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
            collected += len(rows)
            offset += args.limit
            print(f"[INFO] datastore_search fetched={collected:,} offset={offset:,}")
            time.sleep(0.1)

    if collected == 0:
        try:
            os.remove(outfile)
        except OSError:
            pass
        return None, 0

    return outfile, collected


def main():
    ap = argparse.ArgumentParser(
        description="Fetch a CKAN DataStore resource (paginated) and upload to S3. "
                    "Falls back to direct file download if DataStore is not enabled."
    )
    ap.add_argument(
        "--api-base",
        default="https://donnees.montreal.ca/api/3/action",
        help="CKAN API base (default: Montreal). Example for Données Québec: https://www.donneesquebec.ca/recherche/api/3/action",
    )
    ap.add_argument("--resource-id", required=True, help="CKAN resource_id UUID")
    ap.add_argument("--bucket", required=True, help="Target S3 bucket name")
    ap.add_argument("--prefix", required=True, help="S3 prefix like raw/crime or raw/accidents (no leading slash)")
    ap.add_argument("--dataset-name", default="dataset", help="Used in output filename (e.g., actes_criminels, accidents)")
    ap.add_argument("--limit", type=int, default=1000, help="Page size per request (datastore_search)")
    ap.add_argument("--max-rows", type=int, default=2_000_000, help="Safety cap")
    ap.add_argument("--outdir", default="ingest/out", help="Local temp output dir")
    ap.add_argument(
        "--filter",
        action="append",
        default=[],
        help='Exact-match filter (repeatable). Format: FIELD=VALUE  (e.g., --filter DATE=2025-10-15)',
    )
    ap.add_argument("--profile", default=None, help='Optional AWS profile to use (e.g., "ug1")')
    ap.add_argument("--region", default=None, help="Optional AWS region override (e.g., ca-central-1)")

    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    filters_dict: Dict[str, str] = {}
    for kv in args.filter:
        if "=" not in kv:
            print(f"[WARN] Ignoring bad --filter (missing '='): {kv}")
            continue
        k, v = kv.split("=", 1)
        filters_dict[k.strip()] = v.strip()

    today = dt.date.today()
    month = month_folder_for(today)

    print(f"[INFO] API base: {args.api_base}")
    print(f"[INFO] Resource: {args.resource_id}")

    # First attempt: datastore_search → NDJSON
    outfile = None
    total_rows = 0
    try:
        print("[INFO] Trying datastore_search (paginated)…")
        outfile, total_rows = try_datastore_to_ndjson(args, filters_dict)
    except HTTPError as e:
        print(f"[WARN] datastore_search HTTP error: {e}. Will try resource_show fallback.")
    except Exception as e:
        print(f"[WARN] datastore_search failed: {e}. Will try resource_show fallback.")

    if outfile and total_rows > 0:
        key_name = f"{args.dataset_name}-{today.isoformat()}.ndjson"
        s3_key = f"{args.prefix}/{month}/{key_name}"
        print(f"[INFO] NDJSON ready: {outfile}  (rows={total_rows:,})")
    else:
        # Second attempt: resource_show, url, download original file (CSV/GeoJSON)
        print("[INFO] Falling back to resource_show (direct file download)…")
        try:
            meta = resource_show(args.api_base, args.resource_id)
        except HTTPError as e:
            print(f"[ERROR] resource_show HTTP error: {e}")
            print("[HINT] Make sure you used the correct API host for the resource (Montreal vs Données Québec).")
            raise
        except Exception as e:
            print(f"[ERROR] resource_show failed: {e}")
            raise

        ds_active = bool(meta.get("datastore_active"))
        file_url = meta.get("url")
        if not file_url:
            raise RuntimeError("resource_show did not return a 'url' for direct download.")

        base_name = filename_from_url(file_url)
        outfile = os.path.join(args.outdir, base_name or f"{args.dataset_name}-{today.isoformat()}")
        print(f"[INFO] Downloading original file: {file_url}")
        download_to_file(file_url, outfile)
        print(f"[INFO] Download complete: {outfile} (datastore_active={ds_active})")

        key_name = base_name
        s3_key = f"{args.prefix}/{month}/{key_name}"

    if args.profile or args.region:
        session = boto3.Session(profile_name=args.profile, region_name=args.region)
        s3 = session.client("s3")
    else:
        s3 = boto3.client("s3")

    print(f"[INFO] Uploading to s3://{args.bucket}/{s3_key}")
    s3.upload_file(outfile, args.bucket, s3_key)
    print("[INFO] Upload complete.")

try:
    main()
except Exception as e:
    print(f"[ERROR] {e}", file=sys.stderr)
    sys.exit(1)
