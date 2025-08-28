# handler.py
import os, io, json, gzip, csv, hashlib, time, urllib.request, urllib.error
from datetime import datetime, timedelta, timezone
import boto3

S3 = boto3.client("s3")
DDB = boto3.resource("dynamodb")

# ---------- Helper: env ----------
def getenv(name, default=None, required=False):
    val = os.getenv(name, default)
    if required and (val is None or val == ""):
        raise RuntimeError(f"Missing required env var: {name}")
    return val

# ---------- OECD fetch ----------
def build_oecd_urls(dataset_id: str, key_path: str, last_n_obs: int, fmt: str):
    """
    OECD SDMX-JSON endpoint. We build 2 candidate URLs:
      A) prefer lastNObservations (fast/incremental)
      B) fallback to startPeriod (more broadly supported)

    key_path follows SDMX key notation for the dataset (e.g. "all" or "AUS....TOT")
    fmt is "csv" or "json".
    """
    base = f"https://stats.oecd.org/sdmx-json/data/{dataset_id}/{key_path}"
    params_common = f"detail=DataOnly&dimensionAtObservation=AllDimensions"
    if fmt == "csv":
        params_common += "&contentType=csv&separator=comma"
    else:
        params_common += "&contentType=json"

    # A) lastNObservations
    url_lastN = f"{base}?{params_common}&lastNObservations={last_n_obs}"

    # B) startPeriod fallback (past N*2 days ~ generous for annual/quarterly/monthly)
    # OECD expects YYYY or YYYY-MM or YYYY-MM-DD; we use year-month-day.
    start = (datetime.now(timezone.utc) - timedelta(days=last_n_obs * 2)).date().isoformat()
    url_start = f"{base}?{params_common}&startPeriod={start}"

    return [url_lastN, url_start]

def http_get(url, retries=3, timeout=60):
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Lambda-OECD-Ingest/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            # 404/400 may mean that parameter not supported; we allow fallback
            last_err = e
            if e.code in (429, 500, 502, 503, 504):
                time.sleep(2 * attempt)
            else:
                break
        except Exception as e:
            last_err = e
            time.sleep(2 * attempt)
    raise last_err

def fetch_oecd(dataset_id: str, key_path: str, last_n_obs: int, fmt: str):
    urls = build_oecd_urls(dataset_id, key_path, last_n_obs, fmt)
    err_acc = []
    for url in urls:
        try:
            blob = http_get(url)
            # sanity check non-empty
            if not blob or len(blob) < 32:
                err_acc.append(f"Empty/short response from {url[:120]}")
                continue
            return blob, url
        except Exception as e:
            err_acc.append(f"{type(e).__name__}: {e}")
    raise RuntimeError(f"All OECD fetch attempts failed for {dataset_id}/{key_path}: " + " | ".join(err_acc))

# ---------- Storage + de-dupe ----------
def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def maybe_compress(data: bytes, do_gzip: bool) -> tuple[bytes, dict]:
    headers = {}
    if do_gzip:
        out = io.BytesIO()
        with gzip.GzipFile(fileobj=out, mode="wb", compresslevel=6) as gz:
            gz.write(data)
        return out.getvalue(), {"ContentEncoding": "gzip"}
    return data, headers

def put_if_new(dataset_id: str, url_used: str, raw: bytes, s3_bucket: str, s3_prefix: str,
               fmt: str, gzip_output: bool, ddb_table_name: str | None):
    content_hash = sha256(raw)
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y%m%d")
    ts_str = now.strftime("%Y%m%dT%H%M%SZ")

    # optional de-dupe via DDB
    dedup_ok = True
    if ddb_table_name:
        table = DDB.Table(ddb_table_name)
        # Partition key 'dataset' (string)
        item = table.get_item(Key={"dataset": dataset_id}).get("Item")
        if item and item.get("last_hash") == content_hash:
            dedup_ok = False
        else:
            table.put_item(Item={
                "dataset": dataset_id,
                "last_hash": content_hash,
                "last_url": url_used,
                "last_run": ts_str
            })

    if not dedup_ok:
        return {
            "stored": False,
            "reason": "unchanged",
            "hash": content_hash,
            "url": url_used
        }

    ext = "csv" if fmt == "csv" else "json"
    key = f"{s3_prefix.rstrip('/')}/{dataset_id}/{date_str}/{dataset_id}_{ts_str}_{content_hash[:12]}.{ext}"
    body, extra_headers = maybe_compress(raw, gzip_output)
    ct = "text/csv" if fmt == "csv" else "application/json"
    S3.put_object(
        Bucket=s3_bucket,
        Key=key,
        Body=body,
        ContentType=ct,
        Metadata={"oecd_source_url": url_used, "sha256": content_hash},
        **extra_headers
    )
    return {
        "stored": True,
        "s3_key": key,
        "hash": content_hash,
        "url": url_used
    }

# ---------- Lambda entry ----------
def lambda_handler(event, context):
    """
    Env vars (examples):
      S3_BUCKET=your-bucket
      S3_PREFIX=oecd/raw
      OECD_DATASETS=HEALTH_STAT,EO,MEI
      OECD_KEYS_JSON={"HEALTH_STAT":"all","EO":"all","MEI":"all"}
      # or you can specify a specific SDMX key per dataset, e.g. "MEI":"AUS.M.BLF.N.PT"
      FORMAT=csv         # csv|json
      GZIP=true          # true|false
      LAST_N_OBS=60      # last observations to fetch (60 ~ 5 years monthly)
      DEDUPE_TABLE=OECDIngestState  # optional DynamoDB table name (pk: dataset)
    """
    s3_bucket = getenv("S3_BUCKET", required=True)
    s3_prefix = getenv("S3_PREFIX", "oecd/raw")
    fmt = getenv("FORMAT", "csv").lower()
    gzip_output = getenv("GZIP", "true").lower() == "true"
    last_n_obs = int(getenv("LAST_N_OBS", "60"))
    ddb_table = getenv("DEDUPE_TABLE", None)

    # dataset list
    datasets = [d.strip() for d in getenv("OECD_DATASETS", required=True).split(",") if d.strip()]
    try:
        keys_map = json.loads(getenv("OECD_KEYS_JSON", "{}"))
    except json.JSONDecodeError as e:
        raise RuntimeError(f"OECD_KEYS_JSON must be valid JSON mapping: {e}")

    results = []
    for ds in datasets:
        key_path = keys_map.get(ds, "all")  # default to 'all' keys if not provided
        try:
            raw, url_used = fetch_oecd(ds, key_path, last_n_obs, fmt)
            out = put_if_new(ds, url_used, raw, s3_bucket, s3_prefix, fmt, gzip_output, ddb_table)
            out["dataset"] = ds
            results.append(out)
        except Exception as e:
            results.append({"dataset": ds, "error": str(e)})

    ok = [r for r in results if r.get("stored") or r.get("reason") == "unchanged"]
    errs = [r for r in results if r.get("error")]
    return {
        "statusCode": 200 if not errs else 207,
        "summary": {
            "stored": sum(1 for r in ok if r.get("stored")),
            "unchanged": sum(1 for r in ok if r.get("reason") == "unchanged"),
            "errors": len(errs)
        },
        "results": results
    }
