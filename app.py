# lambda_generate_uptime.py
# Generates monthly uptime/latency CSVs from CloudWatch Synthetics and (optionally) exports a QuickSight PDF to S3.
#
# Expects these environment variables (Terraform sets them):
# - REPORTS_BUCKET (required): S3 bucket for outputs
# - REPORTS_PREFIX (optional): S3 key prefix (e.g., "reports/roamjobs/CDP")
# - QS_ACCOUNT_ID (optional): QuickSight account ID (required for PDF)
# - DASHBOARD_ID  (optional): QuickSight dashboard ID (required for PDF)
# - QS_SHEET_ID   (optional): QuickSight sheet ID (required for PDF)
# - FAIL_STREAK   (optional): consecutive failed minutes to count as downtime (default 3)
# - CLIENTS_JSON  (required): JSON map { "<ClientName>": ["canary-name-1", ...], ... }
# - SERVICE_NAME  (optional): e.g., "CDP" for file names
# - COMPANY_NAME  (optional): used in footer text if you add it to QS
#
# Notes:
# - Uses the previous calendar month in UTC as the reporting window.
# - Writes:
#   {PREFIX}/{YYYY}/cdp-year-YYYY.csv
#   {PREFIX}/current/cdp-year.csv
#   {PREFIX}/{YYYY}/{MM}/General {SERVICE} Service Level Report YYYY-MM.csv
#   {PREFIX}/{YYYY}/{MM}/General {SERVICE} Service Level Report YYYY-MM.pdf  (if QS is configured)
#
# - QuickSight PDF export requires:
#   * QS_ACCOUNT_ID, DASHBOARD_ID, QS_SHEET_ID provided
#   * QuickSight service role granted S3 access to your reports bucket (in QS Admin → Security & permissions → S3).

import os
import json
import csv
import io
import time
import boto3
import datetime
from datetime import timezone
from typing import Dict, List, Tuple

s3 = boto3.client("s3")
cw = boto3.client("cloudwatch")
qs = boto3.client("quicksight")

# ---------- Env ----------
S3_BUCKET     = os.environ["REPORTS_BUCKET"]
S3_PREFIX     = os.environ.get("REPORTS_PREFIX", "reports/cdp")
QS_ACCOUNT_ID = os.getenv("QS_ACCOUNT_ID")
DASHBOARD_ID  = os.getenv("DASHBOARD_ID")
QS_SHEET_ID   = os.getenv("QS_SHEET_ID")
AWS_REGION    = os.getenv("AWS_REGION", "us-east-1")  # Provided by Lambda; default is safe too
FAIL_STREAK   = int(os.getenv("FAIL_STREAK", "3"))
SERVICE_NAME  = os.getenv("SERVICE_NAME", "CDP")
COMPANY_NAME  = os.getenv("COMPANY_NAME", "LogicEase Solutions Inc.")

try:
    CLIENTS: Dict[str, List[str]] = json.loads(os.getenv("CLIENTS_JSON", "{}"))
except Exception:
    CLIENTS = {}

# ---------- Helpers ----------
def log(msg: str):
    print(msg, flush=True)

def month_window_utc(now: datetime.datetime = None) -> Tuple[datetime.datetime, datetime.datetime]:
    """Return (start_of_prev_month_utc, end_of_prev_month_utc)."""
    now = now or datetime.datetime.now(timezone.utc)
    first_this = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_month_end = first_this - datetime.timedelta(seconds=1)
    first_last = last_month_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return first_last, last_month_end

def get_canary_metric(canary_name: str, metric_name: str,
                      start: datetime.datetime, end: datetime.datetime,
                      stat: str = "Average", period: int = 60) -> List[Tuple[datetime.datetime, float]]:
    """Fetch minute-level metric values for a canary."""
    resp = cw.get_metric_data(
        MetricDataQueries=[{
            "Id": "m1",
            "MetricStat": {
                "Metric": {
                    "Namespace": "CloudWatchSynthetics",
                    "MetricName": metric_name,
                    "Dimensions": [{"Name": "CanaryName", "Value": canary_name}]
                },
                "Period": period,
                "Stat": stat
            }
        }],
        StartTime=start,
        EndTime=end,
        ScanBy="TimestampAscending",
    )
    r = resp["MetricDataResults"][0]
    ts = r.get("Timestamps", [])
    vals = r.get("Values", [])
    return list(zip(ts, vals))

def compute_availability_and_latency(canary_names: List[str],
                                     start: datetime.datetime,
                                     end: datetime.datetime) -> Tuple[float, int, int, float]:
    """
    Returns: (availability_ratio, total_minutes, down_minutes, avg_duration_ms)
    - A minute is considered failed if ANY canary is < 100% SuccessPercent.
    - Down minutes only begin counting after FAIL_STREAK consecutive failed minutes.
    """
    minute_points: Dict[datetime.datetime, Dict[str, List[float]]] = {}

    for name in canary_names:
        for ts, val in get_canary_metric(name, "SuccessPercent", start, end):
            k = ts.replace(second=0, microsecond=0)
            minute_points.setdefault(k, {"success": [], "duration": []})
            minute_points[k]["success"].append(val)

        for ts, val in get_canary_metric(name, "Duration", start, end):
            k = ts.replace(second=0, microsecond=0)
            minute_points.setdefault(k, {"success": [], "duration": []})
            minute_points[k]["duration"].append(val)

    keys = sorted(minute_points.keys())
    failures: List[int] = []
    durations: List[float] = []

    for k in keys:
        succ_vals = minute_points[k]["success"]
        if not succ_vals:
            continue
        all_ok = all(v >= 100.0 for v in succ_vals)
        failures.append(0 if all_ok else 1)

        dvals = minute_points[k]["duration"]
        if dvals:
            durations.append(sum(dvals) / len(dvals))

    total_minutes = len(failures)
    down_minutes = 0
    streak = 0
    for f in failures:
        if f == 1:
            streak += 1
            if streak >= FAIL_STREAK:
                down_minutes += 1
        else:
            streak = 0

    availability = 1.0 - (down_minutes / max(total_minutes, 1))
    avg_duration_ms = (sum(durations) / len(durations)) if durations else 0.0
    return availability, total_minutes, down_minutes, avg_duration_ms

def load_year_rows(bucket: str, key: str, year: int) -> Dict[int, dict]:
    """Return a dict 1..12 -> row or None from an existing year CSV (if present)."""
    rows = {i: None for i in range(1, 13)}
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read().decode("utf-8").splitlines()
        reader = csv.DictReader(content)
        for r in reader:
            try:
                rows[int(r["month_num"])] = r
            except Exception:
                continue
    except s3.exceptions.NoSuchKey:
        pass
    except Exception as e:
        log(f"[warn] load_year_rows error ignored: {type(e).__name__}")
    return rows

def write_rows_csv(bucket: str, key: str, rows) -> None:
    """Write rows to CSV with the fixed header."""
    fieldnames = [
        "year",
        "month_num",
        "month_name",
        "client",
        "service",
        "availability_pct",
        "response_time_sec",
        "generated_at_utc",
    ]
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=fieldnames)
    writer.writeheader()
    iterable = ([rows[i] for i in range(1, 13)] if isinstance(rows, dict) else rows)
    for r in iterable:
        if r:
            writer.writerow(r)
    s3.put_object(Bucket=bucket, Key=key, Body=out.getvalue().encode("utf-8"))

def start_qs_snapshot(year: str, month_str: str, prefix: str) -> str:
    """Kick off a QuickSight PDF snapshot for a specific sheet."""
    job_id = f"uptime-{year}-{month_str}-{int(time.time())}"
    qs.start_dashboard_snapshot_job(
        AwsAccountId=QS_ACCOUNT_ID,
        DashboardId=DASHBOARD_ID,
        SnapshotJobId=job_id,
        UserConfiguration={
            # Minimal; expand if you add RLS or embedding
            "AnonymousUsers": [{}]
        },
        SnapshotConfiguration={
            "FileGroups": [{
                "Files": [{
                    "FormatType": "PDF",
                    "SheetSelections": [{
                        "SheetId": QS_SHEET_ID,
                        "SelectionScope": "ALL_VISUALS"
                    }]
                }]
            }],
            "DestinationConfiguration": {
                "S3Destinations": [{
                    "BucketConfiguration": {
                        "BucketName": S3_BUCKET,
                        "BucketPrefix": prefix,
                        "BucketRegion": AWS_REGION
                    }
                }]
            }
        }
    )
    return job_id

def wait_qs_snapshot(job_id: str, timeout_sec: int = 600, poll_sec: int = 10) -> str:
    """Poll the snapshot job until terminal state or timeout. Returns the status string."""
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        resp = qs.describe_dashboard_snapshot_job(
            AwsAccountId=QS_ACCOUNT_ID,
            DashboardId=DASHBOARD_ID,
            SnapshotJobId=job_id
        )
        status = resp.get("JobStatus")
        if status in ("COMPLETED", "FAILED", "TIMED_OUT"):
            return status
        time.sleep(poll_sec)
    return "TIMED_OUT"

def find_pdf_key(prefix: str) -> str:
    """Find the most recent PDF object under a prefix (QS writes a generated name)."""
    resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    if "Contents" not in resp:
        return None
    pdfs = [o["Key"] for o in resp["Contents"] if o["Key"].lower().endswith(".pdf")]
    return sorted(pdfs)[-1] if pdfs else None

# ---------- Handler ----------
def handler(event, ctx):
    log("[info] uptime report run started")
    start, end = month_window_utc()
    year       = start.strftime("%Y")
    month_num  = int(start.strftime("%m"))
    month_str  = start.strftime("%m")
    month_name = start.strftime("%B")
    generated_at = datetime.datetime.now(timezone.utc).isoformat()

    # Aggregate per-client
    results = []
    for client, canaries in CLIENTS.items():
        if not canaries:
            continue
        log(f"[info] computing metrics for '{client}' across {len(canaries)} canary/canaries")
        availability, total_minutes, down_minutes, avg_duration_ms = compute_availability_and_latency(
            canaries, start, end
        )
        results.append({
            "client": client,
            "year": int(year),
            "month_num": month_num,
            "month_name": month_name,
            "minutes_total": total_minutes,
            "minutes_down": down_minutes,
            "availability_pct": round(availability * 100, 3),
            "response_time_sec": round(avg_duration_ms / 1000.0, 3),
            "generated_at_utc": generated_at
        })

    # Per-run artifacts
    prefix = f"{S3_PREFIX}/{year}/{month_str}/"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{prefix}uptime.jsonl",
        Body=("\n".join(json.dumps(r) for r in results)).encode("utf-8")
    )
    log(f"[info] wrote JSONL to s3://{S3_BUCKET}/{prefix}uptime.jsonl")

    # Year rollup CSV (header-only if no results yet)
    key_year = f"{S3_PREFIX}/{year}/cdp-year-{year}.csv"
    rows = load_year_rows(S3_BUCKET, key_year, int(year))

    if results:
        r = results[0]
        rows[month_num] = {
            "year": str(year),
            "month_num": month_num,
            "month_name": month_name,
            "client": r["client"],
            "service": SERVICE_NAME,
            "availability_pct": r["availability_pct"],
            "response_time_sec": r["response_time_sec"],
            "generated_at_utc": generated_at
        }
        month_csv_key = f"{prefix}General {SERVICE_NAME} Service Level Report {year}-{month_str}.csv"
        write_rows_csv(S3_BUCKET, month_csv_key, [rows[month_num]])
        log(f"[info] wrote month CSV to s3://{S3_BUCKET}/{month_csv_key}")

    # Write/update the year CSV (even if header-only)
    write_rows_csv(S3_BUCKET, key_year, rows)
    log(f"[info] wrote year CSV to s3://{S3_BUCKET}/{key_year}")

    # Stable copy for a fixed QuickSight dataset
    s3.copy_object(
        Bucket=S3_BUCKET,
        CopySource={"Bucket": S3_BUCKET, "Key": key_year},
        Key=f"{S3_PREFIX}/current/cdp-year.csv"
    )
    log(f"[info] updated stable copy at s3://{S3_BUCKET}/{S3_PREFIX}/current/cdp-year.csv")

    # QuickSight snapshot (best-effort). Skip cleanly if not fully configured or no data yet.
    has_data = any(r.get("minutes_total", 0) > 0 for r in results)
    if has_data and QS_ACCOUNT_ID and DASHBOARD_ID and QS_SHEET_ID:
        try:
            log("[info] starting QuickSight snapshot job")
            job_id = start_qs_snapshot(year, month_str, prefix)
            status = wait_qs_snapshot(job_id)
            log(f"[info] QuickSight snapshot job status: {status}")
            if status == "COMPLETED":
                src_key = find_pdf_key(prefix)
                if src_key:
                    dst_key = f"{prefix}General {SERVICE_NAME} Service Level Report {year}-{month_str}.pdf"
                    s3.copy_object(
                        Bucket=S3_BUCKET,
                        CopySource={"Bucket": S3_BUCKET, "Key": src_key},
                        Key=dst_key
                    )
                    log(f"[info] wrote PDF to s3://{S3_BUCKET}/{dst_key}")
            return {"status": "ok", "results": results, "prefix": prefix, "qs_status": status}
        except Exception as e:
            log(f"[warn] QuickSight snapshot failed: {type(e).__name__}: {e}")
            return {"status": "ok", "results": results, "prefix": prefix, "qs_skipped": f"snapshot_error: {type(e).__name__}"}
    else:
        reason = "no_results" if not has_data else "qs_not_configured"
        log(f"[info] skipping QuickSight snapshot ({reason})")
        return {"status": "ok", "results": results, "prefix": prefix, "qs_skipped": reason}
