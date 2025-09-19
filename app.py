# lambda_output_report.py
# Restores monthly HTML reports (with charts) + JSONL + CSV to S3.
#
# Environment variables (Terraform):
# - REPORTS_BUCKET (required): S3 bucket for outputs
# - REPORTS_PREFIX (optional): key prefix, default "reports/cdp"
# - CLIENTS_JSON  (required): JSON map { "<ClientName>": ["canary-1", ...], ... }
# - SERVICE_NAME  (optional): e.g., "CDP" (used in file names and headers)
# - COMPANY_NAME  (optional): e.g., "LogicEase Solutions Inc."
# - FAIL_STREAK   (optional): consecutive failed minutes to count as downtime (default 3)
# - AWS_REGION    (optional): e.g., "us-east-1" (Lambda provides this; default safe)
# - DOWNSAMPLE_MINUTES (optional): integer, downsample charts to N-minute buckets (default 5)
#
# Writes (for previous UTC month):
#   {PREFIX}/{YYYY}/{MM}/uptime.jsonl
#   {PREFIX}/{YYYY}/cdp-year-YYYY.csv  (+ current/cdp-year.csv stable copy)
#   {PREFIX}/{YYYY}/{MM}/General {SERVICE} Service Level Report YYYY-MM-{CLIENT}.csv
#   {PREFIX}/{YYYY}/{MM}/General {SERVICE} Service Level Report YYYY-MM-{CLIENT}.html
#
# Notes on charts:
# - We include two time-series: Success % (avg across canaries) and Duration (avg ms).
# - To keep HTML size reasonable, data are downsampled to DOWNSAMPLE_MINUTES buckets (default 5).
# - Google Charts loader is referenced via gstatic; the viewer needs internet when opening HTML.

import os
import io
import csv
import json
import time
import math
import boto3
import datetime
from datetime import timezone, timedelta
from typing import Dict, List, Tuple, Iterable

s3 = boto3.client("s3")
cw = boto3.client("cloudwatch")

# ---------- Env ----------
S3_BUCKET     = os.environ["REPORTS_BUCKET"]
S3_PREFIX     = os.getenv("REPORTS_PREFIX", "reports/cdp")
AWS_REGION    = os.getenv("AWS_REGION", "us-east-1")
FAIL_STREAK   = int(os.getenv("FAIL_STREAK", "3"))
SERVICE_NAME  = os.getenv("SERVICE_NAME", "CDP")
COMPANY_NAME  = os.getenv("COMPANY_NAME", "LogicEase Solutions Inc.")
DOWNSAMPLE    = max(1, int(os.getenv("DOWNSAMPLE_MINUTES", "5")))  # minutes per point

try:
    CLIENTS: Dict[str, List[str]] = json.loads(os.getenv("CLIENTS_JSON", "{}"))
except Exception:
    CLIENTS = {}

# ---------- Small utils ----------
def log(msg: str) -> None:
    print(msg, flush=True)

def month_window_utc(now: datetime.datetime = None) -> Tuple[datetime.datetime, datetime.datetime]:
    """Return (start_of_prev_month_utc, end_of_prev_month_utc)."""
    now = now or datetime.datetime.now(timezone.utc)
    first_this = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_month_end = first_this - datetime.timedelta(seconds=1)
    first_last = last_month_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return first_last, last_month_end

# ---------- CloudWatch fetch ----------
def get_metric_data_points(
    canary: str, metric_name: str, start: datetime.datetime, end: datetime.datetime,
    stat: str = "Average", period: int = 60
) -> List[Tuple[datetime.datetime, float]]:
    """Fetch minute-level metric values for a canary."""
    resp = cw.get_metric_data(
        MetricDataQueries=[{
            "Id": "m1",
            "MetricStat": {
                "Metric": {
                    "Namespace": "CloudWatchSynthetics",
                    "MetricName": metric_name,
                    "Dimensions": [{"Name": "CanaryName", "Value": canary}]
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
    # Normalize timestamps to minute resolution
    out = []
    for t, v in zip(ts, vals):
        k = t.replace(second=0, microsecond=0, tzinfo=timezone.utc)
        out.append((k, float(v)))
    return out

def aggregate_client_minute_points(
    canaries: List[str], start: datetime.datetime, end: datetime.datetime
) -> Dict[datetime.datetime, Dict[str, List[float]]]:
    """
    Returns dict[minute] -> { "success": [..values..], "duration": [..values..] } merged across canaries.
    """
    mp: Dict[datetime.datetime, Dict[str, List[float]]] = {}
    for name in canaries:
        # SuccessPercent
        for ts, val in get_metric_data_points(name, "SuccessPercent", start, end):
            mp.setdefault(ts, {"success": [], "duration": []})
            mp[ts]["success"].append(val)
        # Duration
        for ts, val in get_metric_data_points(name, "Duration", start, end):
            mp.setdefault(ts, {"success": [], "duration": []})
            mp[ts]["duration"].append(val)
    return mp

def compute_availability_latency(
    minute_points: Dict[datetime.datetime, Dict[str, List[float]]]
) -> Tuple[float, int, int, float]:
    """
    Returns: (availability_ratio, total_minutes, down_minutes, avg_duration_ms)
    - A minute is considered failed if ANY canary is < 100% success.
    - Down minutes counted only after FAIL_STREAK consecutive failed minutes.
    """
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

# ---------- CSV helpers ----------
def load_year_rows(bucket: str, key: str) -> Dict[int, dict]:
    """Return dict 1..12 -> row or None from an existing year CSV (if present)."""
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

# ---------- Downsampling for charts ----------
def downsample_minutes(
    minute_points: Dict[datetime.datetime, Dict[str, List[float]]],
    bucket_minutes: int
) -> List[Tuple[datetime.datetime, float, float]]:
    """
    Returns list of (bucket_start_ts, avg_success_percent, avg_duration_ms).
    - success% averaged across canaries and minutes within bucket.
    - duration averaged likewise.
    """
    if not minute_points:
        return []

    keys = sorted(minute_points.keys())
    buckets: List[Tuple[datetime.datetime, float, float]] = []

    # Initialize
    current_bucket_start = keys[0].replace(second=0, microsecond=0)
    bucket_end = current_bucket_start + timedelta(minutes=bucket_minutes)
    succ_accum: List[float] = []
    dur_accum: List[float] = []

    def flush():
        if succ_accum or dur_accum:
            s_avg = sum(succ_accum) / len(succ_accum) if succ_accum else None
            d_avg = sum(dur_accum) / len(dur_accum) if dur_accum else None
            buckets.append((current_bucket_start, s_avg or 0.0, d_avg or 0.0))

    for k in keys:
        # Move to correct bucket
        while k >= bucket_end:
            flush()
            # advance bucket
            nonlocal_current = bucket_end
            current_bucket_start_local = nonlocal_current
            # assign
            current_bucket_start = current_bucket_start_local
            bucket_end = current_bucket_start + timedelta(minutes=bucket_minutes)
            succ_accum = []
            dur_accum = []

        # Aggregate averages for this minute across canaries
        succ_vals = minute_points[k]["success"]
        dur_vals = minute_points[k]["duration"]
        if succ_vals:
            # minute success avg
            succ_accum.append(sum(succ_vals) / len(succ_vals))
        if dur_vals:
            dur_accum.append(sum(dur_vals) / len(dur_vals))

    flush()
    return buckets

# ---------- HTML render ----------
def render_html_report(*,
    client: str,
    year: int,
    month_num: int,
    month_name: str,
    availability_pct: float,
    response_time_sec: float,
    company: str,
    service: str,
    region: str,
    generated_at_iso: str,
    chart_rows: List[Tuple[datetime.datetime, float, float]],
) -> str:
    """
    Returns a complete HTML document string (self-contained, except Google Charts loader).
    chart_rows: [(ts, success_pct_avg, duration_ms_avg)]
    """
    # Prepare arrays for JS
    # Google DataTable schema: [new Date(y, m-1, d, H, M), success_pct, duration_ms]
    js_rows = []
    for ts, sp, dur in chart_rows:
        # Ensure tz naive in local JS (we keep UTC values)
        js_rows.append(f"[new Date({ts.year}, {ts.month-1}, {ts.day}, {ts.hour}, {ts.minute}), {round(sp,3)}, {round(dur,3)}]")

    rows_js = ",\n            ".join(js_rows)

    title_line = f"{client} — {service} Monthly Service Level Report — {month_name} {year}"
    sub_line   = f"{company} • Region: {region} • Generated: {generated_at_iso}"

    # Simple print-friendly CSS with A4 sizing
    html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{service} Monthly Uptime — {month_name} {year} — {client}</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  @page {{
    size: A4;
    margin: 16mm;
  }}
  body {{
    font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
    color: #0f172a;
    background: #ffffff;
    margin: 0;
    padding: 24px;
  }}
  .header {{
    font-size: 22px;
    font-weight: 700;
    margin-bottom: 4px;
  }}
  .sub {{
    font-size: 12px;
    color: #475569;
    margin-bottom: 20px;
  }}
  .kpis {{
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 12px;
    margin-bottom: 16px;
  }}
  .card {{
    border: 1px solid #e5e7eb;
    border-radius: 10px;
    padding: 12px 14px;
    background: #fafafa;
  }}
  .card .label {{ font-size: 12px; color:#64748b; }}
  .card .value {{ font-size: 20px; font-weight: 700; margin-top: 4px; }}
  .chart {{
    border: 1px solid #e5e7eb;
    border-radius: 10px;
    margin: 14px 0;
    height: 360px;
  }}
  table.meta {{
    width: 100%;
    border-collapse: collapse;
    margin-top: 10px;
    margin-bottom: 6px;
    font-size: 12px;
  }}
  table.meta td {{ padding: 6px 0; }}
  .footer {{
    margin-top: 8px; font-size: 11px; color:#64748b;
  }}
  @media print {{
    body {{ padding: 0; }}
    .chart {{ height: 300px; }}
  }}
</style>
<script src="https://www.gstatic.com/charts/loader.js"></script>
<script>
  google.charts.load('current', {{packages:['corechart', 'line']}});

  function drawCharts() {{
    // Build DataTable
    var data = new google.visualization.DataTable();
    data.addColumn('datetime', 'Time (UTC)');
    data.addColumn('number', 'Success % (avg)');
    data.addColumn('number', 'Duration (ms, avg)');
    data.addRows([
            {rows_js}
    ]);

    var options1 = {{
      title: 'Success Percentage (Average across canaries, downsampled)',
      legend: {{ position: 'bottom' }},
      hAxis: {{ title: 'Time (UTC)' }},
      vAxis: {{ title: 'Success %', viewWindow: {{min: 0, max: 100}} }},
      series: {{
        0: {{ targetAxisIndex: 0 }},
        1: {{ targetAxisIndex: 1 }}
      }},
      vAxes: {{
        // 0: Success %, 1: Duration ms
        0: {{ title: 'Success %', viewWindow: {{min: 0, max: 100}} }},
        1: {{ title: 'Duration (ms)' }}
      }}
    }};

    var chart1 = new google.visualization.LineChart(document.getElementById('chart1'));
    chart1.draw(data, options1);
  }}

  if (document.readyState === 'loading') {{
    document.addEventListener('DOMContentLoaded', drawCharts);
  }} else {{
    drawCharts();
  }}
</script>
</head>
<body>
  <div class="header">{title_line}</div>
  <div class="sub">{sub_line}</div>

  <table class="meta">
    <tr>
      <td><strong>Client:</strong> {client}</td>
      <td><strong>Service:</strong> {service}</td>
      <td><strong>Month:</strong> {month_name} {year}</td>
    </tr>
  </table>

  <div class="kpis">
    <div class="card">
      <div class="label">Availability</div>
      <div class="value">{availability_pct:.3f}%</div>
    </div>
    <div class="card">
      <div class="label">Average Response</div>
      <div class="value">{response_time_sec:.3f} s</div>
    </div>
    <div class="card">
      <div class="label">Region</div>
      <div class="value">{region}</div>
    </div>
    <div class="card">
      <div class="label">Generated (UTC)</div>
      <div class="value">{generated_at_iso.replace('T',' ')}</div>
    </div>
  </div>

  <div id="chart1" class="chart"></div>

  <div class="footer">
    © {year} {company}. This report summarizes CloudWatch Synthetics results. Charts are downsampled to {DOWNSAMPLE}-minute buckets for readability.
  </div>
</body>
</html>"""
    return html

# ---------- S3 write ----------
def put_s3_text(bucket: str, key: str, body: str, content_type: str = "text/plain; charset=utf-8") -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType=content_type,
        CacheControl="no-cache",
    )

# ---------- Handler ----------
def handler(event, ctx):
    log("[info] monthly uptime HTML report run started")
    start, end = month_window_utc()
    year       = int(start.strftime("%Y"))
    month_num  = int(start.strftime("%m"))
    month_str  = start.strftime("%m")
    month_name = start.strftime("%B")
    generated_at = datetime.datetime.now(timezone.utc).isoformat()

    month_prefix = f"{S3_PREFIX}/{year}/{month_str}/"

    # Build results per client
    results: List[dict] = []

    for client, canaries in CLIENTS.items():
        if not canaries:
            continue

        log(f"[info] fetching metrics for client '{client}' across {len(canaries)} canary/canaries")
        mp = aggregate_client_minute_points(canaries, start, end)
        availability, total_minutes, down_minutes, avg_duration_ms = compute_availability_latency(mp)

        # Downsample for charts
        ds_rows = downsample_minutes(mp, DOWNSAMPLE)  # [(ts, avgSucc, avgDur)]
        results.append({
            "client": client,
            "year": year,
            "month_num": month_num,
            "month_name": month_name,
            "minutes_total": total_minutes,
            "minutes_down": down_minutes,
            "availability_pct": round(availability * 100.0, 3),
            "response_time_sec": round(avg_duration_ms / 1000.0, 3),
            "generated_at_utc": generated_at,
            "chart_rows": ds_rows,
        })

    # Persist JSONL snapshot for the month
    put_s3_text(
        S3_BUCKET,
        f"{month_prefix}uptime.jsonl",
        "\n".join(json.dumps({k: (v if k != 'chart_rows' else None) for k, v in r.items()}) for r in results),
        content_type="application/x-ndjson; charset=utf-8"
    )
    log(f"[info] wrote JSONL to s3://{S3_BUCKET}/{month_prefix}uptime.jsonl")

    # Year rollup CSV (keep first client row for backward compat, like before)
    key_year = f"{S3_PREFIX}/{year}/cdp-year-{year}.csv"
    rows_by_month = load_year_rows(S3_BUCKET, key_year)

    if results:
        r0 = results[0]
        rows_by_month[month_num] = {
            "year": str(year),
            "month_num": month_num,
            "month_name": month_name,
            "client": r0["client"],
            "service": SERVICE_NAME,
            "availability_pct": r0["availability_pct"],
            "response_time_sec": r0["response_time_sec"],
            "generated_at_utc": generated_at
        }

    write_rows_csv(S3_BUCKET, key_year, rows_by_month)
    log(f"[info] wrote year CSV to s3://{S3_BUCKET}/{key_year}")

    # Stable copy for QS datasets or other consumers
    s3.copy_object(
        Bucket=S3_BUCKET,
        CopySource={"Bucket": S3_BUCKET, "Key": key_year},
        Key=f"{S3_PREFIX}/current/cdp-year.csv"
    )
    log(f"[info] updated stable copy at s3://{S3_BUCKET}/{S3_PREFIX}/current/cdp-year.csv")

    # Per-client CSV + HTML
    for r in results:
        client = r["client"]
        # CSV
        csv_key = f"{month_prefix}General {SERVICE_NAME} Service Level Report {year}-{month_str}-{client}.csv"
        write_rows_csv(S3_BUCKET, csv_key, [{
            "year": str(year),
            "month_num": r["month_num"],
            "month_name": r["month_name"],
            "client": client,
            "service": SERVICE_NAME,
            "availability_pct": r["availability_pct"],
            "response_time_sec": r["response_time_sec"],
            "generated_at_utc": r["generated_at_utc"],
        }])
        log(f"[info] wrote CSV to s3://{S3_BUCKET}/{csv_key}")

        # HTML
        html_key = f"{month_prefix}General {SERVICE_NAME} Service Level Report {year}-{month_str}-{client}.html"
        html = render_html_report(
            client=client,
            year=year,
            month_num=month_num,
            month_name=month_name,
            availability_pct=r["availability_pct"],
            response_time_sec=r["response_time_sec"],
            company=COMPANY_NAME,
            service=SERVICE_NAME,
            region=AWS_REGION,
            generated_at_iso=generated_at,
            chart_rows=r["chart_rows"],
        )
        put_s3_text(S3_BUCKET, html_key, html, content_type="text/html; charset=utf-8")
        log(f"[info] wrote HTML to s3://{S3_BUCKET}/{html_key}")

    return {
        "status": "ok",
        "prefix": month_prefix,
        "results": [{k: v for k, v in r.items() if k != "chart_rows"} for r in results]
    }
