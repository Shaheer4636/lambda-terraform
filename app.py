# lambda_generate_uptime.py
# ---------------------------------------------------------------------------
# Monthly uptime reports from Amazon CloudWatch Synthetics with robust HTML.
#
# What this does
# --------------
# - Uses the PREVIOUS calendar month in UTC (configurable) as the reporting window.
# - For each "client" (group of canaries), fetches SuccessPercent and Duration
#   at 1-minute resolution using GetMetricData with automatic pagination.
# - Computes:
#     * Availability % using FAIL_STREAK consecutive failures rule
#     * Total minutes, downtime minutes, incident windows, MTTR/MTBF
#     * Average/50th/95th/99th response times
#     * Daily availability rollup
# - Emits artifacts:
#     * {PREFIX}/{YYYY}/{MM}/uptime.jsonl                        (per-client JSON lines)
#     * {PREFIX}/{YYYY}/{MM}/General {SERVICE} ... {CLIENT}.csv  (per-client CSV)
#     * {PREFIX}/{YYYY}/{MM}/General {SERVICE} ... {CLIENT}.html (per-client HTML report)
#     * {PREFIX}/{YYYY}/{MM}/index.html                          (month index linking reports)
#     * {PREFIX}/{YYYY}/cdp-year-{YYYY}.csv                      (year rollup, + current/cdp-year.csv)
#
# Key env vars (Terraform sets these)
# -----------------------------------
# - REPORTS_BUCKET     (required)
# - REPORTS_PREFIX     (default "reports/cdp")
# - CLIENTS_JSON       (required)  {"ClientA":["canary1","canary2"], "ClientB":["..."]}
# - SERVICE_NAME       (default "CDP")
# - COMPANY_NAME       (default "LogicEase Solutions Inc.")
# - AWS_REGION         (default "us-east-1")
# - FAIL_STREAK        (default "3")  # consecutive failed minutes to begin counting downtime
# - DOWNSAMPLE_MINUTES (default "5")  # charts bucket width in minutes
# - USE_CURRENT_MONTH  (default "false")  # if "true", window = firstOfMonth..now (for testing)
#
# Notes
# -----
# - Charts use Google Charts loader; viewer needs internet when opening HTML.
# - All pages are A4 and print-ready with page headers/footers, page numbers.
# - Graceful handling when metrics are sparse or missing (shows "—" or "No incidents").
# - Heavily commented for maintainability.
# ---------------------------------------------------------------------------

import os
import io
import csv
import json
import math
import time
import boto3
import datetime
from datetime import timezone, timedelta
from typing import Dict, List, Tuple, Optional

# ------------------------ AWS clients ------------------------
s3 = boto3.client("s3")
cw = boto3.client("cloudwatch")

# ------------------------ Configuration ----------------------
S3_BUCKET      = os.environ["REPORTS_BUCKET"]
S3_PREFIX      = os.getenv("REPORTS_PREFIX", "reports/cdp")
SERVICE        = os.getenv("SERVICE_NAME", "CDP")
COMPANY        = os.getenv("COMPANY_NAME", "LogicEase Solutions Inc.")
REGION         = os.getenv("AWS_REGION", "us-east-1")
FAIL_STREAK    = max(1, int(os.getenv("FAIL_STREAK", "3")))
DOWNSAMPLE_MIN = max(1, int(os.getenv("DOWNSAMPLE_MINUTES", "5")))
USE_CURRENT    = os.getenv("USE_CURRENT_MONTH", "false").lower() == "true"

def _load_clients() -> Dict[str, List[str]]:
    try:
        return json.loads(os.getenv("CLIENTS_JSON", "{}")) or {}
    except Exception:
        return {}

CLIENTS: Dict[str, List[str]] = _load_clients()

# ------------------------ Logging ----------------------------
def log(msg: str) -> None:
    print(msg, flush=True)

# ------------------------ Time windows -----------------------
def month_window_utc(now: Optional[datetime.datetime] = None) -> Tuple[datetime.datetime, datetime.datetime]:
    """Return the (start, end) window for previous calendar month in UTC."""
    now = now or datetime.datetime.now(timezone.utc)
    if USE_CURRENT:
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return start, now
    first_this = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_end = first_this - datetime.timedelta(seconds=1)
    first_last = last_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return first_last, last_end

def daterange_minutes(start: datetime.datetime, end: datetime.datetime, step_min: int) -> List[Tuple[datetime.datetime, datetime.datetime]]:
    """Split [start, end] into minute ranges of at most `step_min` minutes to keep GetMetricData windows small."""
    out = []
    cur = start
    step = timedelta(minutes=step_min)
    while cur < end:
        nxt = min(cur + step, end)
        out.append((cur, nxt))
        cur = nxt
    return out

# ------------------------ S3 helpers -------------------------
def put_s3_text(bucket: str, key: str, body: str, content_type: str) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType=content_type,
        CacheControl="no-cache",
    )

# ------------------------ CSV helpers ------------------------
def load_year_rows(bucket: str, key: str) -> Dict[int, dict]:
    rows = {i: None for i in range(1, 13)}
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read().decode("utf-8").splitlines()
        rdr = csv.DictReader(content)
        for r in rdr:
            try:
                rows[int(r["month_num"])] = r
            except Exception:
                continue
    except s3.exceptions.NoSuchKey:
        pass
    return rows

def write_rows_csv(bucket: str, key: str, rows) -> None:
    fields = [
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
    w = csv.DictWriter(out, fieldnames=fields)
    w.writeheader()
    iterable = ([rows[i] for i in range(1, 13)] if isinstance(rows, dict) else rows)
    for r in iterable:
        if r:
            w.writerow(r)
    s3.put_object(Bucket=bucket, Key=key, Body=out.getvalue().encode("utf-8"))

# ------------------------ Math/stats helpers -----------------
def percentile(values: List[float], p: float) -> Optional[float]:
    """Compute pth percentile (0..100). Returns None if no values."""
    if not values:
        return None
    if p <= 0:
        return min(values)
    if p >= 100:
        return max(values)
    s = sorted(values)
    k = (len(s) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return s[int(k)]
    d0 = s[f] * (c - k)
    d1 = s[c] * (k - f)
    return d0 + d1

# ------------------------ CloudWatch data access -------------
def get_metric_data_paginated(queries: List[dict], start: datetime.datetime, end: datetime.datetime) -> List[dict]:
    """
    GetMetricData with pagination on NextToken. Returns MetricDataResults list.
    This function assumes 'queries' already specify unique Ids.
    """
    results = None
    next_token = None
    agg = None
    while True:
        kwargs = {
            "MetricDataQueries": queries,
            "StartTime": start,
            "EndTime": end,
            "ScanBy": "TimestampAscending",
        }
        if next_token:
            kwargs["NextToken"] = next_token
        resp = cw.get_metric_data(**kwargs)
        if results is None:
            results = resp["MetricDataResults"]
            # Prepare aggregation holder (by Id)
            agg = {r["Id"]: {"Timestamps": [], "Values": []} for r in results}
        else:
            # Merge into agg by Id
            for r in resp["MetricDataResults"]:
                holder = agg[r["Id"]]
                holder["Timestamps"].extend(r.get("Timestamps", []))
                holder["Values"].extend(r.get("Values", []))
        next_token = resp.get("NextToken")
        if not next_token:
            break
    # Convert agg back to results-like format
    out = []
    for q in queries:
        rid = q["Id"]
        holder = agg[rid]
        out.append({"Id": rid, "Timestamps": holder["Timestamps"], "Values": holder["Values"]})
    return out

def fetch_minute_series_for_canary(canary: str, start: datetime.datetime, end: datetime.datetime) -> Tuple[List[Tuple[datetime.datetime, float]], List[Tuple[datetime.datetime, float]]]:
    """
    Returns (success_series, duration_series)
    Each series is a list of (minute_ts_utc, value).
    """
    # We can request both metrics in the same call with two queries.
    queries = [
        {
            "Id": "succ",
            "MetricStat": {
                "Metric": {
                    "Namespace": "CloudWatchSynthetics",
                    "MetricName": "SuccessPercent",
                    "Dimensions": [{"Name": "CanaryName", "Value": canary}],
                },
                "Period": 60,
                "Stat": "Average",
            },
            "ReturnData": True,
        },
        {
            "Id": "dur",
            "MetricStat": {
                "Metric": {
                    "Namespace": "CloudWatchSynthetics",
                    "MetricName": "Duration",
                    "Dimensions": [{"Name": "CanaryName", "Value": canary}],
                },
                "Period": 60,
                "Stat": "Average",
            },
            "ReturnData": True,
        },
    ]
    res = get_metric_data_paginated(queries, start, end)
    succ = []
    dur = []
    for r in res:
        ts = r.get("Timestamps", [])
        vals = r.get("Values", [])
        pts = []
        for t, v in zip(ts, vals):
            # normalize to minute boundary
            k = t.replace(second=0, microsecond=0, tzinfo=timezone.utc)
            pts.append((k, float(v)))
        if r["Id"] == "succ":
            succ = pts
        elif r["Id"] == "dur":
            dur = pts
    return succ, dur

def merge_minute_points(canaries: List[str], start: datetime.datetime, end: datetime.datetime) -> Dict[datetime.datetime, Dict[str, List[float]]]:
    """
    Returns: dict[minute_ts] -> {"succ":[...], "dur":[...]} merged across canaries.
    """
    mp: Dict[datetime.datetime, Dict[str, List[float]]] = {}
    for name in canaries:
        s_series, d_series = fetch_minute_series_for_canary(name, start, end)
        for ts, v in s_series:
            mp.setdefault(ts, {"succ": [], "dur": []})
            mp[ts]["succ"].append(v)
        for ts, v in d_series:
            mp.setdefault(ts, {"succ": [], "dur": []})
            mp[ts]["dur"].append(v)
    return mp

# ------------------------ Aggregation logic ------------------
def compute_summary(mp: Dict[datetime.datetime, Dict[str, List[float]]]) -> dict:
    """
    Given merged minute points, compute availability, incidents, response stats, daily rollups, etc.
    Returns a dict with keys:
      total_minutes, down_minutes, availability_pct,
      incidents: List[(start_ts, end_ts, minutes)],
      avg_resp_s, p50_resp_s, p95_resp_s, p99_resp_s,
      daily: List[(date(UTC), availability_pct, minutes_total, minutes_down)]
      chart_rows: List[(ts, success_avg_pct, response_avg_s)]
    """
    keys = sorted(mp.keys())
    if not keys:
        return {
            "total_minutes": 0,
            "down_minutes": 0,
            "availability_pct": 100.0,
            "incidents": [],
            "avg_resp_s": None,
            "p50_resp_s": None,
            "p95_resp_s": None,
            "p99_resp_s": None,
            "daily": [],
            "chart_rows": [],
            "mttr_min": None,
            "mtbf_min": None,
        }

    failures: List[int] = []
    per_min_succ: List[float] = []
    per_min_dur_ms: List[float] = []
    # daily maps
    per_day_total: Dict[datetime.date, int] = {}
    per_day_down: Dict[datetime.date, int] = {}

    for k in keys:
        svals = mp[k]["succ"]
        dvals = mp[k]["dur"]
        if not svals:
            # minute missing success -> skip this minute entirely from total
            continue
        all_ok = all(v >= 100.0 for v in svals)
        failures.append(0 if all_ok else 1)
        per_min_succ.append(sum(svals) / len(svals))
        if dvals:
            per_min_dur_ms.append(sum(dvals) / len(dvals))
        # daily totals
        d = k.date()
        per_day_total[d] = per_day_total.get(d, 0) + 1

    # Incident detection with FAIL_STREAK
    total_minutes = len(failures)
    incidents: List[Tuple[datetime.datetime, datetime.datetime, int]] = []
    down_minutes = 0
    run_len = 0
    run_start_idx = None
    for i, f in enumerate(failures):
        if f == 1:
            if run_len == 0:
                run_start_idx = i
            run_len += 1
        else:
            if run_len >= FAIL_STREAK:
                start_i = run_start_idx + FAIL_STREAK - 1
                end_i = i - 1
                incidents.append((keys[start_i], keys[end_i], end_i - start_i + 1))
                down_minutes += (end_i - start_i + 1)
                # mark days for down minutes
                for j in range(start_i, end_i + 1):
                    d = keys[j].date()
                    per_day_down[d] = per_day_down.get(d, 0) + 1
            run_len = 0
            run_start_idx = None
    if run_len >= FAIL_STREAK:
        start_i = run_start_idx + FAIL_STREAK - 1
        end_i = len(failures) - 1
        incidents.append((keys[start_i], keys[end_i], end_i - start_i + 1))
        down_minutes += (end_i - start_i + 1)
        for j in range(start_i, end_i + 1):
            d = keys[j].date()
            per_day_down[d] = per_day_down.get(d, 0) + 1

    availability = 1.0 - (down_minutes / max(total_minutes, 1))
    availability_pct = round(availability * 100.0, 3)

    # Response stats (seconds)
    resp_s_values = [(v / 1000.0) for v in per_min_dur_ms]
    avg_resp_s = (sum(resp_s_values) / len(resp_s_values)) if resp_s_values else None
    p50_resp_s = percentile(resp_s_values, 50.0)
    p95_resp_s = percentile(resp_s_values, 95.0)
    p99_resp_s = percentile(resp_s_values, 99.0)

    # Daily rollup
    daily = []
    for d in sorted(per_day_total.keys()):
        t = per_day_total.get(d, 0)
        dn = per_day_down.get(d, 0)
        pct = 100.0 * (1.0 - (dn / max(t, 1)))
        daily.append((d, round(pct, 3), t, dn))

    # Downsample chart rows
    chart_rows = downsample_series(keys, per_min_succ, resp_s_values, DOWNSAMPLE_MIN)

    # MTTR / MTBF (minutes)
    if incidents:
        mttr = round(sum(m for _, _, m in incidents) / len(incidents), 2)
        # MTBF ~ (total_uptime_minutes) / incidents
        uptime_minutes = total_minutes - down_minutes
        mtbf = round(uptime_minutes / len(incidents), 2) if len(incidents) else None
    else:
        mttr = None
        mtbf = None

    return {
        "total_minutes": total_minutes,
        "down_minutes": down_minutes,
        "availability_pct": availability_pct,
        "incidents": incidents,
        "avg_resp_s": (round(avg_resp_s, 3) if avg_resp_s is not None else None),
        "p50_resp_s": (round(p50_resp_s, 3) if p50_resp_s is not None else None),
        "p95_resp_s": (round(p95_resp_s, 3) if p95_resp_s is not None else None),
        "p99_resp_s": (round(p99_resp_s, 3) if p99_resp_s is not None else None),
        "daily": daily,
        "chart_rows": chart_rows,
        "mttr_min": mttr,
        "mtbf_min": mtbf,
    }

def downsample_series(keys: List[datetime.datetime], succ_pct: List[float], resp_s: List[float], bucket_min: int) -> List[Tuple[datetime.datetime, float, float]]:
    """Return [(bucket_start_ts, succ_avg_pct, resp_avg_s)]."""
    if not keys:
        return []
    rows: List[Tuple[datetime.datetime, float, float]] = []
    b_start = keys[0]
    b_end = b_start + timedelta(minutes=bucket_min)
    s_acc: List[float] = []
    r_acc: List[float] = []

    def flush():
        if not s_acc and not r_acc:
            return
        s_avg = (sum(s_acc) / len(s_acc)) if s_acc else 0.0
        r_avg = (sum(r_acc) / len(r_acc)) if r_acc else 0.0
        rows.append((b_start, round(s_avg, 3), round(r_avg, 3)))

    for i, k in enumerate(keys):
        while k >= b_end:
            flush()
            b_start = b_end
            b_end = b_start + timedelta(minutes=bucket_min)
            s_acc = []
            r_acc = []
        if i < len(succ_pct):
            s_acc.append(succ_pct[i])
        if i < len(resp_s):
            r_acc.append(resp_s[i])
    flush()
    return rows

# ------------------------ HTML rendering ---------------------
def fmt_num(x: Optional[float], suffix: str = "") -> str:
    return "—" if x is None else f"{x:.3f}{suffix}"

def html_escape(s: str) -> str:
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;")
             .replace("'", "&#39;"))

def render_html_report(*,
                       client: str,
                       service: str,
                       company: str,
                       region: str,
                       year: int,
                       month_name: str,
                       month_num: int,
                       generated_iso: str,
                       availability_pct: float,
                       total_minutes: int,
                       down_minutes: int,
                       incidents: List[Tuple[datetime.datetime, datetime.datetime, int]],
                       avg_resp_s: Optional[float],
                       p50_resp_s: Optional[float],
                       p95_resp_s: Optional[float],
                       p99_resp_s: Optional[float],
                       mttr_min: Optional[float],
                       mtbf_min: Optional[float],
                       daily: List[Tuple[datetime.date, float, int, int]],
                       chart_rows: List[Tuple[datetime.datetime, float, float]]) -> str:
    """
    Returns a full, multi-page HTML report with:
      Page 1: Header + KPI + main time-series chart
      Page 2: Daily bars (availability per day) + response stats
      Page 3: Incident table (start/end/minutes)
      Page 4: Methodology & Disclaimer (print-ready)
    """

    # Build JS chart rows
    js_rows = ",\n            ".join(
        f"[new Date({ts.year}, {ts.month-1}, {ts.day}, {ts.hour}, {ts.minute}), {sp:.3f}, {rs:.3f}]"
        for (ts, sp, rs) in chart_rows
    )

    # Daily rows HTML
    if daily:
        daily_rows_html = "\n".join(
            f"<tr><td>{d.strftime('%Y-%m-%d')}</td><td>{pct:.3f}%</td><td>{tot}</td><td>{down}</td></tr>"
            for (d, pct, tot, down) in daily
        )
    else:
        daily_rows_html = "<tr><td colspan='4'>No data available for the selected month.</td></tr>"

    # Incidents table HTML
    if incidents:
        inc_rows_html = "\n".join(
            f"<tr><td>{st.strftime('%Y-%m-%d %H:%M')}Z</td><td>{en.strftime('%Y-%m-%d %H:%M')}Z</td><td>{mins}</td></tr>"
            for (st, en, mins) in incidents
        )
    else:
        inc_rows_html = "<tr><td colspan='3'>No downtime incidents detected under the configured FAIL_STREAK rule.</td></tr>"

    title = f"{html_escape(client)} — {html_escape(service)} Monthly Service Level Report — {month_name} {year}"
    sub = f"{html_escape(company)} • Region: {html_escape(region)} • Generated: {generated_iso}"

    kpi_avail = f"{availability_pct:.3f}%"
    kpi_resp  = fmt_num(avg_resp_s, " s")
    kpi_inc   = str(len(incidents))
    kpi_down  = str(down_minutes)

    p50 = fmt_num(p50_resp_s, " s")
    p95 = fmt_num(p95_resp_s, " s")
    p99 = fmt_num(p99_resp_s, " s")
    mttr = fmt_num(mttr_min, " min")
    mtbf = fmt_num(mtbf_min, " min")

    # Multi-page print CSS with page numbers
    html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>{title}</title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<script src="https://www.gstatic.com/charts/loader.js"></script>
<style>
  :root {{
    --brand:#0f4cbd; --ink:#101828; --muted:#475569; --line:#e5e7eb; --bg:#f8fafc; --chip:#eef2ff;
  }}
  @page {{
    size: A4;
    margin: 14mm;
    @bottom-center {{
      content: counter(page) " / " counter(pages);
      font-size: 10px; color: #64748b;
    }}
  }}
  html, body {{ background:#fff; color:var(--ink); font-family: Segoe UI, Roboto, Helvetica, Arial, sans-serif; }}
  .page {{ page-break-after: always; width: 980px; margin: 0 auto; }}
  .sheet {{ border:1px solid #e6eaf2; box-shadow:0 6px 28px rgba(0,0,0,.06); padding:22px 26px 20px; border-radius:8px; }}
  .header {{
     background:var(--brand); color:#fff; padding:18px 22px; border-radius:10px; font-weight:700; font-size:22px;
  }}
  .sub {{ color:#dbe8ff; font-size:12.5px; margin-top:4px; }}
  .meta {{ display:flex; flex-wrap:wrap; gap:16px; margin:12px 2px 2px; font-size:12px; color:#374151; }}
  .grid4 {{ display:grid; grid-template-columns:repeat(4,1fr); gap:12px; margin:14px 0; }}
  .card {{ background:#fff; border:1px solid #eef1f6; border-radius:10px; padding:12px 14px; }}
  .lbl {{ font-size:12px; color:#64748b; }}
  .val {{ margin-top:4px; font-size:19px; font-weight:700; }}
  .chart {{ height:365px; border:1px solid #eef1f6; border-radius:10px; margin:10px 0 4px; }}
  .sec  {{ font-weight:700; margin:16px 0 8px; font-size:14px; }}
  .tbl  {{ width:100%; border-collapse:collapse; }}
  .tbl th,.tbl td {{ border-bottom:1px solid #eef1f6; padding:8px 10px; text-align:left; font-size:12.5px; }}
  .chips {{ display:flex; gap:8px; flex-wrap:wrap; margin-top:8px; }}
  .chip  {{ background:var(--chip); border:1px solid #d9e0ff; padding:6px 10px; border-radius:999px; font-size:12px; color:#334155; }}
  .brandbar {{ display:flex; justify-content:space-between; align-items:center; margin-top:10px; font-size:12px; color:#3e5a92; background:#f2f6ff; border:1px solid #e3e9ff; border-radius:6px; padding:6px 10px; }}
  .foot  {{ font-size:12px; color:#4b5565; margin-top:6px; }}
  .disclaimer p {{ margin:6px 0; font-size:12.5px; line-height:1.55; color:#374151; }}
  .disclaimer h4 {{ margin:12px 0 6px; font-size:13px; }}
  .center-msg {{ height:340px; display:flex; align-items:center; justify-content:center; color:#64748b; border:1px dashed #e2e8f0; border-radius:10px; background:#fafafa; }}
</style>
<script>
google.charts.load('current', {{packages:['corechart','line']}});

function draw() {{
  var dt = new google.visualization.DataTable();
  dt.addColumn('datetime','Time (UTC)');
  dt.addColumn('number','Success % (avg)');
  dt.addColumn('number','Response (s, avg)');
  dt.addRows([
            {js_rows}
  ]);

  var options = {{
    legend: {{ position:'bottom' }},
    series: {{ 0:{{targetAxisIndex:0}}, 1:{{targetAxisIndex:1}} }},
    vAxes:  {{ 0:{{title:'Success %', viewWindow:{{min:0,max:100}} }},
              1:{{title:'Response (s)'}} }},
    hAxis:  {{ title:'Time (UTC)' }}
  }};

  var el = document.getElementById('chart');
  if (dt.getNumberOfRows() === 0) {{
    el.innerHTML = '<div class="center-msg">No chartable data available for this month.</div>';
  }} else {{
    new google.visualization.LineChart(el).draw(dt, options);
  }}
}}

if (document.readyState === 'loading') {{
  document.addEventListener('DOMContentLoaded', draw);
}} else {{
  draw();
}}
</script>
</head>
<body>
  <!-- Page 1: Summary + Main Time-Series -->
  <div class="page">
    <div class="sheet">
      <div class="header">{title}<div class="sub">{sub}</div></div>

      <div class="meta">
        <div><b>Client:</b> {html_escape(client)}</div>
        <div><b>Service:</b> {html_escape(service)}</div>
        <div><b>Month:</b> {month_name} {year}</div>
      </div>

      <div class="grid4">
        <div class="card"><div class="lbl">Availability</div><div class="val">{kpi_avail}</div></div>
        <div class="card"><div class="lbl">Average Response</div><div class="val">{kpi_resp}</div></div>
        <div class="card"><div class="lbl">Incidents</div><div class="val">{kpi_inc}</div></div>
        <div class="card"><div class="lbl">Downtime Minutes</div><div class="val">{kpi_down}</div></div>
      </div>

      <div id="chart" class="chart"></div>

      <div class="chips">
        <div class="chip">p50: {p50}</div>
        <div class="chip">p95: {p95}</div>
        <div class="chip">p99: {p99}</div>
        <div class="chip">MTTR: {mttr}</div>
        <div class="chip">MTBF: {mtbf}</div>
      </div>

      <div class="foot">Charts are downsampled to {DOWNSAMPLE_MIN}-minute buckets to keep file size reasonable. Data source: Amazon CloudWatch Synthetics.</div>
      <div class="brandbar"><div><b>{html_escape(company)}</b> · {html_escape(service)}</div><div>Page 1 of 4</div></div>
    </div>
  </div>

  <!-- Page 2: Daily rollup -->
  <div class="page">
    <div class="sheet">
      <div class="sec">Daily Availability (UTC)</div>
      <table class="tbl">
        <thead><tr><th>Date</th><th>Availability</th><th>Total Minutes</th><th>Minutes Down</th></tr></thead>
        <tbody>
          {daily_rows_html}
        </tbody>
      </table>

      <div class="foot">Daily availability is computed from the minute-level classification (fail after {FAIL_STREAK} consecutive failed minutes).</div>
      <div class="brandbar"><div><b>{html_escape(company)}</b> · {html_escape(service)}</div><div>Page 2 of 4</div></div>
    </div>
  </div>

  <!-- Page 3: Incidents -->
  <div class="page">
    <div class="sheet">
      <div class="sec">Downtime Incidents (FAIL_STREAK = {FAIL_STREAK})</div>
      <table class="tbl">
        <thead><tr><th>Start (UTC)</th><th>End (UTC)</th><th>Minutes Down</th></tr></thead>
        <tbody>
          {inc_rows_html}
        </tbody>
      </table>

      <div class="foot">Incidents are contiguous blocks of failed minutes starting after {FAIL_STREAK} consecutive failed minutes have been observed.</div>
      <div class="brandbar"><div><b>{html_escape(company)}</b> · {html_escape(service)}</div><div>Page 3 of 4</div></div>
    </div>
  </div>

  <!-- Page 4: Method & Disclaimer -->
  <div class="page">
    <div class="sheet">
      <div class="sec">Methodology & Disclaimer</div>
      <div class="disclaimer">
        <h4>Calculation</h4>
        <p><b>Minute success:</b> A minute is successful only if <i>all</i> configured canaries report SuccessPercent ≥ 100 for that minute. Otherwise the minute is marked failed.</p>
        <p><b>Downtime:</b> Downtime minutes begin accruing <b>after</b> {FAIL_STREAK} consecutive failed minutes. For example, with FAIL_STREAK={FAIL_STREAK}, a run of 5 failed minutes contributes 3 downtime minutes.</p>
        <p><b>Availability:</b> 1 − (downtime_minutes / total_minutes) × 100.</p>

        <h4>Response Time</h4>
        <p>Response times are the per-minute averages of the Synthetics <i>Duration</i> metric across all canaries. Aggregate statistics (mean, p50, p95, p99) are computed over the month.</p>

        <h4>Data Scope</h4>
        <p>Window: {month_name} {year} (UTC). Missing values indicate periods with no canary runs or metric gaps. Charts are downsampled to {DOWNSAMPLE_MIN}-minute buckets for readability.</p>

        <h4>Limitations</h4>
        <p>Results may be affected by browser/network paths, canary schedules, script logic, and AWS service behavior. This report should be interpreted alongside operational logs and alert histories.</p>

        <h4>Ownership</h4>
        <p>© {year} {html_escape(company)}. All rights reserved.</p>
      </div>

      <div class="brandbar"><div><b>{html_escape(company)}</b> · {html_escape(service)}</div><div>Page 4 of 4</div></div>
    </div>
  </div>
</body>
</html>"""
    return html

# ------------------------ Month index page -------------------
def render_month_index(*, company: str, service: str, region: str, year: int, month_name: str, items: List[Tuple[str, str]]) -> str:
    """
    items: list of (client_name, html_key_basename)
    """
    rows = "\n".join(
        f"<tr><td>{html_escape(client)}</td><td><a href=\"{html_escape(link)}\">Open report</a></td></tr>"
        for (client, link) in items
    )
    title = f"{html_escape(service)} — Monthly Reports Index — {month_name} {year}"
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>{title}</title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<style>
  body {{ font-family: Segoe UI, Roboto, Arial, sans-serif; color:#101828; background:#ffffff; padding:24px; }}
  .h1 {{ font-size:22px; font-weight:700; }}
  .sub {{ color:#475569; font-size:12.5px; margin-top:4px; }}
  table {{ width:100%; border-collapse:collapse; margin-top:14px; }}
  th,td {{ border-bottom:1px solid #e5e7eb; padding:8px 10px; text-align:left; font-size:13px; }}
</style>
</head>
<body>
  <div class="h1">{title}</div>
  <div class="sub">{html_escape(company)} • Region: {html_escape(region)}</div>
  <table>
    <thead><tr><th>Client</th><th>Report</th></tr></thead>
    <tbody>
      {rows}
    </tbody>
  </table>
</body>
</html>"""

# ------------------------ Handler ----------------------------
def handler(event, ctx):
    log("[info] Generating monthly uptime reports (HTML + CSV + JSONL)")

    # Time window
    start, end = month_window_utc()
    year = int(start.strftime("%Y"))
    month_num = int(start.strftime("%m"))
    month_str = start.strftime("%m")
    month_name = start.strftime("%B")
    generated_at = datetime.datetime.now(timezone.utc).isoformat()

    # Output prefix for this run
    month_prefix = f"{S3_PREFIX}/{year}/{month_str}/"

    # If no clients, fail loudly (prevents silent empty reports)
    if not CLIENTS:
        log("[error] CLIENTS_JSON is empty or invalid; no reports generated.")
        return {"status": "error", "reason": "no_clients"}

    # Collect results for JSONL and year rollup
    results = []
    index_items = []  # (client, html-file-name)

    for client, canaries in CLIENTS.items():
        if not canaries:
            log(f"[warn] client '{client}' has no canaries; skipping.")
            continue

        log(f"[info] client '{client}' => {len(canaries)} canary/canaries; {start.isoformat()}..{end.isoformat()} UTC")

        # Merge minute data across canaries
        minute_points = merge_minute_points(canaries, start, end)

        # Compute summary/metrics
        summary = compute_summary(minute_points)

        # Per-client CSV
        per_client_csv_key = f"{month_prefix}General {SERVICE} Service Level Report {year}-{month_str}-{client}.csv"
        write_rows_csv(S3_BUCKET, per_client_csv_key, [{
            "year": str(year),
            "month_num": month_num,
            "month_name": month_name,
            "client": client,
            "service": SERVICE,
            "availability_pct": summary["availability_pct"],
            "response_time_sec": (summary["avg_resp_s"] if summary["avg_resp_s"] is not None else ""),
            "generated_at_utc": generated_at,
        }])
        log(f"[info] wrote CSV: s3://{S3_BUCKET}/{per_client_csv_key}")

        # Per-client HTML
        html_body = render_html_report(
            client=client,
            service=SERVICE,
            company=COMPANY,
            region=REGION,
            year=year,
            month_name=month_name,
            month_num=month_num,
            generated_iso=generated_at,
            availability_pct=summary["availability_pct"],
            total_minutes=summary["total_minutes"],
            down_minutes=summary["down_minutes"],
            incidents=summary["incidents"],
            avg_resp_s=summary["avg_resp_s"],
            p50_resp_s=summary["p50_resp_s"],
            p95_resp_s=summary["p95_resp_s"],
            p99_resp_s=summary["p99_resp_s"],
            mttr_min=summary["mttr_min"],
            mtbf_min=summary["mtbf_min"],
            daily=summary["daily"],
            chart_rows=summary["chart_rows"],
        )
        html_file = f"General {SERVICE} Service Level Report {year}-{month_str}-{client}.html"
        per_client_html_key = f"{month_prefix}{html_file}"
        put_s3_text(S3_BUCKET, per_client_html_key, html_body, "text/html; charset=utf-8")
        log(f"[info] wrote HTML: s3://{S3_BUCKET}/{per_client_html_key}")
        index_items.append((client, html_file))

        # JSONL (without chart rows for weight)
        results.append({
            "client": client,
            "year": year,
            "month_num": month_num,
            "month_name": month_name,
            "generated_at_utc": generated_at,
            "minutes_total": summary["total_minutes"],
            "minutes_down": summary["down_minutes"],
            "availability_pct": summary["availability_pct"],
            "avg_resp_s": summary["avg_resp_s"],
            "p50_resp_s": summary["p50_resp_s"],
            "p95_resp_s": summary["p95_resp_s"],
            "p99_resp_s": summary["p99_resp_s"],
            "incidents": [
                {"start": st.isoformat(), "end": en.isoformat(), "minutes": mins}
                for (st, en, mins) in summary["incidents"]
            ],
            "mttr_min": summary["mttr_min"],
            "mtbf_min": summary["mtbf_min"],
        })

    # Month JSONL snapshot
    put_s3_text(
        S3_BUCKET,
        f"{month_prefix}uptime.jsonl",
        "\n".join(json.dumps(r) for r in results),
        "application/x-ndjson; charset=utf-8",
    )
    log(f"[info] wrote JSONL: s3://{S3_BUCKET}/{month_prefix}uptime.jsonl")

    # Create month index page
    if index_items:
        index_html = render_month_index(
            company=COMPANY,
            service=SERVICE,
            region=REGION,
            year=year,
            month_name=month_name,
            items=index_items,
        )
        put_s3_text(S3_BUCKET, f"{month_prefix}index.html", index_html, "text/html; charset=utf-8")
        log(f"[info] wrote month index: s3://{S3_BUCKET}/{month_prefix}index.html")

    # Year rollup CSV + stable copy
    year_key = f"{S3_PREFIX}/{year}/cdp-year-{year}.csv"
    year_rows = load_year_rows(S3_BUCKET, year_key)
    if results:
        r0 = results[0]
        year_rows[month_num] = {
            "year": str(year),
            "month_num": month_num,
            "month_name": month_name,
            "client": r0["client"],  # first client for backward compatibility
            "service": SERVICE,
            "availability_pct": r0["availability_pct"],
            "response_time_sec": (r0["avg_resp_s"] if r0["avg_resp_s"] is not None else ""),
            "generated_at_utc": r0["generated_at_utc"],
        }
    write_rows_csv(S3_BUCKET, year_key, year_rows)
    s3.copy_object(
        Bucket=S3_BUCKET,
        CopySource={"Bucket": S3_BUCKET, "Key": year_key},
        Key=f"{S3_PREFIX}/current/cdp-year.csv",
    )
    log("[info] year rollup updated")

    return {"status": "ok", "prefix": month_prefix, "results": results}
