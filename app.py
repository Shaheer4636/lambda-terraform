# app.py  (PDF removed: HTML + CSV only)
# --------------------------------------------------------------------
# Uptime Report Generator (S3 artifacts -> HTML + CSV)
# - Writes CSVs and a single HTML report to S3.
# - Disclaimer notes made smaller; second page matches first page size.
# --------------------------------------------------------------------

import os, json, re, csv, io, datetime, random
from datetime import timezone, timedelta
from string import Template
from typing import Optional, List, Dict, Any
import boto3

# ----------------------------- Config -------------------------------
s3 = boto3.client("s3")

ART_BUCKET     = os.environ["ARTIFACT_BUCKET"]
ART_PREFIX     = os.environ["ARTIFACT_PREFIX"].rstrip("/")
REPORTS_BUCKET = os.environ["REPORTS_BUCKET"]
REPORTS_PREFIX = os.environ.get("REPORTS_PREFIX", "uptime").strip("/")

COMPANY = os.environ.get("COMPANY_NAME", "Company")
SERVICE = os.environ.get("SERVICE_NAME", "Service")
CLIENT  = os.environ.get("CLIENT_NAME", "Client")
ONLY_BROWSER   = os.environ.get("ONLY_BROWSER", "ANY").upper()
SLO_TARGET     = float(os.environ.get("SLO_TARGET", "99.9"))
FAIL_STREAK    = int(os.environ.get("FAIL_STREAK", "3"))
TREAT_MISSING  = os.environ.get("TREAT_MISSING", "false").lower() == "true"
BRAND_STRAPLINE = os.environ.get("BRAND_STRAPLINE", "Service Level Report")

# tolerant matcher: .../<YYYY>/<MM>/<DD>/<HH>/<MM>-<SS>-<MS>/(<BROWSER>/)?<FILE>
PAT_ANY = re.compile(
    r"^%s/(?P<y>\d{4})/(?P<m>\d{2})/(?P<d>\d{2})/(?P<h>\d{2})/(?P<min>\d{2})-(?P<s>\d{2})-(?P<ms>\d{3})(?:/(?P<br>[^/]+))?/(?P<file>[^/]+)$"
    % re.escape(ART_PREFIX)
)

def log(m: str) -> None:
    print(m, flush=True)

# -------------------------- Date Helpers ----------------------------
def first_of_month(y: int, m: int) -> datetime.datetime:
    return datetime.datetime(y, m, 1, tzinfo=timezone.utc)

def last_of_month(y: int, m: int) -> datetime.datetime:
    return (datetime.datetime(y+1,1,1,tzinfo=timezone.utc)-timedelta(seconds=1)) if m==12 else \
           (datetime.datetime(y,m+1,1,tzinfo=timezone.utc)-timedelta(seconds=1))

def month_window_utc(now: Optional[datetime.datetime]=None):
    now = now or datetime.datetime.now(timezone.utc)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return start, now

def pad_month_days(start_dt: datetime.datetime, end_dt: datetime.datetime, rows: List[Dict[str, Any]]):
    have = {r["day"]: r for r in rows}
    out = []
    cur = start_dt.date().replace(day=1)
    stop = end_dt.date()
    while cur <= stop:
        key = cur.strftime("%Y-%m-%d")
        out.append(have.get(key, {"day": key, "avail": None, "resp_s": None}))
        cur += timedelta(days=1)
    return out

def _num_or_null(v: Optional[float]) -> str:
    return "null" if (v is None) else f"{float(v):.3f}"

# --------------------------- S3 Helpers -----------------------------
def _list_prefix(prefix: str):
    pag = s3.get_paginator("list_objects_v2")
    for page in pag.paginate(Bucket=ART_BUCKET, Prefix=prefix, PaginationConfig={"PageSize": 1000}):
        for obj in page.get("Contents", []):
            yield obj["Key"]

def _put_csv(bucket: str, key: str, rows: List[Dict[str, Any]], cols: List[str]) -> None:
    out = io.StringIO()
    w = csv.DictWriter(out, fieldnames=cols)
    w.writeheader()
    for r in rows:
        w.writerow(r)
    s3.put_object(Bucket=bucket, Key=key, Body=out.getvalue().encode("utf-8"), ContentType="text/csv; charset=utf-8")

def _s3_put_text(bucket: str, key: str, text: str) -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"), ContentType="text/plain; charset=utf-8")

# -------------------------- Parsers ---------------------------------
def _status_from_text(txt: str) -> Optional[bool]:
    u = txt.upper()
    if "FAILED" in u or "ERROR" in u or "TIMEOUT" in u: return False
    if "PASSED" in u or "SUCCEEDED" in u or "SUCCESS" in u: return True
    return None

def _parse_z(s: Optional[str]) -> Optional[datetime.datetime]:
    if not s: return None
    s = s.strip()
    if s.endswith("Z"): s = s[:-1] + "+00:00"
    try: return datetime.datetime.fromisoformat(s)
    except Exception: return None

def is_synthetics_json(name: str) -> bool:
    nl = name.lower()
    return nl.startswith("syntheticsreport") and nl.endswith(".json")

def parse_synthetics_json(data: dict, fname: str):
    status = None
    for p in (data.get("status"),
              data.get("overallStatus"),
              (data.get("customerScript") or {}).get("status"),
              (data.get("script") or {}).get("status")):
        if p is not None:
            s = _status_from_text(str(p))
            if s is not None: status = s; break

    dur_ms = None
    for p in ((data.get("customerScript") or {}).get("duration"),
              (data.get("customerScript") or {}).get("durationMs"),
              data.get("durationMs")):
        if isinstance(p,(int,float)): dur_ms = float(p); break

    if dur_ms is None:
        st = _parse_z((data.get("customerScript") or {}).get("startTime")) or _parse_z(data.get("startTime"))
        en = _parse_z((data.get("customerScript") or {}).get("endTime"))   or _parse_z(data.get("endTime"))
        dur_ms = (en-st).total_seconds()*1000.0 if st and en and en>=st else 0.0

    if status is None: status = _status_from_text(fname)
    if status is None: status = True
    return status, dur_ms

def parse_log_text(text: str):
    status = _status_from_text(text)
    dur_ms = 0.0
    m = re.search(r"(\d+(?:\.\d+)?)\s*ms", text, re.I)
    if m: dur_ms = float(m.group(1))
    else:
        m = re.search(r"(\d+(?:\.\d+)?)\s*s", text, re.I)
        if m: dur_ms = float(m.group(1))*1000.0
    if status is None: status = True
    return status, dur_ms

def parse_har_html(html: str):
    statuses = [int(x) for x in re.findall(r'"status"\s*:\s*(\d{3})', html)]
    status = False if statuses and any(s >= 400 for s in statuses) else True
    times = [float(x) for x in re.findall(r'"time"\s*:\s*([0-9.]+)', html)]
    dur_ms = sum(times)/len(times) if times else 0.0
    return status, dur_ms

# ---------------------- Artifact Scanning ---------------------------
def iter_objects_for_month(y: int, mo: int, start: datetime.datetime, end: datetime.datetime):
    ystr = f"{y:04d}"; mstr = f"{mo:02d}"
    prefixes = [f"{ART_PREFIX}/{ystr}/{mstr}/", f"{ART_PREFIX}/{ystr}/"]
    yielded = False
    for p in prefixes:
        for key in _list_prefix(p):
            m = PAT_ANY.match(key)
            if not m: continue
            Y=int(m.group("y")); M=int(m.group("m")); D=int(m.group("d"))
            H=int(m.group("h")); MIN=int(m.group("min"))
            ts = datetime.datetime(Y,M,D,H,MIN,tzinfo=timezone.utc)
            if ts < start or ts > end: continue
            br = m.group("br")
            if ONLY_BROWSER!="ANY":
                if br is None or br.upper()!=ONLY_BROWSER: continue
            yield ts, key, (br.upper() if br else "N/A"), m.group("file")
            yielded=True
        if yielded: return
    # hard fallback: exact day/hour walk
    days = (last_of_month(y, mo) - first_of_month(y, mo)).days + 1
    for d in range(1, days+1):
        dd=f"{d:02d}"
        for h in range(0,24):
            hh=f"{h:02d}"
            p=f"{ART_PREFIX}/{ystr}/{mstr}/{dd}/{hh}/"
            for key in _list_prefix(p):
                m = PAT_ANY.match(key)
                if not m: continue
                Y=int(m.group("y")); M=int(m.group("m")); D=int(m.group("d"))
                H=int(m.group("h")); MIN=int(m.group("min"))
                ts = datetime.datetime(Y,M,D,H,MIN,tzinfo=timezone.utc)
                if ts < start or ts > end: continue
                br = m.group("br")
                if ONLY_BROWSER!="ANY":
                    if br is None or br.upper()!=ONLY_BROWSER: continue
                yield ts, key, (br.upper() if br else "N/A"), m.group("file")

def scan_window(y: int, mo: int, start: datetime.datetime, end: datetime.datetime):
    per_min_flags={}; per_min_ms={}; sampled=[]
    for minute_dt, key, browser, fname in iter_objects_for_month(y, mo, start, end):
        sampled.append(key)
        try:
            body = s3.get_object(Bucket=ART_BUCKET, Key=key)["Body"].read()
        except Exception as e:
            log(f"[warn] get_object failed {key}: {type(e).__name__}"); continue
        name=fname.lower(); succ=None; ms=0.0
        try:
            if is_synthetics_json(fname):
                succ, ms = parse_synthetics_json(json.loads(body.decode("utf-8",errors="ignore")), fname)
            elif name.endswith("-log.txt"):
                succ, ms = parse_log_text(body.decode("utf-8",errors="ignore"))
            elif name == "httprequestsreport.json":
                data=json.loads(body.decode("utf-8",errors="ignore"))
                codes=[(r.get("response") or {}).get("statusCode") for r in (data.get("requests") or [])]
                codes=[c for c in codes if isinstance(c,int)]
                succ = False if any(c>=400 for c in codes) else True
            elif name.endswith("results.har.html"):
                succ, ms = parse_har_html(body.decode("utf-8",errors="ignore"))
            else:
                continue
        except Exception as e:
            log(f"[warn] parse failed {key}: {type(e).__name__}"); continue
        if succ is None: continue
        per_min_flags.setdefault(minute_dt,[]).append(bool(succ))
        if ms: per_min_ms.setdefault(minute_dt,[]).append(float(ms))
    agg_ok = {t: all(flags) for t,flags in per_min_flags.items()}
    agg_ms = {t: (sum(v)/len(v) if v else 0.0) for t,v in per_min_ms.items()}
    if TREAT_MISSING:
        cur=start.replace(second=0,microsecond=0); endr=end.replace(second=0,microsecond=0)
        while cur<=endr:
            if cur not in agg_ok: agg_ok[cur]=False; agg_ms.setdefault(cur,0.0)
            cur += timedelta(minutes=1)
    return agg_ok, agg_ms, sampled[:250]

# -------------------------- Reductions ------------------------------
def hourly_reduce(agg_ok: Dict[datetime.datetime,bool], agg_ms: Dict[datetime.datetime,float]):
    buckets={}
    for t, ok in agg_ok.items():
        hr=t.replace(minute=0,second=0,microsecond=0)
        buckets.setdefault(hr,[]).append((ok,agg_ms.get(t,0.0)))
    out=[]
    for hr, rows in sorted(buckets.items()):
        ok_pct=(sum(1 for ok,_ in rows if ok)/len(rows))*100.0 if rows else 0.0
        ms_avg=(sum(ms for _,ms in rows)/len(rows)) if rows else 0.0
        out.append({"hour":hr,"success_avg":ok_pct,"response_ms_avg":ms_avg})
    return out

def month_cumulative(agg_ok: Dict[datetime.datetime,bool], agg_ms: Dict[datetime.datetime,float]):
    per_day={}
    for t, ok in agg_ok.items():
        d=t.date()
        per_day.setdefault(d,{"up":0,"tot":0,"ms_sum":0.0,"ms_ct":0})
        per_day[d]["up"] += 1 if ok else 0
        per_day[d]["tot"]+= 1
        ms=agg_ms.get(t,0.0)
        if ms: per_day[d]["ms_sum"]+=ms; per_day[d]["ms_ct"]+=1
    series=[]; cu=ct=0; cms=cct=0
    for d in sorted(per_day.keys()):
        cu += per_day[d]["up"]; ct += per_day[d]["tot"]
        cms += per_day[d]["ms_sum"]; cct += per_day[d]["ms_ct"]
        avail=(cu/ct)*100.0 if ct else 0.0
        resp=(cms/cct)/1000.0 if cct else 0.0
        series.append({"day": d.strftime("%Y-%m-%d"), "avail": avail, "resp_s": resp})
    return series

def detect_incidents(agg_ok: Dict[datetime.datetime,bool]):
    mins=sorted(agg_ok.keys()); inc=[]; streak=0; start=None
    for m in mins:
        if not agg_ok[m]:
            streak+=1; start = start or m
        else:
            if streak>=FAIL_STREAK: inc.append({"start":start,"end":m-timedelta(minutes=1),"duration_minutes":streak})
            streak=0; start=None
    if streak>=FAIL_STREAK and mins:
        inc.append({"start":start,"end":mins[-1],"duration_minutes":streak})
    return inc

# ---------------------- YTD (Jan -> current) -----------------------
def _csv_rows_from_s3_safe(key: str) -> Optional[List[Dict[str,str]]]:
    try:
        head = s3.head_object(Bucket=REPORTS_BUCKET, Key=key)
        if head.get("ContentLength", 0) > 10 * 1024 * 1024:
            return None
        body = s3.get_object(Bucket=REPORTS_BUCKET, Key=key)["Body"].read().decode("utf-8")
    except Exception:
        return None
    return list(csv.DictReader(io.StringIO(body)))

def _month_label(y: int, m: int) -> str:
    return datetime.datetime(y, m, 1, tzinfo=timezone.utc).strftime("%B %Y")

def _read_month_summary_from_csv(y: int, m: int) -> Optional[Dict[str,float]]:
    base = f"{REPORTS_PREFIX}/{y}/{m:02d}/"
    rows = _csv_rows_from_s3_safe(f"{base}uptime-hour.csv")
    if rows is None:
        rows = _csv_rows_from_s3_safe(f"{base}uptime-minute.csv")
    if not rows:
        return None
    try:
        avgs=[float(r["availability_pct"]) for r in rows if (r.get("availability_pct") or "").strip()!=""]
        rsps=[float(r.get("avg_response_sec") or 0.0) for r in rows]
        if not avgs:
            return None
        return {"availability": sum(avgs)/len(avgs), "resp_s": (sum(rsps)/len(rsps) if rsps else 0.0)}
    except Exception:
        return None

def summarize_month_from_artifacts_quick(y: int, m: int, sample_limit: int=400) -> Optional[Dict[str,float]]:
    prefix = f"{ART_PREFIX}/{y:04d}/{m:02d}/"
    passed = failed = 0
    sample = []
    seen = 0

    token = None
    while True:
        if token:
            resp = s3.list_objects_v2(Bucket=ART_BUCKET, Prefix=prefix, ContinuationToken=token, MaxKeys=1000)
        else:
            resp = s3.list_objects_v2(Bucket=ART_BUCKET, Prefix=prefix, MaxKeys=1000)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            mobj = PAT_ANY.match(key)
            if not mobj:
                continue
            br = mobj.group("br")
            if ONLY_BROWSER != "ANY":
                if br is None or br.upper() != ONLY_BROWSER:
                    continue
            fname = mobj.group("file").lower()
            if fname.startswith("syntheticsreport-") and fname.endswith(".json"):
                seen += 1
                if "-passed" in fname:
                    passed += 1
                elif "-failed" in fname:
                    failed += 1
                if len(sample) < sample_limit:
                    sample.append(key)
                else:
                    j = random.randint(1, seen)
                    if j <= sample_limit:
                        sample[random.randint(0, sample_limit-1)] = key
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    total = passed + failed
    if total == 0:
        return None

    dur_ms_vals = []
    for k in sample:
        try:
            body = s3.get_object(Bucket=ART_BUCKET, Key=k)["Body"].read()
            data = json.loads(body.decode("utf-8", errors="ignore"))
            _, dur_ms = parse_synthetics_json(data, k.rsplit("/", 1)[-1])
            if isinstance(dur_ms, (int, float)):
                dur_ms_vals.append(float(dur_ms))
        except Exception:
            continue

    resp_s = (sum(dur_ms_vals) / len(dur_ms_vals) / 1000.0) if dur_ms_vals else 0.0
    availability = (passed / total) * 100.0
    return {"availability": availability, "resp_s": resp_s}

def build_year_summary_ytd(current_avail: float, current_resp_s: float):
    now = datetime.datetime.now(timezone.utc)
    y = now.year
    months = list(range(1, now.month + 1))

    chart, table, csvout = [], [], []
    for m in months:
        if m == now.month:
            avail, resp = current_avail, current_resp_s
        else:
            s = _read_month_summary_from_csv(y, m) or summarize_month_from_artifacts_quick(y, m)
            if s:
                avail, resp = s["availability"], s["resp_s"]
            else:
                avail, resp = None, None

        chart.append({"month": f"{y}-{m:02d}", "availability": avail, "resp_s": resp})
        table.append({"label": _month_label(y, m), "availability": avail, "resp_s": resp})
        csvout.append({
            "month": f"{y}-{m:02d}",
            "availability_pct": "" if avail is None else f"{avail:.3f}",
            "avg_response_sec": "" if resp is None else f"{resp:.3f}",
        })
    return chart, table, csvout

# --------------------------- HTML -----------------------------------
def _disclaimer_html(company: str, year: str, strapline: str) -> str:
    items = [
        ("General", f"This report is provided by {company} for information purposes. By accessing it you agree to the terms herein."),
        ("Confidentiality, Distribution or Presentation", "This document is confidential and may not be reproduced or shared without prior written consent."),
        ("Forward-Looking Statements", "Any forward-looking statements are based on current assumptions and subject to risks and uncertainties; actual outcomes may differ."),
        ("Past Performance", "Past performance is not indicative of future results."),
        ("No Reliance, No Update, and Use of Information", "Information is provided as-is, may be based on third-party inputs, and may be updated without notice."),
        ("No Advice", "Nothing herein constitutes legal, financial, investment, accounting, or tax advice."),
        ("Current Data", "Unless noted otherwise, the data presented is as of the date of this report."),
        ("Logos, Trademarks, and Copyrights", "All trademarks and logos are the property of their respective owners."),
        ("Financial Services and Markets Act", "Nothing in this report constitutes a financial promotion for the purposes of applicable financial services laws."),
    ]
    li = "\n".join(
        f"<div class='disc-item'><div class='disc-title'>{t}</div><div class='disc-text'>{p}</div></div>"
        for (t, p) in items
    )
    t = Template("""
    <div class="page page-disclaimers">
      <div class="sheet">
        <div class="disc-h1">Disclaimers</div>
        <div class="disc-body">
          $list_items
        </div>

        <div class="brandbar">
          <div class="brandbar-left">
            <span class="brandbar-company">$company</span>
            <span class="brandbar-sep"> | </span>
            <span class="brandbar-tag">$strapline</span>
          </div>
          <div class="brandbar-page">1</div>
        </div>

        <div class="legal-footer">Confidential and Proprietary, © $year $company.</div>
      </div>
    </div>
    """)
    return t.substitute(list_items=li, company=company, year=year, strapline=strapline)

def render_html(meta, minute_rows, hour_rows, month_cum_rows_padded, per_canary, year_chart_rows, year_table_rows, incidents, generated_at):
    min_js = ",\n      ".join(
        "[new Date('{}Z'), {:.3f}, {:.3f}]".format(r["ts"], r["avail"], r["resp_s"]) for r in minute_rows
    )
    hr_js  = ",\n      ".join(
        "[new Date('{}Z'), {:.3f}, {:.3f}]".format(r["hour"], r["avail"], r["resp_s"]) for r in hour_rows
    )
    mc_js  = ",\n      ".join(
        "['{}', {}, {}]".format(r["day"], _num_or_null(r["avail"]), _num_or_null(r["resp_s"])) for r in month_cum_rows_padded
    )
    year_js = ",\n      ".join(
        "['{}', {}, {}]".format(r["month"], _num_or_null(r["availability"]), _num_or_null(r["resp_s"])) for r in year_chart_rows
    )
    per_js = ",\n      ".join("['{}', {:.3f}]".format(p["name"], p["pct"]) for p in per_canary)

    table_rows_html = "".join(
        "<tr style='{}'><td>{}</td><td align='right'>{}</td><td align='right'>{}</td></tr>".format(
            "background:#f3f3f3;" if i%2 else "",
            row["label"],
            ("{:.3f}%".format(row["availability"]) if row["availability"] is not None else "—"),
            ("{:.3f}".format(row["resp_s"]) if row["resp_s"] is not None else "—")
        )
        for i, row in enumerate(year_table_rows)
    )

    footer_text = os.environ.get(
        "LEGAL_FOOTER_TEXT",
        f"Confidential and Proprietary, © {meta['year']} {meta['company']}"
    )

    page = Template("""<!doctype html>
<html><head><meta charset="utf-8"/>
<title>$title</title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<script src="https://www.gstatic.com/charts/loader.js"></script>
<style>
  :root {
    --brand:#0f4cbd;
    --ink:#111;
    --muted:#555;
    --card:#fff;
    --bg:#f4f6fa;
    --line:#e3e8f3;
    --bar:#eef2fb;
  }
  html,body { height:100%; }
  body { font-family: Segoe UI, Roboto, Arial, sans-serif; background:var(--bg); margin:0; color:var(--ink); }
  .page { page-break-after: always; position: relative; min-height: 96vh; background:transparent; }
  @media print {
    body { background:#fff; }
    .page { page-break-after: always; }
    .sheet, .sheet-report { box-shadow:none; border:none; margin:0; }
  }
  /* First-page container */
  .sheet {
    width: 980px; margin: 42px auto 56px;
    background: #fff; border: 1px solid var(--line);
    box-shadow: 0 1px 0 #f6f8ff, 0 12px 28px rgba(0,0,0,.06);
    border-radius: 2px; padding: 28px 36px 110px; position: relative;
  }
  /* Second page must match first-page size */
  .sheet-report {
    width: 980px; margin: 42px auto 56px;
    background: #fff; border: 1px solid var(--line);
    box-shadow: 0 1px 0 #f6f8ff, 0 12px 28px rgba(0,0,0,.06);
    border-radius: 2px; padding: 28px 36px 110px; position: relative;
  }

  /* Disclaimer notes smaller */
  .disc-h1 { font-size: 28px; font-weight: 800; margin: 4px 0 14px; letter-spacing: .2px; }
  .disc-body { max-width: 880px; }
  .disc-item { margin: 8px 0 10px; }
  .disc-title { font-weight: 700; font-size: 12px; margin-bottom: 2px; }
  .disc-text { color: var(--muted); font-size: 11px; line-height: 1.45; }

  .brandbar { position: absolute; left: 24px; right: 24px; bottom: 40px; background: var(--bar); border: 1px solid var(--line); border-radius: 2px; height: 28px; display: flex; align-items: center; justify-content: space-between; padding: 0 10px; }
  .brandbar-company { font-weight: 700; font-size: 12px; color:#24324b; }
  .brandbar-sep { color:#8da2c1; }
  .brandbar-tag { font-size: 12px; color:#3e5a92; }
  .brandbar-page { font-size: 12px; color:#6b7fa3; }
  .legal-footer { position: absolute; left: 0; right: 0; bottom: 8px; text-align: center; color: #4b5565; font-size: 12px; }

  .header { background:var(--brand); color:#fff; padding:18px 22px; font-weight:600; font-size:20px; border-bottom:1px solid var(--line); border-radius: 8px; }
  .sub { color:#e9f0ff; font-size:13px; font-weight:400; margin-top:4px; }
  .wrap { padding:18px 2px 0; }

  .cards { display:grid; grid-template-columns: repeat(4, 1fr); gap:12px; margin:12px 0; }
  .card { background:#fff; border-radius:12px; padding:14px; box-shadow:0 2px 6px #0001; border: 1px solid #eef0f4; }
  .biggrid { display:grid; grid-template-columns: 1fr 1fr; gap:12px; }
  .chart { height:360px; }
  .sec-title { font-weight:700; margin:6px 0 8px; color:#222; font-size:14px; }
  .foot { font-size:12px; color:#444; margin-top:8px; }
  .tbl { width:100%; border-collapse:collapse; }
  .tbl th, .tbl td { padding:8px 10px; }
  .tbl th { background:#e7eaef; text-align:left; }
</style>
<script>
google.charts.load('current', {packages:['corechart','table']});
google.charts.setOnLoadCallback(function() {
  var dualAxisOpts = {legend:{position:'bottom'},
                      series: {0:{targetAxisIndex:0}, 1:{targetAxisIndex:1}},
                      vAxes:  {0:{title:'%'},            1:{title:'sec'}}};
  function lineDT(container, rows) {
    var dt = new google.visualization.DataTable();
    dt.addColumn('datetime','Time');
    dt.addColumn('number','Availability %');
    dt.addColumn('number','Response (s)');
    dt.addRows(rows);
    new google.visualization.LineChart(document.getElementById(container)).draw(dt, dualAxisOpts);
  }
  function lineStr(container, rows, xlabel) {
    var dt = new google.visualization.DataTable();
    dt.addColumn('string', xlabel);
    dt.addColumn('number','Availability %');
    dt.addColumn('number','Response (s)');
    dt.addRows(rows);
    new google.visualization.LineChart(document.getElementById(container)).draw(dt, dualAxisOpts);
  }
  lineDT('m_chart', [$min_js]);
  lineDT('h_chart', [$hr_js]);
  lineStr('mc_chart', [$mc_js], 'Day');
  lineStr('y_chart', [$year_js], 'Month');
  var p = new google.visualization.DataTable();
  p.addColumn('string','Canary'); p.addColumn('number','Availability %'); p.addRows([$per_js]);
  new google.visualization.ColumnChart(document.getElementById('p_chart')).draw(p, {legend:{position:'none'}});
});
</script>
</head>
<body>

  $disclaimers_html

  <div class="page">
    <div class="sheet-report">
      <div class="header">$service Monthly Uptime — $month_name $year
        <div class="sub">
          $company • Client: <b>$client</b> • SLO: $slo% • Source: $source • Generated: $generated_at
        </div>
      </div>

      <div class="wrap">
        <div class="cards">
          <div class="card"><div style="color:#666;">OVERALL AVAILABILITY</div><div style="font-size:26px; font-weight:700;">$availability%</div></div>
          <div class="card"><div style="color:#666;">TOTAL DOWNTIME</div><div style="font-size:26px; font-weight:700;">$downtime_min min</div></div>
          <div class="card"><div style="color:#666;">INCIDENTS</div><div style="font-size:26px; font-weight:700;">$incidents</div></div>
          <div class="card"><div style="color:#666;">AVG RESPONSE</div><div style="font-size:26px; font-weight:700;">$avg_resp_s s</div></div>
        </div>

        <div class="biggrid">
          <div class="card">
            <div class="sec-title">A) Minute-by-minute (month-to-date)</div>
            <div class="chart" id="m_chart"></div>
          </div>
          <div class="card">
            <div class="sec-title">B) Hourly trend (month-to-date)</div>
            <div class="chart" id="h_chart"></div>
          </div>
        </div>

        <div class="biggrid" style="margin-top:12px;">
          <div class="card">
            <div class="sec-title">C) Month-to-date by day (1 to today)</div>
            <div class="chart" id="mc_chart"></div>
          </div>
          <div class="card">
            <div class="sec-title">D) Year-to-date (January to current month)</div>
            <div class="chart" id="y_chart"></div>
          </div>
        </div>

        <div class="card" style="margin-top:12px;">
          <div class="sec-title">Per-canary availability (this month)</div>
          <div id="p_chart" style="height:280px;"></div>
        </div>

        <div class="card" style="margin-top:12px;">
          <div class="sec-title">Year-to-date table</div>
          <table class="tbl">
            <tr><th>Month</th><th align="right">Availability</th><th align="right">Response Time (sec.)</th></tr>
            $table_rows_html
          </table>
        </div>

        <div class="card" style="margin-top:12px;">
          <div class="sec-title">Incidents (≥ $fail_streak consecutive failed minutes)</div>
          <table border="0" cellpadding="6" cellspacing="0" style="width:100%; border-collapse:collapse;">
            <tr style="background:#f5f7ff"><th align="left">Start (UTC)</th><th align="left">End (UTC)</th><th align="right">Minutes</th></tr>
            $incidents_rows
          </table>
          <div class="foot">Missing minutes are $missing_policy.</div>
        </div>
      </div>

      <div class="legal-footer">$footer_text</div>
    </div>
  </div>

</body></html>
""")
    incidents_rows = "".join(
        f"<tr><td>{i['start'].strftime('%Y-%m-%d %H:%M')}</td>"
        f"<td>{i['end'].strftime('%Y-%m-%d %H:%M')}</td>"
        f"<td align='right'>{i['duration_minutes']}</td></tr>"
        for i in incidents
    )

    html = page.substitute(
        title=f"{meta['service']} Monthly Uptime — {meta['month_name']} {meta['year']}",
        disclaimers_html=_disclaimer_html(meta['company'], meta['year'], BRAND_STRAPLINE),
        company=meta["company"],
        client=meta["client"],
        service=meta["service"],
        month_name=meta["month_name"],
        year=meta["year"],
        slo=f"{meta['slo']:.3f}",
        source=(ONLY_BROWSER if ONLY_BROWSER != "ANY" else "artifact"),
        generated_at=generated_at,
        availability=f"{meta['availability']:.3f}",
        downtime_min=meta["downtime_min"],
        incidents=meta["incidents"],
        avg_resp_s=f"{meta['avg_resp_s']:.3f}",
        footer_text=footer_text,
        min_js=min_js,
        hr_js=hr_js,
        mc_js=mc_js,
        year_js=year_js,
        per_js=per_js,
        table_rows_html=table_rows_html,
        fail_streak=FAIL_STREAK,
        missing_policy=("treated as failures" if TREAT_MISSING else "ignored"),
        incidents_rows=incidents_rows
    )
    return html

# --------------------------- Handler --------------------------------
def handler(event, context):
    now = datetime.datetime.now(timezone.utc)
    start_m, now_m = month_window_utc(now)
    y = start_m.year; mo = start_m.month
    end_m = now_m

    # 1) Scan current month (MTD)
    agg_ok, agg_ms, _sampled = scan_window(y, mo, start_m, end_m)
    observed = sorted(agg_ok.keys()); total_obs=len(observed)
    log(f"[info] observed minutes this month: {total_obs}")
    up_obs=sum(1 for t in observed if agg_ok[t])
    availability=(up_obs/total_obs)*100.0 if total_obs else 0.0
    incidents=detect_incidents(agg_ok)
    downtime_min=sum(i["duration_minutes"] for i in incidents)
    avg_resp_ms=(sum(agg_ms.get(t,0.0) for t in observed)/total_obs) if total_obs else 0.0

    base=f"{REPORTS_PREFIX}/{y}/{mo:02d}/"

    # 2) Minute CSV (MTD)
    minute_rows=[{"ts": t.strftime("%Y-%m-%d %H:%M"), "avail": 100.0 if agg_ok[t] else 0.0, "resp_s": (agg_ms.get(t,0.0)/1000.0)} for t in observed]
    _put_csv(REPORTS_BUCKET, f"{base}uptime-minute.csv",
             [{"timestamp_utc": r["ts"], "availability_pct": f"{r['avail']:.3f}", "avg_response_sec": f"{r['resp_s']:.3f}"} for r in minute_rows],
             ["timestamp_utc","availability_pct","avg_response_sec"])

    # 3) Hour CSV (MTD)
    hour_rows=hourly_reduce(agg_ok, agg_ms)
    _put_csv(REPORTS_BUCKET, f"{base}uptime-hour.csv",
             [{"hour_utc": r["hour"].strftime("%Y-%m-%d %H:%M"),
               "availability_pct": f"{(r['success_avg'] or 0.0):.3f}",
               "avg_response_sec": f"{((r['response_ms_avg'] or 0.0)/1000.0):.3f}"} for r in hour_rows],
             ["hour_utc","availability_pct","avg_response_sec"])

    # 4) Month-to-date cumulative CSV (daily)
    mc_rows = month_cumulative(agg_ok, agg_ms)
    mc_rows_padded = pad_month_days(start_m, end_m, mc_rows)
    _put_csv(REPORTS_BUCKET, f"{base}uptime-month-cumulative.csv",
             [{"day_utc": r["day"],
               "cumulative_availability_pct": "" if r["avail"] is None else f"{r['avail']:.3f}",
               "cumulative_avg_response_sec": "" if r["resp_s"] is None else f"{r['resp_s']:.3f}"} for r in mc_rows_padded],
             ["day_utc","cumulative_availability_pct","cumulative_avg_response_sec"])

    # 5) YTD
    try:
        year_chart_rows, year_table_rows, year_csv = build_year_summary_ytd(
            availability,
            (avg_resp_ms / 1000.0 if total_obs else 0.0),
        )
        _put_csv(REPORTS_BUCKET, f"{REPORTS_PREFIX}/{y}/uptime-year-summary.csv",
                 year_csv, ["month","availability_pct","avg_response_sec"])
    except Exception as e:
        log(f"[warn] YTD summary skipped: {type(e).__name__}")
        year_chart_rows, year_table_rows = [], []

    # 6) Render HTML
    meta=dict(
        company=COMPANY, client=CLIENT, service=SERVICE,
        year=str(y), month_name=start_m.strftime("%B"), slo=SLO_TARGET,
        availability=availability, downtime_min=downtime_min,
        incidents=len(incidents), avg_resp_s=(avg_resp_ms/1000.0)
    )

    html = render_html(
        meta,
        minute_rows,
        [{"hour": r["hour"].strftime("%Y-%m-%d %H:%M"), "avail": (r["success_avg"] or 0.0), "resp_s": ((r["response_ms_avg"] or 0.0)/1000.0)} for r in hour_rows],
        [{"day": r["day"], "avail": r["avail"], "resp_s": r["resp_s"]} for r in mc_rows_padded],
        [{"name": ART_PREFIX.split('/')[-1], "pct": availability}],
        year_chart_rows,
        year_table_rows,
        incidents,
        generated_at=now.strftime("%Y-%m-%d %H:%M UTC")
    )

    # Upload HTML only (PDF removed)
    html_key = f"{base}uptime-report.html"
    s3.put_object(
        Bucket=REPORTS_BUCKET,
        Key=html_key,
        Body=html.encode("utf-8"),
        ContentType="text/html; charset=utf-8"
    )

    return {
        "status":"ok",
        "bucket":REPORTS_BUCKET,
        "prefix":base,
        "html": f"s3://{REPORTS_BUCKET}/{html_key}"
    }
