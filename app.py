We’re not building a single Lambda; it’s a pipeline (EventBridge → Synthetics canaries → Report Lambda → S3 outputs → optional QuickSight PDF) provisioned via Terraform with IAM, SSM, and Secrets wired in.

Where we are right now: the Lambda does generate the monthly HTML report (plus CSV, JSONL, year rollup, month index) from the canary data we have, with proper charts, incidents, and daily rollups.

We fixed data paging, added guards for sparse data, standardized A4 pages, and set up alarms and least-privilege IAM. These parts are stable.

What’s still hard: several app endpoints arrived late and some still fail/auth; without reliable endpoints we can’t fully validate SLA math, incident windows, or percentiles across all clients.

Each client needs its own secrets, schedules, and FAIL_STREAK/SLA sign-off; those policy decisions change the math and must be agreed.

To reach “system ready” we must: stabilize all endpoints, finalize canary flows, tune schedules/thresholds, wire HTML→PDF (or QS snapshot) for deliverables, and run UAT with sign-off.

This is 10+ apps per client, minute-level data, and multi-artifact output; the complexity is integration and reliability, not just code.

We can keep sending the HTML report while the remaining pieces land; full production readiness needs the above dependencies cleared
