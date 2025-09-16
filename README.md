
# LinkedIn Job Alerts — Hourly (IST) with Quiet Hours

This starter kit gives you **hourly LinkedIn job alerts** with:
- Quiet hours: **01:00–09:59 IST** (no pings)
- A **10:00 IST** catch-up blast for anything posted overnight
- Dedupe via SQLite
- Notifications via **Slack** (recommended) and/or **Email** (optional)
- Runs locally (cron) or on **GitHub Actions** if you want it to work while your laptop is off

> Heads-up: This script reads **public LinkedIn job-search result pages** (no login). LinkedIn can change markup or rate-limit. For a rock-solid, TOS-friendly setup, we’ll add **Greenhouse/Lever** boards in Step 2.

---

## 0) Prerequisites (5–10 min)
- **Slack incoming webhook** (recommended)  
  Create one in Slack: *Apps → Incoming Webhooks → Add → Choose channel → Copy Webhook URL*.
- (Optional) **Email SMTP** creds (FROM, TO, host, port, user, pass).

---

## 1) Setup (Mac/Linux)
Open Terminal and run:
```bash
cd ~/Downloads
unzip linkedin-alerts-starter.zip -d ~/linkedin-alerts
cd ~/linkedin-alerts

# Create venv & install deps
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
```

### Configure secrets
Copy `.env.example` to `.env` and fill the values:
```bash
cp .env.example .env
open -a TextEdit .env   # or use any editor
```

### Configure searches
Edit `searches.json` with your LinkedIn job-search URLs:
```bash
open -a TextEdit searches.json
```
> Tip: Build your search on LinkedIn Jobs with filters, then **copy the URL** from the address bar.  
> You can include keywords like `"Head of Marketing" OR "VP Marketing" -assistant -intern -junior` and set location to **Mumbai, Maharashtra, India**.  
> `f_TPR=r86400` restricts to jobs posted in the last 24h (optional).

### Test run once
```bash
python linkedin_alerts.py
```
If there are new results and you're **not** in quiet hours, you’ll get a Slack ping (and/or email).

---

## 2) Schedule (choose one)

### Option A — Run on your Mac via cron
Edit cron:
```bash
crontab -e
```
Paste these lines (assumes your Mac timezone is IST). Adjust the path below to your folder.

```
# Hourly 10:00–23:00 IST
0 10-23 * * * cd ~/linkedin-alerts && ~/linkedin-alerts/venv/bin/python linkedin_alerts.py >> run.log 2>&1
# Hourly 00:00–00:59 IST
0 0 * * * cd ~/linkedin-alerts && ~/linkedin-alerts/venv/bin/python linkedin_alerts.py >> run.log 2>&1
# Safety flush at 10:00 IST
0 10 * * * cd ~/linkedin-alerts && ~/linkedin-alerts/venv/bin/python linkedin_alerts.py >> run.log 2>&1
```

### Option B — GitHub Actions (runs even if laptop is off)
1. Create a new **private** GitHub repo (e.g., `linkedin-alerts`).
2. Upload all files from this folder to the repo.
3. Add **Actions secrets** (Repo → Settings → Secrets and variables → Actions → New repository secret):
   - `SLACK_WEBHOOK_URL` — your Slack webhook URL
   - (Optional) `EMAIL_FROM`, `EMAIL_TO`, `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`
4. Enable Actions; the workflow is set to run **hourly** by default.

---

## 3) Tune your filters
- In LinkedIn search, use includes/excludes, e.g.:  
  `"Head of Marketing" OR "VP Marketing" OR "Director Marketing" OR "Growth Head" -assistant -intern -junior -associate`
- Lock Location: **Mumbai, Maharashtra, India**.
- Optional: Apply Company filter in UI, copy the URL again, and paste into `searches.json`.

---

## 4) Troubleshooting
- Check `run.log` for errors (cron output).
- See what’s stored:
  ```bash
  sqlite3 jobs.db 'select count(*) from jobs;'
  sqlite3 jobs.db 'select title,company,posted_at from jobs order by first_seen_at desc limit 5;'
  ```
- Not receiving Slack alerts? Re-check `SLACK_WEBHOOK_URL` in `.env` and ensure you’re outside quiet hours.

---

## 5) Next (Step 2 — Greenhouse/Lever)
When Step 1 is stable, we’ll add **Greenhouse/Lever** feeds for your target companies. These are JSON APIs (no scraping) and fire **instantly** when recruiters post a role.

---

**Note**: Tuned to **Asia/Kolkata** and your “quiet hours” requirement. Tweak in `linkedin_alerts.py` if needed.
