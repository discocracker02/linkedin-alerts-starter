import asyncio, json, os, re, sqlite3, pytz, aiosmtplib, time, random
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from pathlib import Path
from typing import List, Dict, Any
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from slack_sdk.webhook.async_client import AsyncWebhookClient
from playwright.async_api import async_playwright, Error as PWError

# ================================
# Config / Env
# ================================
load_dotenv()
TZ = pytz.timezone(os.getenv("TZ", "Asia/Kolkata"))
DB_PATH = "jobs.db"
SEARCHES_PATH = "searches.json"

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
EMAIL_FROM = os.getenv("EMAIL_FROM")
EMAIL_TO   = os.getenv("EMAIL_TO")
SMTP_HOST  = os.getenv("SMTP_HOST")
SMTP_PORT  = int(os.getenv("SMTP_PORT", "587") or "587")
SMTP_USER  = os.getenv("SMTP_USER")
SMTP_PASS  = os.getenv("SMTP_PASS")

# Quiet hours (IST): suppress immediate notifications; queued items flush at ~10:00
QUIET_START = 0      # 01:00 inclusive
QUIET_END   = 0     # 10:00 exclusive

# Rate limiting / stealth knobs (conservative)
PAGE_LOAD_SETTLE_RANGE = (1.0, 1.6)      # seconds after navigation
SCROLL_STEPS_MAX       = 4               # how many lazy-load scrolls at most
SCROLL_SLEEP_RANGE     = (0.6, 1.0)      # sleep between scrolls
INTER_SEARCH_SLEEP     = (8.0, 14.0)     # pause between searches
GLOBAL_TIMEOUT_MS      = 35000           # default wait timeout per step
FAIL_FAST_ON_CHALLENGE = True
HEADLESS = os.getenv("HEADLESS", "false").lower() == "true"

Path(DB_PATH).touch(exist_ok=True)

# ================================
# Utilities
# ================================
def now_ist() -> datetime:
    return datetime.now(TZ)

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
              job_id TEXT PRIMARY KEY,
              title TEXT,
              company TEXT,
              location TEXT,
              url TEXT,
              label TEXT,
              posted_at TEXT,
              first_seen_at TEXT,
              notified INTEGER DEFAULT 0,
              queued INTEGER DEFAULT 0
            )
        """)
        conn.commit()

def is_quiet_hour(dt: datetime) -> bool:
    return QUIET_START <= dt.hour < QUIET_END

def linkedin_relative_to_dt(txt: str, ref_dt: datetime) -> datetime:
    txt = (txt or "").strip().lower()
    if not txt or "just now" in txt:
        return ref_dt
    m = re.search(r"(\d+)\s+(minute|hour|day|week)s?\s+ago", txt)
    if not m:
        return ref_dt
    n, unit = int(m.group(1)), m.group(2)
    delta = {
        "minute": timedelta(minutes=n),
        "hour": timedelta(hours=n),
        "day": timedelta(days=n),
        "week": timedelta(weeks=n),
    }.get(unit, timedelta(0))
    return ref_dt - delta

async def click_if_exists(page, selector, timeout=2500):
    try:
        el = await page.wait_for_selector(selector, timeout=timeout, state="visible")
        if el:
            await el.click()
            await page.wait_for_timeout(500)
            return True
    except PWError:
        pass
    return False

async def is_challenge(page) -> bool:
    """Return True only for *real* checkpoints/captchas."""
    try:
        url = page.url.lower()
        if "checkpoint" in url:
            return True
        # Only treat captcha as real if the widget/input is actually visible
        cap_iframe = await page.query_selector("iframe[src*='recaptcha'], iframe[title*='captcha']")
        cap_input = await page.query_selector("input[name='captcha']")
        target = cap_iframe or cap_input
        if target:
            try:
                box = await target.bounding_box()
                if box and box.get("width", 0) > 0 and box.get("height", 0) > 0:
                    return True
            except Exception:
                pass
        # Severe verification banners
        if await page.query_selector("div[role='alert']:has-text('verify'), h1:has-text('Verification')"):
            return True
    except PWError:
        pass
    return False

async def settle_and_find(page) -> bool:
    # Consent banners (best effort)
    for text in ["Accept", "I agree", "Allow all", "Allow"]:
        if await click_if_exists(page, f"button:has-text('{text}')", 1500):
            break

    # Sometimes there’s a “See all jobs” CTA
    await click_if_exists(page, "a:has-text('See all jobs')", 2000)
    await click_if_exists(page, "button:has-text('See all jobs')", 2000)

    # Candidate containers (LinkedIn has multiple variants)
    containers = [
        "ul.jobs-search__results-list",
        ".jobs-search-results-list",
        "section.two-pane-serp-page__results-list",
        "div.jobs-search-two-pane__results",
        "div.base-serp-page__content",
        "main[role='main']"
    ]

    # First pass: wait for any plausible result
    for sel in containers:
        try:
            await page.wait_for_selector(
                f"{sel} li, {sel} .job-card-container, {sel} a[href*='/jobs/view/']",
                timeout=12000,
                state="attached"
            )
            return True
        except PWError:
            continue

    # Lazy-load by scrolling (limited)
    for _ in range(SCROLL_STEPS_MAX):
        await page.mouse.wheel(0, 1200)
        await page.wait_for_timeout(int(random.uniform(*SCROLL_SLEEP_RANGE) * 1000))
        for sel in containers:
            try:
                el = await page.query_selector(f"{sel} li, {sel} .job-card-container, {sel} a[href*='/jobs/view/']")
                if el:
                    return True
            except PWError:
                continue

    # Ultra-fallback: any job link anywhere
    try:
        if await page.query_selector("a[href*='/jobs/view/']"):
            return True
    except PWError:
        pass

    # Debug snapshot
    try:
        snap = f"debug_{int(time.time())}.png"
        await page.screenshot(path=snap, full_page=True)
        print(f"[debug] Saved screenshot: {snap}")
    except:
        pass
    return False

def parse_jobs_from_html(html: str, label: str, ref_dt: datetime) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select("""
        ul.jobs-search__results-list li,
        .jobs-search-results-list li,
        section.two-pane-serp-page__results-list li,
        li.jobs-search-results__list-item,
        div.job-card-container
    """)
    if not items:
        items = soup.select("a[href*='/jobs/view/']")  # ultra-fallback

    out = []
    for li in items:
        a = li if li.name == "a" else li.select_one("a.base-card__full-link, a.job-card-list__title, a[href*='/jobs/view/']")
        if not a:
            continue
        href = a.get("href") or ""
        if href.startswith("/"):
            href = "https://www.linkedin.com" + href
        m = re.search(r"/jobs/view/(\d+)", href)
        if not m:
            continue
        jid = m.group(1)

        if li.name == "a":
            title = a.get_text(strip=True)
            company = ""
            location = ""
            posted_txt = ""
        else:
            title_el = li.select_one(".base-search-card__title, .job-card-list__title, .sr-only, .result-card__title, h3")
            comp_el  = li.select_one(".base-search-card__subtitle, .job-card-container__company-name, .job-card-company-name, .result-card__subtitle, a[href*='/company/']")
            loc_el   = li.select_one(".job-search-card__location, .result-card__meta .job-result-card__location, .job-card-container__metadata-item")
            time_el  = li.select_one("time")

            title = (title_el.get_text(strip=True) if title_el else a.get_text(strip=True))
            company = comp_el.get_text(strip=True) if comp_el else ""
            location = loc_el.get_text(strip=True) if loc_el else ""
            posted_txt = time_el.get_text(strip=True).lower() if time_el else ""

        posted_at = linkedin_relative_to_dt(posted_txt, ref_dt).isoformat()

        out.append({
            "job_id": jid,
            "title": title,
            "company": company,
            "location": location,
            "url": href.split("?")[0],
            "label": label,
            "posted_at": posted_at
        })
    return out

def upsert_and_classify(jobs: List[Dict[str, Any]]):
    out_notify, out_queue = [], []
    nowiso = now_ist().isoformat()
    qhour = is_quiet_hour(now_ist())
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        for j in jobs:
            c.execute("SELECT job_id FROM jobs WHERE job_id=?", (j["job_id"],))
            if c.fetchone():
                continue
            c.execute("""
                INSERT INTO jobs(job_id, title, company, location, url, label, posted_at, first_seen_at, notified, queued)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0)
            """, (j["job_id"], j["title"], j["company"], j["location"], j["url"], j["label"], j["posted_at"], nowiso))
            if qhour:
                out_queue.append(j)
                c.execute("UPDATE jobs SET queued=1 WHERE job_id=?", (j["job_id"],))
            else:
                out_notify.append(j)
                c.execute("UPDATE jobs SET notified=1 WHERE job_id=?", (j["job_id"],))
        conn.commit()
    return {"new_to_notify": out_notify, "queued": out_queue}

async def notify_slack(items: List[Dict[str, Any]], heading: str):
    if not SLACK_WEBHOOK_URL or not items:
        return
    client = AsyncWebhookClient(SLACK_WEBHOOK_URL)
    lines = [f"*{heading}*"]
    for j in items:
        company = j['company'] or "-"
        location = j['location'] or "-"
        lines.append(f"• *{j['title']}* — {company} ({location})\n<{j['url']}|Apply / View>  — _{j['label']}_")
    await client.send(text="\n".join(lines))

async def notify_email(items: List[Dict[str, Any]], heading: str):
    if not (EMAIL_FROM and EMAIL_TO and SMTP_HOST and items):
        return
    body = [heading, ""]
    for j in items:
        body.append(f"{j['title']} — {j['company']} ({j['location']})\n{j['url']}  — {j['label']}\n")
    msg = MIMEText("\n".join(body))
    msg["Subject"] = heading
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    await aiosmtplib.send(msg, hostname=SMTP_HOST, port=SMTP_PORT, start_tls=True, username=SMTP_USER, password=SMTP_PASS)

def fetch_queued_to_flush():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        rows = c.execute("""
            SELECT job_id, title, company, location, url, label, posted_at
            FROM jobs WHERE queued=1 AND notified=0
        """).fetchall()
    return [{"job_id": r[0], "title": r[1], "company": r[2], "location": r[3], "url": r[4], "label": r[5], "posted_at": r[6]} for r in rows]

def mark_flushed(ids: List[str]):
    if not ids:
        return
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        q = ",".join(["?"] * len(ids))
        c.execute(f"UPDATE jobs SET queued=0, notified=1 WHERE job_id IN ({q})", ids)
        conn.commit()

# ================================
# Run once (hourly via cron/Actions)
# ================================
async def run_once():
    init_db()

    with open(SEARCHES_PATH, "r") as f:
        searches = json.load(f)
    print("[DEBUG] Loaded searches:", [(s.get("label"), s.get("url")) for s in searches])

    now = now_ist()
    # 10:00 IST flush for overnight finds
    if now.hour == 10 and now.minute < 10:
        queued = fetch_queued_to_flush()
        if queued:
            heading = f"Queued LinkedIn jobs from overnight — {now.strftime('%d %b %Y, %H:%M %Z')}"
            await notify_slack(queued, heading)
            await notify_email(queued, heading)
            mark_flushed([j["job_id"] for j in queued])

    async with async_playwright() as p:
        profile_dir = str(Path("playwright_profile").resolve())
        mode = os.getenv("PW_CONTEXT", "persistent").lower()
        user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36"

        browser = None
        if mode == "storage":
            # GitHub Actions mode: use storage_state from auth.json supplied via secret
            browser = await p.chromium.launch(
                headless=HEADLESS,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu"
                ],
            )
            context = await browser.new_context(
                storage_state="auth.json",
                user_agent=user_agent,
                locale="en-US",
                timezone_id="Asia/Kolkata",
                viewport={"width": 1366, "height": 768}
            )
        else:
            # Local mode: persistent profile that stays logged in across runs
            context = await p.chromium.launch_persistent_context(
                user_data_dir=profile_dir,
                headless=HEADLESS,
                # channel="chrome",
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu"
                ],
                locale="en-US",
                timezone_id="Asia/Kolkata",
                viewport={"width": 1366, "height": 768}
            )

        context.set_default_timeout(GLOBAL_TIMEOUT_MS)
        page = await context.new_page()

        all_new_to_notify = []
        for idx, s in enumerate(searches, start=1):
            try:
                await page.goto(s["url"], wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(int(random.uniform(*PAGE_LOAD_SETTLE_RANGE) * 1000))

                ready = await settle_and_find(page)
                print("[debug] URL:", page.url, "ready=", ready)

                # Only consider it a challenge if both (a) challenge present and (b) no results found
                if FAIL_FAST_ON_CHALLENGE:
                    challenge = await is_challenge(page)
                    if challenge and not ready:
                        print("[warn] Real challenge detected (no results + checkpoint). Backing off this run.")
                        break

                if not ready:
                    # Not a challenge; just skip this variant quietly
                    print("[info] No results container found; skipping this search variant.")
                    continue

                html = await page.content()
                parsed = parse_jobs_from_html(html, s["label"], now_ist())
                res = upsert_and_classify(parsed)
                all_new_to_notify.extend(res["new_to_notify"])

                # Inter-search cool-off (randomized)
                if idx < len(searches):
                    await page.wait_for_timeout(int(random.uniform(*INTER_SEARCH_SLEEP) * 1000))

            except Exception as e:
                print(f"[{s.get('label','?')}] error: {e}")

        await context.close()
        if browser:
            await browser.close()

    if all_new_to_notify and not is_quiet_hour(now_ist()):
        heading = f"New LinkedIn jobs — {now_ist().strftime('%d %b %Y, %H:%M %Z')}"
        await notify_slack(all_new_to_notify, heading)
        await notify_email(all_new_to_notify, heading)

async def main():
    await run_once()

if __name__ == "__main__":
    asyncio.run(main())