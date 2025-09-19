# linkedin_alerts.py
import asyncio, json, os, re, sqlite3, pytz, time, random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

from dotenv import load_dotenv
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Error as PWError
from slack_sdk.webhook.async_client import AsyncWebhookClient

# ================================
# Config / Env
# ================================
load_dotenv()

TZ = pytz.timezone(os.getenv("TZ", "Asia/Kolkata"))
DB_PATH = "jobs.db"
SEARCHES_PATH = "searches.json"

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# Quiet hours DISABLED (send 24×7 as requested)
QUIET_START = 0
QUIET_END = 0

# Headless & context mode (env-controlled)
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
PW_CONTEXT_MODE = os.getenv("PW_CONTEXT", "persistent").lower()   # "persistent" (local) or "storage" (CI)
IS_CI = os.getenv("CI", "").lower() == "true"

# Stealth / pacing
PAGE_LOAD_SETTLE_RANGE = (0.8, 1.2)       # seconds after navigation
SCROLL_STEPS_MAX       = 4                # soft scrolls to trigger lazy-load
SCROLL_SLEEP_RANGE     = (0.5, 0.9)
INTER_SEARCH_SLEEP     = (3.0, 6.0) if IS_CI else (8.0, 14.0)
GLOBAL_TIMEOUT_MS      = 30000 if IS_CI else 35000
FAIL_FAST_ON_CHALLENGE = True

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
              notified INTEGER DEFAULT 0
            )
        """)
        conn.commit()

def is_quiet_hour(dt: datetime) -> bool:
    # disabled
    return QUIET_START <= dt.hour < QUIET_END if (QUIET_START or QUIET_END) else False

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

def _collapse_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def clean_title_company_location(title: str, company: str, location: str) -> tuple:
    # Strip badges like "with verification"/"verified"
    def strip_badges(x: str) -> str:
        x = re.sub(r"\bwith verification\b", "", (x or ""), flags=re.IGNORECASE)
        x = re.sub(r"\bverified\b", "", x, flags=re.IGNORECASE)
        return _collapse_ws(x)

    title = strip_badges(title)
    company = strip_badges(company)
    location = _collapse_ws(location)

    # Collapse duplicated titles (e.g., "AAA AAA" or "AAAAAA" halves equal)
    xs = _collapse_ws(title)
    if len(xs) >= 6 and len(xs) % 2 == 0:
        h = len(xs) // 2
        if xs[:h].lower() == xs[h:].lower():
            title = xs[:h]
        else:
            m = re.match(r"^(.+?)\1$", xs, flags=re.IGNORECASE)
            title = m.group(1) if m else xs
    else:
        m = re.match(r"^(.+?)\1$", xs, flags=re.IGNORECASE)
        title = m.group(1) if m else xs

    return title, _collapse_ws(company), _collapse_ws(location)

async def click_if_exists(page, selector, timeout=2000):
    try:
        el = await page.wait_for_selector(selector, timeout=timeout, state="visible")
        if el:
            await el.click()
            await page.wait_for_timeout(400)
            return True
    except PWError:
        pass
    return False

async def is_challenge(page) -> bool:
    """Return True only for real checkpoint/captcha states."""
    try:
        url = (page.url or "").lower()
        if "checkpoint" in url:
            return True
        cap_iframe = await page.query_selector("iframe[src*='recaptcha'], iframe[title*='captcha']")
        cap_input  = await page.query_selector("input[name='captcha']")
        tgt = cap_iframe or cap_input
        if tgt:
            box = await tgt.bounding_box()
            if box and box.get("width", 0) > 0 and box.get("height", 0) > 0:
                return True
        if await page.query_selector("div[role='alert']:has-text('verify'), h1:has-text('Verification')"):
            return True
    except PWError:
        pass
    return False

async def settle_and_find(page) -> bool:
    # cookie/consent
    for text in ["Accept", "I agree", "Allow all", "Allow"]:
        if await click_if_exists(page, f"button:has-text('{text}')", 1500):
            break

    await click_if_exists(page, "a:has-text('See all jobs')", 2000)
    await click_if_exists(page, "button:has-text('See all jobs')", 2000)

    containers = [
        "ul.jobs-search__results-list",
        ".jobs-search-results-list",
        "section.two-pane-serp-page__results-list",
        "div.jobs-search-two-pane__results",
        "div.base-serp-page__content",
        "main[role='main']"
    ]
    for sel in containers:
        try:
            await page.wait_for_selector(
                f"{sel} li, {sel} .job-card-container, {sel} a[href*='/jobs/view/']",
                timeout=10000,
                state="attached"
            )
            return True
        except PWError:
            continue

    # Lazy load
    for _ in range(SCROLL_STEPS_MAX):
        await page.mouse.wheel(0, 1200)
        await page.wait_for_timeout(int(random.uniform(*SCROLL_SLEEP_RANGE) * 1000))
        for sel in containers:
            el = await page.query_selector(f"{sel} li, {sel} .job-card-container, {sel} a[href*='/jobs/view/']")
            if el:
                return True

    # Final fallback
    if await page.query_selector("a[href*='/jobs/view/']"):
        return True

    # Debug
    try:
        snap = f"debug_{int(time.time())}.png"
        await page.screenshot(path=snap, full_page=True)
        print(f"[debug] Saved screenshot: {snap}")
    except Exception:
        pass
    return False

def parse_jobs_from_html(html: str, label: str, ref_dt: datetime) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    seen_ids = set()

    items = soup.select("""
        ul.jobs-search__results-list li,
        .jobs-search-results-list li,
        section.two-pane-serp-page__results-list li,
        li.jobs-search-results__list-item,
        div.job-card-container
    """)
    if not items:
        items = soup.select("a[href*='/jobs/view/']")

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
        if jid in seen_ids:
            continue
        seen_ids.add(jid)

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

        title, company, location = clean_title_company_location(title, company, location)
        posted_at = linkedin_relative_to_dt(posted_txt, ref_dt).isoformat()

        out.append({
            "job_id": jid,
            "title": title,
            "company": company or "-",
            "location": location or "-",
            "url": href.split("?")[0],
            "label": label,
            "posted_at": posted_at
        })
    return out

def upsert_jobs(jobs: List[Dict[str, Any]]):
    inserted = []
    nowiso = now_ist().isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        for j in jobs:
            c.execute("SELECT job_id FROM jobs WHERE job_id=?", (j["job_id"],))
            if c.fetchone():
                continue
            c.execute("""
                INSERT INTO jobs(job_id, title, company, location, url, label, posted_at, first_seen_at, notified)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
            """, (j["job_id"], j["title"], j["company"], j["location"], j["url"], j["label"], j["posted_at"], nowiso))
            inserted.append(j)
        conn.commit()
    return inserted

async def notify_slack(items: List[Dict[str, Any]], heading: str):
    if not SLACK_WEBHOOK_URL or not items:
        if not SLACK_WEBHOOK_URL:
            print("[error] SLACK_WEBHOOK_URL not set; skipping Slack.")
        return
    client = AsyncWebhookClient(SLACK_WEBHOOK_URL)
    lines = [f"*{heading}*"]
    for j in items:
        lines.append(f"• *{j['title']}* — {j['company']} ({j['location']})\n<{j['url']}|Apply / View>  — _{j['label']}_")
    await client.send(text="\n".join(lines))

# ================================
# Main run
# ================================
async def run_once():
    init_db()

    if not Path(SEARCHES_PATH).exists():
        raise FileNotFoundError(f"{SEARCHES_PATH} not found")

    with open(SEARCHES_PATH, "r") as f:
        searches = json.load(f)

    print("[DEBUG] Loaded searches:", [(s.get("label"), s.get("url")) for s in searches])

    total_parsed = 0
    total_inserted = 0
    new_jobs_all: List[Dict[str, Any]] = []

    async with async_playwright() as p:
        browser = None
        user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36"

        if PW_CONTEXT_MODE == "storage":
            # CI mode: use saved storage_state (auth.json) from secret
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
            # Local mode: persistent profile
            profile_dir = str(Path("playwright_profile").resolve())
            context = await p.chromium.launch_persistent_context(
                user_data_dir=profile_dir,
                headless=HEADLESS,
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

        # Start tracing on CI for post-mortem
        if IS_CI:
            try:
                await context.tracing.start(screenshots=True, snapshots=True, sources=False)
            except Exception:
                pass

        try:
            for idx, s in enumerate(searches, start=1):
                url = s["url"]
                label = s["label"]

                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    await page.wait_for_timeout(int(random.uniform(*PAGE_LOAD_SETTLE_RANGE) * 1000))

                    ready = await settle_and_find(page)
                    print(f"[debug] Search {idx}/{len(searches)} url={page.url} ready={ready}")

                    if FAIL_FAST_ON_CHALLENGE:
                        challenge = await is_challenge(page)
                        if challenge and not ready:
                            print("[warn] Real challenge detected (checkpoint). Backing off.")
                            break

                    if not ready:
                        print("[info] No results container found; skip.")
                        # short pause before next query
                        if idx < len(searches):
                            await page.wait_for_timeout(int(random.uniform(*INTER_SEARCH_SLEEP) * 1000))
                        continue

                    html = await page.content()
                    parsed = parse_jobs_from_html(html, label, now_ist())
                    inserted = upsert_jobs(parsed)

                    total_parsed += len(parsed)
                    total_inserted += len(inserted)
                    new_jobs_all.extend(inserted)

                    print(f"[summary] {label}: parsed={len(parsed)} inserted_new={len(inserted)}")

                    if idx < len(searches):
                        await page.wait_for_timeout(int(random.uniform(*INTER_SEARCH_SLEEP) * 1000))

                except Exception as e:
                    print(f"[{label}] error: {e}")

        finally:
            # Save trace and close cleanly
            try:
                await context.close()
            finally:
                if IS_CI:
                    try:
                        await context.tracing.stop(path="trace.zip")
                    except Exception:
                        pass
                if 'browser' in locals() and browser:
                    await browser.close()

    print(f"[summary] parsed_total={total_parsed} inserted_new_total={total_inserted} across {len(searches)} searches")

    if new_jobs_all and not is_quiet_hour(now_ist()):
        heading = f"New LinkedIn jobs — {now_ist().strftime('%d %b %Y, %H:%M %Z')}"
        print(f"[summary] sending to Slack: {len(new_jobs_all)} jobs")
        await notify_slack(new_jobs_all, heading)
    else:
        print("[summary] no new jobs to send this run (or quiet hour active).")

async def main():
    await run_once()

if __name__ == "__main__":
    asyncio.run(main())