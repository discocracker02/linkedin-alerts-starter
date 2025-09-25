# linkedin_alerts.py
import asyncio, json, os, re, sqlite3, pytz, time, random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

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
FRESH_HOURS = int(os.getenv("FRESH_HOURS", "24"))  # <= freshness window (default 24h)
HEARTBEAT = os.getenv("HEARTBEAT", "true").lower() == "true"  # send heartbeat when 0 jobs

# Quiet hours DISABLED (send 24×7)
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

def _collapse_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _parse_iso_ts(s: str) -> datetime:
    """Parse ISO 8601 safely. Normalize 'Z' to '+00:00'. Assume UTC if naive, then convert to IST."""
    if not s:
        raise ValueError("empty ts")
    s2 = s.replace('Z', '+00:00')
    dt = datetime.fromisoformat(s2)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.UTC)
    return dt.astimezone(TZ)

def linkedin_relative_to_dt(txt: str, ref_dt: datetime) -> Optional[datetime]:
    """Parse relative time strings from LinkedIn with comprehensive patterns and debug logging."""
    original_txt = txt
    txt = (txt or "").strip().lower()
    
    print(f"[TIME_DEBUG] Parsing timestamp text: '{original_txt}' -> '{txt}'")

    if not txt:
        print("[TIME_DEBUG] Empty text, returning None")
        return None
        
    if "just now" in txt or "moments ago" in txt or "now" == txt:
        print(f"[TIME_DEBUG] 'Just now' detected, using ref_dt: {ref_dt.isoformat()}")
        return ref_dt

    # today / yesterday -> use start of day (IST)
    start_today = ref_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    if "today" in txt:
        result = start_today
        print(f"[TIME_DEBUG] 'Today' detected, using: {result.isoformat()}")
        return result
    if "yesterday" in txt:
        result = start_today - timedelta(days=1)
        print(f"[TIME_DEBUG] 'Yesterday' detected, using: {result.isoformat()}")
        return result

    # Handle "30+ days ago" or similar
    if re.search(r"\b30\+?\s*days?\b", txt):
        result = ref_dt - timedelta(days=31)
        print(f"[TIME_DEBUG] '30+ days' detected, using: {result.isoformat()}")
        return result

    # Comprehensive patterns for relative timestamps
    patterns = [
        # Standard formats
        r"(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago",
        # Abbreviated formats
        r"(\d+)\s*(s|sec|secs|m|min|mins|h|hr|hrs|d|w|wk|wks|mo|mon|months?|y|yr|yrs|years?)\s*ago",
        # With "about", "over", "more than" etc.
        r"(?:about|over|more than|nearly)\s+(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago",
        r"(?:about|over|more than|nearly)\s+(\d+)\s*(s|sec|secs|m|min|mins|h|hr|hrs|d|w|wk|wks|mo|mon|months?|y|yr|yrs|years?)\s*ago",
        # Edge cases
        r"(\d+)\+\s+(day|week|month)s?\s+ago",  # "1+ days ago"
        r"a\s+(second|minute|hour|day|week|month|year)\s+ago",  # "a day ago"
        r"an\s+(hour|day|week|month|year)\s+ago",  # "an hour ago"
    ]
    
    unit_map = {
        # Time units
        "s": "second", "sec": "second", "secs": "second", "second": "second",
        "m": "minute", "min": "minute", "mins": "minute", "minute": "minute",
        "h": "hour", "hr": "hour", "hrs": "hour", "hour": "hour",
        "d": "day", "day": "day",
        "w": "week", "wk": "week", "wks": "week", "week": "week",
        "mo": "month", "mon": "month", "month": "month", "months": "month",
        "y": "year", "yr": "year", "yrs": "year", "year": "year", "years": "year",
    }
    
    for i, pat in enumerate(patterns):
        m = re.search(pat, txt)
        if m:
            print(f"[TIME_DEBUG] Pattern {i+1} matched: {pat}")
            print(f"[TIME_DEBUG] Match groups: {m.groups()}")
            
            # Handle "a/an" cases
            if m.group(1) in ['a', 'an']:
                n = 1
                unit = m.group(2) if len(m.groups()) > 1 else m.group(1)
            else:
                n = int(m.group(1))
                unit = m.group(2) if len(m.groups()) > 1 else "day"  # fallback
            
            unit = unit_map.get(unit.lower(), unit.lower())
            print(f"[TIME_DEBUG] Parsed: {n} {unit}(s) ago")
            
            delta_map = {
                "second": timedelta(seconds=n),
                "minute": timedelta(minutes=n),
                "hour":   timedelta(hours=n),
                "day":    timedelta(days=n),
                "week":   timedelta(weeks=n),
                "month":  timedelta(days=30*n),  # coarse approximation
                "year":   timedelta(days=365*n), # coarse approximation
            }
            
            delta = delta_map.get(unit)
            if delta:
                result = ref_dt - delta
                print(f"[TIME_DEBUG] Calculated timestamp: {result.isoformat()}")
                return result
            else:
                print(f"[TIME_DEBUG] Unknown unit '{unit}', skipping")

    print(f"[TIME_DEBUG] No patterns matched for '{original_txt}', returning None")
    return None

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

# Add this enhanced parse_jobs_from_html function to your existing code
# Replace the existing function completely

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

    print(f"[PARSE_DEBUG] Found {len(items)} job items to process")

    out = []
    for idx, li in enumerate(items):
        print(f"\n[PARSE_DEBUG] Processing item {idx+1}/{len(items)}")
        
        a = li if li.name == "a" else li.select_one("a.base-card__full-link, a.job-card-list__title, a[href*='/jobs/view/']")
        if not a:
            print("[PARSE_DEBUG] No anchor found, skipping")
            continue
            
        href = a.get("href") or ""
        if href.startswith("/"):
            href = "https://www.linkedin.com" + href
        m = re.search(r"/jobs/view/(\d+)", href)
        if not m:
            print(f"[PARSE_DEBUG] No job ID in href: {href}")
            continue
            
        jid = m.group(1)
        if jid in seen_ids:
            print(f"[PARSE_DEBUG] Duplicate job ID {jid}, skipping")
            continue
        seen_ids.add(jid)

        # Skip bare anchors (no metadata — often ads or stale links)
        if li.name == "a":
            print("[PARSE_DEBUG] Bare anchor (likely ad), skipping")
            continue

        print(f"[PARSE_DEBUG] Processing job {jid}")
        
        # DEBUG: Print the entire HTML structure of this job card
        print(f"[HTML_DEBUG] Job {jid} full HTML structure:")
        print("-" * 80)
        print(li.prettify()[:2000])  # First 2000 chars
        if len(li.prettify()) > 2000:
            print("... [TRUNCATED] ...")
        print("-" * 80)
        
        # Enhanced element selection with more selectors
        title_el = li.select_one("""
            .base-search-card__title, 
            .job-card-list__title, 
            .sr-only, 
            .result-card__title, 
            h3, 
            .job-card-container__link,
            .job-card-list__title-link,
            a[data-tracking-control-name="public_jobs_jserp-result_search-card"]
        """)
        
        comp_el = li.select_one("""
            .base-search-card__subtitle, 
            .job-card-container__company-name, 
            .job-card-company-name, 
            .result-card__subtitle, 
            a[href*='/company/'],
            .job-card-container__primary-description,
            .job-result-card__subtitle
        """)
        
        loc_el = li.select_one("""
            .job-search-card__location, 
            .result-card__meta .job-result-card__location, 
            .job-card-container__metadata-item,
            .job-result-card__location,
            .job-card-container__metadata-wrapper span
        """)

        title = (title_el.get_text(strip=True) if title_el else a.get_text(strip=True))
        company = comp_el.get_text(strip=True) if comp_el else ""
        location = loc_el.get_text(strip=True) if loc_el else ""
        
        print(f"[PARSE_DEBUG] Job {jid}: {title[:50]}... at {company}")

        # ULTRA-COMPREHENSIVE timestamp detection
        posted_at_iso: Optional[str] = None
        timestamp_source = "none"
        
        # Method 1: Look for ANY element with datetime attribute
        datetime_elements = li.select("[datetime]")
        print(f"[TIME_DEBUG] Found {len(datetime_elements)} elements with datetime attribute")
        
        for dt_el in datetime_elements:
            datetime_val = dt_el.get("datetime")
            print(f"[TIME_DEBUG] datetime element: {dt_el.name} with datetime='{datetime_val}'")
            try:
                posted_dt = _parse_iso_ts(datetime_val)
                posted_at_iso = posted_dt.isoformat()
                timestamp_source = f"datetime_attr: {datetime_val}"
                print(f"[TIME_DEBUG] SUCCESS: Parsed datetime='{datetime_val}' -> {posted_at_iso}")
                break
            except Exception as e:
                print(f"[TIME_DEBUG] FAILED to parse datetime='{datetime_val}': {e}")

        # Method 2: Look for time elements specifically
        if not posted_at_iso:
            time_elements = li.select("time")
            print(f"[TIME_DEBUG] Found {len(time_elements)} <time> elements")
            
            for time_el in time_elements:
                print(f"[TIME_DEBUG] <time> element HTML: {time_el}")
                if time_el.has_attr("datetime"):
                    try:
                        posted_dt = _parse_iso_ts(time_el["datetime"])
                        posted_at_iso = posted_dt.isoformat()
                        timestamp_source = f"time_datetime: {time_el['datetime']}"
                        print(f"[TIME_DEBUG] SUCCESS: time element datetime -> {posted_at_iso}")
                        break
                    except Exception as e:
                        print(f"[TIME_DEBUG] FAILED time element datetime: {e}")

        # Method 3: Search ALL text content for time patterns
        if not posted_at_iso:
            print(f"[TIME_DEBUG] Searching all text content for timestamp patterns...")
            all_text_elements = li.find_all(string=True)
            time_candidates = []
            
            for text in all_text_elements:
                text_clean = text.strip()
                if text_clean and any(keyword in text_clean.lower() for keyword in 
                    ['ago', 'today', 'yesterday', 'just now', 'moment', 'hour', 'day', 'week', 'month']):
                    time_candidates.append(text_clean)
                    print(f"[TIME_DEBUG] Found time candidate in text: '{text_clean}'")
            
            # Try parsing each candidate
            for candidate in time_candidates:
                print(f"[TIME_DEBUG] Trying to parse candidate: '{candidate}'")
                parsed_dt = linkedin_relative_to_dt(candidate, ref_dt)
                if parsed_dt:
                    posted_at_iso = parsed_dt.isoformat()
                    timestamp_source = f"text_content: {candidate}"
                    print(f"[TIME_DEBUG] SUCCESS: text parsing -> {posted_at_iso}")
                    break

        # Method 4: Look for data attributes that might contain timestamps
        if not posted_at_iso:
            print(f"[TIME_DEBUG] Checking data attributes...")
            for attr in li.attrs:
                if 'time' in attr.lower() or 'date' in attr.lower():
                    attr_value = li.attrs[attr]
                    print(f"[TIME_DEBUG] Found time-related attribute: {attr}='{attr_value}'")
                    try:
                        if isinstance(attr_value, str) and len(attr_value) > 8:  # Could be a timestamp
                            parsed_dt = _parse_iso_ts(attr_value)
                            posted_at_iso = parsed_dt.isoformat()
                            timestamp_source = f"data_attr: {attr}={attr_value}"
                            print(f"[TIME_DEBUG] SUCCESS: data attribute -> {posted_at_iso}")
                            break
                    except Exception:
                        continue

        # Method 5: Look in nested elements more aggressively
        if not posted_at_iso:
            print(f"[TIME_DEBUG] Deep searching nested elements...")
            
            # Get all elements and check their text
            all_nested = li.find_all()
            for nested_el in all_nested:
                element_text = nested_el.get_text(strip=True)
                if element_text and len(element_text) < 50:  # Reasonable timestamp length
                    if any(keyword in element_text.lower() for keyword in ['ago', 'today', 'yesterday']):
                        print(f"[TIME_DEBUG] Checking nested element {nested_el.name}: '{element_text}'")
                        parsed_dt = linkedin_relative_to_dt(element_text, ref_dt)
                        if parsed_dt:
                            posted_at_iso = parsed_dt.isoformat()
                            timestamp_source = f"nested_text: {element_text}"
                            print(f"[TIME_DEBUG] SUCCESS: nested element -> {posted_at_iso}")
                            break

        print(f"[PARSE_DEBUG] Job {jid}: Timestamp source = {timestamp_source}")
        print(f"[PARSE_DEBUG] Job {jid}: Final timestamp = {posted_at_iso}")

        # If still no timestamp found, we need to make a decision
        if not posted_at_iso:
            print(f"[PARSE_DEBUG] Job {jid}: No timestamp found after all methods")
            
            # OPTION A: Skip the job (current behavior)
            # print(f"[PARSE_DEBUG] Job {jid}: SKIPPING due to no timestamp")
            # continue
            
            # OPTION B: Assume it's fresh and use current time (risky but includes more jobs)
            print(f"[PARSE_DEBUG] Job {jid}: ASSUMING FRESH - using current time as fallback")
            posted_at_iso = ref_dt.isoformat()
            timestamp_source = "fallback_current_time"

        title, company, location = clean_title_company_location(title, company, location)

        job_data = {
            "job_id": jid,
            "title": title,
            "company": company or "-",
            "location": location or "-",
            "url": href.split("?")[0],
            "label": label,
            "posted_at": posted_at_iso
        }
        
        out.append(job_data)
        print(f"[PARSE_DEBUG] Job {jid}: Added to results")

    print(f"[PARSE_DEBUG] Total jobs parsed: {len(out)}")
    return out

def upsert_jobs(jobs: List[Dict[str, Any]]):
    inserted = []
    nowiso = now_ist().isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        for j in jobs:
            c.execute("SELECT job_id FROM jobs WHERE job_id=?", (j["job_id"],))
            if c.fetchone():
                print(f"[DB_DEBUG] Job {j['job_id']} already exists, skipping")
                continue
            c.execute("""
                INSERT INTO jobs(job_id, title, company, location, url, label, posted_at, first_seen_at, notified)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
            """, (j["job_id"], j["title"], j["company"], j["location"], j["url"], j["label"], j["posted_at"], nowiso))
            inserted.append(j)
            print(f"[DB_DEBUG] Job {j['job_id']} inserted as new")
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
        company = j.get('company') or "-"
        location = j.get('location') or "-"
        lines.append(
            f"• *{j['title']}* — {company} ({location})\n"
            f"<{j['url']}|Apply / View>  — _{j['label']}_"
        )

    text = "\n".join(lines)
    try:
        resp = await client.send(text=text)
        code = getattr(resp, "status_code", None)
        body = getattr(resp, "body", None)
        ok = (code == 200)
        print(f"[slack] sent={ok} status={code} body={body!r} chars={len(text)}")
    except Exception as e:
        print(f"[slack] send failed: {e}")

async def send_heartbeat(count: int):
    """Optional heartbeat when no new jobs, controlled by HEARTBEAT env."""
    if not HEARTBEAT or not SLACK_WEBHOOK_URL:
        return
    client = AsyncWebhookClient(SLACK_WEBHOOK_URL)
    ts = now_ist().strftime('%d %b %Y, %H:%M %Z')
    text = f"Heartbeat — pipeline OK at {ts}. New jobs this run (≤{FRESH_HOURS}h): {count}."
    try:
        resp = await client.send(text=text)
        code = getattr(resp, "status_code", None)
        print(f"[slack] heartbeat status={code}")
    except Exception as e:
        print(f"[slack] heartbeat failed: {e}")

# ================================
# Main run
# ================================
async def run_once():
    init_db()

    if not Path(SEARCHES_PATH).exists():
        raise FileNotFoundError(f"{SEARCHES_PATH} not found")

    with open(SEARCHES_PATH, "r") as f:
        searches = json.load(f)

    current_time = now_ist()
    cutoff_time = current_time - timedelta(hours=FRESH_HOURS)
    
    print(f"[FILTER_DEBUG] Current time: {current_time.isoformat()}")
    print(f"[FILTER_DEBUG] Fresh hours: {FRESH_HOURS}")
    print(f"[FILTER_DEBUG] Cutoff time: {cutoff_time.isoformat()}")
    print(f"[FILTER_DEBUG] Jobs must be posted after: {cutoff_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")

    print("[DEBUG] Loaded searches:", [(s.get("label"), s.get("url")) for s in searches])
    print(f"[DEBUG] Slack webhook configured: {bool(SLACK_WEBHOOK_URL)}")

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

                print(f"\n[SEARCH_DEBUG] Starting search {idx}/{len(searches)}: {label}")

                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    await page.wait_for_timeout(int(random.uniform(*PAGE_LOAD_SETTLE_RANGE) * 1000))

                    ready = await settle_and_find(page)
                    print(f"[debug] Search {idx}/{len(searches)} url={page.url} ready={ready}")
                    try:
                        snap = f"search_{idx}_{'ready' if ready else 'noready'}_{int(time.time())}.png"
                        await page.screenshot(path=snap, full_page=True)
                        print(f"[debug] Saved per-search screenshot: {snap}")
                    except Exception:
                        pass

                    if FAIL_FAST_ON_CHALLENGE:
                        challenge = await is_challenge(page)
                        if challenge and not ready:
                            print("[warn] Real challenge detected (checkpoint). Backing off.")
                            break

                    if not ready:
                        print("[info] No results container found; skip.")
                        if idx < len(searches):
                            await page.wait_for_timeout(int(random.uniform(*INTER_SEARCH_SLEEP) * 1000))
                        continue

                    html = await page.content()
                    parsed = parse_jobs_from_html(html, label, current_time)

                    # Strict freshness filtering BEFORE insert with detailed logging
                    filtered = []
                    dropped_invalid = 0
                    dropped_stale = 0
                    
                    print(f"\n[FILTER_DEBUG] Processing {len(parsed)} parsed jobs for {label}")
                    
                    for j in parsed:
                        job_id = j.get("job_id", "unknown")
                        posted_at_str = j.get("posted_at", "")
                        
                        try:
                            dtp = _parse_iso_ts(posted_at_str)
                            age_hours = (current_time - dtp).total_seconds() / 3600
                            
                            if dtp >= cutoff_time:
                                filtered.append(j)
                                print(f"[FILTER_DEBUG] ✅ Job {job_id} INCLUDED: {age_hours:.1f}h old - {j['title'][:50]}...")
                            else:
                                dropped_stale += 1
                                print(f"[FILTER_DEBUG] ❌ Job {job_id} TOO OLD: {age_hours:.1f}h old - {j['title'][:50]}...")
                                
                        except Exception as e:
                            dropped_invalid += 1
                            print(f"[FILTER_DEBUG] ❌ Job {job_id} INVALID TIMESTAMP: {e} - {j['title'][:50]}...")

                    inserted = upsert_jobs(filtered)

                    total_parsed += len(parsed)
                    total_inserted += len(inserted)
                    new_jobs_all.extend(inserted)

                    print(f"\n[SUMMARY] {label}:")
                    print(f"  - Parsed: {len(parsed)} jobs")
                    print(f"  - After freshness filter: {len(filtered)} jobs")
                    print(f"  - Invalid timestamps: {dropped_invalid}")
                    print(f"  - Too old (>{FRESH_HOURS}h): {dropped_stale}")
                    print(f"  - New jobs inserted: {len(inserted)}")

                    if idx < len(searches):
                        await page.wait_for_timeout(int(random.uniform(*INTER_SEARCH_SLEEP) * 1000))

                except Exception as e:
                    print(f"[{label}] error: {e}")

        finally:
            # Save trace first, then close cleanly
            if IS_CI:
                try:
                    await context.tracing.stop(path="trace.zip")
                except Exception:
                    pass
            try:
                await context.close()
            finally:
                if 'browser' in locals() and browser:
                    await browser.close()

    print(f"\n[FINAL_SUMMARY]")
    print(f"Total jobs parsed: {total_parsed}")
    print(f"Total new jobs inserted: {total_inserted}")
    print(f"Searches processed: {len(searches)}")

    if new_jobs_all and not is_quiet_hour(current_time):
        heading = f"New LinkedIn jobs (≤{FRESH_HOURS}h) — {current_time.strftime('%d %b %Y, %H:%M %Z')}"
        print(f"[SLACK_DEBUG] Sending {len(new_jobs_all)} jobs to Slack")
        await notify_slack(new_jobs_all, heading)
    else:
        print("[SLACK_DEBUG] No new jobs to send this run (or quiet hour active)")
        # Heartbeat when zero results, so you still get a proof-of-life
        await send_heartbeat(len(new_jobs_all))

def debug_timestamp_parsing():
    """Test function to verify timestamp parsing works correctly"""
    ref_dt = now_ist()
    test_cases = [
        "2 hours ago",
        "1 day ago", 
        "just now",
        "3 weeks ago",
        "today",
        "yesterday", 
        "5 minutes ago",
        "1 month ago",
        "30+ days ago",
        "about 2 hours ago",
        "over 1 week ago",
        "2h ago",
        "1d ago",
        "3w ago",
        "a day ago",
        "an hour ago"
    ]
    
    print("Testing timestamp parsing:")
    print(f"Reference time: {ref_dt.isoformat()}")
    print("-" * 60)
    
    for case in test_cases:
        parsed = linkedin_relative_to_dt(case, ref_dt)
        if parsed:
            age_hours = (ref_dt - parsed).total_seconds() / 3600
            print(f"'{case:15}' -> {parsed.isoformat()} ({age_hours:.1f}h ago)")
        else:
            print(f"'{case:15}' -> FAILED TO PARSE")

async def main():
    # Uncomment this line to test timestamp parsing before running
    # debug_timestamp_parsing()
    await run_once()

if __name__ == "__main__":
    asyncio.run(main())