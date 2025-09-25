# linkedin_alerts.py
import asyncio, json, os, re, sqlite3, pytz, time, random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

from dotenv import load_dotenv
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Error as PWError, Page
from slack_sdk.webhook.async_client import AsyncWebhookClient

# ================================
# Config / Env
# ================================
load_dotenv()

TZ = pytz.timezone(os.getenv("TZ", "Asia/Kolkata"))
DB_PATH = "jobs.db"
SEARCHES_PATH = "searches.json"

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
FRESH_HOURS = int(os.getenv("FRESH_HOURS", "24"))  # strict 24h as requested
HEARTBEAT = os.getenv("HEARTBEAT", "true").lower() == "true"  # optional "no new jobs" ping

# Hard gate for Mumbai relevance
REQUIRE_MUMBAI = os.getenv("REQUIRE_MUMBAI", "true").lower() == "true"
ALLOW_REMOTE = os.getenv("ALLOW_REMOTE", "true").lower() == "true"

# Quiet hours disabled (send 24x7)
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
INTER_SEARCH_SLEEP     = (2.0, 4.0) if IS_CI else (5.0, 9.0)
GLOBAL_TIMEOUT_MS      = 28000 if IS_CI else 35000
FAIL_FAST_ON_CHALLENGE = True

# Detail-page fetch limits
DETAIL_CONCURRENCY     = int(os.getenv("DETAIL_CONCURRENCY", "4"))
DETAIL_TIMEOUT_MS      = 16000

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
    return QUIET_START <= dt.hour < QUIET_END if (QUIET_START or QUIET_END) else False

def _collapse_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _parse_iso_ts(s: str) -> datetime:
    if not s:
        raise ValueError("empty ts")
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

def _as_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = TZ.localize(dt)
    return dt.isoformat()

def clean_title_company_location(title: str, company: str, location: str) -> tuple:
    def strip_badges(x: str) -> str:
        x = re.sub(r"\bwith verification\b", "", (x or ""), flags=re.IGNORECASE)
        x = re.sub(r"\bverified\b", "", x, flags=re.IGNORECASE)
        return _collapse_ws(x)

    title = strip_badges(title)
    company = strip_badges(company)
    location = _collapse_ws(location)

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

def mumbai_ok(location: str) -> bool:
    if not REQUIRE_MUMBAI:
        return True
    loc = (location or "").lower()
    if "mumbai" in loc or "maharashtra" in loc:
        return True
    if ALLOW_REMOTE and ("remote" in loc or "hybrid" in loc):
        return True
    return False

def linkedin_relative_to_dt(txt: str, ref_dt: datetime) -> Optional[datetime]:
    """Parse LinkedIn relative timestamps from text; return None if unparseable."""
    if not txt:
        return None
    s = txt.strip().lower()

    if "just now" in s or "moments ago" in s:
        return ref_dt
    if "today" in s:
        return ref_dt.replace(hour=8, minute=0, second=0, microsecond=0)
    if "yesterday" in s:
        y = ref_dt - timedelta(days=1)
        return y.replace(hour=8, minute=0, second=0, microsecond=0)

    patterns = [
        r"(\d+)\s+(minute|hour|day|week|month)s?\s+ago",
        r"(\d+)\s*(min|mins|min\.|hr|hrs|h|d|w|wk|wks|mo|mos|mth|mths)\s*ago",
        r"about\s+(\d+)\s+(minute|hour|day|week|month)s?\s+ago",
        r"posted\s+(\d+)\s+(minute|hour|day|week|month)s?\s+ago",
        r"active\s+(\d+)\s+(minute|hour|day|week|month)s?\s+ago",
    ]
    unit_map = {
        "mins": "minute", "min": "minute", "min.": "minute",
        "hr": "hour", "hrs": "hour", "h": "hour",
        "d": "day",
        "w": "week", "wk": "week", "wks": "week",
        "mo": "month", "mos": "month", "mth": "month", "mths": "month",
    }
    for pat in patterns:
        m = re.search(pat, s)
        if not m:
            continue
        n_str, u = m.group(1), m.group(2)
        try:
            n = int(n_str)
        except:
            continue
        u = unit_map.get(u, u)
        if u == "minute":
            return ref_dt - timedelta(minutes=n)
        if u == "hour":
            return ref_dt - timedelta(hours=n)
        if u == "day":
            return ref_dt - timedelta(days=n)
        if u == "week":
            return ref_dt - timedelta(weeks=n)
        if u == "month":
            # LinkedIn doesn't give precise months; approximate 30d
            return ref_dt - timedelta(days=30 * n)
    return None

# ================================
# Playwright helpers
# ================================
async def click_if_exists(page, selector, timeout=1800):
    try:
        el = await page.wait_for_selector(selector, timeout=timeout, state="visible")
        if el:
            await el.click()
            await page.wait_for_timeout(300)
            return True
    except PWError:
        pass
    return False

async def is_challenge(page) -> bool:
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
    for text in ["Accept", "I agree", "Allow all", "Allow"]:
        if await click_if_exists(page, f"button:has-text('{text}')", 1400):
            break

    await click_if_exists(page, "a:has-text('See all jobs')", 1500)
    await click_if_exists(page, "button:has-text('See all jobs')", 1500)

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
                timeout=9000,
                state="attached"
            )
            return True
        except PWError:
            continue

    for _ in range(SCROLL_STEPS_MAX):
        await page.mouse.wheel(0, 1200)
        await page.wait_for_timeout(int(random.uniform(*SCROLL_SLEEP_RANGE) * 1000))
        for sel in containers:
            el = await page.query_selector(f"{sel} li, {sel} .job-card-container, {sel} a[href*='/jobs/view/']")
            if el:
                return True

    try:
        snap = f"debug_{int(time.time())}.png"
        await page.screenshot(path=snap, full_page=True)
        print(f"[debug] Saved screenshot: {snap}")
    except Exception:
        pass
    return False

# ================================
# Parsing & detail-time resolution
# ================================
def parse_list_cards(html: str, label: str) -> List[Dict[str, Any]]:
    """Parse list page quickly for id/title/company/location/url."""
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
        else:
            title_el = li.select_one(".base-search-card__title, .job-card-list__title, .sr-only, .result-card__title, h3")
            comp_el  = li.select_one(".base-search-card__subtitle, .job-card-container__company-name, .job-card-company-name, .result-card__subtitle, a[href*='/company/']")
            loc_el   = li.select_one(".job-search-card__location, .result-card__meta .job-result-card__location, .job-card-container__metadata-item")

            title = (title_el.get_text(strip=True) if title_el else a.get_text(strip=True))
            company = comp_el.get_text(strip=True) if comp_el else ""
            location = loc_el.get_text(strip=True) if loc_el else ""

        title, company, location = clean_title_company_location(title, company, location)
        out.append({
            "job_id": jid,
            "title": title,
            "company": company or "-",
            "location": location or "-",
            "url": href.split("?")[0],
            "label": label,
            "posted_at": None,       # resolved via detail page
            "posted_source": "none"
        })
    return out

async def _extract_time_from_detail(detail: Page, ref_dt: datetime) -> Optional[datetime]:
    """Try several selectors on the job detail page to get an accurate posted time."""
    # First, any machine-readable <time datetime=...>
    t = await detail.query_selector("time[datetime]")
    if t:
        try:
            dt_attr = await t.get_attribute("datetime")
            if dt_attr:
                return _parse_iso_ts(dt_attr)
        except Exception:
            pass

    # Textual labels LinkedIn uses in various layouts
    sel_text_candidates = [
        "span.jobs-unified-top-card__posted-date",
        "span.posted-time-ago__text",
        "span.tvm__text",  # frequently contains "... ago"
        "div.jobs-unified-top-card__primary-description",
        "div.jobs-unified-top-card__subtitle-primary-group",
        "section.jobs-unified-top-card",
    ]
    for sel in sel_text_candidates:
        el = await detail.query_selector(sel)
        if not el:
            continue
        txt = _collapse_ws(await el.inner_text())
        dt = linkedin_relative_to_dt(txt, ref_dt)
        if dt:
            return dt

    # As a last resort, scan the whole visible text once (lightweight)
    try:
        body_text = _collapse_ws(await detail.inner_text("body"))
        dt = linkedin_relative_to_dt(body_text, ref_dt)
        if dt:
            return dt
    except Exception:
        pass

    return None

async def resolve_times_with_details(context, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Open each job in a temp tab and resolve posted_at; skip if time can't be resolved."""
    sem = asyncio.Semaphore(DETAIL_CONCURRENCY)
    cutoff = now_ist() - timedelta(hours=FRESH_HOURS)
    resolved: List[Dict[str, Any]] = []

    async def worker(item: Dict[str, Any]):
        async with sem:
            url = item["url"]
            dt = None
            try:
                detail = await context.new_page()
                detail.set_default_timeout(DETAIL_TIMEOUT_MS)
                await detail.goto(url, wait_until="domcontentloaded", timeout=DETAIL_TIMEOUT_MS)
                await detail.wait_for_timeout(600)  # brief settle
                # quick challenge short-circuit
                if await is_challenge(detail):
                    print(f"[warn] Challenge on detail page for {item['job_id']}; skipping.")
                    await detail.close()
                    return
                dt = await _extract_time_from_detail(detail, now_ist())
            except Exception as e:
                print(f"[debug] detail-open error for {url}: {e}")
            finally:
                try:
                    await detail.close()
                except Exception:
                    pass

            if not dt:
                # If we cannot resolve a timestamp, DO NOT include.
                print(f"[time] drop {item['job_id']} — no timestamp resolved")
                return

            # Hard freshness cut
            if dt < cutoff:
                age_h = (now_ist() - dt).total_seconds()/3600
                print(f"[time] drop {item['job_id']} — {age_h:.1f}h old (> {FRESH_HOURS}h)")
                return

            item["posted_at"] = _as_iso(dt)
            item["posted_source"] = "detail"
            resolved.append(item)

    await asyncio.gather(*(worker(it) for it in items))
    return resolved

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

    cutoff = now_ist() - timedelta(hours=FRESH_HOURS)
    print(f"[FILTER_DEBUG] Current time: {now_ist().isoformat()}")
    print(f"[FILTER_DEBUG] Fresh hours: {FRESH_HOURS}")
    print(f"[FILTER_DEBUG] Cutoff time: {cutoff.isoformat()}")
    print(f"[FILTER_DEBUG] Jobs must be posted after: {cutoff.astimezone(TZ).strftime('%Y-%m-%d %H:%M %Z')}")

    if not Path(SEARCHES_PATH).exists():
        raise FileNotFoundError(f"{SEARCHES_PATH} not found")

    with open(SEARCHES_PATH, "r") as f:
        searches = json.load(f)

    print("[DEBUG] Loaded searches:", [(s.get("label"), s.get("url")) for s in searches])
    print(f"[DEBUG] Slack webhook configured: {bool(SLACK_WEBHOOK_URL)}")

    total_parsed = 0
    total_inserted = 0
    new_jobs_all: List[Dict[str, Any]] = []

    async with async_playwright() as p:
        browser = None
        user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36"

        if PW_CONTEXT_MODE == "storage":
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

        if IS_CI:
            try:
                await context.tracing.start(screenshots=True, snapshots=True, sources=False)
            except Exception:
                pass

        try:
            for idx, s in enumerate(searches, start=1):
                url = s["url"]; label = s["label"]
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
                    list_items = parse_list_cards(html, label)
                    total_parsed += len(list_items)
                    print(f"[list] {label}: parsed={len(list_items)} (pre Mumbai/time filters)")

                    # Location gate (Mumbai/Remote)
                    if REQUIRE_MUMBAI:
                        before = len(list_items)
                        list_items = [j for j in list_items if mumbai_ok(j.get("location", ""))]
                        print(f"[loc] {label}: kept={len(list_items)} (dropped {before - len(list_items)} non-Mumbai)")

                    # Resolve timestamps via detail pages; drop if unknown or stale
                    resolved = await resolve_times_with_details(context, list_items)

                    inserted = upsert_jobs(resolved)
                    total_inserted += len(inserted)
                    new_jobs_all.extend(inserted)

                    print(f"[summary] {label}: after time+loc filters kept={len(resolved)} inserted_new={len(inserted)}")

                    if idx < len(searches):
                        await page.wait_for_timeout(int(random.uniform(*INTER_SEARCH_SLEEP) * 1000))

                except Exception as e:
                    print(f"[{label}] error: {e}")

        finally:
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

    print(f"[summary] parsed_total={total_parsed} inserted_new_total={total_inserted} across {len(searches)} searches")

    if new_jobs_all and not is_quiet_hour(now_ist()):
        heading = f"New LinkedIn jobs (≤{FRESH_HOURS}h, Mumbai) — {now_ist().strftime('%d %b %Y, %H:%M %Z')}"
        print(f"[summary] sending to Slack: {len(new_jobs_all)} jobs")
        await notify_slack(new_jobs_all, heading)
    else:
        print("[summary] no new jobs to send this run (or quiet hour active).")
        await send_heartbeat(len(new_jobs_all))

async def main():
    await run_once()

if __name__ == "__main__":
    asyncio.run(main())