#!/usr/bin/env python3
"""
extract_articles.py — Collect and archive articles from Telegram channel HTML exports.

Three source types:
  1. Asdaa (asdaa-alsaa.com) external links
  2. Jeddah Club (jeddah-club.com) external links
  3. Long-form articles written directly as Telegram posts

Usage:
  python scripts/extract_articles.py          # full run (parse + fetch + save)
  python scripts/extract_articles.py --no-fetch  # parse only, skip HTTP fetching

Outputs:
  output/articles/articles_full.json      — all entries with full text (UTF-8)
  output/articles/articles_summary.xlsx   — summary table + per-type sheets
  output/articles/fetch_cache.json        — URL fetch cache (resume-safe)
"""

import argparse
import json
import re
import sys
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup
import pandas as pd

# Playwright is used for sites that block plain requests (Cloudflare, etc.)
# Install: pip install playwright
# Browser: already installed at /opt/pw-browsers/chromium-*/chrome-linux/chrome
PLAYWRIGHT_CHROMIUM = next(
    (str(p) for p in Path("/opt/pw-browsers").glob("chromium-*/chrome-linux/chrome")
     if p.exists()),
    None,
)


# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
HTML_FILES = [f for f in [
    BASE_DIR / "messages.html",
    BASE_DIR / "messages2.html",
    BASE_DIR / "messages3.html",
    BASE_DIR / "messages4.html",
    BASE_DIR / "messages_june1.html",
    BASE_DIR / "messages_new.html",
] if f.exists()]
OUT_DIR = BASE_DIR / "output" / "articles"
OUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_FILE = OUT_DIR / "fetch_cache.json"


# ── Config ─────────────────────────────────────────────────────────────────────
ASDAA  = "asdaa-alsaa.com"
JCLUB  = "jeddah-club.com"
REQUEST_DELAY = 1.5   # seconds between requests
MAX_RETRIES   = 3
TG_MIN_CHARS  = 400   # minimum chars for a Telegram post to be considered an article

# Patterns that indicate an audio announcement rather than a written article
AUDIO_ANN_RE = re.compile(
    r'\d+:\d+\s*(?:د\b|دقيقة|دق\b)'        # duration like "25:05 د"
    r'|المدة\s*[:#]'                          # "المدة:" / "المدة :"
    r'|مدة الصوتية'                           # "مدة الصوتية"
    r'|رابط الاستماع'                         # "رابط الاستماع والتحميل"
    r'|صوتيات'                                # "صوتيات إذاعة ..." (audio section headers)
    r'|(?:top4top\.io|archive\.org|goo\.gl)'  # known audio-hosting domains used in 2017 era
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ar,en;q=0.8",
}

# CSS selectors tried in order for article title and body
TITLE_SELS = [
    "h1.entry-title", "h1.td-page-title", "h1.post-title",
    "h1.article-title", "article h1", "h1",
]
BODY_SELS = [
    ".entry-content", ".td-post-content", ".post-content",
    ".article-content", ".article-body", ".post-body",
    "article .content", "article", ".content-area", "#content",
]


# ── Helpers ────────────────────────────────────────────────────────────────────

def parse_date(title_attr: str) -> str:
    """'15.09.2025 16:52:26 UTC+03:00'  →  '15.09.2025'"""
    m = re.match(r'(\d{2}\.\d{2}\.\d{4})', title_attr or "")
    return m.group(1) if m else ""


def get_domain_links(element, domain: str) -> list:
    """Return deduplicated list of hrefs containing domain."""
    seen, out = set(), []
    for a in element.find_all("a", href=True):
        href = a["href"].strip()
        if domain in href and href not in seen:
            seen.add(href)
            out.append(href)
    return out


def has_audio_attachment(msg: BeautifulSoup) -> bool:
    return bool(
        msg.find("a", class_="media_audio_file") or
        msg.find("audio")
    )


def is_forwarded(msg: BeautifulSoup) -> bool:
    return bool(msg.find(class_="forwarded"))


# ── Phase 1: Parse HTML files ──────────────────────────────────────────────────

def parse_html_files() -> tuple:
    """
    Returns:
      asdaa_map   — {url: entry_dict}  (deduped, first-seen wins)
      jclub_map   — {url: entry_dict}
      tg_articles — [entry_dict, ...]
    """
    asdaa_map, jclub_map = {}, {}
    tg_articles = []

    for html_file in HTML_FILES:
        print(f"  Parsing {html_file.name} …")
        soup = BeautifulSoup(html_file.read_text(encoding="utf-8"), "lxml")

        for msg in soup.find_all("div", id=re.compile(r'^message\d+$')):
            msg_id  = msg["id"]
            date_el = msg.find(class_="date")
            date    = parse_date(date_el.get("title", "") if date_el else "")
            text_div = msg.find("div", class_="text")
            if not text_div:
                continue

            asdaa_links = get_domain_links(text_div, ASDAA)
            jclub_links = get_domain_links(text_div, JCLUB)

            # ── Asdaa ──
            for url in asdaa_links:
                if url not in asdaa_map:
                    asdaa_map[url] = {
                        "type": "Asdaa",
                        "msg_id": msg_id,
                        "date": date,
                        "url": url,
                        "source_file": html_file.name,
                    }

            # ── Jeddah Club ──
            for url in jclub_links:
                if url not in jclub_map:
                    jclub_map[url] = {
                        "type": "JeddahClub",
                        "msg_id": msg_id,
                        "date": date,
                        "url": url,
                        "source_file": html_file.name,
                    }

            # ── Telegram Articles ──
            # Skip: already an external-link post, has audio attachment,
            # is forwarded from another channel, or matches audio-announcement patterns
            if asdaa_links or jclub_links:
                continue
            if has_audio_attachment(msg) or is_forwarded(msg):
                continue

            raw_text = text_div.get_text(separator="\n").strip()
            if len(raw_text) < TG_MIN_CHARS:
                continue
            if AUDIO_ANN_RE.search(raw_text):
                continue

            tg_articles.append({
                "type": "TelegramArticle",
                "msg_id": msg_id,
                "date": date,
                "url": None,
                "source_file": html_file.name,
                "raw_text": raw_text,
            })

    return asdaa_map, jclub_map, tg_articles


# ── Phase 2: Fetch external articles ──────────────────────────────────────────

def load_cache() -> dict:
    if CACHE_FILE.exists():
        return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
    return {}


def save_cache(cache: dict):
    CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_article_html(html: str) -> dict:
    """Extract title + full text from an article HTML page."""
    soup = BeautifulSoup(html, "lxml")

    title = ""
    for sel in TITLE_SELS:
        tag = soup.select_one(sel)
        if tag:
            t = tag.get_text(strip=True)
            if len(t) > 3:
                title = t
                break
    if not title:
        tag = soup.find("title")
        if tag:
            title = re.split(r'\s*[|–\-]\s*', tag.get_text(strip=True))[0].strip()

    full_text = ""
    for sel in BODY_SELS:
        tag = soup.select_one(sel)
        if tag:
            candidate = tag.get_text(separator="\n", strip=True)
            if len(candidate) > 150:
                full_text = candidate
                break

    summary = " ".join(full_text.split()[:70])
    return {"title": title, "summary": summary, "full_text": full_text}


def fetch_article_requests(url: str, session: requests.Session) -> dict:
    """Fetch URL via requests. Returns None on 403/connection-error (try Playwright)."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, headers=HEADERS, timeout=25, allow_redirects=True)
            resp.raise_for_status()
            result = _parse_article_html(resp.text)
            result["fetch_status"] = f"ok ({resp.status_code})"
            return result

        except requests.exceptions.HTTPError as e:
            code = e.response.status_code if e.response is not None else 0
            if code in (403, 404, 410):
                return None if code == 403 else {
                    "title": "", "summary": "", "full_text": "",
                    "fetch_status": f"HTTP {code}",
                }
            if attempt == MAX_RETRIES:
                return {"title": "", "summary": "", "full_text": "",
                        "fetch_status": f"HTTP {code} (retries exhausted)"}
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            if attempt == MAX_RETRIES:
                return None  # signal to try Playwright
        except Exception as exc:
            if attempt == MAX_RETRIES:
                return {"title": "", "summary": "", "full_text": "",
                        "fetch_status": f"error: {exc!r}"}

        time.sleep(2 ** attempt)
    return None


def fetch_article_playwright(url: str) -> dict:
    """Fetch URL via headless Chromium (bypasses Cloudflare/bot protection)."""
    if PLAYWRIGHT_CHROMIUM is None:
        return {"title": "", "summary": "", "full_text": "",
                "fetch_status": "playwright_no_browser"}
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return {"title": "", "summary": "", "full_text": "",
                "fetch_status": "playwright_not_installed"}

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                executable_path=PLAYWRIGHT_CHROMIUM,
                args=["--no-sandbox", "--disable-setuid-sandbox",
                      "--no-proxy-server", "--ignore-certificate-errors"],
            )
            ctx = browser.new_context(
                user_agent=HEADERS["User-Agent"],
                locale="ar",
                extra_http_headers={"Accept-Language": "ar,en;q=0.8"},
            )
            page = ctx.new_page()
            page.goto(url, timeout=30000, wait_until="networkidle")
            page.wait_for_timeout(1500)

            # Check for network block message
            body_text = page.inner_text("body") or ""
            if "not in allowlist" in body_text or len(body_text) < 100:
                browser.close()
                return {"title": "", "summary": "", "full_text": "",
                        "fetch_status": "network_blocked"}

            html = page.content()
            browser.close()

        result = _parse_article_html(html)
        result["fetch_status"] = "ok (playwright)"
        return result

    except Exception as exc:
        return {"title": "", "summary": "", "full_text": "",
                "fetch_status": f"playwright_error: {exc!r}"}


def fetch_article(url: str, session: requests.Session) -> dict:
    """Fetch URL, trying requests first then Playwright as fallback."""
    result = fetch_article_requests(url, session)
    if result is None:
        # requests got blocked — try headless browser
        result = fetch_article_playwright(url)
    return result


def fetch_all(entries: list, label: str) -> list:
    """Fetch all URLs in entries, using cache to skip already-done ones."""
    cache = load_cache()
    session = requests.Session()
    enriched = []
    total = len(entries)

    for i, entry in enumerate(entries, 1):
        url = entry["url"]
        if url in cache:
            print(f"  [{i:>4}/{total}] [cache] {url[:70]}")
            entry.update(cache[url])
        else:
            print(f"  [{i:>4}/{total}] Fetching {url[:70]}")
            result = fetch_article(url, session)
            status = result.get("fetch_status", "")
            title  = result.get("title", "")[:60]
            print(f"          → {status}  |  {title}")
            entry.update(result)
            cache[url] = result
            save_cache(cache)
            time.sleep(REQUEST_DELAY)

        enriched.append(entry)

    return enriched


# ── Phase 3: Process Telegram articles ────────────────────────────────────────

def process_tg_articles(articles: list) -> list:
    """Derive title and summary from raw text."""
    processed = []
    for art in articles:
        text  = art.get("raw_text", "")
        lines = [l.strip() for l in text.splitlines() if l.strip()]

        # Title: first line that starts with Arabic letters after stripping
        # emojis / punctuation
        title = ""
        for line in lines[:6]:
            clean = re.sub(r'^[^؀-ۿa-zA-Z0-9]+', '', line).strip()
            # Remove formatting marks (bold markers, etc.)
            clean = re.sub(r'[*_`]+', '', clean).strip()
            if len(clean) > 5:
                title = clean
                break

        summary = " ".join(text.split()[:70])

        art["title"]        = title
        art["summary"]      = summary
        art["full_text"]    = text
        art["fetch_status"] = "n/a"
        processed.append(art)

    return processed


# ── Phase 4: Save outputs ──────────────────────────────────────────────────────

def save_outputs(all_entries: list):
    # ── JSON (full data, no raw_html) ──
    json_entries = [{k: v for k, v in e.items() if k != "raw_text"} for e in all_entries]
    # For TelegramArticles raw_text IS the full_text — already copied above in process_tg_articles
    json_path = OUT_DIR / "articles_full.json"
    json_path.write_text(
        json.dumps(json_entries, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"  Saved JSON: {json_path}  ({len(json_entries)} entries)")

    # ── Excel summary ──
    rows = []
    for db, e in enumerate(all_entries, 1):
        rows.append({
            "DB#":         db,
            "Type":        e.get("type", ""),
            "Date":        e.get("date", ""),
            "URL":         e.get("url") or "—",
            "Title":       e.get("title", ""),
            "Summary":     e.get("summary", ""),
            "FetchStatus": e.get("fetch_status", ""),
            "MessageID":   e.get("msg_id", ""),
            "SourceFile":  e.get("source_file", ""),
        })

    df = pd.DataFrame(rows)
    xlsx_path = OUT_DIR / "articles_summary.xlsx"

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="All", index=False)
        for sheet, label in [("Asdaa", "Asdaa"), ("JeddahClub", "JeddahClub"),
                              ("TGArticles", "TelegramArticle")]:
            sub = df[df["Type"] == label].reset_index(drop=True)
            if not sub.empty:
                sub.to_excel(writer, sheet_name=sheet, index=False)

    print(f"  Saved Excel: {xlsx_path}")
    return json_path, xlsx_path


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--no-fetch", action="store_true",
                        help="Skip HTTP fetching (parse only; useful for testing)")
    args = parser.parse_args()

    # Phase 1
    print("\n" + "=" * 60)
    print("Phase 1 — Parsing HTML files")
    print("=" * 60)
    asdaa_map, jclub_map, tg_articles = parse_html_files()
    asdaa_list = list(asdaa_map.values())
    jclub_list = list(jclub_map.values())
    print(f"\n  Unique Asdaa URLs:        {len(asdaa_list)}")
    print(f"  Unique Jeddah Club URLs:  {len(jclub_list)}")
    print(f"  Telegram articles:        {len(tg_articles)}")

    if args.no_fetch:
        print("\n  --no-fetch: skipping HTTP requests.")
        print("  Adding placeholder fetch fields …")
        for e in asdaa_list + jclub_list:
            e.update({"title": "", "summary": "", "full_text": "",
                      "fetch_status": "not fetched"})
        tg_processed = process_tg_articles(tg_articles)
        all_entries  = asdaa_list + jclub_list + tg_processed
        save_outputs(all_entries)
        return

    # Phase 2 — Asdaa
    print("\n" + "=" * 60)
    print("Phase 2 — Fetching Asdaa articles")
    print("=" * 60)
    asdaa_enriched = fetch_all(asdaa_list, "Asdaa")

    # Phase 3 — Jeddah Club
    print("\n" + "=" * 60)
    print("Phase 3 — Fetching Jeddah Club articles")
    print("=" * 60)
    jclub_enriched = fetch_all(jclub_list, "JeddahClub")

    # Phase 4 — Telegram articles
    print("\n" + "=" * 60)
    print("Phase 4 — Processing Telegram articles")
    print("=" * 60)
    tg_processed = process_tg_articles(tg_articles)
    print(f"  Processed {len(tg_processed)} articles")

    # Phase 5 — Save
    print("\n" + "=" * 60)
    print("Phase 5 — Saving outputs")
    print("=" * 60)
    all_entries = asdaa_enriched + jclub_enriched + tg_processed
    save_outputs(all_entries)

    # Summary
    ok_a = sum(1 for e in asdaa_enriched  if "ok" in e.get("fetch_status", ""))
    ok_j = sum(1 for e in jclub_enriched  if "ok" in e.get("fetch_status", ""))
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  Asdaa:          {len(asdaa_enriched):4d} URLs   "
          f"({ok_a} fetched OK, {len(asdaa_enriched)-ok_a} failed/cached)")
    print(f"  Jeddah Club:    {len(jclub_enriched):4d} URLs   "
          f"({ok_j} fetched OK, {len(jclub_enriched)-ok_j} failed/cached)")
    print(f"  TG Articles:    {len(tg_processed):4d} posts")
    print(f"  Total entries:  {len(all_entries):4d}")
    print(f"\n  Output directory: {OUT_DIR}")


if __name__ == "__main__":
    main()
