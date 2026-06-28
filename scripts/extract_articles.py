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
import random
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
REQUEST_DELAY    = 4.0   # base seconds between requests
REQUEST_JITTER   = 2.0   # random 0–2s added per request
RATE_LIMIT_WAIT  = 90    # seconds to wait after a 429 response
MAX_RETRIES      = 3
MAX_429_RETRIES  = 5     # extra retries specifically for rate-limit responses
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


CA_BUNDLE = "/root/.ccr/ca-bundle.crt" if Path("/root/.ccr/ca-bundle.crt").exists() else True


def fetch_article_requests(url: str, session: requests.Session) -> dict:
    """Fetch URL via requests.  Returns None on SSL/connection error (signal Playwright)."""
    rate_limit_hits = 0

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, headers=HEADERS, timeout=25,
                               allow_redirects=True, verify=CA_BUNDLE)

            # ── 429 Rate Limit: wait and retry indefinitely up to MAX_429_RETRIES ──
            if resp.status_code == 429:
                rate_limit_hits += 1
                if rate_limit_hits > MAX_429_RETRIES:
                    return {"title": "", "summary": "", "full_text": "",
                            "fetch_status": "HTTP 429 (rate-limited, gave up)"}
                retry_after = int(resp.headers.get("Retry-After", RATE_LIMIT_WAIT))
                wait = max(retry_after, RATE_LIMIT_WAIT)
                print(f"    [429] Waiting {wait}s before retry {rate_limit_hits}/{MAX_429_RETRIES} …")
                time.sleep(wait)
                continue  # don't count against normal retries

            resp.raise_for_status()
            result = _parse_article_html(resp.text)
            result["fetch_status"] = f"ok ({resp.status_code})"
            return result

        except requests.exceptions.HTTPError as e:
            code = e.response.status_code if e.response is not None else 0
            if code == 403:
                return None   # signal to try Playwright
            if code in (404, 410):
                return {"title": "", "summary": "", "full_text": "",
                        "fetch_status": f"HTTP {code}"}
            if attempt == MAX_RETRIES:
                return {"title": "", "summary": "", "full_text": "",
                        "fetch_status": f"HTTP {code} (retries exhausted)"}

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout,
                requests.exceptions.SSLError):
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
        # Playwright fallback — skip for known SSL-incompatible domains
        if JCLUB in url:
            result = {"title": "", "summary": "", "full_text": "",
                      "fetch_status": "ssl_error (proxy TLS incompatibility)"}
        else:
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
            time.sleep(REQUEST_DELAY + random.uniform(0, REQUEST_JITTER))

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


# ── Phase 4: Deduplicate ──────────────────────────────────────────────────────

import hashlib

def _text_fingerprint(text: str) -> str:
    """Stable hash of normalised text for duplicate detection."""
    # Strip whitespace and punctuation normalisation for robustness
    normalised = re.sub(r'\s+', ' ', text or "").strip().lower()
    return hashlib.md5(normalised.encode("utf-8")).hexdigest()


def deduplicate(all_entries: list) -> tuple:
    """
    Remove duplicate entries and return (unique_entries, duplicate_entries).

    Duplicate rules:
      - External links (Asdaa/JeddahClub): same URL → keep earliest (first-seen, already
        handled upstream; this catches any that slipped through)
      - Telegram articles: same msg_id OR same text fingerprint → keep first occurrence
      - Cross-source: if a Telegram post is just a link to an Asdaa/JeddahClub URL
        that we already have as an external entry, mark it as duplicate
    """
    unique, duplicates = [], []
    seen_urls:         set = set()
    seen_msg_ids:      set = set()
    seen_fingerprints: set = set()

    for e in all_entries:
        etype  = e.get("type", "")
        url    = e.get("url") or ""
        msg_id = e.get("msg_id", "")
        text   = e.get("full_text") or e.get("raw_text") or ""
        fp     = _text_fingerprint(text) if text else None

        # ── URL dedup (for external link entries) ──
        if url and etype in ("Asdaa", "JeddahClub"):
            if url in seen_urls:
                e["_dup_reason"] = f"duplicate URL: {url}"
                duplicates.append(e)
                continue
            seen_urls.add(url)

        # ── msg_id dedup (Telegram articles) ──
        if msg_id and etype == "TelegramArticle":
            if msg_id in seen_msg_ids:
                e["_dup_reason"] = f"duplicate msg_id: {msg_id}"
                duplicates.append(e)
                continue
            seen_msg_ids.add(msg_id)

            # ── text fingerprint dedup ──
            if fp and fp in seen_fingerprints:
                e["_dup_reason"] = "duplicate content (same text)"
                duplicates.append(e)
                continue
            if fp:
                seen_fingerprints.add(fp)

        unique.append(e)

    return unique, duplicates


# ── Phase 5: Save outputs ──────────────────────────────────────────────────────

def save_outputs(unique_entries: list, dup_entries: list):
    STRIP = {"raw_text", "raw_html"}

    # ── JSON (unique records only) ──
    json_path = OUT_DIR / "articles_full.json"
    json_path.write_text(
        json.dumps([{k: v for k, v in e.items() if k not in STRIP}
                    for e in unique_entries],
                   ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"  Saved JSON: {json_path}  ({len(unique_entries)} unique entries)")

    # ── JSON (duplicates log) ──
    dup_path = OUT_DIR / "articles_duplicates.json"
    dup_path.write_text(
        json.dumps([{k: v for k, v in e.items() if k not in STRIP}
                    for e in dup_entries],
                   ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"  Saved duplicates log: {dup_path}  ({len(dup_entries)} duplicates)")

    def _row(db, e, is_dup=False):
        return {
            "DB#":         db,
            "Type":        e.get("type", ""),
            "Date":        e.get("date", ""),
            "URL":         e.get("url") or "—",
            "Title":       e.get("title", ""),
            "Summary":     e.get("summary", ""),
            "FetchStatus": e.get("fetch_status", ""),
            "MessageID":   e.get("msg_id", ""),
            "SourceFile":  e.get("source_file", ""),
            **({"DupReason": e.get("_dup_reason", "")} if is_dup else {}),
        }

    df_unique = pd.DataFrame([_row(i+1, e) for i, e in enumerate(unique_entries)])
    df_dups   = pd.DataFrame([_row(i+1, e, True) for i, e in enumerate(dup_entries)])

    xlsx_path = OUT_DIR / "articles_summary.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df_unique.to_excel(writer, sheet_name="All (unique)", index=False)
        for sheet, label in [("Asdaa", "Asdaa"), ("JeddahClub", "JeddahClub"),
                              ("TGArticles", "TelegramArticle")]:
            sub = df_unique[df_unique["Type"] == label].reset_index(drop=True)
            if not sub.empty:
                sub.to_excel(writer, sheet_name=sheet, index=False)
        if not df_dups.empty:
            df_dups.to_excel(writer, sheet_name="Duplicates", index=False)

    print(f"  Saved Excel: {xlsx_path}  ({len(df_unique)} unique, {len(df_dups)} dupes)")
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
        unique, dups = deduplicate(all_entries)
        print(f"\n  Unique: {len(unique)}, Duplicates: {len(dups)}")
        save_outputs(unique, dups)
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

    # Phase 5 — Deduplicate
    print("\n" + "=" * 60)
    print("Phase 5 — Deduplicating")
    print("=" * 60)
    all_entries = asdaa_enriched + jclub_enriched + tg_processed
    unique, dups = deduplicate(all_entries)
    print(f"  Before dedup: {len(all_entries)}")
    print(f"  Unique:       {len(unique)}")
    print(f"  Duplicates:   {len(dups)}")
    if dups:
        for d in dups[:10]:
            print(f"    → {d.get('msg_id','?'):15s}  {d.get('_dup_reason','')[:60]}")

    # Phase 6 — Save
    print("\n" + "=" * 60)
    print("Phase 6 — Saving outputs")
    print("=" * 60)
    save_outputs(unique, dups)

    # Summary
    ok_a = sum(1 for e in asdaa_enriched  if "ok" in e.get("fetch_status", ""))
    ok_j = sum(1 for e in jclub_enriched  if "ok" in e.get("fetch_status", ""))
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  Asdaa:          {len(asdaa_enriched):4d} URLs   ({ok_a} fetched OK)")
    print(f"  Jeddah Club:    {len(jclub_enriched):4d} URLs   ({ok_j} fetched OK)")
    print(f"  TG Articles:    {len(tg_processed):4d} posts")
    print(f"  Duplicates:     {len(dups):4d}")
    print(f"  Unique total:   {len(unique):4d}")
    print(f"\n  Output directory: {OUT_DIR}")


if __name__ == "__main__":
    main()
