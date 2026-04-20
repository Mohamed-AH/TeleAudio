"""
Phase 1 – HTML Parser
Extracts every audio-attachment message from all 4 Telegram HTML export files
and writes checkpoints/raw_messages.json.
"""

import json
import re
import sys
from pathlib import Path

from bs4 import BeautifulSoup

ROOT = Path(__file__).parent.parent
HTML_FILES = [
    ROOT / "messages.html",
    ROOT / "messages2.html",
    ROOT / "messages3.html",
    ROOT / "messages4.html",
]
OUTPUT = ROOT / "checkpoints" / "raw_messages.json"


def extract_date(message_div) -> str:
    """Return DD.MM.YYYY from the date element's title attribute."""
    date_el = message_div.find("div", class_="date")
    if date_el and date_el.get("title"):
        # title format: "DD.MM.YYYY HH:MM:SS UTC+03:00"
        return date_el["title"].split(" ")[0]
    return ""


def extract_text(message_div) -> str:
    """Return the plain text content of the message text div."""
    text_el = message_div.find("div", class_="text")
    if text_el:
        return text_el.get_text(separator="\n", strip=True)
    return ""


def parse_file(html_path: Path) -> list[dict]:
    """Parse one HTML file and return a list of audio record dicts."""
    print(f"  Parsing {html_path.name} …", flush=True)
    with open(html_path, encoding="utf-8") as f:
        soup = BeautifulSoup(f, "lxml")

    records = []
    skipped = 0

    for message_div in soup.select("div.message.default"):
        msg_id = message_div.get("id", "")

        # Find audio attachments: media_audio_file OR media_file with audio extension
        audio_link = message_div.find("a", class_=re.compile(r"media_audio_file"))
        if not audio_link:
            # Also catch generic media_file links that are audio formats
            candidate = message_div.find("a", class_=re.compile(r"media_file"))
            if candidate:
                href_check = candidate.get("href", "").lower()
                if any(href_check.endswith(ext) for ext in (".m4a", ".mp3", ".aac", ".amr", ".ogg", ".opus")):
                    audio_link = candidate
        if not audio_link:
            skipped += 1
            continue

        href = audio_link.get("href", "")
        # href is like "files/FILENAME.m4a" or "files/FILENAME.mp3"
        telegram_filename = Path(href).name if href else ""

        # Audio title and clip length from inside the audio block
        title_el = audio_link.find("div", class_="title")
        audio_title = title_el.get_text(strip=True) if title_el else ""

        status_el = audio_link.find("div", class_="status")
        clip_length = status_el.get_text(strip=True) if status_el else ""

        date_raw = extract_date(message_div)
        message_text = extract_text(message_div)

        record = {
            "id": f"{html_path.name}::{msg_id}",
            "source_file": html_path.name,
            "message_id": msg_id,
            "telegram_filename": telegram_filename,
            "audio_title": audio_title,
            "clip_length": clip_length,
            "date_raw": date_raw,
            "message_text": message_text,
        }
        records.append(record)

    print(f"    → {len(records)} audio records, {skipped} non-audio messages skipped")
    return records


def main():
    print("=== Phase 1: HTML Parsing ===\n")

    all_records: list[dict] = []
    for html_file in HTML_FILES:
        if not html_file.exists():
            print(f"  WARNING: {html_file.name} not found — skipping")
            continue
        all_records.extend(parse_file(html_file))

    print(f"\nTotal audio records extracted: {len(all_records)}")

    OUTPUT.parent.mkdir(exist_ok=True)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)

    print(f"Saved → {OUTPUT.relative_to(ROOT)}")
    print("\nPhase 1 complete. Run src/extract_metadata.py next.")


if __name__ == "__main__":
    main()
