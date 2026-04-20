"""
Phase 2 – AI Metadata Extraction (checkpointed)
Reads checkpoints/raw_messages.json, calls Claude API in batches of 50,
and writes checkpoints/progress.json after each batch.
Safe to interrupt (Ctrl+C) and restart — resumes from last saved record.
"""

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import anthropic
from dotenv import load_dotenv
from tqdm import tqdm

ROOT = Path(__file__).parent.parent
RAW_PATH = ROOT / "checkpoints" / "raw_messages.json"
PROGRESS_PATH = ROOT / "checkpoints" / "progress.json"
PROGRESS_TMP = ROOT / "checkpoints" / "progress.json.tmp"

BATCH_SIZE = 50
API_DELAY = 0.5  # seconds between calls
MAX_RETRIES = 4

load_dotenv(ROOT / ".env")

SYSTEM_PROMPT = """You are a data extraction assistant specializing in Arabic Islamic lecture metadata.
Given information about a Telegram audio message from Sheikh Hassan Al-Dugheiri's channel, extract
the structured metadata fields as a JSON object.

Rules:
- Type: "Khutba" if the content is a Friday sermon (خطبة جمعة), Eid sermon (خطبة عيد), or rain-prayer
  sermon (خطبة الاستسقاء). Otherwise use a short English label like "Series" or "Lecture".
- SeriesName: The Arabic name of the series or sermon title. Use the full Arabic text.
- SequenceInSeries: The lesson/episode number as an integer string (e.g. "3"). Null if not a series.
- OriginalAuthor: The author of the book being taught, if applicable. Null otherwise.
- Location_Online: "Online" if the text says "عن بُعد" or similar. Otherwise the mosque name in Arabic.
  If unknown, use null.
- Sheikh: Default "حسن بن محمد منصور الدغريري" unless a different name is clearly stated.
- DateInGreg: Convert the raw date DD.MM.YYYY to DD/MM/YYYY. Use the raw date if already provided.
- Category: One of: "Khutba", "Aqeedah", "Fiqh", "Hadeeth", "Quran", "Seerah", "Other".

Respond ONLY with a valid JSON object — no markdown, no explanation.

Examples:
Input: audio_title="وقفات رمضان ( 3 ) – خطبة جمعة" | date="09.10.2018" | clip="17:25"
Output: {"Type":"Khutba","SeriesName":"وقفات رمضان","SequenceInSeries":"3","OriginalAuthor":null,
"Location_Online":"Online","Sheikh":"حسن بن محمد منصور الدغريري","DateInGreg":"09/10/2018",
"Category":"Khutba","doubts":"none"}

Input: audio_title="تأسيس الأحكام الدرس 12" | text="🗓الإثنين 28 المحرم 1440 هـ" | date="08.10.2018"
Output: {"Type":"Series","SeriesName":"تأسيس الأحكام","SequenceInSeries":"12","OriginalAuthor":null,
"Location_Online":null,"Sheikh":"حسن بن محمد منصور الدغريري","DateInGreg":"08/10/2018",
"Category":"Fiqh","doubts":"none"}"""


def load_progress() -> dict:
    """Load existing progress or initialise from raw_messages.json."""
    if PROGRESS_PATH.exists():
        with open(PROGRESS_PATH, encoding="utf-8") as f:
            data = json.load(f)
        done = sum(1 for r in data["records"] if r["status"] == "done")
        print(f"Resuming: {done}/{data['metadata']['total']} records already done.")
        return data

    if not RAW_PATH.exists():
        print("ERROR: checkpoints/raw_messages.json not found. Run parse_html.py first.")
        sys.exit(1)

    with open(RAW_PATH, encoding="utf-8") as f:
        raw = json.load(f)

    records = [
        {**r, "status": "pending", "extracted": None, "doubts": None}
        for r in raw
    ]
    return {
        "metadata": {
            "total": len(records),
            "processed": 0,
            "batch_size": BATCH_SIZE,
            "last_updated": "",
        },
        "records": records,
    }


def save_progress(data: dict):
    """Atomically overwrite progress.json."""
    data["metadata"]["last_updated"] = datetime.now(timezone.utc).isoformat()
    PROGRESS_TMP.parent.mkdir(exist_ok=True)
    with open(PROGRESS_TMP, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    PROGRESS_TMP.rename(PROGRESS_PATH)


def call_claude(client: anthropic.Anthropic, record: dict) -> tuple[dict, str]:
    """Call Claude API and return (extracted_fields, doubts)."""
    user_content = (
        f"audio_title={record['audio_title']!r}\n"
        f"date={record['date_raw']!r}\n"
        f"clip={record['clip_length']!r}\n"
        f"message_text={record['message_text'][:800]!r}"
    )

    for attempt in range(MAX_RETRIES):
        try:
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=512,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_content}],
            )
            text = response.content[0].text.strip()
            parsed = json.loads(text)
            doubts = parsed.pop("doubts", "none")
            return parsed, doubts
        except json.JSONDecodeError:
            doubts = "json_parse_error"
            return {}, doubts
        except anthropic.RateLimitError:
            wait = 2 ** (attempt + 1)
            print(f"\n  Rate limit — waiting {wait}s …", flush=True)
            time.sleep(wait)
        except anthropic.APIError as e:
            wait = 2 ** (attempt + 1)
            print(f"\n  API error ({e}) — waiting {wait}s …", flush=True)
            time.sleep(wait)

    return {}, "api_error_after_retries"


_shutdown = False


def _handle_sigint(sig, frame):
    global _shutdown
    print("\n\nInterrupt received — finishing current batch then saving …", flush=True)
    _shutdown = True


def main():
    global _shutdown
    signal.signal(signal.SIGINT, _handle_sigint)

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY not set. Copy .env.example to .env and add your key.")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)
    data = load_progress()

    pending = [r for r in data["records"] if r["status"] != "done"]
    print(f"\n=== Phase 2: AI Extraction ===")
    print(f"Pending: {len(pending)} records  |  Batch size: {BATCH_SIZE}\n")

    processed_this_run = 0
    batch_buffer = []

    with tqdm(total=len(pending), unit="rec") as pbar:
        for record in pending:
            if _shutdown:
                break

            extracted, doubts = call_claude(client, record)
            record["extracted"] = extracted
            record["doubts"] = doubts
            record["status"] = "done" if extracted else "failed"
            batch_buffer.append(record)
            processed_this_run += 1
            pbar.update(1)

            time.sleep(API_DELAY)

            if len(batch_buffer) >= BATCH_SIZE:
                data["metadata"]["processed"] += len(batch_buffer)
                save_progress(data)
                batch_buffer.clear()
                tqdm.write(f"  ✓ Batch saved ({data['metadata']['processed']}/{data['metadata']['total']})")

    # Save any remaining records
    if batch_buffer:
        data["metadata"]["processed"] += len(batch_buffer)
        save_progress(data)

    done_count = sum(1 for r in data["records"] if r["status"] == "done")
    failed_count = sum(1 for r in data["records"] if r["status"] == "failed")
    print(f"\nSession complete: {processed_this_run} processed this run")
    print(f"Total done: {done_count}  |  Failed: {failed_count}")
    print(f"Checkpoint → {PROGRESS_PATH.relative_to(ROOT)}")

    if done_count == data["metadata"]["total"]:
        print("\nAll records extracted! Run src/export_excel.py next.")
    else:
        remaining = data["metadata"]["total"] - done_count
        print(f"\n{remaining} records remaining. Re-run this script to continue.")


if __name__ == "__main__":
    main()
