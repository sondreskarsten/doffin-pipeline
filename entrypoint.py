import os
import sys
import time
from datetime import date, timedelta

from collect import DoffinClient
from parse import parse_notice, content_hash
from state import StateManager

API_KEY = os.environ.get("DOFFIN_API_KEY", "75908cf7b6464f82be21170a9136f0b9")
STATE_DIR = os.environ.get("STATE_DIR", "/home/claude/doffin-pipeline/data")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "")
GCS_PREFIX = os.environ.get("GCS_PREFIX", "doffin/state")
RUN_MODE = os.environ.get("RUN_MODE", "daily")
SAVE_EVERY = int(os.environ.get("SAVE_EVERY", "200"))
SCRAPE_DELAY = float(os.environ.get("SCRAPE_DELAY", "0.5"))


def sync_from_gcs(state_dir):
    if not GCS_BUCKET:
        return
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    os.makedirs(state_dir, exist_ok=True)

    for name in ["notices.parquet", "parties.parquet"]:
        blob = bucket.blob(f"{GCS_PREFIX}/{name}")
        local = os.path.join(state_dir, name)
        if blob.exists():
            blob.download_to_filename(local)
            print(f"  Downloaded {GCS_PREFIX}/{name} ({blob.size:,} bytes)", flush=True)


def sync_to_gcs(state_dir):
    if not GCS_BUCKET:
        return
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    for name in ["notices.parquet", "parties.parquet"]:
        local = os.path.join(state_dir, name)
        if os.path.exists(local):
            blob = bucket.blob(f"{GCS_PREFIX}/{name}")
            blob.upload_from_filename(local)
            print(f"  Uploaded {GCS_PREFIX}/{name} ({os.path.getsize(local):,} bytes)", flush=True)

    cl_dir = os.path.join(state_dir, "changelog")
    if os.path.isdir(cl_dir):
        for f in os.listdir(cl_dir):
            local = os.path.join(cl_dir, f)
            blob = bucket.blob(f"{GCS_PREFIX}/changelog/{f}")
            if not blob.exists():
                blob.upload_from_filename(local)
                print(f"  Uploaded changelog/{f}", flush=True)

    raw_dir = os.path.join(state_dir, "raw")
    if os.path.isdir(raw_dir):
        for day in os.listdir(raw_dir):
            day_dir = os.path.join(raw_dir, day)
            if not os.path.isdir(day_dir):
                continue
            for f in os.listdir(day_dir):
                local = os.path.join(day_dir, f)
                blob = bucket.blob(f"{GCS_PREFIX}/raw/{day}/{f}")
                blob.upload_from_filename(local)


def run_daily(client, state):
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    today = date.today().isoformat()
    known = state.known_ids()

    print(f"  Searching {yesterday} → {today}", flush=True)
    hits = client.search_all_in_range(yesterday, today)
    print(f"  Found {len(hits)} notices", flush=True)

    new_hits = [h for h in hits if h["id"] not in known]
    print(f"  New: {len(new_hits)}, already known: {len(hits) - len(new_hits)}", flush=True)

    ingested = 0
    errors = 0
    for i, hit in enumerate(new_hits):
        did = hit["id"]
        xml = client.download_xml(did)
        if xml is None:
            print(f"  SKIP {did}: 404", flush=True)
            errors += 1
            continue

        parsed = parse_notice(xml)
        state.save_raw(did, xml)
        changed = state.ingest_notice(did, xml, parsed, hit, source="daily")
        if changed:
            ingested += 1

        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{len(new_hits)} downloaded, {ingested} ingested", flush=True)
        if (i + 1) % SAVE_EVERY == 0:
            state.save()

    state.save()
    s = state.summary()
    print(f"\n  Daily complete: {ingested} new, {errors} errors", flush=True)
    print(f"  State: {s['notices']:,} notices, {s['parties']:,} parties", flush=True)


def run_backfill(client, state, start_date, end_date):
    known = state.known_ids()
    print(f"  Backfill: {start_date} → {end_date}", flush=True)
    print(f"  Known: {len(known):,}", flush=True)

    d = date.fromisoformat(start_date)
    d_end = date.fromisoformat(end_date)
    total_ingested = 0
    total_errors = 0

    while d <= d_end:
        chunk_end = min(d + timedelta(days=13), d_end)
        chunk_from = d.isoformat()
        chunk_to = chunk_end.isoformat()

        hits = client.search_all_in_range(chunk_from, chunk_to)
        new_hits = [h for h in hits if h["id"] not in known]

        for hit in new_hits:
            did = hit["id"]
            xml = client.download_xml(did)
            if xml is None:
                total_errors += 1
                continue

            parsed = parse_notice(xml)
            state.save_raw(did, xml)
            state.ingest_notice(did, xml, parsed, hit, source="backfill")
            known.add(did)
            total_ingested += 1

            if total_ingested % 50 == 0:
                print(f"  {chunk_from}→{chunk_to}: {total_ingested:,} ingested, {total_errors} errors", flush=True)
            if total_ingested % SAVE_EVERY == 0:
                state.save()
                if GCS_BUCKET:
                    sync_to_gcs(STATE_DIR)

        print(f"  {chunk_from}→{chunk_to}: {len(hits)} found, {len(new_hits)} new", flush=True)
        d = chunk_end + timedelta(days=1)

    state.save()
    s = state.summary()
    print(f"\n  Backfill complete: {total_ingested:,} ingested, {total_errors} errors", flush=True)
    print(f"  State: {s['notices']:,} notices, {s['parties']:,} parties", flush=True)


def main():
    print(f"{'='*60}", flush=True)
    print(f"  doffin-pipeline — mode: {RUN_MODE}", flush=True)
    print(f"  {date.today().isoformat()}", flush=True)
    print(f"{'='*60}", flush=True)

    if GCS_BUCKET:
        print(f"  GCS: {GCS_BUCKET}/{GCS_PREFIX}", flush=True)
        sync_from_gcs(STATE_DIR)

    client = DoffinClient(API_KEY, delay=SCRAPE_DELAY)
    state = StateManager(STATE_DIR)

    if RUN_MODE == "daily":
        run_daily(client, state)
    elif RUN_MODE == "backfill":
        start = os.environ.get("BACKFILL_START", "2017-01-01")
        end = os.environ.get("BACKFILL_END", date.today().isoformat())
        run_backfill(client, state, start, end)
    elif RUN_MODE == "test":
        run_backfill(client, state, "2026-03-24", "2026-03-25")
    else:
        print(f"Unknown RUN_MODE: {RUN_MODE}")
        sys.exit(1)

    if GCS_BUCKET:
        sync_to_gcs(STATE_DIR)
        print(f"  State synced to GCS", flush=True)


if __name__ == "__main__":
    main()
