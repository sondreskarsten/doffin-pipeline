# doffin-pipeline

CDC pipeline for Norwegian public procurement notices from the [Doffin](https://doffin.no) eForms API. Extracts organizations (orgnr), roles (buyer, winner, tenderer, subcontractor), and bid values from all 151K procurement notices published since 2017.

## Architecture

```
Doffin Public API                   State (Parquet on disk/GCS)
──────────────────                  ──────────────────────────
GET /v2/search          ─┐
  └ date range + type    │          notices.parquet
  └ paginate (100/page)  │            └ 1 row per doffin_id
                         ├─ parse ──▶   └ content_hash, root_type, dates
GET /v2/download/{id}   ─┘
  └ eForms UBL XML                  parties.parquet
  └ organization pool                 └ 1 row per (doffin_id, orgnr, role)
  └ role reference chains             └ buyer, winner, tenderer, subcontractor

                                    changelog/YYYY-MM-DD.parquet
                                      └ append-only, new + modified

                                    raw/YYYY-MM-DD/{id}.xml
                                      └ original XML, never modified
```

Follows the same CDC pattern as [losore-pipeline](https://github.com/sondreskarsten/losore-pipeline): hash-based change detection, Parquet state files, append-only changelog, raw preservation.

## Data model

### notices.parquet

One row per Doffin notice. Primary key: `doffin_id`.

| Column | Type | Source | Description |
|---|---|---|---|
| `doffin_id` | string | search | `"2026-105663"` |
| `root_type` | string | XML | `ContractNotice`, `ContractAwardNotice`, `PriorInformationNotice` |
| `notice_type_code` | string | XML | eForms subtype code |
| `issue_date` | string | XML | `"2026-03-23Z"` |
| `publication_date` | string | search | `"2026-03-24"` |
| `procedure_id` | string | XML | Contract folder ID linking related notices |
| `customization_id` | string | XML | eForms SDK version + Norwegian extension indicator |
| `content_hash` | string | derived | SHA-256 first 16 hex chars of raw XML |
| `xml_size` | int32 | derived | Raw XML byte count |
| `n_organizations` | int32 | derived | Count from eForms organization pool |
| `n_parties` | int32 | derived | Count of resolved role assignments |
| `first_seen` | string | derived | UTC ISO timestamp |
| `last_seen` | string | derived | UTC ISO timestamp |

### parties.parquet

One row per (notice, organization, role) combination. Composite key: `(doffin_id, orgnr, role, lot_id)`.

| Column | Type | Source | Description |
|---|---|---|---|
| `doffin_id` | string | notice | Parent notice |
| `role` | string | derived | `buyer`, `winner`, `tenderer`, `subcontractor` |
| `orgnr` | string | BT-501 | 9-digit Norwegian organization number, spaces stripped |
| `name` | string | BT-500 | Organization name |
| `lot_id` | string | XML | Which lot this tender applies to |
| `tender_value` | string | BT-720 | Bid amount (numeric string, NOK assumed) |
| `currency` | string | XML | Currency code from `currencyID` attribute |
| `is_winner` | bool | derived | True if this tender's LotTender is in a SettledContract |
| `is_leader` | bool | XML | True for group lead in consortium tenders |

### changelog/YYYY-MM-DD.parquet

Append-only. One file per pipeline run date.

| Column | Type | Description |
|---|---|---|
| `doffin_id` | string | Notice that changed |
| `change_type` | string | `new` or `modified` |
| `publication_date` | string | From search result |
| `root_type` | string | XML document type |
| `buyer_orgnr` | string | Buyer orgnr for quick filtering |
| `buyer_name` | string | Buyer name |
| `n_parties` | int32 | Total party rows for this notice |
| `detected_at` | string | UTC timestamp when change was found |
| `source` | string | `daily`, `backfill`, or `test` |

## eForms role resolution

eForms does not store roles as a flat field. Organizations sit in a normalized pool (`efac:Organizations`). Roles are determined by tracing a reference chain through the XML:

```
Buyer:          ContractingParty → Party → PartyIdentification/ID → ORG-xxxx

Winner:         LotResult
                  → SettledContract (OPT-315)
                    → LotTender (BT-3202)
                      → TenderingParty (OPT-310)
                        → Tenderer (OPT-300)
                          → Organization pool (OPT-200)

Losing bidder:  Same chain via LotResult → LotTender (OPT-320, all tenders),
                minus those referenced by SettledContract

Subcontractor:  TenderingParty → SubContractor → Organization pool

Consortium:     Multiple Tenderer entries under one TenderingParty,
                GroupLeadIndicator marks the lead entity
```

This resolution is implemented in `parse.py::_resolve_roles()`. The parser follows every reference to its terminal organization, producing flat `(role, orgnr, name, value)` tuples.

## Modules

### collect.py

`DoffinClient` wraps the Doffin Public API with rate limit handling.

| Method | Description |
|---|---|
| `search(params)` | Single search request, returns parsed JSON |
| `search_date_range(from, to)` | Paginated search across up to 10 pages (1000 results max) |
| `search_all_in_range(from, to)` | Recursive date-splitting for windows exceeding 1000 results |
| `download_xml(doffin_id)` | Fetch raw eForms XML, returns `None` on 404 |

Transport: `requests` library by default (for Cloud Run). Set `USE_CURL=1` for environments with proxy restrictions.

Rate limit behavior: ~30 requests before HTTP 429. `retry-after: 10` header. Client sleeps 12s on 429, retries up to 3 times.

### parse.py

Stateless XML extraction using `lxml`.

| Function | Description |
|---|---|
| `parse_notice(xml_bytes)` | Full extraction: organizations, buyer refs, tendering parties, lot tenders, settled contracts, lot results → resolved parties |
| `content_hash(xml_bytes)` | SHA-256 truncated to 16 hex chars |
| `clean_orgnr(raw)` | Strip whitespace from orgnr strings (`"999 601 391"` → `"999601391"`) |

eForms XML namespaces handled: `cac`, `cbc`, `efac`, `efbc`, `efext`, `ext`.

### state.py

`StateManager` handles Parquet persistence following the losore CDC pattern.

| Method | Description |
|---|---|
| `known_ids()` | Set of all ingested doffin_ids |
| `ingest_notice(id, xml, parsed, hit)` | Insert or update. Returns `True` if new/changed, `False` if unchanged (same content_hash) |
| `save_raw(id, xml)` | Write XML to `raw/YYYY-MM-DD/{id}.xml` |
| `save()` | Flush notices, parties, and changelog to Parquet (ZSTD compressed) |
| `summary()` | Dict with counts of notices, parties, changelog entries |

Change detection: SHA-256 of raw XML bytes. If hash matches existing record, `ingest_notice` returns `False` and only updates `last_seen`. If hash differs, replaces all party rows for that notice and writes a `modified` changelog entry.

### entrypoint.py

Mode dispatch and GCS synchronization.

| Mode | Trigger | Behavior |
|---|---|---|
| `daily` | `RUN_MODE=daily` | Search yesterday+today, download new notices only |
| `backfill` | `RUN_MODE=backfill` | Walk date range in 14-day chunks, download all unknown notices |
| `test` | `RUN_MODE=test` | Backfill 2026-03-24 to 2026-03-25 (66 notices) |

GCS sync: if `GCS_BUCKET` is set, downloads state from GCS before run and uploads after. Changelog and raw XML uploaded incrementally.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `DOFFIN_API_KEY` | (hardcoded dev key) | Azure APIM subscription key |
| `STATE_DIR` | `/home/claude/doffin-pipeline/data` | Local state directory |
| `GCS_BUCKET` | (empty = no GCS) | GCS bucket for state persistence |
| `GCS_PREFIX` | `doffin/state` | GCS path prefix |
| `RUN_MODE` | `daily` | `daily`, `backfill`, or `test` |
| `BACKFILL_START` | `2017-01-01` | Start date for backfill mode |
| `BACKFILL_END` | today | End date for backfill mode |
| `SAVE_EVERY` | `200` | Checkpoint interval (notices between saves) |
| `SCRAPE_DELAY` | `0.5` | Seconds between API requests |
| `USE_CURL` | `0` | Set to `1` to use subprocess curl instead of requests |

## Usage

```bash
# Test mode (66 notices, ~30 seconds)
RUN_MODE=test python3 entrypoint.py

# Daily mode
RUN_MODE=daily python3 entrypoint.py

# Full backfill from 2017 (~151K notices, ~14 hours)
RUN_MODE=backfill BACKFILL_START=2017-01-01 python3 entrypoint.py

# With GCS persistence
GCS_BUCKET=sondre_brreg_data RUN_MODE=daily python3 entrypoint.py
```

## Docker

```bash
docker build -t doffin-pipeline .
docker run -e DOFFIN_API_KEY=... -e RUN_MODE=daily doffin-pipeline
```

## Cloud Run deployment

```bash
# Build and push
gcloud builds submit --tag europe-north1-docker.pkg.dev/sondreskarsten-d7d14/r-images/doffin-pipeline:latest

# Create job
gcloud run jobs create doffin-pipeline \
  --image europe-north1-docker.pkg.dev/sondreskarsten-d7d14/r-images/doffin-pipeline:latest \
  --region europe-west4 \
  --cpu 1 --memory 1Gi --task-timeout 36000s --max-retries 0 \
  --set-env-vars "GCS_BUCKET=sondre_brreg_data,GCS_PREFIX=doffin/state,RUN_MODE=daily,DOFFIN_API_KEY=..."

# Schedule daily at 06:00 Oslo time (after Doffin publishes)
gcloud scheduler jobs create http doffin-daily \
  --location europe-west4 \
  --schedule "0 6 * * *" --time-zone "Europe/Oslo" \
  --uri "https://europe-west4-run.googleapis.com/apis/run.googleapis.com/v2/projects/sondreskarsten-d7d14/locations/europe-west4/jobs/doffin-pipeline:run" \
  --http-method POST \
  --oauth-service-account-email s1sfreracct@sondreskarsten-d7d14.iam.gserviceaccount.com
```

## API specification (empirically tested 2026-03-25)

Two working endpoints at `https://api.doffin.no/public`. All other documented paths return 404.

### GET /v2/search

Returns JSON with notice summaries. Buyer orgnr and winner orgnr in search results but no tenderer/subcontractor data.

| Parameter | Behavior (observed) |
|---|---|
| `numHitsPerPage` | Default 15. Accepts 200+ (documented max of 100 not enforced). |
| `page` | **1-indexed** (page=0 errors). Hard cap at page 10 → 1000 results max per query. |
| `sortBy` | All 6 work: `PUBLICATION_DATE_ASC/DESC`, `DEADLINE`, `RELEVANCE`, `ESTIMATED_VALUE_ASC/DESC` |
| `type` | 13 values. Multi-value via repeated param. |
| `status` | `ACTIVE` (1,517), `EXPIRED` (75,383), `AWARDED` (5,612), `CANCELLED` (1,443) |
| `issueDateFrom/To` | `yyyy-mm-dd`. Can use independently. |
| `searchString` | Free text across title + description |
| `cpvCode` | Full 8-digit only. Partial/wildcard returns 0 results. |
| `location` | NUTS codes only (`NO081`, `anyw`). County codes and city names fail silently. |
| `estimatedValueFrom/To` | Numeric, no currency. |

JSON responses contain unescaped control characters. Parse with `json.loads(text, strict=False)`.

### GET /v2/download/{doffinId}

Returns raw eForms UBL 2.3 XML. Content-Type is `application/octet-stream` despite being XML. All 151K notices (including pre-2023 legacy) are served as retroconverted eForms.

### Rate limiting

~30 requests before HTTP 429. Response includes `retry-after: 10` header. No rate limit headers on successful responses.

### Corpus

| Year | Notices |
|---|---|
| 2017 | 14,202 |
| 2018 | 15,019 |
| 2019 | 16,123 |
| 2020 | 16,515 |
| 2021 | 17,545 |
| 2022 | 17,596 |
| 2023 | 17,824 |
| 2024 | 15,956 |
| 2025 | 16,330 |
| 2026 (partial) | 4,178 |
| **Total** | **151,288** |

By type: COMPETITION 97,821 / RESULT 41,073 / PLANNING 12,394 / other 95.

## Observed irregularities

1. `numHitsPerPage` default is 15, not 20 as documented in OpenAPI spec.
2. `page` is 1-indexed. The spec implies 0-indexed. `page=0` returns an error.
3. `numHitsPerPage` accepts values above the documented max of 100.
4. Buyer orgnr formatting inconsistent in search results: some have spaces (`"999 601 391"`), some don't (`"938801363"`). `parse.py` normalizes via `clean_orgnr()`.
5. `status` is null for RESULT-type notices in search results.
6. Search results contain duplicate IDs across pages for multi-type queries. Collector deduplicates by ID.

## Output from test mode (2026-03-24, single day)

```
notices: 66
parties: 132
  buyer: 66
  winner: 28
  tenderer: 33
  subcontractor: 5
unique orgnr: 96
root_types: ContractNotice 43, ContractAwardNotice 20, PriorInformationNotice 3
```

## Relationship to other pipelines

| Pipeline | Data source | Key | What it tracks |
|---|---|---|---|
| [losore-pipeline](https://github.com/sondreskarsten/losore-pipeline) | Løsøreregisteret API | `(orgnr, dokumentnummer)` | Movable property liens, status changes |
| **doffin-pipeline** | Doffin Public API | `(doffin_id)` | Public procurement participants, contract awards |
| kunngjøring (w2.brreg.no) | BRREG legacy JSP | `(orgnr, kid)` | Corporate events: fusjon, fisjon, konkurs, styre |

Cross-referencing orgnr across all three gives a comprehensive view of a company's public activity: who they borrow from (losore), who they contract with (doffin), and what corporate events they undergo (kunngjøring).
