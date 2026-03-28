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
| `notice_type_code` | string | XML | eForms subtype code (e.g., `cn-standard`, `can-standard`, `pin-only`) |
| `issue_date` | string | XML | `"2026-03-23+02:00"` (timezone-aware, no trailing Z) |
| `publication_date` | string | search | `"2026-03-24"` |
| `procedure_id` | string | XML | Contract folder ID linking related notices. Null for PriorInformationNotice (7.4%). |
| `customization_id` | string | XML | eForms SDK version. EU notices: `eforms-sdk-1.7#...eforms:eu`. National below-threshold: `...eforms:national`. |
| `content_hash` | string | derived | SHA-256 first 16 hex chars of raw XML |
| `xml_size` | int32 | derived | Raw XML byte count (8 KB – 872 KB, avg 27 KB) |
| `n_organizations` | int32 | derived | Count from eForms organization pool (1–185) |
| `n_parties` | int32 | derived | Count of resolved role assignments (1–184) |
| `first_seen` | string | derived | UTC ISO timestamp |
| `last_seen` | string | derived | UTC ISO timestamp |

### parties.parquet

One row per (notice, organization, role) combination. Composite key: `(doffin_id, orgnr, role, lot_id)`.

| Column | Type | Source | Description |
|---|---|---|---|
| `doffin_id` | string | notice | Parent notice |
| `role` | string | derived | `buyer`, `winner`, `tenderer`, `subcontractor` |
| `orgnr` | string | BT-501 | 9-digit Norwegian org number. Normalized from `NO`-prefix, `MVA`-suffix, and whitespace variants. `None` for foreign orgs and legacy notices missing `cbc:CompanyID`. |
| `name` | string | BT-500 | Organization name. Never null. |
| `lot_id` | string | XML | Which lot this tender applies to. `None` for buyers. `"MISSING"` in retroconverted legacy notices. |
| `tender_value` | string | BT-720 | Bid amount as numeric string. `None` for buyers and subcontractors. |
| `currency` | string | XML | ISO currency code from `currencyID` attribute. Usually `NOK`. Also `EUR`, `SEK`, `DKK`, `USD`, `GBP`. |
| `is_winner` | bool | derived | True if this tender's LotTender is in a SettledContract. Consistent with `role`: `is_winner=True` ↔ `role="winner"`. |
| `is_leader` | bool | XML | True for group lead in consortium tenders |

### changelog/YYYY-MM-DD.parquet

One file per pipeline run date. Overwritten at each checkpoint (file grows during long backfill runs).

| Column | Type | Description |
|---|---|---|
| `doffin_id` | string | Notice that changed |
| `change_type` | string | `new` or `modified` |
| `publication_date` | string | From search result |
| `root_type` | string | XML document type |
| `buyer_orgnr` | string | Buyer orgnr for quick portfolio filtering. Null when buyer has no CompanyID in XML. |
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

This resolution is implemented in `parse.py::_resolve_roles()`. The parser follows every reference to its terminal organization, producing flat `(role, orgnr, name, value)` tuples. See `docs/vignette-eforms-role-resolution.md` for a worked example with 11 organizations and 22 tenders.

## Modules

### collect.py

`DoffinClient` wraps the Doffin Public API with rate limit handling.

| Method | Description |
|---|---|
| `search(params)` | Single search request, returns parsed JSON (`strict=False` for control chars) |
| `search_date_range(from, to)` | Paginated search across up to 10 pages (1000 results max) |
| `search_all_in_range(from, to)` | Recursive date-splitting for windows exceeding 1000 results |
| `download_xml(doffin_id)` | Fetch raw eForms XML as `bytes`, returns `None` on 404 |

Transport: `requests` library by default (for Cloud Run). Set `USE_CURL=1` for environments with proxy restrictions.

Rate limit behavior: ~30 requests before HTTP 429. `retry-after: 10` header. Client sleeps 12s on 429, retries up to 3 times.

### parse.py

Stateless XML extraction using `lxml`.

| Function | Description |
|---|---|
| `parse_notice(xml_bytes)` | Full extraction: organizations, buyer refs, tendering parties, lot tenders, settled contracts, lot results → resolved parties |
| `content_hash(xml_bytes)` | SHA-256 truncated to 16 hex chars |
| `clean_orgnr(raw)` | Normalize orgnr from XML: strips whitespace/`NO`-prefix/`MVA`-suffix/`Org.nr.`-prefix, returns `None` for foreign/garbage IDs |

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
| `test` | `RUN_MODE=test` | Backfill 2026-03-24 to 2026-03-25 (~125 notices) |

GCS sync: if `GCS_BUCKET` is set, downloads state from GCS before run and uploads after. State files and changelog overwritten at each checkpoint. Raw XML uploaded at run completion.

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
# Test mode (~125 notices, ~60 seconds)
RUN_MODE=test python3 entrypoint.py

# Daily mode
RUN_MODE=daily python3 entrypoint.py

# Full backfill from 2017 (~151K notices, ~60 hours at rate limit)
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

### Infrastructure

| Resource | Value |
|---|---|
| Docker image | `europe-west4-docker.pkg.dev/sondreskarsten-d7d14/losore/doffin-pipeline:latest` |
| Artifact Registry repo | `losore` in `europe-west4` (shared with losore-pipeline) |
| Cloud Run Job | `doffin-pipeline` in `europe-west4` |
| Cloud Scheduler | `doffin-daily` in `europe-west1` (only available European region) |
| Service account | `s1sfreracct@sondreskarsten-d7d14.iam.gserviceaccount.com` |
| Build source | `gs://sondre_brreg_data/doffin/_build/source.tar.gz` |
| State on GCS | `gs://sondre_brreg_data/doffin/state/` |

### Build and deploy

```bash
# 1. Package source
tar czf /tmp/doffin-source.tar.gz collect.py parse.py state.py entrypoint.py Dockerfile requirements.txt

# 2. Upload to GCS
gsutil cp /tmp/doffin-source.tar.gz gs://sondre_brreg_data/doffin/_build/source.tar.gz

# 3. Cloud Build (via API — storageSource, not gitSource)
#    Builds from GCS tarball, pushes to Artifact Registry europe-west4

# 4. Job config
#    daily:    RUN_MODE=daily,    timeout=3600s  (1h)
#    backfill: RUN_MODE=backfill, timeout=43200s (12h)

# 5. Scheduler: 0 6 * * * Europe/Oslo → POST .../jobs/doffin-pipeline:run
#    Paused during backfill, resume after completion.
```

### GCS state layout

```
gs://sondre_brreg_data/doffin/state/
    notices.parquet          ~1 MB at 37K notices
    parties.parquet          ~600 KB at 53K parties
    changelog/
        2026-03-25.parquet   first run
        2026-03-28.parquet   resumed backfill
    raw/
        2026-03-25/          21,500 XML files from first run
        2026-03-28/          continuing
```

### Backfill status

Multiple 12-hour Cloud Run Job executions with GCS checkpoint resume. State files uploaded every 500 notices.

| Execution | Date | Notices | Outcome |
|---|---|---|---|
| `s25hr` | 2026-03-25 | 0 → 21,500 | Timeout at 12h |
| `4fzdf` | 2026-03-25 | — | Failed (scheduler fired during backfill, `sync_from_gcs` bug) |
| `9grdz` | 2026-03-28 | — | Failed (`blob.size` None bug in `sync_from_gcs`) |
| `b9ggr` | 2026-03-28 | 21,500 → ongoing | Running |

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

## Data quality

### orgnr normalization

`cbc:CompanyID` in eForms XML contains inconsistent formats. `clean_orgnr()` handles:

| Input format | Example | Output | Count in backfill |
|---|---|---|---|
| Plain 9-digit | `938801363` | `938801363` | ~85% |
| Spaces | `999 601 391` | `999601391` | ~5% |
| `NO` prefix | `NO981604032` | `981604032` | 18 |
| `NO` prefix + `MVA` | `NO952522035MVA` | `952522035` | 161 |
| `MVA` suffix | `932151286MVA` | `932151286` | 16 |
| `Org.nr.` prefix | `Org.nr.978693024` | `978693024` | ~20 |
| Foreign identifier | `SE556289739601`, `2348368-2` | `None` | ~100 |
| Garbage | `xxxxxxx`, `N/A`, `0` | `None` | ~50 |
| Zero-width spaces | `983974791<U+200B>` | `983974791` | ~5 |

### Null orgnr in legacy data

Retroconverted 2017–2018 notices omit `cbc:CompanyID` for many organizations despite having valid names. This is a source data gap in the retroconversion, not a parser issue.

| Role | Null orgnr rate (2017) | Null orgnr rate (2023+) |
|---|---|---|
| buyer | ~7% | 0% |
| winner | ~41% | 0% |
| tenderer | ~64% | 0% |

Null-orgnr organizations are real entities (Atea AS, Veidekke, Norconsult, Tysvær kommune, etc.). A post-backfill enrichment step matching names against the enhetsregisteret API would recover most of these.

### Other known gaps

`lot_id = "MISSING"` appears on ~183 party rows from retroconverted notices where the original data had no lot structure. `procedure_id` is null on ~7.4% of notices (PriorInformationNotice type, which legitimately lacks a contract folder).

## Observed irregularities

1. `numHitsPerPage` default is 15, not 20 as documented in OpenAPI spec.
2. `page` is 1-indexed. The spec implies 0-indexed. `page=0` returns an error.
3. `numHitsPerPage` accepts values above the documented max of 100.
4. Buyer/winner `organizationId` formatting inconsistent in search results: spaces in some orgnr (`"999 601 391"`), not in others. XML `cbc:CompanyID` has additional variations (`NO`-prefix, `MVA`-suffix, `Org.nr.`-prefix, foreign formats, zero-width Unicode).
5. `status` is null for RESULT-type notices in search results.
6. Search results contain duplicate IDs across pages — the API's `PUBLICATION_DATE_ASC` sort is unstable within the same publication date, causing ~20% of IDs to appear on multiple pages. `search_date_range()` deduplicates by ID during pagination.
7. `issue_date` has `+02:00` timezone suffix on all notices (never `Z`), inconsistent with the API documentation showing UTC.

## Output from test mode (2026-03-24 to 2026-03-25)

```
notices: 125
parties: 235
  buyer: 137
  winner: 44
  tenderer: 47
  subcontractor: 7
unique orgnr: 165
null orgnr: 5
format violations: 0
root_types: ContractNotice 76, ContractAwardNotice 39, PriorInformationNotice 10
customization_id: EU 108, national 17
```

## Changelog

### 2026-03-28

* Fixed `sync_from_gcs` crash on resume: `blob.size` is `None` after `download_to_filename()` — use `os.path.getsize()` instead. This bug caused every execution after the first to fail immediately (`ae03239`).
* Fixed `search_date_range` returning duplicate IDs across pages. The API's `PUBLICATION_DATE_ASC` sort is unstable within the same publication date — ~20% of IDs appear on multiple pages. Added `seen` set dedup during pagination (`a546123`).

### 2026-03-26

* Extended `clean_orgnr()` to handle `NO` prefix, `MVA` suffix, `Org.nr.` prefix, and zero-width Unicode spaces. Previously passed through raw strings like `NO952522035MVA`. Now extracts the 9-digit orgnr and returns `None` for foreign/garbage identifiers. Reduces format violations from 447 to 0 on modern notices (`b163d5f`).

### 2026-03-25

* Fixed `sync_to_gcs` changelog upload: removed `blob.exists()` guard that prevented overwriting the growing changelog file during long backfill runs (`8f9a6ae`).
* Initial production deployment: Docker image built via Cloud Build, Cloud Run Job created in `europe-west4`, Cloud Scheduler set to `0 6 * * *` Europe/Oslo.
* Comprehensive docstrings on all functions/methods/classes, 3 vignettes (`9dfc7f0`).
* Initial README (`58f0bdc`).
* v1 pipeline: collect, parse, state, entrypoint (`d4a1626`).

## Relationship to other pipelines

| Pipeline | Data source | Key | What it tracks |
|---|---|---|---|
| [losore-pipeline](https://github.com/sondreskarsten/losore-pipeline) | Løsøreregisteret API | `(orgnr, dokumentnummer)` | Movable property liens, status changes |
| **doffin-pipeline** | Doffin Public API | `(doffin_id)` | Public procurement participants, contract awards |
| kunngjøring (w2.brreg.no) | BRREG legacy JSP | `(orgnr, kid)` | Corporate events: fusjon, fisjon, konkurs, styre |

Cross-referencing orgnr across all three gives a comprehensive view of a company's public activity: who they borrow from (losore), who they contract with (doffin), and what corporate events they undergo (kunngjøring).
