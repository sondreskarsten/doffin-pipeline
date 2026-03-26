# Doffin Public API — empirical specification

Tested 2026-03-25 against `https://api.doffin.no/public` via 40+ requests covering every parameter and boundary condition. This document records observed behavior, which diverges from the OpenAPI spec in 7 places.

## Endpoints

Only two endpoints exist. All others return HTTP 404 with `{"statusCode": 404, "message": "Resource not found"}`.

| Endpoint | Response |
|---|---|
| `GET /v2/search` | JSON notice summaries |
| `GET /v2/download/{doffinId}` | eForms UBL 2.3 XML (`application/octet-stream`) |
| `GET /v2/notices/{id}/documents` | 404 |
| `GET /v2/search/cpv` | 404 |
| `GET /v2/reference-data/*` | 404 |
| `GET /v2/notices/{id}` | 404 |
| `GET /v1/*` | 404 |

## Authentication

Header `Ocp-Apim-Subscription-Key` or query parameter `subscription-key`. Both work. Missing key returns 401 "Access denied due to missing subscription key". Invalid key returns 401 "Access denied due to invalid subscription key" (different message).

## GET /v2/search — parameters

### numHitsPerPage

| Spec | Observed |
|---|---|
| Default: 20 | **Default: 15** |
| Max: 100 | **Accepts 200+** (max not enforced) |

### page

| Spec | Observed |
|---|---|
| 0-indexed | **1-indexed** (page=0 returns error/empty) |
| — | **Hard cap at page 10** |

Maximum accessible results per query: 10 pages × 100/page = 1 000. Page 11+ returns `{"numHitsTotal": N, "numHitsAccessible": 1000, "hits": []}`.

`numHitsAccessible` equals `min(numHitsTotal, 1000)`.

### sortBy

All 6 values work.

| Value | Behavior |
|---|---|
| `PUBLICATION_DATE_ASC` | Oldest publication date first |
| `PUBLICATION_DATE_DESC` | Newest first (effective default) |
| `DEADLINE` | By submission deadline |
| `RELEVANCE` | Text relevance (requires `searchString`) |
| `ESTIMATED_VALUE_ASC` | Lowest estimated value first |
| `ESTIMATED_VALUE_DESC` | Highest estimated value first |

### type

13 values. Multi-value via repeated parameter (`type=X&type=Y`).

| Type | Count (2026-03-25) |
|---|---|
| `COMPETITION` | 97,821 |
| `ANNOUNCEMENT_OF_COMPETITION` | 96,728 |
| `RESULT` | 41,073 |
| `ANNOUNCEMENT_OF_CONCLUSION_OF_CONTRACT` | 29,963 |
| `ADVISORY_NOTICE` | 12,360 |
| `PLANNING` | 12,394 |
| `CANCELLED_OR_MISSING_CONCLUSION_OF_CONTRACT` | 7,965 |
| `ANNOUNCEMENT_OF_INTENT` | 3,559 |
| `DYNAMIC_PURCHASING_SCHEME` | 754 |
| `PRE_ANNOUNCEMENT` | 185 |
| `QUALIFICATION_SCHEME` | 154 |
| `CHANGE_OF_CONCLUSION_OF_CONTRACT` | 74 |
| `NOTICE_ON_BUYER_PROFILE` | 34 |

### status

| Status | Count |
|---|---|
| `ACTIVE` | 1,517 |
| `EXPIRED` | 75,383 |
| `AWARDED` | 5,612 |
| `CANCELLED` | 1,443 |

Status is `null` for RESULT-type notices. Only COMPETITION-type notices carry status values.

### issueDateFrom / issueDateTo

Format: `yyyy-mm-dd`. Can be used independently or together. Both boundaries are inclusive.

### searchString

Free text search across title and description. Case-insensitive.

### cpvCode

Must be the full 8-digit CPV code (e.g., `72000000`). Partial codes (`72`), wildcards (`72*`), and truncated codes silently return 0 results.

### location

NUTS codes only: `NO081` (Oslo), `NO0A2` (Vestland), `anyw` (unspecified/national). County number codes (`03`), city names (`Oslo`), and country codes (`NO`) return 0 or unrelated results.

### estimatedValueFrom / estimatedValueTo

Numeric. No currency specification. Filters on the `estimatedValue.amount` field in the response.

## GET /v2/search — response structure

```json
{
  "numHitsTotal": 151288,
  "numHitsAccessible": 1000,
  "hits": [
    {
      "id": "2026-105663",
      "buyer": [
        {
          "id": "326cafd8...",
          "organizationId": "938801363",
          "name": "Sarpsborg kommune"
        }
      ],
      "heading": "...",
      "description": "...",
      "locationId": ["NO085"],
      "estimatedValue": {"currencyCode": "NOK", "amount": 5000000.0},
      "type": "ANNOUNCEMENT_OF_CONCLUSION_OF_CONTRACT",
      "allTypes": ["RESULT", "ANNOUNCEMENT_OF_CONCLUSION_OF_CONTRACT"],
      "status": null,
      "issueDate": "2026-03-20T14:12:39Z",
      "deadline": null,
      "publicationDate": "2026-03-24",
      "receivedTenders": 22,
      "allReceivedTenders": [
        {"type": "tenders", "total": 22},
        {"type": "t-esubm", "total": 0}
      ],
      "cpvCodes": ["72000000"],
      "limitedDataFlag": null,
      "doffinClassicUrl": null,
      "lots": [
        {
          "heading": "...",
          "description": "...",
          "winner": [
            {
              "id": "b88b5c2e...",
              "organizationId": "997711017",
              "name": "KS-Konsulent as"
            }
          ]
        }
      ]
    }
  ]
}
```

JSON responses contain **unescaped control characters** (newlines, tabs) inside `description` and `heading` fields. Standard `json.loads()` fails on these. Use `json.loads(text, strict=False)`.

Buyer `organizationId` formatting is **inconsistent**: some have spaces (`"999 601 391"`), some don't (`"938801363"`). Winner `organizationId` similarly. The download XML uses the organization pool format which also varies.

## GET /v2/download/{doffinId}

Returns the full eForms UBL 2.3 XML. Content-Type header says `application/octet-stream` but the body is valid UTF-8 XML.

All 151K notices — including pre-2023 legacy — are served as retroconverted eForms with the full organization pool structure.

Returns HTTP 404 for nonexistent IDs.

Typical response sizes: 10–50 KB for simple notices, up to 150 KB for multi-lot multi-tenderer awards.

## Rate limiting

~30 requests trigger HTTP 429 with response:

```json
{"statusCode": 429, "message": "Rate limit is exceeded. Try again in 10 seconds."}
```

Headers on 429: `retry-after: 10`. No rate limit headers on 200 responses. No `x-ratelimit-remaining` or `x-ratelimit-reset` headers.

Sustainable throughput: 2–3 requests/second with 12 s backoff on 429. Observed during production backfill: ~2,500 notices/hour sustained, with 429 backoffs every ~30 requests. Full 151K corpus requires ~60 hours (multiple 12-hour Cloud Run Job executions with GCS checkpoint resume).

## Corpus

151,288 total notices as of 2026-03-25. Data goes back to 2017-01-02.

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

Average ~50–75 notices/day. Monthly peaks ~1,500.

## Divergences from documented OpenAPI spec

1. `numHitsPerPage` default is 15, not 20.
2. `numHitsPerPage` accepts values above the documented max of 100.
3. `page` is 1-indexed, not 0-indexed.
4. `page=0` returns an error or empty response rather than the first page.
5. Buyer/winner `organizationId` formatting inconsistent (spaces in some orgnr).
6. `status` is null for RESULT-type notices.
7. Search results contain duplicate IDs across pages for multi-type queries.
8. `issue_date` carries `+02:00` timezone suffix on all observed notices, never `Z` (UTC).

## `cbc:CompanyID` format variations in XML

The eForms organization pool's `cbc:CompanyID` field contains Norwegian orgnr in inconsistent formats. These are source data issues in the retroconversion and in contracting authority data entry, not API bugs. `clean_orgnr()` normalizes all of these:

| Pattern | Example | Frequency | Handling |
|---|---|---|---|
| Plain 9-digit | `938801363` | ~85% | Passed through |
| Spaces | `999 601 391` | ~5% | Whitespace stripped |
| `NO` prefix | `NO981604032` | Rare | Prefix removed |
| `NO` + `MVA` suffix | `NO952522035MVA` | ~160 instances | Both removed |
| `MVA` suffix only | `932151286MVA` | ~16 instances | Suffix removed |
| `Org.nr.` prefix | `Org.nr.978693024` | ~20 instances | Prefix removed |
| Zero-width Unicode | `983974791<U+200B>` | Rare | ZWS/ZWNJ/BOM stripped |
| Foreign identifiers | `SE556289739601`, `2348368-2` | ~100 instances | Returns `None` |
| Garbage | `xxxxxxx`, `N/A`, `DIHVAIKS` | ~50 instances | Returns `None` |

Missing `cbc:CompanyID` entirely (element absent from XML): ~41% of winners and ~64% of tenderers in retroconverted 2017 notices. 0% in post-2023 eForms-native notices.
