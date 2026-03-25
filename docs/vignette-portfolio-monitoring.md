# Portfolio monitoring across Norwegian public data sources

How to cross-reference procurement data from Doffin with the losore-pipeline and kunngjøring datasets to build a comprehensive view of corporate activity.

## Three data sources, one key

All three pipelines produce party-level data keyed by Norwegian organization number (orgnr, 9 digits). Joining across sources enables detection of patterns invisible in any single dataset.

| Pipeline | Source | Primary signal | Key fields |
|---|---|---|---|
| **doffin-pipeline** | Doffin Public API | Public contract wins, bid activity | `(doffin_id, orgnr, role)` |
| **losore-pipeline** | Løsøreregisteret | Movable property liens, collateral changes | `(orgnr, dokumentnummer)` |
| **kunngjøring** | w2.brreg.no | Fusjon, fisjon, konkurs, styre changes | `(orgnr, kid)` |

## What each source tells you

### Doffin — procurement participation

A company's `orgnr` appears in `parties.parquet` with role `winner` when it wins a public contract, `tenderer` when it bids but loses, `buyer` when it issues a procurement, and `subcontractor` when named as a subcontractor.

Signals available per notice:

- **Contract win with value**: `role=winner`, `tender_value` populated. Company revenue from public sector.
- **Losing bid with value**: `role=tenderer`, `tender_value` populated. Company is actively competing. Bid amounts reveal pricing strategy.
- **Subcontractor relationship**: `role=subcontractor`. Supply chain dependency.
- **Frequency**: Multiple appearances across dates indicate systematic public-sector engagement.
- **Buyer dependency**: Repeated wins from the same buyer orgnr indicate key customer concentration.

### Løsøre — collateral and liens

A company's `orgnr` appears in `snapshots.parquet` when a rettsstiftelse (security interest) is registered. Change types in `changelog/YYYY-MM-DD.parquet` indicate new liens, modifications, and discharges.

Signals: new debt collateralization, floating charges, factoring arrangements, refinancing events.

### Kunngjøring — corporate events

A company's `orgnr` appears as `party_orgnr` in the fisjon/fusjon CSV and inventory JSONL. The `listing_type` field distinguishes event categories.

Signals: merger (fusjon), demerger (fisjon), bankruptcy (konkursåpning), forced dissolution (tvangsoppløsning), capital changes, board changes.

## Join patterns

### R: Load and join the three Parquet datasets

```r
library(arrow)
library(dplyr)

doffin_parties <- read_parquet("doffin/state/parties.parquet")
losore_snapshots <- read_parquet("losore/state/snapshots.parquet")
kunngjoring_inv <- read_parquet("kunngjoring/2026-03-22/all_kids_inventory.parquet")

portfolio_orgnr <- c("997711017", "938801363", "995781778")

doffin_parties |>
  filter(orgnr %in% portfolio_orgnr) |>
  count(orgnr, role, name = "n_notices")

losore_snapshots |>
  filter(orgnr %in% portfolio_orgnr, status == "active") |>
  count(orgnr, name = "active_liens")

kunngjoring_inv |>
  filter(sokeverdi %in% portfolio_orgnr) |>
  count(sokeverdi, kunngjoringstype, name = "n_events")
```

### Python: Portfolio activity timeline

```python
import pyarrow.parquet as pq
from collections import defaultdict

parties = pq.read_table("doffin/state/parties.parquet").to_pydict()
portfolio = {"997711017", "938801363", "995781778"}

by_org = defaultdict(list)
for i in range(len(parties["orgnr"])):
    if parties["orgnr"][i] in portfolio:
        by_org[parties["orgnr"][i]].append({
            "doffin_id": parties["doffin_id"][i],
            "role": parties["role"][i],
            "value": parties["tender_value"][i],
        })

for orgnr, entries in by_org.items():
    wins = sum(1 for e in entries if e["role"] == "winner")
    bids = sum(1 for e in entries if e["role"] == "tenderer")
    print(f"{orgnr}: {wins} wins, {bids} losing bids")
```

## Signal combinations

| Doffin signal | + Løsøre signal | Interpretation |
|---|---|---|
| Winning large contracts | New floating charge | Normal: securing working capital for new contract |
| Winning large contracts | Lien discharged | Positive: paying down debt with contract revenue |
| No recent wins | New liens accumulating | Risk: debt without revenue pipeline |
| Frequent tenderer, no wins | — | Company actively bidding but not competitive |
| Winner + subcontractor | Subcontractor has new liens | Supply chain risk |

| Doffin signal | + Kunngjøring signal | Interpretation |
|---|---|---|
| Winner | Fusjonsbeslutning | Acquiring entity absorbing capacity for contract delivery |
| Winner | Fisjonsbeslutning | Restructuring; verify which entity retains the contract |
| Buyer | Konkursåpning (on winner) | Contract counterparty risk |
| — | Tvangsoppløsning | Company may have unfulfilled contract obligations |

## Backfill considerations

| Dataset | Volume | Backfill time | Update frequency |
|---|---|---|---|
| Doffin parties | ~300K rows from 151K notices | ~14 h (rate limited) | Daily at 06:00 |
| Løsøre snapshots | ~310K dokumentnummer | 5 min from existing JSONL | Daily at 02:00 |
| Kunngjøring inventory | ~245K kids | Completed (one-time scrape) | Weekly or on-demand |

The doffin-pipeline backfill is the bottleneck. The API rate limit (~30 req/10 s) means the full 2017→present corpus requires ~14 hours. After initial backfill, daily runs take 30–60 seconds.
