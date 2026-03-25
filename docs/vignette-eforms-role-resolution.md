# eForms role resolution

How `parse.py` traces the XML reference chain from organization pool to flat role assignments.

## The normalization problem

eForms does not store "this organization is the winner" as a field on the organization record. Instead, all organizations sit in a shared pool with internal IDs (`ORG-0001`, `ORG-0002`, ...). Roles are determined by which other elements reference those IDs, through a chain 3–5 levels deep.

## XML document types

The Doffin API serves three root types. All are valid eForms UBL 2.3, including pre-2023 legacy notices that were retroconverted.

| Root type | Doffin `type` filter values | Roles present |
|---|---|---|
| `ContractNotice` | `COMPETITION`, `DYNAMIC_PURCHASING_SCHEME`, `QUALIFICATION_SCHEME` | buyer |
| `ContractAwardNotice` | `RESULT`, `CANCELLED_OR_MISSING_CONCLUSION_OF_CONTRACT`, `CHANGE_OF_CONCLUSION_OF_CONTRACT` | buyer, winner, tenderer, subcontractor |
| `PriorInformationNotice` | `PLANNING`, `ADVISORY_NOTICE`, `PRE_ANNOUNCEMENT` | buyer |

Only `ContractAwardNotice` contains tenderer/winner data. The other two types have buyer(s) only.

## The reference chain

### Buyer (all notice types)

```
ContractingParty
  └── Party
        └── PartyIdentification
              └── ID → "ORG-0001"   ← look up in organization pool
```

Multiple buyers occur in joint municipal procurements. `_parse_buyer_refs()` returns a list.

### Winner (award notices only)

```
NoticeResult
  └── LotResult (RES-0001)
        └── SettledContract → "CON-0001"

NoticeResult
  └── SettledContract (CON-0001)
        └── LotTender → "TEN-0001"       ← this is the winning tender

NoticeResult
  └── LotTender (TEN-0001)
        └── TenderingParty → "TPA-0001"
        └── LegalMonetaryTotal/PayableAmount → "492700"

NoticeResult
  └── TenderingParty (TPA-0001)
        └── Tenderer → "ORG-0004"         ← this org is the winner
```

The key distinction: a LotTender is a winner if and only if it is referenced by a `SettledContract`.

### Losing tenderer (award notices only)

Same chain as winner, but the LotTender is NOT referenced by any SettledContract. The `LotResult` element lists ALL tenders for a lot via `OPT-320`, including both winners and losers.

```
NoticeResult
  └── LotResult (RES-0001)
        └── LotTender → ["TEN-0001", "TEN-0002", "TEN-0003"]   ← all tenders
        └── SettledContract → "CON-0001"                         ← only winning contract

SettledContract (CON-0001)
  └── LotTender → "TEN-0001"                                    ← only TEN-0001 won

TEN-0001 → winner
TEN-0002 → losing tenderer
TEN-0003 → losing tenderer
```

Whether losing tenderers appear depends on the contracting authority. DFØ statistics show ~34% of competitions publish result notices. Among those, listing all tenderers is voluntary.

### Subcontractor

```
NoticeResult
  └── TenderingParty (TPA-0001)
        └── SubContractor → "ORG-0006"    ← look up in organization pool
```

### Consortium (joint tender)

```
NoticeResult
  └── TenderingParty (TPA-0001)
        └── Tenderer → "ORG-0004"   (GroupLeadIndicator = true)
        └── Tenderer → "ORG-0005"   (GroupLeadIndicator = false)
```

All consortium members share the same LotTender (and thus the same `tender_value`). The `is_leader` flag distinguishes the lead entity.

## Implementation in `_resolve_roles()`

```python
winning_tender_ids = {sc["winning_tender_id"]
                      for sc in settled_contracts.values()
                      if sc["winning_tender_id"]}

for lt_id, lt in lot_tenders.items():
    tp = tendering_parties.get(lt["tp_id"], {})
    is_winner = lt_id in winning_tender_ids

    for tenderer in tp.get("tenderers", []):
        role = "winner" if is_winner else "tenderer"
        # emit (role, org_id, orgnr, name, tender_value)

    for subcontractor_org_id in tp.get("subcontractors", []):
        # emit ("subcontractor", org_id, orgnr, name, None)
```

## Concrete example: notice 2026-105663

Sarpsborg kommune, 2 lots, 22 tenders received, 11 unique organizations.

| Org | Name | Lot 1 role | Lot 1 value | Lot 2 role | Lot 2 value |
|---|---|---|---|---|---|
| ORG-0001 | Sarpsborg kommune | buyer | — | buyer | — |
| ORG-0008 | KS-Konsulent as | **winner** | 492,700 | **winner** | 492,700 |
| ORG-0012 | Stiftelsen Imtec | tenderer | 56,583 | tenderer | 565,835 |
| ORG-0009 | OsloMet | tenderer | 597,585 | tenderer | 0 |
| ORG-0010 | Valued AS | tenderer | 2,807,750 | tenderer | 0 |
| ORG-0003 | Flowit AS | tenderer | 660,099 | tenderer | 0 |
| ORG-0004 | Amesto People and Culture AS | tenderer | 1,300,735 | tenderer | 0 |
| ORG-0005 | Stiftelsen Handelshøyskolen BI | tenderer | 943,520 | tenderer | 0 |
| ORG-0006 | Agenda Kaupang AS | tenderer | 941,430 | tenderer | 0 |
| ORG-0007 | Lederkompasset AS | tenderer | 998,330 | tenderer | 0 |
| ORG-0013 | VITAL LEARNING AS | tenderer | 1,193,750 | tenderer | 1,193,750 |
| ORG-0011 | STIFTELSEN ADMINISTRATIVT FORSKNINGSFOND VED NHH | tenderer | 1,388,455 | tenderer | 0 |

The `0` values on Lot 2 indicate the tenderer bid on Lot 2 but the bid amount was not published (or was zero — the API does not distinguish).

## BT field reference

| BT field | XPath | Content |
|---|---|---|
| BT-500 | `cac:PartyName/cbc:Name` | Organization name |
| BT-501 | `cac:PartyLegalEntity/cbc:CompanyID` | Norwegian orgnr (9 digits) |
| BT-165 | `efbc:CompanySizeCode` | SME status (`sme`, `large`) — winners only |
| BT-720 | `cac:LegalMonetaryTotal/cbc:PayableAmount` | Bid value |
| OPT-200 | `cac:PartyIdentification/cbc:ID` | Internal reference (ORG-xxxx) |
| OPT-300 | Tenderer `cbc:ID` | Org reference within TenderingParty |
| OPT-310 | LotTender `efac:TenderingParty/cbc:ID` | Links tender → party |
| OPT-315 | LotResult `efac:SettledContract/cbc:ID` | Winning contract reference |
| OPT-320 | LotResult `efac:LotTender/cbc:ID` | ALL tenders for a lot |
| BT-3202 | SettledContract `efac:LotTender/cbc:ID` | Winning tender reference |
