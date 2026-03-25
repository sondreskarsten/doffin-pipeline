from lxml import etree
import hashlib
import json
import re

NS = {
    "cac": "urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2",
    "cbc": "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2",
    "efac": "http://data.europa.eu/p27/eforms-ubl-extension-aggregate-components/1",
    "efbc": "http://data.europa.eu/p27/eforms-ubl-extension-basic-components/1",
    "efext": "http://data.europa.eu/p27/eforms-ubl-extensions/1",
    "ext": "urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2",
}

_ORGNR_CLEAN = re.compile(r"\s+")


def clean_orgnr(raw):
    if not raw:
        return None
    return _ORGNR_CLEAN.sub("", raw.strip())


def content_hash(xml_bytes):
    return hashlib.sha256(xml_bytes).hexdigest()[:16]


def parse_notice(xml_bytes):
    root = etree.fromstring(xml_bytes)
    root_tag = etree.QName(root.tag).localname

    notice = {
        "root_type": root_tag,
        "doffin_id": _text(root, "cbc:ID"),
        "issue_date": _text(root, "cbc:IssueDate"),
        "issue_time": _text(root, "cbc:IssueTime"),
        "notice_type_code": _text(root, "cbc:NoticeTypeCode"),
        "customization_id": _text(root, "cbc:CustomizationID"),
        "procedure_id": _text(root, "cbc:ContractFolderID"),
        "organizations": _parse_organizations(root),
        "buyer_org_ids": _parse_buyer_refs(root),
        "tendering_parties": _parse_tendering_parties(root),
        "lot_tenders": _parse_lot_tenders(root),
        "settled_contracts": _parse_settled_contracts(root),
        "lot_results": _parse_lot_results(root),
    }

    notice["parties"] = _resolve_roles(notice)
    return notice


def _text(el, xpath):
    node = el.find(xpath, NS)
    return node.text.strip() if node is not None and node.text else None


def _parse_organizations(root):
    orgs = {}
    for org_el in root.findall(".//efac:Organizations/efac:Organization", NS):
        company = org_el.find("efac:Company", NS)
        if company is None:
            continue
        org_id = _text(company, "cac:PartyIdentification/cbc:ID")
        if not org_id:
            continue
        orgs[org_id] = {
            "org_id": org_id,
            "name": _text(company, "cac:PartyName/cbc:Name"),
            "orgnr": clean_orgnr(_text(company, "cac:PartyLegalEntity/cbc:CompanyID")),
            "size_code": _text(company, "efbc:CompanySizeCode"),
            "street": _text(company, "cac:PostalAddress/cbc:StreetName"),
            "city": _text(company, "cac:PostalAddress/cbc:CityName"),
            "postal_code": _text(company, "cac:PostalAddress/cbc:PostalZone"),
            "country": _text(company, "cac:PostalAddress/cac:Country/cbc:IdentificationCode"),
            "nuts": _text(company, "cac:PostalAddress/cbc:CountrySubentityCode"),
            "email": _text(company, "cac:Contact/cbc:ElectronicMail"),
            "website": _text(company, "cbc:WebsiteURI"),
        }
    return orgs


def _parse_buyer_refs(root):
    refs = []
    for el in root.findall(".//cac:ContractingParty/cac:Party/cac:PartyIdentification/cbc:ID", NS):
        if el.text:
            refs.append(el.text.strip())
    return refs


def _parse_tendering_parties(root):
    tps = {}
    for tp_el in root.findall(".//efac:NoticeResult/efac:TenderingParty", NS):
        tp_id = _text(tp_el, "cbc:ID")
        if not tp_id:
            continue
        tenderers = []
        for t in tp_el.findall("efac:Tenderer", NS):
            t_id = _text(t, "cbc:ID")
            leader = _text(t, "efbc:GroupLeadIndicator")
            if t_id:
                tenderers.append({"org_id": t_id, "leader": leader == "true"})
        subcontractors = []
        for sc in tp_el.findall("efac:SubContractor", NS):
            sc_id = _text(sc, "cbc:ID")
            if sc_id:
                subcontractors.append(sc_id)
        tps[tp_id] = {
            "tp_id": tp_id,
            "tenderers": tenderers,
            "subcontractors": subcontractors,
        }
    return tps


def _parse_lot_tenders(root):
    lts = {}
    for lt_el in root.findall(".//efac:NoticeResult/efac:LotTender", NS):
        lt_id = _text(lt_el, "cbc:ID")
        if not lt_id:
            continue
        lts[lt_id] = {
            "lt_id": lt_id,
            "tp_id": _text(lt_el, "efac:TenderingParty/cbc:ID"),
            "value": _text(lt_el, "cac:LegalMonetaryTotal/cbc:PayableAmount"),
            "currency": None,
        }
        amt_el = lt_el.find("cac:LegalMonetaryTotal/cbc:PayableAmount", NS)
        if amt_el is not None:
            lts[lt_id]["currency"] = amt_el.get("currencyID")
    return lts


def _parse_settled_contracts(root):
    scs = {}
    for sc_el in root.findall(".//efac:NoticeResult/efac:SettledContract", NS):
        sc_id = _text(sc_el, "cbc:ID")
        lt_ref = _text(sc_el, "efac:LotTender/cbc:ID")
        if sc_id:
            scs[sc_id] = {"sc_id": sc_id, "winning_tender_id": lt_ref}
    return scs


def _parse_lot_results(root):
    lrs = {}
    for lr_el in root.findall(".//efac:NoticeResult/efac:LotResult", NS):
        lr_id = _text(lr_el, "cbc:ID")
        if not lr_id:
            continue
        sc_ref = _text(lr_el, "efac:SettledContract/cbc:ID")
        all_tenders = [t.text.strip() for t in lr_el.findall("efac:LotTender/cbc:ID", NS) if t.text]
        result_code = _text(lr_el, "cbc:TenderResultCode")
        lot_ref = _text(lr_el, "efac:TenderLot/cbc:ID")
        lrs[lr_id] = {
            "lr_id": lr_id,
            "settled_contract_id": sc_ref,
            "all_tender_ids": all_tenders,
            "result_code": result_code,
            "lot_id": lot_ref,
        }
    return lrs


def _resolve_roles(notice):
    orgs = notice["organizations"]
    tps = notice["tendering_parties"]
    lts = notice["lot_tenders"]
    scs = notice["settled_contracts"]
    lrs = notice["lot_results"]

    winning_tender_ids = {sc["winning_tender_id"] for sc in scs.values() if sc["winning_tender_id"]}

    parties = []

    for org_id in notice["buyer_org_ids"]:
        org = orgs.get(org_id, {})
        parties.append({
            "role": "buyer",
            "org_id": org_id,
            "orgnr": org.get("orgnr"),
            "name": org.get("name"),
            "lot_id": None,
            "tender_value": None,
            "currency": None,
            "is_winner": False,
            "is_leader": False,
        })

    for lt_id, lt in lts.items():
        tp = tps.get(lt.get("tp_id"), {})
        is_winner = lt_id in winning_tender_ids

        lot_id = None
        for lr in lrs.values():
            if lt_id in lr.get("all_tender_ids", []):
                lot_id = lr.get("lot_id")
                break

        for tenderer in tp.get("tenderers", []):
            org = orgs.get(tenderer["org_id"], {})
            parties.append({
                "role": "winner" if is_winner else "tenderer",
                "org_id": tenderer["org_id"],
                "orgnr": org.get("orgnr"),
                "name": org.get("name"),
                "lot_id": lot_id,
                "tender_value": lt.get("value"),
                "currency": lt.get("currency"),
                "is_winner": is_winner,
                "is_leader": tenderer.get("leader", False),
            })

        for sc_org_id in tp.get("subcontractors", []):
            org = orgs.get(sc_org_id, {})
            parties.append({
                "role": "subcontractor",
                "org_id": sc_org_id,
                "orgnr": org.get("orgnr"),
                "name": org.get("name"),
                "lot_id": lot_id,
                "tender_value": None,
                "currency": None,
                "is_winner": False,
                "is_leader": False,
            })

    return parties
