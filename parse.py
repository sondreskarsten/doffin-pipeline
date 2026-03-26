"""eForms UBL 2.3 XML parser for Doffin procurement notices.

Extracts organizations, roles, and bid values from the three XML document types
served by the Doffin Public API:

* ``ContractNotice`` — competitions (buyer only, no result data)
* ``ContractAwardNotice`` — results, cancellations, contract changes
  (buyer + winners + tenderers + subcontractors)
* ``PriorInformationNotice`` — planning/advisory (buyer only)

eForms uses a normalized architecture: all organizations sit in a single
``efac:Organizations`` pool, each assigned an internal reference ID
(``ORG-0001``).  Roles are determined by tracing reference chains through
``ContractingParty``, ``TenderingParty``, ``LotTender``, and
``SettledContract`` elements.  This module resolves those chains into flat
``(role, orgnr, name, value)`` tuples.

eForms namespace prefixes handled: ``cac``, ``cbc``, ``efac``, ``efbc``,
``efext``, ``ext``.

Typical usage::

    from parse import parse_notice, content_hash

    xml_bytes = client.download_xml("2026-105663")
    notice = parse_notice(xml_bytes)
    for party in notice["parties"]:
        print(party["role"], party["orgnr"], party["name"])
"""

from lxml import etree
import hashlib
import re

NS = {
    "cac": "urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2",
    "cbc": "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2",
    "efac": "http://data.europa.eu/p27/eforms-ubl-extension-aggregate-components/1",
    "efbc": "http://data.europa.eu/p27/eforms-ubl-extension-basic-components/1",
    "efext": "http://data.europa.eu/p27/eforms-ubl-extensions/1",
    "ext": "urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2",
}

_ORGNR_CLEAN = re.compile(r"[\s\u200b\u200c\u200d\ufeff\u00a0]+")
_ORGNR_NO_PREFIX = re.compile(r"^NO(\d{9})(?:MVA)?$", re.IGNORECASE)
_ORGNR_MVA_SUFFIX = re.compile(r"^(\d{9})[Mm][Vv][Aa]$")
_ORGNR_ORG_NR_PREFIX = re.compile(r"^[Oo]rg\.?nr\.?(\d{9})$")
_ORGNR_VALID = re.compile(r"^\d{9}$")


def clean_orgnr(raw):
    """Normalize a Norwegian organization number from eForms XML.

    The ``cbc:CompanyID`` field in eForms XML contains inconsistent formats:
    spaces (``"999 601 391"``), NO-prefix VAT numbers (``"NO952522035MVA"``),
    MVA suffix (``"932151286MVA"``), ``Org.nr.`` prefix
    (``"Org.nr.978693024"``), and foreign identifiers (Finnish
    ``"2348368-2"``, Swedish ``"SE556289739601"``).

    This function strips whitespace, then attempts to extract a 9-digit
    Norwegian orgnr.  If the result is not 9 digits, returns ``None``
    (foreign or garbage identifiers are not Norwegian orgnr).

    Args:
        raw: Organization number string from ``cbc:CompanyID``.
            ``None`` and empty strings return ``None``.

    Returns:
        9-digit Norwegian orgnr string, or ``None`` if the input was
        empty, not parseable, or not a Norwegian org number.

    Examples:
        >>> clean_orgnr("999 601 391")
        '999601391'
        >>> clean_orgnr("NO952522035MVA")
        '952522035'
        >>> clean_orgnr("932151286MVA")
        '932151286'
        >>> clean_orgnr("Org.nr.978693024")
        '978693024'
        >>> clean_orgnr("NO981604032")
        '981604032'
        >>> clean_orgnr("2348368-2") is None
        True
        >>> clean_orgnr("SE556289739601") is None
        True
        >>> clean_orgnr(None) is None
        True
    """
    if not raw:
        return None
    s = _ORGNR_CLEAN.sub("", raw.strip())
    if _ORGNR_VALID.match(s):
        return s
    m = _ORGNR_NO_PREFIX.match(s) or _ORGNR_MVA_SUFFIX.match(s) or _ORGNR_ORG_NR_PREFIX.match(s)
    if m:
        return m.group(1)
    return None


def content_hash(xml_bytes):
    """Compute a truncated SHA-256 hash of raw XML bytes.

    Used for change detection: if the hash of a downloaded notice matches the
    stored hash, the content is unchanged and parsing can be skipped.

    Args:
        xml_bytes: Raw XML as ``bytes``.  Passing a ``str`` raises
            ``TypeError``.

    Returns:
        First 16 hexadecimal characters of the SHA-256 digest.

    Examples:
        >>> content_hash(b"<xml>test</xml>")
        'fdb0e0e133e1f498'
        >>> content_hash(b"<xml>test</xml>") == content_hash(b"<xml>test</xml>")
        True
    """
    return hashlib.sha256(xml_bytes).hexdigest()[:16]


def parse_notice(xml_bytes):
    """Parse an eForms XML notice into a structured dict.

    Extracts the full organization pool, traces all role reference chains,
    and resolves each organization to a ``(role, orgnr, name, value)`` tuple.

    Args:
        xml_bytes: Raw eForms UBL 2.3 XML as ``bytes``, as returned by
            :meth:`~collect.DoffinClient.download_xml`.

    Returns:
        Dict with keys:

        * ``root_type`` (str): XML document type — ``"ContractNotice"``,
          ``"ContractAwardNotice"``, or ``"PriorInformationNotice"``.
        * ``doffin_id`` (str or None): Notice ID from ``cbc:ID``.
        * ``issue_date`` (str or None): ``cbc:IssueDate`` value.
        * ``issue_time`` (str or None): ``cbc:IssueTime`` value.
        * ``notice_type_code`` (str or None): ``cbc:NoticeTypeCode`` value.
        * ``customization_id`` (str or None): eForms SDK version string.
          Notices using Norwegian below-threshold extensions contain
          ``#extended#urn:fdc:anskaffelser.no:2023:eforms:national``.
        * ``procedure_id`` (str or None): ``cbc:ContractFolderID`` linking
          related notices in the same procurement procedure.
        * ``organizations`` (dict): Map of internal org ID (``ORG-xxxx``) to
          org detail dicts with keys ``org_id``, ``name``, ``orgnr``,
          ``size_code``, ``street``, ``city``, ``postal_code``, ``country``,
          ``nuts``, ``email``, ``website``.
        * ``buyer_org_ids`` (list[str]): Internal org IDs for buyers.
        * ``tendering_parties`` (dict): TenderingParty structures.
        * ``lot_tenders`` (dict): LotTender structures with bid values.
        * ``settled_contracts`` (dict): Winning tender references.
        * ``lot_results`` (dict): Lot-level result summaries.
        * ``parties`` (list[dict]): Resolved role assignments.  Each dict has
          keys ``role``, ``org_id``, ``orgnr``, ``name``, ``lot_id``,
          ``tender_value``, ``currency``, ``is_winner``, ``is_leader``.

    Note:
        The ``parties`` list is the primary output for downstream consumers.
        The intermediate structures (``tendering_parties``, ``lot_tenders``,
        etc.) are exposed for debugging and audit purposes.
    """
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
    """Extract text content from a single XML element matched by XPath.

    Args:
        el: lxml Element to search within.
        xpath: XPath expression using namespace prefixes from :data:`NS`.

    Returns:
        Stripped text content, or ``None`` if the element was not found or
        has no text.
    """
    node = el.find(xpath, NS)
    return node.text.strip() if node is not None and node.text else None


def _parse_organizations(root):
    """Extract the eForms organization pool.

    Parses all ``efac:Organizations/efac:Organization/efac:Company`` elements,
    extracting BT-500 (name), BT-501 (orgnr), BT-165 (size), and address
    fields.

    Args:
        root: lxml root Element of the eForms XML.

    Returns:
        Dict mapping internal org ID (``"ORG-0001"``) to org detail dict with
        keys: ``org_id``, ``name``, ``orgnr`` (cleaned via :func:`clean_orgnr`),
        ``size_code``, ``street``, ``city``, ``postal_code``, ``country``,
        ``nuts``, ``email``, ``website``.
    """
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
    """Extract internal org IDs referenced as contracting parties (buyers).

    Args:
        root: lxml root Element.

    Returns:
        List of internal org ID strings (e.g., ``["ORG-0001"]``).  Typically
        one buyer, but multi-buyer notices exist (e.g., joint municipal
        procurements).
    """
    refs = []
    for el in root.findall(".//cac:ContractingParty/cac:Party/cac:PartyIdentification/cbc:ID", NS):
        if el.text:
            refs.append(el.text.strip())
    return refs


def _parse_tendering_parties(root):
    """Extract TenderingParty structures from the NoticeResult section.

    Each TenderingParty groups one or more tenderers (which may form a
    consortium) and optional subcontractors.  Only present in result-type
    notices (``ContractAwardNotice``).

    Args:
        root: lxml root Element.

    Returns:
        Dict mapping TenderingParty ID (``"TPA-0001"``) to dict with keys:
        ``tp_id``, ``tenderers`` (list of ``{"org_id": str, "leader": bool}``),
        ``subcontractors`` (list of org ID strings).
    """
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
    """Extract LotTender structures linking tenders to tendering parties and values.

    Each LotTender represents one bid on one lot, referencing a TenderingParty
    and optionally including the bid value (BT-720, ``PayableAmount``).

    Args:
        root: lxml root Element.

    Returns:
        Dict mapping LotTender ID (``"TEN-0001"``) to dict with keys:
        ``lt_id``, ``tp_id`` (TenderingParty reference), ``value``
        (numeric string or ``None``), ``currency`` (``"NOK"`` or ``None``).
    """
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
    """Extract SettledContract structures identifying winning tenders.

    A SettledContract references the LotTender that won the contract.
    Only tenders referenced by a SettledContract are winners; all others
    are losing bidders.

    Args:
        root: lxml root Element.

    Returns:
        Dict mapping SettledContract ID (``"CON-0001"``) to dict with keys:
        ``sc_id``, ``winning_tender_id`` (LotTender reference).
    """
    scs = {}
    for sc_el in root.findall(".//efac:NoticeResult/efac:SettledContract", NS):
        sc_id = _text(sc_el, "cbc:ID")
        lt_ref = _text(sc_el, "efac:LotTender/cbc:ID")
        if sc_id:
            scs[sc_id] = {"sc_id": sc_id, "winning_tender_id": lt_ref}
    return scs


def _parse_lot_results(root):
    """Extract LotResult structures summarizing outcomes per lot.

    Each LotResult links to a SettledContract (winner) and lists all
    LotTender IDs that participated in that lot — both winners and losers.
    The ``result_code`` field indicates whether a winner was selected
    (``selec-w``) or the lot was cancelled.

    Args:
        root: lxml root Element.

    Returns:
        Dict mapping LotResult ID (``"RES-0001"``) to dict with keys:
        ``lr_id``, ``settled_contract_id``, ``all_tender_ids`` (list of
        LotTender IDs), ``result_code``, ``lot_id``.
    """
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
    """Resolve the eForms reference chain into flat party role assignments.

    Traces each reference chain to its terminal organization in the pool
    and determines whether each participant is a buyer, winner, losing
    tenderer, or subcontractor.

    The resolution logic:

    1. **Buyers** — organizations referenced by ``ContractingParty``.
    2. **Winners** — organizations whose LotTender is referenced by a
       SettledContract (i.e., they won the contract).
    3. **Tenderers** (losing bidders) — organizations with a LotTender
       that is NOT referenced by any SettledContract.
    4. **Subcontractors** — organizations listed under a TenderingParty's
       SubContractor elements.

    For consortium tenders, multiple tenderers appear under one
    TenderingParty, with ``is_leader=True`` on the group lead.

    Args:
        notice: Dict as returned by :func:`parse_notice` (must include
            ``organizations``, ``buyer_org_ids``, ``tendering_parties``,
            ``lot_tenders``, ``settled_contracts``, ``lot_results``).

    Returns:
        List of party dicts, each with keys: ``role`` (str), ``org_id``
        (str), ``orgnr`` (str or None), ``name`` (str or None), ``lot_id``
        (str or None), ``tender_value`` (str or None), ``currency``
        (str or None), ``is_winner`` (bool), ``is_leader`` (bool).
    """
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
