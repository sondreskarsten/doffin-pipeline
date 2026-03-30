"""Parquet-based state management following the losore CDC pattern.

Maintains three Parquet files on disk (or GCS via :func:`entrypoint.sync_to_gcs`):

* ``notices.parquet`` — one row per ``doffin_id``, with content hash for
  change detection, notice metadata, and org/party counts.
* ``parties.parquet`` — one row per ``(doffin_id, orgnr, role)`` combination,
  with bid values and winner/leader flags.
* ``changelog/YYYY-MM-DD.parquet`` — daily log of new and modified notices,
  overwritten at each checkpoint during a run (file grows as new entries
  accumulate).

Change detection uses SHA-256 of the raw XML bytes.  If the hash matches
the stored hash, :meth:`StateManager.ingest_notice` returns ``False``
(no change) and only updates the ``last_seen`` timestamp.  If the hash
differs, all party rows for that notice are replaced and a ``modified``
changelog entry is written.

All Parquet files use ZSTD compression.  At full corpus size (~151K notices,
~300K party rows), expected file sizes are ~2 MB notices, ~5 MB parties.
"""

import pyarrow as pa
import pyarrow.parquet as pq
import os
from datetime import datetime, timezone, date

NOTICES_SCHEMA = pa.schema([
    ("doffin_id", pa.string()),
    ("root_type", pa.string()),
    ("notice_type_code", pa.string()),
    ("issue_date", pa.string()),
    ("publication_date", pa.string()),
    ("procedure_id", pa.string()),
    ("customization_id", pa.string()),
    ("content_hash", pa.string()),
    ("xml_size", pa.int32()),
    ("n_organizations", pa.int32()),
    ("n_parties", pa.int32()),
    ("first_seen", pa.string()),
    ("last_seen", pa.string()),
])
"""PyArrow schema for the notices state file.

Columns:
    doffin_id: Primary key (e.g., ``"2026-105663"``).
    root_type: XML document type.
    notice_type_code: eForms subtype code.
    issue_date: From ``cbc:IssueDate`` in XML.
    publication_date: From search result ``publicationDate``.
    procedure_id: ``cbc:ContractFolderID`` linking related notices.
    customization_id: eForms SDK version + Norwegian extension flag.
    content_hash: SHA-256[:16] of raw XML bytes.
    xml_size: Raw XML byte count.
    n_organizations: Count of orgs in eForms pool.
    n_parties: Count of resolved role assignments.
    first_seen: UTC ISO timestamp of first ingestion.
    last_seen: UTC ISO timestamp of most recent ingestion.
"""

PARTIES_SCHEMA = pa.schema([
    ("doffin_id", pa.string()),
    ("role", pa.string()),
    ("orgnr", pa.string()),
    ("name", pa.string()),
    ("lot_id", pa.string()),
    ("tender_value", pa.string()),
    ("currency", pa.string()),
    ("is_winner", pa.bool_()),
    ("is_leader", pa.bool_()),
])
"""PyArrow schema for the parties state file.

Columns:
    doffin_id: Parent notice.
    role: One of ``buyer``, ``winner``, ``tenderer``, ``subcontractor``.
    orgnr: 9-digit Norwegian org number (spaces stripped).
    name: Organization name from BT-500.
    lot_id: Lot identifier (e.g., ``"LOT-0000"``), or ``None`` for buyers.
    tender_value: Bid amount as numeric string, or ``None``.
    currency: ISO currency code (usually ``"NOK"``), or ``None``.
    is_winner: ``True`` if this tender's LotTender is in a SettledContract.
    is_leader: ``True`` for group lead in consortium tenders.
"""

CHANGELOG_SCHEMA = pa.schema([
    ("doffin_id", pa.string()),
    ("change_type", pa.string()),
    ("publication_date", pa.string()),
    ("root_type", pa.string()),
    ("buyer_orgnr", pa.string()),
    ("buyer_name", pa.string()),
    ("n_parties", pa.int32()),
    ("detected_at", pa.string()),
    ("source", pa.string()),
])
"""PyArrow schema for the daily changelog.

Columns:
    doffin_id: Notice that changed.
    change_type: ``"new"`` or ``"modified"``.
    publication_date: From search result.
    root_type: XML document type.
    buyer_orgnr: Buyer orgnr for quick portfolio filtering.
    buyer_name: Buyer name.
    n_parties: Total party rows for this notice.
    detected_at: UTC ISO timestamp when change was found.
    source: ``"daily"``, ``"backfill"``, or ``"test"``.
"""


class StateManager:
    """Manages Parquet-based pipeline state on the local filesystem.

    Loads existing state from disk on initialization.  Accumulates changes
    in memory via :meth:`ingest_notice`.  Flushes to disk via :meth:`save`.

    Args:
        state_dir: Directory for state files.  Created if it doesn't exist.
            Expected layout after first run::

                state_dir/
                    notices.parquet
                    parties.parquet
                    changelog/
                        2026-03-25.parquet
                    raw/
                        2026-03-25/
                            2026-105663.xml

    Examples:
        >>> state = StateManager("/tmp/doffin-state")
        >>> state.notice_count()
        0
        >>> changed = state.ingest_notice("2026-105663", xml, parsed, hit)
        >>> changed
        True
        >>> state.save()
    """

    def __init__(self, state_dir):
        self.state_dir = state_dir
        self.notices_path = os.path.join(state_dir, "notices.parquet")
        self.parties_path = os.path.join(state_dir, "parties.parquet")
        self.changelog_dir = os.path.join(state_dir, "changelog")
        self.raw_dir = os.path.join(state_dir, "raw")
        os.makedirs(self.changelog_dir, exist_ok=True)
        os.makedirs(self.raw_dir, exist_ok=True)

        self._notices_index = {}
        self._notices_rows = {col: [] for col in NOTICES_SCHEMA.names}
        self._parties_rows = {col: [] for col in PARTIES_SCHEMA.names}
        self._changelog = []
        self._now = datetime.now(timezone.utc).isoformat()

        self._load()

    def _load(self):
        """Load existing state from Parquet files on disk.

        Populates :attr:`_notices_index` (dict keyed by ``doffin_id``) and
        :attr:`_parties_rows` (column-oriented dict).  Called once during
        ``__init__``.
        """
        if os.path.exists(self.notices_path):
            table = pq.read_table(self.notices_path)
            d = table.to_pydict()
            for i in range(len(d["doffin_id"])):
                self._notices_index[d["doffin_id"][i]] = {
                    col: d[col][i] for col in NOTICES_SCHEMA.names
                }
            print(f"  Loaded {len(self._notices_index):,} notices from state", flush=True)

        if os.path.exists(self.parties_path):
            table = pq.read_table(self.parties_path)
            self._parties_rows = table.to_pydict()
            n = len(self._parties_rows["doffin_id"])
            print(f"  Loaded {n:,} party rows from state", flush=True)

    def known_ids(self):
        """Return the set of all ingested doffin_ids.

        Returns:
            Set of doffin_id strings.  Used by the entrypoint to skip
            already-known notices during backfill.
        """
        return set(self._notices_index.keys())

    def notice_count(self):
        """Return the number of notices in state.

        Returns:
            Integer count.
        """
        return len(self._notices_index)

    def party_count(self):
        """Return the number of party rows in state.

        Returns:
            Integer count.
        """
        return len(self._parties_rows["doffin_id"])

    def ingest_notice(self, doffin_id, xml_bytes, parsed, search_hit, source="daily"):
        """Insert or update a notice in state.

        Change detection: computes SHA-256[:16] of ``xml_bytes`` and compares
        against the stored ``content_hash``.  If they match, only ``last_seen``
        is updated and the method returns ``False``.  Otherwise, the notice
        metadata is updated, all party rows for this ``doffin_id`` are replaced
        with the freshly parsed parties, and a changelog entry is written.

        Args:
            doffin_id: Notice identifier (e.g., ``"2026-105663"``).
            xml_bytes: Raw XML as ``bytes``.
            parsed: Dict as returned by :func:`~parse.parse_notice`.
            search_hit: Search result dict (used for ``publicationDate``).
                Pass ``None`` to fall back to ``parsed["issue_date"]``.
            source: Changelog source label.  One of ``"daily"``,
                ``"backfill"``, ``"test"``.

        Returns:
            ``True`` if the notice was new or modified, ``False`` if unchanged.
        """
        from parse import content_hash

        h = content_hash(xml_bytes)
        is_new = doffin_id not in self._notices_index

        if not is_new:
            old = self._notices_index[doffin_id]
            if old["content_hash"] == h:
                old["last_seen"] = self._now
                return False

        pub_date = search_hit.get("publicationDate") if search_hit else parsed.get("issue_date")
        buyer = next((p for p in parsed["parties"] if p["role"] == "buyer"), None)

        notice_row = {
            "doffin_id": doffin_id,
            "root_type": parsed["root_type"],
            "notice_type_code": parsed.get("notice_type_code"),
            "issue_date": parsed.get("issue_date"),
            "publication_date": pub_date,
            "procedure_id": parsed.get("procedure_id"),
            "customization_id": parsed.get("customization_id"),
            "content_hash": h,
            "xml_size": len(xml_bytes),
            "n_organizations": len(parsed["organizations"]),
            "n_parties": len(parsed["parties"]),
            "first_seen": self._notices_index.get(doffin_id, {}).get("first_seen", self._now),
            "last_seen": self._now,
        }
        self._notices_index[doffin_id] = notice_row

        self._remove_parties_for(doffin_id)
        for p in parsed["parties"]:
            for col in PARTIES_SCHEMA.names:
                self._parties_rows[col].append(p.get(col) if col != "doffin_id" else doffin_id)

        change_type = "new" if is_new else "modified"
        self._changelog.append({
            "doffin_id": doffin_id,
            "change_type": change_type,
            "publication_date": pub_date,
            "root_type": parsed["root_type"],
            "buyer_orgnr": buyer["orgnr"] if buyer else None,
            "buyer_name": buyer["name"] if buyer else None,
            "n_parties": len(parsed["parties"]),
            "detected_at": self._now,
            "source": source,
        })
        return True

    def _remove_parties_for(self, doffin_id):
        """Remove all party rows for a given notice ID.

        Used before re-inserting updated party rows on notice modification.
        Operates in-place on :attr:`_parties_rows`.

        Args:
            doffin_id: Notice identifier whose party rows should be removed.
        """
        if not self._parties_rows["doffin_id"]:
            return
        keep = [i for i, did in enumerate(self._parties_rows["doffin_id"]) if did != doffin_id]
        if len(keep) == len(self._parties_rows["doffin_id"]):
            return
        for col in PARTIES_SCHEMA.names:
            self._parties_rows[col] = [self._parties_rows[col][i] for i in keep]

    def save_raw(self, doffin_id, xml_bytes):
        """Write raw XML to the raw archive directory.

        Path: ``{state_dir}/raw/{today}/{doffin_id}.xml``.  Never overwrites —
        multiple downloads of the same notice on the same day produce the
        same file.

        Args:
            doffin_id: Notice identifier.
            xml_bytes: Raw XML as ``bytes``.

        Returns:
            Absolute path to the written file.
        """
        today = date.today().isoformat()
        day_dir = os.path.join(self.raw_dir, today)
        os.makedirs(day_dir, exist_ok=True)
        path = os.path.join(day_dir, f"{doffin_id}.xml")
        with open(path, "wb") as f:
            f.write(xml_bytes)
        return path

    def save(self):
        """Flush all in-memory state to Parquet files.

        Writes three files:

        * ``notices.parquet`` — full snapshot of all notices.
        * ``parties.parquet`` — full snapshot of all party rows.
        * ``changelog/{today}.parquet`` — new changelog entries since last save.

        All files use ZSTD compression.  Called by the entrypoint at
        checkpoint intervals and at the end of each run.
        """
        rows = list(self._notices_index.values())
        if rows:
            d = {col: [r[col] for r in rows] for col in NOTICES_SCHEMA.names}
            table = pa.table(d, schema=NOTICES_SCHEMA)
            pq.write_table(table, self.notices_path, compression="zstd")
            print(f"  Saved {len(rows):,} notices", flush=True)

        if self._parties_rows["doffin_id"]:
            table = pa.table(self._parties_rows, schema=PARTIES_SCHEMA)
            pq.write_table(table, self.parties_path, compression="zstd")
            print(f"  Saved {len(self._parties_rows['doffin_id']):,} party rows", flush=True)

        if self._changelog:
            today = date.today().isoformat()
            cl_path = os.path.join(self.changelog_dir, f"{today}.parquet")
            d = {col: [r[col] for r in self._changelog] for col in CHANGELOG_SCHEMA.names}
            table = pa.table(d, schema=CHANGELOG_SCHEMA)
            pq.write_table(table, cl_path, compression="zstd")
            print(f"  Saved {len(self._changelog)} changelog entries → {cl_path}", flush=True)

    def summary(self):
        """Return summary counts for logging.

        Returns:
            Dict with keys ``notices`` (int), ``parties`` (int),
            ``changelog_new`` (int), ``changelog_modified`` (int).
        """
        n_new = sum(1 for c in self._changelog if c["change_type"] == "new")
        n_mod = sum(1 for c in self._changelog if c["change_type"] == "modified")
        return {
            "notices": self.notice_count(),
            "parties": self.party_count(),
            "changelog_new": n_new,
            "changelog_modified": n_mod,
        }
    def max_publication_date(self):
        """Return the latest publication_date in state, or None if empty.

        Used by backfill to skip ahead past already-ingested date ranges,
        avoiding hundreds of wasted search requests scanning known chunks.

        Returns:
            Date string (``yyyy-mm-dd``) or ``None``.
        """
        dates = [r["publication_date"] for r in self._notices_index.values()
                 if r.get("publication_date")]
        return max(dates) if dates else None
