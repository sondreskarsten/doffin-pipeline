import pyarrow as pa
import pyarrow.parquet as pq
import json
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


class StateManager:

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
        return set(self._notices_index.keys())

    def notice_count(self):
        return len(self._notices_index)

    def party_count(self):
        return len(self._parties_rows["doffin_id"])

    def ingest_notice(self, doffin_id, xml_bytes, parsed, search_hit, source="daily"):
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
        if not self._parties_rows["doffin_id"]:
            return
        keep = [i for i, did in enumerate(self._parties_rows["doffin_id"]) if did != doffin_id]
        if len(keep) == len(self._parties_rows["doffin_id"]):
            return
        for col in PARTIES_SCHEMA.names:
            self._parties_rows[col] = [self._parties_rows[col][i] for i in keep]

    def save_raw(self, doffin_id, xml_bytes):
        today = date.today().isoformat()
        day_dir = os.path.join(self.raw_dir, today)
        os.makedirs(day_dir, exist_ok=True)
        path = os.path.join(day_dir, f"{doffin_id}.xml")
        with open(path, "wb") as f:
            f.write(xml_bytes)
        return path

    def save(self):
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
        n_new = sum(1 for c in self._changelog if c["change_type"] == "new")
        n_mod = sum(1 for c in self._changelog if c["change_type"] == "modified")
        return {
            "notices": self.notice_count(),
            "parties": self.party_count(),
            "changelog_new": n_new,
            "changelog_modified": n_mod,
        }
