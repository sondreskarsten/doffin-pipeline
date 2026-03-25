import subprocess
import json
import time
import urllib.parse
import os
from datetime import date, timedelta

USE_CURL = os.environ.get("USE_CURL", "0") == "1"


class DoffinClient:

    BASE = "https://api.doffin.no/public"
    MAX_PER_PAGE = 100
    MAX_PAGE = 10

    def __init__(self, api_key, delay=0.5, max_retries=3):
        self.api_key = api_key
        self.delay = delay
        self.max_retries = max_retries
        self._request_count = 0
        self._use_curl = USE_CURL
        if not self._use_curl:
            import requests as _req
            self._session = _req.Session()
            self._session.headers["Ocp-Apim-Subscription-Key"] = api_key

    def _get_curl(self, url):
        result = subprocess.run(
            ["curl", "-s", "-w", "\n%{http_code}", "-H",
             f"Ocp-Apim-Subscription-Key: {self.api_key}", url],
            capture_output=True, text=True, timeout=60,
        )
        parts = result.stdout.rsplit("\n", 1)
        content = parts[0] if len(parts) > 1 else result.stdout
        code = int(parts[1]) if len(parts) > 1 and parts[1].strip().isdigit() else 0
        return code, content.encode("utf-8")

    def _get_requests(self, url):
        r = self._session.get(url, timeout=60)
        return r.status_code, r.content

    def _get(self, url):
        get_fn = self._get_curl if self._use_curl else self._get_requests
        for attempt in range(self.max_retries):
            self._request_count += 1
            code, content = get_fn(url)
            if code == 429:
                print(f"  429 at request #{self._request_count}, sleeping 12s", flush=True)
                time.sleep(12)
                continue
            time.sleep(self.delay)
            return code, content
        raise RuntimeError(f"Failed after {self.max_retries} retries: {url}")

    def _build_url(self, path, params=None):
        url = f"{self.BASE}{path}"
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"
        return url

    def search(self, params):
        url = self._build_url("/v2/search", params)
        code, body = self._get(url)
        if code != 200:
            raise RuntimeError(f"Search HTTP {code}: {body[:200]}")
        return json.loads(body, strict=False)

    def search_date_range(self, date_from, date_to, notice_type=None):
        results = []
        params = {
            "numHitsPerPage": self.MAX_PER_PAGE,
            "sortBy": "PUBLICATION_DATE_ASC",
            "issueDateFrom": date_from,
            "issueDateTo": date_to,
        }
        if notice_type:
            params["type"] = notice_type

        for page in range(1, self.MAX_PAGE + 1):
            params["page"] = page
            d = self.search(params)
            hits = d.get("hits", [])
            results.extend(hits)
            if len(hits) < self.MAX_PER_PAGE:
                break
        return results, d.get("numHitsTotal", 0)

    def download_xml(self, doffin_id):
        url = self._build_url(f"/v2/download/{doffin_id}")
        code, body = self._get(url)
        if code == 404:
            return None
        if code != 200:
            raise RuntimeError(f"Download HTTP {code}")
        return body

    def search_all_in_range(self, date_from, date_to):
        hits, total = self.search_date_range(date_from, date_to)
        if total <= 1000:
            return hits

        print(f"  {total} notices in {date_from}→{date_to}, splitting...", flush=True)
        d_from = date.fromisoformat(date_from)
        d_to = date.fromisoformat(date_to)
        mid = d_from + (d_to - d_from) // 2

        left = self.search_all_in_range(date_from, mid.isoformat())
        right = self.search_all_in_range((mid + timedelta(days=1)).isoformat(), date_to)

        seen = set()
        merged = []
        for h in left + right:
            if h["id"] not in seen:
                seen.add(h["id"])
                merged.append(h)
        return merged
