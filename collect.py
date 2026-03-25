"""Doffin Public API client with rate limit handling and recursive date splitting.

Wraps the two working endpoints at ``https://api.doffin.no/public``:

* ``GET /v2/search`` — paginated JSON notice summaries
* ``GET /v2/download/{doffinId}`` — raw eForms UBL 2.3 XML

Transport is configurable: ``requests`` library by default (for Cloud Run),
or subprocess ``curl`` when ``USE_CURL=1`` (for environments with proxy
restrictions that block Python's HTTP stack).

Rate limit: ~30 requests before HTTP 429.  The API returns
``retry-after: 10``.  This client sleeps 12 s on 429 and retries up to
``max_retries`` times.

Pagination is 1-indexed with a hard cap of 10 pages × 100 results = 1 000
results per query window.  For date ranges exceeding this,
:meth:`DoffinClient.search_all_in_range` recursively bisects the window.
"""

import subprocess
import json
import time
import urllib.parse
import os
from datetime import date, timedelta

USE_CURL = os.environ.get("USE_CURL", "0") == "1"


class DoffinClient:
    """HTTP client for the Doffin Public API.

    Args:
        api_key: Azure API Management subscription key
            (``Ocp-Apim-Subscription-Key`` header).
        delay: Seconds to sleep between successful requests.  Default ``0.5``.
        max_retries: Maximum number of retry attempts on HTTP 429.
            Default ``3``.

    Attributes:
        BASE: API base URL (``https://api.doffin.no/public``).
        MAX_PER_PAGE: Maximum results per search page (``100``).
        MAX_PAGE: Hard pagination ceiling (``10``).

    Examples:
        >>> client = DoffinClient("your-api-key")
        >>> hits, total = client.search_date_range("2026-03-24", "2026-03-24")
        >>> len(hits)
        66
        >>> xml = client.download_xml(hits[0]["id"])
        >>> xml[:5]
        b'<?xml'
    """

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
        """Execute a GET request via subprocess curl.

        Args:
            url: Fully-qualified URL including query string.

        Returns:
            Tuple of (http_status_code, response_body_bytes).
        """
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
        """Execute a GET request via the ``requests`` library.

        Args:
            url: Fully-qualified URL including query string.

        Returns:
            Tuple of (http_status_code, response_body_bytes).
        """
        r = self._session.get(url, timeout=60)
        return r.status_code, r.content

    def _get(self, url):
        """Execute a GET request with retry on HTTP 429.

        Dispatches to :meth:`_get_curl` or :meth:`_get_requests` depending on
        the ``USE_CURL`` environment variable.  On 429, sleeps 12 s and
        retries.

        Args:
            url: Fully-qualified URL including query string.

        Returns:
            Tuple of (http_status_code, response_body_bytes).

        Raises:
            RuntimeError: If all retries are exhausted.
        """
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
        """Construct a full API URL from path and optional query parameters.

        Args:
            path: API path relative to :attr:`BASE` (e.g., ``/v2/search``).
            params: Dict of query parameters.  Values are URL-encoded.

        Returns:
            Fully-qualified URL string.
        """
        url = f"{self.BASE}{path}"
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"
        return url

    def search(self, params):
        """Execute a single search request and return parsed JSON.

        The API returns JSON with unescaped control characters in description
        fields, so this method parses with ``strict=False``.

        Args:
            params: Dict of query parameters.  Supported keys:
                ``numHitsPerPage`` (int, default 15, max ~200),
                ``page`` (int, **1-indexed**, max 10),
                ``sortBy`` (str, one of ``PUBLICATION_DATE_ASC``,
                ``PUBLICATION_DATE_DESC``, ``DEADLINE``, ``RELEVANCE``,
                ``ESTIMATED_VALUE_ASC``, ``ESTIMATED_VALUE_DESC``),
                ``type`` (str, e.g. ``COMPETITION``, ``RESULT``),
                ``status`` (str, e.g. ``ACTIVE``, ``EXPIRED``),
                ``issueDateFrom`` / ``issueDateTo`` (str, ``yyyy-mm-dd``),
                ``searchString`` (str), ``cpvCode`` (str, 8-digit),
                ``location`` (str, NUTS code e.g. ``NO081``),
                ``estimatedValueFrom`` / ``estimatedValueTo`` (numeric).

        Returns:
            Parsed JSON dict with keys ``numHitsTotal`` (int),
            ``numHitsAccessible`` (int, capped at 1000), and ``hits`` (list of
            notice summary dicts).  Each hit contains ``id``, ``buyer``,
            ``heading``, ``type``, ``allTypes``, ``status``,
            ``publicationDate``, ``lots``, etc.

        Raises:
            RuntimeError: On non-200 HTTP response.
        """
        url = self._build_url("/v2/search", params)
        code, body = self._get(url)
        if code != 200:
            raise RuntimeError(f"Search HTTP {code}: {body[:200]}")
        return json.loads(body, strict=False)

    def search_date_range(self, date_from, date_to, notice_type=None):
        """Paginate through all results in a date range, up to 1 000.

        Issues sequential page requests (pages 1–10, 100 results each).
        Stops early when a page returns fewer than ``MAX_PER_PAGE`` results.

        Args:
            date_from: Start date as ``yyyy-mm-dd`` string.
            date_to: End date as ``yyyy-mm-dd`` string.
            notice_type: Optional notice type filter (e.g.,
                ``"ANNOUNCEMENT_OF_COMPETITION"``).

        Returns:
            Tuple of (hits, numHitsTotal) where *hits* is a list of notice
            summary dicts and *numHitsTotal* is the API's reported total
            (may exceed ``len(hits)`` if total > 1 000).

        Note:
            A single day averages ~50–66 notices.  Monthly windows can reach
            ~1 500.  If ``numHitsTotal > 1 000``, use
            :meth:`search_all_in_range` instead.
        """
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
        """Download the full eForms UBL 2.3 XML for a notice.

        The API returns ``Content-Type: application/octet-stream`` but the body
        is valid UTF-8 XML.  All 151K notices — including pre-2023 legacy —
        are served as retroconverted eForms.

        Args:
            doffin_id: Notice identifier (e.g., ``"2026-105663"``).

        Returns:
            Raw XML as ``bytes``, or ``None`` if the notice was not found
            (HTTP 404).

        Raises:
            RuntimeError: On non-200/non-404 HTTP response.
        """
        url = self._build_url(f"/v2/download/{doffin_id}")
        code, body = self._get(url)
        if code == 404:
            return None
        if code != 200:
            raise RuntimeError(f"Download HTTP {code}")
        return body

    def search_all_in_range(self, date_from, date_to):
        """Retrieve all notices in a date range, recursively splitting if > 1 000.

        First attempts a normal :meth:`search_date_range`.  If
        ``numHitsTotal > 1 000`` (the pagination cap), bisects the date range
        and recurses on each half, deduplicating by notice ID.

        Args:
            date_from: Start date as ``yyyy-mm-dd`` string.
            date_to: End date as ``yyyy-mm-dd`` string.

        Returns:
            Deduplicated list of notice summary dicts.  May be slightly fewer
            than ``numHitsTotal`` because the API's total count can include
            cross-boundary duplicates.

        Note:
            For a single day (~66 notices), no splitting occurs.  For a full
            month (~1 500 notices), one split into two ~750-notice halves
            suffices.  The deepest recursion observed is 2 levels (quarterly
            windows of ~4 000 notices).
        """
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
