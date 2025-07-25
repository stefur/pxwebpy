import time
from pathlib import Path
from tempfile import gettempdir
from typing import Optional

from requests.exceptions import HTTPError, JSONDecodeError
from requests_cache import CachedSession


class PxApi:
    def __init__(self, url: str, timeout: int, language: str | None = None):
        _tmp_dir = gettempdir()
        _cache = Path(_tmp_dir) / "pxwebpy_cache"
        self.session: CachedSession = CachedSession(cache_name=_cache, ttl=600)
        self.url: str = url
        self.timeout: int = timeout

        # Setting up params used for every query
        self.params: dict = {"lang": language, "outputFormat": "json-stat2"}

        # Run the init without rate limiting since it's not set up yet
        configuration = self.call(endpoint="/config", enforce_rate_limit=False)

        self.max_data_cells: int = configuration.get("maxDataCells")
        self.max_calls: int = configuration.get("maxCallsPerTimeWindow")
        self.time_window: int = configuration.get("timeWindow")
        self.call_timestamps: list[float] = []

        # Now that we have the configuration set up, get the language if needed
        self.params["lang"] = language or configuration.get("defaultLanguage", None)

    def call(
        self, endpoint: str, query: dict | None = None, enforce_rate_limit: bool = True
    ) -> dict:
        """Call the API using the table URL and optional query"""
        if enforce_rate_limit:
            # Wait if needed
            self.rate_limit()

        if query:
            response = self.session.post(
                self.url + endpoint,
                json=query,
                timeout=self.timeout,
                params=self.params,
            )
        else:
            response = self.session.get(
                self.url + endpoint,
                timeout=self.timeout,
                params=self.params,
            )

        if response.ok:
            return response.json()
        else:
            # Try to extract any response body for more meaningful errors
            try:
                response_body = response.json()
            except JSONDecodeError:
                response_body = {}
            raise HTTPError(
                f"""Error {response.status_code}: {response.reason}\nType: {response_body.get("type")}\nTitle: {response_body.get("title")}\nStatus: {response_body.get("status")}\nDetail: {response_body.get("detail")}\nInstance: {response_body.get("instance")}"""
            )

    def rate_limit(self) -> None:
        now = time.time()

        # Only keep timestamps within the time window
        self.remove_old_timestamps(now)

        # Check to see if we've hit the max number of calls
        if len(self.call_timestamps) >= self.max_calls:
            # Get the sleep time by comparing now and the oldest timestamp
            sleep_time = self.time_window - (now - self.call_timestamps[0])
            if sleep_time > 0:
                time.sleep(sleep_time)

        # Do another time check
        now = time.time()

        # And then another run to ensure keeping relevant timestamps
        self.remove_old_timestamps(now)

        # And lastly add the timestamp for this call
        self.call_timestamps.append(now)

    def remove_old_timestamps(self, now: float) -> None:
        """Discard timestamps older than the time window."""
        self.call_timestamps = [
            timestamp
            for timestamp in self.call_timestamps
            if now - timestamp < self.time_window
        ]


def call(
    session: CachedSession, timeout: int, url: str, query: Optional[dict] = None
) -> dict:
    """Call the API using the table URL and optional query"""

    if query:
        response = session.post(url, json=query, timeout=timeout)
    else:
        response = session.get(url, timeout=timeout)

    if response.ok:
        return response.json()
    else:
        raise HTTPError(f"Error {response.status_code}: {response.reason}")
