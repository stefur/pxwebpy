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

        # Run the init without rate limiting since it's not set up yet
        configuration = self.call(endpoint="/config", rate_limit=False)

        self.language = language or configuration.get("defaultLanguage")
        self.max_data_cells = configuration.get("maxDataCells")
        self.rate_limit = ApiRateLimiter(
            configuration.get("maxCallsPerTimeWindow"), configuration.get("timeWindow")
        )

    def call(
        self, endpoint: str, query: dict | None = None, rate_limit: bool = True
    ) -> dict:
        """Call the API using the table URL and optional query"""
        if rate_limit:
            # Wait if needed
            self.rate_limit.wait()
        if query:
            response = self.session.post(
                self.url + endpoint, json=query, timeout=self.timeout
            )
        else:
            response = self.session.get(self.url + endpoint, timeout=self.timeout)

        # Count the API call towards the limit
        if rate_limit:
            self.rate_limit.count()

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


class ApiRateLimiter:
    """A helper to ensure that the number of calls follow the rate limit of the API"""

    def __init__(self, max_calls: int, time_window: int):
        self.max_calls = max_calls
        self.time_window = time_window
        self.call_count = 0
        self.window_start = time.time()

    def allow_call(self) -> bool:
        """Check whether another call to the API can be allowed given the rate limit"""
        now = time.time()

        if now - self.window_start > self.time_window:
            # Start a new window if enough time has passed
            self.window_start = now
            self.call_count = 0

        return self.call_count < self.max_calls

    def count(self) -> None:
        """Count the calls made"""
        self.call_count += 1

    def wait(self) -> None:
        """Wait until enough time has passed for another call to the API to be made"""
        while not self.allow_call():
            time_to_wait = self.time_window - (time.time() - self.window_start)
            time.sleep(max(0.1, time_to_wait))


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
