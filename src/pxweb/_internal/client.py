import time
from threading import Lock

from packaging.version import parse
from requests.exceptions import HTTPError, JSONDecodeError
from requests_cache import CachedSession, CacheSettings, Request


class ApiVersionError(Exception):
    """Raised if the API version is not >2.0.0"""


class Client:
    def __init__(
        self,
        url: str,
        timeout: int,
        disable_cache: bool,
        language: str | None = None,
    ):
        self.session: CachedSession = CachedSession(
            ttl=3600,
            allowable_methods=("GET", "POST"),
            backend="memory",
            settings=CacheSettings(disabled=disable_cache),
        )
        self.url: str = url
        self.timeout: int = timeout
        self.lock = Lock()

        # Setting up params used for every query
        self.params: dict = {"lang": language, "outputFormat": "json-stat2"}

        # Run the init without rate limiting since it's not set up yet
        configuration = self.call(endpoint="/config", enforce_rate_limit=False)

        if parse(configuration.get("apiVersion", "0.0.0")) < parse("2.0.0"):
            raise ApiVersionError(
                f"""The version of the API is {configuration.get("apiVersion")}. pxwebpy requires 2.0.0 or greater."""
            )

        self.max_data_cells: int = configuration.get("maxDataCells")
        self.max_calls: int = configuration.get("maxCallsPerTimeWindow")
        self.time_window: int = configuration.get("timeWindow")
        self.call_timestamps: list[float] = []

        # Now that we have the configuration set up, get the language if needed
        self.params["lang"] = language or configuration.get(
            "defaultLanguage", None
        )

    def call(
        self,
        endpoint: str,
        query: dict | None = None,
        params: dict | None = None,
        max_retries: int = 3,
        enforce_rate_limit: bool = True,
    ) -> dict:
        """Call the endpoint with optional query"""

        # Set up the request and prepare it for being sent
        request = Request(
            method="POST" if query else "GET",
            url=self.url + endpoint,
            json=query or None,
            params=self.params | params if params else self.params,
        ).prepare()

        # Handle cache settings
        if not self.session.settings.disabled:
            # Use the request as the cache key so we can look it up first
            cache_key = self.session.cache.create_key(request)

            # If there's a cache, go ahead without rate limiting
            if cache_key in self.session.cache.responses:
                return self.session.send(request).json()

        # Otherwise rate limit first
        if enforce_rate_limit:
            # Wait if needed
            self.rate_limit()

        # We do a set number of retries. This is because
        # the API can sometimes respond with 429 (too many requests) given
        # a heavy number of subqueries. The response contains a retry-after in the headers
        # so we basically back off and then go again
        for attempt in range(max_retries + 1):
            response = self.session.send(request)
            if response.ok:
                return response.json()

            # If we get this response, despite rate limiting, we basically
            # need to "back off" and then retry
            elif response.status_code == 429 and attempt < max_retries:
                retry_after = response.headers.get("Retry-After", "1")
                time.sleep(int(retry_after))
                # Then continue to do another attempt
                continue
            else:
                # Try to extract any response body for more meaningful errors
                try:
                    response_body = response.json()
                except JSONDecodeError:
                    response_body = {}
                raise HTTPError(
                    f"""Error {response.status_code}: {response.reason}\nType: {response_body.get("type")}\nTitle: {response_body.get("title")}\nStatus: {response_body.get("status")}\nDetail: {response_body.get("detail")}\nInstance: {response_body.get("instance")}"""
                )
        else:
            raise HTTPError("Reached max amount of retries.")

    def rate_limit(self) -> None:
        """Ensure we respect the rate limit of the API"""
        with self.lock:
            now = time.monotonic()
            # Drop old timstamps no longer in the window
            self.call_timestamps = [
                timestamp
                for timestamp in self.call_timestamps
                if now - timestamp < self.time_window
            ]
            # If we still have slots in the window, stop blocking and proceed
            if len(self.call_timestamps) < self.max_calls:
                self.call_timestamps.append(now)
                return None
            # Otherise there have been to many calls, so we need to sleep
            sleep_time = self.time_window - (now - self.call_timestamps[0])
            # This way we hold the lock so other threads queue up
            time.sleep(sleep_time)
