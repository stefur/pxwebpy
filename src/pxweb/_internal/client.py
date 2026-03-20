import time
from logging import getLogger
from threading import Lock

from packaging.version import parse
from requests.exceptions import HTTPError, JSONDecodeError
from requests_cache import CachedSession, CacheSettings, Request

logger = getLogger(__name__)


class ApiVersionError(Exception):
    """Raised if the API version is not >2.0.0"""


class ApiConfigurationError(Exception):
    """Required configuration is missing"""


class Client:
    """Used to communicate with the API"""

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
        self.params: dict = {"lang": language}

        logger.debug("Getting the API configuration")
        # Run the init without rate limiting since it's not set up yet
        self.configuration = self.call(
            endpoint="/config", enforce_rate_limit=False
        )

        if parse(self.configuration.get("apiVersion", "0.0.0")) < parse(
            "2.0.0"
        ):
            raise ApiVersionError(
                f"""The version of the API is {self.configuration.get("apiVersion")}. pxwebpy requires 2.0.0 or greater."""
            )

        for key in ("maxDataCells", "maxCallsPerTimeWindow", "timeWindow"):
            if self.configuration.get(key) is None:
                raise ApiConfigurationError(
                    f"The required key {key} is missing in the API configuration."
                )

        logger.debug(
            f"""Query size is limited to {self.configuration["maxDataCells"]} number of data cells"""
        )

        if self.configuration["maxCallsPerTimeWindow"] > 0:
            logger.debug(
                "Rate limiting set to a maximum of %s calls per time window of %s seconds",
                self.configuration["maxCallsPerTimeWindow"],
                self.configuration["timeWindow"],
            )
        else:
            logger.debug("No rate limit is configured")

        # Now that we have the configuration set up, get the language if needed
        self.params["lang"] = language or self.configuration.get(
            "defaultLanguage", None
        )

        self.call_timestamps: list[float] = []

    def call(
        self,
        endpoint: str,
        query: dict | None = None,
        params: dict | None = None,
        max_retries: int = 3,
        enforce_rate_limit: bool = True,
    ) -> dict:
        """Call the endpoint with optional query"""

        # If we're asking for table data, set parameters to get json-stat2
        if endpoint.endswith("/data"):
            params = params or {}
            params["outputFormat"] = "json-stat2"

        # Set up the request and prepare it for being sent
        request = Request(
            method="POST" if query else "GET",
            url=self.url + endpoint,
            json=query,
            params=(
                set_params := self.params | params if params else self.params
            ),
        ).prepare()

        logger.debug("%s request prepared for %s", request.method, request.url)
        logger.debug("Request with parameters: %s", set_params)
        if query:
            logger.debug("Request with query: %s", query)

        # Handle cache settings
        if not self.session.settings.disabled:
            # Use the request as the cache key so we can look it up first
            cache_key = self.session.cache.create_key(request)

            # If there's a cache, go ahead without rate limiting
            if cache_key in self.session.cache.responses:
                logger.debug("Request found in cache")
                return self.session.send(request).json()

        # Otherwise rate limit first, if there's a limit set by the API
        if (
            enforce_rate_limit
            and self.configuration["maxCallsPerTimeWindow"] > 0
        ):
            # Wait if needed
            self.rate_limit()

        # We do a set number of retries. This is because
        # the API can sometimes respond with 429 (too many requests) given
        # a heavy number of subqueries. The response contains a retry-after in the headers
        # so we basically back off and then go again
        for attempt in range(max_retries + 1):
            logger.debug("Sending request, attempt %s", attempt + 1)
            response = self.session.send(request)
            if response.ok:
                logger.debug("OK response")
                return response.json()

            # If we get this response, despite rate limiting, we basically
            # need to "back off" and then retry
            elif response.status_code == 429 and attempt < max_retries:
                retry_after = response.headers.get("Retry-After", "1")
                logger.debug(
                    "Response 429, backing off for %s second(s)", retry_after
                )

                time.sleep(float(retry_after))
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
                if now - timestamp < self.configuration["timeWindow"]
            ]
            # If we still have slots in the window, stop blocking and proceed
            if (
                len(self.call_timestamps)
                < self.configuration["maxCallsPerTimeWindow"]
            ):
                self.call_timestamps.append(now)
                return None
            # Otherise there have been to many calls, so we need to sleep
            sleep_time = self.configuration["timeWindow"] - (
                now - self.call_timestamps[0]
            )
            logger.debug(
                "Hit the rate limit, sleeping for %s second(s)",
                sleep_time,
            )
            # This way we hold the lock so other threads queue up
            time.sleep(sleep_time)
