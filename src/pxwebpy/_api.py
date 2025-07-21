from pathlib import Path
from tempfile import gettempdir
from typing import Optional

from requests.exceptions import HTTPError
from requests_cache import CachedSession

# TODO


class PxApi:
    def __init__(self, url: str, timeout: int = 30):
        _tmp_dir = gettempdir()
        _cache = Path(_tmp_dir) / "pxwebpy_cache"
        self.session: CachedSession = CachedSession(cache_name=_cache, ttl=600)
        self.url: str = url
        self.timeout: int = timeout

    def call(self, endpoint: str, query: Optional[dict] = None) -> dict:
        """Call the API using the table URL and optional query"""

        if query:
            response = self.session.post(
                self.url + endpoint, json=query, timeout=self.timeout
            )
        else:
            response = self.session.get(self.url + endpoint, timeout=self.timeout)

        if response.ok:
            return response.json()
        else:
            raise HTTPError(f"Error {response.status_code}: {response.reason}")


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
