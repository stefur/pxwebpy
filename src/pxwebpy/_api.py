from typing import Optional

from requests.exceptions import HTTPError
from requests_cache import CachedSession


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
