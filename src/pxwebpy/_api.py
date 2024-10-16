from typing import Optional

import requests
from requests_cache import CachedSession


def call(session: CachedSession, url: str, query: Optional[dict] = None) -> dict:
    """Call the API using the table URL and optional query"""
    timeout = 10

    if query:
        response = session.post(url, json=query, timeout=timeout)
    else:
        response = session.get(url, timeout=timeout)

    if response.ok:
        return response.json()
    else:
        raise requests.exceptions.HTTPError(
            f"Error {response.status_code}: {response.reason}"
        )
