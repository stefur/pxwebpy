"""
Check some basics to ensure functionality.
"""

from pxweb import get_known_apis

apis = get_known_apis()

if not isinstance(apis, dict) or not apis:
    raise RuntimeError("Smoke test failed")
else:
    print("Smoke test passed")
