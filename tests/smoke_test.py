"""
Check that basic features work on release.
"""

from pxweb import get_known_apis

apis = get_known_apis()

if not isinstance(apis, dict) or not apis:
    raise RuntimeError("Smoke test failed")
else:
    print("Smoke test passed")
