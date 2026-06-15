import json
from pathlib import Path
from typing import Any

RESPONSES = Path(__file__).parent / "responses"
BASE_URL = "https://mock.api"


def load_response(name: str) -> Any:
    return json.loads((RESPONSES / name).read_text())
