#%%
import requests, json
from pathlib import Path

class PxWebpy:
    """
    Instantiate a PxWebpy object.
    """
    
    def __init__(self):
        pass
    
    def get_data(self, json_query: Path | str, url: str) -> str:
        """Sends a json query and returns the response"""
        match json_query:
            case json_query if isinstance(json_query, Path):
                with open(json_query, "r") as read:
                    query = json.load(read)
            case str():
                query = json.loads(json_query)
            case _:
                print("No appropriate format")
                return

        response = requests.post(url, json=query)

        return response.text
