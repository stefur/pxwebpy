# TODO
# TABLE
# Drop string import for queries, let any string query be handled by user / json
# Query construction with kwargs
# get_data() with option to turn off caching?
# get_variables()

# API
# Use the max cell count to batch a query for optimal size
# Throttle according to timewindow and queries in window

from typing import Literal, Union

from pxwebpy import _api
from pxwebpy.table import PxTable

KnownDatabase = Literal["scb"]
PathOrLabel = Union[str, tuple[str]]

_DATABASE_URLS: dict[KnownDatabase, str] = {
    "scb": "https://api.scb.se/ov0104/v2beta/api/v2",
}


class PxDatabase:
    def __init__(self, api_url: str | KnownDatabase):
        # TODO Should probably either clean trailing slashes or opt to add it
        self._api = _api.PxApi(
            _DATABASE_URLS.get(api_url, api_url)
        )  # Resolve the URL if known else assume it's a full URL
        self._previous_location: list[str | None] = []
        self._current_location = self._api.call(endpoint="/navigation")

    def reset(self):
        # Reset the trace
        self._previous_location = []
        self._current_location = self._api.call(
            endpoint="/navigation",
        )

    def trace(self) -> list:
        """
        Used to check path to the current location in the navigation tree.
        The current location is the last item in the list.
        """
        return self._previous_location

    def back(self) -> None:
        """
        Go up one level in the navigation tree.

        Examples
        -------

        ```python

        db.go_to("Befolkning", "Befolkningsstatistik")

        db.back()

        ```

        """
        try:
            previous = self._previous_location.pop()
        except IndexError:
            raise IndexError("Failed to go back. Already at the top of navigation.")

        self._current_location = self._api.call(
            endpoint=previous,
        )
        return

    def get_contents(self) -> dict:
        """
        Shows the contents of the current location.
        """
        folder_contents = self._current_location.get("folderContents")

        folders = [
            folder.get("label")
            for folder in folder_contents
            if folder.get("type") == "FolderInformation"
        ]
        tables = [
            folder.get("label")
            for folder in folder_contents
            if folder.get("type") == "Table"
        ]
        # TODO Likely a more usable way to do this
        return {"folders": folders, "tables": tables}

    def get_table(self, *table: PathOrLabel) -> PxTable | None:
        # This assumes the last item is the table name, if a tuple is supplied
        if len(table) > 1:
            path: list[str] = list(table)
            table = path.pop()
            self.go_to(*path)
        else:
            table = table[0]

        folder_contents = self._current_location.get("folderContents")
        for folder in folder_contents:
            if folder.get("label") == table and folder.get("type") == "Table":
                id_ = folder.get("id")
                return PxTable(url=f"{self._api.url}/tables/{id_}")
                # TODO Instantiating a table should fetch metadata directly
                # Setting a language should probably be done at a database level too, but possibility to switch
        else:
            # And if we can't find the table...
            raise RuntimeError(f"The table '{table}' could not be found in the path.")

    def go_to(self, *folder: PathOrLabel) -> None:
        """
        Go to folder using a single string or move over a path by supplying multiple strings.ArithmeticError

        Examples
        --------
        ```python
        db = PxDatabase("scb")

        db.go_to("Befolkning")

        db.back()

        db.go_to("Befolkning", "Befolkningsstatistik", "Folkmängd")

        ```

        """
        for element in folder:
            link: str = next(
                link["href"]
                for link in self._current_location.get("links")
                if link["rel"] == "self"
            )

            # Trace our steps
            self._previous_location.append(link)

            for item in self._current_location.get("folderContents"):
                # Only move into folders
                if (
                    item.get("label") == element
                    and item.get("type") == "FolderInformation"
                ):
                    # Update the location and then break the loop,
                    # moving to the next element in the path
                    id_ = item.get("id")
                    self._current_location = self._api.call(
                        endpoint=f"/navigation/{id_}",
                    )
                    break
            else:
                # And if we can't find it...
                raise RuntimeError(
                    f"Folder '{element}' could not be found in the path."
                )
