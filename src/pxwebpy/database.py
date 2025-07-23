from typing import Literal

from ._api import PxApi
from ._utils import (
    build_query,
    convert_wildcards,
    count_data_cells,
    map_value_codes,
    split_query,
    unpack_table_data,
)

KnownDatabase = Literal["scb"]

_DATABASE_URLS: dict[KnownDatabase, str] = {
    "scb": "https://api.scb.se/ov0104/v2beta/api/v2",
}


class PxDatabase:
    """
    An object representing a PxWeb database. Allows for exploring a database using its API.
    """

    def __init__(
        self,
        api_url: str | KnownDatabase,
        language: str | None = None,
        timeout: int = 30,
    ):
        self._api = PxApi(
            url=_DATABASE_URLS.get(api_url, api_url),
            language=language,
            timeout=timeout,
        )  # Resolve the URL if known else assume it's a full URL
        self.previous_location: list[str | None] = []
        self.current_location = self._api.call(endpoint="/navigation")

    def get_config(self) -> str:
        """Retrieve the configuration for the API"""
        return self._api.call(
            endpoint="/config",
        )

    def here(self) -> str:
        """Retrieve the current location in the navigation"""
        return self.current_location

    def reset(self):
        """
        Go back to the toplevel navigation of the database API.
        """
        # Reset the trace
        self.previous_location = []
        self.current_location = self._api.call(
            endpoint="/navigation",
        )

    def trace(self) -> list:
        """
        Used to check path to the current location in the navigation tree.
        The current location is the last item in the list.
        """
        return self.previous_location

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
            previous = self.previous_location.pop()
        except IndexError:
            raise IndexError("Failed to go back. Already at the top of navigation.")

        self.current_location = self._api.call(
            endpoint=f"/navigation/{previous}",
        )
        return

    def get_contents(self) -> dict:
        """
        Shows the contents of the current location.
        """
        folder_contents = self.current_location.get("folderContents")

        information = ["id", "label"]

        folders = [
            {k: item.get(k) for k in information}
            for item in folder_contents
            if item.get("type") == "FolderInformation"
        ]
        tables = [
            {k: item.get(k) for k in information}
            for item in folder_contents
            if item.get("type") == "Table"
        ]
        return {"folders": folders, "tables": tables}

    def get_codelist(self, codelist_id) -> dict:
        """Get the codelist information"""
        return self._api.call(
            endpoint=f"/codelists/{codelist_id}",
        )

    def get_table_metadata(self, table_id: str) -> dict:
        """Get the complete set of metadata for a table"""
        return self._api.call(
            endpoint=f"/tables/{table_id}/metadata",
        )

    def get_table_variables(self, table_id: str) -> dict:
        """Shorthand for getting the variables and values with their respective code and labels. Also includes information  whether a variable can be eliminated as well as the available codelists."""
        dimensions = self._api.call(
            endpoint=f"/tables/{table_id}/metadata",
        ).get("dimension")
        result = {}

        # Trim the information
        for key, value in dimensions.items():
            out = {}
            out["label"] = value.get("label", "")
            if "category" in value and "label" in value["category"]:
                out["category"] = {"label": value["category"]["label"]}
            else:
                out["category"] = {}
            extension = value.get("extension", {})
            out["elimination"] = extension.get("elimination", False)
            code_lists = extension.get("codeLists", [])
            out["codelists"] = [
                {"id": cl.get("id"), "label": cl.get("label")}
                for cl in code_lists
                if "id" in cl and "label" in cl
            ]
            result[key] = out

        return result

    def get_table_data(
        self,
        table_id: str,
        value_codes: dict = {},
        code_list: dict | None = None,
    ) -> list[dict]:
        # TODO support output_values

        # Make sure all selections provided are in a list, even if single values
        value_codes = {
            k: v if isinstance(v, (list, tuple, set)) else [v]
            for k, v in value_codes.items()
        }

        # Pull in the all labels and codes
        table_variables = self.get_table_variables(table_id)

        # Perform conversion
        value_codes = convert_wildcards(value_codes, table_variables)

        # Ensure that only value_codes are use that match a code list, if one is used
        if code_list:
            # Use each variable and code list ID to first get the actual code list, then map the values
            # So that we get a correct selection based on the list
            for variable, code_list_id in code_list.items():
                table_code_list = self.get_codelist(code_list_id)
                value_codes = map_value_codes(variable, value_codes, table_code_list)

        # Now count the data cells we're getting to check against the max allowed
        if count_data_cells(value_codes) > self._api.max_data_cells:
            result = []

            # Split the query into several API calls
            for sub_query in split_query(value_codes, self._api.max_data_cells):
                query = build_query(sub_query, code_list)
                response = self._api.call(
                    endpoint=f"/tables/{table_id}/data?&outputFormat=json-stat2",
                    query=query,
                )
                dataset = unpack_table_data(response)
                result.extend(dataset)

            return result
        else:
            query = build_query(value_codes, code_list)
            response = self._api.call(
                endpoint=f"/tables/{table_id}/data?&outputFormat=json-stat2",
                query=query,
            )
            dataset = unpack_table_data(response)
            return dataset

    # TODO: accept optional id instead of folder to move directly to an endpoint
    def go_to(self, *folder: str) -> None:
        """
        Go to folder using a single string or move over a path by supplying multiple strings.

        Examples
        --------
        ```python
        db = PxDatabase("scb")

        db.go_to("Befolkning")

        db.back()

        db.go_to("Befolkning", "Befolkningsstatistik", "Folkmängd"

        ```

        """
        for element in folder:
            previous = self.current_location.get("id")
            # Trace our steps
            self.previous_location.append(previous)

            for item in self.current_location.get("folderContents"):
                # Only move into folders
                if element in (item.get("label"), item.get("id")):
                    # Update the location and then break the loop,
                    # moving to the next element in the path
                    if item.get("type") == "FolderInformation":
                        self.current_location = self._api.call(
                            endpoint=f"/navigation/{item.get('id')}",
                        )
                    break
            else:
                # And if we can't find it...
                raise ValueError(f"Folder '{element}' could not be found in the path.")
