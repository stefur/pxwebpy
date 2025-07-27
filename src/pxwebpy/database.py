from concurrent.futures import ThreadPoolExecutor
from typing import Literal

from ._api import PxApi
from ._utils import (
    build_query,
    count_data_cells,
    expand_wildcards,
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

    @property
    def language(self):
        """Get the current language."""
        return self._api.params["lang"]

    @language.setter
    def language(self, value):
        """Set the current language."""
        self._api.params["lang"] = value

    def here(self) -> str:
        """Retrieve the current location in the navigation"""
        return self.current_location

    def reset(self):
        """
        Go back to the toplevel navigation of the database.
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

        # Check if any wildcards exist in the value_codes
        wildcard_in_codelist_variables = [
            variable
            for variable, codes in value_codes.items()
            if code_list and variable in code_list and codes == ["*"]
        ]

        value_codes_has_wildcard: bool = any(
            (isinstance(codes, (list, tuple, set)) and "*" in codes)
            for variable, codes in value_codes.items()
            # Don't include a variable that is using a codelist to avoid double lookups
            if variable not in wildcard_in_codelist_variables
        )

        # Get the codelists if there's a wildcard
        if code_list and wildcard_in_codelist_variables:
            code_lists = {var: self.get_codelist(cid) for var, cid in code_list.items()}
            value_codes = expand_wildcards(value_codes, code_lists)

        if value_codes_has_wildcard:
            # Pull in the all labels and codes
            table_variables = self.get_table_variables(table_id)

            # Perform wildcard expansion
            value_codes = expand_wildcards(value_codes, table_variables)

        # Now count the data cells we're getting to check against the max allowed
        if count_data_cells(value_codes) > self._api.max_data_cells:
            # Split the query into several subqueries for API calls
            subqueries = [
                build_query(sub_query, code_list)
                for sub_query in split_query(value_codes, self._api.max_data_cells)
            ]

            dataset = []

            # Use threading for the subqueries
            with ThreadPoolExecutor() as executor:
                # Map() so that we yield results in order
                for result in executor.map(
                    lambda subquery: unpack_table_data(
                        self._api.call(
                            endpoint=f"/tables/{table_id}/data", query=subquery
                        )
                    ),
                    subqueries,
                ):
                    dataset.extend(result)

        else:
            # No batching needed so we just go ahead with the query as is
            query = build_query(value_codes, code_list)
            response = self._api.call(
                endpoint=f"/tables/{table_id}/data",
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
