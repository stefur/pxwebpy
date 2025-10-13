from concurrent.futures import ThreadPoolExecutor
from typing import Literal, TypeAlias


from ._api import PxApi
from ._utils import (
    build_query,
    count_data_cells,
    expand_wildcards,
    split_value_codes,
    unpack_table_data,
)

KnownDatabase: TypeAlias = Literal["scb", "ssb"]
"""Selectable databases with a preconfigured API URL"""

DATABASE_URLS: dict[KnownDatabase, str] = {
    "scb": "https://api.scb.se/ov0104/v2beta/api/v2",
    "ssb": "https://data.ssb.no/api/pxwebapi/v2",
}


def get_known_databases() -> dict[str, str]:
    """
    Get all the known builtin databases, shorthand names as keys and corresponding API URL as value.

    Returns
    -------
    dict
        A dictionary with the database shorthand names as keys and the URLs as values.
    """
    return DATABASE_URLS


class PxDatabase:
    """
    A wrapper around the PxWeb API. Enables exploring available datasets interactively, getting table data, variables as well as other metadata.

    Parameters
    ----------
    api_url : str | KnownDatabase
        Either a shorthand name for a builtin database API, e.g. "scb". To check out avaiable databases, use `get_known_databases()`.
    language : str, optional
        The language to be used with the API. You can check available languages using the `~~.PxDatabase.get_config()` method.
    disable_cache : bool
        Disable the in-memory cache that is used for API responses.
    timeout : int
        The timeout in seconds to use when calling the database API.

    Examples
    --------
    Get the SCB database API using the shorthand:
    >>> db = PxDatabase("scb")
    >>> db
    PxDatabase(api_url='https://api.scb.se/ov0104/v2beta/api/v2',
               language='sv',
               disable_cache=False,
               timeout=30)
    """

    def __init__(
        self,
        api_url: str | KnownDatabase,
        language: str | None = None,
        disable_cache: bool = False,
        timeout: int = 30,
    ):
        self._api = PxApi(
            url=DATABASE_URLS.get(api_url, api_url),
            language=language,
            timeout=timeout,
            disable_cache=disable_cache,
        )  # Resolve the name if known else assume it's a full URL

        # Pull in the total number of elements (tables) available in the database
        self.number_of_tables: int | None = (
            self._api.call(endpoint="/tables").get("page").get("totalElements")
        )

    def __repr__(self) -> str:
        return f"""PxDatabase(api_url='{self._api.url}',
        language={self._api.params.get("lang")},
        disable_cache={self._api.session.settings.disabled},
        timeout={self._api.timeout},
        number_of_tables={self.number_of_tables})"""

    def __eq__(self, other) -> bool:
        return self._api.url == other

    def get_config(self) -> dict:
        """
        Retrieve the configuration for the API.

        Returns
        -------
        dict
            The API response containing the configuration.

        Examples
        --------
        >>> conf = db.get_config()

        Check the languages available.

        >>> conf.get("languages")
        [{'id': 'sv', 'label': 'Svenska'},
         {'id': 'en', 'label': 'English'}]
        """
        return self._api.call(
            endpoint="/config",
        )

    @property
    def disable_cache(self) -> None:
        """Get the cache setting."""
        return self._api.session.settings.disabled

    @disable_cache.setter
    def disable_cache(self, value) -> None:
        """Set the cache setting."""
        self._api.session.settings.disabled = value

    @property
    def language(self) -> str:
        """Get the current language."""
        return self._api.params["lang"]

    @language.setter
    def language(self, value) -> None:
        """Set the language to use with the API."""
        self._api.params["lang"] = value

    def search(
        self,
        query: str | None = None,
        past_days: int | None = None,
        include_discontinued: bool | None = None,
        page_size: int | None = None,
    ) -> dict:
        """
        Search for tables in the database.

        Parameters
        ----------
        query : str, optional
            A string to search for.
        past_days : int, optional
            Return results where tables have been updated within n number of days.
        include_discontinued : bool, optional
            Include any tables that are discontinued.
        page_size : int, optional
            Number of results per page in the returning dict. Results will be paginated if they exceed this value.

        Returns
        -------
        dict
            The API response of the search query.

        Examples
        --------
        >>> db = PxDatabase("scb")
        >>> search = db.search(query="arbetsmarknad", past_days=180)
        >>> len(search.get("tables"))
        4
        """
        parameters = {
            "query": query,
            "pastDays": past_days,
            "includeDiscontinued": include_discontinued,
            "pageSize": page_size,
        }

        # TODO Some nicer ux for multi page responses?

        return self._api.call(endpoint="/tables", params=parameters)

    def get_code_list(self, code_list_id: str) -> dict:
        """
        Get information about a code list.

        Parameters
        ----------
        code_list_id : str
            The ID of a code list.

        Returns
        -------
        dict
            The API response with the code list information.

        Examples
        --------
        By checking out the table variables with the `~~.PxDatabase.get_table_variables()` method we can get available code lists.

        >>> meta = db.get_table_variables("TAB638")

        With the metadata, get the code lists available for "Region".

        >>> meta.get("Region").get("codelists")
        [{'id': 'agg_RegionA-region_2', 'label': 'A-regioner'},
        {'id': 'agg_RegionKommungrupp2005-_1', 'label': 'Kommungrupper (SKL:s) 2005'},
        {'id': 'agg_RegionKommungrupp2011-', 'label': '...'},
        {'id': 'vs_RegionKommun07', 'label': 'Kommuner'},
        {'id': 'vs_RegionLän07', 'label': 'Län'},
        {'id': 'vs_RegionRiket99', 'label': 'Riket'},
        ...]

        Now we can look closer at a specific code list by using the method.

        >>> db.get_code_list("vs_RegionLän07")
        {
        ...     'id': 'vs_RegionLän07',
        ...     'label': 'Län',
        ...     'language': 'sv',
        ...     'type': 'Valueset',
        ...     'values': [
        ...         {'code': '01', 'label': 'Stockholms län'},
        ...         {'code': '03', 'label': 'Uppsala län'},
        ...         {'code': '04', 'label': 'Södermanlands län'},
        ...         ...
        ...     ]
        ... }
        """
        return self._api.call(
            endpoint=f"/codelists/{code_list_id}",
        )

    def get_table_metadata(self, table_id: str) -> dict:
        """
        Get the complete set of metadata for a table.
        Parameters
        ----------
        table_id : str
            The ID of a table to get metadata from.

        Returns
        -------
        dict
            The API response containing the metadata.

        Examples
        --------
        >>> meta = db.get_table_metadata("TAB638")
        >>> meta.keys()
        dict_keys(['version', 'class', 'href', 'label', 'source',
        ...         'updated', 'link', 'note', 'role', 'id',
        ...         'size', 'dimension', 'extension'])
        >>> meta.get("label")
        'Folkmängden efter region, civilstånd, ålder, kön, tabellinnehåll och år'
        """
        return self._api.call(
            endpoint=f"/tables/{table_id}/metadata",
        )

    def get_table_variables(self, table_id: str) -> dict:
        """
        Get the specific metadata for variables and values with their respective code and labels. Also includes information  whether a variable can be eliminated as well as the available code lists.
        The information returned is unpacked and slightly more easily navigated than the output from the `~~.PxDatabase.get_table_metadata()` method.

        Parameters
        ----------
        table_id : str
            The ID of a table to get metadata from.

        Returns
        -------
        dict
            The API response containing the metadata.

        Examples
        --------
        >>> db.get_table_variable("TAB638")
        {
        ...     'Region': {
        ...         'label': 'region',
        ...         'category': {'label': {'00': 'Riket', '01': 'Stockholms län', ...}},
        ...         'elimination': True,
        ...         'codelists': [{'id': 'vs_RegionKommun07', 'label': 'Kommuner'}, ...]
        ...     },
        ...     'Alder': {
        ...         'label': 'ålder',
        ...         'category': {'label': {'0': '0 år', '1': '1 år', ...}},
        ...         'elimination': True,
        ...         'codelists': [{'id': 'agg_Ålder5år', 'label': '5-årsklasser'}, ...]
        ...     },
        ...     'Tid': {
        ...         'label': 'år',
        ...         'category': {'label': {'2022': '2022', '2023': '2023', ...}},
        ...         'elimination': False,
        ...         'codelists': []
        ...     },
        ...     ...
        }
        """
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
        value_codes: dict[str, list[str]] | None = None,
        code_list: dict[str, str] | None = None,
    ) -> list[dict]:
        """
        Get table data that can be used with dataframes like `polars` or `pandas`. The query is constructed with the method parameters.
        An empty value code selection returns a default selection for the table.

        Parameters
        ----------
        table_id : str
            An ID of a table to get data from.
        value_codes : dict, optional
            The value codes to use for data selection where the keys are the variable codes. You can use the `~~.PxDatabase.get_table_variables()` to explore what's available.
        code_list : dict, optional
            Any named code list to use with a variable for code selection.

        Returns
        -------
        list[dict]
            A dataset in a native format that can be loaded into a dataframe.

        Examples
        --------
        A simple query to get the population of 2024 for  all the Stockholm municipalities using 5-year age groups.
        >>> dataset = db.get_table_data(
        ...     table_id="TAB638",
        ...     value_codes={
        ...         "ContentsCode": ["BE0101N1"],
        ...         "Region": ["01*"],
        ...         "Alder": ["*"],
        ...         "Tid": ["2024"]
        ...     },
        ...     code_list={
        ...         "Alder": "agg_Ålder5år",
        ...         "Region": "vs_RegionKommun07"
        ...     }
        ... )

        This dataset can then easily be turned into a dataframe, for example with `polars`.

        >>> pl.DataFrame(dataset)
        shape: (572, 5)
        ┌─────────────────────┬────────────────┬────────────────┬──────┬───────┐
        │ region              ┆ ålder          ┆ tabellinnehåll ┆ år   ┆ value │
        │ ---                 ┆ ---            ┆ ---            ┆ ---  ┆ ---   │
        │ str                 ┆ str            ┆ str            ┆ str  ┆ i64   │
        ╞═════════════════════╪════════════════╪════════════════╪══════╪═══════╡
        │ 0114 Upplands Väsby ┆ 0-4 år         ┆ Folkmängd      ┆ 2024 ┆ 2931  │
        │ 0114 Upplands Väsby ┆ 5-9 år         ┆ Folkmängd      ┆ 2024 ┆ 3341  │
        │ 0114 Upplands Väsby ┆ 10-14 år       ┆ Folkmängd      ┆ 2024 ┆ 3237  │
        │ 0114 Upplands Väsby ┆ 15-19 år       ┆ Folkmängd      ┆ 2024 ┆ 3083  │
        │ 0114 Upplands Väsby ┆ 20-24 år       ┆ Folkmängd      ┆ 2024 ┆ 2573  │
        │ …                   ┆ …              ┆ …              ┆ …    ┆ …     │
        │ 0192 Nynäshamn      ┆ 85-89 år       ┆ Folkmängd      ┆ 2024 ┆ 554   │
        │ 0192 Nynäshamn      ┆ 90-94 år       ┆ Folkmängd      ┆ 2024 ┆ 230   │
        │ 0192 Nynäshamn      ┆ 95-99 år       ┆ Folkmängd      ┆ 2024 ┆ 51    │
        │ 0192 Nynäshamn      ┆ 100+ år        ┆ Folkmängd      ┆ 2024 ┆ 7     │
        │ 0192 Nynäshamn      ┆ uppgift saknas ┆ Folkmängd      ┆ 2024 ┆ 0     │
        └─────────────────────┴────────────────┴────────────────┴──────┴───────┘
        """
        # TODO support output_values

        # If no value codes are supplied, send a query to get the default selection
        if value_codes is None:
            response = self._api.call(
                endpoint=f"/tables/{table_id}/data",
            )
            dataset = unpack_table_data(response)
            return dataset

        # Make sure all selections provided are in a list, even if single values
        for variable in list(value_codes.keys()):
            if not isinstance(variable, str):
                raise ValueError("All variables must be strings.")

            value_code = value_codes[variable]

            if isinstance(value_code, str):
                value_codes[variable] = [value_code]  # Coerce single strings to list
            elif isinstance(value_code, list):
                if not all(isinstance(v, str) for v in value_code):
                    raise ValueError(
                        f"All value codes in list for variable '{variable}' must be strings."
                    )
            else:
                raise ValueError(
                    f"Value codes for variable '{variable}' must be a string or a list of strings."
                )

        # Check if any wildcards exist in the code list
        wildcard_in_codelist_variables = [
            variable
            for variable, codes in value_codes.items()
            if code_list and variable in code_list and any(s for s in codes if "*" in s)
        ]

        # And the same for value codes if the variable is not already included above
        value_codes_has_wildcard: bool = any(
            "*" in code
            for variable, codes in value_codes.items()
            # Don't include a variable that is using a codelist to avoid double lookups
            if variable not in wildcard_in_codelist_variables
            for code in codes
        )

        # Get the codelists if there's a wildcard
        if code_list and wildcard_in_codelist_variables:
            code_lists = {
                var: self.get_code_list(cid) for var, cid in code_list.items()
            }
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
                for sub_query in split_value_codes(
                    value_codes, self._api.max_data_cells
                )
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

    def all_tables(self) -> list[dict[str, str]]:
        """
        Get a list of all tables available with some basic metadata. Use `~~.PxDatabase.get_table_metadata()` for extensive metadata about a specific table.

        Returns
        -------
        list[dict]
            All tables.
        """
        return self._api.call(
            endpoint="/tables", params={"pageSize": self.number_of_tables}
        ).get("tables")

    def _unpack_paths(self, table: dict) -> list[dict]:
        """Flatten the list of lists containing paths"""
        return [item for sublist in table.get("paths", []) for item in sublist]

    def tables_on_path(self, path_id: str) -> list[dict[str, str]]:
        """
        List all the tables available on the path.

        Examples
        --------
        >>> db.tables_on_path("AM0101C")
        [
        ... {'id': 'TAB2566',
        ...  'label': 'Genomsnittlig månadslön för tjänstemän, privat sektor (KLP) efter näringsgren SNI2002 ...'},
        ... {'id': 'TAB2552',
        ...  'label': 'Genomsnittlig timlön för arbetare, privat sektor (KLP) efter näringsgren SNI2002 ...'},
        ... {'id': 'TAB386',
        ...  'label': 'Antal arbetare inom industrin efter näringsgren SNI92 ...'},
        ... {'id': 'TAB2565',
        ...  'label': 'Genomsnittlig månadslön för tjänstemän, privat sektor (KLP) efter provision och näringsgren SNI92 ...'},
        ... {'id': 'TAB2551',
        ...  'label': 'Genomsnittlig timlön för arbetare, privat sektor (KLP) efter näringsgren SNI92 ...'},
        ... ...
        ]     
        
        Returns
        -------
        list[dict]
            All tables on the path.
        """

        tables = self.all_tables()

        result = []

        for table in tables:
            paths = self._unpack_paths(table)

            if any(path["id"] == path_id for path in paths):
                result.append(
                    {
                        "id": table.get("id"),
                        "label": table.get("label"),
                        "paths": table.get("paths"),
                    }
                )

        return result

    def get_paths(self, path_id: str | None = None) -> list[dict[str, str]]:
        """
        List all paths available to explore. Use the ID to list tables on a specific path with `~~.PxDatabase.tables_on_path()`.

        Examples
        --------
        >>> db.get_paths()
        [
        ... {'id': 'AA', 'label': 'Ämnesövergripande statistik'},
        ... {'id': 'AA0003', 'label': 'Registerdata för integration'},
        ... {'id': 'AA0003B', 'label': 'Statistik med inriktning mot arbetsmarknaden'},
        ... {'id': 'AA0003C', 'label': 'Statistik med inriktning mot flyttmönster'},
        ... {'id': 'AA0003D', 'label': 'Statistik med inriktning mot boende'},
        ... ...
        ]

        You can also inspect a subpath by supplying a `path_id`.

        >>> db.get_paths("AM0101")
        [
        ... {'id': 'AM', 'label': 'Arbetsmarknad'},
        ... {'id': 'AM0101',
        ...  'label': 'Konjunkturstatistik, löner för privat sektor (KLP)'},
        ... {'id': 'AM0101A', 'label': 'Arbetare: Timlön efter näringsgren'},
        ... {'id': 'AM0101B', 'label': 'Tjänstemän: Månadslön efter näringsgren'},
        ... {'id': 'AM0101C', 'label': 'Äldre tabeller som inte uppdateras'},
        ... {'id': 'AM0101X', 'label': 'Nyckeltal'},
        ]
        
        Returns
        -------
        list[dict]
            Paths available.

        """

        tables = self.all_tables()

        result = []
        seen = set()
        for table in tables:
            # Unpack the sublist, cause for whatever reason there's a list in a list
            paths = self._unpack_paths(table)

            # If there's a path_id supplied, filter on it
            if path_id and all(path["id"] != path_id for path in paths):
                continue

            # Proceed to unpack unique IDs
            for path in paths:
                if path["id"] not in seen:
                    seen.add(path["id"])
                    result.append(path)

        result = sorted(result, key=lambda x: x["id"])

        return result
