import itertools


def count_data_cells(value_codes: dict) -> int:
    """
    Based on the supplied value codes, calculate the number of data cells this would
    selection would produce.
    """
    value_codes_length = {
        variable: len(value) for variable, value in value_codes.items()
    }
    number_of_data_cells = 1
    for value_code in value_codes_length:
        number_of_data_cells *= value_codes_length[value_code]

    return number_of_data_cells


def build_query(value_codes: dict, code_list: dict | None = None) -> dict:
    """
    Construct a query based on supplied value codes and optional code list.
    """
    selection = []
    for variable, value in value_codes.items():
        item = {"variableCode": variable, "valueCodes": value}
        if code_list and variable in code_list:
            item["codelist"] = code_list[variable]
        selection.append(item)

    return {"selection": selection}


def split_value_codes(value_codes: dict, max_cells: int) -> list[dict]:
    """
    Recursively split a query to not go over API limit of max data cells allowed.
    Will try to optimize the batch size for each split, minimizing the number of queries sent.
    """
    # Similar to count_data_cells, but here we keep the data cell size of each variable selection
    sizes = {k: len(v) for k, v in value_codes.items()}
    total_cells = 1
    for size in sizes.values():
        total_cells *= size

    # If it's within the allowed batch size, return this query in a list
    if total_cells <= max_cells:
        return [value_codes]

    # If there are still variables with multiple values, split further
    # Basically splitting a variable with 1 value makes no difference
    split_variables = [k for k, v in sizes.items() if v > 1]

    # If there are no such variables left, we reached the point where we can't be split
    # any further, so just return the query in a list
    if not split_variables:
        return [value_codes]

    # We go by largest-dimension-first, so choose variable with largest number of values (biggest "contributor" to the query being too large)
    largest_variable = max(split_variables, key=sizes.get)

    # Figure out max chunk size for this variable
    other_cell_count = total_cells // sizes[largest_variable]
    max_chunk_size = max_cells // other_cell_count or 1

    values = value_codes[largest_variable]
    results = []

    # Now split values into chunks of at most max_chunk_size
    for i in range(0, len(values), max_chunk_size):
        new_query = value_codes.copy()
        new_query[largest_variable] = values[i : i + max_chunk_size]
        results.extend(split_value_codes(new_query, max_cells))
    return results


def expand_wildcards(value_codes: dict, source: dict) -> dict:
    """
    Expand wildcards in value_codes using the provided `source`.
    `source` is either a `dict` with either wcode list or table_variables.
    """
    result = {}
    for variable, codes in value_codes.items():
        # Expand the items using the source
        if "values" in source.get(variable, {}):
            # Code list structure
            items = [entry["code"] for entry in source[variable]["values"]]
        elif "category" in source.get(variable, {}):
            # Table variables structure
            items = list(source[variable]["category"]["label"].keys())
        else:
            items = codes

        expanded = []
        # Now start expanding the wildcards
        for value in codes:
            # TODO Probably use match case when 3.9 is dropped
            if value == "*":
                expanded.extend(items)
            elif value.endswith("*") and not value.startswith("*"):
                expanded.extend(
                    [code for code in items if code.startswith(value[:-1])]
                )  # Drop the asterisk
            elif value.startswith("*") and not value.endswith("*"):
                expanded.extend(
                    [code for code in items if code.endswith(value[1:])]
                )  # Same here
            elif value.startswith("*") and value.endswith("*"):
                expanded.extend(
                    [code for code in items if value[1:-1] in code]
                )  # And here!
            else:
                # Just default if no wildcard
                expanded.append(value)

        result[variable] = expanded

    return result


def unpack_table_data(json_data: dict) -> list[dict]:
    """
    Takes json-stat2 data and turns it into a list of dicts that can
    be used to convert into a dataframe, using either pandas or polars.
    """

    dimension_categories = {}

    # Go over each dimension
    for dim in json_data["dimension"]:
        # If the dimension has extension data along with a key for show, use
        # that to determine the values shown in the output
        try:
            show_value = json_data["dimension"][dim]["extension"]["show"]

            if show_value == "code_value":
                values = [
                    " ".join([str(k), str(v)])
                    for k, v in json_data["dimension"][dim]["category"][
                        "label"
                    ].items()
                ]

            elif show_value == "code":
                values = [
                    str(k)
                    for k in json_data["dimension"][dim]["category"][
                        "label"
                    ].keys()
                ]

            elif show_value == "value":
                values = [
                    str(v)
                    for v in json_data["dimension"][dim]["category"][
                        "label"
                    ].values()
                ]
                # Raise an error if we hit some value in the show key that we don't know how to handle

            else:
                raise ValueError(
                    f"""Unexpected show value. Expected "code", "value" or "code_value", got: {show_value}"""
                )

        # If there's no show key at all we default to using the label values
        except KeyError:
            values = [
                str(v)
                for v in json_data["dimension"][dim]["category"][
                    "label"
                ].values()
            ]

        dimension_categories.update(
            {json_data["dimension"][dim]["label"]: values}
        )

    # The result is a list of dicts with the dimension as key and product of the category labels for values
    result = [
        dict(zip(dimension_categories.keys(), x))
        for x in itertools.product(*dimension_categories.values())
    ]

    # Finally add the value for each dict representing a row
    for value, dict_row in zip(json_data["value"], result):
        dict_row["value"] = value

    return result
