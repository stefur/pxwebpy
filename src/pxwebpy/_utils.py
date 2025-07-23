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


def map_value_codes(variable: str, value_codes: dict, table_code_list: dict) -> dict:
    """
    Go over the value codes together with code_list for the given variable
    """
    result = value_codes.copy()
    codes = value_codes.get(variable, [])

    # Gather all aggregate codes
    all_agg_codes = {entry["code"] for entry in table_code_list["values"]}

    # Build acodes detailed map of aggregate codes and value codes included
    value_to_aggcode = {}
    for entry in table_code_list["values"]:
        # Pick up each aggregate code
        agg_code = entry["code"]
        # And get each code from the value map as a key, and the aggregate code as value
        for val in entry.get("valueMap", []):
            value_to_aggcode[val] = agg_code

    mapped_codes = []

    for code in codes:
        if code in all_agg_codes:
            # Already an aggregate code (say age group), so keep as is
            if code not in mapped_codes:
                mapped_codes.append(code)
        else:
            # Try to map the code to an aggregate code using the lookup
            agg_code = value_to_aggcode.get(code)
            if agg_code and agg_code not in mapped_codes:
                mapped_codes.append(agg_code)

    result[variable] = mapped_codes
    return result


def split_query(query, max_cells) -> list[dict]:
    """
    Recursively split a query to not go over API limit of max data cells allowed.
    """
    # Similar to count_data_cells, but here we keep the data cell size of each variable selection
    sizes = {k: len(v) for k, v in query.items()}

    total_cells = 1
    for size in sizes.values():
        total_cells *= size

    # If it's within the allowed batch size, return this query in a list
    if total_cells <= max_cells:
        return [query]

    # If there are still variables with multiple values, split further
    split_variables = [k for k, v in sizes.items() if v > 1]
    if not split_variables:
        # In case all variables are down to a single value,
        # but for some reason total_cells is still too high
        return [query]

    # Pick the smallest dimension (fewest values) to split on
    smallest_variable = min(split_variables, key=sizes.get)

    # For each value in that variable, keep that fixed and split the rest
    results = []
    for i in range(sizes[smallest_variable]):
        new_query = query.copy()
        # Only include the i-th value for this variable in the next query
        new_query[smallest_variable] = [query[smallest_variable][i]]
        # Recursively get subqueries from this split and add to results
        sub_results = split_query(new_query, max_cells)
        results.extend(sub_results)
    return results


def convert_wildcards(value_codes: dict, table_variables) -> dict:
    """
    Convert any single wildcards to actual values so that we can calculate the
    number of data cells
    """
    # TODO handle wildcards like '01*' etc.
    result = {}

    for variable, value_code in value_codes.items():
        if (
            variable in table_variables.keys()
            and "*" in value_code
            and len(value_code) == 1
        ):
            # Replace '*' with all codes from the category
            result[variable] = list(
                table_variables[variable]["category"]["label"].keys()
            )
        else:
            # Keep the original value(s)
            result[variable] = value_code

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
                    for k, v in json_data["dimension"][dim]["category"]["label"].items()
                ]

            elif show_value == "code":
                values = [
                    str(k)
                    for k in json_data["dimension"][dim]["category"]["label"].keys()
                ]

            elif show_value == "value":
                values = [
                    str(v)
                    for v in json_data["dimension"][dim]["category"]["label"].values()
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
                for v in json_data["dimension"][dim]["category"]["label"].values()
            ]

        dimension_categories.update({json_data["dimension"][dim]["label"]: values})

    # The result is a list of dicts with the dimension as key and product of the category labels for values
    result = [
        dict(zip(dimension_categories.keys(), x))
        for x in itertools.product(*dimension_categories.values())
    ]

    # Finally add the value for each dict representing a row
    for value, dict_row in zip(json_data["value"], result):
        dict_row["value"] = value

    return result
