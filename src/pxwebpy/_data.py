import itertools


def build_query(json_data: dict, query: dict) -> list:
    """Build an API query for a table"""
    # Set up a dictionary to use for conversion between value and valueText
    value_mapping = {}

    for variable in json_data["variables"]:
        text = variable["text"]
        code = variable["code"]
        value_texts = variable["valueTexts"]
        values = variable["values"]

        value_mapping[(code, text)] = {
            "values": [
                (value, value_text) for value, value_text in zip(values, value_texts)
            ],
        }

    result = []

    for variable, values in query.items():
        # Go over the selected variables, a match can be either code or text
        matching_variable = next(
            (code_text for code_text in value_mapping.keys() if variable in code_text),
            None,
        )
        # Extract the code from the matching variable tuple
        if matching_variable:
            code = matching_variable[0]

            # Handle wild card filtering
            if "*" in values[0] and len(values) == 1:
                result.append(
                    {
                        "code": code,
                        "selection": {
                            "filter": "all",
                            "values": values,
                        },
                    }
                )
            else:
                # Select the values to map
                value_tuples = value_mapping[matching_variable]["values"]

                converted_values = []

                # Go over the values and convert any provided valueTexts to values
                for value in values:
                    # Try to find a match within either code
                    matching_value = next(
                        (
                            value_valuetext
                            for value_valuetext in value_tuples
                            if value in value_valuetext
                        ),
                        None,
                    )
                    # If we find a match we append the value that the API wants
                    if matching_value:
                        converted_values.append(matching_value[0])
                    else:
                        # Raise an error if the value is not found in the variable
                        raise KeyError(
                            f"Value '{value}' not found in variable '{variable}'."
                        )

                result.append(
                    {
                        "code": code,
                        "selection": {
                            "filter": "item",
                            "values": converted_values,
                        },
                    }
                )
        else:
            raise KeyError(f"Variable '{variable}' not found in table.")

    return result


def unpack_table_variables(json_data: dict) -> dict:
    """Takes a JSON response of variables and turns it into a more readable format"""
    try:
        result = {}

        for var in json_data["variables"]:
            code = var.get("code")
            value_texts = var.get("valueTexts")
            values = var.get("values")
            elimination = var.get("elimination", False)

            # Print values as tuples if the valueText is different from the value
            result[code] = {
                "values": [
                    v if v == vt else (v, vt) for v, vt in zip(values, value_texts)
                ],
                # Flip the logic :)
                "mandatory": not elimination,
            }

        return result
    except KeyError as err:
        raise KeyError(f"Failed to unpack, missing key: {err}") from None


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
