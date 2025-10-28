# pxwebpy


[![GitHub Actions Workflow
Status](https://img.shields.io/github/actions/workflow/status/stefur/pxwebpy/ci.yml?style=flat-square&label=ci)](https://github.com/stefur/pxwebpy/actions/workflows/ci.yml)
![PyPI -
Version](https://img.shields.io/pypi/v/pxwebpy?style=flat-square.png)
![PyPI - Python
Version](https://img.shields.io/pypi/pyversions/pxwebpy?style=flat-square.png)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json&style=flat-square)](https://github.com/astral-sh/uv)
[![ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json&style=flat-square)](https://github.com/astral-sh/ruff)  
Client library for the PxWeb API to easily load data into a DataFrame.

[Get started](https://stefur.github.io/pxwebpy) \|
[Examples](https://stefur.github.io/pxwebpy/examples) \|
[Reference](https://stefur.github.io/pxwebpy/reference)

## Features

- Automatic query batching to handle large queries to respect rate
  limits
- Multithreading for faster data fetching on large queries
- In-memory caching for quicker iterative use and exploration
- Wildcard support in queries
- BYODF (Bring Your Own DataFrame): native return formats for use with
  `pandas` or `polars`
- Search for tables, browse and list tables, get metadata, and more

It has been tested with [Statistics Sweden](https://scb.se) and
[Statistics Norway](https://www.ssb.no).

> \[!NOTE\]  
> pxwebpy only supports version 2.0 of the PxWeb API.

## Quick start

``` python
from pxweb import PxApi
import polars as pl

# Prepare to get data from the Statistics Norway API by using the builtin URL
api = PxApi("ssb")

# Set the language to english
api.language = "en"

# Check the population per year in Norway during the 1990's
data = api.get_table_data(
    "06913",
    value_codes={"Region": "0", "ContentsCode": "Folkemengde", "Tid": "199*"},
)

# Turn it into a polars dataframe
df = pl.DataFrame(data)

# A quick look at the result
print(df)
```

    shape: (10, 4)
    ┌─────────────────────┬──────────────────────┬──────┬─────────┐
    │ region              ┆ contents             ┆ year ┆ value   │
    │ ---                 ┆ ---                  ┆ ---  ┆ ---     │
    │ str                 ┆ str                  ┆ str  ┆ i64     │
    ╞═════════════════════╪══════════════════════╪══════╪═════════╡
    │ 0 The whole country ┆ Population 1 January ┆ 1990 ┆ 4233116 │
    │ 0 The whole country ┆ Population 1 January ┆ 1991 ┆ 4249830 │
    │ 0 The whole country ┆ Population 1 January ┆ 1992 ┆ 4273634 │
    │ 0 The whole country ┆ Population 1 January ┆ 1993 ┆ 4299167 │
    │ 0 The whole country ┆ Population 1 January ┆ 1994 ┆ 4324815 │
    │ 0 The whole country ┆ Population 1 January ┆ 1995 ┆ 4348410 │
    │ 0 The whole country ┆ Population 1 January ┆ 1996 ┆ 4369957 │
    │ 0 The whole country ┆ Population 1 January ┆ 1997 ┆ 4392714 │
    │ 0 The whole country ┆ Population 1 January ┆ 1998 ┆ 4417599 │
    │ 0 The whole country ┆ Population 1 January ┆ 1999 ┆ 4445329 │
    └─────────────────────┴──────────────────────┴──────┴─────────┘
