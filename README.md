# pxwebpy
[![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/stefur/pxwebpy/ci.yml?style=flat-square&label=ci)](https://github.com/stefur/pxwebpy/actions/workflows/ci.yml)
![PyPI - Version](https://img.shields.io/pypi/v/pxwebpy?style=flat-square)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pxwebpy?style=flat-square)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json&style=flat-square)](https://github.com/astral-sh/uv)
[![ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json&style=flat-square)](https://github.com/astral-sh/ruff)   
Easily get data from the PxWeb API and into a dataframe.  

[Get started]() | [Examples]() | [Reference]()

# Features
- Automatic query batching to handle large queries to respect rate limits
- Multithreading for faster data fetching on large queries
- In-memory caching for quicker iterative use and exploration
- Wildcard support in queries
- BYODF (Bring Your Own DataFrame): native return formats for use with `pandas` or `polars`
- API navigation tools: search for tables, browse the database, list contents, get metadata, and more

It has been tested with [Statistics Sweden](https://scb.se), [Statistics Finland](https://www.stat.fi), [Statistics Greenland](https://stat.gl) and [Statistics Norway](https://www.ssb.no).  

> [!NOTE] 
> Note that pxwebpy only supports version 2.0 of the PxWeb API.
