# pxwebpy
[![ci](https://github.com/stefur/pxwebpy/actions/workflows/ci.yml/badge.svg)](https://github.com/stefur/pxwebpy/actions/workflows/ci.yml)
[![Rye](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/mitsuhiko/rye/main/artwork/badge.json)](https://rye-up.com)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)   
Easily get data from the PxWeb API and into either a polars or pandas dataframe.  
  
Pxwebpy parses the PxWeb table data as well as metadata using the json-stat2 response format. 
  
It has been tested with [Statistics Sweden](https://scb.se), [Statistics Finland](https://www.stat.fi), [Statistics Greenland](https://stat.gl) and [Statistics Norway](https://www.ssb.no).  

## Basic usage
```python
import pandas as pd
import polars as pl

from pxwebpy import PxWeb

some_px_table = PxWeb(url, query)

pandas_df = pd.DataFrame(some_px_table.dataset)

polars_df = pl.DataFrame(some_px_table.dataset)
```

See [examples](examples/example.py) for more details on how to use pxwebpy.