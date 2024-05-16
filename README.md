# pxwebpy
[![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/stefur/pxwebpy/ci.yml?style=flat-square&label=ci)](https://github.com/stefur/pxwebpy/actions/workflows/ci.yml)
![PyPI - Version](https://img.shields.io/pypi/v/pxwebpy?style=flat-square)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pxwebpy?style=flat-square)
[![Rye](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/mitsuhiko/rye/main/artwork/badge.json&style=flat-square)](https://rye-up.com)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json&style=flat-square)](https://github.com/astral-sh/ruff)   
Easily get data from the PxWeb API and into either a polars or pandas dataframe.  
  
Pxwebpy parses the PxWeb table data as well as metadata using the json-stat2 response format. 
  
It has been tested with [Statistics Sweden](https://scb.se), [Statistics Finland](https://www.stat.fi), [Statistics Greenland](https://stat.gl) and [Statistics Norway](https://www.ssb.no).  

## Example usage
```python
>>> from pxwebpy.table import PxTable
>>> import pandas as pd

>>> URL = "https://api.scb.se/OV0104/v1/doris/sv/ssd/START/HE/HE0110/HE0110A/SamForvInk1"

>>> QUERY = {
            "query": [
                {
                "code": "Tid",
                "selection": {
                    "filter": "item",
                    "values": [
                    "2021"
                    ]
                }
                }
            ],
            "response": {
                "format": "json-stat2"
            }
        }

>>> tbl = PxTable(URL, QUERY)
>>> print(tbl)

PxTable(url='https://api.scb.se/OV0104/v1/doris/sv/ssd/START/HE/HE0110/HE0110A/SamForvInk1',
    query={'query': [{'code': 'Tid', 'selection': {'filter': 'item', 'values': ['2021']}}], 'response': {'format': 'json-stat2'}},
    metadata={'label': 'Sammanräknad förvärvsinkomst för boende i Sverige hela året efter ålder, tabellinnehåll och år', 'source': 'SCB', 'updated': '2023-01-10T10:42:00Z'},
    last_refresh=2023-10-29 14:21:57.628639,
    dataset=[{'ålder': 'totalt 16+ år', 'tabellinnehåll': 'Medelinkomst, tkr', 'år': '2021' ...

>>> df = pd.DataFrame(tbl.dataset)
>>> print(df)
            ålder      tabellinnehåll    år      value
0   totalt 16+ år   Medelinkomst, tkr  2021      331.5
1   totalt 16+ år  Medianinkomst, tkr  2021      301.5
2   totalt 16+ år    Totalsumma, mnkr  2021  2779588.9
3   totalt 16+ år      Antal personer  2021  8383640.0
4        16-19 år   Medelinkomst, tkr  2021       28.1
..            ...                 ...   ...        ...
71       80-84 år      Antal personer  2021   290684.0
72         85+ år   Medelinkomst, tkr  2021      214.4
73         85+ år  Medianinkomst, tkr  2021      200.1
74         85+ år    Totalsumma, mnkr  2021    57529.3
75         85+ år      Antal personer  2021   268320.0

[76 rows x 4 columns]
```

See [examples](examples/example.py) for more details on how to use pxwebpy.