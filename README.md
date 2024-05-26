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

>>> tbl = PxTable(url="https://api.scb.se/OV0104/v1/doris/sv/ssd/START/HE/HE0110/HE0110A/SamForvInk1")

>>> tbl.create_query({"tabellinnehåll": ["Medianinkomst, tkr"], "ålder": ["totalt 16+ år"]})

>>> tbl.get_data()

>>> print(tbl)

PxTable(url='https://api.scb.se/OV0104/v1/doris/sv/ssd/START/HE/HE0110/HE0110A/SamForvInk1',
    query={'query': [{'code': 'Tid', 'selection': {'filter': 'item', 'values': ['2021']}}], 'response': {'format': 'json-stat2'}},
    metadata={'label': 'Sammanräknad förvärvsinkomst för boende i Sverige hela året efter ålder, tabellinnehåll och år', 'source': 'SCB', 'updated': '2023-01-10T10:42:00Z'},
    fetched=2023-10-29 14:21:57.628639,
    dataset=[{'ålder': 'totalt 16+ år', 'tabellinnehåll': 'Medelinkomst, tkr', 'år': '2021' ...

>>> df = pd.DataFrame(tbl.dataset)
>>> print(df)

          ålder      tabellinnehåll    år  value
0   totalt 16+ år  Medianinkomst, tkr  1999  159.4
1   totalt 16+ år  Medianinkomst, tkr  2000  165.3
2   totalt 16+ år  Medianinkomst, tkr  2001  172.4
3   totalt 16+ år  Medianinkomst, tkr  2002  179.4
4   totalt 16+ år  Medianinkomst, tkr  2003  185.1
5   totalt 16+ år  Medianinkomst, tkr  2004  189.4
6   totalt 16+ år  Medianinkomst, tkr  2005  192.9
7   totalt 16+ år  Medianinkomst, tkr  2006  198.8
8   totalt 16+ år  Medianinkomst, tkr  2007  206.2
9   totalt 16+ år  Medianinkomst, tkr  2008  215.1
10  totalt 16+ år  Medianinkomst, tkr  2009  218.7
11  totalt 16+ år  Medianinkomst, tkr  2010  219.7
12  totalt 16+ år  Medianinkomst, tkr  2011  225.0
13  totalt 16+ år  Medianinkomst, tkr  2012  233.7
14  totalt 16+ år  Medianinkomst, tkr  2013  240.5
15  totalt 16+ år  Medianinkomst, tkr  2014  244.8
16  totalt 16+ år  Medianinkomst, tkr  2015  253.7
17  totalt 16+ år  Medianinkomst, tkr  2016  263.9
18  totalt 16+ år  Medianinkomst, tkr  2017  272.0
19  totalt 16+ år  Medianinkomst, tkr  2018  279.8
20  totalt 16+ år  Medianinkomst, tkr  2019  286.8
21  totalt 16+ år  Medianinkomst, tkr  2020  291.9
22  totalt 16+ år  Medianinkomst, tkr  2021  301.5
23  totalt 16+ år  Medianinkomst, tkr  2022  316.6
```

See [examples](examples/example.py) for more details on how to use pxwebpy.