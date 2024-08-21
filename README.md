# pxwebpy
[![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/stefur/pxwebpy/ci.yml?style=flat-square&label=ci)](https://github.com/stefur/pxwebpy/actions/workflows/ci.yml)
![PyPI - Version](https://img.shields.io/pypi/v/pxwebpy?style=flat-square)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pxwebpy?style=flat-square)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json&style=flat-square)](https://github.com/astral-sh/uv)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json&style=flat-square)](https://github.com/astral-sh/ruff)   
Easily get data from the PxWeb API and into either a polars or pandas dataframe.  
  
Pxwebpy parses the PxWeb table data as well as metadata using the json-stat2 response format. 
  
It has been tested with [Statistics Sweden](https://scb.se), [Statistics Finland](https://www.stat.fi), [Statistics Greenland](https://stat.gl) and [Statistics Norway](https://www.ssb.no).  

## Example usage
```python
>>> from pxwebpy.table import PxTable
>>> import pandas as pd

# Create a table object, setting up a URL for a table
>>> tbl = PxTable(url="https://api.scb.se/OV0104/v1/doris/sv/ssd/START/HE/HE0110/HE0110A/SamForvInk1")

# Check out the table variables that we can use
>>> tbl.get_table_variables()
{'region': ['Riket',
  'Stockholms län',
  'Upplands Väsby',
  'Vallentuna',
...
  'Piteå',
  'Haparanda',
  'Kiruna'],
 'kön': ['män', 'kvinnor', 'totalt'],
 'ålder': ['totalt 16+ år',
  '16-19 år',
  'totalt 20+ år',
  '20-64 år',
  '65+ år',
  '20-24 år',
  '25-29 år',
...
  '75-79 år',
  '80-84 år',
  '85+ år'],
 'inkomstklass': ['totalt',
  '0',
  '1-19 tkr',
  '20-39 tkr',
...
  '600-799 tkr',
  '800-999 tkr',
  '1000+ tkr'],
 'tabellinnehåll': ['Medelinkomst, tkr',
  'Medianinkomst, tkr',
  'Totalsumma, mnkr',
  'Antal personer'],
 'år': ['1999',
  '2000',
...
  '2021',
  '2022']}

# Construct a query using a selection of variables we're interested in
>>> tbl.create_query({"tabellinnehåll": ["Medianinkomst, tkr"], "ålder": ["totalt 16+ år"]})

# Now we can get the data
>>> tbl.get_data()

>>> tbl

PxTable(url='https://api.scb.se/OV0104/v1/doris/sv/ssd/START/HE/HE0110/HE0110A/SamForvInk1',
        query={'query': [{'code': 'ContentsCode', 'selection': {'filter': 'item', 'values': ['HE0110J8']}}, {'code': 'Alder', 'selection': {'filter': 'item', 'values': ['tot16+']}}], 'response': {'format': 'json-stat2'}},
        metadata={'label': 'Sammanräknad förvärvsinkomst, medianinkomst för boende i Sverige hela året, tkr efter ålder, tabellinnehåll och år', 'note': None, 'source': 'SCB', 'updated': '2024-01-12T05:52:00Z'},
        fetched=2024-06-16 10:30:34.085020,
        dataset=[{'ålder': 'totalt 16+ år', 'tabellinnehåll': 'Medianinkomst, tkr', 'år': '1999', 'value': 159.4}, {'ålder': 'totalt 16+ år', 'tabellinnehåll': 'Medianinkomst, tkr', 'år': '2000', 'value': 165.3}, ...])

# Using the dataset we can then create a Pandas dataframe
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