# pxwebpy
Helper class to get data from PxWeb API.

```python
import pandas as pd
import polars as pl

from pxwebpy import PxWeb

some_px_table = PxWeb(url, query)

pandas_df = pd.DataFrame(some_px_table.dataset)

polars_df = pl.DataFrame(some_px_table.dataset)
```

See examples for more details.

**Todo**
- Support for more response formats
- Build query directly from a dict input
- Timestamp for tracking last data refresh
- Description metadata for data table
