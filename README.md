# pxwebpy
Get data from PxWeb API.

```python
from pxwebpy import PxWeb

some_data = PxWeb(url, query)

some_data_dict = some_data.to_dict()
```

See examples for more details.

**Todo**
- Support for more response formats
- Build query directly from a dict input
- Timestamp for tracking last data refresh
- Description metadata for data table
- Tests
