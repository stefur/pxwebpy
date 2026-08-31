from typing import Literal, TypeAlias

KnownApi: TypeAlias = Literal["scb", "ssb"]
"""Available APIs with a preconfigured URL.

Currently supported:

- `scb`: Statistics Sweden
- `ssb`: Statistics Norway
"""

Show: TypeAlias = Literal["code", "value", "code_value"]
"""How variables are presented.

Available options are:

- `code`: Only the code, e.g. "22"
- `value`: Only the value, e.g. "Västernorrlands län"
- `code_value`: Code and value presented together, e.g. "22: Västernorrlands län"
"""
