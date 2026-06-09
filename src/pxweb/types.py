from typing import Literal, TypeAlias

KnownApi: TypeAlias = Literal["scb", "ssb"]
"""Selectable APIs with a preconfigured URL"""

Show: TypeAlias = Literal["code", "value", "code_value"]
"""How variables are presented"""
