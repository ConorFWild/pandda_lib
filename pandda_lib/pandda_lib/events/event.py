from __future__ import annotations
from dataclasses import dataclass
from typing import *

from pandda_lib.pandda_lib.common import Dtag


@dataclass()
class Event:
    ...

    @staticmethod
    def from_mapping(mapping: Mapping) -> Event:
        ...
