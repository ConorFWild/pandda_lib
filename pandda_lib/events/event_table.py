from __future__ import annotations
from dataclasses import dataclass
from typing import *
from pathlib import Path

from pandda_lib.common import Dtag
from pandda_lib.events import Event

@dataclass()
class PanDDAEventTable:
    events: Dict[(Dtag, int), Event]

    @staticmethod
    def from_path(path: Path) -> PanDDAEventTable:
        ...
