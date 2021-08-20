from __future__ import annotations
from dataclasses import dataclass
from typing import *
from pathlib import Path

from pandda_lib.pandda_lib.common import Dtag
from pandda_lib.pandda_lib.events import Event


@dataclass()
class Ligands:
    structures: List[Any]

    @staticmethod
    def from_structure(struc: Any):
        ...
