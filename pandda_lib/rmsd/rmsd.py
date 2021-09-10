from __future__ import annotations
from dataclasses import dataclass
from typing import *
from pathlib import Path

from pandda_lib.common import Dtag
from pandda_lib.events import Event

@dataclass()
class RMSD:
    ...

    @staticmethod
    def from_structures(structure_1, structure_2):
        ...