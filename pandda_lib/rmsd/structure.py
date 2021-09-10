from __future__ import annotations
from dataclasses import dataclass
from typing import *
from pathlib import Path

import gemmi

from pandda_lib.common import Dtag
from pandda_lib.events import Event

@dataclass()
class Structure:
    structure: Any

    @staticmethod
    def from_path(path: Path) -> Structure:
        struc = gemmi.read_structure(str(path))
        return Structure(struc)