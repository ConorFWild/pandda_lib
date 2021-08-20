from __future__ import annotations
from dataclasses import dataclass
from typing import *
from pathlib import Path

from pandda_lib.common import Dtag
from pandda_lib.events import Event


@dataclass()
class PanDDARuntime:
    ...

    @staticmethod
    def from_text(text: str) -> PanDDARuntime:
        ...


@dataclass()
class PanDDALog:
    ...

    @staticmethod
    def from_path(log_path: Path) -> PanDDALog:
        ...
