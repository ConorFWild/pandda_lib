from __future__ import annotations
from dataclasses import dataclass


@dataclass()
class Dtag:
    dtag: str

    @staticmethod
    def from_string(string: str) -> Dtag:
        ...
