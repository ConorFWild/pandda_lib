from __future__ import annotations
from dataclasses import dataclass
import re


@dataclass()
class Dtag:
    dtag: str

    @staticmethod
    def from_string(string: str) -> Dtag:
        ...

    @staticmethod
    def from_name(string: str) -> Dtag:
        # match = re.search("[^-]+-[^0-9]+[0-9]+", string)
        match = re.search("^.+-[^0-9]+[0-9]+", string)
        if match:

            return Dtag(match.group())
        else:
            return None

    def __hash__(self):
        return hash(self.dtag)
