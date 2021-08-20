from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import re

from pandda_lib import constants
from pandda_lib.common import Dtag


@dataclass()
class SystemName:
    system_name: str

    @staticmethod
    def from_dtag(dtag: Dtag) -> SystemName:
        match = re.match(f"(([^-]+)-.*)", dtag.dtag)
        system_string = match.groups()[1]
        return SystemName(system_string)

    @staticmethod
    def from_pandda_dir(path: Path):
        processed_datasets_dir = path / constants.PANDDA_PROCESSED_DATASETS_DIR

        paths = processed_datasets_dir.glob("*")
        first_path = next(paths)
        print(first_path)

        dtag = Dtag(first_path.name)

        return SystemName.from_dtag(dtag)