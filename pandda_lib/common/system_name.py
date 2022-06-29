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
        # match = re.match(f"(([^-]+)-.*)", dtag.dtag)
        # print(match)
        match = re.match(
            f"(^(.+)-[^0-9]+[0-9]+)",
            dtag.dtag,
        )

        system_string = match.groups()[1]
        # print(system_string)
        return SystemName(system_string)

    @staticmethod
    def from_pandda_dir(path: Path):
        processed_datasets_dir = path / constants.PANDDA_PROCESSED_DATASETS_DIR
        print(processed_datasets_dir)
        paths = processed_datasets_dir.glob("*")
        for dtag_path in paths:
            try:
                print(dtag_path)
                dtag = Dtag(dtag_path.name)
                return SystemName.from_dtag(dtag)
            except:
                continue

        raise Exception("Could not match any dtags in dir")

    def __hash__(self):
        return hash(self.system_name)
