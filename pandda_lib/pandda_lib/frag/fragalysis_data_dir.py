from __future__ import annotations
from dataclasses import dataclass
from typing import *
from pathlib import Path
import os
import re

from fragalysis_api.xcextracter.xcextracter import xcextracter
from fragalysis_api.xcextracter.getdata import GetTargetsData, GetPdbData

from pandda_lib.pandda_lib.common import Dtag, SystemName
from pandda_lib.pandda_lib.events import Event


@dataclass()
class FragalysisDataDir:
    fragalysis_data_dir: Path
    system_dir_paths: Dict[SystemName, Path]
    system_dirs: Dict[SystemName, Dict[Dtag, Path]]
    all_model_paths: Dict[Dtag, Path]
    ...

    @staticmethod
    def from_path(fragalysis_data_dir: Path):
        system_dir_paths = {SystemName.from_string(path.name): path for path in fragalysis_data_dir.glob("*")}
        system_dirs = {}
        for system_name, system_dir_path in system_dirs.items():
            system_dirs[system_name] = {Dtag.from_string(path.name): path for path in system_dir_path.rglob("*.pdb")}
        model_paths = {Dtag.from_string(path.name): path for path in fragalysis_data_dir.rglob("*.pdb")}

        return FragalysisDataDir(
            fragalysis_data_dir,
            system_dir_paths,
            system_dirs,
            model_paths,
        )

    @staticmethod
    def populate(results_dir: Path, system_name):
        summary = xcextracter(system_name.system_name)

        for index, row in summary.iterrows():
            prot_id = row["protein_code"]
            pdb_grabber = GetPdbData()

            print(prot_id)
            match = re.match(r"(([^_]+)_([^:]+))", prot_id)
            print(match)
            print(match.groups())
            all, dtag, num = match.groups()

            # use our selected code to pull out the pdb file (currently the file with ligand removed)
            print(prot_id)
            try:
                print("Got bound pdb block")
                if not results_dir.exists():
                    os.mkdir(results_dir)

                dtag_dir = results_dir / dtag
                if not dtag_dir.exists():
                    os.mkdir(dtag_dir)

                pdb_file = dtag_dir / num
                if not pdb_file.exists():
                    pdb_block = pdb_grabber.get_bound_pdb_file(prot_id)

                    with open(pdb_file, "w") as f:
                        f.write(pdb_block)

            except Exception as e:
                print(e)
