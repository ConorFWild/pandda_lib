from __future__ import annotations
from dataclasses import dataclass
from typing import *
from pathlib import Path

from pandda_lib.common import Dtag, SystemName
from pandda_lib.events import Event


@dataclass
class XChemDiamondFS:
    model_building_dirs: Dict[SystemName, Path]
    pandda_dirs: Dict[SystemName, List[Path]]

    @staticmethod
    def from_path(xchem_diamond_dir: str = "/dls/labxchem/data"):
        xchem_diamond_dir = Path(xchem_diamond_dir)

        # Look for finished PanDDAs
        finished_pandda_mark_paths = list(xchem_diamond_dir.glob("*/processing/analysis/**/pandda.done"))
        print(finished_pandda_mark_paths)

        # FInished
        finished_pandda_dirs = [path.parent for path in finished_pandda_mark_paths]
        print(finished_pandda_dirs)

        system_names = [SystemName.from_pandda_dir(path) for path in finished_pandda_dirs]
        print(system_names)

        # Get model building dirs
        model_building_dirs_list = []
        for finished_pandda_dir in finished_pandda_dirs:
            # 0: / 1: dls 2: labxchem 3: data 4: year 5: code 6: processing 7: analysis
            analysis_dir = Path("/") / ("/".join(finished_pandda_dir.parts[1:8]))
            print(analysis_dir)
            model_dir_model_building = analysis_dir / "model_building"
            model_dir_initial_model = analysis_dir / "initial_model"

            if model_dir_model_building.exists():
                model_dir = model_dir_model_building
            elif model_dir_initial_model.exists():
                model_dir = model_dir_initial_model
            else:
                model_dir = None

            model_building_dirs_list.append(model_dir)

        model_building_dirs = {}
        pandda_dirs = {}
        for system_name, pandda_dir, model_building_dir in zip(system_names, finished_pandda_dirs,
                                                               model_building_dirs_list):
            if system_name not in pandda_dirs:
                pandda_dirs[system_name] = []
            pandda_dirs[system_name].append(pandda_dir)
            model_building_dirs[system_name] = model_building_dir

        return XChemDiamondFS(
            model_building_dirs,
            pandda_dirs,
        )

    # @staticmethod
    # def from_json_path(json_path: Path):
    #     ...
    #
    # def save_json(self, path):
    #     with open(path, "w") as f:
    #         json.dump()
