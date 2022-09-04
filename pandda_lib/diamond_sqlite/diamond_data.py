import pathlib
import re
import os

import gemmi

from pandda_lib import constants
from pandda_lib.common import Dtag, SystemName


class SystemEventMap:
    def __init__(self, path):

        self.path = path

        matches = re.findall("event_([^_]+)_", path.name)
        if matches:
            self.event_idx = int(matches[0])
        else:
            self.event_idx = None

        matches = re.findall("BDC_([^_]+)_", path.name)
        if matches:
            self.bdc = float(matches[0])
        else:
            self.bdc = None


class DiamondDataset:
    def __init__(self, dtag, path):
        self.dtag = dtag
        self.path = path

        model_path = path / "dimple.pdb"
        mtz_path = path / "dimple.mtz"
        if model_path.exists():
            self.model_path = model_path
        else:
            self.model_path = None

        if mtz_path.exists():
            self.mtz_path = mtz_path
        else:
            self.mtz_path = None

        pandda_model_path = path / constants.PANDDA_EVENT_MODEL.format(dtag.dtag)

        if pandda_model_path.exists():
            # print(f"\tDataset {dtag} has model {pandda_model_path}, checking for ligs")
            st = gemmi.read_structure(str(pandda_model_path))
            sel = gemmi.Selection("(LIG)")
            num_ligs = 0
            for model in sel.models(st):
                # print('Model', model.name)
                for chain in sel.chains(model):
                    # print('-', chain.name)
                    for residue in sel.residues(chain):
                        num_ligs += 1

            # print(f"\t\tDataset num ligs: {num_ligs}")

            if num_ligs == 0:
                self.pandda_model_path = None
            else:
                self.pandda_model_path = pandda_model_path
        else:
            self.pandda_model_path = None

        self.event_maps = []
        for event_map_path in path.glob("*event*.ccp4"):
            system_event_map = SystemEventMap(event_map_path)
            if (system_event_map.bdc is not None) & (system_event_map.event_idx is not None):
                self.event_maps.append(
                    system_event_map
                )


class DiamondDataDir:
    def __init__(self, path):
        self.path = path
        self.datasets = {}
        for _dataset_dir in self.path.glob("*"):
            if _dataset_dir.is_dir():
                # try:
                _dataset_dtag = Dtag.from_name(_dataset_dir.name)
                if not _dataset_dtag:
                    continue
                _dataset = DiamondDataset(_dataset_dtag, _dataset_dir)
                self.datasets[_dataset_dtag] = _dataset
                # except:
                #     continue


class DiamondDataDirs:
    def __init__(self):
        xchem_data_path = pathlib.Path('/dls/labxchem/data')

        self.systems = {}

        for year_dir in xchem_data_path.glob('*'):
            print(f"Year: {year_dir.name}")
            for project_dir in year_dir.glob('*'):
                print(f"\tProject: {project_dir.name}")
                model_building_dir = project_dir / 'processing' / 'analysis' / 'model_building'

                initial_model_dir = project_dir / 'processing' / 'analysis' / 'initial_model'

                project = project_dir.name

                datasets_list = None
                data_dir_path = None
                # try:
                if os.access(model_building_dir, os.R_OK):
                    if model_building_dir.exists():
                        datasets_list = list(model_building_dir.glob('*'))
                        data_dir_path = model_building_dir

                if os.access(initial_model_dir, os.R_OK):
                    if initial_model_dir.exists():
                        datasets_list = list(initial_model_dir.glob('*'))
                        data_dir_path = initial_model_dir

                if not datasets_list:
                    print(f"\t\tNo dtags for dir: {data_dir_path}...")

                    continue

                # num_datasets = len(datasets_list)
                dtags = []
                for _dataset_dir in datasets_list:
                    # try:
                    _dtag = Dtag.from_name(_dataset_dir.name)
                    if _dtag:
                        dtags.append(_dtag)
                    # except:
                    #     continue

                if len(dtags) == 0:
                    print(f"\t\tNo dtags for dir: {data_dir_path}...")
                    continue

                system = max([
                    SystemName.from_dtag(dtag)
                    for dtag
                    in dtags
                ],
                    key=lambda _system_name: len(_system_name.system_name)
                )
                # print(f"{dtags[0]}: {system}: {datasets_list[0]}")

                if system not in self.systems:
                    self.systems[system] = {}

                print(f"\t\tSystem: {system.system_name}")

                data_dir = DiamondDataDir(data_dir_path)

                self.systems[system][project] = data_dir

                num_models = len([dataset for dataset in data_dir.datasets.values() if dataset.pandda_model_path])

                print(f"\t\tNum datasets is: {len(data_dir.datasets)}")
                print(f"\t\tNum models is: {num_models}")

                # except Exception as e:
                #     # print(e)
                #     continue

    def __getitem__(self, item):
        return self.systems[item]

    def __iter__(self):
        for system in self.systems:
            yield system

# class DiamondPanDDAResult
