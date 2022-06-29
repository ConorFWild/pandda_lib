import pathlib

from pandda_lib import constants
from pandda_lib.common import Dtag, SystemName


class DiamondDataset:
    def __init__(self, dtag, path):
        self.dtag = dtag
        self.path = path

        _model_path = path / constants.PANDDA_EVENT_MODEL.format(dtag.dtag)

        if _model_path.exists():
            self.model_path = _model_path
        else:
            self.model_path = None


class DiamondDataDir:
    def __init__(self, path):
        self.path = path
        self.datasets = {}
        for _dataset_dir in self.path.glob("*"):
            if _dataset_dir.is_dir():
                try:
                    _dataset_dtag = Dtag.from_name(_dataset_dir.name)
                    _dataset = DiamondDataset(_dataset_dtag, _dataset_dir)
                    self.datasets[_dataset_dtag] = _dataset
                except:
                    continue


class DiamondDataDirs:
    def __init__(self):
        xchem_data_path = pathlib.Path('/dls/labxchem/data')

        self.systems = {}

        for year_dir in xchem_data_path.glob('*'):
            for project_dir in year_dir.glob('*'):
                model_building_dir = project_dir / 'processing' / 'analysis' / 'model_building'

                initial_model_dir = project_dir / 'processing' / 'analysis' / 'initial_model'

                try:

                    if model_building_dir.exists():
                        datasets_list = list(model_building_dir.glob('*'))
                        data_dir_path = model_building_dir

                    else:
                        datasets_list = list(initial_model_dir.glob('*'))
                        data_dir_path = initial_model_dir

                    # num_datasets = len(datasets_list)
                    dtag = Dtag.from_name(datasets_list[0].name)
                    system = SystemName.from_dtag(dtag)
                    self.systems[system] = DiamondDataDir(data_dir_path)

                except:
                    continue

    def __getitem__(self, item):
        return self.systems[item]

    def __iter__(self):
        for system in self.systems:
            yield system
