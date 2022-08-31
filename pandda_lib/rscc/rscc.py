import pathlib
import os

import numpy as np
import gemmi
import fire
from sqlalchemy.orm import sessionmaker, subqueryload
from sqlalchemy import create_engine
# import joblib
# from joblib import Parallel, delayed
import multiprocessing as mp


from pandda_lib import constants
from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs
from pandda_lib.fs.pandda_result import PanDDAResult
from pandda_lib.diamond_sqlite.diamond_sqlite import (Base, ProjectDirSQL, DatasetSQL, PanDDADirSQL,
                                                      PanDDADatasetSQL, PanDDABuildSQL, PanDDAEventSQL, SystemSQL,
                                                      BoundStateModelSQL, SystemEventMapSQL)
from pandda_lib.rscc import get_rscc
# from pandda_lib.rscc.rscc import get_dataset_rsccs

# from pandda_lib.custom_score import get_custom_score

class Runner:
    def __call__(self, f):
        return f()


class GetDatasetRSCC:
    def __init__(self, dataset_dtag, dataset_path, dataset_bound_state_model_path, event_maps, mtz_path, tmp_dir):
        self.dataset_dtag = dataset_dtag
        self.dataset_path = dataset_path
        self.dataset_bound_state_model_path = dataset_bound_state_model_path
        self.event_maps = event_maps
        self.mtz_path = mtz_path
        self.tmp_dir = tmp_dir

    def __call__(self, ):
        return get_dataset_rsccs(
            self.dataset_dtag, self.dataset_path, self.dataset_bound_state_model_path, self.event_maps, self.mtz_path,
            self.tmp_dir
        )


def get_dataset_rsccs(dataset_dtag, dataset_path, dataset_bound_state_model_path, event_maps, mtz_path, tmp_dir):
    try:
        dataset_dtag = dataset_dtag
        dataset_path = pathlib.Path(dataset_path)
        dataset_bound_state_model_path = pathlib.Path(dataset_bound_state_model_path)

        if not tmp_dir.exists():
            os.mkdir(tmp_dir)

        # event_maps = dataset.event_maps
        if not pathlib.Path(mtz_path).exists():
            return None
        resolution = gemmi.read_mtz_file(mtz_path).resolution_high()

        rsccs = {}
        for event_map in event_maps:
            rscc = get_rscc(
                dataset_bound_state_model_path,
                event_map.path,
                resolution,
                tmp_dir
            )
            print(rscc)

            if not rscc:
                continue

            for chain_res, _rscc in rscc.items():
                # rsccs.append(_rscc)
                rsccs[(event_map.path, chain_res[0], chain_res[1])] = _rscc

        # # Get the custom scores
        # custom_scores = {}
        # for event_map in event_maps:
        #     custom_score = get_custom_score(
        #         dataset_bound_state_model_path,
        #         event_map.path,
        #         resolution,
        #     )
        #     for chain_res, _custom_score in custom_score:
        #     #     custom_scores.append(_custom_score)
        #         custom_scores[(event_map.path, chain_res[0], chain_res[1])] = _custom_score

        if len(rsccs) == 0:
            return None

        # selected_rscc_index = np.argmax(rsccs)
        selected_rscc_id = max(rsccs, key=lambda _key: rsccs[_key])
        selected_rscc = rsccs[selected_rscc_id]

        return selected_rscc

    except Exception as e:
        raise Exception(f"Exception occured for dtag {dataset_dtag} model {dataset_bound_state_model_path}:\n{str(e)}")
