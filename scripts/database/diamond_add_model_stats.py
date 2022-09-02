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


def diamond_add_model_stats(sqlite_filepath, tmp_dir, cpus=3):
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    tmp_dir = pathlib.Path(tmp_dir).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)

    # Remove tables
    Base.metadata.create_all(engine)
    BoundStateModelSQL.__table__.drop(engine)
    Base.metadata.create_all(engine)

    initial_datasets = session.query(DatasetSQL).options(subqueryload(DatasetSQL.event_maps)).order_by(
        DatasetSQL.id).all()
    print(len(initial_datasets))

    print("Updating database...")
    datasets = []
    datasets_without_pandda_models = []
    # for system in systems:
    #     for project in system.projects:
    for dataset in initial_datasets:
        print(dataset.pandda_model_path)
        if dataset.pandda_model_path != "None":
            datasets.append(dataset)
        else:
            datasets_without_pandda_models.append(dataset)
            # selected_custom_score = custom_scores[selected_rscc_id]
    print(f"\tNumber of datasets to score: {len(datasets)}; number not to: {len(datasets_without_pandda_models)}")

    print("Getting RSCCs...")
    # selected_rsccs = Parallel(
    #     n_jobs=24,
    #     verbose=50,
    # )(
    #     delayed(get_dataset_rsccs)(
    #         dataset.dtag,
    #         dataset.path,
    #         dataset.pandda_model_path,
    #         dataset.event_maps,
    #         dataset.mtz_path,
    #         tmp_dir / dataset.dtag
    #     )
    #     for dataset
    #     in datasets
    # )
    try:
        mp.set_start_method('spawn')
    except Exception as e:
        print(e)

    with mp.Pool(cpus) as p:
        print("Getting run set")
        run_set = [
            GetDatasetRSCC(dataset.dtag,
                           dataset.path,
                           dataset.pandda_model_path,
                           dataset.event_maps,
                           dataset.mtz_path,
                           tmp_dir / dataset.dtag)
            for dataset
            in datasets
        ]
        print("Running")
        selected_rsccs = p.map(
            Runner(),
            run_set
        )

    print("Inserting to database...")
    for dataset, selected_rscc in zip(datasets, selected_rsccs):
        bound_state_model = BoundStateModelSQL(
            rscc=selected_rscc,
            custom_score=None,
        )
        dataset.bound_state_model = bound_state_model

        session.add(bound_state_model)

    session.commit()

    print("Printing database datasets...")
    for instance in session.query(DatasetSQL).order_by(DatasetSQL.id):
        print(f"{instance.dtag}")
        if instance.bound_state_model:
            print(f"\t{instance.bound_state_model.rscc}")

    # for instance in session.query(DatasetSQL).order_by(DatasetSQL):


if __name__ == "__main__":
    fire.Fire(diamond_add_model_stats)
