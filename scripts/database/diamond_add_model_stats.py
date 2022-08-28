import pathlib
import os

import numpy as np
import gemmi
import fire
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import joblib
from joblib import Parallel, delayed

from pandda_lib import constants
from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs
from pandda_lib.fs.pandda_result import PanDDAResult
from pandda_lib.diamond_sqlite.diamond_sqlite import (Base, ProjectDirSQL, DatasetSQL, PanDDADirSQL,
                                                      PanDDADatasetSQL, PanDDABuildSQL, PanDDAEventSQL, SystemSQL,
                                                      BoundStateModelSQL)
from pandda_lib.rscc import get_rscc


# from pandda_lib.custom_score import get_custom_score


def get_dataset_rsccs(dataset, tmp_dir):
    dataset_dtag = dataset.dtag
    dataset_path = pathlib.Path(dataset.path)
    dataset_bound_state_model_path = pathlib.Path(dataset.model_path)

    if not tmp_dir.exists():
        os.mkdir(tmp_dir)

    event_maps = dataset.event_maps
    resolution = gemmi.read_mtz_file(dataset.mtz_path).resolution_high()

    rsccs = {}
    for event_map in event_maps:
        rscc = get_rscc(
            dataset_bound_state_model_path,
            event_map.path,
            resolution,
            tmp_dir
        )
        for chain_res, _rscc in rscc:
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

    # selected_rscc_index = np.argmax(rsccs)
    selected_rscc_id = max(rsccs, key=lambda _key: rsccs[_key])
    selected_rscc = rsccs[selected_rscc_id]

    return selected_rscc


def diamond_add_model_stats(sqlite_filepath, tmp_dir):
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    tmp_dir = pathlib.Path(sqlite_filepath).resolve(tmp_dir)
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)

    # Remove tables
    Base.metadata.create_all(engine)
    BoundStateModelSQL.__table__.drop(engine)
    Base.metadata.create_all(engine)

    initial_datasets = session.query(DatasetSQL).join(DatasetSQL.bound_state_model).order_by(DatasetSQL.id).all()
    print(len(initial_datasets))

    print("Updating database...")
    datasets = []
    # for system in systems:
    #     for project in system.projects:
    for dataset in initial_datasets:
        if dataset.bound_state_model:
            datasets.append(dataset)
                # selected_custom_score = custom_scores[selected_rscc_id]
    print(f"\tNumber of datasets to score: {len(datasets) }")

    print("Getting RSCCs...")
    selected_rsccs = Parallel(n_jobs=30,
                              verbose=50)(delayed(get_dataset_rsccs)(dataset, tmp_dir / dataset.dtag) for dataset in
                                         datasets)

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
        print(f"\t{instance.bound_state_model.rscc}")

    # for instance in session.query(DatasetSQL).order_by(DatasetSQL):


if __name__ == "__main__":
    fire.Fire(diamond_add_model_stats)
