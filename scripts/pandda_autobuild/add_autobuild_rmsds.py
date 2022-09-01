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
from pandda_lib.rscc.rscc import GetDatasetRSCC, Runner


# from pandda_lib.custom_score import get_custom_score


def diamond_add_model_stats(sqlite_filepath, tmp_dir):
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    tmp_dir = pathlib.Path(tmp_dir).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)

    # Remove tables
    Base.metadata.create_all(engine)
    # BoundStateModelSQL.__table__.drop(engine)
    # Base.metadata.create_all(engine)

    # Get Autobuild PanDDA sqls
    run_set = {

    }
    sqls = {}
    for system in session.query(SystemSQL).options(
            subqueryload(
                SystemSQL.projects
            ).subqueryload(
                ProjectDirSQL.datasets
            ).subqueryload(
                DatasetSQL.event_maps
            ).subqueryload(
                SystemEventMapSQL.event_map_quantiles
            )).order_by(SystemSQL.id).all():
        for project in system.projects:
            for pandda_2 in project.pandda_2s:
                for pandda_dataset in pandda_2.pandda_dataset_results:
                    for event in pandda_dataset.events:
                        for build in event.builds:
                            build_to_run = GetDatasetRSCC(
                                pandda_dataset.dtag,
                                pandda_dataset.path,
                                build.path,
                                event.event_map_path,
                                pandda_dataset.input_mtz_path,
                                tmp_dir / f"{pandda_dataset.dtag}_{event.idx}_{build.id}",
                            )

                            run_set[(system.system_name, project.project_name, pandda_dataset.dtag,
                                     event.event_idx, build.id)] = build_to_run
                            sqls[(system.system_name, project.project_name, pandda_dataset.dtag,
                                     event.event_idx, build.id)] = {
                                "build": build
                            }

    print(f"\tNumber of builds to score: {len(run_set)};")

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
    mp.set_start_method('spawn')

    with mp.Pool(30) as p:
        print("Getting run set")

        print("Running")
        selected_rsccs = p.map(
            Runner(),
            run_set.values()
        )

    print("Inserting to database...")
    for run_key, selected_rscc in zip(run_set, selected_rsccs):
        bound_state_model = BuildScoreSQL(
            rscc=selected_rscc,
            custom_score=None,
        )
        sqls[]
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
