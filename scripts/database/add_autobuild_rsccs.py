import dataclasses
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
from pandda_lib.diamond_sqlite.diamond_sqlite import *
from pandda_lib.rscc import get_rscc
from pandda_lib.rscc.rscc import GetDatasetRSCC, Runner


@dataclasses.dataclass
class EventMap:
    path: str


def diamond_add_autobuild_rsccs(sqlite_filepath, tmp_dir, cpus=3):
    try:
        mp.set_start_method('spawn')
    except Exception as e:
        print(e)
    with mp.Pool(cpus) as p:
        sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
        tmp_dir = pathlib.Path(tmp_dir).resolve()
        # tmp_dir = pathlib.Path(tmp_dir).resolve()
        engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
        session = sessionmaker(bind=engine)()
        Base.metadata.create_all(engine)

        # Remove tables
        Base.metadata.create_all(engine)
        BuildRSCCSQL.__table__.drop(engine)
        Base.metadata.create_all(engine)

        # Get Autobuild PanDDA sqls
        run_set = {}
        sqls = {}

        # Construct the jobs
        for pandda_2 in session.query(PanDDADirSQL).options(subqueryload("*")).order_by(PanDDADirSQL.id).all():
            system = pandda_2.system
            project = pandda_2.project
            print(f"PanDDA 2: {system.system_name}: {project.project_name}: {project.path}")

            print(f"\t{len(pandda_2.pandda_dataset_results)}")
            for pandda_dataset in pandda_2.pandda_dataset_results:

                for event in pandda_dataset.events:
                    for build in event.builds:
                        dataset_dtag = pandda_dataset.dtag
                        dataset_path = pandda_dataset.path
                        dataset_bound_state_model_path = build.build_path
                        event_maps = [EventMap(event.event_map_path), ]  # Only need the one that the build came from
                        mtz_path = pandda_dataset.input_mtz_path
                        build_tmp_dir = tmp_dir / f"{system.system_name}_{project.project_name}" \
                                                  f"_{pandda_dataset.dtag}_" \
                                                  f"{event.idx}_{build.id}"
                        build_to_run = GetDatasetRSCC(
                            dataset_dtag,
                            dataset_path,
                            dataset_bound_state_model_path,
                            event_maps,
                            mtz_path,
                            build_tmp_dir,
                        )

                        run_set[(system.system_name, project.project_name, pandda_dataset.dtag,
                                 event.idx, build.id)] = build_to_run
                        sqls[(system.system_name, project.project_name, pandda_dataset.dtag,
                              event.idx, build.id)] = {
                            # "System": system,
                            # "Project": project,
                            # "PanDDA": pandda_2,
                            # "Dataset": pandda_dataset,
                            # "Event": event,
                            "Build": build
                        }

        print(f"Number of builds to score: {len(run_set)};")

        print("Getting RSCCs...")

        print("Getting run set")

        print("Running")
        selected_rsccs = p.map(
            Runner(),
            run_set.values(),
            chunksize=10
        )

    print("Inserting to database...")
    for run_key, selected_rscc in zip(run_set, selected_rsccs):
        build_rscc_sql = BuildRSCCSQL(
            score=selected_rscc,
            # broken_ligand=selected_rmsd['broken_ligand'],
            # alignment_error=selected_rmsd['alignment_error'],
            # # closest_event=selected_rmsd['closest_event'],
            # closest_rmsd=selected_rmsd['closest_rmsd'],  # None and num_events>0&num_builds>0 implies broken ligand
            # high_confidence=selected_rmsd['high_confidence']
        )
        build_sql = sqls[run_key]["Build"]
        build_sql.rscc = build_rscc_sql

        session.add(build_rscc_sql)

    session.commit()

    print("Printing database datasets...")
    for instance in session.query(BuildRSCCSQL).order_by(BuildRSCCSQL.id):
        print(f"{instance.score}")


if __name__ == "__main__":
    fire.Fire(diamond_add_autobuild_rsccs)
