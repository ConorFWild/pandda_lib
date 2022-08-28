import pathlib
import os

import numpy as np
import gemmi
import fire
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from pandda_lib import constants
from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs
from pandda_lib.fs.pandda_result import PanDDAResult
from pandda_lib.diamond_sqlite.diamond_sqlite import (Base, ProjectDirSQL, DatasetSQL, PanDDADirSQL,
                                                      PanDDADatasetSQL, PanDDABuildSQL, PanDDAEventSQL, SystemSQL, BoundStateModelSQL)
from pandda_lib.rscc import get_rscc
# from pandda_lib.custom_score import get_custom_score


def diamond_add_model_stats(sqlite_filepath, tmp_dir):
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)

    # Remove tables
    Base.metadata.create_all(engine)
    BoundStateModelSQL.__table__.drop(engine)
    Base.metadata.create_all(engine)

    systems = session.query(SystemSQL).order_by(SystemSQL.id).all()

    print("Updating database...")
    for system in systems:
        for project in system.projects:
            for dataset in project.datasets:
                dataset_dtag = dataset.dtag
                dataset_path = pathlib.Path(dataset.path)
                dataset_bound_state_model_path = pathlib.Path(dataset.model_path)

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
                # selected_custom_score = custom_scores[selected_rscc_id]

                bound_state_model = BoundStateModelSQL(
                    rscc=selected_rscc,
                    custom_score=None,
                )
                dataset.bound_state_model = bound_state_model

                session.add(bound_state_model)

                session.commit()


    print("Printing database datasets...")
    for instance in session.query(PanDDADirSQL).order_by(PanDDADirSQL.id):
        print(f"\t{instance.path}")
        print(f"\t\tNum datasets: {len(instance.pandda_dataset_results)}")
        num_events = sum(
            [
                len(_dataset.events)
                for _dataset
                in instance.pandda_dataset_results
            ]
        )
        print(f"\t\tNum events: {num_events}")
        num_builds = sum(
            [
                sum(
                    [
                        len(_event.builds)
                        for _event
                        in _dataset.events
                    ]
                )

                for _dataset
                in instance.pandda_dataset_results
            ]
        )
        print(f"\t\tNum builds: {num_builds}")

    # for instance in session.query(DatasetSQL).order_by(DatasetSQL):


if __name__ == "__main__":
    fire.Fire(diamond_add_model_stats)
