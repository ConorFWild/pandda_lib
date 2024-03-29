import pathlib
import os
from typing import *

import fire
from sqlalchemy.orm import sessionmaker, subqueryload
from sqlalchemy import create_engine

from pandda_lib import constants
from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs
from pandda_lib.fs.pandda_result import PanDDAResult
from pandda_lib.diamond_sqlite.diamond_sqlite import (Base, ProjectDirSQL, DatasetSQL, PanDDADirSQL,
                                                      PanDDADatasetSQL, PanDDABuildSQL, PanDDAEventSQL, SystemSQL)


def get_pandda_2_result(pandda_path) -> Optional[PanDDADirSQL]:
    if not (pandda_path / constants.PANDDA_ANALYSES_DIR / constants.PANDDA_ANALYSE_EVENTS_FILE).exists():
        print(f"\t\tNo PanDDA event table!")
        return None

    pandda_result = PanDDAResult.from_dir(pandda_path)

    dataset_results = {}
    for dtag, dataset in pandda_result.processed_datasets.items():
        dataset_events = {}
        # print(len(dataset.events.items()))
        for event_id, event in dataset.events.items():
            event_builds = {}

            for build_id, build in event.build_results.items():
                event_builds[build_id] = PanDDABuildSQL(
                    build_path=str(build.path),
                )

            dataset_events[event_id] = PanDDAEventSQL(
                event_map_path=str(event.event_map_path),
                builds=[
                    _build
                    for _build
                    in event_builds.values()
                ],
            )

        dataset_results[dtag] = PanDDADatasetSQL(
            dtag=dtag.dtag,
            path=str(dataset.path),
            events=[
                _event
                for _event
                in dataset_events.values()
            ],
        )

    pandda_result_sql = PanDDADirSQL(
        path=str(pandda_path),
        pandda_dataset_results=[
            _dataset_sql
            for _dataset_sql
            in dataset_results.values()
        ]
    )

    return pandda_result_sql

def diamond_add_pandda_results(sqlite_filepath, output_dir_name):
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)

    # Remove tables
    PanDDADirSQL.__table__.drop(engine)
    PanDDADatasetSQL.__table__.drop(engine)
    PanDDAEventSQL.__table__.drop(engine)
    PanDDABuildSQL.__table__.drop(engine)

    Base.metadata.create_all(engine)

    systems = session.query(SystemSQL).options(subqueryload("*")).order_by(SystemSQL.id).all()
    dtag_to_dataset_sql = {}
    for system in systems:
        for project in system.projects:
            for dataset in project.datasets:
                dtag_to_dataset_sql[dataset.dtag] = dataset

    print("Updating database...")
    for system in systems:
        for project in system:
            _pandda_dir = pathlib.Path(project.path).parent / output_dir_name
            print(f"\tAnalysing run at: {_pandda_dir}")

            if not (_pandda_dir / constants.PANDDA_ANALYSES_DIR / constants.PANDDA_ANALYSE_EVENTS_FILE).exists():
                print(f"\t\tNo PanDDA event table!")
                continue

            pandda_result = PanDDAResult.from_dir(_pandda_dir)

            dataset_results = {}
            for dtag, dataset in pandda_result.processed_datasets.items():
                dataset_events = {}
                # print(len(dataset.events.items()))
                for event_id, event in dataset.events.items():
                    event_builds = {}

                    for build_id, build in event.build_results.items():
                        event_builds[build_id] = PanDDABuildSQL(
                            build_path=str(build.path),
                        )

                    dataset_events[event_id] = PanDDAEventSQL(
                        event_map_path=str(event.event_map_path),
                        builds=[
                            _build
                            for _build
                            in event_builds.values()
                        ],
                    )
                if dtag.dtag in dtag_to_dataset_sql:
                    dataset_sql = dtag_to_dataset_sql[dtag.dtag]
                else:
                    dataset_sql = None
                dataset_results[dtag] = PanDDADatasetSQL(
                    dtag=dtag.dtag,
                    path=str(dataset.path),
                    events=[
                        _event
                        for _event
                        in dataset_events.values()
                    ],
                    dataset_sql=dataset_sql

                )

            pandda_result_sql = PanDDADirSQL(
                path=str(_pandda_dir),
                pandda_dataset_results=[
                    _dataset_sql
                    for _dataset_sql
                    in dataset_results.values()
                ]
            )

            session.add(pandda_result_sql)

    session.commit()
    #
    # print("Printing database systems...")
    #
    # for instance in session.query(ProjectDirSQL).order_by(ProjectDirSQL.id):
    #     print(f"{instance.system_name}: {instance.path}")
    #     for dataset in instance.datasets:
    #         print(f"\t{dataset.dtag}")

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
    fire.Fire(main)
