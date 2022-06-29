import pathlib
import os

import fire
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs
from pandda_lib.fs.pandda_result import PanDDAResult
from pandda_lib.diamond_sqlite.diamond_sqlite import (Base, SystemDataDirSQL, DatasetSQL, PanDDADirSQL,
                                                      PanDDADatasetSQL, PanDDABuildSQL, PanDDAEventSQL)


def main(sqlite_filepath, output_dir_name):
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()
    PanDDADirSQL.drop(engine)
    Base.metadata.create_all(engine)

    systems = session.query(SystemDataDirSQL).order_by(SystemDataDirSQL.id).all()

    print("Updating database...")
    for system in systems:
        _pandda_dir = pathlib.Path(system.path).parent / output_dir_name
        print(f"\tAnalysing run at: {_pandda_dir}")

        pandda_result = PanDDAResult.from_dir(_pandda_dir)

        dataset_results = {}
        for dtag, dataset in pandda_result.processed_datasets.items():
            dataset_events = {}

            for event_id, event in dataset.events.items():
                event_builds = {}

                for build_id, build in event.build_results.items():
                    event_builds[build_id] = PanDDABuildSQL(
                        build_path=build.path,
                    )

                dataset_events[event_id] = PanDDAEventSQL(
                    event_map_path=event.event_map_path,
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
            path=str(_pandda_dir),
            pandda_dataset_results=[
                _dataset_sql
                for _dataset_sql
                in dataset_results.values()
            ]
        )

        session.add(pandda_result_sql)

    session.commit()

    print("Printing database systems...")

    for instance in session.query(SystemDataDirSQL).order_by(SystemDataDirSQL.id):
        print(f"{instance.system_name}: {instance.path}")
        for dataset in instance.datasets:
            print(f"\t{dataset.dtag}")

    # print("Printing database datasets...")
    # for instance in session.query(DatasetSQL).order_by(S)


if __name__ == "__main__":
    fire.Fire(main)
