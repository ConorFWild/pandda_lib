import pathlib
import os
from typing import *

import fire
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from pandda_lib import constants
from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs
from pandda_lib.fs.pandda_result import PanDDAResult
from pandda_lib.diamond_sqlite.diamond_sqlite import (Base, ProjectDirSQL, DatasetSQL, PanDDADirSQL,
                                                      PanDDADatasetSQL, PanDDABuildSQL, PanDDAEventSQL, SystemSQL)


def get_pandda_2_result(pandda_path, dtag_to_dataset_sql, system=None, project=None) -> Optional[PanDDADirSQL]:
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
                    # signal_samples=float(build.signal_samples),
                    # total_signal_samples=float(build.total_signal_samples),
                    # noise_samples=float(build.noise_samples),
                    # total_noise_samples=float(build.total_noise_samples),
                    # percentage_signal=float(build.percentage_signal),
                    # percentage_noise=float(build.percentage_noise),
                    score=float(build.score),
                )

            dataset_events[event_id] = PanDDAEventSQL(
                event_map_path=str(event.event_map_path),
                idx = int(event.idx),
                x=float(event.centroid[0]),
                y=float(event.centroid[1]),
                z=float(event.centroid[2]),
                size=float(event.size),
                bdc=float(event.bdc),
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
            input_pdb_path=dataset.structure_path,
            input_mtz_path = dataset.mtz_path,
            events=[
                _event
                for _event
                in dataset_events.values()
            ],
            dataset=dataset_sql
        )

    pandda_result_sql = PanDDADirSQL(
        path=str(pandda_path),
        pandda_dataset_results=[
            _dataset_sql
            for _dataset_sql
            in dataset_results.values()
        ],
        system=system,
        project=project,
    )

    return pandda_result_sql
