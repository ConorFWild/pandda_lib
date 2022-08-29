import pathlib
import os

import numpy as np
import gemmi
import fire
from sqlalchemy.orm import sessionmaker, subqueryload
from sqlalchemy import create_engine
import joblib
from joblib import Parallel, delayed

from pandda_lib import constants
from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs
from pandda_lib.fs.pandda_result import PanDDAResult
from pandda_lib.diamond_sqlite.diamond_sqlite import (Base, ProjectDirSQL, DatasetSQL, PanDDADirSQL,
                                                      PanDDADatasetSQL, PanDDABuildSQL, PanDDAEventSQL, SystemSQL,
                                                      BoundStateModelSQL, EventMapQualtiles)
from pandda_lib.rscc import get_rscc


def diamond_add_model_stats(sqlite_filepath, ):
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    # tmp_dir = pathlib.Path(tmp_dir).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)

    Base.metadata.create_all(engine)
    EventMapQualtiles.__table__.drop(engine)
    Base.metadata.create_all(engine)

    # Get datasets
    initial_datasets = session.query(DatasetSQL).options(subqueryload(DatasetSQL.event_maps)).order_by(
        DatasetSQL.id).all()

    # For dataset, get 2Fo-Fc>0 mean and scale, then for event map>0 mean and scale
    for dataset in initial_datasets:
        if len(dataset.event_maps) == 0:
            continue
        mtz_path = dataset.mtz_path
        print(f"\t{mtz_path}")
        mtz = gemmi.read_mtz_file(mtz_path)
        try:
            grid = mtz.transform_f_phi_to_map(
                "FWT",
                "PHWT",
                sample_rate=3,
            )

        except Exception as e:
            grid = mtz.transform_f_phi_to_map(
                "2FOFCWT",
                "PH2FOFCWT",
                sample_rate=3,
            )
            # print(e)

        grid_array = np.array(grid)
        grid_array_positive = grid_array[grid_array > 0]
        grid_mean = np.mean(grid_array_positive)
        grid_std = np.std(grid_array_positive)
        grid_quantiles = np.quantile(
            grid_array[grid_array > 1.0],
            [0.5, 0.75, 0.9],
        ).round(3)

        event_map_stats = {}
        for event_map_sql in dataset.event_maps:
            event_map_idx = event_map_sql.event_idx
            try:
                event_map = gemmi.read_ccp4_map(event_map_sql.path, setup=True)
            except Exception as e:
                print(e)
                continue
            event_map_grid = event_map.grid
            event_map_grid_array = np.array(event_map_grid)
            # print(event_map_grid_array)
            event_map_grid_array_positive = event_map_grid_array[event_map_grid_array > 0]
            event_map_mean = np.mean(event_map_grid_array_positive)
            event_map_std = np.std(event_map_grid_array_positive)
            event_map_quantiles = np.quantile(event_map_grid_array[event_map_grid_array > 1.0], [0.5, 0.75, 0.9]).round(
                3)

            event_map_stats[int(event_map_idx)] = {
                "1-BDC": event_map_sql.bdc,
                "mean": event_map_mean,
                "std": event_map_std,
                "quantiles": event_map_quantiles
            }

            event_map_quantiles = EventMapQualtiles(
                base_50=grid_quantiles[0],
                base_75=grid_quantiles[1],
                base_90=grid_quantiles[2],
                event_50=event_map_quantiles[0],
                event_75=event_map_quantiles[1],
                event_90=event_map_quantiles[2],
            )
            event_map_sql.event_map_quantiles = event_map_quantiles

            session.add(event_map_quantiles)


        print(f"Grid Mean: {grid_mean}; Grid std: {grid_std}; Quantiles: {grid_quantiles}")
        print(event_map_stats)
        print("#########################################")

    session.commit()

if __name__ == "__main__":
    fire.Fire(diamond_add_model_stats)
