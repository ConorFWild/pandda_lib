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



def mask_grid(st, grid, mask_radius=3.0):
    new_grid = gemmi.Int8Grid(grid.nu, grid.nv, grid.nw)
    new_grid.set_unit_cell(grid.unit_cell)
    new_grid.spacegroup =gemmi.find_spacegroup_by_name("P 1")
    for model in st:
        for chain in model:
            for residue in chain.get_polymer():
                for atom in residue:
                    pos = atom.pos
                    new_grid.set_points_around(pos,
                                           radius=mask_radius,
                                           value=1,
                                           )

    return new_grid

def diamond_add_event_stats(sqlite_filepath, ):
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

        if not pathlib.Path(mtz_path).exists():
            continue

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

        # Get the dataset
        structure_path = dataset.model_path
        st = gemmi.read_structure(structure_path)
        mtz_mask_grid = mask_grid(st, grid)
        mtz_mask_array = np.array(mtz_mask_grid, copy=False, dtype=np.int8)

        # Get the mask for the mtz

        grid_array_initial = np.array(grid)
        # grid_array_non_zero = grid_array_initial[grid_array_initial != 0.0]
        # grid_array = (grid_array_initial - grid_array_non_zero.mean()) / (grid_array_non_zero.std())
        # grid_array_positive = grid_array[grid_array > 1.0]
        grid_array = grid_array_initial[mtz_mask_array != 0]
        grid_array_scaled = (grid_array - np.mean(grid_array)) / np.std(grid_array)
        grid_array_for_quantiles = grid_array_scaled

        grid_mean = np.mean(grid_array_for_quantiles)
        grid_std = np.std(grid_array_for_quantiles)
        if not grid_array_for_quantiles.size > 3:
            continue
        grid_quantiles = np.quantile(
            grid_array_for_quantiles,
            [0.5, 0.75, 0.9],
        ).round(3)

        event_map_stats = {}
        for event_map_sql in dataset.event_maps:
            event_map_idx = event_map_sql.event_idx
            try:
                event_map = gemmi.read_ccp4_map(event_map_sql.path)
                event_map.spacegroup = gemmi.find_spacegroup_by_name("P 1")
                event_map.setup()
            except Exception as e:
                print(e)
                continue

            event_mask_grid = mask_grid(st, event_map.grid)
            event_mask_array = np.array(event_mask_grid, copy=False, dtype=np.int8)

            event_map_grid = event_map.grid
            event_map_grid_array_initial = np.array(event_map_grid)
            event_map_array = event_map_grid_array_initial[event_mask_array != 0]
            event_map_array_scaled = (event_map_array - np.mean(event_map_array)) / np.std(event_map_array)
            event_map_array_for_quantiles = event_map_array_scaled

            # event_map_grid_array_non_zero = event_map_grid_array_initial[event_map_grid_array_initial != 0.0]
            # event_map_grid_scaled_array =
            # event_map_scaled_masked_array = event_map_grid_scaled_array[event_mask_array != 0]
            # print(event_map_grid_array)
            # event_map_grid_array_positive = event_map_grid_array[event_map_grid_array > 1.0]
            if event_map_array_for_quantiles.size < 3:
                continue
            event_map_mean = np.mean(event_map_array_for_quantiles)
            event_map_std = np.std(event_map_array_for_quantiles)
            event_map_quantiles = np.quantile(
                event_map_array_for_quantiles,
                [0.5, 0.75, 0.9]).round(3)

            event_map_stats[int(event_map_idx)] = {
                "1-BDC": event_map_sql.bdc,
                "mean": event_map_mean,
                "std": event_map_std,
                "quantiles": event_map_quantiles,
                "max": np.max(event_map_array_for_quantiles),
                "Percent > 1": event_map_array_for_quantiles[
                                   event_map_array_for_quantiles > 1.0].size / event_map_array_for_quantiles.size,
                "Percent > 2": event_map_array_for_quantiles[
                                   event_map_array_for_quantiles > 2.0].size / event_map_array_for_quantiles.size,
                "Percent > 3": event_map_array_for_quantiles[
                                   event_map_array_for_quantiles > 3.0].size / event_map_array_for_quantiles.size
            }

            event_map_quantiles = EventMapQualtiles(
                base_50=grid_quantiles[0],
                base_75=grid_quantiles[1],
                base_90=grid_quantiles[2],
                event_50=event_map_quantiles[0],
                event_75=event_map_quantiles[1],
                event_90=event_map_quantiles[2],
                base_greater_than_1=grid_array_for_quantiles[grid_array_for_quantiles > 1.0].size / grid_array_for_quantiles.size,
                base_greater_than_2=grid_array_for_quantiles[grid_array_for_quantiles > 2.0].size / grid_array_for_quantiles.size,
                base_greater_than_3=grid_array_for_quantiles[grid_array_for_quantiles > 3.0].size / grid_array_for_quantiles.size,
                event_greater_than_1=event_map_array_for_quantiles[
                                         event_map_array_for_quantiles > 1.0].size / event_map_array_for_quantiles.size,
                event_greater_than_2=event_map_array_for_quantiles[event_map_array_for_quantiles > 2.0].size /
                                     event_map_array_for_quantiles.size,
                event_greater_than_3=event_map_array_for_quantiles[event_map_array_for_quantiles > 3.0].size /
                                     event_map_array_for_quantiles.size,
            )
            event_map_sql.event_map_quantiles = event_map_quantiles

            session.add(event_map_quantiles)

        print(
            (
                f"Grid Mean: {grid_mean}; Grid std: {grid_std}; Quantiles: {grid_quantiles}; Max {grid_array.max()}; "
                f">1 {grid_array_for_quantiles[grid_array_for_quantiles > 1.0].size / grid_array_for_quantiles.size} "
                f">2 {grid_array_for_quantiles[grid_array_for_quantiles > 2.0].size / grid_array_for_quantiles.size} "
                f">3 {grid_array_for_quantiles[grid_array_for_quantiles > 3.0].size / grid_array_for_quantiles.size} "
            )
        )
        print(event_map_stats)
        print("#########################################")

    session.commit()


if __name__ == "__main__":
    fire.Fire(diamond_add_event_stats)
