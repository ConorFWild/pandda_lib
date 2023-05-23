import itertools
import pathlib
import os

import numpy as np
# import pandas as pd
# from matplotlib import pyplot as plt

import fire
from sqlalchemy.orm import sessionmaker, subqueryload
from sqlalchemy import create_engine

from pandda_lib import constants
from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs
from pandda_lib.fs.pandda_result import PanDDAResult
from pandda_lib.diamond_sqlite.diamond_sqlite import *

import gemmi

# import seaborn as sns
#
# sns.set(rc={'figure.figsize': (2 * 11.7, 2 * 8.27)})
# sns.set(font_scale=3)
# # sns.color_palette("hls", 8)
# sns.set_palette("hls")
# sns.set_palette("crest")


def plot_rscc_vs_rmsd():
    sqlite_filepath = "/dls/science/groups/i04-1/conor_dev/pandda_lib/diamond_2.db"
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    output_dir = pathlib.Path("/dls/science/groups/i04-1/conor_dev/pandda_lib/thesis_figures")

    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)

    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    # tmp_dir = pathlib.Path(tmp_dir).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()
    # Base.metadata.create_all(engine)

    # Remove tables
    # Base.metadata.create_all(engine)
    # BoundStateModelSQL.__table__.drop(engine)
    # Base.metadata.create_all(engine)

    # Get the datasets
    print("Getting SQL data...")
    initial_datasets = session.query(DatasetSQL).options(subqueryload(DatasetSQL.bound_state_model)).order_by(
        DatasetSQL.id).all()
    print(f"\tGot {len(initial_datasets)} datasets!")

    # Get the RSCCs and resolutions of each dataset
    print("Getting RSCCs and Resolutions...")
    records = []
    for dataset in initial_datasets:

        # Skip if no RSCC
        if not dataset.bound_state_model:
            continue

        if not dataset.bound_state_model.rscc:
            continue

        # Get the RSCC
        rscc = dataset.bound_state_model.rscc

        # Get the mtz
        mtz_path = dataset.mtz_path
        if not pathlib.Path(mtz_path).exists():
            continue
        mtz = gemmi.read_mtz_file(mtz_path)

        # Get the resolution
        resolution = mtz.resolution_high()

        records.append(
            {
                "Dataset": dataset,
                "Resolution": resolution,
                "RSCC": rscc,
            }
        )
    print(f"\tGot {len(records)} datasets with RSCCs!")

    # Partition the datasets by resolution and RSCC
    print(f"Partitioning datasets on resolution and RSCC...")
    resolutions = [x["Resolution"] for x in records]
    rsccs = [x["RSCC"] for x in records]
    min_res = min(resolutions)
    max_res = max(resolutions)
    res_samples = np.linspace(min_res, max_res, num=11)
    rscc_samples = np.linspace(0.0,1.0,num=11)
    sample_datasets = {}
    for x, y in itertools.product(range(11), range(11)):
        sample_datasets[(x, y)] = []

    res_indexes = np.searchsorted(res_samples, resolutions)
    rscc_indexes = np.searchsorted(rscc_samples, rsccs)

    for record, res_index, rscc_index in zip(records, res_indexes, rscc_indexes):
        # res, rscc = res_samples[res_index], rscc_samples[rscc_index]
        sample_datasets[(res_index, rscc_index)].append(record)

    # Generate a balanced sample of datasets
    print(f"Generating balanced sample of datasets...")
    rng = np.random.default_rng()
    sample = []
    for (res, rscc), subsample_records in sample_datasets.items():
        if len(subsample_records) == 0:
            continue
        selected_records = rng.choice(subsample_records, min(10, len(subsample_records)), replace=False)
        sample += selected_records
    print(f"\tGot a sample of size: {len(sample)}")

    # Generate a fake PanDDA inspect dataset from this balanced sample


if __name__ == "__main__":
    fire.Fire(plot_rscc_vs_rmsd)


