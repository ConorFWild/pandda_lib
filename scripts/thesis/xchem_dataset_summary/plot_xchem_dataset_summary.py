import itertools
import pathlib
import os
import pdb
from typing import List
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

import pandas as pd

import seaborn as sns

sns.set(rc={'figure.figsize': (2 * 11.7, 2 * 8.27)})
sns.set(font_scale=3)
# sns.color_palette("hls", 8)
sns.set_palette("hls")
sns.set_palette("crest")


def try_make(path):
    try:
        os.mkdir(path)
    except Exception as e:
        return

def get_system_from_dtag(dtag):
    hyphens = [pos for pos, char in enumerate(dtag) if char == "-"]
    if len(hyphens) == 0:
        return None
    else:
        last_hypen_pos = hyphens[-1]
        system_name = dtag[:last_hypen_pos]
        return system_name
def plot_xchem_dataset_summaries():


    sqlite_filepath = "/dls/science/groups/i04-1/conor_dev/pandda_lib/diamond_2.db"
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    output_dir = pathlib.Path("/dls/science/groups/i04-1/conor_dev/pandda_lib/thesis/xchem_dataset_summary")
    try_make(output_dir)

    # Get the database
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    # tmp_dir = pathlib.Path(tmp_dir).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()

    # Get the information on each system
    initial_datasets: List[DatasetSQL] = session.query(DatasetSQL).options(subqueryload(DatasetSQL.bound_state_model)).order_by(
        DatasetSQL.id).all()
    datasets_by_system = {}
    for dataset in initial_datasets:

        # Add the dataset to the datasets by system
        system_name = get_system_from_dtag(dataset.dtag)
        if not system_name:
            continue

        if system_name not in datasets_by_system:
            datasets_by_system[system_name] = {}
        datasets_by_system[system_name][dataset.dtag] = dataset

    # For each system, get the relevant information and output a latex formated table
    for system, system_datasets in datasets_by_system.items():
        # Get number of datasets
        num_datasets = len(system_datasets)

        # Get minimum resolution
        system_dataset_resolutions = [gemmi.read_mtz_file(ds.mtz_path).resolution_high() for ds in system_datasets.values()]
        min_res = min(system_dataset_resolutions)

        # Get mean resolution
        mean_res = sum(system_dataset_resolutions) / len(system_dataset_resolutions)

        # Get max resolution
        max_res = max(system_dataset_resolutions)

        # Get % with bound state model
        project_hit_rate = len([x for x in system_datasets.values() if x.bound_state_model is not None]) / len(system_datasets)

        # Get the organism

        # Get the protein id

        #

        print([system, num_datasets, min_res, mean_res, max_res, project_hit_rate])


if __name__ == "__main__":
    fire.Fire(plot_xchem_dataset_summaries)