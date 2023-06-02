import itertools
import pathlib
import os
import pdb

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

# import seaborn as sns
#
# sns.set(rc={'figure.figsize': (2 * 11.7, 2 * 8.27)})
# sns.set(font_scale=3)
# # sns.color_palette("hls", 8)
# sns.set_palette("hls")
# sns.set_palette("crest")

def try_make(path):
    try:
        os.mkdir(path)
    except Exception as e:
        return


def try_link(source_path, target_path):
    try:
        os.symlink(source_path, target_path)
    except Exception as e:
        # print(e)
        return



def plot_rscc_vs_rmsd():
    sqlite_filepath = "/dls/science/groups/i04-1/conor_dev/pandda_lib/diamond_2.db"
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    output_dir = pathlib.Path("/dls/science/groups/i04-1/conor_dev/pandda_lib/thesis")
    try_make(output_dir)

    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)

    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    # tmp_dir = pathlib.Path(tmp_dir).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()


    # Get the datasets
    print("Getting SQL data...")
    initial_datasets = session.query(DatasetSQL).options(subqueryload(DatasetSQL.bound_state_model)).order_by(
        DatasetSQL.id).all()
    print(f"\tGot {len(initial_datasets)} datasets!")

    # Iterate, printing out paths to data that are not found
    for dataset in initial_datasets:
        if dataset.model_path:
            if dataset.model_path != "None":
                if not pathlib.Path(dataset.model_path).exists():
                    print(f"\tMissing structure: {dataset.model_path}")

        if dataset.mtz_path:
            if dataset.mtz_path != "None":
                if not pathlib.Path(dataset.mtz_path).exists():
                    print(f"\tMissing reflections: {dataset.mtz_path}")

        if dataset.pandda_model_path:
            if dataset.pandda_model_path != "None":
                if not pathlib.Path(dataset.pandda_model_path).exists():
                    print(f"\tMissing bound state structure: {dataset.pandda_model_path}")



if __name__ == "__main__":
    fire.Fire(plot_rscc_vs_rmsd)