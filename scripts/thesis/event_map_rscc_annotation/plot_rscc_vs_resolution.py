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
def plot_rscc_vs_res():
    thesis_dir = pathlib.Path("/dls/science/groups/i04-1/conor_dev/pandda_lib/thesis/")
    output_dir = thesis_dir / "pandda_1_rscc_interpretation"
    try_make(output_dir)

    # Generate a fake PanDDA inspect dataset from this balanced sample
    fake_pandda_dir = thesis_dir / "fake_pandda_rsccs"
    analyses_dir = fake_pandda_dir / constants.PANDDA_ANALYSES_DIR
    inspect_path = analyses_dir / constants.PANDDA_INSPECT_EVENTS_PATH
    inspect_table = pd.read_csv(inspect_path)

    # Make the table of categorizations
    # category_sums = {value: 0 for value in comment_key.values()}
    im = np.zeros((10,10))
    for idx, row in inspect_table.iterrows():
        rscc = row["z_peak"]
        res = row["high_resolution"]
        confidence = row["Ligand Confidence"]
        # Get the
        x =
        y =




    # Plot bars for each category
    fig, ax = plt.subplots(
        ncols=1,
        # figsize=(30, 30),
    )  # sharey=True)

    sns.barplot(
        data=category_table,
        x="Category",
        y="Num. Occurrences",
        ax=ax
    )
    plt.xticks(rotation=45, ha='right')

    # Save the plot
    fig.savefig(
        output_dir / "high_ranking_not_hit_categories.png",
        bbox_inches='tight',
    )
    plt.cla()
    plt.clf()
    plt.close("all")
    plt.close()


if __name__ == "__main__":
    fire.Fire(plot_rscc_vs_res)