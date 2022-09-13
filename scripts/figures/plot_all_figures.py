import pathlib
import os

import numpy as np
import pandas as pd

import fire
from sqlalchemy.orm import sessionmaker, subqueryload
from sqlalchemy import create_engine

from pandda_lib import constants
from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs
from pandda_lib.fs.pandda_result import PanDDAResult
from pandda_lib.diamond_sqlite.diamond_sqlite import *

import gemmi

import seaborn as sns

sns.set(rc={'figure.figsize': (2 * 11.7, 2 * 8.27)})
sns.set(font_scale=3)
# sns.color_palette("hls", 8)
sns.set_palette("hls")
sns.set_palette("crest")

from plot_rscc_vs_rmsd import plot_rscc_vs_rmsd
from plot_rankings import plot_rankings


def plot_all_figures(only=None):
    plots = {
        "plot_rscc_vs_rmsd": lambda: plot_rscc_vs_rmsd,
        "plot_rankings": lambda: plot_rankings,
    }

    print(only)

    if only is not None:

        print(only)
        plots[only]()

    else:

        print(f"Plotting RSCC vs RMSD...")

        for plot_name, plot_function in plots.items():
            print(f"{plot_name}")
            plot_function()

    print("Finished!")


if __name__ == "__main__":
    fire.Fire(plot_all_figures())
