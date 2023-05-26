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
from matplotlib import pyplot as plt

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
    im = np.zeros((10,10))
    im_tot = np.zeros((10,10))
    ress = np.array([0.90000201, 1.2040883,  1.5081746,  1.81226089, 2.11634719, 2.42043349, 2.72451978, 3.02860608, 3.33269237, 3.63677867, 3.94086497])
    rsccs = np.linspace(0.0,1.0,num=11)
    for idx, row in inspect_table.iterrows():
        rscc = row["z_peak"]
        if rscc < 0:
            continue
        res = row["high_resolution"]
        confidence = row["Ligand Confidence"]

        x = np.searchsorted(ress, res)
        if x > 0:
            x = x - 1
        y = np.searchsorted(rsccs, rscc)
        if y > 0:
            y = y - 1

        im_tot[x,y] += 1

        # Get the
        if confidence == "High":

            im[x, y] += 1

    im_dis = np.zeros((10,10))
    for x,y in itertools.product(range(10), range(10)):
        if im_tot[x,y] != 0:
            im_dis[x,y] = im[x,y] / im_tot[x,y]
        else:
            im_dis[x,y] = -1


    # Plot bars for each category
    fig, ax = plt.subplots(
        ncols=1,
        # figsize=(30, 30),
    )  # sharey=True)

    plt.imshow(
        im_dis,
        extent=(
            min(ress),
            max(ress),
            min(rsccs),
            max(rsccs)
        )
    )
    plt.xticks(rotation=45, ha='right')
    plt.colorbar()

    # Save the plot
    fig.savefig(
        output_dir / "rscc_res_confidence_map.png",
        bbox_inches='tight',
    )
    plt.cla()
    plt.clf()
    plt.close("all")
    plt.close()

    x = []
    y = []
    c = []
    for idx, row in inspect_table.iterrows():
        rscc = row["z_peak"]
        if rscc < 0:
            continue
        res = row["high_resolution"]
        confidence = row["Ligand Confidence"]

        x.append(res)
        y.append(rscc)
        if confidence == "High":
            c.append('#1f77b4')
        else:
            c.append('#8c564b')

    fig, ax = plt.subplots(
        ncols=1,
        # figsize=(30, 30),
    )  # sharey=True)

    plt.scatter(
        x=x,
        y=y,
        c=c
    )
    plt.xticks(rotation=45, ha='right')
    plt.colorbar()

    # Save the plot
    fig.savefig(
        output_dir / "rscc_res_confidence_scatter.png",
        bbox_inches='tight',
    )
    plt.cla()
    plt.clf()
    plt.close("all")
    plt.close()


if __name__ == "__main__":
    fire.Fire(plot_rscc_vs_res)