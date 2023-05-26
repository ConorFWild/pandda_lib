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
sns.set(font_scale=5)
# sns.color_palette("hls", 8)
sns.set_palette("hls")
sns.set_palette("crest")


# {
#     0: "Low Resolution",  # Data with a low resolution in same conf. as ground state
#     1: "Poor Data Quality",  # Broadly uninterpretable event map
#     2: "Merging of Unrelated Blobs",  # Deprecated
#     3: "Disorder",  # Missing protein density
#     4: "High Resolution Blob",  # A large isolated blob too big to be water and too small to be a ligand
#     5: "Conformational Change",  # A Clear conformational change relative to the ground state map
#     6: "Misannotated Hit",  # A clear fragment binding event
#     7: "Contaminent Binding",  # Interpretable, non-ligand density
#     8: "Blob far from Protein",  # A blob far from protein density
#     9: "Interface Artefact",  # A blob at a crystal contact
#     10: "Cofactor Orientation Change",  # A change in the position of a cofactor
#     11: "Localized Uninterpretable Density",  # Non-isolated blob
#     12: "Water Repositioning",  # Clear relocation of water sized blob
#     13: "Data Viewing Error"  # Data failed to open
# }

comment_key = {
    # Protein Dependent
    0: "Low Resolution Smoothing",  # Data with a low resolution in same conf. as ground state
    1: "Disorder",  # Missing protein density
    2: "Conformational Change",  # A Clear conformational change relative to the ground state map
    3: "Interface Conformational Change",  # A Clear conformational change relative to the ground state map
    # Solvent Dependent
    4: "High Resolution Blob",  # A large isolated blob too big to be water and too small to be a ligand
    # Protein-Solvent Dependent
    5: "Poor Data Quality",  # Broadly uninterpretable event map
    6: "Misannotated Hit",  # A clear fragment binding event
    7: "Contaminent Binding",  # Interpretable, non-ligand density
    8: "Interface Artefact",  # A blob at a crystal contact
    9: "Water Repositioning",  # Clear relocation of water sized blob
    10: "Localized Uninterpretable Density",  # Non-isolated blob
    # Algorithm Dependent
    11: "Blob far from Protein",  # A blob far from protein density
    12: "Data Viewing Error",  # Data failed to open
    13: "Merging of Unrelated Blobs",  # Deprecated
    14: "Cofactor Orientation Change",  # A change in the position of a cofactor
    15: "Strong Solvent Channel Density"
}

def try_make(path):
    try:
        os.mkdir(path)
    except Exception as e:
        return

def plot_high_ranking_non_hits():
    # sqlite_filepath = "/dls/science/groups/i04-1/conor_dev/pandda_lib/diamond_2.db"
    # sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    thesis_dir = pathlib.Path("/dls/science/groups/i04-1/conor_dev/pandda_lib/thesis/")


    output_dir = thesis_dir / "pandda_1_high_ranking_non_hits"
    try_make(output_dir)

    # engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    # session = sessionmaker(bind=engine)()
    # Base.metadata.create_all(engine)

    # sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    # tmp_dir = pathlib.Path(tmp_dir).resolve()
    # engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    # session = sessionmaker(bind=engine)()
    # Base.metadata.create_all(engine)

    # Remove tables
    # Base.metadata.create_all(engine)
    # BoundStateModelSQL.__table__.drop(engine)
    # Base.metadata.create_all(engine)

    # Get the datasets
    # print("Getting SQL data...")
    # initial_datasets = session.query(DatasetSQL).options(subqueryload(DatasetSQL.bound_state_model)).order_by(
    #     DatasetSQL.id).all()
    # panddas = session.query(PanDDA1DirSQL).order_by(PanDDA1DirSQL.id).all()
    # print(f"\tGot {len(panddas)} panddas!")



    # Generate a fake PanDDA inspect dataset from this balanced sample
    fake_pandda_dir = thesis_dir / "fake_pandda_high_scoring_non_hits_with_mean"
    analyses_dir = fake_pandda_dir / constants.PANDDA_ANALYSES_DIR
    inspect_path = analyses_dir / constants.PANDDA_INSPECT_EVENTS_PATH
    inspect_table = pd.read_csv(inspect_path)

    # Make the table of categorizations
    category_sums = {value: 0 for value in comment_key.values()}
    for idx, row in inspect_table.iterrows():
        comment = row["Comment"]
        if comment == "None":
            continue

        catagory = comment_key[int(comment)]
        category_sums[catagory] += 1

    records = [
        {"Category": key, "Num. Occurrences": value}
        for key, value
        in category_sums.items()
        if value > 0
    ]
    category_table = pd.DataFrame(records)


    # Plot bars for each category
    fig, ax = plt.subplots(ncols=1, figsize=(30, 30))  # sharey=True)

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
    fire.Fire(plot_high_ranking_non_hits)