import itertools
import pathlib
import os
import pdb
import string

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


high_rank_non_hit_key = {
    0: "Unannotated Hit",
    1: "Plausible Density",
    2: "Large Interface Blob",
    3: "Bad Event Map",
    4: "Overlapping Similar Protein",
    5: "No Build",
    6: "Partial Density"
}

low_rank_hit_key = {
    0: "Partial Density",
    1: "No Good Build",
    2: "Built into Protein",
    3: "Wrong Ligand",
    4: "No Pathology",
    5: "Slight Misbuild",
    6: "No Build",
    7: "No Ligand Density"
}


def plot_rscc_vs_rmsd():
    # Get the table
    sqlite_filepath = "/dls/science/groups/i04-1/conor_dev/pandda_lib/diamond_2.db"
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)
    thesis_dir = pathlib.Path("/dls/science/groups/i04-1/conor_dev/pandda_lib/thesis/")
    output_dir = thesis_dir / "autobuild_samples"
    try_make(output_dir)

    # Generate a fake PanDDA inspect dataset from this balanced sample
    fake_pandda_dir = output_dir / "autobuild_ranking_low_scoring_high_confidence"
    analyses_dir = fake_pandda_dir / constants.PANDDA_ANALYSES_DIR
    inspect_table_path = analyses_dir / constants.PANDDA_INSPECT_EVENTS_PATH
    inspect_table = pd.read_csv(inspect_table_path)
    # Make the table of categorizations
    category_sums = {value: 0 for value in low_rank_hit_key.values()}
    for idx, row in inspect_table.iterrows():
        comment = row["Comment"]
        if comment == "None":
            continue

        catagory = low_rank_hit_key[int(comment)]
        category_sums[catagory] += 1

    records = [
        {"Category": key, "Num. Occurrences": value}
        for key, value
        in category_sums.items()
        if value > 0
    ]
    category_table = pd.DataFrame(records)

    for record in sorted(records, key=lambda _record: _record["Num. Occurrences"], reverse=True):
        print(f"{record['Category']} {record['Num. Occurrences']}")

    print(category_table["Num. Occurrences"].sum())


    # Generate a fake PanDDA inspect dataset from this balanced sample
    fake_pandda_dir = output_dir / "autobuild_ranking_high_scoring_low_confidence"
    analyses_dir = fake_pandda_dir / constants.PANDDA_ANALYSES_DIR
    inspect_table_path = analyses_dir / constants.PANDDA_INSPECT_EVENTS_PATH
    inspect_table = pd.read_csv(inspect_table_path)
    # Make the table of categorizations
    category_sums = {value: 0 for value in high_rank_non_hit_key.values()}
    for idx, row in inspect_table.iterrows():
        comment = row["Comment"]
        if comment == "None":
            continue

        catagory = high_rank_non_hit_key[int(comment)]
        category_sums[catagory] += 1

    records = [
        {"Category": key, "Num. Occurrences": value}
        for key, value
        in category_sums.items()
        if value > 0
    ]
    category_table = pd.DataFrame(records)

    for record in sorted(records, key=lambda _record: _record["Num. Occurrences"], reverse=True):
        print(f"{record['Category']} {record['Num. Occurrences']}")

    print(category_table["Num. Occurrences"].sum())


if __name__ == "__main__":
    fire.Fire(plot_rscc_vs_rmsd)
