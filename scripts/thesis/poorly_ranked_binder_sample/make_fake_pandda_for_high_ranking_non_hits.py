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


def generate_fake_pandda(unattested_events, fake_pandda_dir):
    # For each sample, find the event corresponding inspect table, get the built event
    # and from that get the event map and link everything into a fake pandda dir

    # Generate new table
    new_event_rows = []
    j = 0
    event_ids = {}
    for unattested_event in unattested_events:

        event_row = unattested_event[-1]
        event_key = (event_row["dtag"], event_row["event_idx"])
        if event_key in event_ids:
            continue

        event_ids[event_key] = event_row

        if j == 0:
            print(event_row)
        event_row["site_idx"] = int(j / 100) + 1
        event_row["z_peak"] = unattested_event[-2]

        new_event_rows.append(event_row)
        j = j + 1
    new_event_table = pd.DataFrame(
        new_event_rows
    ).reset_index()

    # del new_event_table["Unnamed: 0"]
    # del new_event_table["index"]
    print(new_event_table)
    try:
        new_event_table.drop(["index"], axis=1, inplace=True)
    except Exception as e:
        print(e)
    try:
        new_event_table.drop(["Unnamed: 0"], axis=1, inplace=True)
    except Exception as e:
        print(e)
    print(new_event_table)

    # site_ids = np.unique(new_event_table["site_idx"])
    site_records = []
    num_sites = int(len(unattested_events) / 100)
    print(f"Num sites is: {num_sites}")
    for site_id in np.arange(0, num_sites + 1):
        site_records.append(
            {
                "site_idx": int(site_id) + 1,
                "centroid": (0.0, 0.0, 0.0),
                # "Name": None,
                # "Comment": None
            }
        )
    print(len(site_records))
    site_table = pd.DataFrame(site_records)
    print(site_table)
    print(len(site_table))

    # print(f"New event table: {new_event_table}")
    # print(new_event_table["z_peak"])

    try_make(fake_pandda_dir)
    try_make(fake_pandda_dir / constants.PANDDA_PROCESSED_DATASETS_DIR)
    try_make(fake_pandda_dir / "analyses")

    new_event_table.to_csv(fake_pandda_dir / "analyses" / "pandda_analyse_events.csv", index=False)
    site_table.to_csv(fake_pandda_dir / "analyses" / "pandda_analyse_sites.csv", index=False)

    for event_row in unattested_events:
        dtag = event_row[-1]["dtag"]
        # print([event_row[-1]["dtag"], event_row[-1]["event_idx"]])
        dataset_dir = fake_pandda_dir / constants.PANDDA_PROCESSED_DATASETS_DIR / dtag
        try_make(dataset_dir)
        built_model_dir = dataset_dir / constants.PANDDA_MODELLED_STRUCTURES_DIR
        try_make(built_model_dir)
        # event map
        try_link(
            event_row[3],
            dataset_dir / event_row[3].name,
        )
        # ligand files
        try_link(
            event_row[4],
            dataset_dir / event_row[4].name
        )
        # Initial model
        try_link(
            event_row[5],
            dataset_dir / event_row[5].name
        )
        # Initial mtz
        try_link(
            event_row[6],
            dataset_dir / event_row[6].name
        )
        # Built model
        try_link(
            event_row[7],
            built_model_dir / event_row[7].name
        )


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
    # Base.metadata.create_all(engine)

    # Remove tables
    # Base.metadata.create_all(engine)
    # BoundStateModelSQL.__table__.drop(engine)
    # Base.metadata.create_all(engine)

    # Get the datasets
    print("Getting SQL data...")
    # initial_datasets = session.query(DatasetSQL).options(subqueryload(DatasetSQL.bound_state_model)).order_by(
    #     DatasetSQL.id).all()
    panddas = session.query(PanDDA1DirSQL).order_by(PanDDA1DirSQL.id).all()
    print(f"\tGot {len(panddas)} panddas!")

    # Get the events from each PanDDA
    print("Getting event samples...")
    used_systems = []
    records = []
    unattested_events = []
    rng = np.random.default_rng()
    used_panddas = {}
    for pandda in panddas:
        pandda_dir = pathlib.Path(pandda.path)
        print(f"PanDDA: {pandda_dir}")

        if not pandda_dir.exists():
            print(f"\t\tNo such pandda dir {pandda_dir}: skipping!")
            continue

        if not pandda_dir.is_dir():
            # print(f"\t\t")
            continue

        done_file = pandda_dir / "pandda.done"
        if not done_file.exists():
            print(f"\t\tNo pandda done file for {pandda_dir}: skipping!")
            continue

        # pandda_dir = dataset_dir.parent.parent
        analyses_dir = pandda_dir / constants.PANDDA_ANALYSES_DIR
        inspect_table_path = analyses_dir / constants.PANDDA_INSPECT_EVENTS_PATH

        if not inspect_table_path.exists():
            print("\t\tNo inspect table: skipping!")
            continue

        inspect_table = pd.read_csv(inspect_table_path)
        inspect_table.sort_values(by="cluster_size", inplace=True, ignore_index=True, ascending=False)

        # Check if there are high confidence modelled hits
        high_conf_event_table = inspect_table[
            (inspect_table[constants.PANDDA_INSPECT_HIT_CONDFIDENCE] == "High")
            & (inspect_table[constants.PANDDA_INSPECT_LIGAND_PLACED] == True)
            ]
        high_conf_event_rows = [row for idx, row in high_conf_event_table.iterrows()]
        if len(high_conf_event_rows) < 1:
            print(f"\tDid not get any events for pandda {pandda_dir} with a ligand place! Skipping!")
            continue

        # Lowest ranking high conf dataset
        lowest_ranked_high_conf_index = high_conf_event_table.index.values[-1]
        print(f"\tLowest ranked high confidence hit: {lowest_ranked_high_conf_index}")
        truncated_event_table = inspect_table.loc[:lowest_ranked_high_conf_index]

        # If so, get highest ranked non-hit, and up to 5 random non-hits up to lowest ranked high confidence, placed lig

        low_conf_event_table = truncated_event_table[
            (truncated_event_table[constants.PANDDA_INSPECT_HIT_CONDFIDENCE] == "Low")
            & (truncated_event_table[constants.PANDDA_INSPECT_VIEWED] == True)
            ]
        low_conf_event_rows = [row for idx, row in low_conf_event_table.iterrows()]

        event_sample = rng.choice(
            range(len(low_conf_event_rows[1:])),
            min(5, len(low_conf_event_rows[1:])),
            replace=False,
        )

        print(f"\t\t{event_sample}")

        for j in event_sample + [0,]:
            row = low_conf_event_rows[j]
            dtag = row[constants.PANDDA_INSPECT_DTAG]
            event_idx = row[constants.PANDDA_INSPECT_EVENT_IDX]
            bdc = row[constants.PANDDA_INSPECT_BDC]
            x, y, z = row["x"], row["y"], row["z"]

            score = row["z_peak"]

            # print(row)
            # print(dtag)
            if not isinstance(dtag, str):
                print(f"Error with dtag for some reason: {dtag}! Skipping!")
                continue
            dataset_dir = pandda_dir / constants.PANDDA_PROCESSED_DATASETS_DIR / dtag
            event_row = [
                dtag,
                event_idx,
                pandda_dir,
                dataset_dir / constants.PANDDA_EVENT_MAP_TEMPLATE.format(
                    dtag=dtag,
                    event_idx=event_idx,
                    bdc=bdc
                ),
                dataset_dir / "ligand_files",
                dataset_dir / constants.PANDDA_INITIAL_MODEL_TEMPLATE.format(dtag=dtag),
                dataset_dir / constants.PANDDA_INITIAL_MTZ_TEMPLATE.format(dtag=dtag),
                dataset_dir / constants.PANDDA_MODELLED_STRUCTURES_DIR / constants.PANDDA_MODEL_FILE.format(dtag=dtag),
                score,
                row
            ]
            unattested_events.append(event_row)
            used_panddas[pandda.path] = True

    print(f"\tManaged to get {len(unattested_events)} events from {len(used_panddas)} panddas!")

    # Generate a fake PanDDA inspect dataset from this balanced sample
    fake_pandda_dir = output_dir / "fake_pandda_high_scoring_non_hits"
    try_make(fake_pandda_dir)
    generate_fake_pandda(unattested_events, fake_pandda_dir)


if __name__ == "__main__":
    fire.Fire(plot_rscc_vs_rmsd)
