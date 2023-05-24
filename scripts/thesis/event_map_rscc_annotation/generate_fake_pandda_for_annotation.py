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


def generate_fake_pandda(sample, fake_pandda_dir):
    # For each sample, find the event corresponding inspect table, get the built event
    # and from that get the event map and link everything into a fake pandda dir

    unattested_events = []

    for record in sample:
        dataset = record["Dataset"]
        # Get the inspect table
        print(f"\t\t{dataset.pandda_model_path}")
        dataset_dir = pathlib.Path(dataset.pandda_model_path).parent.parent
        experiment_dir = dataset_dir.parent.parent

        # Check panddas for a matching
        got_event = False
        for pandda_dir in experiment_dir.glob("*"):
            if got_event:
                continue
            if not pandda_dir.is_dir():
                continue

            done_file = pandda_dir / "pandda.done"
            if not done_file.exists():
                continue

            # pandda_dir = dataset_dir.parent.parent
            analyses_dir = pandda_dir / constants.PANDDA_ANALYSES_DIR
            inspect_table_path = analyses_dir / constants.PANDDA_INSPECT_EVENTS_PATH
            if not inspect_table_path.exists():
                continue

            inspect_table = pd.read_csv(inspect_table_path)

            # Get the dataset events
            dataset_event_table = inspect_table[
                (inspect_table[constants.PANDDA_INSPECT_DTAG] == dataset.dtag)
                & (inspect_table[constants.PANDDA_INSPECT_LIGAND_PLACED] == True)
            ]
            event_rows = [row for idx, row in dataset_event_table.iterrows()]
            if len(event_rows) != 1:
                print(f"\tDid not get exactly 1 ligand for dataset {dataset.dtag}! Skipping!")
                continue

            row = event_rows[0]

            dtag = row[constants.PANDDA_INSPECT_DTAG]
            event_idx = row[constants.PANDDA_INSPECT_EVENT_IDX]
            bdc = row[constants.PANDDA_INSPECT_BDC]
            x, y, z = row["x"], row["y"], row["z"]
            score = row["z_peak"]
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
                score,
                row
            ]
            unattested_events.append(event_row)
            got_event = True

    print(f"\tManaged to assign {len(unattested_events)} events!")

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
        new_event_rows.append(event_row)
        j = j + 1
    new_event_table = pd.DataFrame(
        new_event_rows
    ).reset_index()

    # del new_event_table["Unnamed: 0"]
    # del new_event_table["index"]
    new_event_table.drop(["index", "Unnamed: 0"], axis=1, inplace=True)
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
        try_link(
            event_row[3],
            dataset_dir / event_row[3].name,
        )
        try_link(
            event_row[4],
            dataset_dir / event_row[4].name
        )
        try_link(
            event_row[5],
            dataset_dir / event_row[5].name
        )
        try_link(
            event_row[6],
            dataset_dir / event_row[6].name
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
    print(f"Res samples are: {res_samples}")
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
        selected_records = rng.choice(
            subsample_records,
            min(10, len(subsample_records)),
            replace=False,
        )
        sample += [x for x in selected_records]
    print(f"\tGot a sample of size: {len(sample)}")

    # Generate a fake PanDDA inspect dataset from this balanced sample
    fake_pandda_dir = output_dir / "fake_pandda_rsccs"
    try_make(fake_pandda_dir)
    generate_fake_pandda(sample, fake_pandda_dir)


if __name__ == "__main__":
    fire.Fire(plot_rscc_vs_rmsd)


