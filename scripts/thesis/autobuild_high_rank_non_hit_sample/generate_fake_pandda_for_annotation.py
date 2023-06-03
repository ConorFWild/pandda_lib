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


def rank_table_from_pandda_sizes_first_dtag(pandda_2_sql):
    records = []
    for pandda_dataset in pandda_2_sql.pandda_dataset_results:

        for event in pandda_dataset.events:
            # RMSD
            rmsds = {}
            rsccs = {}
            build_paths = {}

            for build in event.builds:
                if build.rmsd:
                    if build.rmsd.closest_rmsd:
                        rmsd = build.rmsd.closest_rmsd
                        rmsds[build.id] = rmsd

                # RSCC
                if build.rscc:
                    if build.rscc.score:
                        rscc = build.rscc.score
                        rsccs[build.id] = rscc
                        build_paths[build.id] = build.build_path

            if len(rmsds) == 0:
                rmsd = None
            else:
                rmsd = min(rmsds.values())

            if len(rsccs) == 0:
                rscc = None
            else:
                rscc = max(rsccs.values())

            build_path = None
            if len(rsccs) != 0:
                highest_rscc_build_id = max(rsccs, key=lambda _key: rsccs[_key])
                build_path = build_paths[highest_rscc_build_id]

            # Determine if it is a hit event
            _hit = False
            if rmsd:
                if rmsd < 6:
                    _hit = True

            has_builds = False
            if len(event.builds) != 0:
                has_builds = True

            #
            event_size = event.size

            record = {
                "Dtag": pandda_dataset.dtag,
                "Event IDX": event.idx,
                # "Score": event_scores[event_idx],
                "RSCC": rscc,
                "RMSD": rmsd,
                "Event Size": event_size,
                "Hit?": _hit,
                "Has Builds?": has_builds,
                "Build Path": build_path,
                "Event Map Path": event.event_map_path
            }
            records.append(record)

    table = pd.DataFrame(records).sort_values("Event Size", ascending=False)

    rank_records = []
    cumulative_hits = 0
    rank = 0
    used_dtags = []

    for index, row in table.iterrows():
        dtag = row["Dtag"]
        event_idx = row["Event IDX"]

        rank += 1

        # Check if it is a hit
        if row["Hit?"] & (dtag not in used_dtags):
            cumulative_hits += 1
            used_dtags.append(dtag)

        rank_records.append({"Rank": rank, "Cumulative Hits": cumulative_hits, "Dtag": dtag, "Event IDX": event_idx,
                             "RSCC": row["RSCC"], "RMSD": row["RMSD"], "Has Builds?": row["Has Builds?"],
                             "Build Path": row["Build Path"],
                             "Event Map Path": row["Event Map Path"]
                             })

    return pd.DataFrame(rank_records)

def generate_fake_pandda(sample, fake_pandda_dir):
    # For each sample, find the event corresponding inspect table, get the built event
    # and from that get the event map and link everything into a fake pandda dir

    unattested_events = []

    for record in sample:

        dataset = record["Dataset"]
        rscc = record["RSCC"]
        print(f"\t{dataset.dtag} : {dataset.pandda_model_path}")

        st = gemmi.read_structure(dataset.pandda_model_path)
        lig_centroids = []
        for model in st:
            for chain in model:
                for res in chain:
                    if res.name != "LIG":
                        continue
                    poss = []
                    for atom in res:
                        pos = atom.pos
                        poss.append([pos.x, pos.y, pos.z])
                    mean_pos = np.mean(np.array(poss), axis=0)
                    lig_centroids.append(mean_pos)

        if len(lig_centroids) == 0:
            print("\tNot lig: skipping!")
            continue
        lig_centroid = lig_centroids[0]

        # Get the inspect table
        # print(f"\t\t{dataset.pandda_model_path}")
        dataset_dir = pathlib.Path(dataset.pandda_model_path).parent.parent
        experiment_dir = dataset_dir.parent

        # Check panddas for a matching
        got_event = False
        for pandda_dir in experiment_dir.glob("*"):
            if got_event:
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

            # Get the dataset events
            dataset_event_table = inspect_table[
                (inspect_table[constants.PANDDA_INSPECT_DTAG] == dataset.dtag)
                & (inspect_table[constants.PANDDA_INSPECT_LIGAND_PLACED] == True)
            ]
            event_rows = [row for idx, row in dataset_event_table.iterrows()]
            if len(event_rows) < 1:
                print(f"\tDid not get any events for dataset {dataset.dtag} with a ligand place! Skipping!")
                continue

            row = event_rows[0]

            dtag = row[constants.PANDDA_INSPECT_DTAG]
            event_idx = row[constants.PANDDA_INSPECT_EVENT_IDX]
            bdc = row[constants.PANDDA_INSPECT_BDC]
            x, y, z = row["x"], row["y"], row["z"]


            # for centroid in lig_centroids:
            distance = np.linalg.norm(lig_centroid.flatten() - np.array([x, y, z]))
            if distance > 4.0:
                print(f"No nearby lig for dataset {dataset.dtag}")
                continue


            # score = row["z_peak"]
            score = rscc

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
                pathlib.Path(dataset.pandda_model_path),
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

def rank_table_from_pandda_rsccs_first_dtag(pandda_2_sql):
    records = []
    for pandda_dataset in pandda_2_sql.pandda_dataset_results:

        for event in pandda_dataset.events:
            # RMSD
            rmsds = {}
            rsccs = {}
            build_paths = {}

            for build in event.builds:
                if build.rmsd:
                    if build.rmsd.closest_rmsd:
                        rmsd = build.rmsd.closest_rmsd
                        rmsds[build.id] = rmsd

                # RSCC
                if build.rscc:
                    if build.rscc.score:
                        rscc = build.rscc.score
                        rsccs[build.id] = rscc
                        build_paths[build.id] = build.build_path

            if len(rmsds) == 0:
                rmsd = None
            else:
                rmsd = min(rmsds.values())

            if len(rsccs) == 0:
                rscc = None
            else:
                rscc = max(rsccs.values())

            build_path = None
            if len(rsccs) != 0:
                highest_rscc_build_id = max(rsccs, key=lambda _key: rsccs[_key])
                build_path = build_paths[highest_rscc_build_id]

            # Determine if it is a hit event
            _hit = False
            if rmsd:
                if rmsd < 6:
                    _hit = True

            has_builds = False
            if len(event.builds) != 0:
                has_builds = True

            record = {
                "Dtag": pandda_dataset.dtag,
                "Event IDX": event.idx,
                # "Score": event_scores[event_idx],
                "RSCC": rscc,
                "RMSD": rmsd,
                "Hit?": _hit,
                "Has Builds?": has_builds,
                "Build Path": build_path,
                "Event Map Path": event.event_map_path
            }
            records.append(record)
    table = pd.DataFrame(records).sort_values("RSCC", ascending=False)

    rank_records = []
    cumulative_hits = 0
    rank = 0
    used_dtags = []

    for index, row in table.iterrows():
        dtag = row["Dtag"]
        event_idx = row["Event IDX"]

        rank += 1

        # Check if it is a hit
        if row["Hit?"] & (dtag not in used_dtags):
            cumulative_hits += 1
            used_dtags.append(dtag)

        rank_records.append({"Rank": rank, "Cumulative Hits": cumulative_hits, "Dtag": dtag, "Event IDX": event_idx,
                             "RSCC": row["RSCC"], "RMSD": row["RMSD"], "Has Builds?": row["Has Builds?"],
                             "Build Path": row["Build Path"],
                             "Event Map Path": row["Event Map Path"]
                             })

    return pd.DataFrame(rank_records)


def plot_rscc_vs_rmsd():
    # Get the table
    sqlite_filepath = "/dls/science/groups/i04-1/conor_dev/pandda_lib/diamond_2.db"
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)

    # get the inspect tables
    projects = session.query(ProjectDirSQL).order_by(ProjectDirSQL.id).all()
    print(f"Got {len(projects)} projects!")

    inspect_tables = {}
    for instance in session.query(PanDDA1DirSQL).order_by(PanDDA1DirSQL.id):
        # print(f"{instance.system.system_name}: {instance.system_id}: {instance.path}")
        inspect_table_path = pathlib.Path(
            instance.path) / constants.PANDDA_ANALYSES_DIR / constants.PANDDA_INSPECT_EVENTS_PATH
        try:
            inspect_table = pd.read_csv(inspect_table_path)
        except Exception as e:
            print(e)
            continue

        project = None
        for _project in projects:
            pandda_dir = pathlib.Path(instance.path).parent.parent.parent.name
            # print(pandda_dir)
            if pandda_dir == _project.project_name:
                project = _project
        if not project:
            continue

        if not (instance.system.system_name, project.project_name) in inspect_tables:
            inspect_tables[(instance.system.system_name, project.project_name)] = {}

        inspect_tables[(instance.system.system_name, project.project_name)][inspect_table_path] = inspect_table
    print(f"Got {len(inspect_tables)} inspect tables!")

    # Get the PanDDAs
    panddas = session.query(PanDDADirSQL).options(subqueryload("*")).order_by(PanDDADirSQL.id).all()
    print(f"Got {len(panddas)} PanDDAs!")

    print("Outputting tables and figures...")
    for pandda in panddas:
        print(f"PanDDA: {pandda.system.system_name}: {pandda.project.project_name}")
        test_pandda = pandda

        # Inspect table
        pandda_event_table_path = pathlib.Path(test_pandda.path) / constants.PANDDA_ANALYSES_DIR / constants.PANDDA_ANALYSE_EVENTS_FILE
        pandda_event_table = pd.read_csv(pandda_event_table_path)

        # Relevant inspect tables
        system_inspect_tables = {key: value for key, value in inspect_tables.items() if key[0] == pandda.system.system_name}
        if len(system_inspect_tables) == 0:
            continue
        print(f"Got {len(system_inspect_tables)} relevant inspect tables")

        # Match events to known high confidence hits from historical inspect tables
        matched_events = {}
        for event_row_idx, event_row in pandda_event_table.iterrows():
            dtag, event_idx = event_row["dtag"], event_row["event_idx"]
            event_x, event_y, event_z = event_row["x"], event_row["y"], event_row["z"]
            for system_experiment_key, system_experiment_inspect_tables in system_inspect_tables.items():
                for inspect_table_path, inspect_table in system_experiment_inspect_tables.items():

                    print(inspect_table)
                    print(inspect_table.columns)
                    dtag_mask = inspect_table["dtag"] == dtag
                    same_dtag_events = inspect_table[dtag_mask]
                    if len(same_dtag_events) == 0:
                        continue

                    for inspect_event_row_idx, inspect_event_row in same_dtag_events.iterrows():
                        inspect_event_class = inspect_event_row["Confidence"]
                        if inspect_event_class not in ["High", "high"]:
                            continue

                        inspect_x, inspect_y, inspect_z = inspect_event_row["x"], inspect_event_row["y"], inspect_event_row["z"]

                        distance = np.linalg.norm(np.array([inspect_x-event_x, inspect_y-event_y, inspect_z-event_z]))

                        if distance < 5.0:
                            matched_events[(dtag, event_idx)] = inspect_event_row
        print(f"Num matched events: {len(matched_events)}")
        if len(matched_events) == 0:
            continue

        #

        # Get the size ranking table
        default_rank_table = rank_table_from_pandda_sizes_first_dtag(test_pandda)

        # Get the build rscc ranking table
        build_score_rank_table = rank_table_from_pandda_rsccs_first_dtag(test_pandda)

        # Get events that were matched to high confidence but scored poorly

        # Get events that were not matched to high confidence but scored well

    # Generate a fake PanDDA inspect dataset from this balanced sample
    fake_pandda_dir = output_dir / "fake_pandda_rsccs"
    try_make(fake_pandda_dir)
    generate_fake_pandda(sample, fake_pandda_dir)


if __name__ == "__main__":
    fire.Fire(plot_rscc_vs_rmsd)


