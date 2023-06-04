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


def get_fake_event_table(samples, event_table_samples):
    new_event_rows = []
    j = 0
    event_ids = {}
    for event_key, event_row in event_table_samples.items():

        sample = samples[event_key]

        event_key = (event_row["dtag"], event_row["event_idx"])
        if event_key in event_ids:
            continue

        event_ids[event_key] = event_row

        if j == 0:
            print(event_row)
        event_row["site_idx"] = int(j / 100) + 1
        event_row["z_peak"] = sample["RSCC"]

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

    return new_event_table


def get_fake_site_table(samples, event_table_samples):
    site_records = []
    num_sites = int(len(samples) / 100)
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

    return site_table


def _get_event_map_path(sample, event_row):
    return pathlib.Path(sample["Event Map Path"])


def _get_ligand_files_path(sample, event_row):
    dataset_dir = pathlib.Path(sample["Event Map Path"]).parent
    ligand_files = dataset_dir / "ligand_files"
    return ligand_files


def _get_initial_model_path(sample, event_row):
    dtag = sample["Dtag"]
    dataset_dir = pathlib.Path(sample["Event Map Path"]).parent
    return dataset_dir / constants.PANDDA_INITIAL_MODEL_TEMPLATE.format(dtag=dtag)


def _get_initial_mtz_path(sample, event_row):
    dtag = sample["Dtag"]
    dataset_dir = pathlib.Path(sample["Event Map Path"]).parent
    return dataset_dir / constants.PANDDA_INITIAL_MTZ_TEMPLATE.format(dtag=dtag)


def _make_combined_model(
        initial_model_path,
        sample,
        event_row,
        output_path
):
    initial_model_structure = gemmi.read_structure(str(initial_model_path))

    if sample["Build Path"]:
        build_path =  pathlib.Path(sample["Build Path"])
        if build_path.exists():
            # Get the initial model and build
            build_path_structure = gemmi.read_structure(str(build_path))

            # Get the first alphabetically free chain name
            # chain_ids = []
            # for model in initial_model_structure:
            #     for chain in model:
            #         chain_ids.append(chain.name)
            # new_chain_name = None
            # for char in string.ascii_uppercase:
            #     if char not in chain_ids:
            #         new_chain_name = char
            #         break

            # Merge the autobuild ligand in
            for initial_model in initial_model_structure:
                for build_model in build_path_structure:
                    for chain in build_model:
                        initial_model.add_chain(chain, unique_name=True)

    # Save the new model
    initial_model_structure.write_minimal_pdb(str(output_path))

def generate_fake_pandda(samples, event_table_samples, fake_pandda_dir):

    # Generate new table
    new_event_table = get_fake_event_table(samples, event_table_samples)

    # Generate new site table
    site_table = get_fake_site_table(samples, event_table_samples)

    try_make(fake_pandda_dir)
    try_make(fake_pandda_dir / constants.PANDDA_PROCESSED_DATASETS_DIR)
    try_make(fake_pandda_dir / "analyses")

    new_event_table.to_csv(fake_pandda_dir / "analyses" / "pandda_analyse_events.csv", index=False)
    site_table.to_csv(fake_pandda_dir / "analyses" / "pandda_analyse_sites.csv", index=False)

    for sample_key, sample in samples.items():
        event_row = event_table_samples[sample_key]
        dtag = sample_key[0]

        dataset_dir = fake_pandda_dir / constants.PANDDA_PROCESSED_DATASETS_DIR / dtag
        try_make(dataset_dir)
        built_model_dir = dataset_dir / constants.PANDDA_MODELLED_STRUCTURES_DIR
        try_make(built_model_dir)
        # event map
        event_map_path = _get_event_map_path(sample, event_row)
        try_link(
            event_map_path,
            dataset_dir / event_map_path.name,
        )
        # ligand files
        ligand_files_path = _get_ligand_files_path(sample, event_row)
        try_link(
            ligand_files_path,
            dataset_dir / ligand_files_path.name
        )
        # Initial model
        initial_model_path = _get_initial_model_path(sample, event_row)
        try_link(
            initial_model_path,
            dataset_dir / initial_model_path.name
        )
        # Initial mtz
        initial_mtz_path = _get_initial_mtz_path(sample, event_row)
        try_link(
            initial_mtz_path,
            dataset_dir / initial_mtz_path.name
        )
        # Built model
        _make_combined_model(
            initial_model_path,
            sample,
            event_row,
            built_model_dir / constants.PANDDA_MODEL_FILE.format(dtag=dtag)
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
    thesis_dir = pathlib.Path("/dls/science/groups/i04-1/conor_dev/pandda_lib/thesis/")
    output_dir = thesis_dir / "autobuild_samples"
    try_make(output_dir)

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
    low_scoring_high_confidence_samples = {}
    high_scoring_low_confidence_samples = {}
    low_scoring_high_confidence_event_table_samples = {}
    high_scoring_low_confidence_event_table_samples = {}

    for pandda in panddas:
        print(f"########## PanDDA: {pandda.system.system_name}: {pandda.project.project_name} ##########")
        test_pandda = pandda

        # Inspect table
        pandda_event_table_path = pathlib.Path(
            test_pandda.path) / constants.PANDDA_ANALYSES_DIR / constants.PANDDA_ANALYSE_EVENTS_FILE
        pandda_event_table = pd.read_csv(pandda_event_table_path)

        # Relevant inspect tables
        system_inspect_tables = {key: value for key, value in inspect_tables.items() if
                                 key[0] == pandda.system.system_name}
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

                    # print(inspect_table)
                    # print(inspect_table.columns)
                    dtag_mask = inspect_table["dtag"] == dtag
                    same_dtag_events = inspect_table[dtag_mask]
                    if len(same_dtag_events) == 0:
                        continue

                    for inspect_event_row_idx, inspect_event_row in same_dtag_events.iterrows():
                        inspect_event_class = inspect_event_row["Ligand Confidence"]
                        if inspect_event_class not in ["High", "high"]:
                            continue

                        inspect_x, inspect_y, inspect_z = inspect_event_row["x"], inspect_event_row["y"], \
                            inspect_event_row["z"]

                        distance = np.linalg.norm(
                            np.array([inspect_x - event_x, inspect_y - event_y, inspect_z - event_z]))

                        if distance < 5.0:
                            matched_events[(dtag, event_idx)] = inspect_event_row

        print(f"Num matched events: {len(matched_events)}")
        if len(matched_events) == 0:
            continue

        #

        # Get the size ranking table
        # default_rank_table = rank_table_from_pandda_sizes_first_dtag(test_pandda)

        # Get the build rscc ranking table
        build_score_rank_table = rank_table_from_pandda_rsccs_first_dtag(test_pandda)

        # Get the high confidence mask of the build score rank table
        mask_list = []
        lowest_ranked_hit_index = 0
        for record_idx, record in build_score_rank_table.iterrows():
            dtag, event_idx = record["Dtag"], record["Event IDX"]
            if (dtag, event_idx) in matched_events:
                mask_list.append(True)
                lowest_ranked_hit_index = record_idx
            else:
                mask_list.append(False)
        high_confidence_mask = pd.Series(np.array(mask_list))
        num_high_confidence = high_confidence_mask.sum()

        print(lowest_ranked_hit_index)
        # print(build_score_rank_table[high_confidence_mask])

        # Get events that were matched to high confidence but scored poorly
        # In specific those builds with ranks lower than the number of hits in total
        low_scoring_table = build_score_rank_table.iloc[num_high_confidence:]
        low_scoring_high_confidence_mask = high_confidence_mask.loc[num_high_confidence:]
        low_scoring_high_confidence_table = low_scoring_table[low_scoring_high_confidence_mask]
        print(f"Number of low scoring, high confidence events: {len(low_scoring_high_confidence_table)}")
        print(low_scoring_high_confidence_table)
        sample = low_scoring_high_confidence_table.sample(n=min(len(low_scoring_high_confidence_table), 15))
        for _idx, _row in sample.iterrows():
            _dtag, _event_idx = _row["Dtag"], _row["Event IDX"]
            low_scoring_high_confidence_samples[(_dtag, _event_idx)] = _row

        # Get the event table rows corresponding to the sample
        for _idx, _row in pandda_event_table.iterrows():
            _dtag, _event_idx = _row["dtag"], _row["event_idx"]
            if (_dtag, _event_idx) in low_scoring_high_confidence_samples:
                low_scoring_high_confidence_event_table_samples[(_dtag, _event_idx)] = _row

        # Get events that were not matched to high confidence but scored well
        # Specifically low confidence events above the median hit rank
        median_hit_rank = int(np.median(build_score_rank_table[high_confidence_mask]["Rank"]))
        high_scoring_table = build_score_rank_table.iloc[:median_hit_rank]
        high_scoring_high_confidence_mask = high_confidence_mask.iloc[:median_hit_rank]
        high_scoring_low_confidence_table = high_scoring_table[~high_scoring_high_confidence_mask]
        print(f"Number of high scoring, low confidence events: {len(high_scoring_low_confidence_table)}")
        print(high_scoring_low_confidence_table)
        sample = high_scoring_low_confidence_table.sample(n=min(len(high_scoring_low_confidence_table), 15))
        for _idx, _row in sample.iterrows():
            _dtag, _event_idx = _row["Dtag"], _row["Event IDX"]

            high_scoring_low_confidence_samples[(_dtag, _event_idx)] = _row

        # Get the event table rows corresponding to the sample
        for _idx, _row in pandda_event_table.iterrows():
            _dtag, _event_idx = _row["dtag"], _row["event_idx"]
            if (_dtag, _event_idx) in high_scoring_low_confidence_samples:
                high_scoring_low_confidence_event_table_samples[(_dtag, _event_idx)] = _row

    print(len(low_scoring_high_confidence_samples))
    print(len(high_scoring_low_confidence_samples))

    # Generate a fake PanDDA inspect dataset from this balanced sample
    fake_pandda_dir = output_dir / "autobuild_ranking_low_scoring_high_confidence"
    try_make(fake_pandda_dir)
    generate_fake_pandda(
        low_scoring_high_confidence_samples,
        low_scoring_high_confidence_event_table_samples,
        fake_pandda_dir,
    )

    # Generate a fake PanDDA inspect dataset from this balanced sample
    fake_pandda_dir = output_dir / "autobuild_ranking_high_scoring_low_confidence"
    try_make(fake_pandda_dir)
    generate_fake_pandda(
        high_scoring_low_confidence_samples,
        high_scoring_low_confidence_event_table_samples,
        fake_pandda_dir,
    )


if __name__ == "__main__":
    fire.Fire(plot_rscc_vs_rmsd)
