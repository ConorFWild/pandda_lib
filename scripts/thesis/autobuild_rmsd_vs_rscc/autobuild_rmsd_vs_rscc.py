import itertools
import pathlib
import os
import pdb
from typing import List
import numpy as np
# import pandas as pd
from matplotlib import pyplot as plt

import fire
from sqlalchemy.orm import sessionmaker, subqueryload
from sqlalchemy import create_engine

from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score

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


def get_system_from_dtag(dtag):
    hyphens = [pos for pos, char in enumerate(dtag) if char == "-"]
    if len(hyphens) == 0:
        return None
    else:
        last_hypen_pos = hyphens[-1]
        system_name = dtag[:last_hypen_pos]
        return system_name



def get_all_systems_res_sigma_table(inspect_tables):
    rank_records = []
    for inspect_table_key, inspect_table in inspect_tables.items():
        try:
            median_resolution = inspect_table["high_resolution"].median()
            median_sigma = inspect_table["map_uncertainty"].median()
            rank = 0
            cumulative_hits = 0
            for index, row in inspect_table.iterrows():
                if row["Ligand Confidence"] != "Low":
                    cumulative_hits += 1
                    is_hit = True
                else:
                    is_hit = False
                dtag = row["dtag"]
                event_idx = row["event_idx"]
                rank += 1
                resolution = row["high_resolution"]
                map_uncertainty = row["map_uncertainty"]

                rank_records.append(
                    {"Rank": rank, "Cumulative Hits": cumulative_hits, "Dtag": dtag, "Event IDX": event_idx,
                     "Resolution": resolution, "Map Uncertainty": map_uncertainty, "Is Hit?": is_hit,
                     "Resolution Delta": resolution - median_resolution,
                     "Map Uncertainty Delta": map_uncertainty - median_sigma, "System": inspect_table_key[0],
                     "PanDDA": inspect_table_key[1]})
        except Exception as e:
            print(f"{inspect_table_key}: {e}")

    return pd.DataFrame(rank_records)


def plot_xchem_dataset_summaries():
    sqlite_filepath = "/dls/science/groups/i04-1/conor_dev/pandda_lib/diamond_2.db"
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    output_dir = pathlib.Path("/dls/science/groups/i04-1/conor_dev/pandda_lib/thesis/rmsd_vs_rscc")
    try_make(output_dir)

    # Get the database
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()

    panddas = session.query(PanDDADirSQL).options(subqueryload("*")).order_by(PanDDADirSQL.id).all()

    records = []
    for pandda in panddas:
        for dataset in pandda.pandda_dataset_results:

            try:
                bound_state_st = gemmi.read_structure(dataset.pandda_model_path)
            except Exception as e:
                print(f"Couldn't access model, skipping: {dataset.pandda_model_path}")
                continue

            # Get centroids of modelled ligands
            centroids = []
            for model in bound_state_st:
                for chain in model:
                    for resisdue in chain:
                        if resisdue.name != "LIG":
                            continue
                        poss = []
                        for atom in resisdue:
                            pos = atom.pos
                            poss.append([pos.x, pos.y, pos.z])
                        centroid = np.mean(poss, axis=0)
                        centroids.append(centroid)
            if len(centroids) == 0:
                print(f"\t\tNo ligands, skipping!")

                continue

            for event in dataset.events:

                # Skip if event isn't close to the centroid of a modelled ligand
                closest_centroid_distance = min(
                    [
                        np.linalg.norm(centroid-np.array([event.x, event.y, event.z]))
                        for centroid
                        in centroids
                    ]
                )
                if closest_centroid_distance > 5.0:
                    print(f"\t\t\tNo nearby centroid, skipping!")
                    continue

                for build in event.builds:
                    # print(build.score)
                    # if build.score!= -0.01:
                    # print(f"{dataset.dtag} {build.rmsd}")

                    has_build = False
                    if build.score != -0.01:
                        has_build = True

                    is_hit_dataset = False
                    if dataset.dataset:
                        if dataset.dataset.pandda_model_path != "None":
                            is_hit_dataset = True

                    if build.rmsd:
                        closest_rmsd = build.rmsd.closest_rmsd
                        high_confidence = build.rmsd.high_confidence
                        broken_ligand = build.rmsd.broken_ligand
                        alignment_error = build.rmsd.alignment_error
                    else:
                        closest_rmsd = None
                        high_confidence = False
                        broken_ligand = False
                        alignment_error = False

                    rscc = None
                    if build.rscc:
                        rscc = build.rscc.score

                    if dataset.dataset:
                        pandda_model_path = dataset.dataset.pandda_model_path
                    else:
                        pandda_model_path = None
                    records.append(
                        {
                            "System": pandda.system.system_name,
                            "Project": pandda.project.project_name,
                            "Dataset": dataset.dtag,
                            "Event": event.idx,
                            "Build ID": build.id,
                            "Has Build?": has_build,
                            "Is Hit Dataset?": is_hit_dataset,
                            "Score": build.score,
                            "MAD": closest_rmsd,
                            "RSCC": rscc,
                            "High Confidence?": high_confidence,
                            "Alignment Error?": alignment_error,
                            "Broken Ligand?": broken_ligand,
                            "Build Path": build.build_path,
                            "Event Map Path": event.event_map_path,
                            "PanDDA Model": pandda_model_path
                        }
                    )

    build_scores = pd.DataFrame(records)

    graph = sns.scatterplot(
        data=build_scores,
        x="MAD",
        y="RSCC"
    )
    graph.set(xscale='log')
    plt.tight_layout()
    graph.get_figure().savefig(output_dir / "RMSDVsRSCC.png")
    plt.cla()
    plt.clf()
    plt.close("all")


if __name__ == "__main__":
    fire.Fire(plot_xchem_dataset_summaries)
