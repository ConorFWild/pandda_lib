import pathlib
import os

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

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


def get_datasets(systems):
    datasets = []
    for system in systems:
        for project in system.projects:
            for dataset in project.datasets:
                datasets.append(dataset)

    return datasets


def get_models(systems):
    models = []
    for system in systems:
        for project in system.projects:
            for dataset in project.datasets:
                if dataset.pandda_model_path != "None":
                    models.append(dataset.pandda_model_path)

    return models


def get_build_table(panddas):
    records = []
    for pandda in panddas:
        for dataset in pandda.pandda_dataset_results:

            for event in dataset.events:
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

    return build_scores


def print_stats(build_scores):
    builds_with_hits_mask = build_scores["Is Hit Dataset?"] == True
    builds_with_scores_mask = build_scores["Has Build?"] == True
    highest_rscc_dataset_mask = build_scores.groupby(["Dataset"])["RSCC"].transform(max) == build_scores["RSCC"]
    lowest_rmsd_dataset_mask = build_scores.groupby(["Dataset"])["MAD"].transform(min) == build_scores["MAD"]

    builds_highest_dataset_rscc = build_scores[highest_rscc_dataset_mask]
    builds_lowest_dataset_rmsd = build_scores[lowest_rmsd_dataset_mask]

    num_hit_datasets = len(build_scores[builds_with_hits_mask]["Dataset"].unique())
    num_hits_with_builds = len(build_scores[builds_with_hits_mask & builds_with_scores_mask]["Dataset"].unique())

    num_builds_within_1_rmsd_high_scoring = len(builds_highest_dataset_rscc[builds_highest_dataset_rscc["MAD"] < 1])
    num_builds_within_2_rmsd_high_scoring = len(builds_highest_dataset_rscc[builds_highest_dataset_rscc["MAD"] < 2])
    num_builds_within_6_rmsd_high_scoring = len(builds_highest_dataset_rscc[builds_highest_dataset_rscc["MAD"] < 6])

    num_builds_within_1_rmsd_closest = len(builds_lowest_dataset_rmsd[builds_lowest_dataset_rmsd["MAD"] < 1])
    num_builds_within_2_rmsd_closest = len(builds_lowest_dataset_rmsd[builds_lowest_dataset_rmsd["MAD"] < 2])
    num_builds_within_6_rmsd_closest = len(builds_lowest_dataset_rmsd[builds_lowest_dataset_rmsd["MAD"] < 6])

    print(f"Number of hit datasets: {num_hit_datasets}")
    print(f"Number of hit datasets with builds: {num_hits_with_builds}")

    print(
        f"Number of highest scoring builds within: 1A {num_builds_within_1_rmsd_high_scoring}; 2A {num_builds_within_2_rmsd_high_scoring}; 6A {num_builds_within_6_rmsd_high_scoring}")

    print(
        f"Number of highest scoring builds within: 1A {num_builds_within_1_rmsd_closest}; 2A {num_builds_within_2_rmsd_closest}; 6A {num_builds_within_6_rmsd_closest}")


def plot_rmsds():
    #
    output_dir = pathlib.Path("/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2/thesis_figures")

    #
    sqlite_filepath = "/dls/science/groups/i04-1/conor_dev/pandda_lib/diamond_2.db"
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()

    #
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)

    #
    panddas = session.query(PanDDADirSQL).options(subqueryload("*")).order_by(PanDDADirSQL.id).all()

    #
    build_scores = get_build_table(panddas)

    # Get stats
    print_stats(build_scores)

    #

    # Get highest mean-atomic-distance  distribution
    graph = sns.ecdfplot(
        data=build_rsccs_table.query("(RSCC > 0) & (RMSD > 2)"),
        x="RSCC",
    )
    graph.get_figure().savefig(output_dir / "rscc_for_high_rmsd.png")
    plt.cla()
    plt.clf()
    plt.close("all")

    # Get closest mean-atomic-distance dist
    graph = sns.ecdfplot(
        data=build_rsccs_table.query("(RSCC > 0) & (RMSD > 2)"),
        x="RSCC",
    )
    graph.get_figure().savefig(output_dir / "rscc_for_high_rmsd.png")
    plt.cla()
    plt.clf()
    plt.close("all")

    #
