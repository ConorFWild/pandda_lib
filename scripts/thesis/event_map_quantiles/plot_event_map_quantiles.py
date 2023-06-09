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
    sqlite_filepath = "/dls/science/groups/i04-1/conor_dev/diamond.db"
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    output_dir = pathlib.Path("/dls/science/groups/i04-1/conor_dev/pandda_lib/thesis/event_map_stats")
    try_make(output_dir)

    # Get the database
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()

    systems = session.query(SystemSQL).options(
        subqueryload(SystemSQL.projects).subqueryload(ProjectDirSQL.datasets).subqueryload(
            DatasetSQL.event_maps).subqueryload(SystemEventMapSQL.event_map_quantiles)).order_by(
        SystemSQL.id).all()

    # print(systems)

    # Get the inspect tables
    event_map_quantiles = {}
    records = []
    for system in systems:
        # print(system.system_name)
        for project in system.projects:
            for dataset in project.datasets:
                for event_map in dataset.event_maps:
                    quantiles = event_map.event_map_quantiles
                    # print(quantiles)
                    if not quantiles:
                        continue
                    event_map_quantiles[(system.system_name, project.project_name, dataset.dtag,
                                         event_map.event_idx)] = event_map.event_map_quantiles
                    records.append(
                        {
                            "System": system.system_name,
                            "Project": project.project_name,
                            "Dtag": dataset.dtag,
                            "Event IDX": event_map.event_idx,
                            "Event Map > 1 Quantile 0.5": quantiles.event_50,
                            "Event Map > 1 Quantile 0.75": quantiles.event_75,
                            "Event Map > 1 Quantile 0.9": quantiles.event_90,
                            "Base Map > 1 Quantile 0.5": quantiles.base_50,
                            "Base Map > 1 Quantile 0.75": quantiles.base_75,
                            "Base Map > 1 Quantile 0.9": quantiles.base_90,
                            "Difference at Quantile 0.5": quantiles.event_50 - quantiles.base_50,
                            "Difference at Quantile 0.75": quantiles.event_75 - quantiles.base_75,
                            "Difference at Quantile 0.9": quantiles.event_90 - quantiles.base_90,
                            "1-BDC": event_map.bdc,
                            "Event Map > 1": quantiles.event_greater_than_1,
                            "Event Map > 2": quantiles.event_greater_than_2,
                            "Event Map > 3": quantiles.event_greater_than_3,
                            "Base Map > 1": quantiles.base_greater_than_1,
                            "Base Map > 2": quantiles.base_greater_than_2,
                            "Base Map > 3": quantiles.base_greater_than_3,

                        }
                    )

    table = pd.DataFrame(records)
    # print(table)
    # print(table[table["Event Map > 1 Quantile 0.9"] < 1.0])

    # table_without_outliers = table[~table["System"].isin(["XX02KALRNA", "SHMT2A", "B2m", "PHIPA", "CD44MMA"])]

    low_bdc_projects = table[(table["1-BDC"] > 0.95)]["Project"].unique()
    low_difference_projects = table[(table["Difference at Quantile 0.75"] < -0.5)]["Project"].unique()
    table_without_outliers = table[
        (~table["Project"].isin(low_bdc_projects))
        & (~table["Project"].isin(low_difference_projects))
        & (~table["Project"].isin(["refmac-from-coot-refmac-for", "TcHRS"]))
    ]

    print(table_without_outliers[table_without_outliers["Difference at Quantile 0.75"] < -0.2])

    graph = sns.regplot(
        # data=table_without_outliers[table_without_outliers["Event Map > 1 Quantile 0.9"] > 0.7],
        data=table_without_outliers,
        x="1-BDC",
        y="Difference at Quantile 0.75",
        line_kws={"color": "r"})
    plt.tight_layout()
    graph.get_figure().savefig(output_dir / "EventMapsQuantileComparison.png")
    plt.cla()
    plt.clf()
    plt.close("all")

    X_test = X_train = table_without_outliers["Base Map > 1 Quantile 0.75"].to_numpy().reshape(-1,1)
    y_test = y_train = table_without_outliers["Event Map > 1 Quantile 0.75"].to_numpy().reshape(-1,1)

    # Create linear regression object
    regr = linear_model.LinearRegression()

    # Train the model using the training sets
    regr.fit(X_train, y_train)

    # Make predictions using the testing set
    y_pred = regr.predict(X_test)

    # The coefficients
    print(f"Coefficients: {regr.intercept_} {regr.coef_}\n" )
    # The coefficient of determination: 1 is perfect prediction
    print(f"R Squared: {r2_score(y_test, y_pred)}")


if __name__ == "__main__":
    fire.Fire(plot_xchem_dataset_summaries)
