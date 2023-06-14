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
    output_dir = pathlib.Path("/dls/science/groups/i04-1/conor_dev/pandda_lib/thesis/xchem_dataset_summary")
    try_make(output_dir)

    # Get the database
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()

    # Get the inspect tables

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
        inspect_tables[(instance.system.system_name, inspect_table_path)] = inspect_table

    # Concatenate viewed ones
    records = []
    used_systems = {}
    for inspect_table_key, inspect_table in inspect_tables.items():
        print(inspect_table_key)
        if len(inspect_table)*0.3 < len(inspect_table[inspect_table["Viewed"] == True]):
            num_hits = len(inspect_table[inspect_table["Ligand Confidence"] == "High"])
            print(num_hits)
            num_events = len(inspect_table[inspect_table["Viewed"] == True])
            print(num_events)
            if num_hits > 0:
                if inspect_table_key[0] in used_systems:
                    if used_systems[inspect_table_key[0]] > num_events:
                        continue
                records.append(
                    {
                        "System": inspect_table_key[0],
                        # "Experiment": inspect_table_key[1],
                        "Number of Hits": num_hits,
                        "Number of Events": num_events
                    }
                )
                used_systems[inspect_table_key[0]] = num_events
        else:
            print(f"Viewed: {len(inspect_table[inspect_table['Viewed'] == True])} / {len(inspect_table)}")

    system_hit_rate_table = pd.DataFrame(records)

    # Plot the hit rates
    sns.set(rc={'figure.figsize': (2 * 11.7, 5 * 8.27)})
    sns.set(font_scale=3)
    fig, ax = plt.subplots()

    # graph = sns.boxplot(
    #     data=system_hit_rate_table,
    #     x="Resolution",
    #     y="System"
    # )
    ax.barh(
        system_hit_rate_table.sort_values(by="Number of Events")["System"],
        system_hit_rate_table.sort_values(by="Number of Events")["Number of Hits"],
        label="Number of Hits"

    )
    ax.barh(
        system_hit_rate_table.sort_values(by="Number of Events")["System"],
        system_hit_rate_table.sort_values(by="Number of Events")["Number of Events"],
        left=system_hit_rate_table.sort_values(by="Number of Events")["Number of Hits"],
        label="Number of Events"
    )
    ax.set_xscale('log')
    plt.xlabel("Count")
    plt.ylabel("System")
    plt.legend()
    plt.tight_layout()
    fig.savefig(output_dir / "HitVsEvents.png")
    plt.cla()
    plt.clf()
    plt.close("all")

    for record in sorted(records, key=lambda _record: _record['Number of Events']):
        print(f"{record['System']} & {record['Number of Hits']} & {record['Number of Events']} & {round(record['Number of Hits'] / record['Number of Events'], 2)} \\\\")

    print(system_hit_rate_table['Number of Hits'].sum() / system_hit_rate_table['Number of Events'].sum())

if __name__ == "__main__":
    fire.Fire(plot_xchem_dataset_summaries)
