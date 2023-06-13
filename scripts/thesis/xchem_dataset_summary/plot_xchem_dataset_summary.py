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


def plot_xchem_dataset_summaries():
    sqlite_filepath = "/dls/science/groups/i04-1/conor_dev/pandda_lib/diamond_2.db"
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    output_dir = pathlib.Path("/dls/science/groups/i04-1/conor_dev/pandda_lib/thesis/xchem_dataset_summary")
    output_table = output_dir / "dataset_statistics.csv"
    try_make(output_dir)

    # Get the database
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    # tmp_dir = pathlib.Path(tmp_dir).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()

    # Get the information on each system

    initial_datasets: List[DatasetSQL] = session.query(DatasetSQL).options(
        subqueryload(DatasetSQL.bound_state_model)).order_by(
        DatasetSQL.id).all()

    # Make a table and save it
    if not output_table.exists():
        records = []
        for dataset in initial_datasets:

            # Add the dataset to the datasets by system
            system_name = get_system_from_dtag(dataset.dtag)

            pdb_path = dataset.model_path
            mtz_path = dataset.mtz_path
            bound_state = dataset.bound_state_model
            bound_state_path = dataset.pandda_model_path

            # Censor if partial
            accessible = True
            if (not pdb_path) or (pdb_path == "None"):
                accessible = False
            elif not pathlib.Path(pdb_path).exists():
                accessible = False
            if (not mtz_path) or (mtz_path == "None"):
                accessible = False
            elif not pathlib.Path(mtz_path).exists():
                accessible = False
            if bound_state_path and (bound_state_path != "None"):
                if not pathlib.Path(bound_state_path).exists():
                    accessible = False

            if accessible:

                # Load the structure to get the properties
                st = gemmi.read_structure(pdb_path)

                # Get the number of chains and residues
                num_chains = 0
                num_residues = 0
                for model in st:
                    for chain in model:
                        num_chains += 1
                        for res in chain:
                            num_residues += 1

                # Get the spacegroup
                sg = st.spacegroup_hm

                # Get the resolution
                resolution = st.resolution

                # Get the unit cell volume
                vol = st.cell.volume

                # Get the fragment properties if there is a bound state model
                if bound_state_path and (bound_state_path != "None"):

                    bound_st = gemmi.read_structure(bound_state_path)

                    # Get the hit status
                    hit = True

                    # Get the RSCC
                    rscc = bound_state.rscc

                    # Get the number of fragment atoms
                    num_frag_atoms = 0
                    for model in bound_st:
                        for chain in model:
                            for res in chain:
                                # Don't double count
                                if num_frag_atoms > 0:
                                    continue
                                if res.name in ["LIG", "XXX"]:
                                    for atom in res:
                                        if not atom.is_hydrogen():
                                            num_frag_atoms += 1

                else:
                    hit = False
                    rscc = None
                    num_frag_atoms = None
            else:
                resolution = None
                sg = None
                vol = None
                num_residues = None
                num_chains = None
                rscc = None
                hit = None
                num_frag_atoms = None

            record = {
                "System": system_name,
                "Dtag": dataset.dtag,
                "Resolution": resolution,
                "Spacegroup": sg,
                "Volume": vol,
                "Number of Residues": num_residues,
                "Number of Chains": num_chains,
                "Accessible": accessible,
                "Number of Fragment Heavy Atoms": num_frag_atoms,
                "RSCC": rscc,
                "Hit": hit

            }
            records.append(record)
            print(record)

        # Output table of records
        table = pd.DataFrame(records)
        table.to_csv(output_table)

    else:
        table = pd.read_csv(output_table)

    # Get the number of systems
    print(f"Num unique systems: {len(table['System'].unique())}")

    # Get the numbe of datasets
    print(f"Num unique datasets: {len(table['Dtag'].unique())}")

    # Get the number of accessible systems
    print(f"Num unique accessible datasets: {len(table[table['Accessible'] == True]['System'].unique())}")

    # Get the number of accessible datasets
    print(f"Num unique accessible datasets: {len(table[table['Accessible'] == True]['Dtag'].unique())}")



    # Make the plot of spacegroups
    print(table.value_counts("Spacegroup"))

    # Plot the Volume distribution
    graph = sns.ecdfplot(
        data=table[table['Accessible'] == True],
        x="Volume",
    )
    plt.tight_layout()
    graph.get_figure().savefig(output_dir / "XChemVolumeDistribution.png")
    plt.cla()
    plt.clf()
    plt.close("all")

    # Plot the hit RSCC distribution
    graph = sns.ecdfplot(
        data=table[(table['Accessible'] == True) & (table['RSCC'] > 0.0)],
        x="RSCC",
    )
    plt.tight_layout()
    graph.get_figure().savefig(output_dir / "XChemHitRSCCDistribution.png")
    plt.cla()
    plt.clf()
    plt.close("all")

    # Plot the hit rate distribution
    system_hit_rate_records = []
    for system in table[table["Accessible"] == True]["System"].unique():
        system_table = table[table["System"] == system]
        num_hits = len(system_table[system_table["RSCC"] > 0.0]["Dtag"].unique())
        num_datasets = len(system_table["Dtag"].unique())
        num_accessible_datasets = len(system_table[system_table["Accessible"] == True]["Dtag"].unique())
        if num_datasets != 0:
            system_hit_rate_records.append(
                {
                    "System": system,
                    "Hit Rate": num_hits / num_datasets,
                    "Number of Hits": num_hits,
                    "Number of Datasets": num_datasets,
                    "Number of Accessible Datasets": num_accessible_datasets
                })

    system_hit_rate_table = pd.DataFrame(system_hit_rate_records)

    # Get the number of systems with hit rate > 0
    print(f"Hit rate > 0: {len(system_hit_rate_table[system_hit_rate_table['Hit Rate'] > 0])}")
    print(f"Num datasets in systems with Hit rate > 0: {system_hit_rate_table[system_hit_rate_table['Hit Rate'] > 0]['Number of Datasets'].sum()}")
    print(f"Num accessible datasets in systems with Hit rate > 0: {system_hit_rate_table[system_hit_rate_table['Hit Rate'] > 0]['Number of Accessible Datasets'].sum()}")
    print(f"Num Hits: {system_hit_rate_table[system_hit_rate_table['Hit Rate'] > 0]['Number of Hits'].sum()}")
    print(f"")


    graph = sns.ecdfplot(
        data=system_hit_rate_table,
        x="Hit Rate",
    )
    plt.tight_layout()
    graph.get_figure().savefig(output_dir / "XChemHitRateDistribution.png")
    plt.cla()
    plt.clf()
    plt.close("all")

    graph = sns.ecdfplot(
        data=system_hit_rate_table[system_hit_rate_table['Hit Rate'] > 0],
        x="Hit Rate",
    )
    plt.tight_layout()
    graph.get_figure().savefig(output_dir / "XChemNonZeroHitRateDistribution.png")
    plt.cla()
    plt.clf()
    plt.close("all")

    # Plot the resolution distribution
    sns.set(rc={'figure.figsize': (2 * 11.7, 5 * 8.27)})
    sns.set(font_scale=3)

    print(table[(table['Accessible'] == True) & (table["Resolution"] < 4.5)].groupby(by="System").median().sort_values(
        by="Resolution"))

    graph = sns.boxplot(
        data=table[(table['Accessible'] == True) & (table["Resolution"] < 4.5)],
        x="Resolution",
        y="System",
        order=table[(table['Accessible'] == True) & (table["Resolution"] < 4.5)].groupby(by="System").median().sort_values(by="Resolution").index.to_numpy()
    )
    plt.tight_layout()
    graph.get_figure().savefig(output_dir / "XChemResolutionDistribution.png")
    plt.cla()
    plt.clf()
    plt.close("all")

    # Plot the resolution distribution
    sns.set(rc={'figure.figsize': (2 * 11.7, 5 * 8.27)})
    sns.set(font_scale=3)
    fig, ax = plt.subplots()

    # graph = sns.boxplot(
    #     data=system_hit_rate_table,
    #     x="Resolution",
    #     y="System"
    # )
    ax.barh(
        system_hit_rate_table.sort_values(by="Number of Accessible Datasets")["System"],
        system_hit_rate_table.sort_values(by="Number of Accessible Datasets")["Number of Hits"]
            )
    ax.barh(
        system_hit_rate_table.sort_values(by="Number of Accessible Datasets")["System"],
        system_hit_rate_table.sort_values(by="Number of Accessible Datasets")["Number of Accessible Datasets"],
        left=system_hit_rate_table.sort_values(by="Number of Accessible Datasets")["Number of Hits"]
    )
    ax.set_xscale('log')
    plt.xlabel("Count")
    plt.yaxis("System")
    plt.legend()
    plt.tight_layout()
    fig.savefig(output_dir / "HitVsDataset.png")
    plt.cla()
    plt.clf()
    plt.close("all")


    # Plot the RSCC distribution
    sns.set(rc={'figure.figsize': (2 * 11.7, 2 * 8.27)})
    sns.set(font_scale=3)

    graph = sns.ecdfplot(
        data=table[(table['Accessible'] == True) & (table['RSCC'] > 0.0) & (table["Number of Fragment Heavy Atoms"] >= 5)],
        x="Number of Fragment Heavy Atoms",
    )
    plt.tight_layout()
    graph.get_figure().savefig(output_dir / "XChemHitFragmentSizeDistribution.png")
    plt.cla()
    plt.clf()
    plt.close("all")

    #
    graph = sns.ecdfplot(
        data=table[table['Accessible'] == True],
        x="Number of Chains",
    )
    plt.tight_layout()
    graph.get_figure().savefig(output_dir / "XChemHitNumChainsDistribution.png")
    plt.cla()
    plt.clf()
    plt.close("all")

    #
    graph = sns.ecdfplot(
        data=table[table['Accessible'] == True],
        x="Number of Residues",
    )
    plt.tight_layout()
    graph.get_figure().savefig(output_dir / "XChemHitNumResiduesDistribution.png")
    plt.cla()
    plt.clf()
    plt.close("all")

    # Regplot the map uncertainty vs
    initial_datasets: List[DatasetSQL] = session.query(DatasetSQL).options(
        subqueryload(DatasetSQL.bound_state_model)).order_by(
        DatasetSQL.id).all()

    #     if not system_name:
    #         continue
    #
    #     if system_name not in datasets_by_system:
    #         datasets_by_system[system_name] = {}
    #     datasets_by_system[system_name][dataset.dtag] = dataset
    #
    # # For each system, get the relevant information and output a latex formated table
    # records = []
    # for system, system_datasets in datasets_by_system.items():
    #     print(f"### {system}")
    #
    #
    #     # Get number of datasets
    #     num_datasets = len(system_datasets)
    #     print(f"\tNumber of datasets: {num_datasets}")
    #
    #     # Determine whethere the system is accessible
    #     accessible_system_datasets = {}
    #     for dtag, ds in system_datasets.items():
    #         if pathlib.Path(ds.mtz_path).exists():
    #             accessible_system_datasets[dtag] = ds
    #
    #     print(f"\tNumber of accessible datasets: {len(accessible_system_datasets)}")
    #
    #     # Get minimum resolution
    #     system_dataset_resolutions = [ ]
    #     for ds in system_datasets.values():
    #         if pathlib.Path(ds.mtz_path).exists():
    #             system_dataset_resolutions.append(gemmi.read_mtz_file(ds.mtz_path).resolution_high())
    #         else:
    #             print(ds.mtz_path)
    #     if len(system_dataset_resolutions) == 0:
    #         continue
    #     min_res = min(system_dataset_resolutions)
    #
    #     # Get mean resolution
    #     mean_res = sum(system_dataset_resolutions) / len(system_dataset_resolutions)
    #
    #     # Get max resolution
    #     max_res = max(system_dataset_resolutions)
    #
    #     # Get % with bound state model
    #     project_hit_rate = len([x for x in system_datasets.values() if x.bound_state_model is not None]) / len(system_datasets)
    #
    #     # Get the organism
    #
    #     # Get the protein id
    #
    #     #
    #
    #     print([system, num_datasets, min_res, mean_res, max_res, project_hit_rate])


if __name__ == "__main__":
    fire.Fire(plot_xchem_dataset_summaries)
