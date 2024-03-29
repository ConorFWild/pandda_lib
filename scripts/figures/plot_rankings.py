import pathlib
import os

import numpy as np
import pandas as pd

import fire
from sqlalchemy.orm import sessionmaker, subqueryload
from sqlalchemy import create_engine

from pandda_lib import constants
from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs
from pandda_lib.fs.pandda_result import PanDDAResult
from pandda_lib.diamond_sqlite.diamond_sqlite import *
import gemmi

from matplotlib import pyplot as plt

import seaborn as sns

sns.set(rc={'figure.figsize': (2 * 11.7, 2 * 8.27)})
sns.set(font_scale=3)
# sns.color_palette("hls", 8)
sns.set_palette("hls")
sns.set_palette("crest")


def inspect_table_cumulative_hits_table(inspect_table):
    rank_records = []
    cumulative_hits = 0
    sorted_inspect_table = inspect_table.sort_values("cluster_size", ascending=False)
    rank = 1

    for index, row in sorted_inspect_table.iterrows():
        # rank = index
        if row["Ligand Confidence"] != "Low":
            cumulative_hits += 1
        dtag = row["dtag"]
        event_idx = row["event_idx"]
        rank += 1
        rank_records.append({"Rank": rank, "Cumulative Hits": cumulative_hits, "Dtag": dtag, "Event IDX": event_idx})

    return pd.DataFrame(rank_records)


def rank_table_from_pandda_rsccs(pandda_2_sql, inspect_table):
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

            # Determine if it is a hit
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
                "Hit?": _hit,
                "Has Builds?": has_builds,
                "Build Path": build_path,
                "Event Map Path": event.event_map_path
            }
            records.append(record)
    table = pd.DataFrame(records).sort_values("RSCC", ascending=False)

    rank_records = []
    cumulative_hits = 0
    rank = 1

    for index, row in table.iterrows():
        # Skip any events not in inspect tavle
        if not row["Dtag"] in inspect_table["dtag"].unique():
            continue

        # Check if it is a hit
        if row["Hit?"]:
            cumulative_hits += 1
        dtag = row["Dtag"]
        event_idx = row["Event IDX"]
        rank += 1
        rank_records.append({"Rank": rank, "Cumulative Hits": cumulative_hits, "Dtag": dtag, "Event IDX": event_idx,
                             "RSCC": row["RSCC"], "Has Builds?": row["Has Builds?"],
                             "Build Path": row["Build Path"],
                             "Event Map Path": row["Event Map Path"]
                             })

    return pd.DataFrame(rank_records)


def inspect_table_cumulative_hits_table_first_dtag_hit(inspect_table):
    rank_records = []
    cumulative_hits = 0
    sorted_inspect_table = inspect_table.sort_values("cluster_size", ascending=False)
    rank = 1

    used_dtags = []

    for index, row in sorted_inspect_table.iterrows():
        # rank = index
        dtag = row["dtag"]
        event_idx = row["event_idx"]
        rank += 1

        if (row["Ligand Confidence"] != "Low") & (row["Ligand Confidence"] != "low") & (dtag not in used_dtags):
            cumulative_hits += 1
            used_dtags.append(dtag)

        rank_records.append({"Rank": rank, "Cumulative Hits": cumulative_hits, "Dtag": dtag, "Event IDX": event_idx})

    return pd.DataFrame(rank_records)


def rank_table_from_pandda_rsccs_first_dtag_hit(pandda_2_sql, inspect_table):
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
    rank = 1
    used_dtags = []

    for index, row in table.iterrows():
        dtag = row["Dtag"]
        event_idx = row["Event IDX"]

        # Skip any events not in inspect tavle
        if not dtag in inspect_table["dtag"].unique():
            continue

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


def inspect_table_cumulative_hits_table_first_dtag_hit_shared(inspect_table, pandda_2_sql):
    rank_records = []
    cumulative_hits = 0
    sorted_inspect_table = inspect_table.sort_values("cluster_size", ascending=False)
    rank = 1

    pandda_2_dtags = [dataset.dtag for dataset in pandda_2_sql.pandda_dataset_results]

    used_dtags = []

    for index, row in sorted_inspect_table.iterrows():
        # rank = index
        dtag = row["dtag"]
        event_idx = row["event_idx"]

        if dtag not in pandda_2_dtags:
            continue

        rank += 1

        if (row["Ligand Confidence"] != "Low") & (row["Ligand Confidence"] != "low") & (dtag not in used_dtags):
            cumulative_hits += 1
            used_dtags.append(dtag)

        rank_records.append({"Rank": rank, "Cumulative Hits": cumulative_hits, "Dtag": dtag, "Event IDX": event_idx})

    return pd.DataFrame(rank_records)


def rank_table_from_pandda_rsccs_first_dtag_hit_shared(pandda_2_sql, inspect_table):
    inspect_table_hit_dtags = []
    for index, row in inspect_table.iterrows():
        if (row["Ligand Confidence"] != "Low") & (row["Ligand Confidence"] != "low"):
            inspect_table_hit_dtags.append(row["dtag"])

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
            _inspect_hit = False
            # if rmsd:
            #     if rmsd < 6:
            #         _hit = True

            if pandda_dataset.dtag in inspect_table_hit_dtags:
                _inspect_hit = True

            #
            _database_hit = False
            if pandda_dataset.dataset:
                if pandda_dataset.dataset.pandda_model_path != "None":
                    _database_hit = True

            #

            has_builds = False
            if len(event.builds) != 0:
                has_builds = True

            record = {
                "Dtag": pandda_dataset.dtag,
                "Event IDX": event.idx,
                # "Score": event_scores[event_idx],
                "RSCC": rscc,
                "RMSD": rmsd,
                "Inspect Hit?": _inspect_hit,
                "Database Hit?": _database_hit,
                "Has Builds?": has_builds,
                "Build Path": build_path,
                "Event Map Path": event.event_map_path
            }
            records.append(record)
    table = pd.DataFrame(records).sort_values("RSCC", ascending=False)

    rank_records = []
    cumulative_hits = 0
    rank = 1
    used_dtags = []

    for index, row in table.iterrows():
        dtag = row["Dtag"]
        event_idx = row["Event IDX"]

        # Skip any events not in inspect tavle
        if not dtag in inspect_table["dtag"].unique():
            continue

        # Skip any known hit datasets not annotated as such in the inspect table
        if row["Database Hit?"]:
            if not row["Inspect Hit?"]:
                continue

        rank += 1

        # Check if it is a hit
        if row["Inspect Hit?"] & (dtag not in used_dtags):
            cumulative_hits += 1
            used_dtags.append(dtag)

        rank_records.append({"Rank": rank, "Cumulative Hits": cumulative_hits, "Dtag": dtag, "Event IDX": event_idx,
                             "RSCC": row["RSCC"], "RMSD": row["RMSD"], "Has Builds?": row["Has Builds?"],
                             "Build Path": row["Build Path"],
                             "Event Map Path": row["Event Map Path"]
                             })

    return pd.DataFrame(rank_records)


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


def plot_rankings():
    # Get the table
    sqlite_filepath = "/dls/science/groups/i04-1/conor_dev/pandda_lib/diamond_2.db"
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)

    # get the inspect tables
    projects = session.query(ProjectDirSQL).order_by(ProjectDirSQL.id).all()

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

    # Get the PanDDAs
    panddas = session.query(PanDDADirSQL).options(subqueryload("*")).order_by(PanDDADirSQL.id).all()

    # Make the figures
    # output_path = pathlib.Path(
    #     "/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2/pandda_autobuilding/ranking_figures_rscc")
    # table_output_path = pathlib.Path(
    #     "/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2/pandda_autobuilding/ranking_tables_rscc")
    # for pandda in panddas:
    #     print(f"PanDDA: {pandda.system.system_name}: {pandda.project.project_name}")
    #     test_pandda = pandda
    #     if not (test_pandda.system.system_name, test_pandda.project.project_name) in inspect_tables:
    #         continue
    #     pandda_inspect_tables = inspect_tables[(test_pandda.system.system_name, test_pandda.project.project_name)]
    #     for inspect_table_path, inspect_table in pandda_inspect_tables.items():  # = list(inspect_tables[(test_pandda.system.system_name, test_pandda.project.project_name)].values())[0]
    #         default_rank_table = inspect_table_cumulative_hits_table_first_dtag_hit_shared(inspect_table, test_pandda)
    #
    #         default_rank_table_path = table_output_path / f"{pandda.system.system_name}_" \
    #                                                       f"{pandda.project.project_name}_" \
    #                                                       f"{inspect_table_path.parent.parent.name}_" \
    #                                                       f"default.csv"
    #         default_rank_table.to_csv(default_rank_table_path)
    #
    #         default_rank_table["hue"] = "Size ranking"
    #         build_score_rank_table = rank_table_from_pandda_rsccs_first_dtag_hit_shared(test_pandda, inspect_table)
    #         build_rank_table_path = table_output_path / f"{pandda.system.system_name}_" \
    #                                                     f"{pandda.project.project_name}_" \
    #                                                     f"{inspect_table_path.parent.parent.name}_" \
    #                                                     f"build.csv"
    #         build_score_rank_table.to_csv(build_rank_table_path)
    #         build_score_rank_table["hue"] = "Build Ranking"
    #
    #         if len(build_score_rank_table) == 0:
    #             print(f"\tNO RSCCS FOR {inspect_table_path}! SKIPPING!")
    #             continue
    #
    #         figure_path = output_path / f"{pandda.system.system_name}_{pandda.project.project_name}_{inspect_table_path.parent.parent.name}.png"
    #         sns.lineplot(
    #             data=pd.concat(
    #                 [
    #                     build_score_rank_table.query("RSCC > 0"),
    #                     default_rank_table
    #                 ],
    #                 ignore_index=True,
    #             ),
    #             y="Cumulative Hits",
    #             x="Rank",
    #             hue="hue",
    #             palette="tab10"
    #         ).get_figure().savefig(figure_path)
    #
    #         # raise Exception()
    #
    #         plt.cla()
    #         plt.clf()
    #         plt.close("all")

    # Make the self comparison figures
    print("Making output dirs...")
    output_path = pathlib.Path(
        "/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2/pandda_autobuilding"
        "/ranking_figures_size_vs_rscc")
    if not output_path.exists():
        os.mkdir(output_path)
    table_output_path = pathlib.Path(
        "/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2/pandda_autobuilding"
        "/ranking_tables_size_vs_rscc")
    if not table_output_path.exists():
        os.mkdir(table_output_path)

    print("Outputting tables and figures...")
    for pandda in panddas:
        print(f"PanDDA: {pandda.system.system_name}: {pandda.project.project_name}")
        test_pandda = pandda

        # Get the size ranking table
        default_rank_table = rank_table_from_pandda_sizes_first_dtag(test_pandda)

        default_rank_table_path = table_output_path / f"{pandda.system.system_name}_" \
                                                      f"{pandda.project.project_name}_" \
                                                      f"size.csv"
        default_rank_table.to_csv(default_rank_table_path)

        default_rank_table["hue"] = "Size ranking"

        # Get the build ranking table
        build_score_rank_table = rank_table_from_pandda_rsccs_first_dtag(test_pandda)
        build_rank_table_path = table_output_path / f"{pandda.system.system_name}_" \
                                                    f"{pandda.project.project_name}_" \
                                                    f"build.csv"
        build_score_rank_table.to_csv(build_rank_table_path)
        build_score_rank_table["hue"] = "Build Ranking"

        if len(build_score_rank_table) == 0:
            print(f"\tNO RSCCS FOR {pandda.system.system_name}: {pandda.project.project_name}! SKIPPING!")
            continue

        figure_path = output_path / f"{pandda.system.system_name}_{pandda.project.project_name}.png"
        sns.lineplot(
            data=pd.concat(
                [
                    build_score_rank_table.query("RSCC > 0"),
                    default_rank_table
                ],
                ignore_index=True,
            ),
            y="Cumulative Hits",
            x="Rank",
            hue="hue",
            palette="tab10"
        ).get_figure().savefig(figure_path)

        # raise Exception()

        plt.cla()
        plt.clf()
        plt.close("all")

    print("Done!")


if __name__ == "__main__":
    fire.Fire(plot_rankings)
