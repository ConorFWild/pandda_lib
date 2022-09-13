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

import seaborn as sns

sns.set(rc={'figure.figsize': (2 * 11.7, 2 * 8.27)})
sns.set(font_scale=3)
# sns.color_palette("hls", 8)
sns.set_palette("hls")
sns.set_palette("crest")


def plot_rscc_vs_rmsd():
    sqlite_filepath = "/dls/science/groups/i04-1/conor_dev/pandda_lib/diamond_2.db"
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    output_path = pathlib.Path("/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2/pandda_autobuilding"
                               "/figures/rscc_vs_rmsd.png")

    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)

    print("\tGetting SQL data")
    panddas = session.query(PanDDADirSQL).options(subqueryload("*")).order_by(PanDDADirSQL.id).all()

    print("\tMaking table...")
    records = []
    for pandda in panddas:
        for dataset in pandda.pandda_dataset_results:
            event_scores = {}
            rmsds = {}

            # Get all the build rmsds in order to determine if
            for event in dataset.events:
                # rmsds = {}
                event_build_rsccs = {}
                for build in event.builds:
                    # print(build.score)
                    event_scores[build.id] = build.score

                    build_rscc = None
                    if build.rscc:
                        if build.rscc.score:
                            build_rscc = build.rscc.score
                            event_build_rsccs[build.id] = build.rscc.score

                    build_rmsd = None
                    if build.rmsd:
                        if build.rmsd.closest_rmsd:
                            build_rmsd = build.rmsd.closest_rmsd

                    records.append(
                        {
                            "System": pandda.system.system_name,
                            "Project": pandda.project.project_name,
                            "Dataset": dataset.dtag,
                            "Event": event.idx,
                            "Build ID": build.id,
                            "Score": build.score,
                            "RMSD": build_rmsd,
                            "RSCC": build_rscc
                        }
                    )

    build_rsccs_table = pd.DataFrame(records)

    print("\tMaking graph...")
    graph = sns.scatterplot(
        data=build_rsccs_table,
        x="RMSD",
        y="RSCC",
    )

    print("\tSaving graph...")
    graph.get_figure().savefig(output_path)


if __name__ == "__main__":
    fire.Fire(plot_rscc_vs_rmsd)


