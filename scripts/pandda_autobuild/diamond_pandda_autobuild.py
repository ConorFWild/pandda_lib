import json
import os
from pathlib import Path
import time
import shutil
import pickle
import subprocess
import pathlib

import fire
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs
from pandda_lib.diamond_sqlite.diamond_sqlite import Base, ProjectDirSQL, SystemSQL
from pandda_lib.schedulers.qsub_scheduler import QSubScheduler
from pandda_lib.jobs.pandda_job import PanDDAJob


def main(
        options_json="diamond_pandda_autobuild.json"
):
    with open(options_json, "r") as f:
        options = json.load(f)

    tmp_dir = options["tmp_dir"]
    sqlite_filepath = options["sqlite_filepath"]
    output_dir_name = options["output_dir_name"]
    fresh = options["fresh"]
    print(f"Frest: {fresh}")
    remove = options["remove"]
    print(f"remove: {remove}")
    cores = options["cores"]

    print("Starting")
    # Define data
    # container_path = Path(container_path)
    # data_dirs = Path('/opt/clusterscratch/pandda/data')
    # results_dirs = Path('/opt/clusterscratch/pandda/output/pandda_cluster_results')
    # ignores = ['containers', 'pandda_results', 'scripts']
    tmp_dir = pathlib.Path(tmp_dir).resolve()
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()

    # Get the database
    # os.remove(sqlite_filepath)
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()

    # Get Scheduler
    scheduler = QSubScheduler(tmp_dir)

    # Submit jobs
    for system in session.query(SystemSQL).order_by(SystemSQL.id):
        # if system.system_name != "PDE5":
        #     continue
        for project in system.projects:
            print(f"{system.system_name}")
            output_dir = tmp_dir / f"system_{system.system_name}_project_{project.project_name}"
            if (output_dir / "analyses" / "pandda_analyse_events.csv").exists():
                continue

            # Handle existing runs
            if fresh and output_dir.exists():
                if not (output_dir / "analyses" / "pandda_analyse_events.csv").exists():
                    shutil.rmtree(output_dir)
            if output_dir.exists() and not fresh:
                continue
            if remove:
                shutil.rmtree(output_dir)
                continue

            job = PanDDAJob(
                name=system.system_name,
                system_data_dir=Path(project.path),
                output_dir=output_dir,
                cores=cores,
                comparison_strategy="high_res_first",
                event_score="size",
                memory_availability="memory_availability",
            )
            scheduler.submit(job, cores=cores, mem_per_core=int(220 / cores))


if __name__ == "__main__":
    fire.Fire(main)
