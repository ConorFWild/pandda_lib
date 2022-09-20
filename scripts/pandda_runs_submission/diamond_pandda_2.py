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
        options_json="diamond_pandda_2.json"
):
    # Open the options json
    with open(options_json, "r") as f:
        options = json.load(f)

    # Set the parameters
    tmp_dir = options["tmp_dir"]

    sqlite_filepath = options["sqlite_filepath"]
    # fresh = options["fresh"]
    # print(f"Frest: {fresh}")
    # remove = options["remove"]
    # print(f"remove: {remove}")
    cores = options["cores"]

    print("Starting")

    # Define data
    tmp_dir = pathlib.Path(tmp_dir).resolve()
    print(f"Tmp Dir is: {tmp_dir}")

    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    output_dir = pathlib.Path(options["output_dir"]).resolve()

    print(f"Output Dir is: {output_dir}")


    # Get the database
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()

    # Get Scheduler
    scheduler = QSubScheduler(tmp_dir)

    # Submit jobs
    for system in session.query(SystemSQL).order_by(SystemSQL.id):
        print(f"{system.system_name}")

        for project in system.projects:
            print(f"\t{project.project_name}")

            # Define the output dir
            project_output_dir = tmp_dir / f"system_{system.system_name}_project_{project.project_name}"

            if not project_output_dir.exists():
                os.mkdir(project_output_dir)

            # Skip existing runs
            if (project_output_dir / "analyses" / "pandda_analyse_events.csv").exists():
                continue

            # # Handle existing runs
            # if fresh and output_dir.exists():
            #     if not (output_dir / "analyses" / "pandda_analyse_events.csv").exists():
            #         shutil.rmtree(output_dir)
            # if output_dir.exists() and not fresh:
            #     continue
            # if remove:
            #     shutil.rmtree(output_dir)
            #     continue

            # Make the job
            name = f"system_{system.system_name}_project_{project.project_name}"
            system_data_dir = Path(project.path)

            print(f"\t\tJob Name: {name}")
            print(f"\t\tSystem Data Dir: {system_data_dir}")
            print(f"\t\tOutput Dir: {project_output_dir}")
            # print(f"\t\tJob Name: {name}")

            job = PanDDAJob(
                name=name,
                system_data_dir=system_data_dir,
                output_dir=project_output_dir,
                cores=cores,
            )

            # Submit the job
            scheduler.submit(job, cores=cores, mem_per_core=int(220 / cores))


if __name__ == "__main__":
    fire.Fire(main)
