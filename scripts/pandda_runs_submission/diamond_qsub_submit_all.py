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
from pandda_lib.diamond_sqlite.diamond_sqlite import Base, SystemDataDirSQL
from pandda_lib.schedulers.qsub_scheduler import QSubScheduler
from pandda_lib.jobs.pandda_job import PanDDAJob

SINGULARITY_SCRIPT = """#!/bin/bash

singularity exec -B /opt {personal_container_path} bash {pandda_script}
"""

SCRIPT_CONTAINER_PANDDA = """#!/bin/bash

export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python

echo "In container!" 

. /xtal_software/ccp4/ccp4-7.1/bin/ccp4.setup-sh 
echo "CCP4" 

. /usr/local/phenix-1.19.2-4158/phenix_env.sh 
echo "phenix" 
. /xtal_software/GPhL/BUSTER_snapshot_20210716/setup.sh 
echo "gph" 
. /xtal_software/anaconda/bin/activate 
echo "conda" 
conda activate pandda2 
echo "pandda" 
python -u /xtal_software/pandda_2_gemmi/pandda_gemmi/analyse.py --data_dirs={data_dirs} --out_dir={out_dir} --pdb_regex=\"dimple.pdb\" --mtz_regex=\"dimple.mtz\" --ligand_smiles_regex=\"[0-9a-zA-Z-]+[.]smiles\" --ligand_cif_regex=\"[0-9a-zA-Z-]+[.]cif\" --debug=5 --only_datasets=\"{only_datasets}\" --local_processing=\"multiprocessing_spawn\" --comparison_strategy=\"hybrid\"


echo "done pandda" 
"""


def main(
        # container_path: str,
        sqlite_filepath: str,
        output_dir: str,
):
    print("Starting")
    # Define data
    # container_path = Path(container_path)
    # data_dirs = Path('/opt/clusterscratch/pandda/data')
    # results_dirs = Path('/opt/clusterscratch/pandda/output/pandda_cluster_results')
    ignores = ['containers', 'pandda_results', 'scripts']

    # Get the database
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    os.remove(sqlite_filepath)
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()

    # Get Scheduler
    scheduler = QSubScheduler()

    # Submit jobs
    for system_data_dir in session.query(SystemDataDirSQL).order_by(SystemDataDirSQL.id):
        # print(f"{instance.system_name}: {instance.path}")

        job = PanDDAJob(
            system_data_dir = system_data_dir.path,
            output_dir = Path(system_data_dir).parent / output_dir,
        )
        scheduler.submit(job)




if __name__ == "__main__":
    fire.Fire(main)
