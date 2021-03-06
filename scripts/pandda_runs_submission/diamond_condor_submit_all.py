import os
from pathlib import Path
import time
import shutil
import pickle
import subprocess

import fire
import htcondor

# SINGULARITY_SCRIPT = """#!/bin/bash
#
# singularity exec -B /opt,/tmp --writable-tmpfs /opt/clusterdata/pandda/containers/pandda.sif bash {pandda_script}
# """

SINGULARITY_SCRIPT = """#!/bin/bash

singularity exec -B /opt {personal_container_path} bash {pandda_script}
"""

SCRIPT = """#!/bin/bash

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

python /xtal_software/pandda_2_gemmi/pandda_gemmi/analyse.py {data_dirs} {out_dir} --pdb_regex='dimple.pdb' --mtz_regex='dimple.mtz' --structure_factors='("FWT","PHWT")' --autobuild=True --global_processing='serial' --local_cpus=12 --distributed_mem_per_core=10 --distributed_scheduler='HTCONDOR' --distributed_tmp=/opt/clusterdata/pandda/tmp --rank_method=autobuild --comparison_strategy="high_res" 

echo "done pandda" 
"""


def main(container_path: str):
    print("Starting")
    # Define data
    container_path = Path(container_path)
    data_dirs = Path('/opt/clusterscratch/pandda/data')
    results_dirs = Path('/opt/clusterscratch/pandda/output/pandda_results')
    ignores = ['containers', 'pandda_results', 'scripts']

    # Get Scheduler
    print("Getting scheduler")
    schedd = htcondor.Schedd()  # get the Python representation of the scheduler

    # Loop over PanDDA data dirs
    # print("Globbing...")
    # paths_dir = Path("/tmp/paths.pickle")
    # if paths_dir.exists():
    #     with open(paths_dir, "rb") as f:
    #         paths = pickle.load(f)
    # else:
    #     paths = [path for path in data_dirs.glob("*")]
    #     with open(paths_dir, "wb") as f:
    #         pickle.dump(paths, f)
    paths = [path for path in data_dirs.glob("*")]

    jobs = {}

    for data_dir in paths:
        print(f"\tProcessing: {data_dir}")
        system_name = data_dir.name
        print(f"\t\tSystem name is: {system_name}")
        out_dir = results_dirs / system_name
        print(f"\t\tOut dir is: {out_dir}")
        # If not a directory, continue
        if not data_dir.is_dir():
            continue

        # Check if should ignore because not pandda data
        if data_dir.name in ignores:
            print(f"\t\tSkipped: {data_dir}: not pandda data")
            continue

        # Check if should ignore because already got results
        event_table_file = out_dir / 'analyses' / 'pandda_analyse_events.csv'
        if event_table_file.exists():
            print(f"\t\tSkipped: {data_dir}: not already has analyse events")
            continue

        pandda_log_file = out_dir / 'pandda_log.json'
        if pandda_log_file.exists():
            print(f"\t\tSkipped: {data_dir}: already started!")
            continue

        # If not generate job script
        script = SCRIPT.format(
            data_dirs=str(data_dir),
            out_dir=str(out_dir),
        )
        print(f"\t\tScript is: {script}")

        # Write job script
        pandda_script_file = results_dirs / f"{system_name}.sh"
        with open(pandda_script_file, "w") as f:
            f.write(script)

        os.chmod(str(pandda_script_file), 0o777)

        # Generate the args for singularity
        # personal_container_path = results_dirs / f"{system_name}.sif"
        # if personal_container_path.exists():
        #     os.remove(str(personal_container_path))
        #
        # shutil.copy(str(container_path), str(personal_container_path))

        # p = subprocess.Popen(
        #     f"cp {str(container_path)} {str(personal_container_path)}",
        #     shell=True,)
        #
        # p.communicate()

        singularity_script = SINGULARITY_SCRIPT.format(
            # container_path=str(container_path),
            personal_container_path=str(container_path),
            pandda_script=str(pandda_script_file),
        )
        print(f"\t\tsingularity_script are: {singularity_script}")


        # Write singularity script
        singularity_script_file = results_dirs / f"{system_name}.singularity.sh"
        with open(singularity_script_file, "w") as f:
            f.write(singularity_script)

        os.chmod(str(singularity_script_file), 0o777)

        # Generate job
        job_dict = {
            "executable": f"{str(singularity_script_file)}",  # the program to run on the execute node
            # "arguments": arguments,
            "output": str(results_dirs / f"{system_name}.out"),  # anything the job prints to standard output will end up in this file
            "error": str(results_dirs / f"{system_name}.err"),  # anything the job prints to  standard error will end up in this file
            "log": str(results_dirs / f"{system_name}.log"),  # this file will contain a record  of what happened to the job
            "request_cpus": "47",  # how many CPU cores we want
            "request_memory": "300GB",  # how much memory we want
            # "request_disk": "300GB",
        }
        job = htcondor.Submit(job_dict)

        # Submit job
        submit_result = schedd.submit(job)
        print(f"\t\tSubmitted!")
        time.sleep(20)

        jobs[system_name] = submit_result

        query = schedd.query()

        # print(query)

        num_jobs = len(query)
        while num_jobs > 10:
            print(f"\t\t\tToo many jobs: {len(query)} at once, hold on there!")
            status = {_system_name: len(schedd.query(constraint=f"ClusterId == {_submit_result.cluster()}"))
                   for _system_name, _submit_result
                   in jobs.items()
                   }
            print(status)
            time.sleep(10)
            num_jobs = sum([length for length in status.values()])


if __name__ == "__main__":
    fire.Fire(main)
