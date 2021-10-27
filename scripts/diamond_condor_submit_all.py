import os
from pathlib import Path

import fire
import htcondor

# SINGULARITY_SCRIPT = """#!/bin/bash
#
# singularity exec -B /opt,/tmp --writable-tmpfs /opt/clusterdata/pandda/containers/pandda.sif bash {pandda_script}
# """

SINGULARITY_SCRIPT = """#!/bin/bash

singularity exec -B /opt /opt/clusterdata/pandda/containers/pandda.sif bash {pandda_script}
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

python /xtal_software/pandda_2_gemmi/pandda_gemmi/analyse.py {data_dirs} {out_dir} --pdb_regex='dimple.pdb' 
--mtz_regex='dimple.mtz' --structure_factors='("FWT","PHWT")' --autobuild=True --global_processing='serial' --local_cpus=12 --distributed_mem_per_core=10 --distributed_scheduler='HTCONDOR' --distributed_tmp=/opt/clusterdata/pandda/tmp --rank_method=autobuild --comparison_strategy="high_res" 

echo "done pandda" 
"""


def main():
    print("Starting")
    # Define data
    data_dirs = Path('/opt/clusterdata/pandda')
    results_dirs = Path('/opt/clusterdata/pandda/pandda_results')
    ignores = ['containers', 'pandda_results', 'scripts']

    # Get Scheduler
    print("Getting scheduler")
    schedd = htcondor.Schedd()  # get the Python representation of the scheduler

    # Loop over PanDDA data dirs
    print("Globbing...")
    for data_dir in data_dirs.glob("*"):
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
        singularity_script = SINGULARITY_SCRIPT.format(
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
            "output": f"/tmp/{system_name}.out",  # anything the job prints to standard output will end up in this file
            "error": f"/tmp/{system_name}.err",  # anything the job prints to standard error will end up in this file
            "log": f"/tmp/{system_name}.log",  # this file will contain a record of what happened to the job
            "request_cpus": "47",  # how many CPU cores we want
            "request_memory": "300GB",  # how much memory we want
            # "request_disk": "300GB",
        }
        job = htcondor.Submit(job_dict)

        # Submit job
        submit_result = schedd.submit(job)
        print(f"\t\tSubmitted!")


if __name__ == "__main__":
    fire.Fire(main)