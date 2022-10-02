from pathlib import Path
import shutil
import json

import fire

from pandda_lib.schedulers.qsub_scheduler import QSubScheduler
from pandda_lib.jobs.pandda_job import PanDDAJob

TARGET_KEY = "target"
WORKING_DIR_KEY = "working_dir"
DATA_DIR_KEY = "data_dir"
PDB_REGEX_KEY = "pdb_regex"
MTZ_REGEX_KEY = "mtz_regex"
OUT_DIR_FORMAT = "output_{target}"
JOB_SCRIPT_FORMAT = "{target}.sh"
CHMOD_COMMAND_FORMAT = "chmod 777 {script_path}"
QSUB_COMMAND = "qsub -pe smp 12 -l m_mem_free=30G -q medium.q -o {log_file} -e {err_file} {script_file}"
LOG_FILE_FORMAT = "{target}.log"
ERR_FILE_FORMAT = "{target}.err"


def main(
        targets_json_path: str,
        total_mem: int = 220,
        cores: int = 6,
):
    print("Starting")
    # Define data
    targets_json_path = Path(targets_json_path).resolve()

    # Get the targets json
    with open(targets_json_path, "r") as f:
        targets_dict = json.load(f)

    # Get the working dir
    data_working_dir = Path(targets_dict[WORKING_DIR_KEY]).resolve()
    working_dir = Path(targets_dict[WORKING_DIR_KEY]).resolve() / "pandda_2"

    # Get Scheduler
    scheduler = QSubScheduler(working_dir)

    # Submit jobs
    for target, target_info in targets_dict[TARGET_KEY].items():
        print(f"Target: {target}")

        data_dir = data_working_dir / target_info[DATA_DIR_KEY]
        out_dir = working_dir / OUT_DIR_FORMAT.format(target=target)

        # Cleanup possible old runs
        # try:
        #     shutil.rmtree(out_dir)
        # except Exception as e:
        #     print(e)
        if out_dir.exists():
            continue

        # Define the job
        job = PanDDAJob(
            name=f"pandda_{target}.sh",
            system_data_dir=data_dir,
            output_dir=out_dir,
            cores=6,
            pdb_regex=target_info[PDB_REGEX_KEY],
            mtz_regex=target_info[MTZ_REGEX_KEY],
        )

        # Submit the job
        mem_per_core = int(total_mem / cores)
        scheduler.submit(job, cores=cores, mem_per_core=mem_per_core)


if __name__ == "__main__":
    fire.Fire(main)
