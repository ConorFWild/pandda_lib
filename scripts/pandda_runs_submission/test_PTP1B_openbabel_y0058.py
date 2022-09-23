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
QSUB_COMMAND = "qsub -pe smp 3 -l m_mem_free=30G -q medium.q -o {log_file} -e {err_file} {script_file}"
LOG_FILE_FORMAT = "{target}.log"
ERR_FILE_FORMAT = "{target}.err"


def test_PTP1B_openbabel_y0058():
    # Get Scheduler
    scheduler = QSubScheduler(working_dir)

    # Submit jobs
    print(f"Target: {target}")

    data_dir = "/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2/pandda_2_reproduce_cluster4x/data_PTP1B"
    out_dir = Path("/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2" \
                   "/pandda_2_reproduce_cluster4x" \
                   "/test_PTP1B")

    # Cleanup possible old runs
    if out_dir.exists():
        return

    # Define the job
    job = PanDDAJob(
        name=f"test_PTP1B",
        system_data_dir=data_dir,
        output_dir=out_dir,
        pdb_regex=target_info[PDB_REGEX_KEY],
        mtz_regex=target_info[MTZ_REGEX_KEY],
    )

    # Submit the job
    mem_per_core = int(total_mem / cores)
    scheduler.submit(job, cores=cores, mem_per_core=mem_per_core)


if __name__ == "__main__":
    fire.Fire(test_PTP1B_openbabel_y0058)
