from pathlib import Path
import shutil
import json

import fire

from pandda_lib.schedulers.qsub_scheduler import QSubScheduler
from pandda_lib.jobs.pandda_job import PanDDAJob


def qsub_pandda(data_dirs, out_dir, total_mem=220, cores=12, processor="ray", memory_availability="low"):
    # Get the working dir
    data_dir = Path(data_dirs).resolve()
    working_dir = Path(out_dir).resolve()

    # Get Scheduler
    scheduler = QSubScheduler(working_dir)

    # Define the job
    job = PanDDAJob(
        name=f"pandda_{processor}_{memory_availability}_{cores}.sh",
        system_data_dir=data_dir,
        output_dir=out_dir,
        pdb_regex="dimple.pdb",
        mtz_regex="dimple.mtz",
        cores=cores,
        comparison_strategy="hybrid",
        event_score="inbuilt",
        rank_method="autobuild",
        memory_availability=memory_availability,
        debug="1",
        local_processing=processor,
    )

    # Submit the job
    mem_per_core = int(total_mem / cores)
    scheduler.submit(job, cores=cores, mem_per_core=mem_per_core)


if __name__ == "__main__":
    fire.Fire(qsub_pandda)
