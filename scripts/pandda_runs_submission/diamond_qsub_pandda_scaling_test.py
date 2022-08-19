from pathlib import Path
import shutil

import fire

from pandda_lib.schedulers.qsub_scheduler import QSubScheduler
from pandda_lib.jobs.pandda_job import PanDDAJob


def main(
        data_dir: str,
        output_dir: str,
        total_mem: int = 220,
        fresh=False,
        remove=False,
):
    print("Starting")
    # Define data
    data_dir = Path(data_dir)
    output_dir = Path(output_dir)

    # Get Scheduler
    scheduler = QSubScheduler(output_dir)

    # Submit jobs
    for cores in [1, 3, 6, 12, 18, 24, 30, 36, 42, 48, 54, 60]:
        print(f"\tcores: {cores}")

        mem_per_core = int(total_mem / cores)
        print(f"\tmemory per core: {mem_per_core}")

        pandda_output_dir = Path(output_dir) / f"{cores}"

        # Handle existing runs
        if fresh and pandda_output_dir.exists():
            shutil.rmtree(pandda_output_dir)
        if pandda_output_dir.exists() and not fresh:
            continue
        if remove:
            shutil.rmtree(pandda_output_dir)
            continue

        job = PanDDAJob(
            name=str(cores),
            system_data_dir=data_dir,
            output_dir=pandda_output_dir,
            cores=cores,
        )
        scheduler.submit(job, cores=cores, mem_per_core=mem_per_core)


if __name__ == "__main__":
    fire.Fire(main)
