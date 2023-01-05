from pathlib import Path
import shutil
import json

import fire

from pandda_lib.schedulers.qsub_scheduler import QSubScheduler
# from pandda_lib.schedulers.condor_scheduler import HTCondorScheduler
from pandda_lib.jobs.pandda_job import PanDDAJob


def qsub_pandda(data_dirs,
                out_dir,
                working_dir,
                pdb_regex="dimple.pdb",
                mtz_regex="dimple.mtz",
                total_mem=220,
                cores=12,
                processor="multiprocessing_spawn",
                processor_global="serial",
                memory_availability="high",
                autobuild="True",
                autobuild_strategy="rhofit",
                rescore_event_method="autobuild_rscc"
                ):
    # Get the working dir
    data_dir = Path(data_dirs).resolve()
    working_dir = Path(working_dir).resolve()
    out_dir = Path(out_dir).resolve()

    # Get Scheduler
    scheduler = QSubScheduler(working_dir)
    # scheduler = HTCondorScheduler(working_dir)

    # Define the job
    job = PanDDAJob(
        name=f"pandda_{processor_global}_{processor}_{memory_availability}_{cores}",
        system_data_dir=data_dir,
        output_dir=out_dir,
        pdb_regex=pdb_regex,
        mtz_regex=mtz_regex,
        cores=cores,
        comparison_strategy="hybrid",
        event_score="inbuilt",
        rank_method="autobuild",
        autobuild=autobuild,
        memory_availability=memory_availability,
        debug="0",
        local_processing=processor,
        global_processing=processor_global,
        autobuild_strategy=autobuild_strategy,
        rescore_event_method=rescore_event_method
    )
    print(job.script)

    # Submit the job
    mem_per_core = int(total_mem / cores)
    scheduler.submit(job, cores=cores, mem_per_core=mem_per_core)


if __name__ == "__main__":
    fire.Fire(qsub_pandda)
