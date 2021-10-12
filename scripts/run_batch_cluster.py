from pathlib import Path

import fire

from pandda_lib.command import PanDDAClusterCommand, TryMake, ShellCommand, TryRemove
from pandda_lib.distribution import ClusterHTCondor
from pandda_lib import constants


def main(cluster_path,
         data_dirs,
         out_dirs,
         jobs=1,
         main_cores_per_worker=6,
         main_mem_per_core=25,
         cores_per_worker=6,
         mem_per_core=10,
         num_workers=25,
         pdb_regex="dimple.pdb",
         mtz_regex="dimple.mtz",
         structure_factors_f="FWT",
         structure_factors_phi="PHWT",
         autobuild=True,
         global_processing="distributed",
         distributed_scheduler="HTCONDOR",
         distributed_tmp="/data/share-2/conor/pandda/tmp",
         ):
    data_dirs = Path(data_dirs)
    out_dirs = Path(out_dirs)

    TryMake(out_dirs)()

    htcondor = ClusterHTCondor(
        jobs=jobs,
        cores_per_worker=main_cores_per_worker,
        distributed_mem_per_core=main_mem_per_core,
        log_directory=distributed_tmp,
    )

    commands = []

    callbacks = []
    for data_dir in data_dirs.glob("*"):
        out_dir = out_dirs / data_dir.name

        out_file = out_dir / constants.PANDDA_ANALYSES_DIR / constants.PANDDA_ANALYSE_EVENTS_FILE

        if not out_file.exists():
            print(f"\tNo event file for {data_dir.name} at {out_file}: submitting a cluster job!")

            TryRemove(out_dir)()
            TryMake(out_dir)()

            pandda_command = PanDDAClusterCommand(
                cluster_path=cluster_path,
                data_dirs=data_dir,
                out_dir=out_dir,
                pdb_regex=pdb_regex,
                mtz_regex=mtz_regex,
                structure_factors_f=structure_factors_f,
                structure_factors_phi=structure_factors_phi,
                autobuild=autobuild,
                global_processing=global_processing,
                distributed_scheduler=distributed_scheduler,
                local_cpus=cores_per_worker,
                mem_per_core=mem_per_core,
                distributed_tmp=distributed_tmp,
                rank_method="autobuild",
                comparison_strategy="high_res",
                cluster_selection="close",
                num_workers=num_workers,
                log_file=out_dirs / f"{data_dir.name}.txt",
            )
            print(f"\tPanDDA command for {data_dir.name}: {pandda_command.command}")

            # commands.append(ShellCommand(pandda_command.command))

            htcondor.submit(ShellCommand(pandda_command.command, ), lambda: f"{out_dir}")
            # callbacks.append(out_file.exists)
        else:
            print(f"\tEvent csv file: {out_file} already generated!")

if __name__ == "__main__":
    fire.Fire(main)
