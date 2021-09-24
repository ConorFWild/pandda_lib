from pathlib import Path

import fire

from pandda_lib.command import PanDDA2Command, TryMake, ShellCommand, TryRemove
from pandda_lib.distribution import ClusterHTCondor
from pandda_lib import constants


def main(analyse_path,
         data_dirs,
         pandda_dirs,
         jobs=1,
         main_cores_per_worker=6,
         main_mem_per_core=50,
         cores_per_worker=6,
         mem_per_core=10,
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
    pandda_dirs = Path(pandda_dirs)

    TryMake(pandda_dirs)()

    htcondor = ClusterHTCondor(
        jobs=jobs,
        cores_per_worker=main_cores_per_worker,
        distributed_mem_per_core=main_mem_per_core,
        log_directory=distributed_tmp,
    )

    commands = []

    callbacks = []
    for data_dir in data_dirs.glob("*"):
        pandda_dir = pandda_dirs / data_dir.name

        out_file = pandda_dir / constants.PANDDA_ANALYSES_DIR / constants.PANDDA_ANALYSE_EVENTS_FILE

        if not out_file.exists():
            print(f"\tNo event file for {data_dir.name} at {out_file}: submitting a cluster job!")

            TryRemove(pandda_dir)()
            TryMake(pandda_dir)()

            pandda_command = PanDDA2Command(
                analyse_path=analyse_path,
                data_dirs=data_dir,
                out_dir=pandda_dir,
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
                comparison_strategy="closest_cluster",
                cluster_selection="close",
                log_file=pandda_dirs / f"{data_dir.name}.txt",
            )
            print(f"\tPanDDA command for {data_dir.name}: {pandda_command.command}")

            # commands.append(ShellCommand(pandda_command.command))

            htcondor.submit(ShellCommand(pandda_command.command, ), lambda: f"{pandda_dir}")
            # callbacks.append(out_file.exists)
        else:
            print(f"\tEvent csv file: {out_file} already generated!")

    # print(f"Got {len(commands)} commands to submit...")
    #
    # from functools import partial
    #
    # def callback(_callbacks):
    #     return [_callback() for _callback in _callbacks]
    # # callback = lambda: [x() for x in callbacks]
    #
    # htcondor(commands, partial(callback, callbacks))


if __name__ == "__main__":
    fire.Fire(main)
