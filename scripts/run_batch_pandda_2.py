from pathlib import Path

import fire

from pandda_lib.command import PanDDA2Command, TryMake, ShellCommand
from pandda_lib.distribution import ClusterHTCondor


def main(analyse_path, data_dirs, pandda_dirs, cores_per_worker=12, mem_per_core=10,
         pdb_regex="dimple.pdb",
         mtz_regex="dimple.mtz",
         structure_factors_f="FWT",
         structure_factors_phi="PHWT",
         autobuild=True,
         global_processing="distributed",
         distributed_scheduler="HTCONDOR",
         ):

    data_dirs = Path(data_dirs)
    pandda_dirs = Path(pandda_dirs)

    TryMake(pandda_dirs)()

    htcondor = ClusterHTCondor(
        cores_per_worker=cores_per_worker,
        distributed_mem_per_core=mem_per_core
    )

    for pandda_dir in data_dirs.glob("*"):
        pandda_dir = pandda_dirs / pandda_dir.name
        TryMake(pandda_dir)()

        pandda_command = PanDDA2Command(
            analyse_path=analyse_path,
            data_dirs=pandda_dir,
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
        )
        print(pandda_command.command)

        # htcondor.submit(pandda_command.run)


if __name__ == "__main__":
    fire.Fire(main)
