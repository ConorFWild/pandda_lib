import fire

from pandda_lib.command import PanDDA2Command
from pandda_lib.distribution import ClusterHTCondor


def main(data_dirs, pandda_dirs, cores_per_worker, mem_per_core):
    htcondor = ClusterHTCondor(
        cores_per_worker=cores_per_worker,
        distributed_mem_per_core=mem_per_core
    )

    for pandda_dir in pandda_dirs.glob("*"):
        pandda_command = PanDDA2Command(
            data_dirs,
            out_dir,
            cpus,
            autobuild,

        )


if __name__ == "__main__":
    fire.Fire(main)
