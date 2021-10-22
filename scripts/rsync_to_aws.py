from pathlib import Path

import fire
# import pymongo

import subprocess
from dataclasses import dataclass

from pandda_lib import constants
from pandda_lib.fs import XChemDiamondFS

from pandda_lib.command.rsync import RsyncPanDDADirsToAWS

from joblib import Parallel, delayed

command_aws: str = "cp -R -L {path_to_local_dir} {path_to_remote_dir}"

@dataclass()
class CpDirToAWS:
    # def __init__(self):
    # command: str = command_aws
    path_to_remote_dir :Path
    path_to_local_dir: Path

    def run(self):

        # Copy dir
        command = f"mkdir {self.path_to_remote_dir}"
        print(command)
        p = subprocess.Popen(
            command,
            shell=True,
        )
        p.communicate()

        dataset_paths = [path for path in self.path_to_local_dir.glob("*")]
        for path in dataset_paths:
            remote_dataset_path = self.path_to_remote_dir / path.name
            # cp dir
            # command = f"cp {str(path)} {str(remote_dataset_path)}"
            command = f"mkdir {str(remote_dataset_path)}"
            print(command)
            p = subprocess.Popen(
                command,
                shell=True,
            )
            p.communicate()

            # copy mtz
            local_mtz_path = path / "dimple.mtz"
            mtz_path = remote_dataset_path / "dimple.mtz"
            command = f"cp -L --remove-destination {str(local_mtz_path)} {str(mtz_path)}"
            print(command)
            p = subprocess.Popen(
                command,
                shell=True,
            )
            p.communicate()

            # copy pdb
            local_pdb_path = path / "dimple.pdb"
            pdb_path = remote_dataset_path / "dimple.pdb"
            command = f"cp -L --remove-destination {str(local_pdb_path)} {str(pdb_path)}"
            print(command)
            p = subprocess.Popen(
                command,
                shell=True,
            )
            p.communicate()

            # copy compound dir
            local_compound_dir = path / "compound"
            compound_dir_path = remote_dataset_path / "compound"
            command = f"cp -R {str(local_compound_dir)} {str(compound_dir_path)}"
            print(command)
            p = subprocess.Popen(
                command,
                shell=True,
            )
            p.communicate()


        # p = subprocess.Popen(
        #     self.command,
        #     shell=True,
        # )
        #
        # p.communicate()

    @staticmethod
    def from_paths(
            path_to_remote_dir: Path,
            path_to_local_dir: Path,
            # password: str,
    ):
        return CpDirToAWS(
            # command_aws.format(
            path_to_remote_dir=path_to_remote_dir,
            path_to_local_dir=path_to_local_dir,
                # password=password,
            # )
        )

def main(diamond_dir: str, output_dir: str):
    diamond_dir = Path(diamond_dir)
    print(f"Diamond dir is: {diamond_dir}")

    print("Getting XChemDiamondFS...")
    diamond_fs = XChemDiamondFS.from_path(diamond_dir)

    print(diamond_fs.pandda_dirs)

    print(diamond_fs.model_building_dirs)

    # print(",".join([str(x) for x in diamond_fs.model_building_dirs.values()]))
    # print(",".join([str(",") for x in diamond_fs.pandda_dirs.values()]))

    # client = pymongo.MongoClient()
    # print(client.server_info())

    # diamond_paths = client.pandda.diamond_paths
    # diamond_paths.drop()

    rsyncs = []
    docs = []
    for system_name, model_building_dir in diamond_fs.model_building_dirs.items():
        doc = {
            constants.mongo_diamond_paths_system_name: system_name.system_name,
            constants.mongo_diamond_paths_model_building_dir: str(model_building_dir),
            constants.mongo_diamond_paths_pandda_dirs: [str(x) for x in diamond_fs.pandda_dirs[system_name]],
        }
        print(
            doc
        )

        rsync = CpDirToAWS.from_paths(
            path_to_remote_dir=Path('/opt/clusterdata/pandda') / doc[constants.mongo_diamond_paths_system_name],
            path_to_local_dir=Path(doc[constants.mongo_diamond_paths_model_building_dir]),
        )

        docs.append(doc)
        rsyncs.append(rsync)

    # diamond_paths.insert_many(docs)



        # print(rsync.command)

    Parallel(n_jobs=20)(
        delayed(
            rsync.run
        )() for rsync in rsyncs)


if __name__ == "__main__":
    fire.Fire(main)
