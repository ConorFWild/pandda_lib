from pathlib import Path

import fire
# import pymongo

import subprocess
from dataclasses import dataclass

from pandda_lib import constants
from pandda_lib.fs import XChemDiamondFS

from pandda_lib.command.rsync import RsyncPanDDADirsToAWS

from joblib import Parallel, delayed

command_aws: str = "cp -R {path_to_local_dir} {path_to_remote_dir}"

@dataclass()
class RsyncPanDDADirsToAWS:
    # def __init__(self):
    command: str = command_aws

    def run(self):
        p = subprocess.Popen(
            self.command,
            shell=True,
        )

        p.communicate()

    @staticmethod
    def from_paths(
            path_to_remote_dir: Path,
            path_to_local_dir: Path,
            # password: str,
    ):
        return RsyncPanDDADirsToAWS(
            command_aws.format(
                path_to_remote_dir=path_to_remote_dir,
                path_to_local_dir=path_to_local_dir,
                # password=password,
            )
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

        rsync = RsyncPanDDADirsToAWS.from_paths(
            path_to_remote_dir=Path(doc[constants.mongo_diamond_paths_model_building_dir]),
            path_to_local_dir=Path('/opt/clusterdata') / doc[constants.mongo_diamond_paths_system_name],
        )

        docs.append(doc)
        rsyncs.append(rsync)

    # diamond_paths.insert_many(docs)



        print(rsync.command)

    Parallel(n_jobs=1)(
        delayed(
            rsync.run
        )() for doc in docs)


if __name__ == "__main__":
    fire.Fire(main)
