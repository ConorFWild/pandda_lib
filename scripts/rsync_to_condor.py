import os
from pathlib import Path

import fire
from pymongo import MongoClient

from pandda_lib import constants
from pandda_lib.command.rsync import RsyncDirs

def main(output_dir, password):
    client = MongoClient()

    mongo_diamond_paths = client[constants.mongo_pandda][constants.mongo_diamond_paths]

    output_dir = Path(output_dir).resolve()
    try:
        os.mkdir(output_dir)
    except Exception as e:
        print(e)

    model_dirs = output_dir / "model_dirs"
    try:
        os.mkdir(model_dirs)
    except Exception as e:
        print(e)

    for doc in mongo_diamond_paths.find():
        system_name = doc[constants.mongo_diamond_paths_system_name]
        model_building_dir = doc[constants.mongo_diamond_paths_model_building_dir]
        pandda_dirs = doc[constants.mongo_diamond_paths_pandda_dirs]

        # try:
        #     os.mkdir(output_dir / system_name)
        # except Exception as e:
        #     print(e)

        rsync_command = RsyncDirs.from_paths(
            Path(model_building_dir),
            output_dir / system_name,
            password
        )
        print(rsync_command.command)



if __name__ == "__main__":
    fire.Fire(main)
