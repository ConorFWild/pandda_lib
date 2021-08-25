import os
from pathlib import Path

import fire
from pymongo import MongoClient

from pandda_lib import constants
from pandda_lib.command.rsync import RsyncDirs


def try_make(path: Path):
    print(f"Trying to make: {path}")
    try:
        os.mkdir(path)
    except Exception as e:
        print(e)


def main(output_dir, password):
    client = MongoClient()

    mongo_diamond_paths = client[constants.mongo_pandda][constants.mongo_diamond_paths]

    output_dir = Path(output_dir).resolve()
    try_make(output_dir)

    model_dirs = output_dir / "model_dirs"
    try_make(model_dirs)

    target_pandda_dirs = output_dir / "panddas"
    try_make(target_pandda_dirs)

    for doc in mongo_diamond_paths.find():
        system_name = doc[constants.mongo_diamond_paths_system_name]
        model_building_dir = doc[constants.mongo_diamond_paths_model_building_dir]
        pandda_dirs = doc[constants.mongo_diamond_paths_pandda_dirs]

        #
        system_pandda_dir = target_pandda_dirs / system_name
        try_make(system_pandda_dir)
        for pandda_dir in pandda_dirs:
            rsync_command = RsyncDirs.from_paths(
                Path(pandda_dir),
                system_pandda_dir,
                password
            )
            print(rsync_command.command)
            rsync_command.run()

        #
        system_model_dir = model_dirs / system_name
        try_make(system_model_dir)

        rsync_command = RsyncDirs.from_paths(
            Path(model_building_dir),
            output_dir / system_name,
            password
        )
        print(rsync_command.command)

        rsync_command.run()


if __name__ == "__main__":
    fire.Fire(main)
