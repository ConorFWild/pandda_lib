import os
from pathlib import Path

import fire
from pymongo import MongoClient

from pandda_lib import constants
from pandda_lib.command.rsync import RsyncDirToDiamond


def try_make(path: Path):
    print(f"Trying to make: {path}")
    try:
        os.mkdir(path)
    except Exception as e:
        print(e)


def main(local_dir, remote_dir, password):


        rsync_command = RsyncDirToDiamond.from_paths(
            Path(local_dir),
            Path(remote_dir),
            password
        )
        print(rsync_command.command)

        rsync_command.run()


if __name__ == "__main__":
    fire.Fire(main)
