import os
from pathlib import Path

import fire

from pandda_lib import constants
from pandda_lib.command.rsync import RsyncDirToDiamond


def main(local_dir, remote_dir, password, dry=True):
    rsync_command = RsyncDirToDiamond.from_paths(
        Path(local_dir),
        Path(remote_dir),
        password
    )
    print(rsync_command.command)

    if not dry:
        rsync_command.run()
    else:
        print("Dry run! Not executing generated command")

if __name__ == "__main__":
    fire.Fire(main)
