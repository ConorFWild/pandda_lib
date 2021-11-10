from pathlib import Path

import fire

from pandda_lib.command import EDSTATS

def main(data_dirs, mtz_regex="dimple.mtz", pdb_regex="dimple.pdb"):
    data_dirs = Path(data_dirs)

    records = []

    for data_dir in data_dirs.glob("*"):
        dtag = data_dir.name

        mtz_file = data_dir / mtz_regex
        pdb_file = data_dir / pdb_regex

        stats = EDSTATS(mtz_file, pdb_file)

        stats_results = stats.run()

        record = {
            "dtag": dtag,
        }

        exit()

        records.append(record)