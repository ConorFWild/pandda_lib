from pathlib import Path

import fire

from pandda_lib.command import EDSTATS


def main(data_dirs, mtz_regex="dimple.mtz", pdb_regex="dimple.pdb"):
    data_dirs = Path(data_dirs).resolve()

    records = []

    for data_dir in data_dirs.glob("*"):
        dtag = data_dir.name
        print(f"Processing dtag: {dtag}")

        mtz_file = data_dir / mtz_regex
        pdb_file = data_dir / pdb_regex

        if not mtz_file.exists():
            print("\tSkipping! No mtz!")

        if not pdb_file.exists():
            print("\tSkipping! No pdb!")


        stats = EDSTATS(mtz_file, pdb_file)

        stats_results = stats.run()

        record = {
            "dtag": dtag,
        }

        exit()

        records.append(record)


if __name__ == "__main__":
    fire.Fire(main)
