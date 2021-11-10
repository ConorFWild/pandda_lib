from pathlib import Path

import pandas as pd
import seaborn as sns
import fire

from pandda_lib.command import EDSTATS


def main(data_dirs, output_plot_file, mtz_regex="dimple.mtz", pdb_regex="dimple.pdb"):
    data_dirs = Path(data_dirs).resolve()
    output_plot_file = Path(output_plot_file).resolve()

    records = []

    j = 0
    for data_dir in data_dirs.glob("*"):
        if j > 10:
            break
        dtag = data_dir.name
        print(f"Processing dtag: {dtag}")

        mtz_file = data_dir / mtz_regex
        pdb_file = data_dir / pdb_regex

        if not mtz_file.exists():
            print("\tSkipping! No mtz!")

        if not pdb_file.exists():
            print("\tSkipping! No pdb!")

        stats = EDSTATS(mtz_file, pdb_file)

        rsccs = stats.run()

        for rscc in rsccs:
            record = {
                "dtag": dtag,
                "rscc": rscc,
            }

            records.append(record)

    table = pd.DataFrame(records)

    p = sns.catplot(
        x='dtag',
        y='rscc',
        data=table,
        kind='violin'
    )

    p.savefig(output_plot_file)


if __name__ == "__main__":
    fire.Fire(main)
