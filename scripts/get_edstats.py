from pathlib import Path

import pandas as pd
import seaborn as sns
import fire

from pandda_lib.command import EDSTATS


def main(data_dirs, output_plot_file, mtz_regex="dimple.mtz", pdb_regex="dimple.pdb",
         f="FWT", phi="PHWT", delta_f="DELFWT", delta_phi="PHDELWT", fix=True):
    data_dirs = Path(data_dirs).resolve()
    output_plot_file = Path(output_plot_file).resolve()

    records = []

    j = 0
    for data_dir in data_dirs.glob("*"):
        if j > 10:
            break
        j = j + 1
        dtag = data_dir.name
        print(f"Processing dtag: {dtag}")

        try:
            mtz_file = next(data_dir.glob(mtz_regex))
        except:
            print("\tSkipping! No mtz!")
            continue
        try:
            pdb_file = next(data_dir.glob(pdb_regex))
        except:
            print("\tSkipping! No pdb!")
            continue

        if not mtz_file.exists():
            print("\tSkipping! No mtz!")
            continue

        if not pdb_file.exists():
            print("\tSkipping! No pdb!")
            continue

        stats = EDSTATS(mtz_file, pdb_file,
                        f=f, phi=phi, delta_f=delta_f, delta_phi=delta_phi,
                        fix=fix
                        )

        # print(stats.command)

        rsccs = stats.run()

        for rscc in rsccs:
            record = {
                "dtag": dtag,
                "rscc": rscc,
            }

            records.append(record)

    table = pd.DataFrame(records)

    num_datasets = table['dtag'].nunique()

    p = sns.catplot(
        x='dtag',
        y='rscc',
        data=table,
        kind='violin',
    height = 8.27, aspect = (11.7 / 8.27) * (num_datasets / 5),
    )

    p.set(ylim=(0, 1))

    p.savefig(output_plot_file)

    for unique_dtag in table['dtag'].unique():
        print(f"{unique_dtag}: {table[table['dtag'] == unique_dtag].mean()}")

    # g = sns.catplot(
    #     x='dtag',
    #     y='rscc',
    #     data=table,
    #     kind='bar',
    # height = 8.27, aspect = (11.7 / 8.27) * (num_datasets / 5),
    # )
    #
    # g.set(ylim=(0, 1))
    #
    # g.savefig(output_plot_file)


if __name__ == "__main__":
    fire.Fire(main)
