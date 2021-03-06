from pathlib import Path
import shutil

import pandas as pd
import seaborn as sns
import fire

from pandda_lib.command import EDSTATS


def main(data_dirs, output_dir, mtz_regex="dimple.mtz", pdb_regex="dimple.pdb",
         f="FWT", phi="PHWT", delta_f="DELFWT", delta_phi="PHDELWT", fix=True):
    data_dirs = Path(data_dirs).resolve()
    output_dir = Path(output_dir).resolve()
    output_plot_file = output_dir / "violin.png"
    output_csv_file = output_dir / "table.csv"

    if not shutil.which("edstats"):
        print('The "edstats" command is not in you path: have you got ccp4 installed?')
        exit()

    records = []

    j = 0
    for data_dir in data_dirs.glob("*"):
        # if j > 10:
        #     break
        # j = j + 1
        dtag = data_dir.name
        print(f"Processing dtag: {dtag} in dir {data_dir}")

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

        print(stats.command)

        rsccs, b_factors, rszos, rszds = stats.run()

        for k, rscc in enumerate(rsccs):
            rscc = rsccs[k]
            b_factor = b_factors[k]
            rszo = rszos[k]
            rszd = rszds[k]
            record = {
                "dtag": dtag,
                "rscc": rscc,
                "b_factor": b_factor,
                "rszo": rszo,
                "rszd": rszd,
            }

            records.append(record)

    table = pd.DataFrame(records)

    num_datasets = table['dtag'].nunique()

    # p = sns.catplot(
    #     x='dtag',
    #     y='rszo',
    #     data=table,
    #     kind='violin',
    #     height=8.27, aspect=(11.7 / 8.27) * (num_datasets / 8),
    # )
    #
    # # p.set(ylim=(0, 1))
    # p.set(ylim=(0, 10))
    # # p.set_xticklabels(p.get_xticklabels(), rotation=30)
    # for ax in p.axes.ravel():
    #     ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
    # p.savefig(output_plot_file)

    drop = []
    keep = []

    for unique_dtag in table['dtag'].unique():
        dtag_rsccs = table[table['dtag'] == unique_dtag]
        print(f"{unique_dtag}")
        print(f"\t{table[table['dtag'] == unique_dtag].mean()}")
        print(f"\t{len(dtag_rsccs[dtag_rsccs['rszo'] < 1.0]) / len(dtag_rsccs)}")

        bad_residues = len(dtag_rsccs[dtag_rsccs['rszo'] < 1.0]) / len(dtag_rsccs)
        if bad_residues > 0.05:
            drop.append(unique_dtag)
        else:
            keep.append(unique_dtag)


    print(f'Dtags to drop: {drop}')
    print(f'Dtags to keep: {keep}')

    table.to_csv(output_csv_file)

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
