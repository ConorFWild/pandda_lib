from pathlib import Path

import fire
import gemmi
import pandas as pd


def __main__(pandda_dir):
    analyses_dir = Path(pandda_dir) / "analyses"
    pandda_inspect_table_path = analyses_dir / "pandda_inspect_events.csv"
    processed_datasets_dir = Path(pandda_dir) / "processed_datasets"
    pandda_inspect_table = pd.read_csv(pandda_inspect_table_path)

    new_rows = []
    for idx, row in pandda_inspect_table.iterrows():
        dtag = row['dtag']
        event_idx = row['event_idx']
        x, y, z = row['x'], row['y'], row['z']
        processed_dataset_dir = processed_datasets_dir / dtag
        pdb_file = processed_dataset_dir / f"{dtag}-pandda-input.pdb"
        st = gemmi.read_structure(str(pdb_file))

        for model in st:
            for chain in model:
                if chain.name != "A":
                    continue
                for residue in chain:
                    if residue.seqid.num not in (147, 40, 71, 161):
                        continue
                    ca = residue['CA'][0]
                    pos = ca.pos
                    dist = pos.dist(gemmi.Position(x, y, z))
                    if dist < 8.0:
                        new_rows.append(row)
    new_dataframe = pd.DataFrame(new_rows)
    print(new_dataframe)
    new_dataframe.to_csv(Path(pandda_dir) / "active_site_events.csv")


if __name__ == "__main__":
    fire.Fire(__main__)
