import pathlib
import os
import shutil

import fire

def ligands_to_compound_dir(data_dir: str):
    data_dir = pathlib.Path(data_dir).resolve()

    for dataset_dir in data_dir.glob("*"):
        # Make the compound dir
        compound_dir = dataset_dir / "compound"
        os.mkdir(compound_dir)

        # Find the cifs in the dataset dir
        cif_paths = list(dataset_dir.glob("*.cif"))

        # Move them
        new_ligand_path = compound_dir / "ligand.cif"
        if len(cif_paths) > 0:
            shutil.copy(cif_paths[0], new_ligand_path)

if __name__ == "__main__":
    fire.Fire(ligands_to_compound_dir)