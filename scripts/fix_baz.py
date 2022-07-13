import os
from pathlib import Path
import shutil

import fire


def fix_baz(baz_dir: str):
    baz_dir = Path(baz_dir).resolve()

    for dataset_dir in baz_dir.glob("*"):



        compound_dir = dataset_dir / "compound"

        # Make the directory for compounds
        if not compound_dir.exists():
            os.mkdir(compound_dir)

        # Move the ligand pdb
        ligand_pdb_path = dataset_dir / "ligand.pdb"
        new_ligand_pdb_path = compound_dir
        if ligand_pdb_path.exists():
            shutil.move(ligand_pdb_path, new_ligand_pdb_path)

        # Move the ligand cif
        ligand_cif_path = dataset_dir / "ligand.cif"
        new_ligand_cif_path = compound_dir / "ligand.cif"
        if ligand_cif_path.exists():
            shutil.move(ligand_cif_path, new_ligand_cif_path)


if __name__ == "__main__":
    fire.Fire(fix_baz)
