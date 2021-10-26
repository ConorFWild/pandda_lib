import os
import shutil
from pathlib import Path

import pandas as pd
import fire
import mongoengine
import gemmi

from pandda_lib.common import Dtag, SystemName
from pandda_lib.mongo import pandda
from pandda_lib import rmsd
from pandda_lib import constants
from pandda_lib.command import rsync


def main(reference_dir: str, remote_dir: str, table: str, password: str):
    reference_dir = Path(reference_dir).resolve()
    remote_dir = Path(remote_dir).resolve()

    if not reference_dir.exists():
        os.mkdir(reference_dir)

    mongoengine.connect(table)

    reference_models = pandda.ReferenceModel.objects()
    print(f"Got {len(reference_models)} reference models!")

    for reference_model in reference_models:
        dataset = reference_model.dataset
        dtag = dataset.dtag
        print(f"\tDtag: {dtag}")

        # Check if has any findable ligands modelled, else skip
        if len(reference_model.ligands) == 0:
            print(f"\t\tNo ligands in reference: skipping!")

        # Get base model, reflections and fragment model
        model_file = Path(reference_model.path)
        mtz_file = Path(dataset.reflections.path)
        structure_file = Path(dataset.structure.path)

        # Get output files
        dataset_dir = reference_dir / dtag
        new_mtz_file = dataset_dir / "initial.mtz"
        new_structure_file = dataset_dir / "initial.pdb"
        new_model_file = dataset_dir / "final.pdb"

        # Check all files are there to copy
        if not model_file.exists():
            print(f"\t\tNo model for reference: skipping!")
            continue
        if not mtz_file.exists():
            print(f"\t\tNo reflections for structure: skipping!")
            continue
        if not structure_file.exists():
            print(f"\t\tNo structure  for reference: skipping!")
            continue

        # Make outputdir
        os.mkdir(dataset_dir)

        # Copy
        shutil.copyfile(mtz_file, new_mtz_file)
        shutil.copyfile(structure_file, new_structure_file)
        shutil.copyfile(model_file, new_model_file)

        print(f"\t\tFinished!")

    # Upload
    rsync.RsyncDirToDiamond.from_paths(
        reference_dir,
        remote_dir,
        password
    )

if __name__ == "__main__":
    fire.Fire(main)
