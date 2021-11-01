import os
from typing import *
from pathlib import Path
from dataclasses import dataclass
import itertools
import shutil

import fire


def main(reference_structure_dir, reference_data_dir):
    for reference_structure_path in reference_structure_dir:
        dtag = reference_structure_path.stem

        reference_dataset_dir = reference_data_dir / dtag
        if reference_dataset_dir.exists():
            continue

        else:
            os.mkdir(reference_dataset_dir)
            new_reference_structure_path = reference_dataset_dir / 'final.pdb'
            shutil.copy(reference_structure_path, new_reference_structure_path)


if __name__ == "__main__":
    fire.Fire(main)
