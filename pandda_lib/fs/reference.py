from __future__ import annotations
from dataclasses import dataclass
from typing import *
from pathlib import Path
import re

import pandas as pd
import gemmi

from pandda_lib.common import Dtag, SystemName
from pandda_lib.events import Event
from pandda_lib import constants
from pandda_lib.rmsd import Structure, Ligands, RMSD


@dataclass()
class ReferenceStructure:
    path: Path
    structure: Structure

    @staticmethod
    def from_file(file: Path):

        structure = Structure.from_path(file)

        return ReferenceStructure(
            file,
            structure,
        )


@dataclass()
class ReferenceDataset:
    path: Path
    # reference_structure: ReferenceStructure
    reference_structure_path: Path

    @staticmethod
    def from_dir(reference_dataset_dir: Path):
        pdb_file_path = reference_dataset_dir / 'final.pdb'
        # reference_structure = ReferenceStructure.from_file(pdb_file_path)

        return ReferenceDataset(
            reference_dataset_dir,
            pdb_file_path,
            # reference_structure,
        )


@dataclass()
class ReferenceDatasets:
    path: Path
    reference_datasets: Dict[Dtag, ReferenceDataset]

    @staticmethod
    def from_dir(reference_datasets_dir: Path):
        reference_datasets = {}
        for reference_dataset_dir in reference_datasets_dir.glob('*'):
            dtag = Dtag(reference_dataset_dir.name)
            # print(f'\tAttempting to load: {dtag.dtag}')
            reference_datasets[dtag] = ReferenceDataset.from_dir(reference_dataset_dir)

        return ReferenceDatasets(
            reference_datasets_dir,
            reference_datasets,
        )

    def get_reference_dataset(self, key):
        return self.reference_datasets[key]
