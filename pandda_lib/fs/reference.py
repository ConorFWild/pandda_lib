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

        return ReferenceStructure(file,
                                  structure)

    def get_rmsds_from_path(self, path_align: Path, path_lig: Path):
        structure_align = Structure.from_path(path_align)

        st_ref = self.structure.structure
        st_align = structure_align.structure

        polymer_ref = st_ref[0][0].get_polymer()
        polymer_comp = st_align[0][0].get_polymer()
        ptype = polymer_ref.check_polymer_type()
        sup = gemmi.calculate_superposition(polymer_ref, polymer_comp, ptype, gemmi.SupSelect.CaP)

        compatator_structure = Structure.from_path(path_lig)
        st_comp = compatator_structure.structure
        for model in st_comp:
            for chain in model:
                ress = chain.get_ligands()

                sup.apply(ress)

        ligands_ref = Ligands.from_structure(self.structure)
        ligands_comp = Ligands.from_structure(compatator_structure)

        # Check every rmsd of every ligand against every ligand
        rmsds = []
        for ligand_ref in ligands_ref.structures:
            for ligand_comp in ligands_comp.structure:
                rmsd = RMSD.from_structures_iso(ligand_ref.structure, ligand_comp.structure)
                rmsds.append(rmsd.rmsd)

        return rmsds


@dataclass()
class ReferenceDataset:
    path: Path
    reference_structure: ReferenceStructure

    @staticmethod
    def from_dir(reference_dataset_dir: Path):
        pdb_file_path = next(reference_dataset_dir.glob('*.pdb'))
        reference_structure = ReferenceStructure.from_file(pdb_file_path)

        return ReferenceDataset(
            reference_dataset_dir,
            reference_structure,
        )


@dataclass()
class ReferenceDatasets:
    path: Path
    reference_datasets: Dict[Dtag, ReferenceStructure]

    @staticmethod
    def from_dir(reference_datasets_dir: Path):
        reference_datasets = {}
        for reference_dataset_dir in reference_datasets_dir.glob('*'):
            dtag = Dtag(reference_dataset_dir.name)
            reference_datasets[dtag] = ReferenceDataset.from_dir()

        return ReferenceDatasets(
            reference_datasets_dir,
            reference_datasets,
        )

    def get_reference_dataset(self, key):
        return self.reference_datasets[key]