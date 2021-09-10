from __future__ import annotations
from dataclasses import dataclass
from typing import *
from pathlib import Path

import numpy as np
import gemmi

from pandda_lib.common import Dtag
from pandda_lib.events import Event
from pandda_lib.rmsd.structure import Structure


@dataclass()
class Ligand:
    structure: Any

    def centroid(self):
        res = self.structure


        xs = []
        ys = []
        zs = []

        for atom in res:
            pos = atom.pos
            x = pos.x
            y = pos.y
            z = pos.z
            xs.append(x)
            ys.append(y)
            zs.append(z)

        return (np.mean(xs), np.mean(ys), np.mean(zs),)

    @staticmethod
    def from_structure(struc: Any):
        ...


@dataclass()
class Ligands:
    structures: List[Ligand]

    @staticmethod
    def from_structure(struc: Structure):

        ligands: List[Ligand] = []
        # sel = gemmi.Selection('(LIG)')

        for model in struc.structure:

            for chain in model:

                for residue in chain:
                    if residue.name == "LIG":
                        ligands.append(Ligand(residue))

        return Ligands(ligands)
