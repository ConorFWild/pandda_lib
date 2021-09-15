from __future__ import annotations
from dataclasses import dataclass
from typing import *
from pathlib import Path

import numpy as np

from pandda_lib.common import Dtag
from pandda_lib.events import Event

@dataclass()
class RMSD:
    rmsd: float

    @staticmethod
    def from_structures(res_1, res_2):

        closest_distances = []
        for atom_1 in res_1:
            distances = []
            for atom_2 in res_2:
                distance = atom_1.pos.dist(atom_2.pos)
                distances.append(distance)

            closest_distance = min(distances)
            closest_distances.append(closest_distance)
        return RMSD(np.mean(closest_distances))

