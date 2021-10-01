from __future__ import annotations
from dataclasses import dataclass
from typing import *
from pathlib import Path
import networkx
from networkx.algorithms import isomorphism

import numpy as np

from pandda_lib.common import Dtag
from pandda_lib.events import Event


@dataclass()
class RMSD:
    rmsd: float

    @staticmethod
    def graph_from_res(res):
        G = networkx.Graph()
        for j, atom in enumerate(res):
            if atom.element.name == "H":
                continue
            else:
                G.add_node(j, Z=atom.element.atomic_number,
                           # x=atom.pos.x, y=atom.pos.y, z=atom.pos.z,
                           pos=atom.pos,
                           )
        # add bonds
        for j, atom_1 in enumerate(res):
            if atom_1.element.name == "H":
                continue
            pos_1 = atom_1.pos

            for k, atom_2 in enumerate(res):
                if atom_2.element.name == "H":
                    continue
                pos_2 = atom_2.pos
                if pos_1.dist(pos_2) < 2.0:
                    G.add_edge(j, k)

        return G

    @staticmethod
    def from_structures_iso(res_1, res_2):
        graph_1 = RMSD.graph_from_res(res_1)
        graph_2 = RMSD.graph_from_res(res_2)

        # Match!
        node_match = isomorphism.categorical_node_match('Z', 0)
        gm = isomorphism.GraphMatcher(graph_1, graph_2, node_match=node_match)

        # Get shortest iso
        if gm.is_isomorphic():
            # print(cc1.name, 'is isomorphic')
            # we could use GM.match(), but here we try to find the shortest diff
            short_diff = None
            for n, mapping in enumerate(gm.isomorphisms_iter()):
                diff = {k: v for k, v in mapping.items() if k != v}
                if short_diff is None or len(diff) < len(short_diff):
                    short_diff = diff
                if n == 10000:  # don't spend too much here
                    # print(' (it may not be the simplest isomorphism)')
                    break

            print(short_diff)

            # Get Distance between points
            distances = []

            for j in graph_1.nodes:

                atom_1_node = graph_1.nodes[j]

                atom_2_id = short_diff[j]
                atom_2_node = graph_2.nodes[atom_2_id]

                assert atom_1_node["Z"] == atom_2_node["Z"]

                distance = atom_1_node["pos"].dist(atom_2_node["pos"])
                distances.append(distance)
            mean_distance = np.mean(distances)

            return RMSD(mean_distance)

        # Ottherwise something has gone terribly wrong
        else:
            raise Exception(f"{res_1} and {res_2} are NOT isomorphic, wtf? "
                            f"res 1 len: {len([atom for atom in res_1])}; "
                            f"res 2 len: {len([atom for atom in res_2])}")

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
