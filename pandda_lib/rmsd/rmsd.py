from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import *
from pathlib import Path
import networkx
from networkx.algorithms import isomorphism

import numpy as np
import gemmi

from pandda_lib.common import Dtag, SystemName
from pandda_lib.events import Event

from pandda_lib import constants
from pandda_lib.rmsd.structure import Structure
from pandda_lib.rmsd.ligand import Ligands

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
                G.add_node(j,
                           Z=atom.element.atomic_number,
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
                if pos_1.dist(pos_2) < 1.81:
                    G.add_edge(j, k)

        return G

    @staticmethod
    def from_structures_iso(res_1, res_2):
        graph_1 = RMSD.graph_from_res(res_1)
        # print(graph_1.nodes)
        graph_2 = RMSD.graph_from_res(res_2)
        # print(graph_2.nodes)

        # Match!
        node_match = isomorphism.categorical_node_match('Z', 0)
        gm = isomorphism.GraphMatcher(graph_1, graph_2, node_match=node_match)

        # Get shortest iso
        if gm.is_isomorphic():
            # print(cc1.name, 'is isomorphic')
            # we could use GM.match(), but here we try to find the shortest diff

            mean_distances = []
            # short_diff = None
            for n, mapping in enumerate(gm.isomorphisms_iter()):
                # diff = {k: v for k, v in mapping.items() if k != v}
                # if short_diff is None or len(diff) < len(short_diff):
                #     short_diff = diff
                if n == 10000:  # don't spend too much here
                    print(' (it may not be the simplest isomorphism)')

                #     break
                # print(short_diff)

                # Get Distance between points
                distances = []

                for j in graph_1.nodes:

                    atom_1_node = graph_1.nodes[j]
                    # print(atom_1_node)

                    atom_2_id = mapping[j]
                    atom_2_node = graph_2.nodes[atom_2_id]

                    assert atom_1_node["Z"] == atom_2_node["Z"]

                    distance = atom_1_node["pos"].dist(atom_2_node["pos"])
                    distances.append(distance)
                mean_distance = np.mean(distances)
                mean_distances.append(mean_distance)

            # print(mean_distances)
            min_mean_distance = min(mean_distances)

            return RMSD(min_mean_distance)

        # Ottherwise something has gone terribly wrong
        else:
            raise Exception(f"{res_1} and {res_2} are NOT isomorphic, wtf? "
                            f"res 1 len: {len([atom for atom in res_1])}; "
                            f"res 2 len: {len([atom for atom in res_2])}; "
                            f"Graph 1: {graph_1.nodes}; {[graph_1.nodes[j]['Z'] for j in graph_1.nodes]}; "
                            f"{len(graph_1.edges)}; "
                            f"Graph 2: {graph_2.nodes}; {[graph_2.nodes[j]['Z'] for j in graph_2.nodes]}; "
                            f"{len(graph_2.edges)}; ")

    # @staticmethod
    # def from_symmetry_structures_iso(res_1, res_2s):
    #     graph_1 = RMSD.graph_from_res(res_1)
    #     # print(graph_1.nodes)
    #     graph_2 = RMSD.graph_from_res(res_2)
    #     # print(graph_2.nodes)
    #
    #     # Match!
    #     node_match = isomorphism.categorical_node_match('Z', 0)
    #     gm = isomorphism.GraphMatcher(graph_1, graph_2, node_match=node_match)
    #
    #     # Get shortest iso
    #     if gm.is_isomorphic():
    #         # print(cc1.name, 'is isomorphic')
    #         # we could use GM.match(), but here we try to find the shortest diff
    #
    #         mean_distances = []
    #         # short_diff = None
    #         for n, mapping in enumerate(gm.isomorphisms_iter()):
    #             # diff = {k: v for k, v in mapping.items() if k != v}
    #             # if short_diff is None or len(diff) < len(short_diff):
    #             #     short_diff = diff
    #             if n == 10000:  # don't spend too much here
    #                 print(' (it may not be the simplest isomorphism)')
    #
    #             #     break
    #             # print(short_diff)
    #
    #             # Get Distance between points
    #             distances = []
    #
    #             for j in graph_1.nodes:
    #                 atom_1_node = graph_1.nodes[j]
    #                 # print(atom_1_node)
    #
    #                 atom_2_id = mapping[j]
    #                 atom_2_node = graph_2.nodes[atom_2_id]
    #
    #                 assert atom_1_node["Z"] == atom_2_node["Z"]
    #
    #                 distance = atom_1_node["pos"].dist(atom_2_node["pos"])
    #                 distances.append(distance)
    #             mean_distance = np.mean(distances)
    #             mean_distances.append(mean_distance)
    #
    #         # print(mean_distances)
    #         min_mean_distance = min(mean_distances)
    #
    #         return RMSD(min_mean_distance)
    #
    #     # Ottherwise something has gone terribly wrong
    #     else:
    #         raise Exception(f"{res_1} and {res_2} are NOT isomorphic, wtf? "
    #                         f"res 1 len: {len([atom for atom in res_1])}; "
    #                         f"res 2 len: {len([atom for atom in res_2])}; "
    #                         f"Graph 1: {graph_1.nodes}; {[graph_1.nodes[j]['Z'] for j in graph_1.nodes]}; "
    #                         f"{len(graph_1.edges)}; "
    #                         f"Graph 2: {graph_2.nodes}; {[graph_2.nodes[j]['Z'] for j in graph_2.nodes]}; "
    #                         f"{len(graph_2.edges)}; ")

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


def get_closest_event(reference_structure_path,
        dataset_structure_path,
        events):

    return _get_closest_event(reference_structure_path,
                              dataset_structure_path,
                              {
                                  event_id: event.centroid for event_id, event in events.items()
                               })


def _get_closest_event(
        reference_structure_path,
        dataset_structure_path,
        events,
):

    structure_align = Structure.from_path(dataset_structure_path)
    structure_ref = Structure.from_path(reference_structure_path)

    st_ref = structure_ref.structure
    st_align = structure_align.structure

    try:
        polymer_ref = st_ref[0][0].get_polymer()
        polymer_comp = st_align[0][0].get_polymer()
        ptype = polymer_ref.check_polymer_type()
        sup = gemmi.calculate_superposition(polymer_ref, polymer_comp, ptype, gemmi.SupSelect.CaP)
    except Exception as e:
        return "ALIGNMENTERROR"

    reference_structure = Structure.from_path(reference_structure_path)
    reference_ligands = Ligands.from_structure(reference_structure)

    event_distances = []
    for event_num, event_result in events.items():

        reference_structure_ligand_distance_to_events = []
        for ligand in reference_ligands.structures:
            ligand_centroid = ligand.centroid()
            event_centroid_native = event_result
            event_centroid = sup.transform.apply(gemmi.Position(*event_centroid_native))

            distance_to_event = np.linalg.norm(
                (
                    event_centroid[0] - ligand_centroid[0],
                    event_centroid[1] - ligand_centroid[1],
                    event_centroid[2] - ligand_centroid[2],
                ))
            reference_structure_ligand_distance_to_events.append(distance_to_event)

        distance_to_event = min(reference_structure_ligand_distance_to_events)
        event_distances.append(distance_to_event)

    return min(event_distances)


def get_symmetry_images(ligand_comp, structure_ref):
    unit_cell = structure_ref.structure.cell
    spacegroup = structure_ref.structure.find_spacegroup()

    ds = [-1.0, 0.0, 1.0]
    ops = spacegroup.operations()
    symmetry_images = []
    for dx, dy, dz in itertools.product(ds, ds, ds):
        for op in ops:
            res = gemmi.Residue()
            for atom in ligand_comp:
                pos = atom.pos
                fractional_pos = unit_cell.fractionalize(pos)
                symmetry_fractional_pos = op.apply(fractional_pos)
                translated_symmetry_fractional_pos = symmetry_fractional_pos + gemmi.Fractional(dx, dy, dz)
                new_pos = unit_cell.orthogonalize(translated_symmetry_fractional_pos)

                # Get the atomic symbol
                atom_symbol: str = atom.GetSymbol()
                gemmi_element: gemmi.Element = gemmi.Element(atom_symbol)


                # Get the
                gemmi_atom: gemmi.Atom = gemmi.Atom()
                gemmi_atom.name = atom_symbol
                gemmi_atom.pos = new_pos
                gemmi_atom.element = gemmi_element

                # Add atom to residue
                res.add_atom(gemmi_atom)

            symmetry_images.append(res)

    return symmetry_images


def get_rmsds_from_path(path_ref, path_align: Path, path_lig: Path):
    structure_align = Structure.from_path(path_align)
    structure_ref = Structure.from_path(path_ref)

    st_ref = structure_ref.structure
    st_align = structure_align.structure

    try:
        polymer_ref = st_ref[0][0].get_polymer()
        polymer_comp = st_align[0][0].get_polymer()
        ptype = polymer_ref.check_polymer_type()
        sup = gemmi.calculate_superposition(polymer_ref, polymer_comp, ptype, gemmi.SupSelect.CaP)
    except Exception as e:
        return "ALIGNMENTERROR"

    compatator_structure = Structure.from_path(path_lig)
    st_comp = compatator_structure.structure
    for model in st_comp:
        for chain in model:
            ress = chain.get_ligands()

            ress.transform_pos_and_adp(sup.transform)

    ligands_ref = Ligands.from_structure(structure_ref)
    ligands_comp = Ligands.from_structure(compatator_structure)

    # Check every rmsd of every ligand against every ligand
    rmsds = []
    try:
        for ligand_ref in ligands_ref.structures:
            for ligand_comp in ligands_comp.structures:
                for ligand_comp_symmetry_image in get_symmetry_images(ligand_comp.structure, structure_ref):
                    rmsd = RMSD.from_structures_iso(ligand_ref.structure, ligand_comp_symmetry_image)
                    rmsds.append(rmsd.rmsd)
    except Exception as e:
        print(e)
        return "BROKENLIGAND"

    return rmsds