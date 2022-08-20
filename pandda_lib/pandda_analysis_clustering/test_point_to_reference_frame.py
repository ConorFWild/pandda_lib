import numpy as np
# import scipy
import gemmi


def get_test_pos_in_reference_frame(x, y, z, dataset, alignment):
    # Get closest residue id
    ca_distances = {}
    for res_id in dataset.structure.protein_residue_ids():
        res_span = dataset.structure[res_id]
        res = res_span[0]
        res_ca = res["CA"][0]
        ca_pos = res_ca.pos
        distance = np.linalg.norm([x-ca_pos.x, y-ca_pos.y, z-ca_pos.z])
        ca_distances[res_id] = distance

    closest_res_id = min(ca_distances, key=lambda _x: ca_distances[_x])

    # get alignment
    transform = alignment[closest_res_id]

    # Perform alignment
    transformed_pos = transform.apply_moving_to_reference({(0,0,0): gemmi.Position(x, y, z),})[(0,0,0)]

    # return as tuple
    return transformed_pos.x, transformed_pos.y, transformed_pos.z