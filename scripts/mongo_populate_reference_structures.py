from pathlib import Path

import pandas as pd
import fire
import mongoengine
import gemmi

from pandda_lib.common import Dtag, SystemName
from pandda_lib.mongo import pandda
from pandda_lib import rmsd
from pandda_lib import constants


def main(reference_structure_dir: str):
    mongoengine.connect("test_pandda")

    pandda.System.drop_collection()
    pandda.Dataset.drop_collection()
    pandda.Structure.drop_collection()
    pandda.Reflections.drop_collection()
    pandda.Compound.drop_collection()
    pandda.Event.drop_collection()
    pandda.PanDDA.drop_collection()
    pandda.ReferenceModel.drop_collection()
    pandda.Ligand.drop_collection()

    reference_structure_dir = Path(reference_structure_dir)

    for path in reference_structure_dir.glob("*"):
        dtag = Dtag.from_string(path.name)
        print(dtag)
        system_name = SystemName.from_dtag(dtag)
        print(system_name)

        mongo_system = pandda.System(system_name=system_name.system_name, )
        mongo_system.save()
        mongo_dataset = pandda.Dataset(dtag=dtag.dtag, system=mongo_system)
        mongo_dataset.save()

        # mongo_system = pandda.System.objects(system_name=system_name.system_name,)[0]
        # mongo_dtag = pandda.Dataset.objects(dtag=dtag.dtag)[0]
        #

        structure = rmsd.Structure.from_path(path)
        ligands = rmsd.Ligands.from_structure(structure)

        mongo_ligands=[]
        for ligand in ligands.structures:
            centroid = ligand.centroid()
            mongo_ligand = pandda.Ligand(
                reference_model=None,
                x=centroid[0],
                y=centroid[1],
                z=centroid[2],
            )
            mongo_ligand.save()
            mongo_ligand.append(mongo_ligand)

        mongo_reference_model = pandda.ReferenceModel(
            path=str(path),
            system=mongo_system,
            dataset=mongo_dataset,
            event=None,
            ligands=mongo_ligands
        )
        mongo_reference_model.save()