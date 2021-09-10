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

    print(f"Reference scructure dir is: {reference_structure_dir}")

    for path in reference_structure_dir.glob("*"):
        try:
            print(f"\tPath is: {path}")
            dtag = Dtag.from_name(path.name)
            print(f"\t\tDtag is {dtag}")
            system_name = SystemName.from_dtag(dtag)
            print(f"\t\tSystem is: {system_name}")

            try:
                mongo_system = pandda.System.objects(system_name=system_name.system_name,)[0]
            except Exception as e:
                mongo_system = pandda.System(system_name=system_name.system_name, )
                mongo_system.save()

            try:
                mongo_dataset = pandda.Dataset.objects(dtag=dtag.dtag)[0]
            except Exception as e:
                mongo_dataset = pandda.Dataset(dtag=dtag.dtag, system=mongo_system)
                mongo_dataset.save()


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
                mongo_ligands.append(mongo_ligand)

            mongo_reference_model = pandda.ReferenceModel(
                path=str(path),
                system=mongo_system,
                dataset=mongo_dataset,
                event=None,
                ligands=mongo_ligands
            )
            mongo_reference_model.save()

        except Exception as e:
            print(e)

if __name__ == "__main__":
    fire.Fire(main)