from pathlib import Path

import pandas as pd
import fire
import mongoengine
import gemmi

from pandda_lib.common import Dtag, SystemName
from pandda_lib.mongo import pandda
from pandda_lib import rmsd
from pandda_lib import constants


def main(pandda_dir: str, table="pandda"):
    mongoengine.connect(table)

    pandda_dir = Path(pandda_dir)

    print(f"Reference scructure dir is: {pandda_dir}")

    processed_dataset_dirs = pandda_dir / constants.PANDDA_PROCESSED_DATASETS_DIR

    for processed_dataset_dir in processed_dataset_dirs.glob("*"):

        try:
            print(f"\tPath is: {processed_dataset_dir}")
            dtag = Dtag.from_name(processed_dataset_dir.name)
            print(f"\t\tDtag is {dtag}")
            system_name = SystemName.from_dtag(dtag)
            print(f"\t\tSystem is: {system_name}")

            modelled_structure_path = processed_dataset_dir / constants.PANDDA_MODELLED_STRUCTURES_DIR / \
                                      constants.PANDDA_EVENT_MODEL.format(dtag.dtag)

            if not modelled_structure_path.exists():
                raise Exception(f"No such model: {modelled_structure_path}")

            try:
                mongo_system = pandda.System.objects(system_name=system_name.system_name, )[0]
            except Exception as e:
                mongo_system = pandda.System(system_name=system_name.system_name, )
                mongo_system.save()

            try:
                mongo_dataset = pandda.Dataset.objects(dtag=dtag.dtag)[0]
            except Exception as e:
                mongo_dataset = pandda.Dataset(dtag=dtag.dtag, system=mongo_system)
                mongo_dataset.save()

            structure = rmsd.Structure.from_path(modelled_structure_path)
            ligands = rmsd.Ligands.from_structure(structure)

            try:
                mongo_reference_model = pandda.ReferenceModel(path=str(modelled_structure_path))[0]
            except Exception as e:
                mongo_ligands = []
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
                    path=str(modelled_structure_path.resolve()),
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
