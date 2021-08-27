from pathlib import Path

import pandas as pd
import fire
import mongoengine

from pandda_lib.mongo.pandda import *
from pandda_lib import constants

def add_pandda():
    ...

def main(data_dirs, pandda_dirs):

    mongoengine.connect("pandda")

    System.drop_collection()
    Dataset.drop_collection()
    Structure.drop_collection()
    Reflections.drop_collection()
    Compound.drop_collection()
    Event.drop_collection()
    PanDDA.drop_collection()

    data_dirs = Path(data_dirs).resolve()
    pandda_dirs = Path(pandda_dirs).resolve()

    for data_dir in data_dirs.glob("*"):
        print(f"\tProcessing data dir: {data_dir}")
        system_name = data_dir.name
        system = System(
            system_name=system_name,
        )
        system.save()

        system_datasets = []
        for dataset_dir in data_dir.glob("*"):

            dtag = dataset_dir.name
            reflections_path = dataset_dir / "dimple.mtz"
            structure_path = dataset_dir / "dimple.pdb"
            compound_dir = dataset_dir / "compound"
            compound_paths = compound_dir.glob("*.cif")

            reflections = Reflections(path=str(reflections_path))
            reflections.save()
            structure = Structure(path=str(structure_path))
            structure.save()
            compounds = [Compound(path=str(compound_path)) for compound_path in compound_paths]
            for compound in compounds:
                compound.save()

            dataset = Dataset(
                dtag=dtag,
                system=system,
                structure=structure,
                reflections=reflections,
                compounds=compounds,
            )
            dataset.save()
            system_datasets.append(dataset)

        system.datasets = system_datasets
        system.save(cascade=True)

    for pandda_dir in pandda_dirs.glob("*"):
        # system_name = pandda_dir.name
        # system = System.objects(system_name=system_name)[0]
        print(f"\tProcessing pandda dir: {pandda_dir}")
        processed_datasets_dir = pandda_dir / constants.PANDDA_PROCESSED_DATASETS_DIR
        pandda_datasets = []
        pandda_models = []
        for dataset_dir in processed_datasets_dir.glob("*"):
            dtag = dataset_dir.name
            dataset = Dataset.objects(dtag=dtag)[0]

            model_dirs_path = dataset_dir / constants.PANDDA_MODELLED_STRUCTURES_DIR
            model_path = model_dirs_path / constants.PANDDA_EVENT_MODEL.format(dtag)

            model = Model(
                path=str(model_path),
                dataset=dataset,
            )
            model.save(cascade=True)
            pandda_models.append(model)
            pandda_datasets.append(dataset)

        analysis_dir_path = pandda_dir / constants.PANDDA_ANALYSES_DIR
        event_table_path = analysis_dir_path / constants.PANDDA_ANALYSE_EVENTS_FILE

        event_table = pd.read_csv(event_table_path)
        pandda_events = []
        for index, event_record in event_table.iterrows():
            print(event_record)
            dtag = event_record["dtag"]
            dataset = Dataset.objects(dtag=dtag)[0]
            event_idx = event_record["event_idx"]
            x = event_record["x"]
            y = event_record["y"]
            z = event_record["z"]
            model = Model.objects(
                dataset=dataset,
            )
            event = Event(
                datasset=pandda_datasets,
                event_idx=event_idx,
                x=x,
                y=y,
                z=z,
                model=model,
            )
            event.save()
            pandda_events.append(event)

        pandda_system = pandda_datasets[0].system

        pandda = PanDDA(
            path=str(pandda_dir),
            system=pandda_system,
            events=pandda_events,
            datasets=pandda_datasets,
        )

        pandda.save(cascade=True)



if __name__ == "__main__":
    fire.Fire(main)
