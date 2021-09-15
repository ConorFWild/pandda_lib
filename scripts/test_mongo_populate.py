from pathlib import Path

import pandas as pd
import fire
import mongoengine

from pandda_lib.mongo.pandda import *
from pandda_lib import constants


def main(data_dir, pandda_dir, mtz_regex="*.dimple.mtz", pdb_regex="*.dimple.pdb", compound_dir=None):
    mongoengine.connect("test_pandda")

    System.drop_collection()
    Dataset.drop_collection()
    Structure.drop_collection()
    Reflections.drop_collection()
    Compound.drop_collection()
    Event.drop_collection()
    PanDDA.drop_collection()

    data_dir = Path(data_dir).resolve()
    pandda_dir = Path(pandda_dir).resolve()

    print(f"\tProcessing data dir: {data_dir}")
    system_name = data_dir.name
    system = System(
        system_name=system_name,
    )
    system.save()

    system_datasets = []
    for dataset_dir in data_dir.glob("*"):

        dtag = dataset_dir.name
        reflections_path = next(dataset_dir.glob(mtz_regex))
        structure_path = next(dataset_dir.glob(pdb_regex))
        if compound_dir:
            compound_dir = dataset_dir / "compound"
        else:
            compound_dir = dataset_dir
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

    print(f"\tProcessing pandda dir: {pandda_dir}")
    processed_datasets_dir = pandda_dir / constants.PANDDA_PROCESSED_DATASETS_DIR
    pandda_datasets = []
    pandda_models = []
    for dataset_dir in processed_datasets_dir.glob("*"):
        dtag = dataset_dir.name
        try:
            dataset = Dataset.objects(dtag=dtag)[0]
        except Exception as e:
            print(f"\t\tNo dataset with dtag in database: {dtag}: {e}")
            continue
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

    try:
        event_table = pd.read_csv(event_table_path)
    except Exception as e:
        print(f"\t\tNo such csv: {event_table_path}: {e}")
        return

    pandda_events = []
    for index, event_record in event_table.iterrows():
        # print(event_record)
        dtag = event_record["dtag"]
        try:
            dataset = Dataset.objects(dtag=dtag)[0]
        except Exception as e:
            print(f"\t\tNo dataset with dtag in database: {dtag}: {e}")
            continue
        event_idx = event_record["event_idx"]
        x = event_record["x"]
        y = event_record["y"]
        z = event_record["z"]
        event_model_path = pandda_dir / constants.PANDDA_PROCESSED_DATASETS_DIR / event_idx / "rhofit" / "best.pdb"
        event_model = Model(path=str(event_model_path.resolve()),
                            dataset=dataset,
                            )
        event_model.save()

        # model = Model.objects(
        #     dataset=dataset,
        # )[0]

        event = Event(
            dataset=dataset,
            event_idx=event_idx,
            x=x,
            y=y,
            z=z,
            model=event_model,
        )
        # print(event)
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
