import os
from typing import *
from pathlib import Path
from dataclasses import dataclass
import itertools

import fire
import numpy as np
import pandas as pd

from pandda_lib.fs import PanDDAResult
from pandda_lib.fs.reference import ReferenceDatasets
from pandda_lib.common import Dtag, SystemName
from pandda_lib.rmsd import Structure, Ligands, RMSD, get_rmsds_from_path, get_closest_event


def main(reference_data_dir, reference_structure_dir, panddas_dir, dataset_output_file_path, system_output_file_path):
    reference_data_dir = Path(reference_data_dir).resolve()
    reference_structure_dir = Path(reference_structure_dir).resolve()
    panddas_dir = Path(panddas_dir).resolve()

    high_confidence_structures = [x.stem for x in reference_structure_dir.glob('*')]
    print(high_confidence_structures)

    reference_datasets = ReferenceDatasets.from_dir(reference_data_dir)
    print(f'Got reference datasets model')

    dataset_records = []
    system_records = []

    for pandda_dir in panddas_dir.glob('*'):

        system = pandda_dir.name
        if not pandda_dir.is_dir():
            continue

        print(f'PanDDA: {pandda_dir.name}')

        if not (pandda_dir / 'analyses' / 'pandda_analyse_events.csv').exists():
            print(f'\tNO EVENT TABLE! SKIPPING!')
            complete = False
            num_datasets = len(list((pandda_dir/'processed_datasets').glob('*')))

        else:
            complete = True

            pandda_result = PanDDAResult.from_dir(pandda_dir)

            for dtag, dataset_result in pandda_result.processed_datasets.items():
                print(f"\tDtag: {dtag}")

                record = {
                    'system': system,
                    'dtag': dtag.dtag,
                    'processed': dataset_result.processed,
                }
                dataset_records.append(record)
                print(f"\t\tDataset record: {record}")

            num_datasets = len(pandda_result.processed_datasets)

        system_record = {
            'system': system,
            'complete': complete,
            'num_datasets': num_datasets,

        }
        system_records.append(system_record)
        print(f"\tsystem_record: {system_record}")

    dataset_table = pd.DataFrame(dataset_records)
    print(dataset_table)
    dataset_table.to_csv(dataset_output_file_path)

    system_table = pd.DataFrame(system_records)
    print(system_table)
    system_table.to_csv(system_output_file_path)


    print("Finished!")


if __name__ == "__main__":
    fire.Fire(main)
