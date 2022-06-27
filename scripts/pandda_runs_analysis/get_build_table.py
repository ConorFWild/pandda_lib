import os
from typing import *
from pathlib import Path
from dataclasses import dataclass
import itertools

import fire
import numpy as np
import pandas as pd
import joblib

from pandda_lib.fs import PanDDAResult
from pandda_lib.fs.reference import ReferenceDatasets
from pandda_lib.common import Dtag, SystemName
from pandda_lib.rmsd import Structure, Ligands, RMSD, get_rmsds_from_path, get_closest_event


def get_records_from_pandda_dir(pandda_dir, reference_datasets, high_confidence_structures):
    records = []

    system = pandda_dir.name

    if not pandda_dir.is_dir():
        return []

    print(f'PanDDA: {pandda_dir.name}')
    if not (pandda_dir / 'analyses' / 'pandda_analyse_events.csv').exists():
        print(f'\tNO EVENT TABLE! SKIPPING!')
        return []
    pandda_result = PanDDAResult.from_dir(pandda_dir)

    for dtag, reference_dataset in reference_datasets.reference_datasets.items():
        if dtag not in pandda_result.processed_datasets:
            continue

        if dtag.dtag in high_confidence_structures:
            high_confidence = True
        else:
            high_confidence = False

        dataset_result = pandda_result.processed_datasets[dtag]
        dataset_structure_path = dataset_result.structure_path

        processed = dataset_result.processed
        if not processed:
            continue
        else:
            if len(dataset_result.events) != 0:

                # event_distances = []
                is_ligand_broken = False
                has_alignment_error = False

                has_closest_event = get_closest_event(reference_dataset.reference_structure_path,
                                                      dataset_structure_path,
                                                      dataset_result.events,
                                                      )


                closest_event = has_closest_event

                for event_num, event_result in dataset_result.events.items():

                    # if len(event_result.build_results) != 0:
                    for build_num, build in event_result.build_results.items():
                        try:
                            build_path = build.path
                            _rmsds = get_rmsds_from_path(reference_dataset.reference_structure_path,
                                                         dataset_structure_path,
                                                         build_path)

                            if _rmsds == "BROKENLIGAND":
                                is_ligand_broken = True
                                # continue

                            if _rmsds == "ALIGNMENTERROR":
                                has_alignment_error = True

                            closest = min(_rmsds)

                            record = {
                                'system': system,
                                'dtag': dtag.dtag,
                                'processed': processed,
                                'event_num': event_num,
                                'build_num': build_num,
                                'broken_ligand': is_ligand_broken,
                                'alignment_error': has_alignment_error,
                                'closest_event': closest_event,
                                'closest_rmsd': closest,
                                'signal': build.percentage_signal,
                                'noise': build.percentage_noise,
                                'high_confidence': high_confidence,
                            }
                            records.append(record)

                        except Exception as e:
                            continue


    return records

def main(reference_data_dir, reference_structure_dir, panddas_dir, output_file_path):
    reference_data_dir = Path(reference_data_dir).resolve()
    reference_structure_dir = Path(reference_structure_dir).resolve()
    panddas_dir = Path(panddas_dir).resolve()

    high_confidence_structures = [x.stem for x in reference_structure_dir.glob('*')]
    print(high_confidence_structures)

    reference_datasets = ReferenceDatasets.from_dir(reference_data_dir)
    print(f'Got reference datasets model')

    records_list = joblib.Parallel(n_jobs=10, verbose=50)(
        joblib.delayed(
            get_records_from_pandda_dir
)(
            pandda_dir, reference_datasets, high_confidence_structures
        )
        for pandda_dir
        in panddas_dir.glob('*')
    )

    #
    # # print(pd.DataFrame(records).head())
    # print(pd.DataFrame(records).tail())
    #
    # # except Exception as e:
    # #     print(e)

    records = [x for l in records_list for x in l]

    table = pd.DataFrame(records)
    print(table)
    table.to_csv(output_file_path)

    print("Finished!")


if __name__ == "__main__":
    fire.Fire(main)
