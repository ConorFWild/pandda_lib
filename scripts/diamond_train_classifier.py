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


def main(reference_data_dir, reference_structure_dir, panddas_dir, output_file_path):
    reference_data_dir = Path(reference_data_dir).resolve()
    reference_structure_dir = Path(reference_structure_dir).resolve()
    panddas_dir = Path(panddas_dir).resolve()

    high_confidence_structures = [x.stem for x in reference_structure_dir.glob('*')]
    print(high_confidence_structures)

    reference_datasets = ReferenceDatasets.from_dir(reference_data_dir)
    print(f'Got reference datasets model')

    records = []

    for pandda_dir in panddas_dir.glob('*'):

        system = pandda_dir.name

        if not pandda_dir.is_dir():
            continue

        print(f'PanDDA: {pandda_dir.name}')
        if not (pandda_dir / 'analyses' / 'pandda_analyse_events.csv').exists():
            print(f'\tNO EVENT TABLE! SKIPPING!')
            continue
        pandda_result = PanDDAResult.from_dir(pandda_dir)
        # print(f'Got PanDDA model')

        for dtag, reference_dataset in reference_datasets.reference_datasets.items():
            # print(f'Getting RMSDs for dtag: ')
            # try:
            if dtag not in pandda_result.processed_datasets:
                continue

            dataset_result = pandda_result.processed_datasets[dtag]
            dataset_structure_path = dataset_result.structure_path

            processed = dataset_result.processed
            if not processed:
                # print(f'\tDtag {dtag.dtag} not in pandda results')
                processed = processed
                num_events = None
                num_builds = None
                broken_ligand = False
                alignment_error = False
                closest_event = None
                closest_rmsd = None
                best_signal_to_noise = None
            else:
                # print(f'\tDtag {dtag.dtag} IS in pandda results')

                signal_to_noises = []
                rmsds = []

                num_events = len(dataset_result.events)
                num_builds = sum([len(event.build_results) for event in dataset_result.events.values()])

                if len(dataset_result.events) != 0:

                    # event_distances = []
                    is_ligand_broken = False
                    has_alignment_error = False

                    has_closest_event = get_closest_event(reference_dataset.reference_structure_path,
                                                      dataset_structure_path,
                                                      dataset_result.events,
                                                      )

                    if has_closest_event == "ALIGNMENTERROR":
                        has_alignment_error = True
                        alignment_error = has_alignment_error
                        broken_ligand = False
                        closest_event = None
                        closest_rmsd = None
                        best_signal_to_noise = None

                    else:
                        closest_event = has_closest_event

                        for event_num, event_result in dataset_result.events.items():

                            for build_num, build in event_result.build_results.items():
                                try:
                                    build_path = build.path
                                    _rmsds = get_rmsds_from_path(reference_dataset.reference_structure_path,
                                                                 dataset_structure_path,
                                                                 build_path)

                                    if _rmsds == "BROKENLIGAND":
                                        is_ligand_broken = True
                                        continue

                                    if _rmsds == "ALIGNMENTERROR":
                                        has_alignment_error = True
                                        continue
                                    # print(_rmsds)
                                    closest_rmsd = min(_rmsds)
                                    # print("########")
                                    # print(build.percentage_signal)
                                    # print(build.percentage_noise)
                                    signal_to_noise = build.percentage_signal - (build.percentage_noise)

                                    record = {
                                        'system': system,
                                        'dtag': dtag.dtag,
                                        'processed': processed,
                                        'num_events': num_events,
                                        'num_builds': num_builds,
                                        'closest_event': closest_event,
                                        'closest_rmsd': closest_rmsd,
                                        # None and num_events>0&num_builds>0 implies broken ligand
                                        'signal': build.percentage_signal,
                                        'noise': build.percentage_noise,
                                        'signal_to_noise': signal_to_noise,
                                    }
                                    print(record)
                                    records.append(record)

                                except Exception as e:
                                    # is_ligand_broken = True
                                    continue

    table = pd.DataFrame(records)
    print(table)
    table.to_csv(output_file_path)

    print("Finished!")


if __name__ == "__main__":
    fire.Fire(main)
