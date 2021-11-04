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


def main(reference_data_dir, reference_structure_dir, panddas_dir):
    reference_data_dir = Path(reference_data_dir).resolve()
    reference_structure_dir = Path(reference_structure_dir).resolve()
    panddas_dir = Path(panddas_dir).resolve()

    high_confidence_structures = [x.stem for x in reference_structure_dir.glob('*')]
    print(high_confidence_structures)

    reference_datasets = ReferenceDatasets.from_dir(reference_data_dir)
    print(f'Got reference datasets model')

    records = []

    for pandda_dir in panddas_dir.glob('*'):

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

                    event_distances = []
                    is_ligand_broken = False

                    closest_event = get_closest_event(reference_dataset.reference_structure_path,
                                                                 dataset_structure_path,
                                                      dataset_result.events,
                                                      )

                    for event_num, event_result in dataset_result.events.items():


                        if len(event_result.build_results) != 0:
                            for build_num, build in event_result.build_results.items():
                                try:
                                    build_path = build.path
                                    _rmsds = get_rmsds_from_path(reference_dataset.reference_structure_path,
                                                                 dataset_structure_path,
                                                                 build_path)
                                    # print(_rmsds)
                                    closest = min(_rmsds)
                                    rmsds.append(closest)
                                    # print("########")
                                    # print(build.percentage_signal)
                                    # print(build.percentage_noise)
                                    signal_to_noises.append(build.percentage_signal - (build.percentage_noise))
                                except Exception as e:
                                    is_ligand_broken = True
                                    continue




                    if num_builds == 0:
                        num_builds = 0
                        broken_ligand = False
                        closest_rmsd = None
                        best_signal_to_noise = None

                    else:
                        if len(rmsds) == 0:
                            broken_ligand = is_ligand_broken
                            closest_rmsd = None
                            best_signal_to_noise = None

                        else:
                            broken_ligand = is_ligand_broken
                            closest_rmsd = min(rmsds)
                            best_signal_to_noise = max(signal_to_noises)

                    closest_event = min(event_distances)


                else:
                    num_events = 0
                    num_builds = None
                    broken_ligand = False
                    closest_event = None
                    closest_rmsd = None
                    best_signal_to_noise = None
                #
                #     # If have builds to compare to
                #     if len(rmsds) != 0:
                #
                #         closest = min(rmsds)
                #         signalest = max(signal_to_noises)
                #         if dtag.dtag in high_confidence_structures:
                #
                #             print(f"\t\tHIGH CONFIDENCE: {dtag.dtag}: {dataset_result.processed}: {closest}: {signalest}")
                #         else:
                #             print(f"\t\t{dtag.dtag}: {dataset_result.processed}: {closest}: {signalest}")
                #
                #     # If no builds
                #     else:
                #         if dtag.dtag in high_confidence_structures:
                #             print(f'\t\tHIGH CONFIDENCE: {dtag.dtag}: {dataset_result.processed}: NO BUILDS OR BROKEN '
                #                   f'LIGANDS!')
                #         else:
                #             print(f"\t\t{dtag.dtag}: {dataset_result.processed}: NO BUILDS OR BROKEN LIGANDS!")
                #
                # else:
                #     if dtag.dtag in high_confidence_structures:
                #         print(f'\t\tHIGH CONFIDENCE: {dtag.dtag}: {dataset_result.processed}: NO EVENTS!')
                #     else:
                #         print(f"\t\t{dtag.dtag}: {dataset_result.processed}: NO EVENTS!")

            record = {
                'dtag': dtag.dtag,
                'processed': processed,
                'num_events': num_events,
                'num_builds': num_builds,
                'broken_ligand': broken_ligand,
                'closest_event': closest_event,
                'closest_rmsd': closest_rmsd,  # None and num_events>0&num_builds>0 implies broken ligand
                'best_signal_to_noise': best_signal_to_noise,
            }
            records.append(record)

        print(pd.DataFrame(records).head())
        print(pd.DataFrame(records).tail())

            # except Exception as e:
            #     print(e)

    table = pd.DataFrame(records)
    print(table)

    print("Finished!")


if __name__ == "__main__":
    fire.Fire(main)
