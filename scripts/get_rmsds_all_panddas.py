import os
from typing import *
from pathlib import Path
from dataclasses import dataclass
import itertools

import fire

from pandda_lib.fs import PanDDAResult
from pandda_lib.fs.reference import ReferenceDatasets
from pandda_lib.common import Dtag, SystemName
from pandda_lib.rmsd import Ligands, RMSD, get_rmsds_from_path


def main(reference_structure_dir, panddas_dir):
    reference_structure_dir = Path(reference_structure_dir).resolve()
    panddas_dir = Path(panddas_dir).resolve()

    reference_datasets = ReferenceDatasets.from_dir(reference_structure_dir)
    print(f'Got reference datasets model')

    for pandda_dir in panddas_dir.glob('*'):
        if not (pandda_dir / 'analyses' / 'pandda_analyse_events.csv').exists():
            continue
        pandda_result = PanDDAResult.from_dir(pandda_dir)
        print(f'Got PanDDA model')

        for dtag, reference_dataset in reference_datasets.reference_datasets.items():
            # print(f'Getting RMSDs for dtag: ')
            # try:
            if dtag not in pandda_result.processed_datasets:
                # print(f'\tDtag {dtag.dtag} not in pandda results')
                continue

            dataset_result = pandda_result.processed_datasets[dtag]
            dataset_structure_path = dataset_result.structure_path

            rmsds = []
            for event_num, event_result in dataset_result.events.items():
                for build_num, build in event_result.build_results.items():
                    try:
                        build_path = build.path
                        _rmsds = get_rmsds_from_path(reference_dataset.reference_structure_path, dataset_structure_path,
                                                     build_path)
                        # print(_rmsds)
                        closest = min(_rmsds)
                        rmsds.append(closest)
                    except:
                        continue

            if len(rmsds) > 0:
                closest = min(rmsds)
                print(f"\t\t{dtag.dtag}: {closest}")
            else:
                print(f'\t\t{dtag.dtag}: NO EVENTS!')

            # except Exception as e:
            #     print(e)

        print("Finished!")

if __name__ == "__main__":
    fire.Fire(main)
