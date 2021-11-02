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


def main(reference_structure_dir, pandda_dir):
    reference_structure_dir = Path(reference_structure_dir)
    pandda_dir = Path(pandda_dir)


    reference_datasets = ReferenceDatasets.from_dir(reference_structure_dir)
    print(f'Got reference datasets model')
    pandda_result = PanDDAResult.from_dir(pandda_dir)
    print(f'Got PanDDA model')

    for dtag, reference_dataset in reference_datasets.reference_datasets.items():
        # print(f'Getting RMSDs for dtag: ')
        # try:
        if dtag not in pandda_result.processed_datasets:
            print(f'\tDtag {dtag.dtag} not in pandda results')
            continue

        dataset_result = pandda_result.processed_datasets[dtag]
        dataset_structure_path = dataset_result.structure_path
        for event_num, event_result in dataset_result.events.items():
            for build_num, build in event_result.build_results.items():
                build_path = build.path
                rmsds = get_rmsds_from_path(reference_dataset.reference_structure_path, dataset_structure_path, build_path)
                closest = min(rmsds)
                print(f"\t\t{dtag.dtag}: {closest}")

        # except Exception as e:
        #     print(e)

    print("Finished!")

if __name__ == "__main__":
    fire.Fire(main)
