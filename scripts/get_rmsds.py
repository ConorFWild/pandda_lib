import os
from typing import *
from pathlib import Path
from dataclasses import dataclass
import itertools

import fire

from pandda_lib.fs import PanDDAResult
from pandda_lib.fs.reference import ReferenceDatasets
from pandda_lib.common import Dtag, SystemName
from pandda_lib.rmsd import Ligands, RMSD


def main(reference_structure_dir, pandda_dir):
    pandda_result = PanDDAResult.from_dir(pandda_dir)
    reference_datasets = ReferenceDatasets.from_dir(reference_structure_dir)

    for dtag, reference_dataset in reference_datasets.reference_datasets.items():
        try:
            dataset_result = pandda_result.processed_datasets[dtag]
            dataset_structure_path = dataset_result.structure_path
            for event_num, event_result in dataset_result.events.items():
                for build_num, build in event_result.build_results.items():
                    build_path = build.path
                    rmsds = reference_dataset.get_rmsds_from_path(dataset_structure_path, build_path)
                    closest = min(rmsds)
                    print(f"\t\t{dtag.dtag}: {closest}")

        except Exception as e:
            print(e)


if __name__ == "__main__":
    fire.Fire(main)
