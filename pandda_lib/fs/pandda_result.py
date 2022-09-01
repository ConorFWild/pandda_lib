from __future__ import annotations
from dataclasses import dataclass
from typing import *
from pathlib import Path
import re
import json

import pandas as pd

from pandda_lib.common import Dtag, SystemName
from pandda_lib.events import Event
from pandda_lib import constants


@dataclass()
class BuildResult:
    path: Path
    signal_samples: int
    total_signal_samples: int
    noise_samples: int
    total_noise_samples: int
    percentage_signal: float
    percentage_noise: float
    score: float

    @staticmethod
    def from_file(file: Path, build_log, score):
        signal_log = build_log['signal_log']
        noise_log = build_log['noise_log']
        signal_samples = signal_log['signal_samples_signal']
        signal_samples_total = signal_log['total_valid_samples']
        noise_samples = noise_log['noise_samples']
        noise_samples_total = noise_log['total_valid_samples']

        total_noise = (signal_samples_total - signal_samples) + noise_samples

        percentage_signal = signal_samples / signal_samples_total
        percentage_noise = total_noise / (noise_samples_total + (signal_samples_total - signal_samples))

        return BuildResult(file,
                           signal_log['signal_samples_signal'],
                           signal_log['total_valid_samples'],
                           noise_log['noise_samples'],
                           noise_log['total_valid_samples'],
                           percentage_signal,
                           percentage_noise,
                           score,
                           )


@dataclass()
class EventResult:
    idx: int
    event_map_path: Path
    build_results: Dict[str, BuildResult]
    centroid: Tuple[float, float, float]
    bdc: float
    size: int
    bdc: float
    size: float

    @staticmethod
    def from_dir(event_dir: Path, event_table):
        rhofit_dir = event_dir / 'rhofit'

        dataset_dir = event_dir.parent
        event_map_file = None
        for file in dataset_dir.glob('*'):
            if re.match(f"{dataset_dir.name}-event_{event_dir.name}.+", file.name):
                event_map_path = file
        if not event_map_file:
            raise Exception("Should be an event map file!")

        # Get the score log

        # Get build log
        build_log_file = event_dir / 'log.json'
        if build_log_file.exists():
            with open(build_log_file, 'r') as f:
                build_log = json.load(f)

            rescoring_log = build_log['rescoring_log']

            score_log_file = event_dir / "scores.json"
            with open(score_log_file, 'r') as f:
                score_log = json.load(f)

            build_results = {}
            for file in rhofit_dir.glob("*"):
                if re.match('Hit.+\.pdb', file.name):
                    build_log = rescoring_log[str(file)]
                    score = float(score_log[str(file)])
                    build_result = BuildResult.from_file(file, build_log, score)
                    build_results[file.name] = build_result

        else:
            build_results = {}

        dtag = event_dir.parent.name
        event_idx = int(event_dir.name)

        for index, row in event_table.iterrows():
            # print(row)
            # print((dtag, event_idx))
            if row['dtag'] == dtag:
                if row['event_idx'] == event_idx:
                    centroid = (row['x'], row['y'], row['z'])
                    bdc = row["1-BDC"]
                    size = row["cluster_size"]

        return EventResult(
            event_idx,
            event_map_path,
            build_results,
            centroid,
            bdc,
            size
        )

    def get_build_result(self, key):
        return self.build_results[key]


@dataclass()
class DatasetResult:
    path: Path
    structure_path: Path
    events: Dict[str, EventResult]
    processed: bool
    dataset_log: Optional[Dict]

    @staticmethod
    def from_dir(processed_dataset_dir, event_table):

        path = processed_dataset_dir

        dtag = Dtag(processed_dataset_dir.name)

        structure_path = processed_dataset_dir / constants.PANDDA_PDB_FILE.format(dtag.dtag)

        dataset_log_path = processed_dataset_dir / 'log.json'

        if dataset_log_path.exists():
            processed = True
        else:
            processed = False

        if processed:
            with open(dataset_log_path, "r") as f:
                dataset_log = json.load(f)
        else:
            dataset_log = None

        events = {}
        for event_dir in processed_dataset_dir.glob('*'):
            if re.match('[0-9]+', event_dir.name):
                try:
                    event_result = EventResult.from_dir(event_dir, event_table)
                    events[event_dir.name] = event_result
                except Exception as e:
                    print(f"\t{event_dir}: {e}")
                    continue

        return DatasetResult(
            path,
            structure_path,
            events,
            processed=processed,
            dataset_log=dataset_log,
        )

    def get_event_result(self, key):
        return self.events[key]


@dataclass()
class PanDDAResult:
    pandda_dir: Path
    processed_datasets: Dict[Dtag, DatasetResult]

    @staticmethod
    def from_dir(pandda_dir: Path):
        processed_datasets_dir = pandda_dir / constants.PANDDA_PROCESSED_DATASETS_DIR
        analyses_dir = pandda_dir / constants.PANDDA_ANALYSES_DIR
        event_csv = analyses_dir / constants.PANDDA_ANALYSE_EVENTS_FILE
        event_table = pd.read_csv(event_csv)

        processed_datasets = {}
        for processed_dataset_dir in processed_datasets_dir.glob('*'):
            dtag = Dtag(processed_dataset_dir.name)
            processed_dataset = DatasetResult.from_dir(processed_dataset_dir, event_table)
            processed_datasets[dtag] = processed_dataset

        return PanDDAResult(
            pandda_dir,
            processed_datasets,
        )

    def get_dataset_result(self, key):
        return self.processed_datasets[key]


@dataclass()
class PanDDAsResult:
    panddas: Dict[SystemName, PanDDAResult]

    @staticmethod
    def from_dir(panddas_dir: Path):
        # For each dir construct the PanDDA
        pandda_results = {}
        for pandda_dir in panddas_dir.glob('*'):
            system_name = SystemName(dir.name)
            pandda_result = PanDDAResult.from_dir(dir)
            pandda_results[system_name] = pandda_result

        return PanDDAsResult(
            pandda_results
        )

    def get_pandda_result(self, key):
        return self.panddas[key]
