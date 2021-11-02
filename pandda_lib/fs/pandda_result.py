from __future__ import annotations
from dataclasses import dataclass
from typing import *
from pathlib import Path
import re

import pandas as pd

from pandda_lib.common import Dtag, SystemName
from pandda_lib.events import Event
from pandda_lib import constants


@dataclass()
class BuildResult:
    path: Path

    @staticmethod
    def from_file(file: Path):
        return BuildResult(file)


@dataclass()
class EventResult:
    event_map_path: Path
    build_results: Dict[str, BuildResult]

    @staticmethod
    def from_dir(event_dir: Path):
        rhofit_dir = event_dir / 'rhofit'

        dataset_dir = event_dir.parent
        for file in dataset_dir.glob('*'):
            if re.match(f"{dataset_dir.name}-event_{event_dir.name}.+", file.name):
                event_map_path = file

        build_results = {}

        for file in rhofit_dir.glob("*"):
            if re.match('Hit.+\.pdb', file.name):
                build_result = BuildResult.from_file(file)
                build_results[file.name] = build_result

        return EventResult(
            event_map_path,
            build_results
        )

    def get_build_result(self, key):
        return self.build_results[key]


@dataclass()
class DatasetResult:
    structure_path: Path
    events: Dict[str, EventResult]

    @staticmethod
    def from_dir(processed_dataset_dir, event_table):

        dtag = Dtag(processed_dataset_dir.name)

        structure_path = processed_dataset_dir / constants.PANDDA_PDB_FILE.format(dtag.dtag)

        events = {}

        for event_dir in processed_dataset_dir.glob('*'):
            if re.match('[0-9]+', event_dir.name):
                event_result = EventResult.from_dir(event_dir)
                events[event_dir.name] = event_result

        return DatasetResult(
            structure_path,
            events
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
