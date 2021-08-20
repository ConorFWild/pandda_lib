from __future__ import annotations
from dataclasses import dataclass
from typing import *
from pathlib import Path

from pandda_lib import constants
from pandda_lib.common import Dtag
from pandda_lib.events import Event


@dataclass()
class PanDDADir:
    pandda_dir: Path
    pandda_analyses_dir: Path
    pandda_analyse_event_file: Path
    pandda_processed_dataset_dir: Path
    pandda_processed_datasets_dirs: Dict[Dtag, Path]
    pandda_model_paths: Dict[Dtag, Path]

    @staticmethod
    def from_path(pandda_dir: Path) -> PanDDADir:
        pandda_analyses_dir = pandda_dir / constants.PANDDA_ANALYSES_DIR
        pandda_processed_datasets_dir = pandda_dir / constants.PANDDA_PROCESSED_DATASETS_DIR

        pandda_analyse_event_file = pandda_analyses_dir / constants.PANDDA_ANALYSE_EVENTS_FILE

        pandda_processed_datasets_dirs = {Dtag(path.name): path
                                          for path
                                          in pandda_processed_datasets_dir.glob("*")}


        pandda_model_paths = {
            dtag: path / constants.PANDDA_MODELLED_STRUCTURES_DIR / constants.PANDDA_EVENT_MODEL.format(dtag)
            for dtag, path
            in pandda_processed_datasets_dirs.items()
        }

        return PanDDADir(
            pandda_dir,
            pandda_analyses_dir,
            pandda_analyse_event_file,
            pandda_processed_datasets_dirs
        )
