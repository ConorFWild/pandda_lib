import os
from typing import *
from pathlib import Path
from dataclasses import dataclass
import re
import itertools

import fire

from fragalysis_api.xcextracter.xcextracter import xcextracter
from fragalysis_api.xcextracter.getdata import GetTargetsData, GetPdbData

from pandda_lib.pandda_lib.common import Dtag, SystemName
from pandda_lib.pandda_lib.frag import FragalysisDataDir
from pandda_lib.pandda_lib.rmsd import Ligands, RMSD
from pandda_lib.pandda_lib.fs import PanDDADir


# Functions for getting RMSD
@dataclass()
class RMSDResult:
    ...


def rmsd_from_structures(structure_1, structure_2) -> RMSDResult:
    ...


def structure_from_path():
    ...


def model_path_from_pandda_dir(pandda_dir, dtag):
    ...


# Functions for graphing
def figure_from_results():
    ...


def plot_results(out_path, results):
    figure = figure_from_results(results)

    figure.savefig(str(out_path))


def autobuild_vs_fragalysis(pandda_dir_path: str, fragalysis_data_dir: str, results_dir: str, fragalysis_id: str, ):
    # Type input
    pandda_dir_path = Path(pandda_dir_path)
    fragalysis_data_dir = Path(fragalysis_data_dir)
    results_dir = Path(results_dir)

    # Get system name
    system_name = SystemName(fragalysis_id)

    # Fetch data from fragalysis
    model_paths: Dict[Dtag, Path] = FragalysisDataDir.from_path(fragalysis_data_dir)[system_name]

    # Get PanDDA models
    pandda_models = PanDDADir.from_path(pandda_dir_path).pandda_model_paths

    # Loop over comparing
    results: Dict[Dtag, RMSD] = {}
    for dtag, model_path in model_paths.items():
        rmsds = []
        for reference_ligand, new_ligand in itertools.product(
                Ligands.from_structure(Structure.from_path(model_path)),
                Ligands.from_structure(Structure.from_path(pandda_models[dtag])),
        ):
            rmsd = RMSD.from_structures(
                reference_ligand.structure,
                new_ligand.structure,
            )
            rmsds.append(rmsd)
        results[dtag] = min(rmsds, key=lambda _x: _x.rmsd)

    # Make a graph of results
    plot_results(
        results_dir / "",
        results,
    )


if __name__ == "__main__":
    fire.Fire(autobuild_vs_fragalysis)
