import pathlib
import numpy as np
import gemmi
import fire
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import json

from pandda_lib import constants
from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs
from pandda_lib.fs.pandda_result import PanDDAResult
from pandda_lib.diamond_sqlite.diamond_sqlite import (Base, ProjectDirSQL, DatasetSQL, PanDDADirSQL,
                                                      PanDDADatasetSQL, PanDDABuildSQL, PanDDAEventSQL, SystemSQL,
                                                      BoundStateModelSQL)
from pandda_lib.rscc import get_rscc


def test_rscc(input_json):
    with open(input_json, "r") as f:
        input_data = json.load(f)

    model_path = pathlib.Path(input_data["pandda_model_path"])
    resolution = input_data["resolution"]
    tmp_dir = input_data["tmp_dir"]
    event_map_path = input_data["event_map_path"]

    rscc = get_rscc(
        model_path,
        event_map_path,
        resolution,
        tmp_dir
    )

    print(rscc)


if __name__ == "__main__":
    fire.Fire(test_rscc)
