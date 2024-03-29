import pathlib
import os
import json

import fire
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs

from diamond_build_input_data_database import diamond_build_input_data_database
from database_add_diamond_panddas import database_add_diamond_panddas
from diamond_add_fragalysis_reference_structures import diamond_add_fragalysis_reference_structures
from diamond_add_model_stats import diamond_add_model_stats
from add_autobuild_panddas_to_sql import database_add_autobuild_panddas
from add_autobuild_rmsds import diamond_add_autobuild_rmsds
from event_map_stats import diamond_add_event_stats
from add_autobuild_rsccs import diamond_add_autobuild_rsccs


def diamond_build_database_all(options_json="database_options.json", step=0, cpus=3):
    with open(options_json, "r") as f:
        options = json.load(f)

    sqlite_filepath = options["sqlite_filepath"]
    reference_structure_dir = options["reference_structure_dir"]
    tmp_dir = options["tmp_dir"]
    build_tmp_dir = options["build_tmp_dir"]
    pandda_autobuilds_dir = options["pandda_autobuilds_dir"]

    print(f"Database: {sqlite_filepath}")
    print(f"Reference Structures: {sqlite_filepath}")
    print(f"RSCC Temporary Dir: {tmp_dir}")
    print(f"PanDDA Autobuilds Dir: {pandda_autobuilds_dir}")



    if step <= 1:
        diamond_build_input_data_database(sqlite_filepath)
    if step <= 2:
        database_add_diamond_panddas(sqlite_filepath)
    if step <= 3:
        diamond_add_fragalysis_reference_structures(sqlite_filepath, reference_structure_dir)
    if step <= 4:
        diamond_add_model_stats(sqlite_filepath, tmp_dir, cpus=cpus)
    if step <= 5:
        diamond_add_event_stats(sqlite_filepath)
    if step <= 6:
        database_add_autobuild_panddas(sqlite_filepath, pandda_autobuilds_dir)
    if step <= 7:
        diamond_add_autobuild_rmsds(sqlite_filepath, cpus=cpus)
    if step <= 8:
        diamond_add_autobuild_rsccs(sqlite_filepath, build_tmp_dir, cpus=cpus)


if __name__ == "__main__":
    fire.Fire(diamond_build_database_all)
