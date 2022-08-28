import pathlib
import os

import fire
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs

from diamond_build_input_data_database import diamond_build_input_data_database
from database_add_diamond_panddas import database_add_diamond_panddas
from diamond_add_fragalysis_reference_structures import diamond_add_fragalysis_reference_structures
from diamond_add_model_stats import diamond_add_model_stats


def diamond_build_database_all(sqlite_filepath, reference_structure_dir, tmp_dir):
    diamond_build_input_data_database(sqlite_filepath)
    database_add_diamond_panddas(sqlite_filepath)
    diamond_add_fragalysis_reference_structures(sqlite_filepath, reference_structure_dir)
    diamond_add_model_stats(sqlite_filepath, tmp_dir)

if __name__ == "__main__":
    fire.Fire(diamond_build_database_all)
