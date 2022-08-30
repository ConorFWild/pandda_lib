import pathlib
import os
import re

import fire
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from pandda_lib import constants
from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs
from pandda_lib.fs.pandda_result import PanDDAResult
from pandda_lib.diamond_sqlite.diamond_data_to_sql import get_pandda_2_result
from pandda_lib.diamond_sqlite.diamond_sqlite import (Base, ProjectDirSQL, DatasetSQL, PanDDA1DirSQL,
                                                      PanDDADatasetSQL, PanDDABuildSQL, PanDDAEventSQL, SystemSQL,
                                                      PanDDADirSQL)


def database_add_diamond_panddas(sqlite_filepath, pandda_autobuilds_dir):
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    pandda_autobuilds_dir = pathlib.Path(pandda_autobuilds_dir).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)

    # Remove tables
    PanDDADirSQL.__table__.drop(engine)
    # PanDDADatasetSQL.__table__.drop(engine)
    # PanDDAEventSQL.__table__.drop(engine)
    # PanDDABuildSQL.__table__.drop(engine)

    Base.metadata.create_all(engine)

    # Get systems
    systems = session.query(SystemSQL).order_by(SystemSQL.id).all()

    # For each system, get the path, then get paths to PanDDA 1 results in the directory above it
    # for system in systems:
    #     print(f"Looking for panddas for system: {system.system_name}")
    #     for project in system.projects:

    for system in systems:
        for project in system.projects:
            system_project_dir = pandda_autobuilds_dir / f"system_{system.system_name}_project_{project.project_name}"
            try:
                pandda_dir_sql = get_pandda_2_result(system_project_dir)
                session.add(pandda_dir_sql)
            except Exception as e:
                print(f"\tCouldn't get PanDDA for {system_project_dir}")

    session.commit()

    # for pandda_dir in pandda_autobuilds_dir.glob("*"):
    #     # match = re.match("system_([^_]+)_project_([^_]+)")
    #     if not match:
    #         continue
    #     system_name, project_name = match.groups()


if __name__ == "__main__":
    fire.Fire(database_add_diamond_panddas)
