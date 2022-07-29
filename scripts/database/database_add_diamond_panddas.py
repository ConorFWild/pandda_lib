import pathlib
import os

import fire
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from pandda_lib import constants
from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs
from pandda_lib.fs.pandda_result import PanDDAResult
from pandda_lib.diamond_sqlite.diamond_sqlite import (Base, SystemDataDirSQL, DatasetSQL, PanDDA1DirSQL,
                                                      PanDDADatasetSQL, PanDDABuildSQL, PanDDAEventSQL)


def try_func(func, x):
    try:
        return func(x)

    except Exception as e:
        print(f"{e}: {x}")
        return False


def main(sqlite_filepath):
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)

    # Remove tables
    PanDDA1DirSQL.__table__.drop(engine)
    # PanDDADatasetSQL.__table__.drop(engine)
    # PanDDAEventSQL.__table__.drop(engine)
    # PanDDABuildSQL.__table__.drop(engine)

    Base.metadata.create_all(engine)

    # Get systems
    systems = session.query(SystemDataDirSQL).order_by(SystemDataDirSQL.id).all()

    # For each system, get the path, then get paths to PanDDA 1 results in the directory above it
    for system in systems:
        print(f"Looking for panddas for system: {system.system_name}")
        analysis_dir = pathlib.Path(system.path).parent

        # Get the possible PanDDA dirs
        possible_pandda_dirs = analysis_dir.glob("*")

        # Filter on being a directory

        possible_pandda_dirs = [
            possible_pandda_dir
            for possible_pandda_dir
            in possible_pandda_dirs
            if try_func(lambda x: x.is_dir(), possible_pandda_dir)]

        # Filter on containing an analysis csv
        possible_pandda_dirs = [
            possible_pandda_dir
            for possible_pandda_dir
            in possible_pandda_dirs
            if try_func(lambda x: (x / constants.PANDDA_ANALYSES_DIR / constants.PANDDA_ANALYSE_EVENTS_FILE).exists(),
                        possible_pandda_dir)
        ]

        # Filter on being PanDDA 1
        possible_pandda_dirs = [possible_pandda_dir
                                for possible_pandda_dir
                                in possible_pandda_dirs
                                if try_func(lambda x: (x / "pandda.done").exists(), possible_pandda_dir)
                                ]

        # Add the remaining folders
        for possible_pandda_dir in possible_pandda_dirs:
            print(f"\tAdding pandda for system {system.system_name}: {possible_pandda_dir}")
            pandda_1_dir = PanDDA1DirSQL(
                path=str(possible_pandda_dir),
                system=system,
            )

            session.add(pandda_1_dir)

        session.commit()

    for instance in session.query(PanDDA1DirSQL).order_by(PanDDA1DirSQL.id):
        print(f"{instance.system.system_name}: {instance.system_id}: {instance.path}")
        # for dataset in instance.datasets:
        #     print(f"\t{dataset.dtag}")


if __name__ == "__main__":
    fire.Fire(main)
