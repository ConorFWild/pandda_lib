import pathlib
import os

import fire
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs
from pandda_lib.diamond_sqlite.diamond_sqlite import Base, SystemDataDirSQL, DatasetSQL


def main(sqlite_filepath):

    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()


    # List the number of systetms

    systems = session.query(SystemDataDirSQL).order_by(SystemDataDirSQL.id).all()
    print(f"Number of systems: {len(systems)}")

    # List key stats for each system
    for system in systems:
        print(f"# {system.system_name}")
        print(f"Number of datasets: {len(system.datasets)}")
        datasets_with_models = [_dataset for _dataset in system.datasets if _dataset.model_path is not None]
        print(f"Number of datasets with models: {len(datasets_with_models)}")


if __name__ == "__main__":
    fire.Fire(main)
