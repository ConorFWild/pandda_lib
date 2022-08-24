import pathlib
import os

import fire
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs
from pandda_lib.diamond_sqlite.diamond_sqlite import Base, ProjectDirSQL, DatasetSQL, PanDDADirSQL, SystemSQL


def main(sqlite_filepath: str):

    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()


    # List the number of systetms
    systems = session.query(SystemSQL).order_by(SystemSQL.id).all()
    print(f"Number of systems: {len(systems)}")

    # List key stats for each system
    for system in systems:
        for project in system.projects:
            print(f"# {system.system_name}: {project.path}")
            print(f"Number of datasets: {len(project.datasets)}")
            datasets_with_models = [
                _dataset.model_path
                for _dataset
                in project.datasets
                if _dataset.model_path != 'None'
            ]
            # print(datasets_with_models)
            print(f"Number of datasets with models: {len(datasets_with_models)}")

    # List key stats for panddas
    panddas = session.query(PanDDADirSQL).order_by(PanDDADirSQL.id).all()
    print(f"Number of panddas: {len(panddas)}")

    for pandda in panddas:
        print(f"# {pandda.path}")





if __name__ == "__main__":
    fire.Fire(main)
