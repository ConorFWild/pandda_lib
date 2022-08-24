import pathlib
import os

import fire
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs
from pandda_lib.diamond_sqlite.diamond_sqlite import Base, ProjectDirSQL, DatasetSQL, SystemSQL


def __main__(sqlite_filepath):

    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    os.remove(sqlite_filepath)
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)

    DatasetSQL.__table__.drop(engine)
    ProjectDirSQL.__table__.drop(engine)
    SystemSQL.__table__.drop(engine)

    print("Getting diamond data dirs...")
    diamond_data_dirs = DiamondDataDirs()

    print("Updating database...")
    for system, system_project_dirs in diamond_data_dirs.systems.items():

        system_project_sqls = []
        system_datasets = []

        for project_name, project_data_dir in system_project_dirs.items():
            # project_data_dir = diamond_data_dirs[system]
            project_datasets = [
                    DatasetSQL(
                        dtag=_dataset.dtag.dtag,
                        path=str(_dataset.path),
                        model_path=str(_dataset.model_path)
                    )
                    for _dtag, _dataset
                    in project_data_dir.datasets.items()
                ]
            system_datasets = system_datasets + project_datasets
            project_data_dir_sql = ProjectDirSQL(
                project_name=system.system_name,
                path=str(project_data_dir.path),
                datasets=project_datasets
            )
            session.add(project_data_dir_sql)
            system_project_sqls.append(project_data_dir_sql)

            system_project_sqls[project_name] = project_data_dir_sql

        system_sql = SystemSQL(
            system_name=system,
            project_dirs=system_project_sqls,
        )
        session.add(system_sql)

    session.commit()

    print("Printing database systems...")

    for instance in session.query(SystemSQL).order_by(SystemSQL.id):
        print(f"{instance.system_name}: {instance.systems}")
        for dataset in instance.datasets:
            print(f"\t{dataset.dtag}")

    # print("Printing database datasets...")
    # for instance in session.query(DatasetSQL).order_by(S)

if __name__ == "__main__":
    fire.Fire(__main__)
