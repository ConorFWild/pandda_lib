import pathlib
import os

import fire
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
# from sqlalchemy_utils import database_exists, create_database


from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs
from pandda_lib.diamond_sqlite.diamond_sqlite import Base, ProjectDirSQL, DatasetSQL, SystemSQL, SystemEventMapSQL


def diamond_build_input_data_database(sqlite_filepath):
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    if sqlite_filepath.exists():
        os.remove(sqlite_filepath)
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    # if not sqlite_filepath.exists():
    #     create_database(str(sqlite_filepath))
    session = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)

    DatasetSQL.__table__.drop(engine)
    ProjectDirSQL.__table__.drop(engine)
    SystemSQL.__table__.drop(engine)

    Base.metadata.create_all(engine)

    print("Getting diamond data dirs...")
    diamond_data_dirs = DiamondDataDirs()

    print("Updating database...")
    for system, system_project_dirs in diamond_data_dirs.systems.items():
        print(f"System: {system.system_name}")
        system_project_sqls = []
        system_datasets = []

        for project_name, project_data_dir in system_project_dirs.items():
            print(f"Project: {project_name}")
            # project_data_dir = diamond_data_dirs[system]
            project_datasets = [
                DatasetSQL(
                    dtag=_dataset.dtag.dtag,
                    path=str(_dataset.path),
                    model_path=str(_dataset.model_path),
                    mtz_path=str(_dataset.mtz_path),
                    pandda_model_path=str(_dataset.pandda_model_path),
                    event_maps=[
                        SystemEventMapSQL(
                            path=str(event_map.path),
                            event_idx=event_map.event_idx,
                            bdc=event_map.bdc,
                        )
                        for event_map
                        in _dataset.event_maps

                    ],
                )
                for _dtag, _dataset
                in project_data_dir.datasets.items()
            ]
            system_datasets = system_datasets + project_datasets
            project_data_dir_sql = ProjectDirSQL(
                project_name=project_name,
                path=str(project_data_dir.path),
                datasets=project_datasets
            )
            session.add(project_data_dir_sql)
            system_project_sqls.append(project_data_dir_sql)

            # system_project_sqls[project_name] = project_data_dir_sql

        system_sql = SystemSQL(
            system_name=system.system_name,
            projects=system_project_sqls,
            datasets=system_datasets
        )
        session.add(system_sql)

        session.commit()

    print("Printing database systems...")

    for instance in session.query(SystemSQL).order_by(SystemSQL.id):
        print(f"{instance.system_name}: {[project.project_name for project in instance.projects]}")
        for dataset in instance.datasets:
            print(f"\t{dataset.dtag}")

    # print("Printing database datasets...")
    # for instance in session.query(DatasetSQL).order_by(S)


if __name__ == "__main__":
    fire.Fire(diamond_build_input_data_database)
