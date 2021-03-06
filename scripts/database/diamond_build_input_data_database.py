import pathlib
import os

import fire
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs
from pandda_lib.diamond_sqlite.diamond_sqlite import Base, SystemDataDirSQL, DatasetSQL


def __main__(sqlite_filepath):

    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    os.remove(sqlite_filepath)
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)

    print("Getting diamond data dirs...")
    diamond_data_dirs = DiamondDataDirs()

    print("Updating database...")
    for system in diamond_data_dirs:
        system_data_dir = diamond_data_dirs[system]
        system_data_dir_sql = SystemDataDirSQL(
            system_name=system.system_name,
            path=str(system_data_dir.path),
            datasets=[
                DatasetSQL(
                    dtag=_dataset.dtag.dtag,
                    path=str(_dataset.path),
                    model_path=str(_dataset.model_path)
                )
                for _dtag, _dataset
                in system_data_dir.datasets.items()
            ]
        )
        session.add(system_data_dir_sql)

    session.commit()

    print("Printing database systems...")

    for instance in session.query(SystemDataDirSQL).order_by(SystemDataDirSQL.id):
        print(f"{instance.system_name}: {instance.path}")
        for dataset in instance.datasets:
            print(f"\t{dataset.dtag}")

    # print("Printing database datasets...")
    # for instance in session.query(DatasetSQL).order_by(S)

if __name__ == "__main__":
    fire.Fire(__main__)
