import pathlib

import fire
from sqlalchemy.orm import sessionmaker, create_engine

from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs
from pandda_lib.diamond_sqlite.diamond_sqlite import SystemDataDirSQL


def __main__(sqlite_filepath):

    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)

    diamond_data_dirs = DiamondDataDirs()

    for system in diamond_data_dirs:
        system_data_dir = diamond_data_dirs[system]
        system_data_dir_sql = SystemDataDirSQL(
            path=str(system_data_dir.path)
        )
        session.add(system_data_dir_sql)

    session.commit()

    for instance in session.query(SystemDataDirSQL).order_by(SystemDataDirSQL.id):
        print(instance.name, instance.fullname)


if __name__ == "__main__":
    fire.Fire(__main__)
