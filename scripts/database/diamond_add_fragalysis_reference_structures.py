import pathlib
import os
import re

import fire
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from pandda_lib import constants
from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs
from pandda_lib.fs.pandda_result import PanDDAResult
from pandda_lib.diamond_sqlite.diamond_sqlite import (Base, ProjectDirSQL, DatasetSQL, PanDDADirSQL,
                                                      PanDDADatasetSQL, PanDDABuildSQL, PanDDAEventSQL,
                                                      ReferenceStructureSQL, SystemSQL)

reference_structure_regex = "([^\-]+)-([^\.]+)\.pdb"


def diamond_add_fragalysis_reference_structures(sqlite_filepath, reference_structure_dir):
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    reference_structure_dir = pathlib.Path(reference_structure_dir).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)

    # Remove table if already present
    ReferenceStructureSQL.__table__.drop(engine)

    Base.metadata.create_all(engine)

    # Get the systems
    systems = {instance.system_name: instance for instance in session.query(SystemSQL).order_by(
        SystemSQL.id).all()}

    # Get the datasets
    datasets = {
        instance.dtag: instance for instance in session.query(DatasetSQL).order_by(DatasetSQL.id).all()
    }

    # Go voer the reference structures
    for reference_stucture_path in reference_structure_dir.glob("*"):
        # print(reference_stucture_path.name)
        match = re.match(reference_structure_regex, reference_stucture_path.name, )
        system = match.groups()[0]
        dtag_number = match.groups()[1]
        if system == "Mpro":
            dtag_number = dtag_number[:-1]
            print(dtag_number)
        dtag = f"{system}-{dtag_number}"


        if system in systems:
            system_sql = systems[system]
        else:
            system_sql = None

        if dtag in datasets:
            dataset_sql = datasets[dtag]
        else:
            dataset_sql = None

        reference_structure_sql = ReferenceStructureSQL(
            path=str(reference_stucture_path),
            system=system_sql,
            dataset = dataset_sql
        )
        session.add(reference_structure_sql)

    session.commit()

    for instance in session.query(ReferenceStructureSQL).order_by(ReferenceStructureSQL.id):

        if instance.system:
            system_name = instance.system.system_name
        else:
            system_name = None

        if instance.dataset:
            dtag = instance.dataset.dtag
        else:
            dtag = None

        print(f"{system_name}: {dtag}: {instance.path}")

if __name__ == "__main__":
    fire.Fire(diamond_add_fragalysis_reference_structures)
