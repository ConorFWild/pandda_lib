import pathlib
import os

import numpy as np
import gemmi
import fire
from sqlalchemy.orm import sessionmaker, subqueryload
from sqlalchemy import create_engine
# import joblib
# from joblib import Parallel, delayed
import multiprocessing as mp

from pandda_lib import constants
from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs
from pandda_lib.fs.pandda_result import PanDDAResult
from pandda_lib.diamond_sqlite.diamond_sqlite import *
from pandda_lib.rscc import get_rscc
from pandda_lib.rscc.rscc import GetDatasetRSCC, Runner
from pandda_lib.rmsd.rmsd import _get_closest_event, get_rmsds_from_path


# from pandda_lib.custom_score import get_custom_score

class GetBuildRMSD:
    def __init__(self,
                 dataset_structure_path,
                 reference_structure_path,
                 build_path,
                 events,
                 high_confidence, ):
        self.dataset_structure_path = dataset_structure_path
        self.reference_structure_path = reference_structure_path
        self.build_path = build_path
        self.events = events
        self.high_confidence = high_confidence

    def __call__(self, ):

        high_confidence = self.high_confidence
        dataset_structure_path = self.dataset_structure_path
        reference_structure_path = self.reference_structure_path
        build_path = self.build_path
        events = self.events

        if len(events) != 0:

            # event_distances = []
            is_ligand_broken = False
            has_alignment_error = False

            has_closest_event = _get_closest_event(
                reference_structure_path,
                dataset_structure_path,
                events,
            )
            closest_event = has_closest_event

            if has_closest_event == "ALIGNMENTERROR":
                has_alignment_error = True
                alignment_error = has_alignment_error
                broken_ligand = False
                closest_event = None
                closest_rmsd = None

            else:

                try:
                    _rmsds = get_rmsds_from_path(
                        reference_structure_path,
                        dataset_structure_path,
                        build_path,
                    )

                    if _rmsds == "BROKENLIGAND":
                        is_ligand_broken = True

                    if _rmsds == "ALIGNMENTERROR":
                        has_alignment_error = True

                    if len(_rmsds) == 0:
                        broken_ligand = is_ligand_broken
                        alignment_error = has_alignment_error
                        closest_rmsd = None
                    else:
                        broken_ligand = is_ligand_broken
                        alignment_error = has_alignment_error
                        closest_rmsd = min(_rmsds)
                except Exception as e:
                    broken_ligand = is_ligand_broken
                    alignment_error = has_alignment_error
                    closest_rmsd = None
                    print(e)

        else:

            alignment_error = False
            broken_ligand = False
            closest_event = None
            closest_rmsd = None

        record = {
            'broken_ligand': broken_ligand,
            'alignment_error': alignment_error,
            'closest_event': closest_event,
            'closest_rmsd': closest_rmsd,  # None and num_events>0&num_builds>0 implies broken ligand
            'high_confidence': high_confidence,
        }

        return record


def diamond_add_autobuild_rmsds(sqlite_filepath, ):
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    # tmp_dir = pathlib.Path(tmp_dir).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)

    # Remove tables
    Base.metadata.create_all(engine)
    BuildRMSDSQL.__table__.drop(engine)
    Base.metadata.create_all(engine)

    # Get Autobuild PanDDA sqls
    run_set = {

    }
    sqls = {}

    reference_structures = {
        reference_structure_sql.dataset.dtag: reference_structure_sql
        for reference_structure_sql in
        session.query(ReferenceStructureSQL).options(subqueryload("*")).order_by(ReferenceStructureSQL.id).all()
        if reference_structure_sql.dataset
    }
    print(f"Matched {len(reference_structures)} refernces to datasets")

    for pandda_2 in session.query(PanDDADirSQL).options(subqueryload("*")).order_by(PanDDADirSQL.id).all():
        # for project in system.projects:
        #     for pandda_2 in project.pandda_2s:
        system = pandda_2.system
        project = pandda_2.project
        print(f"PanDDA 2: {system.system_name}: {project.project_name}")

        print(f"\t{len(pandda_2.pandda_dataset_results)}")
        for pandda_dataset in pandda_2.pandda_dataset_results:
            print(f"\tdataset {pandda_dataset.dtag} events: {len(pandda_dataset.events)}")

            if not pandda_dataset.input_pdb_path:
                continue
            high_confidence = False
            if pandda_dataset.dtag in reference_structures:
                reference_structure_path = reference_structures[pandda_dataset.dtag].path
                high_confidence = True
            else:
                if pandda_dataset.dataset:
                    dataset = pandda_dataset.dataset
                    if dataset.pandda_model_path:
                        reference_structure_path = dataset.pandda_model_path
                    else:
                        continue
                else:
                    continue

            for event in pandda_dataset.events:
                if event.builds:
                    print(f"\tGetting RMSDS for {system.system_name} {project.project_name} "
                          f"{pandda_dataset.dtag} {event.idx}")

                print(f"\t\tevent {event.idx} num builds: {len(event.builds)}")

                for build in event.builds:

                    build_to_run = GetBuildRMSD(
                        dataset_structure_path=pandda_dataset.input_pdb_path,
                        reference_structure_path=reference_structure_path,
                        build_path=build.build_path,
                        events={
                            _event.idx: [
                                _event.x,
                                _event.y,
                                _event.z,
                            ]
                            for _event in pandda_dataset.events
                        },
                        high_confidence=high_confidence,
                    )

                    run_set[(system.system_name, project.project_name, pandda_dataset.dtag,
                             event.idx, build.id)] = build_to_run
                    sqls[(system.system_name, project.project_name, pandda_dataset.dtag,
                          event.idx, build.id)] = {
                        "System": system,
                        "Project": project,
                        "PanDDA": pandda_2,
                        "Dataset": pandda_dataset,
                        "Event": event,
                        "Build": build
                    }

    print(f"Number of builds to score: {len(run_set)};")

    print("Getting RMSDs...")
    mp.set_start_method('spawn')

    with mp.Pool(1) as p:
        print("Getting run set")

        print("Running")
        selected_rmsds = p.map(
            Runner(),
            run_set.values()
        )

    print("Inserting to database...")
    for run_key, selected_rmsd in zip(run_set, selected_rmsds):
        build_rmsd_sql = BuildRMSDSQL(
            broken_ligand=selected_rmsd['broken_ligand'],
            alignment_error=selected_rmsd['alignment_error'],
            # closest_event=selected_rmsd['closest_event'],
            closest_rmsd=selected_rmsd['closest_rmsd'],  # None and num_events>0&num_builds>0 implies broken ligand
            high_confidence=selected_rmsd['high_confidence']
        )
        build_sql = sqls[run_key]["Build"]
        build_sql.rmsd = build_rmsd_sql

        session.add(build_rmsd_sql)

    session.commit()

    print("Printing database datasets...")
    for instance in session.query(BuildRMSDSQL).order_by(BuildRMSDSQL.id):
        print(f"{instance.closest_rmsd}")
        # if instance.bound_state_model:
        #     print(f"\t{instance.bound_state_model.rscc}")

    # for instance in session.query(DatasetSQL).order_by(DatasetSQL):


if __name__ == "__main__":
    fire.Fire(diamond_add_autobuild_rmsds)
