import itertools
import pathlib
import os
import pdb
import re
import string

import numpy as np
# import pandas as pd
# from matplotlib import pyplot as plt

import fire
from sqlalchemy.orm import sessionmaker, subqueryload
from sqlalchemy import create_engine

from pandda_lib import constants
from pandda_lib.diamond_sqlite.diamond_data import DiamondDataDirs
from pandda_lib.fs.pandda_result import PanDDAResult
from pandda_lib.diamond_sqlite.diamond_sqlite import *

import gemmi

import pandas as pd



molecules = {
    "Merging": [("70X-x0165", 1), ("STAG1A-x0333", 2), ("STAG1A-x0153", 4), ("AAVNAR-x1233", 2),
                ("AAVNAR-x1501", 1), ("NSP16-x0155", 4)],
    "LowRes": [("AAVNAR-x1660", 1), ("70X-x0090", 3), ("STAG1A-x0535", 5)],
    "Hetro": [("NSP16-x0440", 2), ("STAG1A-x0452", 2), ("70X-x0070", 1), ("AAVNAR-x1513", 1), ("AAVNAR-x1705", 1)],
    "Noise": [("STAG1A-x0579", 1), ("NSP16-x0357", 1), ("NSP16_x0415", 1), ("70X_x0137", 4), ("70X_x0109", 1)],
    "Contaminant": [("NSP16_x0653", 1)],
    "BVKP126": [("BKVP126-x0692", None)],
    "NSP16_PARTIAL_LIG": [("NSP16-x0422", None), ("NSP16_x0489", None)],
    "NSP16_CONFORTMATIONS": [("NSP16_x0415", 1)],
    "RSCC23": [
        ("LchARH3-x0073", None, 840,), ("AAVNAR-x1099", None, 783), ("XX02KALRNA-x1630", None, 491), ("Mpro-x0395", None, 494),
        ("PHIPA-x2089", None, 510,), ("Mpro-x3015", None, 268)],
    "RSCC45": [
        ("Mac1-AP0176", None, 623), ("Mpro-x3086", None, 707), ("SOCS2A-x0082", None, 137), ("SHH-x484", None, 490), ("Mpro-x10163", None, 277),
        ("TbMDO-x200", None, 42)
    ],
    "RSCC67": [("NUDT22A-x0649", None, 550), ("NUDT22A-x0637", None, 223), ("PHIPA-x20028", None, 997), ("PHIPA-x11138", None, 655),
               ("SHH-x185", None, 20), ("XX02KALRNA-x5056", None, 513)],
    "RSCC89": [("DCP2B-x1364", None, 143), ("LchARH3-x0756", None, 78), ("NUDT22A-x0917", None, 407), ("PHIPA-x20551", None, 14),
               ("XX02KALRNA-x5035", None, 741), ("PHIPA-x20594", None, 10)],
    "AutobuildFailureExamplesPanDDA": [("HSP90-x0363", None), ("XX02KALRNA-x1667", None)],
    "AutobuildFailureExamplesAmbiguity": [("KPNF-x0075", None), ("NUDT7A-x0399", None)],
    "AutobuildFailureExamplesRhofit": [("PlPro-x0689", None), ("CYCK-x0347", None)],
    "AutobuildFailureExamplesSource": [("PlPro-x0630", None), ("PKM1-x0103", None)],
    "AutobuildFailureExamplesLegit": [("TRF1-x0185", None)],
    "Unannotated": [("G13D_x0191", None), ("G13D-x0184", None), ("JMJD1BA-x1431", None), ("ATAD2A-x1727", None),
                    ("ATAD2A-x1754", None), ("Mpro-i0380", None), ("NUDT22A-x0917", None), ("NUDT22A-x0289", None)],
    "Iso": [("mkeap1-x0080", None), ("mkeap1-x0527", None), ("MACROD1A-x0302", None)],
    "Portein": [("TNCA-x0188", None), ("TNCA-x0067", None), ("MACROD1A-x0702", None)],
    "AmbigDense": [("JMJD1BA-x1172", None), ("MACROD1A-x0310", None), ("Mpro-i0039", None)],
    "PartialDense": [("JMJD1BA-x1231", None), ("ATAD2A-x1835", None), ("ATAD2A-x1710", None), ("mkeap1-x0068", None),
                     ("NUDT22A-x1058", None)],
    "BadBuilds": [("G13D-x0796", None), ("G13D-x0758", None), ("TNCA-x0100", None)],
    "ProteinDensityDistracting": [("JMJD1BA-x1108", None), ("TNCA-x0321", None)],
    "ContaminantAutobuildAttempt": [("NUDT22A-x1059", None)],
    "UnluckyBuilds": [("MACROD1A-x0276", None), ("Mpro-i0031", None), ("Mpro-i0012", None)],
    "BAZ2BA_x447": [("BAZ2BA-x447", None)],
    "BAZ2BA_x557": [("BAZ2BA-x557", None)],
    "JMJD2DA_x390": [("JMJD2DA-x390", None)],
    "JMJD2D_x427": [("JMJD2DA-x427", None)],
    "JMJD2DA_x533": [("JMJD2DA-x533", None)],
    "JMJD2DA_x620": [("JMJD2DA-x620", None)],
    "PHIPAMultiConf": [("PHIPA_1316", 6), ("PHIPA_1431", 5)],
    "FALZA_2824": [("FALZA_2824_4", None)],
    "BAZ2BANewHits": [("BAZ2BA-x446", None), ("BAZ2BA-x470", None), ("BAZ2BA-x480", None), ("BAZ2BA-x556", None)],
    "JMJD2DNewHits": [("JMJD2D-x353", None), ("JMJD2D-x408", None), ("JMJD2DA-x379", None), ("JMJD2DA-x381", None),
                      ("JMJD2DA-x453", None), ("JMJD2DA-x622", None)],
    "KPNFNewHits": [("KPNF-x0114", None), ("KPNF-x0115", None), ("KPNF-x0123", None), ("KPNF-x0157", None)],
    "FALZANewHits": [("FALZA-x0253", None), ("FALZA-x0341", None), ("FALZA-x0371", None), ("FALZA-x0439", None)],
    "TDP2NewHits": [("TDP2-x0027", None), ("TDP2-x0047", None), ("TDP2-x0156", None), ("TDP2-x0165", None)],
    "MeanMapsDiff": [("NUDT7A-x0136", None)],

}

experiment_groups = {
    "PanDDA 1 High Ranked Non-Hits": ["Merging", "LowRes", "Hetro", "Noise", "Contaminant"],
    "Custom PanDDA 1s For Missing Hits": ["NSP16_PARTIAL_LIG", "NSP16_CONFORTMATIONS", ],
    "PanDDA 1 Build RSCCs": ["RSCC23", "RSCC45", "RSCC67", "RSCC89"],
    "Autobuild Human Build Reproduction Failure": ["AutobuildFailureExamplesPanDDA",
                                                   "AutobuildFailureExamplesAmbiguity",
                                                   "AutobuildFailureExamplesRhofit", "AutobuildFailureExamplesSource",
                                                   "AutobuildFailureExamplesLegit"],
    "Autobuild High Rank Non Hit": ["Unannotated", "Iso", "Portein", "AmbigDense"],
    "Autobuild Low Rank Hit": ["PartialDense", "BadBuilds", "ProteinDensityDistracting", "ContaminantAutobuildAttempt",
                               "UnluckyBuilds"],
    "Known Missing Hits Custom PanDDAs BAZ2BA": ["BAZ2BA_x447", "BAZ2BA_x557"],
    "Known Missing Hits Custom PanDDAs JMJD2DA": ["JMJD2DA_x390", "JMJD2D_x427", "JMJD2DA_x533", "JMJD2DA_x620"],
}

data_sources_inverse = {
    "PanDDA 1 High Ranked Non-Hits": "database",
    "Custom PanDDA 1s For Missing Hits": "database",
    "PanDDA 1 Build RSCCs": "/dls/science/groups/i04-1/conor_dev/experiments/pandda_autobuilding",
    "Autobuild Human Build Reproduction Failure": "database",
    "Autobuild High Rank Non Hit": "database",
    "Known Missing Hits Custom PanDDAs BAZ2BA": "/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2/pandda_analysis/output_BAZ2BA",
    "Known Missing Hits Custom PanDDAs JMJD2DA": "/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2/pandda_analysis/output_JMJD2D",
    "BVKP126": "/dls/labxchem/data/2020/lb25586-3",
    "PHIPAMultiConf": "/dls/science/groups/i04-1/conor_dev/experiments/panddas/PHIPA",
    "FALZA_2824": "/dls/science/groups/i04-1/conor_dev/experiments/panddas/FALZA",
    "BAZ2BANewHits": "/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2/pandda_2_reproduce_cluster4x/pandda_2/output_BAZ2BA/",
    "JMJD2DNewHits": "/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2/pandda_2_reproduce_cluster4x/pandda_2/output_JMJD2DA/",
    "KPNFNewHits": "/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2/autobuilding/KPNF_lb24383-4",
    "FALZANewHits": "/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2/autobuilding/FALZA_lb13385-61",
    "TDP2NewHits": "/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2/autobuilding/TDP2_lb17436-1",
    "MeanMapsDiff": "/dls/science/groups/i04-1/conor_dev/experiments/panddas/NUDT7A"
}

data_sources = {
    "database": ["PanDDA 1 High Ranked Non-Hits", "Custom PanDDA 1s For Missing Hits", "Autobuild Human Build Reproduction Failure", "Autobuild High Rank Non Hit", ],
    "/dls/science/groups/i04-1/conor_dev/experiments/pandda_autobuilding": ["PanDDA 1 Build RSCCs"],
    "/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2/pandda_analysis/output_BAZ2BA": ["Known Missing Hits Custom PanDDAs BAZ2BA"],
    "/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2/pandda_analysis/output_JMJD2D": ["Known Missing Hits Custom PanDDAs JMJD2DA"],
    "/dls/labxchem/data/2020/lb25586-3": "BVKP126",
    "/dls/science/groups/i04-1/conor_dev/experiments/panddas/PHIPA": ["PHIPAMultiConf"],
    "/dls/science/groups/i04-1/conor_dev/experiments/panddas/FALZA": ["FALZA_2824"],
    "/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2/pandda_2_reproduce_cluster4x/pandda_2/output_BAZ2BA/": ["BAZ2BANewHits"],
    "/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2/pandda_2_reproduce_cluster4x/pandda_2/output_JMJD2DA/": ["JMJD2DNewHits"],
    "/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2/autobuilding/KPNF_lb24383-4": ["KPNFNewHits"],
    "/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2/autobuilding/FALZA_lb13385-61": ["FALZANewHits"],
    "/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2/autobuilding/TDP2_lb17436-1": ["TDP2NewHits"],
    "/dls/science/groups/i04-1/conor_dev/experiments/panddas/NUDT7A": ["MeanMapsDiff"],
}
def try_make(path):
    try:
        os.mkdir(path)
    except Exception as e:
        return


def try_link(source_path, target_path):
    if target_path.exists():
        return
    try:
        os.symlink(source_path, target_path)
    except Exception as e:
        print(e)
        return

def get_event_map_idx(event_map_name):
    pattern = "_([0-9]+)_1-BDC_"
    matches = re.findall(pattern, event_map_name)
    event_idx = int(matches[0])

    return event_idx

# def get_files_from_database(molecules_list, output_dir):
#     sqlite_filepath = "/dls/science/groups/i04-1/conor_dev/pandda_lib/diamond_2.db"
#     sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
#     engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
#     session = sessionmaker(bind=engine)()
#     Base.metadata.create_all(engine)
#
#     initial_datasets = session.query(DatasetSQL).options(subqueryload(DatasetSQL.bound_state_model)).order_by(
#         DatasetSQL.id).all()
#     print(f"Got {len(initial_datasets)} inital datasets")
#
#     molecule_dtags = {molecule[0]: molecule[1] for molecule in molecules_list}
#
#     for dataset in initial_datasets:
#         dtag = dataset.dtag
#         if dtag not in molecule_dtags:
#             continue
#
#         pandda_model_path = dataset.pandda_model_path
#         if pandda_model_path is not None:
#             if pandda_model_path != "None":
#                 try_link(
#                     pandda_model_path,
#                     output_dir / f"{dtag}.pdb"
#                 )
#
#         mtz_path = dataset.mtz_path
#         if mtz_path is not None:
#             if mtz_path != "None":
#                 try_link(
#                     mtz_path,
#                     output_dir / f"{dtag}.mtz"
#                 )
#
#         event_maps = dataset.event_maps
#         event_idx = molecule_dtags[dtag]
#         if event_idx:
#             for event_map in event_maps:
#                 event_map_idx = get_event_map_idx(pathlib.Path(event_map.path).name)
#                 if event_map_idx == event_idx:
#                     try_link(
#                         event_map.path,
#                         output_dir / f"{dtag}_{event_map_idx}.ccp4"
#                     )


def get_files_from_database(molecules_list, output_dir):
    sqlite_filepath = "/dls/science/groups/i04-1/conor_dev/pandda_lib/diamond_2.db"
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)

    initial_datasets = session.query(PanDDADatasetSQL).options(
        subqueryload(PanDDADatasetSQL.events).options(
            subqueryload(PanDDAEventSQL.builds).options(
                subqueryload(PanDDABuildSQL.rscc)
            )
        )
    ).order_by(
        DatasetSQL.id).all()
    print(f"Got {len(initial_datasets)} inital datasets")

    molecule_dtags = {molecule[0]: molecule[1] for molecule in molecules_list}

    for dataset in initial_datasets:
        dtag = dataset.dtag
        if dtag not in molecule_dtags:
            continue

        pandda_model_path = dataset.pandda_model_path
        if pandda_model_path is not None:
            if pandda_model_path != "None":
                try_link(
                    pandda_model_path,
                    output_dir / f"{dtag}.pdb"
                )

        mtz_path = dataset.mtz_path
        if mtz_path is not None:
            if mtz_path != "None":
                try_link(
                    mtz_path,
                    output_dir / f"{dtag}.mtz"
                )

        event_maps = dataset.event_maps
        event_idx = molecule_dtags[dtag]
        if event_idx:
            for event_map in event_maps:
                event_map_idx = get_event_map_idx(pathlib.Path(event_map.path).name)
                if event_map_idx == event_idx:
                    try_link(
                        event_map.path,
                        output_dir / f"{dtag}_{event_map_idx}.ccp4"
                    )


def plot_rscc_vs_rmsd():
    # Get the table

    thesis_dir = pathlib.Path("/dls/science/groups/i04-1/conor_dev/pandda_lib/thesis/")
    output_dir = thesis_dir / "autobuild_samples"
    try_make(output_dir)

    # Iterate through the datasources, making PanDDAs to get figures for each experiment group
    for datasource, datasource_experiments in data_sources.items():
        print(f"Datasource: {datasource}")
        for experiment_key in datasource_experiments:
            print(f"\tExperiment: {experiment_key}")
            experiment_output_dir = output_dir / experiment_key
            try_make(experiment_output_dir)
            # Get the molecules
            if experiment_key in experiment_groups:
                molecules_list = []
                for molecule_group_key in experiment_groups[experiment_key]:
                    for molecule in molecules[molecule_group_key]:
                        molecules_list.append(molecule)

            else:
                molecules_list = molecules[experiment_key]

            print(f"\t\tMolecules list: {molecules_list}")

            if datasource == "database":
                get_files_from_database(molecules_list, experiment_output_dir)

            exit()



if __name__ == "__main__":
    fire.Fire(plot_rscc_vs_rmsd)