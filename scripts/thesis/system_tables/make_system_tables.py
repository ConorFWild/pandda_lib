import itertools
import pathlib
import os
import pdb

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

import seaborn as sns

sns.set(rc={'figure.figsize': (2 * 11.7, 2 * 8.27)})
sns.set(font_scale=3)
# sns.color_palette("hls", 8)
sns.set_palette("hls")
sns.set_palette("crest")


class SystemInfo:
    def __init__(self,
                 published,
                 protein_name,
                 organism,
                 classification,
                 domain,
                 pdb_deposition_group,
                 gene,
                 pandda_data_deposition,
                 literature
                 ):
        self.published=published
        self.protein_name = protein_name
        self.organism = organism
        self.classification = classification
        self.domain = domain
        self.pdb_deposition_group = pdb_deposition_group
        self.gene = gene
        self.pandda_data_deposition = pandda_data_deposition
        self.literature = literature


system_info = {
    "PDK2": SystemInfo(
        published=False,
        protein_name="Pyruvate dehydrogenase kinase 2",
        organism="Homo sapiens",
        classification="Transferase",
        domain="Histidine kinase",
        pdb_deposition_group=["6TN2", "6TN0", "6TMZ", "6TMQ", "6TMP"],
        gene="PDK2",
        pandda_data_deposition=None,
        literature="10.1038/s42004-020-00367-0",
    ),
    "BKVP126": SystemInfo(
        published=True,
        protein_name="Capsid protein VP1",
        organism="Merkel cell polyomavirus",
        classification="Viral protein",
        domain=None,
        pdb_deposition_group=["7B69", "7B6A", "7B6C"],
        gene="VP1",
        pandda_data_deposition=None,
        literature="10.1039/D2CB00052K",
    ),
    "NSP16": SystemInfo(
        published=True,

        protein_name="Non-structural protein 16",
        organism="Severe acute respiratory syndrome coronavirus 2",
        classification="Viral protein",
        domain="2′-O-methyltransferase",
        pdb_deposition_group=["7B69", "7B6A", "7B6C"],
        gene=None,  # "orf1ab",
        pandda_data_deposition="Fragalysis",
        literature=None,
    ),
    "STAG1A": SystemInfo(
        published=True,

        protein_name="Cohesin subunit SA-1",
        organism="Homo sapiens",
        classification="Transcription",
        domain=None,
        pdb_deposition_group="G_1002084",
        gene="STAG1",
        pandda_data_deposition="Fragalysis",
        literature=None,
    ),
    "AAVNAR": SystemInfo(
        published=False,
        protein_name="Cohesin subunit SA-1",
        organism="Homo sapiens",
        classification="Transcription",
        domain=None,
        pdb_deposition_group="G_1002084",
        gene="STAG1",
        pandda_data_deposition="Fragalysis",
        literature=None,
    ),

    "70X": SystemInfo(
        published=False,
        protein_name="Cohesin subunit SA-1",
        organism="Homo sapiens",
        classification="Transcription",
        domain=None,
        pdb_deposition_group="G_1002084",
        gene="STAG1",
        pandda_data_deposition="Fragalysis",
        literature="10.1107/S2053230X21007378",  # Actually right
    ),
    "XX02KALRNA": SystemInfo(
        published=True,
        protein_name="Kalirin RhoGEF kinase in complex with Rac family small GTPase 1",
        organism="Homo sapiens",
        classification="Hydrolase",
        domain=None,
        pdb_deposition_group=["5O33"],
        gene=["KALRN", "Rac1"],
        pandda_data_deposition="10.5281/zenodo.3271348",
        literature="10.1002/anie.201900585",
    ),
    "HSP90": SystemInfo(
        published=False,

        protein_name=None,
        organism=None,
        classification=None,
        domain=None,
        pdb_deposition_group=None,
        gene=None,
        pandda_data_deposition=None,
        literature=None,
    ),
    "BAZ2BA": SystemInfo(
        published=True,
        protein_name="Bromodomain adjacent to zinc finger domain protein",
        organism="Homo sapiens",
        classification="DNA binding protein",
        domain="bromodomain",
        pdb_deposition_group="G_1002018",
        gene="BAZ2B",
        pandda_data_deposition="10.5281/zenodo.48768",
        literature=None,
    ),
    "JMJD2DA": SystemInfo(
        published=True,

        protein_name="Lysine-specific demethylase 4D",
        organism="Homo sapiens",
        classification="Oxidoreductase",
        domain=["JmjN", "JmjC"],
        pdb_deposition_group=None,
        gene="KDM4D",
        pandda_data_deposition="10.5281/zenodo.48770",
        literature=None,
    ),
    "Mpro": SystemInfo(
        published=True,

        protein_name="Mpro",
        organism="Severe acute respiratory syndrome coronavirus 2",
        classification="Hydrolase/hydrolase inhibitor",
        domain=["JmjN", "JmjC"],
        pdb_deposition_group="G_1002153",
        gene=None,
        pandda_data_deposition="Fragalysis",
        literature="10.1038/s41467-020-18709-w",
    ),
    "Mac1": SystemInfo(
        published=True,

        protein_name="Non-structural protein 3",
        organism="Severe acute respiratory syndrome coronavirus 2",
        classification="Hydrolase",
        domain="Macrodomain 1",
        pdb_deposition_group="G_1002171",
        gene=None,
        pandda_data_deposition="10.5281/zenodo.4716089",
        literature="10.1101/2020.11.24.393405",
    ),
    "NME4": SystemInfo(
        published=False,
        protein_name="Nucleoside diphosphate kinase, mitochondrial",
        organism="Homo sapiens",
        classification="Transferase",
        domain="Macrodomain 1",
        pdb_deposition_group=None,
        gene="NME4",
        pandda_data_deposition=None,
        literature=None,
    ),
    "GN6S": SystemInfo(
        published=False,
        protein_name="Glucosamine (N-acetyl)-6-sulfatase",
        organism="Homo sapiens",
        classification="Hydrolase",
        domain=None,
        pdb_deposition_group=None,
        gene="GNS",
        pandda_data_deposition=None,
        literature=None,
    ),
    "SOCS2A": SystemInfo(
        published=False,

        protein_name="Suppressor of cytokine signaling 2",
        organism="Homo sapiens",
        classification="Ligase",
        domain=None,
        pdb_deposition_group=None,
        gene="SOCS2",
        pandda_data_deposition=None,
        literature="10.26434/chemrxiv-2022-bvj80",
    ),
    "B2m": SystemInfo(
        published=False,
        protein_name="Beta-2-microglobulin",
        organism="Homo sapiens",
        classification="Histocompatibility antigen",
        domain=None,
        pdb_deposition_group=None,
        gene="B2M",
        pandda_data_deposition=None,
        literature=None,
    ),
    "LcARH3": SystemInfo(
        published=False,
        protein_name="Beta-2-microglobulin",
        organism="Homo sapiens",
        classification="Histocompatibility antigen",
        domain=None,
        pdb_deposition_group=None,
        gene="B2M",
        pandda_data_deposition=None,
        literature=None,
    ),
    "SHH": SystemInfo(
        published=False,
        protein_name="Sonic hedgehog protein",
        organism="Homo sapiens",
        classification="Signalling protein",
        domain=None,
        pdb_deposition_group=None,
        gene="SHH",
        pandda_data_deposition=None,
        literature=None,
    ),
    "PHIPA": SystemInfo(
        published=True,
        protein_name="Pleckstrin Homology Domain Interacting Protein",
        organism="Homo sapiens",
        classification="Signalling protein",
        domain="Bromodomain",
        pdb_deposition_group="G_1002187",
        gene="PHIP",
        pandda_data_deposition="Fragalysis",
        literature="10.1039/c5sc03115j",
    ),
    "NUDT22A": SystemInfo(
        published=True,
        protein_name="Uridine diphosphate glucose pyrophosphatase NUDT22",
        organism="Homo sapiens",
        classification="Hydrolase",
        domain="Bromodomain",
        pdb_deposition_group="G_1002128",
        gene="NUDT22",
        pandda_data_deposition="SGC",
        literature=None,
    ),
    "DCP2B": SystemInfo(
        published=True,
        protein_name="mRNA-decapping enzyme 2",
        organism="Homo sapiens",
        classification="Hydrolase",
        domain="Bromodomain",
        pdb_deposition_group="G_1002061",
        gene="DCP2",
        pandda_data_deposition="10.5281/zenodo.1437589",
        literature=None,
    ),
    "TbMDO": SystemInfo(
        published=False,

        protein_name="mRNA-decapping enzyme 2",
        organism="Homo sapiens",
        classification="Hydrolase",
        domain="Bromodomain",
        pdb_deposition_group="G_1002061",
        gene="DCP2",
        pandda_data_deposition="10.5281/zenodo.1437589",
        literature=None,
    ),
    "SHMT2A": SystemInfo(
        published=False,

        protein_name="Serine hydroxymethyltransferase, mitochondrial",
        organism="Homo sapiens",
        classification="Transferase",
        domain=None,
        pdb_deposition_group=None,
        gene="SHMT2",
        pandda_data_deposition=None,
        literature=None,
    ),
    "Lc_TbrB1": SystemInfo(
        published=False,
        protein_name="Serine hydroxymethyltransferase, mitochondrial",
        organism="Homo sapiens",
        classification="Transferase",
        domain=None,
        pdb_deposition_group=None,
        gene="SHMT2",
        pandda_data_deposition=None,
        literature=None,
    ),
    "TBXTA": SystemInfo(
        published=True,
        protein_name="T-box transcription factor T",
        organism="Homo sapiens",
        classification="Transcription",
        domain=None,
        pdb_deposition_group="G_1002080",
        gene="TBXT",
        pandda_data_deposition="Fragalysis",
        literature=None,
    ),
    "KPNF": SystemInfo(
        published=False,

        protein_name=None,
        organism=None,
        classification=None,
        domain=None,
        pdb_deposition_group=None,
        gene=None,
        pandda_data_deposition=None,
        literature=None,
    ),
    "CYCK": SystemInfo(
        published=False,

        protein_name=None,
        organism=None,
        classification=None,
        domain=None,
        pdb_deposition_group=None,
        gene=None,
        pandda_data_deposition=None,
        literature=None,
    ),
    "P1Pro": SystemInfo(
        published=False,

        protein_name=None,
        organism=None,
        classification=None,
        domain=None,
        pdb_deposition_group=None,
        gene=None,
        pandda_data_deposition=None,
        literature=None,
    ),
    "PKM1": SystemInfo(
        published=False,

        protein_name="Pyruvate kinase M1",
        organism="Homo sapiens",
        classification="Kinase",
        domain=None,
        pdb_deposition_group=None,
        gene="PKM",
        pandda_data_deposition=None,
        literature=None,
    ),
    "TRF1": SystemInfo(
        published=False,
        protein_name="Telomeric repeat-binding factor 1",
        organism="Homo sapiens",
        classification="Telomere binding",
        domain=None,
        pdb_deposition_group=None,
        gene="TERF1",
        pandda_data_deposition=None,
        literature=None,
    ),
    "G13D": SystemInfo(
        published=False,
        protein_name="Telomeric repeat-binding factor 1",
        organism="Homo sapiens",
        classification="Telomere binding",
        domain=None,
        pdb_deposition_group=None,
        gene="TERF1",
        pandda_data_deposition=None,
        literature=None,
    ),
    "JMJD1BA": SystemInfo(
        published=True,
        protein_name="Lysine-specific demethylase 3B",
        organism="Homo sapiens",
        classification="Oxidoreductase",
        domain=None,
        pdb_deposition_group="G_1002146",
        gene="KDM3B",
        pandda_data_deposition="10.5281/zenodo.3831207",
        literature=None,
    ),
    "ATAD2A": SystemInfo(
        published=True,
        protein_name="ATPase family AAA domain-containing protein 2",
        organism="Homo sapiens",
        classification="Hydrolase",
        domain="Bromodomain",
        pdb_deposition_group="G_1002118",
        gene="ATAD2",
        pandda_data_deposition="Fragalysis",
        literature="10.1039/C8OB00099A",
    ),
    "mkeap1": SystemInfo(
        published=True,
        protein_name="Kelch-like ECH-associated protein 1",
        organism="Homo sapiens",
        classification="Transcription",
        domain=None,
        pdb_deposition_group=None,
        gene="KEAP1",
        pandda_data_deposition=None,
        literature=None,
    ),
    "MACRO1DA": SystemInfo(
        published=False,
        protein_name="ADP-ribose glycohydrolase MACROD1",
        organism="Homo sapiens",
        classification="Hydrolase",
        domain=None,
        pdb_deposition_group=None,
        gene="MACROD1",
        pandda_data_deposition=None,
        literature=None,
    ),
    "DCLRE1AA": SystemInfo(
        published=True,

        protein_name="DNA cross-link repair 1A protein",
        organism="Homo sapiens",
        classification="Hydrolase",
        domain=None,
        pdb_deposition_group="G_1002034",
        gene="DCLRE1A",
        pandda_data_deposition="Fragalysis",
        literature=None,
    ),
    "TNCA": SystemInfo(
        published=True,
        protein_name="Tenascin-C",
        organism="Homo sapiens",
        classification="Cell adhesion",
        domain=None,
        pdb_deposition_group=None,
        gene="TNC",
        pandda_data_deposition="Fragalysis",
        literature=None,
    ),
    "BRD1": SystemInfo(
        published=True,
        protein_name="Bromodomain-containing protein 1",
        organism="Homo sapiens",
        classification="Gene regulation",
        domain=None,
        pdb_deposition_group="G_1002022",
        gene="BRD1",
        pandda_data_deposition="10.5281/zenodo.290217",
        literature=None,
    ),
    "PTP1B": SystemInfo(
        published=True,
        protein_name="Tyrosine-protein phosphatase non-receptor type 1",
        organism="Homo sapiens",
        classification="Hydrolase",
        domain=None,
        pdb_deposition_group="G_1002043",
        gene="PTPN1",
        pandda_data_deposition="10.5281/zenodo.1044103",
        literature="10.7554/eLife.36307",
    ),
    "FALZA": SystemInfo(
        published=True,
        protein_name="Tyrosine-protein phosphatase non-receptor type 1",
        organism="Homo sapiens",
        classification="Hydrolase",
        domain=None,
        pdb_deposition_group="G_1002043",
        gene="PTPN1",
        pandda_data_deposition="10.5281/zenodo.1044103",
        literature="10.7554/eLife.36307",
    ),

}

sgc_summaries = [
    "mArh",
    "INPP5DA",
    "NUDT21A",
    "FAM83BA",
    "NUDT22A",
    "NUDT4A",
    "PARP14A",
    "HAO1A",
    "DCLRE1AA",
    "NUDT5A",
    "NUDT7A",
    "FALZA",
    "SP100A",
    "BAZ2BA",
    "BRD1A",
    "JMJD2DA",
    "ATAD2",
    "KDM5B",
    "JMJD1B",
]

fragalysis = [
    "EPB41L3A",
    "HAO1A",
    "NUDT4",
    "STAG1A",
    "MURD",
    "NSP16",
    "PGN_RS02895PGA",
    "NSP15_B",
    "nsp13",
    "FAM83BA",
    "ALAS2A",
    "MUREECA",
    "ATAD2A",
    "SMTGR",
    "VIM2",
    "OXA10OTA",
    "Nprot",
    "ACVR1A",
    "PARP14A",
    "NUDT7A_CRUDE",
    "XX02KALRNA",
    "CAMK1DA",
    "ATAD",
    "DCP2B",
    "BRD1A",
    "SOS3ATA",
    "mArh",
    "TBXTA",
    "NUDT7A",
    "D68EV3CPROA",
    "macro-combi",
    "FALZA",
    "NSP14",
    "CD44MMA",
    "TNCA",
    "smTGRNEW",
    "PTP1B",
    "NUDT4A",
    "Mpro",
    "MID2A",
    "MUREECOLI",
    "NUDT21A",
    "Mac1",
    "DCLRE1AA",
    "INPP5DA",
    "NUDT5A",
    "PHIPA",
]


def try_make(path):
    try:
        os.mkdir(path)
    except Exception as e:
        return


def get_system_from_dtag(dtag):
    hyphens = [pos for pos, char in enumerate(dtag) if char == "-"]
    if len(hyphens) == 0:
        return None
    else:
        last_hypen_pos = hyphens[-1]
        system_name = dtag[:last_hypen_pos]
        return system_name

def make_unacessible_table(system_name, system_info: SystemInfo):
    string = ""
    string += "\\textbf\{System\} & {} \\\\ \n".format(system_name)
    if system_info.published:
        string += "\\textbf\{Protein\} & {} \\\\ \n".format(system_info.protein_name)
        string += "\\textbf\{Gene\} & {} \\\\ \n".format(system_info.gene)
        string += "\\textbf\{Organism\} & {} \\\\ \n".format(system_info.organism)
        string += "\\textbf\{Literature DOI\} & {} \\\\ \n".format(system_info.literature)
        string += "\\textbf\{Data Deposition\} & {} \\\\ \n".format(system_info.pandda_data_deposition)
    else:
        string += f"Published & & False \\\\ \n"
    string += f"Accessible & & False \\\\ \n"

    print(string)

def make_accessible_table(
        system_info: SystemInfo,
        system,
num_datasets,
        num_accessible_datasets,
        num_hits,
        min_res,
        median_res,
        max_res,
        unique_sgs,
        unique_sgs_counts,
        num_chains,
num_chains_counts,
        num_residues,
        num_residues_counts,
        volume,
):
    string = ""
    string += f"System & & {system} \\\\ \n"
    if system_info.published:
        string += f"Protein & & {system_info.protein_name} \\\\ \n"
        string += f"Gene & & {system_info.gene} \\\\ \n"
        string += f"Organism & & {system_info.organism} \\\\ \n"
        string += f"Literature DOI & & {system_info.literature} \\\\ \n"
        string += f"Data Deposition & & {system_info.pandda_data_deposition} \\\\ \n"
    else:
        string += f"Published & & False \\\\ \n"


    string += f"Number of Datasets & & {num_datasets} \\\\ \n"
    string += f"Number of Fragment Hits & & {num_hits} \\\\ \n"
    string += f"Resolution & Minimum & {min_res} \\\\ \n"
    string += f" & Median & {median_res} \\\\ \n"
    string += f" & Maximum & {max_res} \\\\ \n"

    string += f"Spacegroups & {unique_sgs[0]} & {unique_sgs_counts[0]} \\\\ \n"
    if len(unique_sgs) > 1:
        for sg, count in zip(unique_sgs[1:], unique_sgs_counts[1:]):
            string += f" & {sg} & {count} \\\\ \n"

    string += f"Number of Chains & {int(num_chains[0])} & {int(num_chains_counts[0])} \\\\ \n"
    if len(num_chains) > 1:
        for num_chain, count in zip(num_chains[1:], num_chains_counts[1:]):
            string += f" & {int(num_chain)} & {int(count)} \\\\ \n"

    string += f"Number of Residues & {int(num_residues[0])} & {int(num_residues_counts[0])} \\\\ \n"
    if len(num_chains) > 1:
        for num_residue, count in zip(num_residues[1:], num_residues_counts[1:]):
            string += f" & {int(num_residue)} & {int(count)} \\\\ \n"

    string += f"Median Unit Cell Volume &  & {round(volume, 1)} \\\\ \n"

    print(string)

    ...

def make_unpublished_table():
    ...


def make_system_tables():

    sqlite_filepath = "/dls/science/groups/i04-1/conor_dev/pandda_lib/diamond_2.db"
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    output_dir = pathlib.Path("/dls/science/groups/i04-1/conor_dev/pandda_lib/thesis/xchem_dataset_summary")
    output_table = output_dir / "dataset_statistics.csv"
    try_make(output_dir)

    # Get the database
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    # tmp_dir = pathlib.Path(tmp_dir).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()

    # Get the information on each system
    # initial_datasets = session.query(DatasetSQL).options(subqueryload(DatasetSQL.bound_state_model)).order_by(
    #     DatasetSQL.id).all()
    # datasets_by_system = {}
    # for dataset in initial_datasets:
    #
    #     # Add the dataset to the datasets by system
    #     system_name = get_system_from_dtag(dataset.dtag)
    #     if not system_name:
    #         continue
    #
    #     if not system_name in datasets_by_system:
    #         datasets_by_system[system_name] = {}
    #     datasets_by_system[system_name][dataset.dtag] = dataset


    table = pd.read_csv(output_table)

    system_hit_rate_records = []
    for system in table[table["Accessible"] == True]["System"].unique():

        system_table = table[table["System"] == system]
        num_hits = len(system_table[system_table["RSCC"] > 0.0]["Dtag"].unique())
        num_datasets = len(system_table["Dtag"].unique())
        num_accessible_datasets = len(system_table[system_table["Accessible"] == True]["Dtag"].unique())
        if num_datasets != 0:
            system_hit_rate_records.append(
                {
                    "System": system,
                    "Hit Rate": num_hits / num_datasets,
                    "Number of Hits": num_hits,
                    "Number of Datasets": num_datasets,
                    "Number of Accessible Datasets": num_accessible_datasets
                })

    system_hit_rate_table = pd.DataFrame(system_hit_rate_records)

    # For each system, get the relevant information and output a latex formated table
    # for system in table[table["Accessible"] == True]["System"].unique():
    for system in system_info:
        print(f"##### {system} #####")
        system_info_obj = system_info[system]
        # print(f"Published: {system_info_obj.published}")


        # if system not in system_info:
        #     print(f"Skipping: {system}")
        #     continue

        system_table = table[(table["System"] == system) & (table["Accessible"] == True)]

        # Get number of datasets
        num_datasets = len(system_table["Dtag"].unique())
        num_accessible_datasets = len(system_table[system_table["Accessible"] == True]["Dtag"].unique())

        if num_accessible_datasets == 0:
            make_unacessible_table(system, system_info_obj)
            continue

        # Get the number of hits
        num_hits = len(system_table[system_table["RSCC"] > 0.0]["Dtag"].unique())

        # # Get the structures
        # structures = {}
        # for system_dataset in system_datasets:
        #     structures[system_dataset.dtag] = gemmi.read_structure(system_dataset.model_path)
        #
        # # Get the resolutions
        # resolutions = [st.resolution for st in structures.values()]

        # Get minimum resolution
        min_res = system_table["Resolution"].min()

        # Get mean resolution
        median_res = system_table["Resolution"].median()

        # Get max resolution
        max_res = system_table["Resolution"].max()

        # Spacegroups
        sgs = system_table["Spacegroup"]
        unique_sgs, counts = np.unique(sgs, return_counts=True)

        # Num chains
        num_chains, num_chains_counts = np.unique(system_table["Number of Chains"], return_counts=True)
        num_residues, num_residues_counts = np.unique(system_table["Number of Residues"], return_counts=True)

        volume = system_table["Volume"].median()
        # Get % with bound state model

        # Get the organism

        # Get the protein id

        #
        # print(f"##### {system} #####")
        # print(num_datasets)
        # print(num_accessible_datasets)
        # print(num_hits)
        # print(min_res)
        # print(median_res)
        # print(max_res)
        # print(unique_sgs)
        # print(counts)
        # print(num_chains)
        # print(num_residues)
        # print(volume)


        if system_info_obj.published:
            make_accessible_table(
                system_info_obj,
                system,
                num_datasets,
                num_accessible_datasets,
                num_hits,
                min_res,
                median_res,
                max_res,
                unique_sgs,
                counts,
                num_chains,
                num_chains_counts,
                num_residues,
                num_residues_counts,
                volume,
            )

        ...


if __name__ == "__main__":
    fire.Fire(make_system_tables)
