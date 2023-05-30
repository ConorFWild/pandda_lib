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
                 protein_name,
                 organism,
                 classification,
                 domain,
                 pdb_deposition_group,
                 gene,
                 pandda_data_deposition,
                literature
                 ):
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
        protein_name="Non-structural protein 16",
        organism="Severe acute respiratory syndrome coronavirus 2",
        classification="Viral protein",
        domain="2′-O-methyltransferase",
        pdb_deposition_group=["7B69", "7B6A", "7B6C"],
        gene=None,#"orf1ab",
        pandda_data_deposition="Fragalysis",
        literature=None,
    ),
    "STAG1A": SystemInfo(
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
        protein_name="Kalirin RhoGEF kinase in complex with Rac family small GTPase 1",
        organism="Homo sapiens",
        classification="Hydrolase",
        domain=None,
        pdb_deposition_group=["5O33"],
        gene=["KALRN", "Rac1"],
        pandda_data_deposition="10.5281/zenodo.3271348",
        literature="10.1002/anie.201900585",
    ),
    "HSP90": {},
    "BAZ2BA": SystemInfo(
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
        protein_name="T-box transcription factor T",
        organism="Homo sapiens",
        classification="Transcription",
        domain=None,
        pdb_deposition_group="G_1002080",
        gene="TBXT",
        pandda_data_deposition="Fragalysis",
        literature=None,
    ),
    "KPNF": {},
    "CYCK": {},
    "P1Pro": {},
    "PKM1": SystemInfo(
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
def make_system_tables():
    for key in system_info:
        print(key)
    exit()

    sqlite_filepath = "/dls/science/groups/i04-1/conor_dev/pandda_lib/diamond_2.db"
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    output_dir = pathlib.Path("/dls/science/groups/i04-1/conor_dev/pandda_lib/thesis")
    try_make(output_dir)

    # Get the database
    sqlite_filepath = pathlib.Path(sqlite_filepath).resolve()
    # tmp_dir = pathlib.Path(tmp_dir).resolve()
    engine = create_engine(f"sqlite:///{str(sqlite_filepath)}")
    session = sessionmaker(bind=engine)()

    # Get the information on each system
    initial_datasets = session.query(DatasetSQL).options(subqueryload(DatasetSQL.bound_state_model)).order_by(
        DatasetSQL.id).all()
    datasets_by_system = {}
    for dataset in initial_datasets:

        # Add the dataset to the datasets by system
        system_name = get_system_from_dtag(dataset.dtag)
        if not system_name:
            continue

        if not system_name in datasets_by_system:
            datasets_by_system[system_name] = {}
        datasets_by_system[system_name][dataset.dtag] = dataset

    # For each system, get the relevant information and output a latex formated table
    for system, system_datasets in datasets_by_system.items():
        # Get number of datasets

        # Get minimum resolution

        # Get mean resolution

        # Get max resolution

        # Get % with bound state model

        # Get the organism

        # Get the protein id

        #

        ...


if __name__ == "__main__":
    fire.Fire(make_system_tables)