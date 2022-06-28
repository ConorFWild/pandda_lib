pandda_command = """python -u /xtal_software/pandda_2_gemmi/pandda_gemmi/analyse.py --data_dirs={data_dirs} --out_dir={out_dir} --pdb_regex=\"dimple.pdb\" --mtz_regex=\"dimple.mtz\" --ligand_smiles_regex=\"[0-9a-zA-Z-]+[.]smiles\" --ligand_cif_regex=\"[0-9a-zA-Z-]+[.]cif\" --debug=5 --only_datasets=\"{only_datasets}\" --local_processing=\"multiprocessing_spawn\" --comparison_strategy=\"hybrid\"
"""


class PanDDAJob:
    def __init__(self,
                 name,
                 system_data_dir,
                 output_dir,
                 ):
        self.name = name
        self.system_data_dir = system_data_dir
        self.output_dir = output_dir
