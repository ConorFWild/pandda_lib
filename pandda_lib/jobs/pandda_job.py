pandda_command = (
    "#!/bin/sh \n"
    "module load ccp4 \n"
    "module load phenix \n"
    "module load buster \n"
    "__conda_setup=\"$('/dls/science/groups/i04-1/conor_dev/conda/anaconda/bin/conda' 'shell.bash' 'hook' 2> /dev/null)\" \n"
    "if [ $? -eq 0 ]; then \n"
    "    eval \"$__conda_setup\" \n"
    "else \n"
    "    if [ -f \"/dls/science/groups/i04-1/conor_dev/conda/anaconda/etc/profile.d/conda.sh\" ]; then \n"
    "        . \"/dls/science/groups/i04-1/conor_dev/conda/anaconda/etc/profile.d/conda.sh\" \n"
    "    else \n"
    "        export PATH=\"/dls/science/groups/i04-1/conor_dev/conda/anaconda/bin:$PATH\" \n"
    "    fi \n"
    "fi \n"
    "unset __conda_setup \n"
    "export PYTHONPATH=\"\" \n"
    "conda activate pandda2_ray \n"
    "python -u /dls/science/groups/i04-1/conor_dev/pandda_2_gemmi/pandda_gemmi/analyse.py --data_dirs={data_dirs} --out_dir={out_dir} --pdb_regex=\"dimple.pdb\" --mtz_regex=\"dimple.mtz\" --ligand_smiles_regex=\"[0-9a-zA-Z-]+[.]smiles\" --ligand_cif_regex=\"[0-9a-zA-Z-]+[.]cif\" --autobuild=True --global_processing=\"serial\" --local_processing=\"ray\" --local_cpus=6 --rank_method=autobuild --comparison_strategy=\"hybrid\" --min_characterisation_datasets=25 --debug=2 --memory_availability=\"high\""

)


class PanDDAJob:
    def __init__(self,
                 name,
                 system_data_dir,
                 output_dir,
                 ):
        self.name = name
        self.system_data_dir = system_data_dir
        self.output_dir = output_dir
        self.script = pandda_command.format(
            data_dirs=self.system_data_dir,
            out_dir=self.output_dir
        )
