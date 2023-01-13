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
    "python -u /dls/science/groups/i04-1/conor_dev/pandda_2_gemmi/pandda_gemmi/analyse.py "
    "--data_dirs={data_dirs} "
    "--out_dir={out_dir} "
    "--pdb_regex=\"{pdb_regex}\" "
    "--mtz_regex=\"{mtz_regex}\" "
    "--ligand_smiles_regex=\"[0-9a-zA-Z-]+[.]smiles\" "
    "--ligand_cif_regex=\"[0-9a-zA-Z-]+[.]cif\" "
    "--ligand_pdb_regex=\"[0-9a-zA-Z-]+[.]pdb\" "
    "--autobuild={autobuild} "
    "--global_processing=\"{global_processing}\" "
    "--local_processing=\"{local_processing}\" "
    "--local_cpus={cores} "
    "--rank_method={rank_method} "
    "--comparison_strategy=\"{comparison_strategy}\" "
    "--min_characterisation_datasets=25 "
    "--debug={debug} "
    "--event_score=\"{event_score}\" "
    "--memory_availability=\"{memory_availability}\" "
    "--distributed_num_workers=\"{distributed_num_workers}\" "
    "--rescore_event_method=\"{rescore_event_method}\" "
    "--autobuild_strategy=\"{autobuild_strategy}\" "
    "--distributed_mem_per_core=\"{distributed_mem_per_core}\""

)


class PanDDAJob:
    def __init__(self,
                 name,
                 system_data_dir,
                 output_dir,
                 cores=6,
                 pdb_regex="dimple.pdb",
                 mtz_regex="dimple.mtz",
                 comparison_strategy="hybrid",
                 event_score="inbuilt",
                 autobuild="False",
                 rank_method="autobuild",
                 memory_availability="low",
                 local_processing="serial",
                 global_processing="serial",
                 distributed_num_workers=12,
                 debug="0",
                 autobuild_strategy="rhofit",
                 rescore_event_method="autobuild_rscc",
                 distributed_mem_per_core=15
                 ):
        self.name = name
        self.system_data_dir = system_data_dir
        self.output_dir = output_dir
        self.script = pandda_command.format(
            data_dirs=self.system_data_dir,
            out_dir=self.output_dir,
            cores=cores,
            pdb_regex=pdb_regex,
            mtz_regex=mtz_regex,
            comparison_strategy=comparison_strategy,
            event_score=event_score,
            autobuild=autobuild,
            rank_method=rank_method,
            debug=debug,
            memory_availability=memory_availability,
            local_processing=local_processing,
            global_processing=global_processing,
            distributed_num_workers=distributed_num_workers,
            autobuild_strategy=autobuild_strategy,
            rescore_event_method=rescore_event_method,
            distributed_mem_per_core=distributed_mem_per_core

        )
