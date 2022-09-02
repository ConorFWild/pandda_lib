import pathlib
import json
import shutil
import subprocess

import fire

TARGET_KEY = "target"
WORKING_DIR_KEY = "working_dir"
DATA_DIR_KEY = "data_dir"
PDB_REGEX_KEY = "pdb_regex"
MTZ_REGEX_KEY = "mtz_regex"
OUT_DIR_FORMAT = "output_{target}"
JOB_SCRIPT_FORMAT = "{target}.sh"
CHMOD_COMMAND_FORMAT = "chmod 777 {script_path}"
QSUB_COMMAND = "qsub -pe smp 3 -l m_mem_free=30G -q medium.q -o {log_file} -e {err_file} {script_file}"
LOG_FILE_FORMAT = "{target}.log"
ERR_FILE_FORMAT = "{target}.err"
SAMPLE_JSON_KEY = "sample_json"
SAMPLE_JSON_DIR = "/dls/science/groups/i04-1/conor_dev/pandda_lib/scripts/pandda_analysis_clustering"

PANDDA_JOB_TEMPLATE = (
    "#/bin/sh  \n "
    "module load ccp4 \n"
    "module load phenix \n"
    'module load buster \n'
    "__conda_setup=\"$('/dls/science/groups/i04-1/conor_dev/conda/anaconda/bin/conda' 'shell.bash' 'hook' 2> "
    "/dev/null)\" \n"
    "if [ $? -eq 0 ]; then \n"
    "    eval \"$__conda_setup\" \n"
    "else \n"
    "    if [ -f \"/dls/science/groups/i04-1/conor_dev/conda/anaconda/etc/profile.d/conda.sh\" ]; then \n"
    "        . \"/dls/science/groups/i04-1/conor_dev/conda/anaconda/etc/profile.d/conda.sh\" \n"
    "    else \n"
    "        export PATH=\"/dls/science/groups/i04-1/conor_dev/conda/anaconda/bin:$PATH\" \n"
    "    fi "
    "fi \n"
    "unset __conda_setup \n"
    "export PYTHONPATH=\"\" \n"
    "conda activate pandda_analysis \n"
    "python -u "
    "/dls/science/groups/i04-1/conor_dev/pandda_lib/scripts/pandda_analysis_clustering/pandda_analysis_clustering.py "
    "--data_dirs={data_dirs} "
    "--out_dir={out_dir} "
    "--pdb_regex=\"{pdb_regex}\" "
    "--mtz_regex=\"{mtz_regex}\" "
    "--ligand_smiles_regex=\"ligand.smiles\" "
    "--ligand_cif_regex=\"ligand.cif\" "
    "--ligand_pdb_regex=\"ligand.pdb\" "
    "--local_processing=\"multiprocessing_spawn\" "
    "--local_cpus=3 --debug=2 --memory_availability=\"low\" "
    "--sample_json={sample_json}"
    " \n"

)


def run_pandda_mean_maps(targets_json_path: str):
    targets_json_path = pathlib.Path(targets_json_path).resolve()

    # Get the targets json
    with open(targets_json_path, "r") as f:
        targets_dict = json.load(f)

    # Get the working dir
    working_dir = pathlib.Path(targets_dict[WORKING_DIR_KEY]).resolve()

    # construct the PanDDAs
    pandda_jobs_dict = {}
    for target, target_info in targets_dict[TARGET_KEY].items():
        print(f"\tTarget is: {target}")
        data_dir = working_dir / target_info[DATA_DIR_KEY]
        out_dir = working_dir / OUT_DIR_FORMAT.format(target=target)

        pandda_job_script = PANDDA_JOB_TEMPLATE.format(
            data_dirs=data_dir,
            out_dir=out_dir,
            pdb_regex=target_info[PDB_REGEX_KEY],
            mtz_regex=target_info[MTZ_REGEX_KEY],
            sample_json=targets_dict[SAMPLE_JSON_DIR] / target_info[SAMPLE_JSON_KEY]
        )
        pandda_jobs_dict[target] = pandda_job_script

        # Write and chmod the script
        job_script_path = working_dir / JOB_SCRIPT_FORMAT.format(target=target)
        print(f"\t\tJob script file is: {job_script_path}")

        with open(job_script_path, "w") as f:
            f.write(pandda_job_script)

        chmod_command = CHMOD_COMMAND_FORMAT.format(script_path=job_script_path)
        subprocess.Popen(chmod_command, shell=True).communicate()

        # Cleanup possible old runs
        try:
            shutil.rmtree(out_dir)
        except Exception as e:
            print(e)

        # QSUB
        log_file = LOG_FILE_FORMAT.format(target=target)
        err_file = ERR_FILE_FORMAT.format(target=target)
        qsub_command = QSUB_COMMAND.format(
            log_file=log_file,
            err_file=err_file,
            script_file=job_script_path,
        )
        print(f"\t\tQsub command is: {qsub_command}")

        subprocess.Popen(qsub_command, shell=True).communicate()


if __name__ == "__main__":
    fire.Fire(run_pandda_mean_maps)
