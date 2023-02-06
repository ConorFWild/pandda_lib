import os
from pathlib import Path
import subprocess

import fire
import joblib

grade_command = "module load buster; cd {data_dir}; grade -in {in_smiles} -ocif {out_cif}"


def run_grade(compound_dir, smiles_path):
    p = subprocess.Popen(
        grade_command.format(
            data_dir=compound_dir,
            in_smiles=smiles_path,
            out_cif=f"{smiles_path.root}.cif",
        ),
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    p.communicate()

def run_grade_on_model_building(path: str):

    _path = Path(path)
    processes = []
    for model_dir in _path.glob("*"):
        compound_dir = model_dir / "compound"

        for cif in compound_dir.glob("*.cif"):
            os.remove(cif)
            print(f"Removed cif: {cif}")
        try:
            smiles_path = next(compound_dir.glob("*.smiles"))
        except:
            print(f"No smiles in dir: {compound_dir}")
            continue

        processes.append(
            joblib.delayed(run_grade)(compound_dir, smiles_path)
        )

    joblib.Parallel(n_jobs=12)(x for x in processes)

if __name__ == "__main__":
    fire.Fire(run_grade_on_model_building)