import os
from pathlib import Path
import subprocess

import fire
import joblib

grade_command = "module load buster; module load buster; cd {data_dir}; grade -checkdeps; grade -f -in {in_smiles} -ocif {out_cif}"


def run_grade(compound_dir, smiles_path):
    print(f"Running job: {compound_dir} {smiles_path.name}")
    while not Path(f"{ compound_dir / smiles_path.stem.strip()}.cif").exists():
        command = grade_command.format(
            data_dir=compound_dir,
            in_smiles=smiles_path.name,
            out_cif=f"{smiles_path.stem.strip()}.cif",
        )
        print(command)
        p = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = p.communicate()
        print(str(stdout))
        print(str(stderr))


def run_grade_on_model_building(path: str):
    _path = Path(path).resolve()
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
    print(f"Processing {len(processes)} jobs...")
    joblib.Parallel(n_jobs=12, verbose=10)(x for x in processes)


if __name__ == "__main__":
    fire.Fire(run_grade_on_model_building)
