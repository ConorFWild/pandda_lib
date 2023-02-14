import os
from pathlib import Path
import subprocess
import shutil

import fire
import joblib

grade_command = "module load buster; module load buster; cd {data_dir}; grade -checkdeps; grade -f -in {in_smiles} -ocif {out_cif}"


def run_grade_cif(compound_dir, cif_path, new_cif_path):
    command = grade_command.format(
        data_dir=compound_dir,
        in_smiles=cif_path,
        out_cif=new_cif_path,
    )
    print(command)
    while not new_cif_path.exists():
        p = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = p.communicate()
        print(str(stdout))
        print(str(stderr))
    os.remove(cif_path)


def run_grade(compound_dir, smiles_path):
    print(f"Running job: {compound_dir} {smiles_path.name}")
    output_cif_path = compound_dir / f"{smiles_path.stem.strip()}.cif"

    canon_name = smiles_path.stem.strip()
    new_smiles_path = smiles_path.parent / f"{canon_name}.smiles"

    if canon_name != smiles_path.stem:
        shutil.move(smiles_path, new_smiles_path)
        print(f"Moved ligand {smiles_path.name} to {new_smiles_path.name}")

    while not output_cif_path.exists():
        command = grade_command.format(
            data_dir=compound_dir,
            in_smiles=new_smiles_path.name,
            out_cif=f"{canon_name}.cif",
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


class Grade:
    def run_grade_on_model_building(self, path: str, remove=False, ):
        _path = Path(path).resolve()
        processes = []
        for model_dir in _path.glob("*"):
            compound_dir = model_dir / "compound"
            skip = False
            for cif in compound_dir.glob("*.cif"):
                if remove:
                    os.remove(cif)
                    print(f"Removed cif: {cif}")
                else:
                    skip = True
            if skip:
                print(f"Already has cif: {compound_dir}: skipping!")
                continue
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

    def run_grade_on_model_building_cifs(self, path: str):
        _path = Path(path).resolve()
        processes = []
        for model_dir in _path.glob("*"):
            compound_dir = model_dir / "compound"
            skip = False
            for cif in compound_dir.glob("*.cif"):
                new_cif_path = compound_dir / "grade.cif"
                processes.append(
                    joblib.delayed(run_grade_cif)(compound_dir, cif, new_cif_path)
                )
        print(f"Processing {len(processes)} jobs...")
        joblib.Parallel(n_jobs=12, verbose=10)(x for x in processes)

    def delete_dirs_without_cifs(self, path: str):
        _path = Path(path).resolve()
        for model_dir in _path.glob("*"):
            compound_dir = model_dir / "compound"
            skip = False
            num_cifs = len(compound_dir.glob("*.cif"))
            if num_cifs == 0:
                shutil.rmtree(model_dir)

if __name__ == "__main__":
    fire.Fire(Grade)
