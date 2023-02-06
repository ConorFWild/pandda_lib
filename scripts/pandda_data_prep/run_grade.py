import os
from pathlib import Path
import subprocess

grade_command = "module load buster; cd {data_dir}; grade -in {in_smiles} -ocif {out_cif}"


def run_grade_on_model_building(path: Path):
    processes = []
    for model_dir in path.glob("*"):
        compound_dir = model_dir / "compound"

        for cif in compound_dir.glob("*.cif"):
            os.remove(cif)
        try:
            smiles_path = next(compound_dir.glob("*.smiles"))
        except:
            print(f"No smiles in dir: {compound_dir}")
            continue

        processes.append(
            subprocess.Popen(
                grade_command.format(
                    data_dir=compound_dir,
                    in_smiles=smiles_path,
                    out_cif=f"{smiles_path.root}.cif",
                ),
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        )

    [p.communicate() for p in processes]
