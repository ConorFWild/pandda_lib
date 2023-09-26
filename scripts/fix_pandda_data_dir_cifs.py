import os
import pathlib
import shutil

import fire
import subprocess
import joblib

def fix_dataset_cif(dataset_dir):
    compound_dir = dataset_dir / "compound"

    compound_dep_dir = compound_dir / "dep"
    if not compound_dep_dir.exists():
        os.mkdir(compound_dep_dir)
        cif_paths = [x for x in compound_dir.glob('*.cif')]
        if len(cif_paths) == 0:
            return
    else:
        cif_paths = [x for x in compound_dep_dir.glob('*.cif')]
        if len(cif_paths) == 0:
            return
    cif_path = cif_paths[0]
    shutil.move(cif_path, compound_dep_dir / cif_path.name)

    # script = f"cd {compound_dir}; module load buster; -in {compound_dep_dir / cif_path.name} -itype cif -ocif {cif_path.name} -opdb {f'{cif_path.stem}.pdb'} -fixupcif"
    script = f"cd {compound_dep_dir}; module load phenix; phenix.elbow {cif_path.name}"
    print(script)
    # return
    p = subprocess.Popen(script, shell=True)
    p.communicate()
    for path in compound_dep_dir.glob("elbow*.pdb"):

        shutil.move(
            path,
            compound_dir / f'{cif_path.stem}.pdb'
        )
    for path in compound_dep_dir.glob("elbow*.cif"):
        shutil.move(
            path,
            compound_dir / cif_path.name
        )

def main(path):
    path = pathlib.Path(path)
    joblib.Parallel(n_jobs=20, verbose=50)(
        joblib.delayed(fix_dataset_cif)(dataset_dir) for dataset_dir in path.glob('*')
    )


if __name__ == "__main__":
    fire.Fire(main)