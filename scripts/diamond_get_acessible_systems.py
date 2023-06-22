import pathlib

import fire
import gemmi

from pandda_lib.common import Dtag, SystemName

def test_accessible(dataset_dir):
    pdb_format_1 = dataset_dir / f"dimple.pdb"
    pdb_format_2 = dataset_dir / f"{dataset_dir.name}.dimple.pdb"

    if pdb_format_1.exists():
        try:
            st = gemmi.read_structure(str(pdb_format_1))
            return True
        except:
            return False
    elif pdb_format_2.exists():
        try:
            st = gemmi.read_structure(str(pdb_format_2))
            return True
        except:
            return False
    else:
        return False

def get_system_name_from_dtag(dtag):
    hyphens = [pos for pos, char in enumerate(dtag) if char == "-"]
    if len(hyphens) == 0:
        return None
    else:
        last_hypen_pos = hyphens[-1]
        system_name = dtag[:last_hypen_pos ]

        return system_name


def main():
    xchem_data_path = pathlib.Path('/dls/labxchem/data')

    systems = []

    for year_dir in xchem_data_path.glob('*'):
        if (year_dir.name[0] == 's') or (year_dir.name[0] == 'i'):
            continue
        for project_dir in year_dir.glob('*'):
            if (project_dir.name[0] == 's') or (project_dir.name[0] == 'i'):
                continue
            model_building_dir = project_dir / 'processing' / 'analysis' / 'model_building'

            initial_model_dir = project_dir / 'processing' / 'analysis' / 'initial_model'

            # try:

            if model_building_dir.exists():
                datasets_list = list(model_building_dir.glob('*'))

            elif initial_model_dir.exists():
                datasets_list = list(initial_model_dir.glob('*'))

            else:
                continue
            if len(datasets_list) == 0:
                continue

            if test_accessible(datasets_list[0]) or test_accessible(datasets_list[-1]):

                num_datasets = len(datasets_list)
                # dtag = Dtag.from_name(datasets_list[0].name)
                # system = SystemName.from_dtag(dtag)
                system_name_1 = get_system_name_from_dtag(datasets_list[0].name)
                system_name_2 = get_system_name_from_dtag(datasets_list[-1].name)

                if not system_name_1:
                    system_name = system_name_2
                    if not system_name_2:
                        continue
                else:
                    system_name = system_name_1

                # print(f"{system.system_name}: {num_datasets}")

                systems.append(f"{system_name}: {num_datasets}: {project_dir}")
            # except Exception as e:
            #     # print(e)
            #     continue


    for x in sorted(systems):
        print(x)

    print(len(systems))

if __name__ == "__main__":
    fire.Fire(main)
