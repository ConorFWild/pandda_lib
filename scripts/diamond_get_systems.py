import pathlib

import fire

from pandda_lib.common import Dtag, SystemName

def main():
    xchem_data_path = pathlib.Path('/dls/labxchem/data')

    systems = []

    for year_dir in xchem_data_path.glob('*'):
        for project_dir in year_dir.glob('*'):
            model_building_dir = project_dir / 'processing' / 'analysis' / 'model_building'

            initial_model_dir = project_dir / 'processing' / 'analysis' / 'initial_model'

            try:

                if model_building_dir.exists():
                    datasets_list = list(model_building_dir.glob('*'))

                else:
                    datasets_list = list(initial_model_dir.glob('*'))

                num_datasets = len(datasets_list)
                dtag = Dtag.from_name(datasets_list[0].name)
                system = SystemName.from_dtag(dtag)

                # print(f"{system.system_name}: {num_datasets}")

                systems.append(f"{system.system_name}: {num_datasets}: {project_dir}")
            except Exception as e:
                # print(e)
                continue


    for x in sorted(systems):
        print(x)

if __name__ == "__main__":
    fire.Fire(main)
