from pathlib import Path

import fire
import pymongo


from pandda_lib.fs import XChemDiamondFS

def main(diamond_dir: str, output_dir: str):
    diamond_dir = Path(diamond_dir)
    print(f"Diamond dir is: {diamond_dir}")

    print("Getting XChemDiamondFS...")
    diamond_fs = XChemDiamondFS.from_path(diamond_dir)

    print(diamond_fs.pandda_dirs)

    print(diamond_fs.model_building_dirs)

    # print(",".join([str(x) for x in diamond_fs.model_building_dirs.values()]))
    # print(",".join([str(",") for x in diamond_fs.pandda_dirs.values()]))

    client = pymongo.MongoClient()

    diamond_paths = client.pandda.diamond_paths

    for system_name, model_building_dir in diamond_fs.model_building_dirs.items():
        doc = {
                "system_name": system_name.system_name,
                "model_bulding_dir": str(model_building_dir),
                "pandda_dirs": [str(x) for x in diamond_fs.pandda_dirs[system_name]],
            }
        print(
            doc
        )



if __name__ == "__main__":
    fire.Fire(main)
