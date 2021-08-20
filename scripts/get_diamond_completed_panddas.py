from pathlib import Path

import fire

from pandda_lib.fs import XChemDiamondFS

def main(diamond_dir: str, output_dir: str):
    diamond_dir = Path(diamond_dir)
    print(f"Diamond dir is: {diamond_dir}")

    print("Getting XChemDiamondFS...")
    diamond_fs = XChemDiamondFS.from_path(diamond_dir)

    print(diamond_fs.pandda_dirs)

    print(diamond_fs.model_building_dirs)


if __name__ == "__main__":
    fire.Fire(main)
