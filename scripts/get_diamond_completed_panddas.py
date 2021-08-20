from pathlib import Path

import fire

from pandda_lib.pandda_lib.fs import XChemDiamondFS

def main(diamond_dir: str, output_dir: str):
    diamond_dir = Path(diamond_dir)

    diamond_fs = XChemDiamondFS.from_path(diamond_dir)



