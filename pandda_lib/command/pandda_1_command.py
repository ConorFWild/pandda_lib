from __future__ import annotations
from dataclasses import dataclass
from typing import *
from pathlib import Path


@dataclass()
class PanDDA1Command:
    def __init__(self,
                 data_dirs,
                 out_dir,
                 pdb_regex,
                 mtz_regex,
                 local_cpus,
                 min_build_datasets,
                 max_new_datasets,
                 log_file,
                 ):
        self.command = (
            f'pandda.analyse '
            f'data_dirs={data_dirs + "/*"} '
            f'out_dir={out_dir} '
            f'min_build_datasets={min_build_datasets} '
            f'max_new_datasets={max_new_datasets} '
            # f'grid_spacing={self.grid_spacing} '
            f'cpus={local_cpus} '
            # f'events.order_by={self.sort_event} '
            f'pdb_style={pdb_regex} '
            f'mtz_style={mtz_regex} '
            f'lig_style=/compound/*.cif '
            f'|& tee {log_file}'
            # f'apply_b_factor_scaling={self.wilson_scaling} '
            # f'write_average_map={self.write_mean_maps} '
            # f'average_map={self.calc_map_by} '
        )
