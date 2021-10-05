from __future__ import annotations
from dataclasses import dataclass
from typing import *
from pathlib import Path


@dataclass()
class PanDDA1Command:
    def __init__(self,
                 analyse_path,
                 data_dirs,
                 out_dir,
                 pdb_regex,
                 mtz_regex,
                 structure_factors_f,
                 structure_factors_phi,
                 autobuild,
                 global_processing,
                 distributed_scheduler,
                 local_cpus,
                 mem_per_core,
                 distributed_tmp,
                 rank_method,
                 comparison_strategy,
                 cluster_selection,
                 num_workers,
                 log_file,

                 ):
        self.command = (
            f'pandda.analyse '
            f'data_dirs={self.data_directory + "/*"} '
            f'out_dir={self.panddas_directory} '
            f'min_build_datasets={self.min_build_datasets} '
            f'max_new_datasets={self.max_new_datasets} '
            f'grid_spacing={self.grid_spacing} '
            f'cpus={self.nproc} '
            f'events.order_by={self.sort_event} '
            f'pdb_style={self.pdb_style} '
            f'mtz_style={self.mtz_style} '
            f'lig_style=/compound/*.cif '
            f'apply_b_factor_scaling={self.wilson_scaling} '
            f'write_average_map={self.write_mean_maps} '
            f'average_map={self.calc_map_by} '
        )
