from __future__ import annotations
from dataclasses import dataclass
from typing import *
from pathlib import Path


class PanDDA2Command:
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
        self.command = f"python {analyse_path} {data_dirs} {out_dir} --pdb_regex='{pdb_regex}' --mtz_regex='" \
                       f"{mtz_regex}' --structure_factors='(\"{structure_factors_f}\",\"{structure_factors_phi}\")' " \
                       f"--autobuild={autobuild} --global_processing='{global_processing}' --local_cpus={local_cpus} " \
                       f"--distributed_mem_per_core={mem_per_core} --distributed_scheduler={distributed_scheduler} " \
                       f"--distributed_tmp={distributed_tmp} --rank_method=" \
                       f"{rank_method} --comparison_strategy={comparison_strategy} " \
                       f"--cluster_selection={cluster_selection} --memory_availability=low " \
                       f"--distributed_num_workers={num_workers} " \
                       f"|& tee {log_file}"

