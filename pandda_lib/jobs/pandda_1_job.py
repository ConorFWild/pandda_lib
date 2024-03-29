pandda_command = (
    "#!/bin/sh \n"
    "module load pymol/1.8.2.0 \n"
    "module load ccp4/7.0.067 \n"
    "pandda.analyse "
    "data_dirs='{data_dirs}/*' "
    "out_dir={out_dir} "
    "pdb_style='{pdb_style}' "
    "mtz_style='{mtz_style}' "
    "cpus={cores} "
    "max_new_datasets=5000 "

)


class PanDDAJob:
    def __init__(self,
                 name,
                 system_data_dir,
                 output_dir,
                 pdb_style="dimple.pdb",
                 mtz_style="dimple.mtz",
                 cores=24
                 ):
        self.name = name
        self.system_data_dir = system_data_dir
        self.output_dir = output_dir
        self.script = pandda_command.format(
            data_dirs=self.system_data_dir,
            out_dir=self.output_dir,
            pdb_style=pdb_style,
            mtz_style=mtz_style,
            cores=cores
        )
