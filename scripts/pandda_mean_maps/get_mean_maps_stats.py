import dataclasses
import pathlib
import re

import numpy as np
import pandas as pd
import fire
import gemmi


@dataclasses.dataclass
class MeanMapStats:
    delta_min: float
    delta_max: float
    quantile_9: float
    quantile_95: float
    quantile_99: float


def get_pandda_system_project(pandda_dir):
    match = re.match(
        "system_([^_]+)_project_(.+)",
        pandda_dir.name,
    )

    if not match:
        return None

    system = match.groups()[0]
    project = match.groups()[1]

    return system, project


def get_mean_map_stats(mean_map_path, reference_map_path):
    mean_map = gemmi.read_ccp4_map(str(mean_map_path))
    reference_map = gemmi.read_ccp4_map(str(reference_map_path))

    mean_map_array = np.array(mean_map)
    reference_map_array = np.array(reference_map)

    delta_array = reference_map_array - mean_map_array

    mean_map_stats = MeanMapStats(
        np.min(delta_array),
        np.max(delta_array),
        np.quantile(delta_array, 0.9),
        np.quantile(delta_array, 0.95),
        np.quantile(delta_array, 0.99),
    )

    return mean_map_stats


def get_mean_maps_from_matches(mean_map_matches):
    resolutions = np.unique([resolution for resolution, model, path in mean_map_matches])
    # print(resolutions)

    mean_maps = {}
    for resolution in resolutions:
        mean_maps[resolution] = {
            model: path
            for _resolution, model, path
            in mean_map_matches
            if _resolution == resolution
        }

    return mean_maps

def get_mean_maps(pandda_dir):
    pandda_dir_paths = [path for path in pandda_dir.glob("*")]

    mean_map_matches = [
        (float(match.groups()[0]), int(match.groups()[1]), path)
        for match, path
        in [
            (
                re.match("([^_]+)_([^_]+)_mean.ccp4", str(path.name)),
                path)
                for path in pandda_dir_paths
        ]
        if match
    ]

    mean_maps = get_mean_maps_from_matches(mean_map_matches)

    return mean_maps


def get_mean_maps_stats_table():
    table_path =  pathlib.Path("/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2/pandda_2_diamond_data"
                               "/event_map_stats.csv")
    panddas_dir =  pathlib.Path("/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2/pandda_2_diamond_data/output")

    pandda_dirs = [pandda_dir for pandda_dir in panddas_dir.glob("*")]
    print(f"Got {len(pandda_dirs)} PanDDA dirs")

    mean_map_stats = {}
    for pandda_dir in pandda_dirs:
        mean_maps = get_mean_maps(pandda_dir)

        match = get_pandda_system_project(pandda_dir)
        if not match:
            print(f"\tWas unable to match {pandda_dir.name} to a system and project. Skipping...")
            continue

        system, project = match
        print(f"\tSystem: {system}; Project: {project}")
        print(f"\t\tGot {len(mean_maps)} resolution shells")

        for shell in mean_maps:
            for model in mean_maps[shell]:
                mean_map_stats[(system, project, shell, model)] = get_mean_map_stats(
                    mean_maps[shell][model],
                    mean_maps[shell][0]
                )

                # print(f"\t\t")

    print(f"Making table to output to {str(table_path)}")
    mean_map_stats_table = pd.DataFrame(
        [
            {
                "System": map_id[0],
                "Project": map_id[1],
                "Shell": map_id[2],
                "Model": map_id[3],
                "Delta Min": mean_map_stat.delta_min,
                "Delta Max": mean_map_stat.delta_max,
                "Quantile 0.9": mean_map_stat.quantile_9,
                "Quantile 0.95": mean_map_stat.quantile_99,
                "Quantile 0.99": mean_map_stat.quantile_99,
            }
            for map_id, mean_map_stat
            in mean_map_stats.items()
        ]
    )

    mean_map_stats_table.to_csv(table_path)


if __name__ == "__main__":
    fire.Fire(get_mean_maps_stats_table)
