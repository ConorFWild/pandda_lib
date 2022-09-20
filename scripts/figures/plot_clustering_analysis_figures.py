import pathlib
import os
import json
from matplotlib import pyplot as plt
import pandas as pd
import gc

import seaborn as sns

import fire

TARGET_KEY = "target"
WORKING_DIR_KEY = "working_dir"
DATA_DIR_KEY = "data_dir"
PDB_REGEX_KEY = "pdb_regex"
MTZ_REGEX_KEY = "mtz_regex"
OUT_DIR_FORMAT = "output_{target}"
JOB_SCRIPT_FORMAT = "{target}.sh"
CHMOD_COMMAND_FORMAT = "chmod 777 {script_path}"
QSUB_COMMAND = "qsub -P labxchem -pe smp 3 -l m_mem_free=30G -q medium.q -o {log_file} -e {err_file} {script_file}"
LOG_FILE_FORMAT = "{target}.log"
ERR_FILE_FORMAT = "{target}.err"
SAMPLE_JSON_KEY = "sample_json"
SAMPLE_JSON_DIR = "sample_dir"
OUTPUT_DIR_KEY = "output_dir"

sns.set(rc={'figure.figsize': (2 * 11.7, 2 * 8.27)})
sns.set(font_scale=3)
# sns.color_palette("hls", 8)
sns.set_palette("hls")
sns.set_palette("tab10")


def get_qq_dataframe(df1, df2, key):
    x = df1[key].sort_values()
    y = df2[key].sort_values()

    min_res = df1["Model"].min()
    max_res = df2["Model"].max()
    key_1 = f"{key} Of Model {min_res}"
    key_2 = f"{key} Of Model {max_res}"
    records = []
    assert len(x) == len(y)
    for x, y in zip(x, y):
        record = {
            key_1: x,
            key_2: y,
        }
        records.append(record)

    return pd.DataFrame(records), key_1, key_2


def get_graph(dataframe_model_0, dataframe_model_changed, sample_point_dtag):
    # Get ground state samples dataframe
    dataframe_model_0_characterization = dataframe_model_0[dataframe_model_0["Dtag Class"] != "Test"]
    dataframe_model_0_characterization_test = dataframe_model_0[dataframe_model_0["Dtag"] == sample_point_dtag]

    # Get the test points samples dataframe
    dataframe_model_changed_characterization = dataframe_model_changed[dataframe_model_changed["Dtag Class"] != "Test"]
    dataframe_model_changed_characterization_test = dataframe_model_changed[
        dataframe_model_changed["Dtag"] == sample_point_dtag]

    # Get axis
    fig, (ax1, ax2,) = plt.subplots(ncols=2, figsize=(30, 15))  # sharey=True)

    # Get the ED QQ plot
    qq_dataframe, key_1, key_2 = get_qq_dataframe(dataframe_model_0_characterization,
                                                  dataframe_model_changed_characterization, "Electron Density Value")
    ed_qq_plot = sns.regplot(ax=ax1, data=qq_dataframe, x=key_1, y=key_2, )
    # plt.axis('equal')
    test_zmap_value = dataframe_model_0_characterization_test["ZMap Value"].iloc[0]
    ax1.scatter(dataframe_model_0_characterization_test["Electron Density Value"],
                dataframe_model_changed_characterization_test["Electron Density Value"], c="purple")

    dataframe_model_0_characterization_test_for_print = dataframe_model_0_characterization_test[
        ["Dtag",
         "Electron Density Value",
         "ZMap Value",
         "Map Uncertainty",
         "Sample Uncertainty",
         "Dtag Class"]]
    table = ax1.table(
        cellText=dataframe_model_0_characterization_test_for_print.values,
        colLabels=dataframe_model_0_characterization_test_for_print.columns,

        loc="bottom",
        bbox=[-0.2, -0.5, 1.2, 0.3],
    )
    table.auto_set_font_size(False)
    table.set_fontsize(12)
    ax1.axis('equal')

    # Get the Z qq plot
    qq_dataframe, key_1, key_2 = get_qq_dataframe(dataframe_model_0_characterization,
                                                  dataframe_model_changed_characterization, "ZMap Value")
    z_qq_plot = sns.regplot(ax=ax2, data=qq_dataframe, x=key_1, y=key_2, )
    # plt.axis('equal')
    ax2.scatter(dataframe_model_0_characterization_test["ZMap Value"],
                dataframe_model_changed_characterization_test["ZMap Value"], c="purple")
    dataframe_model_changed_characterization_test_for_print = dataframe_model_changed_characterization_test[
        ["Dtag",
         "Electron Density Value",
         "ZMap Value",
         "Map Uncertainty",
         "Sample Uncertainty",
         "Dtag Class"]]
    table = ax2.table(
        cellText=dataframe_model_changed_characterization_test_for_print.values,
        colLabels=dataframe_model_changed_characterization_test_for_print.columns,
        loc="bottom",
        bbox=[-0.2, -0.5, 1.2, 0.3],
    )
    table.auto_set_font_size(False)
    table.set_fontsize(12)
    ax2.axis('equal')

    return fig


def plot_clustering_analysis_figures():
    # Get the poaths
    output_dir = pathlib.Path(
        "/dls/labxchem/data/2017/lb18145-17/processing/analysis/pandda_2/pandda_2_reproduce_cluster4x/figures")

    targets_json_path = pathlib.Path(
        "/dls/science/groups/i04-1/conor_dev/pandda_lib/scripts/pandda_analysis_clustering/targets.json").resolve()

    #
    if not output_dir.exists():
        os.mkdir(output_dir)

    # Get the targets json
    with open(targets_json_path, "r") as f:
        targets_dict = json.load(f)

    # Get the path to the PanDDAs
    pandda_dirs = pathlib.Path(targets_dict[OUTPUT_DIR_KEY]).resolve()
    sample_jsons_dir = pathlib.Path(targets_dict[SAMPLE_JSON_DIR]).resolve()

    #
    for target, target_info in targets_dict[TARGET_KEY].items():
        print(f"Target: {target}")

        #
        system_output_dir = output_dir / target
        if not system_output_dir.exists():
            os.mkdir(system_output_dir)

        # Get the PanDDA data
        pandda_dir = pandda_dirs / OUT_DIR_FORMAT.format(target=target)
        pandda_samples_csv_path = pandda_dir / "samples.csv"
        sample_points_path = sample_jsons_dir / target_info[SAMPLE_JSON_KEY]

        #
        with open(sample_points_path, "r") as f:
            sample_points = json.load(f)

        #
        dataframe = pd.read_csv(pandda_samples_csv_path).round(2)

        # Plot the graphs for each point
        for sample_point in sample_points["Sample Points"]:
            print(f"\tPlotting for sample: {sample_point}")
            sample_point_dtag = sample_points["Sample Points"][sample_point][0]

            dataframe_dtag = dataframe[dataframe["Dtag"] == sample_point_dtag]
            dataframe_min_res = dataframe[dataframe["Resolution"] == dataframe_dtag["Resolution"].min()]

            sample_point_dataframe = dataframe_min_res[dataframe_min_res["Sample Point"] == sample_point]
            dataframe_model_0 = sample_point_dataframe[sample_point_dataframe["Model"] == 0]
            for model in sample_point_dataframe["Model"].unique():
                output_path = system_output_dir / f"{sample_point}_{model}_qq.png"
                dataframe_model_changed = sample_point_dataframe[sample_point_dataframe["Model"] == model]
                fig = get_graph(dataframe_model_0, dataframe_model_changed, sample_point_dtag)
                fig.savefig(output_path, bbox_inches='tight')
                plt.cla()
                plt.clf()
                plt.close("all")
            gc.collect()
            plt.close()

if __name__ == "__main__":
    fire.Fire(plot_clustering_analysis_figures)