from __future__ import annotations

# Base python
import dataclasses
import time
import pprint
from functools import partial
import os
import json
from typing import Set
import pickle

from pandda_gemmi.processing.process_local import ProcessLocalSerial

printer = pprint.PrettyPrinter()

# Scientific python libraries
# import ray
import gemmi
import numpy as np

## Custom Imports
from pandda_gemmi.logs import (
    summarise_array,
)

from pandda_gemmi.analyse_interface import *
from pandda_gemmi import constants
from pandda_gemmi.pandda_functions import (
    process_local_serial,
    truncate,
    save_native_frame_zmap,
    save_reference_frame_zmap,
)
from pandda_gemmi.python_types import *
from pandda_gemmi.common import Dtag, EventID, Partial
from pandda_gemmi.fs import PanDDAFSModel, MeanMapFile, StdMapFile
from pandda_gemmi.dataset import (StructureFactors, Dataset, Datasets,
                                  Resolution, )
from pandda_gemmi.shells import Shell, ShellMultipleModels
from pandda_gemmi.edalignment import Partitioning, Xmap, XmapArray, Grid, from_unaligned_dataset_c, GetMapStatistics
from pandda_gemmi.model import Zmap, Model, Zmaps
from pandda_gemmi.event import (
    Event, Clusterings, Clustering, Events, get_event_mask_indicies,
    save_event_map,
)
from pandda_gemmi.density_clustering import (
    GetEDClustering, FilterEDClusteringsSize,
    FilterEDClusteringsPeak,
    MergeEDClusterings,
)


@dataclasses.dataclass()
class DatasetResult(DatasetResultInterface):
    dtag: DtagInterface
    events: MutableMapping[EventIDInterface, EventInterface]
    event_scores: EventScoringResultsInterface
    log: Dict


@dataclasses.dataclass()
class ShellResult(ShellResultInterface):
    shell: ShellInterface
    dataset_results: DatasetResultsInterface
    log: Dict




def update_log(shell_log, shell_log_path):
    if shell_log_path.exists():
        os.remove(shell_log_path)

    with open(shell_log_path, "w") as f:
        json.dump(shell_log, f, indent=4)



class ModelSelection(ModelSelectionInterface):
    def __init__(self, selected_model_id: ModelIDInterface, log: Dict) -> None:
        self.selected_model_id = selected_model_id
        self.log = log


def EXPERIMENTAL_select_model(
        model_results: ModelResultsInterface,
        inner_mask: CrystallographicGridInterface,
        processed_dataset: ProcessedDatasetInterface,
        debug: Debug = Debug.DEFAULT,
) -> ModelSelectionInterface:
    log = {}

    model_event_scores: Dict[ModelIDInterface, Dict[EventIDInterface, EventScoringResultInterface]] = {
        model_id: model.event_scores for
        model_id,
        model in model_results.items()
    }

    if debug >= Debug.PRINT_NUMERICS:
        print("Event scores for each model are:")
        print(model_event_scores)



        for model_id, event_scores in model_event_scores.items():
            print(f"Best score for model: {model_id}")
            print(
                [
                    event_scores[score_id].get_selected_structure_score()
                    for score_id
                    in event_scores
                ]
            )
            for event_id, event_score_result in event_scores.items():
                print(f"event log: {event_id.event_idx.event_idx} {event_id.dtag.dtag}")
                print(event_score_result.log())


    # Score the top clusters#
    model_scores = {}
    for model_id, event_scores in model_event_scores.items():
        selected_event_scores = [
            event_scores[event_id].get_selected_structure_score()
            for event_id
            in event_scores
        ]
        if debug >= Debug.PRINT_NUMERICS:
            print(f"\tModel {model_id} all scores: {selected_event_scores}")

        filtered_model_scores = [
                                    selected_event_score
                                    for selected_event_score
                                    in selected_event_scores
                                    if selected_event_score
                                ] + [-0.001, ]
        if debug >= Debug.PRINT_NUMERICS:
            print(f"\tModel {model_id}: filtered scores: {filtered_model_scores}")

        maximum_event_score = max(
            filtered_model_scores
        )
        model_scores[model_id] = maximum_event_score


    if debug >= Debug.PRINT_SUMMARIES:
        print("Maximum score of any event for each model are are:")
        print(model_scores)

    log['model_scores'] = {
        model_id: float(score)
        for model_id, score
        in model_scores.items()
    }

    if len(model_scores) == 0:
        return ModelSelection(0, log)

    else:
        selected_model_number = max(
            model_scores,
            key=lambda _score: model_scores[_score],
        )  # [0]

    return ModelSelection(selected_model_number, log)

class SelectModel:

    def __call__(self, model_results: ModelResultsInterface,):
        return EXPERIMENTAL_select_model(
            model_results,
        )


def get_models(
        test_dtags: List[DtagInterface],
        comparison_sets: Dict[int, List[DtagInterface]],
        shell_xmaps: XmapsInterface,
        grid: GridInterface,
        process_local: ProcessorInterface,
):
    masked_xmap_array = XmapArray.from_xmaps(
        shell_xmaps,
        grid,
    )

    models = {}
    for comparison_set_id, comparison_set_dtags in comparison_sets.items():
        # comparison_set_dtags =

        # Get the relevant dtags' xmaps
        masked_train_characterisation_xmap_array: XmapArray = masked_xmap_array.from_dtags(
            comparison_set_dtags)
        masked_train_all_xmap_array: XmapArray = masked_xmap_array.from_dtags(
            comparison_set_dtags + [test_dtag for test_dtag in test_dtags])

        mean_array: np.ndarray = Model.mean_from_xmap_array(masked_train_characterisation_xmap_array,
                                                            )  # Size of grid.partitioning.total_mask > 0
        # dataset_log[constants.LOG_DATASET_MEAN] = summarise_array(mean_array)
        # update_log(dataset_log, dataset_log_path)

        sigma_is: Dict[Dtag, float] = Model.sigma_is_from_xmap_array(masked_train_all_xmap_array,
                                                                     mean_array,
                                                                     1.5,
                                                                     )  # size of n
        # dataset_log[constants.LOG_DATASET_SIGMA_I] = {_dtag.dtag: float(sigma_i) for _dtag, sigma_i in sigma_is.items()}
        # update_log(dataset_log, dataset_log_path)

        sigma_s_m: np.ndarray = Model.sigma_sms_from_xmaps(masked_train_characterisation_xmap_array,
                                                           mean_array,
                                                           sigma_is,
                                                           process_local,
                                                           )  # size of total_mask > 0
        # dataset_log[constants.LOG_DATASET_SIGMA_S] = summarise_array(sigma_s_m)
        # update_log(dataset_log, dataset_log_path)

        model: Model = Model.from_mean_is_sms(
            mean_array,
            sigma_is,
            sigma_s_m,
            grid,
        )
        models[comparison_set_id] = model

    return models





def dump_and_load(ob, name):
    print(f"Testing: {name}")

    # time_dump_start = time.time()
    # dumps = pickle.dumps(ob)
    # time_dump_finish = time.time()
    # print(f"\tDump time is: {time_dump_finish - time_dump_start}")
    #
    # time_load_start = time.time()
    # loaded = pickle.loads(dumps)
    # time_load_finish = time.time()
    # print(f"\tLoad time is: {time_load_finish - time_load_start}")

    time_dump_start = time.time()
    with open(f"{name}.pickle", 'wb') as f:
        pickle.dump(ob, f)
    time_dump_finish = time.time()
    print(f"\tDump to disk time is: {time_dump_finish - time_dump_start}")

    time_load_start = time.time()
    with open(f"{name}.pickle", 'rb') as f:
        loaded = pickle.load(f)
    time_load_finish = time.time()
    print(f"\tLoad from disk time is: {time_load_finish - time_load_start}")


def process_dataset_multiple_models_pandda_analysis(
        test_dtag: DtagInterface,
        models: ModelsInterface,
        shell: ShellInterface,
        dataset_truncated_datasets: DatasetsInterface,
        alignments: AlignmentsInterface,
        dataset_xmaps: XmapsInterface,
        pandda_fs_model: PanDDAFSModelInterface,
        reference: ReferenceInterface,
        grid: GridInterface,
        contour_level: float,
        cluster_cutoff_distance_multiplier: float,
        min_blob_volume: float,
        min_blob_z_peak: float,
        structure_factors: StructureFactorsInterface,
        outer_mask: float,
        inner_mask_symmetry: float,
        max_site_distance_cutoff: float,
        min_bdc: float,
        max_bdc: float,
        sample_rate: float,
        statmaps: bool,
        analyse_model_func: AnalyseModelInterface,
        score_events_func: GetEventScoreInterface,
        process_local: ProcessorInterface,
        test_x: float,
        test_y: float,
        test_z: float,
        debug: Debug = Debug.DEFAULT,
) -> DatasetResultInterface:
    if debug >= Debug.PRINT_SUMMARIES:
        print(f'\tProcessing dtag: {test_dtag}')
    time_dataset_start = time.time()

    dataset_log_path = pandda_fs_model.processed_datasets.processed_datasets[test_dtag].log_path
    dataset_log = {}
    dataset_log["Model analysis time"] = {}

    ###################################################################
    # # Process the models...
    ###################################################################
    time_model_analysis_start = time.time()

    model_results: ModelResultsInterface = {
        model_number: model_result
        for model_number, model_result
        in zip(
            models,
            process_local(
                [
                    Partial(
                        analyse_model_func).paramaterise(
                        model,
                        model_number,
                        test_dtag=test_dtag,
                        dataset_xmap=dataset_xmaps[test_dtag],
                        reference=reference,
                        grid=grid,
                        dataset_processed_dataset=pandda_fs_model.processed_datasets.processed_datasets[test_dtag],
                        dataset_alignment=alignments[test_dtag],
                        max_site_distance_cutoff=max_site_distance_cutoff,
                        min_bdc=min_bdc, max_bdc=max_bdc,
                        contour_level=contour_level,
                        cluster_cutoff_distance_multiplier=cluster_cutoff_distance_multiplier,
                        min_blob_volume=min_blob_volume,
                        min_blob_z_peak=min_blob_z_peak,
                        output_dir=pandda_fs_model.processed_datasets.processed_datasets[test_dtag].path,
                        score_events_func=score_events_func,
                        res=shell.res,
                        rate=0.5,
                        test_x=test_x,
                        test_y=test_y,
                        test_z=test_z,
                        debug=debug
                    )
                    for model_number, model
                    in models.items()
                ]
            )
        )
    }

    dataset_log["Model logs"] = {model_number: model_result.model_log for model_number, model_result in
                                 model_results.items()}  #

    time_model_analysis_finish = time.time()

    dataset_log["Time to analyse all models"] = time_model_analysis_finish - time_model_analysis_start

    if debug >= Debug.PRINT_SUMMARIES:
        print(f"\tTime to analyse all models: {time_model_analysis_finish - time_model_analysis_start}")
        for model_number, model_result in model_results.items():
            model_time = dataset_log["Model logs"][model_number]["Model analysis time"]
            print(f"\t\tModel {model_number} processed in {model_time}")

    # ###################################################################
    # # # Decide which model to use...
    # ###################################################################
    # if debug >= Debug.PRINT_SUMMARIES:
    #     print(f"\tSelecting model...")
    # model_selection: ModelSelectionInterface = EXPERIMENTAL_select_model(
    #     model_results,
    #     grid.partitioning.inner_mask,
    #     pandda_fs_model.processed_datasets.processed_datasets[test_dtag],
    #     debug=debug,
    # )
    # selected_model: ModelInterface = models[model_selection.selected_model_id]
    # selected_model_clusterings = model_results[model_selection.selected_model_id].clusterings_merged
    # zmap = model_results[model_selection.selected_model_id].zmap
    # dataset_log['Selected model'] = int(model_selection.selected_model_id)
    # dataset_log['Model selection log'] = model_selection.log
    #
    # if debug >= Debug.PRINT_SUMMARIES:
    #     print(f'\tSelected model is: {model_selection.selected_model_id}')
    #
    # ###################################################################
    # # # Output the z map
    # ###################################################################
    # time_output_zmap_start = time.time()
    #
    # native_grid = dataset_truncated_datasets[test_dtag].reflections.transform_f_phi_to_map(
    #     structure_factors.f,
    #     structure_factors.phi,
    #     # sample_rate=sample_rate,  # TODO: make this d_min/0.5?
    #     sample_rate=dataset_truncated_datasets[test_dtag].reflections.get_resolution() / 0.5
    # )
    #
    # partitioning = Partitioning.from_structure_multiprocess(
    #     dataset_truncated_datasets[test_dtag].structure,
    #     native_grid,
    #     outer_mask,
    #     inner_mask_symmetry,
    # )
    # # pandda_fs_model.processed_datasets.processed_datasets[dtag].z_map_file.save_reference_frame_zmap(zmap)
    #
    # save_native_frame_zmap(
    #     pandda_fs_model.processed_datasets.processed_datasets[test_dtag].z_map_file.path,
    #     zmap,
    #     dataset_truncated_datasets[test_dtag],
    #     alignments[test_dtag],
    #     grid,
    #     structure_factors,
    #     outer_mask,
    #     inner_mask_symmetry,
    #     partitioning,
    #     sample_rate,
    # )
    #
    # # TODO: Remove altogether
    # if debug >= Debug.DATASET_MAPS:
    #     for model_number, model_result in model_results.items():
    #         save_reference_frame_zmap(
    #             pandda_fs_model.processed_datasets.processed_datasets[
    #                 test_dtag].z_map_file.path.parent / f'{model_number}_ref.ccp4',
    #             model_result.zmap
    #         )
    #         save_native_frame_zmap(
    #             pandda_fs_model.processed_datasets.processed_datasets[
    #                 test_dtag].z_map_file.path.parent / f'{model_number}_native.ccp4',
    #             model_result.zmap,
    #             dataset_truncated_datasets[test_dtag],
    #             alignments[test_dtag],
    #             grid,
    #             structure_factors,
    #             outer_mask,
    #             inner_mask_symmetry,
    #             partitioning,
    #             sample_rate,
    #         )
    #
    # # if statmaps:
    # #     mean_map_file = MeanMapFile.from_zmap_file(
    # #         pandda_fs_model.processed_datasets.processed_datasets[test_dtag].z_map_file)
    # #     mean_map_file.save_native_frame_mean_map(
    # #         selected_model,
    # #         zmap,
    # #         dataset_truncated_datasets[test_dtag],
    # #         alignments[test_dtag],
    # #         grid,
    # #         structure_factors,
    # #         outer_mask,
    # #         inner_mask_symmetry,
    # #         partitioning,
    # #         sample_rate,
    # #     )
    #
    # #     std_map_file = StdMapFile.from_zmap_file(pandda_fs_model.processed_datasets.processed_datasets[
    # #                                                  test_dtag].z_map_file)
    # #     std_map_file.save_native_frame_std_map(
    # #         test_dtag,
    # #         selected_model,
    # #         zmap,
    # #         dataset_truncated_datasets[test_dtag],
    # #         alignments[test_dtag],
    # #         grid,
    # #         structure_factors,
    # #         outer_mask,
    # #         inner_mask_symmetry,
    # #         partitioning,
    # #         sample_rate,
    # #     )
    # time_output_zmap_finish = time.time()
    # dataset_log['Time to output z map'] = time_output_zmap_finish - time_output_zmap_start
    #
    # ###################################################################
    # # # Find the events
    # ###################################################################
    # time_event_start = time.time()
    # # Calculate the shell events
    # # events: Events = Events.from_clusters(
    # #     selected_model_clusterings,
    # #     selected_model,
    # #     dataset_xmaps,
    # #     grid,
    # #     alignments[test_dtag],
    # #     max_site_distance_cutoff,
    # #     min_bdc, max_bdc,
    # #     None,
    # # )
    # events = model_results[model_selection.selected_model_id].events
    #
    # time_event_finish = time.time()
    # dataset_log[constants.LOG_DATASET_EVENT_TIME] = time_event_finish - time_event_start
    # update_log(dataset_log, dataset_log_path)
    #
    # ###################################################################
    # # # Generate event maps
    # ###################################################################
    # time_event_map_start = time.time()
    #
    # # Save the event maps!
    # # printer.pprint(events)
    # Events(events).save_event_maps(
    #     dataset_truncated_datasets,
    #     alignments,
    #     dataset_xmaps,
    #     selected_model,
    #     pandda_fs_model,
    #     grid,
    #     structure_factors,
    #     outer_mask,
    #     inner_mask_symmetry,
    #     sample_rate,
    #     native_grid,
    #     mapper=ProcessLocalSerial(),
    # )
    #
    # if debug >= Debug.DATASET_MAPS:
    #     for model_number, model_result in model_results.items():
    #         for event_id, event in model_result.events.items():
    #             save_event_map(
    #                 pandda_fs_model.processed_datasets.processed_datasets[event_id.dtag].path / f'{model_number}'
    #                                                                                             f'_{event_id.event_idx.event_idx}.ccp4',
    #                 dataset_xmaps[event_id.dtag],
    #                 models[model_number],
    #                 event,
    #                 dataset_truncated_datasets[event_id.dtag],
    #                 alignments[event_id.dtag],
    #                 grid,
    #                 structure_factors,
    #                 outer_mask,
    #                 inner_mask_symmetry,
    #                 partitioning,
    #                 sample_rate,
    #             )
    #
    # time_event_map_finish = time.time()
    # dataset_log[constants.LOG_DATASET_EVENT_MAP_TIME] = time_event_map_finish - time_event_map_start
    # update_log(dataset_log, dataset_log_path)
    #
    # time_dataset_finish = time.time()
    # dataset_log[constants.LOG_DATASET_TIME] = time_dataset_finish - time_dataset_start
    # update_log(dataset_log, dataset_log_path)
    #
    # return DatasetResult(
    #     dtag=test_dtag,
    #     events={event_id: event for event_id, event in events.items()},
    #     event_scores=model_results[model_selection.selected_model_id].event_scores,
    #     log=dataset_log,
    # )


def save_array_to_map_file(
        array: NDArrayInterface,
        template: CrystallographicGridInterface,
        path: Path
):
    spacing = [template.nu, template.nv, template.nw]
    unit_cell = template.unit_cell
    grid = gemmi.FloatGrid(spacing[0], spacing[1], spacing[2])
    grid.set_unit_cell(unit_cell)
    grid.spacegroup = gemmi.find_spacegroup_by_name("P 1")

    grid_array = np.array(grid, copy=False)
    grid_array[:, :, :] = array[:, :, :]

    ccp4 = gemmi.Ccp4Map()
    ccp4.grid = grid
    ccp4.update_ccp4_header(2, True)
    ccp4.setup()
    ccp4.write_ccp4_map(str(path))


def save_xmap(
        xmap: XmapInterface,
        path: Path
):
    xmap.xmap.spacegroup = gemmi.find_spacegroup_by_name("P 1")
    ccp4 = gemmi.Ccp4Map()
    ccp4.grid = xmap.xmap
    ccp4.update_ccp4_header(2, True)
    ccp4.setup()
    ccp4.write_ccp4_map(str(path))


def save_raw_xmap(
        dataset: DatasetInterface,
        path: Path,
        structure_factors,
        sample_rate,
):
    unaligned_xmap: gemmi.FloatGrid = dataset.reflections.transform_f_phi_to_map(structure_factors.f,
                                                                                 structure_factors.phi,
                                                                                 sample_rate=sample_rate,
                                                                                 )
    unaligned_xmap.spacegroup = gemmi.find_spacegroup_by_name("P 1")
    ccp4 = gemmi.Ccp4Map()
    ccp4.grid = unaligned_xmap
    ccp4.update_ccp4_header(2, True)
    ccp4.setup()
    ccp4.write_ccp4_map(str(path))


def process_shell_multiple_models_pandda_analysis_clustering(
        shell: ShellInterface,
        datasets: DatasetsInterface,
        alignments: AlignmentsInterface,
        grid: GridInterface,
        pandda_fs_model: PanDDAFSModelInterface,
        reference: ReferenceInterface,
        process_local: ProcessorInterface,
        structure_factors: StructureFactorsInterface,
        sample_rate: float,
        contour_level: float,
        cluster_cutoff_distance_multiplier: float,
        min_blob_volume: float,
        min_blob_z_peak: float,
        outer_mask: float,
        inner_mask_symmetry: float,
        max_site_distance_cutoff: float,
        min_bdc: float,
        max_bdc: float,
        memory_availability: str,
        statmaps: bool,
        load_xmap_func: LoadXMapInterface,
        analyse_model_func: AnalyseModelInterface,
        score_events_func: GetEventScoreInterface,
        sample_points,
        debug: Debug = Debug.DEFAULT,
):
    if debug >= Debug.PRINT_SUMMARIES:
        print(f"Processing shell at resolution: {shell.res}")

    if memory_availability == "very_low":
        process_local_in_shell: ProcessorInterface = ProcessLocalSerial()
        process_local_in_dataset: ProcessorInterface = ProcessLocalSerial()
        process_local_over_datasets: ProcessorInterface = ProcessLocalSerial()
    elif memory_availability == "low":
        process_local_in_shell: ProcessorInterface = process_local
        process_local_in_dataset: ProcessorInterface = process_local
        process_local_over_datasets: ProcessorInterface = ProcessLocalSerial()
    elif memory_availability == "high":
        process_local_in_shell: ProcessorInterface = process_local
        process_local_in_dataset: ProcessorInterface = ProcessLocalSerial()
        process_local_over_datasets: ProcessorInterface = process_local

    else:
        raise Exception(f"memory_availability: {memory_availability}: does not have defined processors")

    time_shell_start = time.time()
    if pandda_fs_model.shell_dirs:
        shell_log_path = pandda_fs_model.shell_dirs.shell_dirs[shell.res].log_path
    else:
        raise Exception(
            "Attempted to find the log path for the shell, but no shell dir added to pandda_fs_model somehow.")
    shell_log = {}

    # Seperate out test and train datasets
    shell_datasets: DatasetsInterface = {
        dtag: dataset
        for dtag, dataset
        in datasets.items()
        if dtag in shell.all_dtags
    }
    shell_log[constants.LOG_SHELL_DATASETS] = [dtag.dtag for dtag in shell_datasets]
    update_log(shell_log, shell_log_path)

    ###################################################################
    # # Homogonise shell datasets by truncation of resolution
    ###################################################################
    if debug >= Debug.PRINT_SUMMARIES:
        print(f"\tTruncating shell datasets")
    shell_working_resolution: ResolutionInterface = Resolution(
        max([datasets[dtag].reflections.get_resolution() for dtag in shell.all_dtags]))
    shell_working_resolution: ResolutionInterface = Resolution(
        max([datasets[dtag].reflections.get_resolution() for dtag in shell.all_dtags] + [shell.res,]))
    shell_truncated_datasets: DatasetsInterface = truncate(
        shell_datasets,
        resolution=shell_working_resolution,
        structure_factors=structure_factors,
    )
    # TODO: REMOVE?
    # shell_truncated_datasets = shell_datasets
    shell_log["Shell Working Resolution"] = shell_working_resolution.resolution

    ###################################################################
    # # Generate aligned Xmaps
    ###################################################################
    if debug >= Debug.PRINT_SUMMARIES:
        print(f"\tLoading xmaps")

    time_xmaps_start = time.time()

    xmaps: XmapsInterface = {
        dtag: xmap
        for dtag, xmap
        in zip(
            shell_truncated_datasets,
            process_local_in_shell(
                [
                    Partial(load_xmap_func).paramaterise(
                        shell_truncated_datasets[key],
                        alignments[key],
                        grid=grid,
                        structure_factors=structure_factors,
                        sample_rate=shell.res / 0.5,
                    )
                    for key
                    in shell_truncated_datasets
                ]
            )
        )
    }

    time_xmaps_finish = time.time()
    shell_log[constants.LOG_SHELL_XMAP_TIME] = time_xmaps_finish - time_xmaps_start
    update_log(shell_log, shell_log_path)

    if debug >= Debug.DATASET_MAPS:
        for dtag, xmap in xmaps.items():
            xmap_array = np.array(xmap.xmap)
            save_array_to_map_file(
                xmap_array,
                grid.grid,
                pandda_fs_model.pandda_dir / f"{shell.res}_{dtag}_ref.ccp4"
            )

            save_raw_xmap(
                shell_truncated_datasets[dtag],
                pandda_fs_model.pandda_dir / f"{shell.res}_{dtag}_mov.ccp4",
                structure_factors,
                sample_rate
            )

            # save_xmap(
            #     xmap,
            #     pandda_fs_model.pandda_dir / f"{shell.res}_{dtag}.ccp4"
            # )

    ###################################################################
    # # Get the role of each dtag in each model
    ###################################################################
    dtag_class_dict = {}
    for model_number, model_dtags in shell.train_dtags.items():
        dtag_class_dict[model_number] = {}
        for dtag in model_dtags:
            if dtag in shell.test_dtags:
                dtag_class_dict[model_number][dtag] = "Train/Test"
            else:
                dtag_class_dict[model_number][dtag] = "Train"

        for dtag in shell.test_dtags:
            if dtag not in dtag_class_dict[model_number]:
                dtag_class_dict[model_number][dtag] = "Test"

    print(dtag_class_dict)
    ###################################################################
    # # Get the distribution of ED values at the target point
    ###################################################################

    xmap_samples = {}
    for sample_key, sample_point in sample_points.items():
        xmap_samples[sample_key] = {}
        test_x, test_y, test_z = sample_point

        for model_number, model_dtags in shell.train_dtags.items():
            xmap_samples[sample_key][model_number] = {}

            for dtag, xmap in xmaps.items():
                # if dtag not in model_dtags:
                #     continue
                if dtag not in dtag_class_dict[model_number]:
                    continue

                xmap_grid = xmap.xmap
                sample = xmap_grid.interpolate_value(gemmi.Position(test_x, test_y, test_z))
                xmap_samples[sample_key][model_number][dtag] = sample

    ###################################################################
    # # Get the models to test
    ###################################################################
    if debug >= Debug.PRINT_SUMMARIES:
        print(f"\tGetting models")
    models: ModelsInterface = get_models(
        shell.test_dtags,
        shell.train_dtags,
        xmaps,
        grid,
        process_local_in_shell,
    )

    if debug >= Debug.PRINT_SUMMARIES:
        for model_key, model in models.items():
            save_array_to_map_file(
                model.mean,
                grid.grid,
                pandda_fs_model.pandda_dir / f"{shell.res}_{model_key}_mean.ccp4"
            )

    ###################################################################
    # # Process each test dataset
    ###################################################################
    # Now that all the data is loaded, get the comparison set and process each test dtag

    # process_dataset_paramaterized =

    # Process each dataset in the shell
    all_train_dtags_unmerged = [_dtag for l in shell.train_dtags.values() for _dtag in l]
    all_train_dtags = []
    for _dtag in all_train_dtags_unmerged:
        if _dtag not in all_train_dtags:
            all_train_dtags.append(_dtag)

    if debug >= Debug.PRINT_NUMERICS:
        print(f"\tAll train datasets are: {all_train_dtags}")
    # dataset_dtags = {_dtag:  for _dtag in shell.test_dtags for n in shell.train_dtags}
    dataset_dtags = {_dtag: [_dtag] + all_train_dtags for _dtag in shell.test_dtags}
    if debug >= Debug.PRINT_NUMERICS:
        print(f"\tDataset dtags are: {dataset_dtags}")

    # Iterate over every dtag, getting the zmap, and interpolating at the sample point
    zmap_samples = {}
    for sample_key, sample_point in sample_points.items():
        zmap_samples[sample_key] = {}
        test_x, test_y, test_z = sample_point

        for model_number, model_dtags in shell.train_dtags.items():
            zmap_samples[sample_key][model_number] = {}

            model = models[model_number]

            for dtag, xmap in xmaps.items():
                if dtag not in dtag_class_dict[model_number]:
                    continue

                zmaps: ZmapsInterface = Zmaps.from_xmaps(
                    model=model,
                    xmaps={dtag: xmap, },
                    model_number=model_number,
                    debug=debug,
                )
                zmap = zmaps[dtag]
                zmap_grid = zmap.zmap

                sample = zmap_grid.interpolate_value(gemmi.Position(test_x, test_y, test_z))
                zmap_samples[sample_key][model_number][dtag] = sample

    ###################################################################
    # # Get the sigma_sm at the target point
    ###################################################################
    model_sigma_sms = {}
    for sample_key, sample_point in sample_points.items():
        model_sigma_sms[sample_key] = {}
        test_x, test_y, test_z = sample_point
        for model_number, model in models.items():
            sigma_sm_grid = grid.new_grid()
            sigma_sm_grid_array = np.array(sigma_sm_grid, copy=False)
            sigma_sm_grid_array[:,:,:] = model.sigma_s_m
            # xmap_grid = xmap.xmap
            sigma_sm = sigma_sm_grid.interpolate_value(gemmi.Position(test_x, test_y, test_z))
            # xmap_samples[dtag] = sample
            model_sigma_sms[sample_key][model_number] = sigma_sm

    ###################################################################
    # # Get the model sigma_is
    ###################################################################
    model_sigma_is = {model_num: model.sigma_is for model_num, model in models.items()}

    return xmap_samples, zmap_samples, model_sigma_is, model_sigma_sms, dtag_class_dict

    # results = Partial(
    #             process_dataset_multiple_models_pandda_analysis).paramaterise(
    #             test_dtag,
    #             dataset_truncated_datasets={_dtag: shell_truncated_datasets[_dtag] for _dtag in
    #                                         dataset_dtags[test_dtag]},
    #             dataset_xmaps={_dtag: xmaps[_dtag] for _dtag in dataset_dtags[test_dtag]},
    #             models=models,
    #             shell=shell,
    #             alignments=alignments,
    #             pandda_fs_model=pandda_fs_model,
    #             reference=reference,
    #             grid=grid,
    #             contour_level=contour_level,
    #             cluster_cutoff_distance_multiplier=cluster_cutoff_distance_multiplier,
    #             min_blob_volume=min_blob_volume,
    #             min_blob_z_peak=min_blob_z_peak,
    #             structure_factors=structure_factors,
    #             outer_mask=outer_mask,
    #             inner_mask_symmetry=inner_mask_symmetry,
    #             max_site_distance_cutoff=max_site_distance_cutoff,
    #             min_bdc=min_bdc,
    #             max_bdc=max_bdc,
    #             # sample_rate=sample_rate,
    #             sample_rate=shell.res / 0.5,
    #             statmaps=statmaps,
    #             analyse_model_func=analyse_model_func,
    #             score_events_func=score_events_func,
    #             process_local=process_local_in_dataset,
    #             test_x=test_x,
    #             test_y=test_y,
    #             test_z=test_z,
    #             debug=debug,
    #         )()


    # Update shell log with dataset results
    # shell_log[constants.LOG_SHELL_DATASET_LOGS] = {}
    # for result in results:
    #     if result:
    #         shell_log[constants.LOG_SHELL_DATASET_LOGS][str(result.dtag)] = result.log
    #
    # time_shell_finish = time.time()
    # shell_log[constants.LOG_SHELL_TIME] = time_shell_finish - time_shell_start
    # update_log(shell_log, shell_log_path)
    #
    # shell_result: ShellResultInterface = ShellResult(
    #     shell=shell,
    #     dataset_results={dtag: result for dtag, result in zip(shell.test_dtags, results) if result},
    #     log=shell_log,
    #
    # )

    # return shell_result
