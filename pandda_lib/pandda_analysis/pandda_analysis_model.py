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


class ModelResult(ModelResultInterface):
    def __init__(self,
                 zmap,
                 clusterings,
                 clusterings_large,
                 clusterings_peaked,
                 clusterings_merged,
                 events,
                 event_scores,
                 model_log) -> None:
        self.zmap = zmap
        self.clusterings = clusterings
        self.clusterings_large = clusterings_large
        self.clusterings_peaked = clusterings_peaked
        self.clusterings_merged = clusterings_merged
        self.events = events
        self.event_scores: EventScoringResultsInterface = event_scores
        self.model_log = model_log



def analyse_model_pandda_analysis(
        model,
        model_number,
        test_dtag,
        dataset_xmap,
        reference,
        grid,
        dataset_processed_dataset,
        dataset_alignment,
        max_site_distance_cutoff,
        min_bdc, max_bdc,
        contour_level,
        cluster_cutoff_distance_multiplier,
        min_blob_volume,
        min_blob_z_peak,
        output_dir,
        score_events_func: GetEventScoreInterface,
        res, rate,
        test_x: float,
        test_y: float,
        test_z: float,
        debug: Debug = Debug.DEFAULT
):
    if debug >= Debug.PRINT_SUMMARIES:
        print(f'\tAnalysing model: {model_number}')

    model_log = {}

    ###################################################################
    # # Generate the statistical model of the dataset
    ###################################################################
    time_model_analysis_start = time.time()

    # Calculate z maps
    if debug >= Debug.PRINT_SUMMARIES:
        print("\t\tCalculating zmaps")
    time_z_maps_start = time.time()
    zmaps: ZmapsInterface = Zmaps.from_xmaps(
        model=model,
        xmaps={test_dtag: dataset_xmap, },
        model_number=model_number,
        debug=debug,
    )

    if debug >= Debug.PRINT_SUMMARIES:
        print("\t\tCalculated zmaps")



    time_z_maps_finish = time.time()
    model_log[constants.LOG_DATASET_Z_MAPS_TIME] = time_z_maps_finish - time_z_maps_start
    for dtag, zmap in zmaps.items():
        z_map_statistics = GetMapStatistics(
            zmap
        )
        model_log["ZMap statistics"] = {
            "mean": str(z_map_statistics.mean),
            "std": str(z_map_statistics.std),
            ">1.0": str(z_map_statistics.greater_1),
            ">2.0": str(z_map_statistics.greater_2),
            ">3.0": str(z_map_statistics.greater_3),
        }
        if debug >= Debug.PRINT_SUMMARIES:
            print(model_log["ZMap statistics"])

    # update_log(dataset_log, dataset_log_path)

    ###################################################################
    # # Get the distribution of z values at the target point
    ###################################################################


    # ###################################################################
    # # # Cluster the outlying density
    # ###################################################################
    # time_cluster_start = time.time()
    #
    # # Get the clustered electron desnity outliers
    #
    # time_cluster_z_start = time.time()
    #
    # if debug >= Debug.PRINT_SUMMARIES:
    #     print("\t\tClustering")
    #
    # clusterings_list: List[EDClusteringInterface] = process_local_serial(
    #     [
    #         Partial(GetEDClustering()).paramaterise(
    #             zmaps[dtag],
    #             reference=reference,
    #             grid=grid,
    #             contour_level=contour_level,
    #             cluster_cutoff_distance_multiplier=cluster_cutoff_distance_multiplier, )
    #         for dtag
    #         in zmaps
    #     ]
    # )
    # time_cluster_z_finish = time.time()
    #
    # if debug >= Debug.PRINT_SUMMARIES:
    #     print("\t\tClustering finished")
    #
    # # if debug:
    # #     model_log['Time to perform primary clustering of z map'] = time_cluster_z_finish - time_cluster_z_start
    # #     model_log['time_event_mask'] = {}
    # #     for j, clustering in enumerate(clusterings_list):
    # #         model_log['time_cluster'] = clustering.time_cluster
    # #         model_log['time_np'] = clustering.time_np
    # #         model_log['time_event_masking'] = clustering.time_event_masking
    # #         model_log['time_get_orth'] = clustering.time_get_orth
    # #         model_log['time_fcluster'] = clustering.time_fcluster
    # #         for cluster_num, cluster in clustering.clustering.items():
    # #             model_log['time_event_mask'][int(cluster_num)] = cluster.time_event_mask
    #
    # clusterings: EDClusteringsInterface = {dtag: clustering for dtag, clustering in zip(zmaps, clusterings_list)}
    #
    # model_log[constants.LOG_DATASET_INITIAL_CLUSTERS_NUM] = sum(
    #     [len(clustering) for clustering in clusterings.values()])
    # # update_log(dataset_log, dataset_log_path)
    # cluster_sizes = {}
    # for dtag, clustering in clusterings.items():
    #     for cluster_num, cluster in clustering.clustering.items():
    #         cluster_sizes[int(cluster_num)] = {
    #             "size": float(cluster.size(grid)),
    #             "centroid": (float(cluster.centroid[0]), float(cluster.centroid[1]), float(cluster.centroid[2])),
    #         }
    # model_log[constants.LOG_DATASET_CLUSTER_SIZES] = {
    #     cluster_num: cluster_sizes[cluster_num]
    #     for j, cluster_num
    #     in enumerate(sorted(
    #         cluster_sizes, key=lambda _cluster_num: cluster_sizes[_cluster_num]["size"],
    #         reverse=True,
    #     ))
    #     if j < 10
    # }
    # # update_log(dataset_log, dataset_log_path)
    #
    # # Filter out small clusters
    # clusterings_large: EDClusteringsInterface = FilterEDClusteringsSize()(clusterings,
    #                                                                       grid,
    #                                                                       min_blob_volume,
    #                                                                       )
    # if debug >= Debug.PRINT_SUMMARIES:
    #     print("\t\tAfter filtering: large: {}".format(
    #         {dtag: len(cluster) for dtag, cluster in
    #          zip(clusterings_large, clusterings_large.values())}))
    # model_log[constants.LOG_DATASET_LARGE_CLUSTERS_NUM] = sum(
    #     [len(clustering) for clustering in clusterings_large.values()])
    # # update_log(dataset_log, dataset_log_path)
    #
    # # Filter out weak clusters (low peak z score)
    # clusterings_peaked: EDClusteringsInterface = FilterEDClusteringsPeak()(clusterings_large,
    #                                                                        grid,
    #                                                                        min_blob_z_peak)
    # if debug >= Debug.PRINT_SUMMARIES:
    #     print("\t\tAfter filtering: peak: {}".format(
    #         {dtag: len(cluster) for dtag, cluster in
    #          zip(clusterings_peaked, clusterings_peaked.values())}))
    # model_log[constants.LOG_DATASET_PEAKED_CLUSTERS_NUM] = sum(
    #     [len(clustering) for clustering in clusterings_peaked.values()])
    # # update_log(dataset_log, dataset_log_path)
    #
    # # Add the event mask
    # for clustering_id, clustering in clusterings_peaked.items():
    #     for cluster_id, cluster in clustering.clustering.items():
    #         cluster.event_mask_indicies = get_event_mask_indicies(
    #             zmaps[test_dtag],
    #             cluster.cluster_positions_array)
    #
    # # Merge the clusters
    # clusterings_merged: EDClusteringsInterface = MergeEDClusterings()(clusterings_peaked)
    # if debug >= Debug.PRINT_SUMMARIES:
    #     print("\t\tAfter filtering: merged: {}".format(
    #         {dtag: len(_cluster) for dtag, _cluster in
    #          zip(clusterings_merged, clusterings_merged.values())}))
    # model_log[constants.LOG_DATASET_MERGED_CLUSTERS_NUM] = sum(
    #     [len(clustering) for clustering in clusterings_merged.values()])
    # # update_log(dataset_log, dataset_log_path)
    #
    # # Log the clustering
    # time_cluster_finish = time.time()
    # model_log[constants.LOG_DATASET_CLUSTER_TIME] = time_cluster_finish - time_cluster_start
    # # update_log(dataset_log, dataset_log_path)
    #
    #
    # events: Events = Events.from_clusters(
    #     clusterings_large,
    #     model,
    #     {test_dtag: dataset_xmap, },
    #     grid,
    #     dataset_alignment,
    #     max_site_distance_cutoff,
    #     min_bdc, max_bdc,
    #     None,
    # )
    #
    # if debug >= Debug.PRINT_SUMMARIES:
    #     print("\t\tScoring events...")
    #
    # if debug >= Debug.AVERAGE_MAPS:
    #     with open(output_dir / "test_dtag.pickle", "wb") as f:
    #         pickle.dump(test_dtag, f)
    #
    #     with open(output_dir / "model_number.pickle", "wb") as f:
    #         pickle.dump(model_number, f)
    #
    #     with open(output_dir / "dataset_processed_dataset.pickle", "wb") as f:
    #         pickle.dump(dataset_processed_dataset, f)
    #
    #     with open(output_dir / "dataset_xmap.pickle", "wb") as f:
    #         pickle.dump(dataset_xmap, f)
    #
    #     with open(output_dir / f"events_{model_number}.pickle", "wb") as f:
    #         pickle.dump(events, f)
    #
    #     with open(output_dir / f"model_{model_number}.pickle", "wb") as f:
    #         pickle.dump(model, f)
    #
    # if score_events_func.tag == "inbuilt":
    #     event_scores: EventScoringResultsInterface = score_events_func(
    #         test_dtag,
    #         model_number,
    #         dataset_processed_dataset,
    #         dataset_xmap,
    #         zmaps[test_dtag],
    #         events,
    #         model,
    #         grid,
    #         dataset_alignment,
    #         max_site_distance_cutoff,
    #         min_bdc, max_bdc,
    #         reference,
    #         res, rate,
    #         event_map_cut=2.0,
    #         structure_output_folder=output_dir,
    #         debug=debug
    #     )
    # # elif score_events_func.tag == "autobuild":
    # #     raise NotImplementedError()
    #
    # else:
    #     raise Exception("No valid event selection score method!")
    #
    #
    #
    # model_log['score'] = {}
    # model_log['noise'] = {}
    #
    # for event_id, event_scoring_result in event_scores.items():
    #     model_log['score'][int(event_id.event_idx)] = event_scoring_result.get_selected_structure_score()
    #     model_log[int(event_id.event_idx)] = event_scoring_result.log()
    #     # model_log['noise'][int(event_num)] = noises[event_num]
    #
    # time_model_analysis_finish = time.time()
    #
    # # model_results = {
    # #     'zmap': zmaps[test_dtag],
    # #     'clusterings': clusterings,
    # #     'clusterings_large': clusterings_large,
    # #     'clusterings_peaked': clusterings_peaked,
    # #     'clusterings_merged': clusterings_merged,
    # #     'events': events,
    # #     'event_scores': event_scores,
    # #     'log': model_log
    # # }
    # model_results: ModelResult = ModelResult(
    #     zmaps[test_dtag],
    #     clusterings,
    #     clusterings_large,
    #     clusterings_peaked,
    #     clusterings_merged,
    #     {event_id: event for event_id, event in events.events.items()},
    #     event_scores,
    #     model_log
    # )
    #
    # model_log["Model analysis time"] = time_model_analysis_finish - time_model_analysis_start
    # if debug >= Debug.PRINT_SUMMARIES:
    #     print(f"\t\tModel analysis time: {time_model_analysis_finish - time_model_analysis_start}")
    #
    # if debug >= Debug.PRINT_SUMMARIES:
    #     for event_id, event_score_result in model_results.event_scores.items():
    #         print(f"event log: {event_id.event_idx.event_idx} {event_id.dtag.dtag}")
    #         print(event_score_result.log())
    #
    # return model_results

