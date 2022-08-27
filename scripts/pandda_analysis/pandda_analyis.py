# Base python
import os
import traceback
import time
import pprint
from functools import partial
import json
import pickle

# Scientific python libraries
import joblib
import pandas as pd

# import ray

## Custom Imports
from pandda_gemmi import constants
from pandda_gemmi.common import Partial, Dtag
from pandda_gemmi.args import PanDDAArgs
from pandda_gemmi.dataset.dataset import GetReferenceDataset
from pandda_gemmi.edalignment.grid import GetGrid
from pandda_gemmi.pandda_logging import STDOUTManager, log_arguments, PanDDAConsole
from pandda_gemmi.dependencies import check_dependencies
from pandda_gemmi.dataset import (
    StructureFactors,
    SmoothBFactors,
    DatasetsStatistics,
    drop_columns,
    GetDatasets,
    GetReferenceDataset,
)
from pandda_gemmi.edalignment import (GetGrid, GetAlignments,
                                      LoadXmap, LoadXmapFlat
                                      )
from pandda_gemmi.filters import (
    FiltersDataQuality,
    FiltersReferenceCompatibility,
    FilterNoStructureFactors,
    FilterResolutionDatasets,
    FilterRFree,
    FilterDissimilarModels,
    FilterIncompleteModels,
    FilterDifferentSpacegroups,
    DatasetsValidator
)
from pandda_gemmi.comparators import (
    GetComparatorsHybrid, GetComparatorsHighResFirst, GetComparatorsHighResRandom, GetComparatorsHighRes,
    GetComparatorsCluster)
from pandda_gemmi.shells import get_shells_multiple_models
from pandda_gemmi.logs import (
    save_json_log,
)

from pandda_gemmi.pandda_functions import (
    get_dask_client,
    process_global_serial,
    process_global_dask,
    get_shells,
    get_common_structure_factors,
)
from pandda_gemmi.event import GetEventScoreInbuilt, add_sites_to_events
from pandda_gemmi.ranking import (
    GetEventRankingAutobuild,
    GetEventRankingSize,
    GetEventRankingSizeAutobuild,
    GetEventRankingEventScore
)
from pandda_gemmi.autobuild import (
    merge_ligand_into_structure_from_paths,
    save_pdb_file,
    GetAutobuildResultRhofit,
)
from pandda_gemmi.tables import (
    GetEventTable,
    GetSiteTable,
    SaveEvents
)
from pandda_gemmi.fs import GetPanDDAFSModel, GetShellDirs
from pandda_gemmi.processing import (
    process_shell,
    process_shell_multiple_models,
    analyse_model,
    ProcessLocalRay,
    ProcessLocalSerial,
    ProcessLocalSpawn,
)

from pandda_gemmi import event_classification
from pandda_gemmi.sites import GetSites
from pandda_gemmi.analyse_interface import *

# PanDDA analysis imports
from pandda_lib.pandda_analysis.pandda_analysis_shells import get_shells_pandda_analysis
from pandda_lib.pandda_analysis.pandda_analysis_model import analyse_model_pandda_analysis
from pandda_lib.pandda_analysis.pandda_analysis_process import process_shell_multiple_models_pandda_analysis
from pandda_lib.pandda_analysis.pandda_analysis_args import PanDDAArgsPanDDAAnalysis
from pandda_lib.pandda_analysis.test_point_to_reference_frame import get_test_pos_in_reference_frame

joblib.externals.loky.set_loky_pickler('pickle')
printer = pprint.PrettyPrinter()
console = PanDDAConsole()


def update_log(shell_log, shell_log_path):
    if shell_log_path.exists():
        os.remove(shell_log_path)

    with open(shell_log_path, "w") as f:
        json.dump(shell_log, f, indent=2)


def get_comparator_func(pandda_args: PanDDAArgs,
                        load_xmap_flat_func: LoadXMapFlatInterface,
                        process_local: ProcessorInterface) -> GetComparatorsInterface:
    if pandda_args.comparison_strategy == "closest":
        # Closest datasets after clustering
        raise NotImplementedError()

    elif pandda_args.comparison_strategy == "high_res":
        # Almost Old PanDDA strategy: highest res datasets

        comparators_func = GetComparatorsHighRes(
            comparison_min_comparators=pandda_args.comparison_min_comparators,
            comparison_max_comparators=pandda_args.comparison_max_comparators,
        )

    elif pandda_args.comparison_strategy == "high_res_random":

        comparators_func = GetComparatorsHighResRandom(
            comparison_min_comparators=pandda_args.comparison_min_comparators,
            comparison_max_comparators=pandda_args.comparison_max_comparators,
        )

    elif pandda_args.comparison_strategy == "high_res_first":
        # Old pandda strategy: random datasets that are higher resolution

        comparators_func = GetComparatorsHighResFirst(
            comparison_min_comparators=pandda_args.comparison_min_comparators,
            comparison_max_comparators=pandda_args.comparison_max_comparators,
        )

    elif pandda_args.comparison_strategy == "cluster":
        comparators_func = GetComparatorsCluster(
            comparison_min_comparators=pandda_args.comparison_min_comparators,
            comparison_max_comparators=pandda_args.comparison_max_comparators,
            sample_rate=pandda_args.sample_rate,
            # TODO: add option: pandda_args.resolution_cutoff,
            resolution_cutoff=3.0,
            load_xmap_flat_func=load_xmap_flat_func,
            process_local=process_local,
            debug=pandda_args.debug,
        )

    elif pandda_args.comparison_strategy == "hybrid":
        comparators_func = GetComparatorsHybrid(
            comparison_min_comparators=pandda_args.comparison_min_comparators,
            comparison_max_comparators=pandda_args.comparison_max_comparators,
            sample_rate=pandda_args.sample_rate,
            # TODO: add option: pandda_args.resolution_cutoff,
            resolution_cutoff=3.0,
            load_xmap_flat_func=load_xmap_flat_func,
            process_local=process_local,
            debug=pandda_args.debug,
        )

    else:
        raise Exception("Unrecognised comparison strategy")

    return comparators_func


def get_process_global(pandda_args, distributed_tmp):
    if pandda_args.global_processing == "serial":
        process_global = process_global_serial
    elif pandda_args.global_processing == "distributed":
        client = get_dask_client(
            scheduler=pandda_args.distributed_scheduler,
            num_workers=pandda_args.distributed_num_workers,
            queue=pandda_args.distributed_queue,
            project=pandda_args.distributed_project,
            cores_per_worker=pandda_args.local_cpus,
            distributed_mem_per_core=pandda_args.distributed_mem_per_core,
            resource_spec=pandda_args.distributed_resource_spec,
            job_extra=pandda_args.distributed_job_extra,
            walltime=pandda_args.distributed_walltime,
            watcher=pandda_args.distributed_watcher,
        )
        process_global = partial(
            process_global_dask,
            client=client,
            tmp_dir=distributed_tmp
        )
    else:
        raise Exception(f"Could not find an implementation of --global_processing: {pandda_args.global_processing}")

    return process_global


def get_process_local(pandda_args):
    if pandda_args.local_processing == "serial":
        process_local = ProcessLocalSerial()

    elif pandda_args.local_processing == "multiprocessing_spawn":
        process_local = ProcessLocalSpawn(pandda_args.local_cpus)

    # elif pandda_args.local_processing == "joblib":
    #     process_local = partial(process_local_joblib, n_jobs=pandda_args.local_cpus, verbose=50, max_nbytes=None)

    # elif pandda_args.local_processing == "multiprocessing_forkserver":
    #     mp.set_start_method("forkserver")
    #     process_local = partial(process_local_multiprocessing, n_jobs=pandda_args.local_cpus, method="forkserver")
    #     # process_local_load = partial(process_local_joblib, int(joblib.cpu_count() * 3), "threads")

    # elif pandda_args.local_processing == "multiprocessing_spawn":
    #     mp.set_start_method("spawn")
    #     process_local = partial(process_local_multiprocessing, n_jobs=pandda_args.local_cpus, method="spawn")
    #     # process_local_load = partial(process_local_joblib, int(joblib.cpu_count() * 3), "threads")
    # elif pandda_args.local_processing == "dask":
    #     client = Client(n_workers=pandda_args.local_cpus)
    #     process_local = partial(
    #         process_local_dask,
    #         client=client
    #     )

    elif pandda_args.local_processing == "ray":
        process_local = ProcessLocalRay(pandda_args.local_cpus)

    else:
        raise Exception()

    return process_local


def get_smooth_func(pandda_args: PanDDAArgs) -> SmoothBFactorsInterface:
    smooth_func: SmoothBFactorsInterface = SmoothBFactors()

    return smooth_func


def get_load_xmap_func(pandda_args) -> LoadXMapInterface:
    load_xmap_func = LoadXmap()
    return load_xmap_func


def get_load_xmap_flat_func(pandda_args) -> LoadXMapFlatInterface:
    load_xmap_flat_func = LoadXmapFlat()
    return load_xmap_flat_func


def get_analyse_model_func(pandda_args):
    analyse_model_func = analyse_model_pandda_analysis
    return analyse_model_func


def get_filter_data_quality(
        filter_keys: List[str],
        datasets_validator: DatasetsValidatorInterface,
        pandda_args: PanDDAArgs,
) -> FiltersDataQualityInterface:
    filters = {}

    if "structure_factors" in filter_keys:
        filters["structure_factors"] = FilterNoStructureFactors()

    if "resolution" in filter_keys:
        filters["resolution"] = FilterResolutionDatasets(pandda_args.low_resolution_completeness)

    if "rfree" in filter_keys:
        filters["rfree"] = FilterRFree(pandda_args.max_rfree)

    return FiltersDataQuality(filters, datasets_validator)


def get_filter_reference_compatability(
        filter_keys: List[str],
        datasets_validator: DatasetsValidatorInterface,
        pandda_args: PanDDAArgs,
) -> FiltersReferenceCompatibilityInterface:
    filters = {}

    if "dissimilar_models" in filter_keys:
        print("filter models")
        filters["dissimilar_models"] = FilterDissimilarModels(pandda_args.max_rmsd_to_reference)

    if "large_gaps" in filter_keys:
        print("filter gaps")
        filters["large_gaps"] = FilterIncompleteModels()

    if "dissimilar_spacegroups" in filter_keys:
        print("filter sg")
        filters["dissimilar_spacegroups"] = FilterDifferentSpacegroups()

    return FiltersReferenceCompatibility(filters, datasets_validator)


def get_score_events_func(pandda_args: PanDDAArgs) -> GetEventScoreInterface:
    return GetEventScoreInbuilt()


def process_pandda(pandda_args: PanDDAArgsPanDDAAnalysis, ):
    ###################################################################
    # # Configuration
    ###################################################################
    time_start = time.time()

    # Process args
    distributed_tmp = Path(pandda_args.distributed_tmp)

    # CHeck dependencies
    # with STDOUTManager('Checking dependencies...', '\tAll dependencies validated!'):
    console.start_dependancy_check()
    check_dependencies(pandda_args)

    # Initialise log
    # with STDOUTManager('Initialising log...', '\tPanDDA log initialised!'):
    console.start_log()
    pandda_log: Dict = {}
    pandda_log[constants.LOG_START] = time.time()
    initial_args = log_arguments(pandda_args, )

    pandda_log[constants.LOG_ARGUMENTS] = initial_args

    # Get global processor
    console.start_initialise_shell_processor()
    process_global: ProcessorInterface = get_process_global(pandda_args, distributed_tmp)

    # Get local processor
    console.start_initialise_multiprocessor()
    process_local: ProcessorInterface = get_process_local(pandda_args)

    # Get reflection smoothing function
    smooth_func: SmoothBFactorsInterface = get_smooth_func(pandda_args)

    # Get XMap loading functions
    load_xmap_func: LoadXMapInterface = get_load_xmap_func(pandda_args)
    load_xmap_flat_func: LoadXMapFlatInterface = get_load_xmap_flat_func(pandda_args)

    # Get the filtering functions
    datasets_validator: DatasetsValidatorInterface = DatasetsValidator(pandda_args.min_characterisation_datasets)
    filter_data_quality: FiltersDataQualityInterface = get_filter_data_quality(
        [
            "structure_factors",
            "resolution",
            "rfree"
        ],
        datasets_validator,
        pandda_args
    )
    filter_reference_compatability: FiltersReferenceCompatibilityInterface = get_filter_reference_compatability(
        [
            "dissimilar_models",
            "large_gaps",
            # "dissimilar_spacegroups",
        ],
        datasets_validator,
        pandda_args
    )

    # Get the functino for selecting the comparators
    comparators_func: GetComparatorsInterface = get_comparator_func(
        pandda_args,
        load_xmap_flat_func,
        process_local
    )

    # Get the staticial model anaylsis function
    analyse_model_func: AnalyseModelInterface = get_analyse_model_func(pandda_args)

    # Get the event scoring func
    score_events_func: GetEventScoreInterface = get_score_events_func(pandda_args)

    # Set up autobuilding function
    autobuild_func: Optional[GetAutobuildResultInterface] = None
    if pandda_args.autobuild:

        # with STDOUTManager('Setting up autobuilding...', '\tSet up autobuilding!'):
        if pandda_args.autobuild_strategy == "rhofit":
            autobuild_func: Optional[GetAutobuildResultInterface] = GetAutobuildResultRhofit()

        elif pandda_args.autobuild_strategy == "inbuilt":
            raise NotImplementedError("Autobuilding with inbuilt method is not yet implemented")

        else:
            raise Exception(f"Autobuild strategy: {pandda_args.autobuild_strategy} is not valid!")

    # Get the event classification function
    if pandda_args.autobuild:
        get_event_class: GetEventClassInterface = event_classification.GetEventClassAutobuildScore(0.4, 0.25)
    else:
        get_event_class: GetEventClassInterface = event_classification.GetEventClassTrivial()

    # Get the site determination function
    get_sites: GetSitesInterface = GetSites(pandda_args.max_site_distance_cutoff)

    ###################################################################
    # # Run the PanDDA
    ###################################################################
    try:

        ###################################################################
        # # Get datasets
        ###################################################################
        console.start_fs_model()
        time_fs_model_building_start = time.time()
        pandda_fs_model: PanDDAFSModelInterface = GetPanDDAFSModel(
            pandda_args.data_dirs,
            pandda_args.out_dir,
            pandda_args.pdb_regex,
            pandda_args.mtz_regex,
            pandda_args.ligand_dir_regex,
            pandda_args.ligand_cif_regex,
            pandda_args.ligand_pdb_regex,
            pandda_args.ligand_smiles_regex,
        )()
        pandda_fs_model.build()
        time_fs_model_building_finish = time.time()
        pandda_log["FS model building time"] = time_fs_model_building_finish - time_fs_model_building_start

        if pandda_args.debug >= Debug.AVERAGE_MAPS:
            with open(pandda_fs_model.pandda_dir / "pandda_fs_model.pickle", "wb") as f:
                pickle.dump(pandda_fs_model, f)

        console.summarise_fs_model(pandda_fs_model)
        update_log(pandda_log, pandda_args.out_dir / constants.PANDDA_LOG_FILE)
        if pandda_args.debug >= Debug.PRINT_NUMERICS:
            for dtag, data_dir in pandda_fs_model.data_dirs.dataset_dirs.items():
                print(dtag)
                print(data_dir.source_ligand_cif)
                print(data_dir.source_ligand_smiles)
                print(data_dir.source_ligand_pdb)

        ###################################################################
        # # Load datasets
        ###################################################################
        # Get datasets
        console.start_load_datasets()
        datasets_initial: DatasetsInterface = GetDatasets()(pandda_fs_model, )
        datasets_statistics: DatasetsStatisticsInterface = DatasetsStatistics(datasets_initial)
        console.summarise_datasets(datasets_initial, datasets_statistics)

        if pandda_args.debug >= Debug.PRINT_NUMERICS:
            print(datasets_initial)

        # If structure factors not given, check if any common ones are available
        with STDOUTManager('Looking for common structure factors in datasets...', f'\tFound structure factors!'):
            if not pandda_args.structure_factors:
                potential_structure_factors: Optional[StructureFactorsInterface] = get_common_structure_factors(
                    datasets_initial)
                # If still no structure factors
                if not potential_structure_factors:
                    raise Exception(
                        "No common structure factors found in mtzs. Please manually provide the labels with the --structure_factors option.")
                else:
                    structure_factors: StructureFactorsInterface = potential_structure_factors
            else:
                structure_factors: StructureFactorsInterface = StructureFactors(pandda_args.structure_factors[0],
                                                                                pandda_args.structure_factors[1])

        ###################################################################
        # # Data Quality filters
        ###################################################################
        console.start_data_quality_filters()

        datasets_for_filtering: DatasetsInterface = {dtag: dataset for dtag, dataset in
                                                     datasets_initial.items()}

        datasets_quality_filtered: DatasetsInterface = filter_data_quality(datasets_for_filtering, structure_factors)
        console.summarise_filtered_datasets(
            filter_data_quality.filtered_dtags
        )

        ###################################################################
        # # Truncate columns
        ###################################################################
        datasets_wilson: DatasetsInterface = drop_columns(datasets_quality_filtered, structure_factors)

        ###################################################################
        # # Reference Selection
        ###################################################################
        console.start_reference_selection()

        # Select refernce
        with STDOUTManager('Deciding on reference dataset...', f'\tDone!'):
            reference: ReferenceInterface = GetReferenceDataset()(
                datasets_wilson,
                datasets_statistics,
            )
            pandda_log["Reference Dtag"] = str(reference.dtag)
            console.summarise_reference(reference)
            if pandda_args.debug >= Debug.PRINT_SUMMARIES:
                print(reference.dtag)

            if pandda_args.debug >= Debug.AVERAGE_MAPS:
                with open(pandda_fs_model.pandda_dir / "reference.pickle", "wb") as f:
                    pickle.dump(reference, f)

        ###################################################################
        # # B Factor smoothing
        ###################################################################
        console.start_b_factor_smoothing()

        with STDOUTManager('Performing b-factor smoothing...', f'\tDone!'):
            start = time.time()
            datasets_smoother: DatasetsInterface = {
                smoothed_dtag: smoothed_dataset
                for smoothed_dtag, smoothed_dataset
                in zip(
                    datasets_wilson,
                    process_local(
                        [
                            Partial(smooth_func).paramaterise(
                                dataset,
                                reference,
                                structure_factors,
                            )
                            for dtag, dataset
                            in datasets_wilson.items()
                        ]
                    )
                )
            }

            finish = time.time()
            pandda_log["Time to perform b factor smoothing"] = finish - start

        ###################################################################
        # # Reference compatability filters
        ###################################################################
        console.start_reference_comparability_filters()

        datasets_reference: DatasetsInterface = filter_reference_compatability(datasets_smoother, reference)
        datasets: DatasetsInterface = {dtag: dataset for dtag, dataset in
                                       datasets_reference.items()}
        console.summarise_filtered_datasets(
            filter_reference_compatability.filtered_dtags
        )

        ###################################################################
        # # Getting grid
        ###################################################################
        console.start_get_grid()

        # Grid
        with STDOUTManager('Getting the analysis grid...', f'\tDone!'):
            grid: GridInterface = GetGrid()(reference,
                                            pandda_args.outer_mask,
                                            pandda_args.inner_mask_symmetry,
                                            # sample_rate=pandda_args.sample_rate,
                                            sample_rate=reference.dataset.reflections.get_resolution() / 0.5,
                                            debug=pandda_args.debug
                                            )

            if pandda_args.debug >= Debug.AVERAGE_MAPS:
                with open(pandda_fs_model.pandda_dir / "grid.pickle", "wb") as f:
                    pickle.dump(grid, f)

                grid.partitioning.save_maps(
                    pandda_fs_model.pandda_dir
                )

        ###################################################################
        # # Getting alignments
        ###################################################################

        console.start_alignments()

        with STDOUTManager('Getting local alignments of the electron density to the reference...', f'\tDone!'):
            alignments: AlignmentsInterface = GetAlignments()(
                reference,
                datasets,
            )

            if pandda_args.debug >= Debug.AVERAGE_MAPS:
                with open(pandda_fs_model.pandda_dir / "alignments.pickle", "wb") as f:
                    pickle.dump(alignments, f)

        update_log(pandda_log, pandda_args.out_dir / constants.PANDDA_LOG_FILE)

        ###################################################################
        # # Getting probe position in reference frame
        ###################################################################
        # moving_test_x = pandda_args.test_x
        # moving_test_y = pandda_args.test_y
        # moving_test_z = pandda_args.test_z
        # test_dtag = Dtag(pandda_args.test_dtag)
        with open(pandda_args.sample_json, "r") as f:
            sample_json = json.load(f)
        test_dtags = [Dtag(dtag) for dtag in sample_json["Dtags"]]
        print(f"Test dtags are: {test_dtags}")
        initial_sample_points = {
            sample_key: (Dtag(sample[0]), sample[1], sample[2], sample[3])
            for sample_key, sample
            in sample_json["Sample Points"].items()
        }

        sample_points = {}
        for sample_key, sample in initial_sample_points.items():
            test_x, test_y, test_z = get_test_pos_in_reference_frame(
                sample[1], sample[2], sample[3], datasets[sample[0]], alignments[sample[0]],
            )
            print(f"Probe point is at {(sample[1], sample[2], sample[3])} in native frame and "
                  f"{(test_x, test_y, test_z)} in reference frame")
            sample_points[sample_key] = (test_x, test_y, test_z)

        ###################################################################
        # # Assign comparison datasets
        ###################################################################
        console.start_get_comparators()

        with STDOUTManager('Deciding on the datasets to characterise the groundstate for each dataset to analyse...',
                           f'\tDone!'):
            # TODO: Fix typing for comparators func
            comparators: ComparatorsInterface = comparators_func(
                datasets,
                alignments,
                grid,
                structure_factors,
                pandda_fs_model,
            )

        # if pandda_args.comparison_strategy == "cluster" or pandda_args.comparison_strategy == "hybrid":
        #     pandda_log["Cluster Assignments"] = {str(dtag): int(cluster) for dtag, cluster in
        #                                          cluster_assignments.items()}
        #     pandda_log["Neighbourhood core dtags"] = {int(neighbourhood_number): [str(dtag) for dtag in
        #                                                                           neighbourhood.core_dtags]
        #                                               for neighbourhood_number, neighbourhood
        #                                               in comparators.items()
        #                                               }

        # if pandda_args.debug:
        #     print("Comparators are:")
        # printer.pprint(pandda_log["Cluster Assignments"])
        # printer.pprint(pandda_log["Neighbourhood core dtags"])
        # printer.pprint(comparators)

        update_log(pandda_log, pandda_args.out_dir / constants.PANDDA_LOG_FILE)

        ###################################################################
        # # Process shells
        ###################################################################
        console.start_process_shells()

        # Partition the Analysis into shells in which all datasets are being processed at a similar resolution for the
        # sake of computational efficiency
        with STDOUTManager('Deciding on how to partition the datasets into resolution shells for processing...',
                           f'\tDone!'):
            # if pandda_args.comparison_strategy == "cluster" or pandda_args.comparison_strategy == "hybrid":
            shells: ShellsInterface = get_shells_pandda_analysis(
                datasets,
                comparators,
                pandda_args.min_characterisation_datasets,
                pandda_args.max_shell_datasets,
                3.0,  # min res to analyse to
                pandda_args.high_res_increment,
                pandda_args.only_datasets,
                test_dtags,
                debug=pandda_args.debug,
            )
            # if pandda_args.debug >= Debug.PRINT_SUMMARIES:
            #     print('Got shells that support multiple models')
            #     for shell_res, shell in shells.items():
            #         print(f'\tShell res: {shell.res}: {shell.test_dtags[:3]}')
            #         for cluster_num, dtags in shell.train_dtags.items():
            #             print(f'\t\t{cluster_num}: {dtags[:5]}')

            # else:
            #     shells: ShellsInterface = get_shells(
            #         datasets,
            #         comparators,
            #         pandda_args.min_characterisation_datasets,
            #         pandda_args.max_shell_datasets,
            #         pandda_args.high_res_increment,
            #         pandda_args.only_datasets,
            #
            #     )
            pandda_fs_model.shell_dirs = GetShellDirs()(pandda_fs_model.pandda_dir, shells)
            pandda_fs_model.shell_dirs.build()

        if pandda_args.debug >= Debug.PRINT_NUMERICS:
            printer.pprint(shells)

        # Process the shells
        with STDOUTManager('Processing the shells...', f'\tDone!'):
            time_shells_start = time.time()

            shell_results: ShellResultsInterface = {
                shell_id: shell_result
                for shell_id, shell_result
                in zip(
                    shells,
                    process_global(
                        [
                            Partial(
                                process_shell_multiple_models_pandda_analysis).paramaterise(
                                shell,
                                datasets,
                                alignments,
                                grid,
                                pandda_fs_model,
                                reference,
                                process_local=process_local,
                                structure_factors=structure_factors,
                                sample_rate=pandda_args.sample_rate,
                                contour_level=pandda_args.contour_level,
                                cluster_cutoff_distance_multiplier=pandda_args.cluster_cutoff_distance_multiplier,
                                min_blob_volume=pandda_args.min_blob_volume,
                                min_blob_z_peak=pandda_args.min_blob_z_peak,
                                outer_mask=pandda_args.outer_mask,
                                inner_mask_symmetry=pandda_args.inner_mask_symmetry,
                                max_site_distance_cutoff=pandda_args.max_site_distance_cutoff,
                                min_bdc=pandda_args.min_bdc,
                                max_bdc=pandda_args.max_bdc,
                                memory_availability=pandda_args.memory_availability,
                                statmaps=pandda_args.statmaps,
                                load_xmap_func=load_xmap_func,
                                analyse_model_func=analyse_model_func,
                                score_events_func=score_events_func,
                                sample_points=sample_points,
                                debug=pandda_args.debug,
                            )
                            for res, shell
                            in shells.items()
                        ],
                    )
                )
            }

        results = []
        for shell_id, shell in shells.items():
            shell_result = shell_results[shell_id]
            xmap_samples = shell_result[0]
            zmap_samples = shell_result[1]
            model_sigma_is = shell_result[2]
            model_sigma_sms = shell_result[3]
            dtag_class_dict = shell_result[4]

            for sample_key in sample_points:
                for dtag in xmap_samples[sample_key]:
                    for model_number in model_sigma_is:
                        sigma_i = model_sigma_is[model_number][dtag]
                        sigma_sm = model_sigma_sms[sample_key][model_number]
                        record = {
                            "Resolution": shell.res,
                            "Dtag": dtag.dtag,
                            "Model": model_number,
                            "Sample Point": sample_key,
                            "Electron Density Value": xmap_samples[sample_key][dtag],
                            "ZMap Value": zmap_samples[sample_key][dtag],
                            "Map Uncertainty": sigma_i,
                            "Sample Uncertainty": sigma_sm,
                            "Dtag Class": dtag_class_dict[model_number][dtag]
                        }
                        results.append(record)

        dataframe = pd.DataFrame(results)

        dataframe.to_csv(pandda_fs_model.pandda_dir / "samples.csv")

        #     time_shells_finish = time.time()
        #     pandda_log[constants.LOG_SHELLS] = {
        #         res: shell_result.log
        #         for res, shell_result
        #         in shell_results.items()
        #         if shell_result
        #     }
        #     pandda_log["Time to process all shells"] = time_shells_finish - time_shells_start
        #     if pandda_args.debug >= Debug.PRINT_SUMMARIES:
        #         print(f"Time to process all shells: {time_shells_finish - time_shells_start}")
        #
        # all_events: EventsInterface = {}
        # for res, shell_result in shell_results.items():
        #     if shell_result:
        #         for dtag, dataset_result in shell_result.dataset_results.items():
        #             all_events.update(dataset_result.events)
        #
        # event_scores: EventScoresInterface = {}
        # for res, shell_result in shell_results.items():
        #     if shell_result:
        #         for dtag, dataset_result in shell_result.dataset_results.items():
        #             event_scores.update(
        #                 {
        #                     event_id: event_scoring_result.get_selected_structure_score()
        #                     for event_id, event_scoring_result
        #                     in dataset_result.event_scores.items()
        #                 }
        #             )
        #
        # # Add the event maps to the fs
        # for event_id, event in all_events.items():
        #     pandda_fs_model.processed_datasets.processed_datasets[event_id.dtag].event_map_files.add_event(event)
        #
        # update_log(pandda_log, pandda_args.out_dir / constants.PANDDA_LOG_FILE)
        #
        # if pandda_args.debug >= Debug.PRINT_NUMERICS:
        #     print(shell_results)
        #     print(all_events)
        #     print(event_scores)
        #
        # console.summarise_shells(shell_results, all_events, event_scores)

    ###################################################################
    # # Handle Exceptions
    ###################################################################
    # If an exception has occured, print relevant information to the console and save the log
    except Exception as e:
        # if pandda_args.debug:
        #     printer.pprint(pandda_log)

        console.print_exception()
        console.save(pandda_args.out_dir / constants.PANDDA_TEXT_LOG_FILE)

        pandda_log[constants.LOG_TRACE] = traceback.format_exc()
        pandda_log[constants.LOG_EXCEPTION] = str(e)

        print(f"Saving PanDDA log to: {pandda_args.out_dir / constants.PANDDA_LOG_FILE}")

        save_json_log(
            pandda_log,
            pandda_args.out_dir / constants.PANDDA_LOG_FILE,
        )


if __name__ == '__main__':
    with STDOUTManager('Parsing command line args', '\tParsed command line arguments!'):
        args = PanDDAArgsPanDDAAnalysis.from_command_line()
        print(args)
        print(args.only_datasets)
        console.summarise_arguments(args)

    process_pandda(args)
