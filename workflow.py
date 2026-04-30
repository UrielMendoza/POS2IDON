#!/usr/bin/env python3.9
# -*- coding: utf-8 -*-

"""
Main POS2IDON script.

Atlantic International Research Centre (AIR Centre - EO LAB), Terceira, Azores, Portugal.

@author: AIR Centre
"""

### Pré Start

# Detect single-tile worker mode early. When invoked with `--tile <ID>`, this script
# acts as an isolated worker that processes only that tile. The orchestrator (no flag)
# launches one such subprocess per tile so a crash in one tile cannot break the others.
import sys
_single_tile_mode = None
if "--tile" in sys.argv:
    _idx = sys.argv.index("--tile")
    if _idx + 1 < len(sys.argv):
        _single_tile_mode = sys.argv[_idx + 1]

# Start logging. The orchestrator writes to 4_logfile.log (overwrite); each subprocess
# worker appends to 4_logfile_<tile>.log via its own per-tile logger.
try:
    import logging
    _orchestrator_logfile = "4_logfile.log"
    _orchestrator_filemode = 'a' if _single_tile_mode else 'w'
    logging.basicConfig(filename=_orchestrator_logfile, format="%(asctime)s - %(name)s - %(message)s", filemode=_orchestrator_filemode)
    main_logger = logging.getLogger("main")
    main_logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(message)s"))
    main_logger.addHandler(handler)
    main_logger.info("WELCOME TO POS2IDON (Pipeline for ocean feature detection with Sentinel 2)")
    logging_flag = 1
except Exception as e:
    print(str(e))
    logging_flag = 0

# Import defined modules
try:
    main_logger.info("Importing Defined Modules")
    from modules.Auxiliar import *
    from modules.S2L1CProcessing import *
    from modules.S2L2Processing import *
    from modules.Masking import *
    from modules.SpectralIndices import *
    from modules.Tiling import *
    from modules.Classification import *
    modules_flag = 1
except Exception as e:
    main_logger.info(str(e))
    modules_flag = 0

# Clone important modules from GitHub (FeLS and ACOLITE)
try:
    log_list_0 = git_clone_acolite_fels("configs")
    for log in log_list_0: main_logger.info(log)
    clone_flag = 1
except Exception as e:
    main_logger.info(str(e))
    clone_flag = 0

# Import user inputs
try:
    inputs_flag = 1
    main_logger.info("Importing User Inputs")
    from configs.User_Inputs import *
    # Input checker
    main_logger.info("Checking User Inputs")
    inputs_flag, log_list_5 = input_checker()
    for log in log_list_5: main_logger.info(log)
except Exception as e:
    main_logger.info(str(e))
    inputs_flag = 0

# Import some libraries
try:
    main_logger.info("Importing Libraries")
    import os
    from dotenv import load_dotenv
    import glob
    import time
    import subprocess
    from concurrent.futures import ThreadPoolExecutor, as_completed

    libraries_flag = 1
except Exception as e:
    main_logger.info(str(e))
    libraries_flag = 0

# Import credentials
try:
    main_logger.info("Importing Credentials")
    # Path of .env file
    basepath = os.getcwd()
    env_path = os.path.join(basepath,"configs/Environments/.env")
    if os.path.exists(env_path):
        # Environment variables
        evariables = ("CDSEuser", "CDSEpassword", "TSuser", "TSpassword", "EDuser", "EDpassword")
        load_dotenv(env_path)
        credentials_flag = 1
    else:
        main_logger.info("Check credentials .env file.")
        credentials_flag = 0
except Exception as e:
    main_logger.info(str(e))
    credentials_flag = 0

pre_start_flag = logging_flag * clone_flag * \
    libraries_flag * modules_flag * inputs_flag * credentials_flag # julia_packages_flag *

############################################################################################
def process_tile(current_item):
    """
    Process a single tile (or ROI) end-to-end: search, download, atmospheric correction,
    masking and classification. Each call uses its own logger writing to a per-tile
    log file so tiles can run in parallel without interleaving output.
    """
    # Per-tile logger that writes ONLY to its own file. No StreamHandler so the parent's
    # terminal output stays clean when many tiles run in parallel. Watch progress with:
    #   tail -f 4_logfile_<tile>.log
    tile_logger = logging.getLogger(f"main_{current_item}")
    tile_logger.setLevel(logging.INFO)
    tile_logger.propagate = False
    if not tile_logger.handlers:
        fh = logging.FileHandler(f"4_logfile_{current_item}.log", mode='w')
        fh.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(message)s"))
        tile_logger.addHandler(fh)
    # Use 'main_logger' as the local name so the existing body keeps working
    main_logger = tile_logger

    # Stage tracking: write the current stage to a status file so the parent's
    # dashboard can read it and display real-time progress.
    def _set_stage(stage):
        try:
            with open(f"4_status_{current_item}.txt", "w") as _f:
                _f.write(f"{stage}|{int(time.time())}")
        except Exception:
            pass

    _set_stage("STARTING")

    # Determine search mode and resolve folder paths (locals to avoid UnboundLocalError)
    _search_by = search_by if 'search_by' in globals() else "roi"
    if _search_by == "tile":
        current_tile = current_item
        current_roi = None  # full tile, no ROI clipping in ACOLITE
        s2l1c_products_folder = os.path.join(base_output_dir, f"0_S2L1C_Products_{current_tile}_{sensing_period[0]}")
        ac_products_folder = os.path.join(base_output_dir, f"1_Atmospheric_Corrected_Products_{current_tile}_{sensing_period[0]}")
        masked_products_folder = os.path.join(base_output_dir, f"2_Masked_Products_{current_tile}_{sensing_period[0]}")
        classification_products_folder = os.path.join(base_output_dir, f"3_Classification_Results_{current_tile}_{sensing_period[0]}")
        main_logger.info(f"=== TILE: {current_tile} ===")
    else:
        current_tile = None
        current_roi = roi  # ROI clipping applied in ACOLITE
        from configs.User_Inputs import (
            s2l1c_products_folder, ac_products_folder,
            masked_products_folder, classification_products_folder,
        )

    # SEARCH PRODUCTS ######################################################################
    _set_stage("SEARCHING")
    main_logger.info("SEARCH PRODUCTS")
    if search == True:
        # Create folder to store products
        CreateBrandNewFolder(s2l1c_products_folder)

        # Sensing Period definition
        local_sensing_period = sensing_period
        if nrt_sensing_period == True:
            main_logger.info("Using Yesterday date as Start Date")
            local_sensing_period = NearRealTimeSensingDate()

        # Search products using GC or CDSE
        try:
            if service == "GC":
                if _search_by == "tile":
                    main_logger.info("GC service does not support tile mode search. Skipping.")
                    with open(os.path.join(s2l1c_products_folder, "S2L1CProducts_URLs.txt"), "wt") as f:
                        f.write("")
                else:
                    main_logger.info("Searching for Sentinel-2 L1C products on Google Cloud")
                    log_list_1 = CollectDownloadLinkofS2L1Cproducts_GC(current_roi, local_sensing_period, "configs", s2l1c_products_folder)
                    for log in log_list_1: main_logger.info(log)
            else:
                main_logger.info("Searching for Sentinel-2 L1C products on Copernicus Data Space Ecosystem")
                if _search_by == "tile":
                    log_list_9 = collect_s2l1c_cdse_by_tile(current_tile, local_sensing_period, s2l1c_products_folder)
                else:
                    log_list_9 = collect_s2l1c_cdse(current_roi, local_sensing_period, s2l1c_products_folder)
                for log in log_list_9: main_logger.info(log)
        except Exception as e:
            main_logger.info(str(e))
    else:
        main_logger.info("Search of products ignored")

    # PROCESSING ###########################################################################
    main_logger.info("PROCESSING")
    urls_file = os.path.join(s2l1c_products_folder, "S2L1CProducts_URLs.txt")
    if (processing == True) and os.path.isfile(urls_file):
        # Read S2L1CProducts_URLs.txt file
        urls_list = open(urls_file).read().splitlines()
        if (len(urls_list) == 0) or (urls_list == [""]):
            main_logger.info("No product urls")
        else:
            # Create outputs folders
            if atmospheric_correction == True:
                CreateBrandNewFolder(ac_products_folder)
            if masking == True:
                CreateBrandNewFolder(masked_products_folder)
            if masking_options["use_existing_ESAwc"] == False:
                esa_wc_folder = os.path.join(base_output_dir, "2-1_ESA_Worldcover")
                CreateBrandNewFolder(esa_wc_folder)
            else:
                esa_wc_folder = os.path.join(base_output_dir, "2-1_ESA_Worldcover")
            if classification == True:
                CreateBrandNewFolder(classification_products_folder)

            # Create lists of excluded products names to print in the log file
            excluded_products_old_format = []
            excluded_products_no_data_sensing_time = []
            excluded_products_corrupted = []

            # Filter products URLs
            # In tile mode use tile ID as filter; in roi mode use service_options filter
            if _search_by == "tile":
                urls_list, urls_ignored = filter_safe_products(urls_list, "T" + current_tile)
            else:
                urls_list, urls_ignored = filter_safe_products(urls_list, service_options["filter"])
            if len(urls_ignored) != 0:
                main_logger.info("Some URLs have been ignored, because of filtering option")

            # Start loop on urls list
            for i, url in enumerate(urls_list):
                # Get SAFE file name from url link
                safe_file_name = url.split('/')[-1]
                safe_file_path = os.path.join(s2l1c_products_folder, safe_file_name)
                main_logger.info("(" + str(i+1) +  "/" + str(len(urls_list)) + "): " + safe_file_name)
                _product_label = f"{i+1}/{len(urls_list)}"

                try:
                    # -> Download
                    _set_stage(f"DOWNLOAD {_product_label}")
                    if download == True:
                        # Delete old product that might be corrupted
                        if os.path.exists(safe_file_path):
                            shutil.rmtree(safe_file_path)
                        if service == "GC":
                            main_logger.info("Downloading " + url.split('/')[-1])
                            DownloadTile_from_URL_GC(url, s2l1c_products_folder)
                            # Check if OPER file was excluded
                            if not os.path.exists(safe_file_path):
                                excluded_products_old_format.append(safe_file_name)
                                main_logger.info("The scene is in the redundant OPER old-format (before Nov 2016).Product excluded")
                        else:
                            main_logger.info("Downloading " + url.split('/')[-1])
                            log_list_10 = download_s2l1c_cdse(os.getenv(evariables[0]), os.getenv(evariables[1]), url, s2l1c_products_folder)
                            for log in log_list_10: main_logger.info(log)
                    else:
                        main_logger.info("Download of product ignored")
                except Exception as e:
                    main_logger.info("An error occured during download")

                try:
                    # URL list is the reference for product selection used during processing
                    product_in_urls_list = glob.glob(safe_file_path)
                    if len(product_in_urls_list)==1:
                        product_short_name = Extract_ACOLITE_name_from_SAFE(product_in_urls_list[0])
                        # Product folders
                        ac_product = os.path.join(ac_products_folder, product_short_name)
                        masked_product = os.path.join(masked_products_folder, product_short_name)
                        classification_product = os.path.join(classification_products_folder, product_short_name)
                    else:
                        # SAFE not present - try to find existing AC product from ac_products_folder
                        existing_ac = glob.glob(os.path.join(ac_products_folder, "*_stack.tif"), recursive=False)
                        if not existing_ac:
                            existing_ac = [os.path.join(ac_products_folder, d) for d in os.listdir(ac_products_folder) if os.path.isdir(os.path.join(ac_products_folder, d))]
                        if existing_ac:
                            product_short_name = os.path.basename(os.path.dirname(existing_ac[0])) if existing_ac[0].endswith(".tif") else os.path.basename(existing_ac[0])
                            ac_product = os.path.join(ac_products_folder, product_short_name)
                            masked_product = os.path.join(masked_products_folder, product_short_name)
                            classification_product = os.path.join(classification_products_folder, product_short_name)
                            main_logger.info("SAFE not found, using existing AC product: " + product_short_name)
                        else:
                            product_short_name = "NONE"
                except Exception as e:
                    main_logger.info("Product corrupted. Can't extract short name: " + str(e))
                    excluded_products_corrupted.append(safe_file_name)

                try:
                    # -> Atmospheric Correction
                    _set_stage(f"ACOLITE {_product_label}")
                    if atmospheric_correction == True:
                        if product_short_name != "NONE":
                            main_logger.info("Performing atmospheric correction with ACOLITE")
                            # Apply ACOLITE algorithm
                            try:
                                ACacolite(product_in_urls_list[0], ac_products_folder, os.getenv(evariables[4]), os.getenv(evariables[5]), current_roi)
                                corrupted_flag = 0
                            except Exception as e:
                                corrupted_flag = 1
                                main_logger.info("Product might be corrupted or ACOLITE is not well configured: " + str(e) +
                                                 "\nIf this is the first time running the workflow, try to clone ACOLITE manually or check credentials")
                                # If product corrupted, ACOLITE might stop and text files will remain in main folder
                                for trash_txt in glob.glob(os.path.join(ac_products_folder, "*.txt")):
                                    os.remove(trash_txt)
                            # Organize structure of folders and files
                            log_list_2 = CleanAndOrganizeACOLITE(ac_products_folder, s2l1c_products_folder, safe_file_name)
                            for log in log_list_2: main_logger.info(log)
                            if os.path.exists(ac_product):
                                try:
                                    # Calculate spectral indices
                                    CalculateAllIndexes(ac_product)
                                    # Stack all and delete isolated TIF features
                                    create_features_stack(ac_product, ac_product)
                                    main_logger.info("Spectral indices calculated and stacked with bands")
                                except Exception as e:
                                    main_logger.info("Product corrupted. Not all features are available: " + str(e))
                                    excluded_products_corrupted.append(safe_file_name)
                            elif corrupted_flag == 1:
                                excluded_products_corrupted.append(safe_file_name)
                            else:
                                excluded_products_no_data_sensing_time.append(safe_file_name)
                        else:
                            main_logger.info("There is no S2L1C product to perform atmospheric correction")
                    else:
                        main_logger.info("Atmospheric Correction of product ignored")
                except Exception as e:
                    main_logger.info("An error occured during atmospheric correction: " + str(e))

                try:
                    # -> Masking
                    _set_stage(f"MASKING {_product_label}")
                    if masking == True:
                        if (product_short_name != "NONE") and (os.path.exists(os.path.join(ac_product, product_short_name+"_stack.tif"))):
                            # Only a confirmation that you are reading the right atmospheric corrected product
                            with open(os.path.join(ac_product, "Info.txt")) as text_file:
                                safe_file_name = text_file.read()
                            ac_product_name = os.path.basename(ac_product)
                            main_logger.info("Masking: " + safe_file_name + " (" + ac_product_name + ")")

                            # Reproject previous stack bounds to 4326 and provide geometry
                            ac_product_stack = os.path.join(ac_product, ac_product_name+"_stack.tif")
                            stack_epsg, stack_res, stack_bounds, stack_size = stack_info(ac_product_stack)
                            _, stack_geometry = TransformBounds_EPSG(stack_bounds, int(stack_epsg), TargetEPSG=4326)

                            # -> Water mask with ESA Worldcover
                            if masking_options["use_existing_ESAwc"] == False:
                                # TS credentials
                                ts_user = os.getenv(evariables[2])
                                ts_pass = os.getenv(evariables[3])
                                # Download ESA WorldCover Maps
                                main_logger.info("Downloading WorldCover tile")
                                log_list_3, esa_wc_non_existing = Download_WorldCoverMaps([ts_user, ts_pass], stack_geometry, esa_wc_folder)
                                for log in log_list_3: main_logger.info(log)
                            else:
                                main_logger.info("Download of ESA WorldCover maps ignored")
                                if len(glob.glob(os.path.join(esa_wc_folder, "*.tif"))) == 0:
                                    main_logger.info("2-1_ESA_Worldcover folder is empty, using artificial water mask")
                                    esa_wc_non_existing = True
                                else:
                                    esa_wc_non_existing = False

                            # Create masked product folder and masks folder inside
                            CreateBrandNewFolder(masked_product)
                            masks_folder = os.path.join(masked_product, "Masks")
                            CreateBrandNewFolder(masks_folder)

                            # -> Water Mask
                            main_logger.info("Creating Water mask")
                            log_list_4 = Create_Mask_fromWCMaps(masked_product, esa_wc_folder, stack_epsg, stack_bounds, stack_res[0], esa_wc_non_existing, masking_options["land_buffer"])
                            for log in log_list_4: main_logger.info(log)

                            # -> Features Masks
                            if masking_options["features_mask"] == "NDWI":
                                main_logger.info("Creating NDWI-based mask")
                                Create_Mask_fromNDWI(ac_product, masks_folder, masking_options["threshold_values"][0], masking_options["dilation_values"][0])
                            elif masking_options["features_mask"] == "BAND8":
                                main_logger.info("Creating Band8-based mask")
                                Create_Mask_fromBand8(ac_product, masks_folder, masking_options["threshold_values"][1], masking_options["dilation_values"][1])
                            else:
                                main_logger.info("NDWI-based or Band8-based masking ignored")

                            # -> Cloud Mask
                            if masking_options["cloud_mask"] == True:
                                main_logger.info("Creating Cloud mask")
                                try:
                                    CloudMasking_S2CloudLess_ROI_10m(ac_product, masks_folder, masking_options["cloud_mask_threshold"], masking_options["cloud_mask_average"], masking_options["cloud_mask_dilation"])
                                except Exception as e:
                                    if str(e)[-15:] == "'GetRasterBand'":
                                        main_logger.info("Product corrupted. Bands are missing")
                                        excluded_products_corrupted.append(safe_file_name)
                                    else:
                                        main_logger.info(str(e))
                                    masking_options["cloud_mask"] = False
                            else:
                                main_logger.info("Cloud masking ignored")

                            # Create final mask
                            main_logger.info("Creating Final mask")
                            user_inputs_masks = [masking_options["features_mask"], masking_options["cloud_mask"]]
                            log_list_6, final_mask_path = CreateFinalMask(masked_product, user_inputs_masks)
                            for log in log_list_6: main_logger.info(log)

                            # Apply mask
                            if (classification_options["ml_algorithm"] == "rf") or (classification_options["ml_algorithm"] == "xgb"):
                                # Apply final mask to stack
                                main_logger.info("Masking stack")
                                mask_stack(ac_product, masked_product, filter_ignore_value=0)
                            else:
                                # For UNET apply final mask later
                                main_logger.info("For Unet masking will be applied later")
                                shutil.copy(os.path.join(ac_product, ac_product_name+"_stack.tif"), os.path.join(masked_product, ac_product_name+"_masked_stack.tif"))

                            # Copy info text file
                            info_file_in = os.path.join(ac_product, "Info.txt")
                            info_file_out = os.path.join(masked_product, "Info.txt")
                            shutil.copy(info_file_in, info_file_out)
                        else:
                            main_logger.info("There is no atmospheric corrected product to apply masking")
                    else:
                        main_logger.info("Masking of products ignored")
                except Exception as e:
                    main_logger.info("An error occured during masking: " + str(e))

                try:
                    # -> Classification
                    _set_stage(f"CLASSIFY {_product_label}")
                    if classification == True:
                        if (product_short_name != "NONE") and (os.path.exists(masked_product)):
                            # Only a confirmation that you are reading the right masked product
                            with open(os.path.join(masked_product, "Info.txt")) as text_file:
                                safe_file_name = text_file.read()
                            masked_product_name = os.path.basename(masked_product)
                            masked_file_name = os.path.basename(glob.glob(os.path.join(masked_product, "*.tif"))[0])[:-4]
                            main_logger.info("Classification of: " + safe_file_name + " (" + masked_product_name + ")")

                            # -> Split
                            if classification_options["split_and_mosaic"] == True:
                                main_logger.info("Spliting into 256x256 patches")
                                split_image_with_overlap(masked_product, patch_size=(256,256), overlap=0.5) # overlap of 50%
                            else:
                                main_logger.info("Spliting ignored")

                            # -> Classification selection
                            # Create classification product folder
                            CreateBrandNewFolder(classification_product)
                            main_logger.info("Performing classification")
                            if classification_options["split_and_mosaic"] == True:
                                log_list_7 = create_sc_proba_maps(os.path.join(masked_product, "Patches"), classification_product, classification_options)
                                for log in log_list_7: main_logger.info(log)
                            else:
                                log_list_7 = create_sc_proba_maps(masked_product, classification_product, classification_options)
                                for log in log_list_7: main_logger.info(log)

                            # -> Mosaic
                            if classification_options["split_and_mosaic"] == True:
                                main_logger.info("Performing mosaic of patches")
                                sc_maps_folder = os.path.join(classification_product, "sc_maps")
                                if (classification_options["ml_algorithm"] == "unet"):
                                    final_mosaic_name = masked_product_name + "_stack_unet-scmap_mosaic"
                                    mosaic_patches(sc_maps_folder, sc_maps_folder, final_mosaic_name)
                                    # Apply later mask to Unet mosaic
                                    main_logger.info("Creating Nan mask")
                                    masks_folder = os.path.join(masked_product, "Masks")
                                    Create_Nan_Mask(ac_product, masks_folder)
                                    mask_stack_later(sc_maps_folder, masked_product, filter_ignore_value=0)
                                    main_logger.info("Final mask applied to Unet mosaic (sc_map)")
                                else:
                                    final_mosaic_name = masked_file_name + "_" + classification_options["ml_algorithm"] + "-"
                                    mosaic_patches(sc_maps_folder, sc_maps_folder, final_mosaic_name+"scmap")

                                if classification_options["classification_probabilities"] == True:
                                    proba_maps_folder = os.path.join(classification_product, "proba_maps")
                                    if (classification_options["ml_algorithm"] == "unet"):
                                        final_mosaic_name = masked_product_name + "_stack_unet-probamap_mosaic"
                                        mosaic_patches(proba_maps_folder, proba_maps_folder, final_mosaic_name)
                                        # Apply later mask to Unet mosaic
                                        mask_stack_later(proba_maps_folder, masked_product, filter_ignore_value=0)
                                        main_logger.info("Final mask applied to Unet mosaic (proba_map)")
                                    else:
                                        final_mosaic_name = masked_file_name + "_" + classification_options["ml_algorithm"] + "-"
                                        mosaic_patches(proba_maps_folder, proba_maps_folder, final_mosaic_name+"probamap")
                            else:
                                main_logger.info("Mosaic ignored")

                            # Copy info text file
                            info_file_in = os.path.join(masked_product, "Info.txt")
                            info_file_out = os.path.join(classification_product, "Info.txt")
                            shutil.copy(info_file_in, info_file_out)

                            # Convert final classification map to feather
                            raster_to_feather(os.path.join(classification_product, "sc_maps", masked_file_name + "_" + classification_options["ml_algorithm"] + "-scmap.tif"))
                            main_logger.info("SC map converted to feather")
                        else:
                            main_logger.info("There is no masked product to apply classification")
                    else:
                        main_logger.info("Classification of products ignored")
                except Exception as e:
                    main_logger.info("An error occured during classification: " + str(e))

                # Delete processing folders and files
                try:
                    # -> Delete original products
                    if delete["original_products"] == True:
                        delete_folder(safe_file_path)
                        main_logger.info("Original products deleted")

                    # -> Delete some intermediate
                    if delete["some_intermediate"] == True:
                        delete_intermediate(ac_product, masked_product, classification_product, mode="some")
                        main_logger.info("Some intermediate folders and files deleted")

                    # -> Delete all intermediate
                    if delete["all_intermediate"] == True:
                        delete_intermediate(ac_product, masked_product, classification_product, mode="all")
                        main_logger.info("All intermediate folders and files deleted")
                except Exception as e:
                    main_logger.info("An error occurred while deleting folders and files: " + str(e))

            # Statistics
            number_found_products = len(urls_list)
            number_excluded_products_old_format = len(excluded_products_old_format)
            number_excluded_products_no_data_sensing_time = len(excluded_products_no_data_sensing_time)
            number_excluded_products_corrupted = len(excluded_products_corrupted)
            number_processed_products = number_found_products - (number_excluded_products_old_format + \
            number_excluded_products_no_data_sensing_time + number_excluded_products_corrupted)

            # Products found in ROI for selected Sensing Period
            main_logger.info("Number of products found for selected ROI and Sensing Period: " + str(number_found_products))
            # Products processed in ROI for selected Sensing Period
            main_logger.info("Number of products processed for selected ROI and Sensing Period: " + str(number_processed_products))
            # Products excluded (old format)
            main_logger.info("Number of products excluded (old format): " + str(number_excluded_products_old_format))
            if number_excluded_products_old_format != 0:
                excluded_products_old_format = "\n".join(excluded_products_old_format)
                main_logger.info(excluded_products_old_format)
            # Products excluded (ROI falls 100% on no data side of partial tile or scene have same sensing time)
            main_logger.info("Number of products excluded (100% no data or same sensing time): " + str(number_excluded_products_no_data_sensing_time))
            if number_excluded_products_no_data_sensing_time != 0:
                excluded_products_no_data_sensing_time = "\n".join(excluded_products_no_data_sensing_time)
                main_logger.info(excluded_products_no_data_sensing_time)
            # Corrupted products (some bands or metadata not available during download)
            main_logger.info("Number of corrupted products: " + str(number_excluded_products_corrupted))
            if number_excluded_products_corrupted != 0:
                excluded_products_corrupted = "\n".join(excluded_products_corrupted)
                main_logger.info(excluded_products_corrupted)

    else:
        main_logger.info("Processing ignored")

    _set_stage("DONE")


############################################################################################
# Start POS2IDON main processing time
POS2IDON_time0 = time.time()

# Single-tile worker mode: process exactly one tile and exit. Used by the orchestrator
# below to launch each tile as an isolated subprocess.
if _single_tile_mode is not None:
    if pre_start_flag == 1:
        try:
            process_tile(_single_tile_mode)
            sys.exit(0)
        except Exception as e:
            import traceback
            traceback.print_exc()
            sys.exit(1)
    else:
        print("Failed to pré-start script")
        sys.exit(2)

if pre_start_flag == 1:

    # Determine search mode and build the list of items to iterate over
    _search_by = search_by if 'search_by' in vars() else "roi"
    _tiles = tiles if 'tiles' in vars() else []

    if _search_by == "tile":
        processing_items = _tiles
    else:
        processing_items = [zone_name]

    # Read parallel options (defaults if not present in User_Inputs)
    _parallel = parallel_processing if 'parallel_processing' in vars() else True
    _max_workers = parallel_max_workers if 'parallel_max_workers' in vars() else None
    if _max_workers is None:
        _max_workers = os.cpu_count() or 1
    _max_workers = min(_max_workers, len(processing_items)) if processing_items else 1

    total = len(processing_items)
    completed_count = 0
    failed_count = 0
    item_start_times = {}

    def _format_elapsed(seconds):
        m, s = divmod(int(seconds), 60)
        h, m = divmod(m, 60)
        return f"{h:d}h{m:02d}m{s:02d}s" if h else f"{m:d}m{s:02d}s"

    def _run_tile_subprocess(item):
        """Launch a fully isolated Python subprocess to process one tile."""
        proc = subprocess.run(
            [sys.executable, sys.argv[0], "--tile", item],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            # Append captured stderr to the tile's log file for post-mortem debugging
            try:
                with open(f"4_logfile_{item}.log", "a") as f:
                    f.write(f"\n=== SUBPROCESS FAILED (exit {proc.returncode}) ===\n")
                    if proc.stderr:
                        f.write(proc.stderr)
                    if proc.stdout:
                        f.write("\n--- stdout ---\n" + proc.stdout)
            except Exception:
                pass
            raise RuntimeError(f"exit code {proc.returncode}")
        return item

    import threading
    _dashboard_stop = threading.Event()
    _dashboard_refresh = 30  # seconds between dashboard prints

    def _read_stage(item):
        """Read the current stage of a tile from its status file."""
        path = f"4_status_{item}.txt"
        if not os.path.exists(path):
            return ("PENDING", None)
        try:
            with open(path) as f:
                line = f.read().strip()
            if "|" in line:
                stage, ts = line.split("|", 1)
                return (stage, int(ts))
            return (line, None)
        except Exception:
            return ("?", None)

    def _print_dashboard():
        now = time.time()
        total_elapsed = _format_elapsed(now - POS2IDON_time0)
        lines = []
        lines.append("")
        lines.append(f"=== STATUS @ {time.strftime('%H:%M:%S')}  total {total_elapsed}  ({completed_count} done / {failed_count} failed of {total}) ===")
        for idx, item in enumerate(processing_items, 1):
            stage, stage_ts = _read_stage(item)
            if stage_ts is not None:
                stage_elapsed = _format_elapsed(now - stage_ts)
            else:
                stage_elapsed = "-"
            lines.append(f"  [{idx:>2}/{total}] {item:6s}  {stage:18s}  {stage_elapsed}")
        lines.append("")
        sys.stdout.write("\n".join(lines) + "\n")
        sys.stdout.flush()

    def _dashboard_loop():
        while not _dashboard_stop.wait(_dashboard_refresh):
            try:
                _print_dashboard()
            except Exception:
                pass

    if _parallel and total > 1:
        main_logger.info(f"Running {total} items in parallel with {_max_workers} workers (isolated subprocesses)")
        main_logger.info(f"Per-tile logs: 4_logfile_<tile>.log  (tail -f to follow)")
        main_logger.info(f"Dashboard refreshes every {_dashboard_refresh}s on stdout")

        # Start background dashboard thread
        _dashboard_thread = threading.Thread(target=_dashboard_loop, daemon=True)
        _dashboard_thread.start()

        with ThreadPoolExecutor(max_workers=_max_workers) as ex:
            futures = {}
            for idx, item in enumerate(processing_items, 1):
                fut = ex.submit(_run_tile_subprocess, item)
                futures[fut] = (idx, item)
                item_start_times[item] = time.time()
                main_logger.info(f"[{idx}/{total}] {item} - submitted")

            for fut in as_completed(futures):
                idx, item = futures[fut]
                elapsed = _format_elapsed(time.time() - item_start_times[item])
                try:
                    fut.result()
                    completed_count += 1
                    main_logger.info(f"[{idx}/{total}] {item} - DONE ({elapsed}) [{completed_count} ok / {failed_count} failed]")
                except Exception as e:
                    failed_count += 1
                    main_logger.info(f"[{idx}/{total}] {item} - FAILED ({elapsed}): {e} [{completed_count} ok / {failed_count} failed]")

        # Stop dashboard and print one final snapshot
        _dashboard_stop.set()
        _dashboard_thread.join(timeout=2)
        _print_dashboard()

        # Clean up status files
        for item in processing_items:
            try:
                os.remove(f"4_status_{item}.txt")
            except FileNotFoundError:
                pass
            except Exception:
                pass
    else:
        if _parallel and total <= 1:
            main_logger.info("Only one item to process, running sequentially")
        else:
            main_logger.info(f"Running {total} items sequentially")
        for idx, current_item in enumerate(processing_items, 1):
            item_start_times[current_item] = time.time()
            main_logger.info(f"[{idx}/{total}] {current_item} - started")
            try:
                process_tile(current_item)
                completed_count += 1
                elapsed = _format_elapsed(time.time() - item_start_times[current_item])
                main_logger.info(f"[{idx}/{total}] {current_item} - DONE ({elapsed})")
            except Exception as e:
                failed_count += 1
                elapsed = _format_elapsed(time.time() - item_start_times[current_item])
                main_logger.info(f"[{idx}/{total}] {current_item} - FAILED ({elapsed}): {e}")

    main_logger.info(f"Summary: {completed_count} completed, {failed_count} failed out of {total}")

else:
    print("Failed to pré-start script")

# END ######################################################################################

# Finish time of POS2IDON
POS2IDON_timef = time.time()
# Duration of POS2IDON
POS2IDON_timep = int(POS2IDON_timef - POS2IDON_time0)

main_logger.info("POS2IDON processing time: " + str(POS2IDON_timep) + " seconds")

main_logger.info("POS2IDON CLOSED.")
