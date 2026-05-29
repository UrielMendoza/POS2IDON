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

_single_date_mode = None
if "--date" in sys.argv:
    _idx = sys.argv.index("--date")
    if _idx + 1 < len(sys.argv):
        _single_date_mode = sys.argv[_idx + 1]

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

    # Tiles that use low-memory ACOLITE settings (dsf_aot_estimate='fixed')
    try:
        from configs.User_Inputs import low_memory_tiles as _low_memory_tiles
    except ImportError:
        _low_memory_tiles = []
    _use_low_memory_acolite = current_item in _low_memory_tiles

    # Tiles that need split-and-mosaic classification to avoid OOM during RF predict.
    # When enabled, the masked stack is split into 256x256 patches before classification.
    try:
        from configs.User_Inputs import low_memory_classify_tiles as _low_memory_classify_tiles
    except ImportError:
        _low_memory_classify_tiles = []
    if current_item in _low_memory_classify_tiles:
        classification_options["split_and_mosaic"] = True

    # Determine the sensing date for this subprocess run.
    # --date YYYYMMDD overrides the global sensing_period from User_Inputs.
    _current_date = _single_date_mode if _single_date_mode else sensing_period[0]
    _current_sensing_period = (_current_date, _current_date)

    # Determine search mode and resolve folder paths (locals to avoid UnboundLocalError)
    _search_by = search_by if 'search_by' in globals() else "roi"
    if _search_by == "tile":
        current_tile = current_item
        current_roi = None  # full tile, no ROI clipping in ACOLITE
        s2l1c_products_folder = os.path.join(base_output_dir, f"0_S2L1C_Products_{current_tile}_{_current_date}")
        ac_products_folder = os.path.join(base_output_dir, f"1_Atmospheric_Corrected_Products_{current_tile}_{_current_date}")
        masked_products_folder = os.path.join(base_output_dir, f"2_Masked_Products_{current_tile}_{_current_date}")
        classification_products_folder = os.path.join(base_output_dir, f"3_Classification_Results_{current_tile}_{_current_date}")
        main_logger.info(f"=== TILE: {current_tile}  DATE: {_current_date} ===")
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
        # Preserve existing downloaded products: only create the folder if it
        # doesn't exist yet. CreateBrandNewFolder would wipe already-downloaded
        # .SAFE files, forcing a re-download during auto-retry (which can fail
        # due to CDSE rate limiting when 2 tiles retry simultaneously).
        os.makedirs(s2l1c_products_folder, exist_ok=True)

        # Sensing Period definition
        local_sensing_period = _current_sensing_period
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
                # Use makedirs instead of CreateBrandNewFolder: all tiles share this
                # folder. Deleting it while a concurrent tile is reading its WorldCover
                # files causes a race condition (masking fails → no classification TIF).
                os.makedirs(esa_wc_folder, exist_ok=True)
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
                                ACacolite(product_in_urls_list[0], ac_products_folder, os.getenv(evariables[4]), os.getenv(evariables[5]), current_roi, low_memory=_use_low_memory_acolite)
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

    # Verify expected output produced. If classification was supposed to run but no
    # *-scmap.tif was generated, treat the whole tile as FAILED instead of silently
    # reporting DONE. This catches cases where errors were caught and logged but no
    # actual result was produced (e.g. masked stack missing, empty input, etc.).
    if processing == True and classification == True:
        produced_tifs = glob.glob(os.path.join(classification_products_folder, "*", "sc_maps", "*-scmap.tif"))
        if not produced_tifs:
            main_logger.info(f"No classification output produced in {classification_products_folder}")
            raise RuntimeError(f"no classification TIF produced for {current_item}")
        else:
            main_logger.info(f"Verified {len(produced_tifs)} classification TIF(s) in {classification_products_folder}")

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
    import threading
    import shutil as _shutil_orch

    def _format_elapsed(seconds):
        m, s = divmod(int(seconds), 60)
        h, m = divmod(m, 60)
        return f"{h:d}h{m:02d}m{s:02d}s" if h else f"{m:d}m{s:02d}s"

    _search_by = search_by if 'search_by' in vars() else "roi"

    def _run_tile_subprocess(item, date_str, memory_limit_gb):
        """Launch a fully isolated Python subprocess to process one tile for one date."""
        cmd = [sys.executable, sys.argv[0], "--tile", item, "--date", date_str]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        _kill_event = threading.Event()
        _kill_reason = [None]

        def _mem_watchdog():
            if memory_limit_gb is None:
                return
            try:
                import psutil
                ps = psutil.Process(proc.pid)
            except Exception:
                return
            while not _kill_event.wait(5):
                if proc.poll() is not None:
                    break
                try:
                    children = ps.children(recursive=True)
                    mem_gb = sum(p.memory_info().rss for p in [ps] + children) / (1024 ** 3)
                    if mem_gb > memory_limit_gb:
                        _kill_reason[0] = f"memory limit {memory_limit_gb:.0f} GB exceeded ({mem_gb:.1f} GB used)"
                        proc.kill()
                        break
                except Exception:
                    break

        watchdog = threading.Thread(target=_mem_watchdog, daemon=True)
        watchdog.start()
        stdout_str, stderr_str = proc.communicate()
        _kill_event.set()
        watchdog.join(timeout=2)

        if _kill_reason[0] or proc.returncode != 0:
            try:
                with open(f"4_logfile_{item}.log", "a") as f:
                    if _kill_reason[0]:
                        f.write(f"\n=== SUBPROCESS KILLED: {_kill_reason[0]} ===\n")
                    else:
                        f.write(f"\n=== SUBPROCESS FAILED (exit {proc.returncode}) ===\n")
                    if stderr_str:
                        f.write(stderr_str)
                    if stdout_str:
                        f.write("\n--- stdout ---\n" + stdout_str)
            except Exception:
                pass
            raise RuntimeError(_kill_reason[0] or f"exit code {proc.returncode}")
        return item

    def _move_and_clean(tile, date_str, year_dir):
        """Move final scmap TIF to year_dir/ and delete all intermediate folders for tile+date."""
        clf_dir = os.path.join(base_output_dir, f"3_Classification_Results_{tile}_{date_str}")
        tifs = glob.glob(os.path.join(clf_dir, "*", "sc_maps", "*-scmap.tif"))
        for tif in tifs:
            dest = os.path.join(year_dir, os.path.basename(tif))
            _shutil_orch.move(tif, dest)
            main_logger.info(f"    Moved {os.path.basename(tif)} → {os.path.basename(year_dir)}/")
        for _pfx in ("0_S2L1C_Products", "1_Atmospheric_Corrected_Products",
                      "2_Masked_Products", "3_Classification_Results"):
            _d = os.path.join(base_output_dir, f"{_pfx}_{tile}_{date_str}")
            if os.path.exists(_d):
                _shutil_orch.rmtree(_d, ignore_errors=True)

    def _needs_retry(err):
        return bool(err) and ("memory" in err.lower() or "exit code -9" in err)

    def _read_stage(item):
        """Read the current stage written by the subprocess to its status file."""
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

    def _print_progress(date_idx, n_dates, current_date, grand_done, grand_total,
                        grand_failed, stage="EN PROCESO"):
        """Print an ASCII progress bar showing date and tile-date advancement."""
        dates_done = date_idx - 1  # fully completed dates before the current one
        pct_dates = dates_done / n_dates * 100 if n_dates else 0
        pct_tiles = grand_done / grand_total * 100 if grand_total else 0
        bar_w = 34
        d_filled = int(bar_w * dates_done / n_dates) if n_dates else 0
        t_filled = int(bar_w * grand_done / grand_total) if grand_total else 0
        d_bar = "█" * d_filled + "░" * (bar_w - d_filled)
        t_bar = "█" * t_filled + "░" * (bar_w - t_filled)
        sep = "─" * 70
        lines = [
            sep,
            f"  Fecha actual : {current_date}  [{date_idx}/{n_dates}]  — {stage}",
            f"  Fechas       : [{d_bar}] {pct_dates:5.1f}%  ({dates_done}/{n_dates} completas)",
            f"  Tile-fechas  : [{t_bar}] {pct_tiles:5.1f}%  ({grand_done}/{grand_total}"
            + (f", {grand_failed} fallidos" if grand_failed else "") + ")",
            sep,
        ]
        sys.stdout.write("\n" + "\n".join(lines) + "\n\n")
        sys.stdout.flush()
        for line in lines:
            main_logger.info(line)

    if _search_by == "tile":
        # ── Multi-date orchestrator — pool único de N workers ─────────────────
        _tile_batches = tile_batches if 'tile_batches' in vars() else [tiles if 'tiles' in vars() else []]
        _sensing_dates_list = sensing_dates if 'sensing_dates' in vars() else [sensing_period[0]]
        _retry_max_workers = memory_retry_workers if 'memory_retry_workers' in vars() else 1
        _n_workers = parallel_max_workers if 'parallel_max_workers' in vars() else 2

        # Lista plana de tiles en orden (de más ligero a más pesado)
        all_tiles = [t for b in _tile_batches for t in b]
        # None → watchdog deshabilitado; pon un valor en GB en User_Inputs para activarlo
        _mem_limit_cfg = memory_limit_per_worker_gb if 'memory_limit_per_worker_gb' in vars() else None
        mem_limit = _mem_limit_cfg

        grand_total = len(_sensing_dates_list) * len(all_tiles)
        grand_done = 0
        grand_failed = 0

        main_logger.info(
            f"Multi-date run: {len(_sensing_dates_list)} fechas × {len(all_tiles)} tiles"
            f" = {grand_total} tile-fechas  ({_n_workers} workers, límite {mem_limit} GB c/u)"
        )

        for date_idx, current_date in enumerate(_sensing_dates_list, 1):
            year = current_date[:4]
            year_dir = os.path.join(base_output_dir, year)
            date_dir = os.path.join(year_dir, current_date)
            os.makedirs(date_dir, exist_ok=True)

            _print_progress(date_idx, len(_sensing_dates_list), current_date,
                            grand_done, grand_total, grand_failed)
            main_logger.info(f"  Resultados → {date_dir}/")

            # Tiles ya completos para esta fecha (tienen TIF en date_dir)
            skipped = [t for t in all_tiles
                       if glob.glob(os.path.join(date_dir, f"*T{t}*-scmap*.tif"))]
            pending  = [t for t in all_tiles if t not in skipped]

            if skipped:
                main_logger.info(f"  {len(skipped)} tile(s) ya completos: {' '.join(skipped)}")
                grand_done += len(skipped)

            date_done   = len(skipped)
            date_failed = 0

            if not pending:
                _print_progress(date_idx, len(_sensing_dates_list), current_date,
                                grand_done, grand_total, grand_failed,
                                stage=f"COMPLETA  ✓{date_done}")
                continue

            n_workers      = min(_n_workers, len(pending))
            eff_mem_limit  = None if len(pending) == 1 else mem_limit

            main_logger.info(
                f"  {len(pending)} tile(s) pendientes, {n_workers} workers,"
                f" límite {eff_mem_limit} GB c/u"
            )

            # Limpiar status files de corridas anteriores
            for tile in pending:
                try:
                    os.remove(f"4_status_{tile}.txt")
                except FileNotFoundError:
                    pass

            item_start_times = {tile: time.time() for tile in pending}
            tile_status = {}   # tile → string final "DONE ..." / "FAILED ..."
            tile_results = {}  # tile → {"status":..., "error":...}

            def _dashboard():
                now = time.time()
                header = (
                    f"\n  [{current_date}  {time.strftime('%H:%M:%S')}"
                    f"  {n_workers} workers  límite {eff_mem_limit} GB]"
                )
                rows = []
                for idx, t in enumerate(all_tiles, 1):
                    if t in skipped:
                        rows.append(f"    [{idx:>2}] {t:8s}  DONE (previa)")
                    elif t in tile_status:
                        rows.append(f"    [{idx:>2}] {t:8s}  {tile_status[t]}")
                    else:
                        stage, ts = _read_stage(t)
                        age = (_format_elapsed(now - ts) if ts
                               else _format_elapsed(now - item_start_times.get(t, now)))
                        rows.append(f"    [{idx:>2}] {t:8s}  {stage:<22s}  {age}")
                sys.stdout.write(header + "\n" + "\n".join(rows) + "\n")
                sys.stdout.flush()

            _dash_stop = threading.Event()

            def _dash_loop():
                _last = {}
                while not _dash_stop.wait(5):
                    try:
                        changed = False
                        for t in pending:
                            cur = tile_status.get(t) or _read_stage(t)[0]
                            if _last.get(t) != cur:
                                _last[t] = cur
                                changed = True
                        if changed:
                            _dashboard()
                    except Exception:
                        pass

            _dash_thread = threading.Thread(target=_dash_loop, daemon=True)
            _dash_thread.start()

            with ThreadPoolExecutor(max_workers=n_workers) as ex:
                futures = {
                    ex.submit(_run_tile_subprocess, tile, current_date, eff_mem_limit): tile
                    for tile in pending
                }
                for tile in pending:
                    main_logger.info(f"  Submitted: {tile}")

                for fut in as_completed(futures):
                    tile = futures[fut]
                    elapsed_s = time.time() - item_start_times[tile]
                    try:
                        fut.result()
                        tile_results[tile] = {"status": "DONE", "error": None}
                        date_done  += 1
                        grand_done += 1
                        tile_status[tile] = f"DONE ({_format_elapsed(elapsed_s)})"
                        main_logger.info(f"  {tile} DONE ({_format_elapsed(elapsed_s)})")
                        try:
                            _move_and_clean(tile, current_date, date_dir)
                        except Exception as _ce:
                            main_logger.info(f"  WARNING: cleanup failed for {tile}: {_ce}")
                    except Exception as e:
                        tile_results[tile] = {"status": "FAILED", "error": str(e)}
                        date_failed  += 1
                        grand_failed += 1
                        tile_status[tile] = f"FAILED ({_format_elapsed(elapsed_s)})"
                        main_logger.info(f"  {tile} FAILED ({_format_elapsed(elapsed_s)}): {e}")

            _dash_stop.set()
            _dash_thread.join(timeout=2)
            _dashboard()  # snapshot final

            # Auto-retry: tiles que fallaron por memoria, sin límite, 1 a la vez
            retry_tiles = [
                t for t, r in tile_results.items()
                if r["status"] == "FAILED" and _needs_retry(r["error"])
            ]
            if retry_tiles:
                main_logger.info(f"  AUTO-RETRY {len(retry_tiles)} tile(s): {' '.join(retry_tiles)}")
                for tile in retry_tiles:
                    for _pfx in ("0_S2L1C_Products", "1_Atmospheric_Corrected_Products",
                                 "2_Masked_Products"):
                        _d = os.path.join(base_output_dir, f"{_pfx}_{tile}_{current_date}")
                        if os.path.exists(_d):
                            _shutil_orch.rmtree(_d, ignore_errors=True)
                    date_failed  -= 1
                    grand_failed -= 1

                rw = min(_retry_max_workers, len(retry_tiles))
                item_start_times.update({tile: time.time() for tile in retry_tiles})
                with ThreadPoolExecutor(max_workers=rw) as ex:
                    futures = {
                        ex.submit(_run_tile_subprocess, tile, current_date, None): tile
                        for tile in retry_tiles
                    }
                    for fut in as_completed(futures):
                        tile = futures[fut]
                        elapsed_s = time.time() - item_start_times[tile]
                        try:
                            fut.result()
                            tile_results[tile] = {"status": "DONE", "error": "(retried)"}
                            date_done  += 1
                            grand_done += 1
                            tile_status[tile] = f"DONE-retry ({_format_elapsed(elapsed_s)})"
                            main_logger.info(f"  RETRY {tile} DONE ({_format_elapsed(elapsed_s)})")
                            try:
                                _move_and_clean(tile, current_date, date_dir)
                            except Exception as _ce:
                                main_logger.info(f"  WARNING: cleanup failed for {tile}: {_ce}")
                        except Exception as e:
                            tile_results[tile]["error"] = f"retry failed: {e}"
                            date_failed  += 1
                            grand_failed += 1
                            tile_status[tile] = f"RETRY-FAILED ({_format_elapsed(elapsed_s)})"
                            main_logger.info(f"  RETRY {tile} FAILED ({_format_elapsed(elapsed_s)}): {e}")

            # Limpiar status files
            for tile in pending:
                try:
                    os.remove(f"4_status_{tile}.txt")
                except FileNotFoundError:
                    pass

            _print_progress(date_idx, len(_sensing_dates_list), current_date,
                            grand_done, grand_total, grand_failed,
                            stage=f"COMPLETA  ✓{date_done}"
                                  + (f"  ✗{date_failed}" if date_failed else ""))

        # Clean shared ESA WorldCover folder after all dates are done
        esa_wc_folder = os.path.join(base_output_dir, "2-1_ESA_Worldcover")
        if os.path.exists(esa_wc_folder):
            try:
                _shutil_orch.rmtree(esa_wc_folder)
                main_logger.info("Removed 2-1_ESA_Worldcover after all dates completed")
            except Exception as _e:
                main_logger.info(f"WARNING: could not remove ESA WorldCover: {_e}")

        # Final summary
        main_logger.info("")
        main_logger.info("=" * 70)
        main_logger.info("FINAL SUMMARY")
        main_logger.info("=" * 70)
        main_logger.info(f"Total time:      {_format_elapsed(time.time() - POS2IDON_time0)}")
        main_logger.info(f"Dates processed: {len(_sensing_dates_list)}")
        main_logger.info(f"Total completed: {grand_done}/{grand_total}")
        main_logger.info(f"Total failed:    {grand_failed}/{grand_total}")
        main_logger.info("=" * 70)

    else:
        # ── ROI / single-zone mode ──────────────────────────────────────────
        current_item = zone_name
        main_logger.info(f"ROI mode: processing zone '{current_item}' for {sensing_period}")
        try:
            process_tile(current_item)
            main_logger.info(f"{current_item} - DONE")
        except Exception as e:
            main_logger.info(f"{current_item} - FAILED: {e}")

else:
    print("Failed to pré-start script")

# END ######################################################################################

# Finish time of POS2IDON
POS2IDON_timef = time.time()
# Duration of POS2IDON
POS2IDON_timep = int(POS2IDON_timef - POS2IDON_time0)

main_logger.info("POS2IDON processing time: " + str(POS2IDON_timep) + " seconds")

main_logger.info("POS2IDON CLOSED.")
