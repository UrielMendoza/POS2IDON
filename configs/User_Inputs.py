#!/usr/bin/env python3.9
# -*- coding: utf-8 -*-
"""
User Inputs.
Credentials must be modified in the hidden .env file.

@author: AIR Centre
"""

import os

# OUTPUT LOCATION ##########################################################################

# Base directory where all output folders will be created.
base_output_dir = "/data/tmp/POS2IDON/output/RF_Model_MARIDA"

# Short name identifying the region of interest (used in folder names when search_by = "roi").
zone_name = "TUL"

# SEARCH MODE ##############################################################################

# Search mode:
# "roi" - Search using a polygon region of interest (roi variable below).
#         Folder names use zone_name. Full pipeline clips to the ROI extent.
# "tile" - Search by Sentinel-2 tile ID(s) (tiles variable below).
#          Each tile is processed independently. Full tile is processed (no ROI clipping).
#          Folder names use the tile ID. Only CDSE service is supported in this mode.
# Other inputs besides string will stop the pré-start.
search_by = "tile"

# Tiles grouped into sequential batches. Batches run one at a time; tiles within
# each batch run in parallel. Group light tiles together and heavy tiles together
# so each batch gets a consistent memory budget per worker.
# Batch 1 – lightest  (5 tiles, 2 workers  → 251×0.85/2 ≈ 106 GB each)
# Batch 2 – medium    (5 tiles, 2 workers  → 251×0.85/2 ≈ 106 GB each)
# Batch 3 – heavy     (4 tiles, 2 workers  → 251×0.85/2 ≈ 106 GB each)
# Batch 4 – heaviest  (4 tiles, 1 worker   → 251×0.85/1 ≈ 213 GB each, sequential)
tile_batches = [
    ["16QDG", "16PEC", "16QED", "16PCC", "16PDC"],
    ["16QEE", "16QDD", "16QEF", "16QEH", "16QCF"],
    ["16QDJ", "16QDF", "16QCD", "16QEG"],
    ["16QDE", "16QCE", "16QDH", "16QEJ"],
]

# Number of parallel workers for each batch (one integer per batch).
# Fewer workers per batch → more RAM per tile → fewer OOM retries.
batch_workers = [2, 2, 2, 1]

# Flat tile list derived from batches (used internally and for backward compat).
tiles = [t for batch in tile_batches for t in batch]

# SEARCH ###################################################################################

# Query Sentinel-2 L1C Products in the services catalogue and creates a list of products
# that will be saved inside s2l1c_products_folder.
# If False, this step will be ignored and the products list must exist inside s2l1c_products_folder
# for the download to work.
# Other inputs besides bool will stop the pré-start.
search = True

# Search service:
# "GC" (Google Cloud - better for old data and long term application). Not supported for search_by="tile".
# "CDSE" (Copernicus Data Space Ecosystem - better for recent data and near real time application).
# Other inputs besides string will stop the pré-start.
# Other strings will be considered as "CDSE".
service = "CDSE"

# Search service options (used when search_by = "roi"):
# Other inputs besides dictionary with correct values will stop the pré-start.
                   # Filter specific combination from the S2L1CProducts_URLs.txt,
                   # e.g. "T31UDU", "R094_T31UDU" or "R094"
                   # String, use "" to ignore.

# --- PRUEBA 1: Gulf of Honduras - Marine Debris (18 Sep 2020) ---
# service_options = {"filter": ""}
# roi = {"type":"Polygon","coordinates":[[[-88.308792,15.660726],[-88.308792,15.928978],[-88.040314,15.928978],[-88.040314,15.660726],[-88.308792,15.660726]]]}
# sensing_period = ('20200917', '20200919')

# --- PRUEBA 2: Playa del Carmen, Mexico - Tile 16QDH (10 Apr 2026) ---
# service_options = {"filter": "T16QDH"}
# roi = {"type":"Polygon","coordinates":[[[-87.15,20.50],[-87.15,20.75],[-86.95,20.75],[-86.95,20.50],[-87.15,20.50]]]}
# sensing_period = ('20260410', '20260411')

# --- PRUEBA 3: Playa del Carmen, Mexico - Tile 16QDH (06 Nov 2025) ---
# service_options = {"filter": "T16QDH"}
# roi = {"type":"Polygon","coordinates":[[[-87.15,20.50],[-87.15,20.75],[-86.95,20.75],[-86.95,20.50],[-87.15,20.50]]]}
# sensing_period = ('20251106', '20251107')

# --- PRUEBA 4: Tulum, Mexico - extendido al mar Caribe - Tile 16QDH (06 Nov 2025) ---
service_options = {"filter": "T16QDH"}
roi = {"type":"Polygon","coordinates":[[[-87.60,19.90],[-87.60,20.45],[-86.70,20.45],[-86.70,19.90],[-87.60,19.90]]]}

# List of sensing dates to process in the multi-date batch run.
# Each date is a string 'YYYYMMDD'. The pipeline processes every date sequentially;
# within each date, tiles run in parallel per tile_batches above.
# Dates are grouped by actual calendar year (even if the sargassum season crosses years).
# Results are saved to base_output_dir/YEAR/  (one subfolder per calendar year).
sensing_dates = [
    # 2016
    '20160527', '20160616', '20160706',          # sargazo
    '20161014', '20161103', '20161113',          # no sargazo
    # 2017
    '20170115', '20170201', '20170316',          # no sargazo
    '20170405', '20170502', '20170611',          # sargazo
    # 2018
    '20180427', '20180507', '20180527',          # sargazo
    '20180914', '20181113',                      # no sargazo
    # 2019
    '20190112',                                  # no sargazo (2018 season, calendar 2019)
    '20190502', '20190611', '20190621',          # sargazo
    '20190919', '20191024',                      # no sargazo
    # 2020
    '20200216',                                  # no sargazo (2019 season, calendar 2020)
    '20200610', '20200720', '20200809',          # sargazo
    '20201112',                                  # no sargazo
    # 2021
    '20210106', '20210210',                      # no sargazo (2020 season, calendar 2021)
    '20210411', '20210516', '20210809',          # sargazo
    '20211007', '20211018',                      # no sargazo
    # 2022
    '20220126',                                  # no sargazo (2021 season, calendar 2022)
    '20220605', '20220610', '20220705',          # sargazo
    '20220923', '20221222',                      # no sargazo
    # 2023
    '20230210',                                  # no sargazo (2022 season, calendar 2023)
    '20230526', '20230610', '20230715',          # sargazo
    '20231207', '20231227',                      # no sargazo
    # 2024
    '20240106', '20240111',                      # no sargazo (2023 season, calendar 2024)
    '20240311', '20240425', '20240430', '20240604', '20240619',  # sargazo
    '20241116', '20241201', '20241231',          # no sargazo
    # 2025
    '20250120',                                  # no sargazo (2024 season, calendar 2025)
    '20250306', '20250520', '20250629', '20250818',  # sargazo
    '20251101', '20251226', '20251231',          # no sargazo
    # 2026
    '20260105',                                  # no sargazo (2025 season, calendar 2026)
    '20260301', '20260316', '20260410',          # sargazo
]

# Single sensing_period kept for backward compat (ROI mode, NRT mode, single-date runs).
sensing_period = (sensing_dates[0], sensing_dates[0])

# Near real time sensing period:
# Uses yesterday as start date and today as end date.
# sensing_period is ignored.
# Other inputs besides bool will stop the pré-start.
nrt_sensing_period = False

# PROCESSING ###############################################################################

# Includes download, atmospheric correction, masks and classification.
# URLs file created by SEARCH must exist.
# You can also create a dummy URLs list file named S2L1CProducts_URLs.txt, inside place dummy
# URLs: dummy/NAMEOFPRODUCT1.SAFE
#       dummy/NAMEOFPRODUCT2.SAFE
# The PROCESSING needs to have S2L1C products that correspond to the URLs file, 
# even if you have already atmospheric corrected products.
# Other inputs besides bool will stop the pré-start.
processing = True


# Download is done for each url. Use SEARCH if needed.
# True - Downloads Sentinel-2 L1C products from the services using URLs file. 
# False - Does not download products, it assumes you downloaded already.
# Other inputs besides bool will stop the pré-start.
download = True


# Atmospheric correction of Sentinel-2 L1C Products using ACOLITE. 
# True - AC products inside s2l1c_products_folder.
# False - Does not AC products, it assumes they exist already.
# Other inputs besides bool will stop the pré-start.
atmospheric_correction = True


# Apply masks to the atmospheric corrected product.
# True - Creates masks that are applied or will be applied (UNet) to AC products inside ac_products_folder.
# False - Assumes you have already the masks. Note: This does not mean that application of masks will be ignored.
# Other inputs besides bool will stop the pré-start.
masking = True

# Masking options.
# Other inputs besides dictionary with correct values will stop the pré-start.
# Ignore application of masks (classify entire image) by:
# "use_existing_ESAwc": True and leave 2-1_ESA_Worldcover folder empty.
# "features_mask": None
# "cloud_mask": False
                   # Use existing ESA WorldCover Tiles that are inside 2-1_ESA_Worldcover to create water mask.
                   # If False, it will download the tiles.
masking_options = {"use_existing_ESAwc": False,
                   # Buffer size applied to land, 0 to ignore.
                   "land_buffer": 0, # 50
                   # Apply mask based on 'NDWI' or 'BAND8' (features), None to ignore.
                   "features_mask": None, # NDWI
                   # NDWI and Band8 thresholds
                   "threshold_values": [0, None], #NDWI 0.01 
                   # NDWI and Band8 dilations (number of iteration must be equal or greater then 1)
                   "dilation_values": [6, 2], #6,2
                   # Create cloud mask using s2cloudless, false to ignore.
                   "cloud_mask": True,
                   # Cloud probability threshold, pixels with cloud probability above this threshold are masked as cloudy pixels.
                   "cloud_mask_threshold": 0.05, #0.01 didnt consider a big patch of MD in Honduras. For South Africa needs to be higher value.
                   # Size of the disk in pixels to performe convolution (averaging probability over pixels).
                   "cloud_mask_average": 10, #10
                   # Size of the disk in pixels to performe dilation.
                   "cloud_mask_dilation": 10 #50
                   }


# Perform classification on masked products.
# True - Classification using data from masked_products_folder.
# False - Ignores classification.
# Other inputs besides bool will stop the pré-start.
classification = True

# Classification options.
# Other inputs besides dictionary with correct values will stop the pré-start.                       
                          # Split full image into 256x256 patches and consider each one during classification.
                          # Mosaic all patches into single image after classification.
classification_options = {"split_and_mosaic": False,
                          # Outputs TIF file with the class probability for each pixel.
                          "classification_probabilities": False,
                          # Machine Learning algorithm:
                          # "rf" for Random Forest. 
                          # "xgb" for XGBoost. 
                          # "unet" for Unet. This model needs split_and_mosaic option True. 
                          # Unet Julia is recognized by the model extension .bson  
                          "ml_algorithm": "rf",
                          # Path to the folder containing the machine learning model.
                          "model_path": "configs/MLmodels/RF_Model_Example_MARIDA",
                          # Model_type:
                          # "sk" for original model created with scikit.
                          # "CPUpt" for model converted to pytorch and processed on CPU.
                          # "GPUpt" for model converted to pytorch and processed on GPU. Max size of model limited by GPU memory.
                          # None for Unet models.
                          "model_type": "sk",
                          # Number of classification classes. Change when changing model.
                          "n_classes": 11,
                          # Tuple of features to consider, must match the ones used during model train. Change when changing model.
                          # Features available are ('B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A', 'B11', 'B12', 'NDVI', 'FAI', 'FDI', 'SI', 'NDWI', 'NRD', 'NDMI', 'BSI') or ('B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A', 'B11', 'B12')
                          # For Unet use only the 11 bands as features
                          "features": ('B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A', 'B11', 'B12', 'NDVI', 'FAI', 'FDI', 'SI', 'NDWI', 'NRD', 'NDMI', 'BSI'),
                          # Only used for Unet models. Number of hidden channels.
                          "n_hchannels": 16,
                          # Only used for Unet models. Mean value for each feature.
                          "features_mean": [0.05197577, 0.04783991, 0.04056812, 0.03163572, 0.02972606, 0.03457443, 0.03875053, 0.03436435, 0.0392113,  0.02358126, 0.01588816],
                          # Only used for Unet models. Standard Deviation value for each feature.
                          "features_std": [0.04725893, 0.04743808, 0.04699043, 0.04967381, 0.04946782, 0.06458357, 0.07594915, 0.07120246, 0.08251058, 0.05111466, 0.03524419]
                          }


# PARALLELIZATION ##########################################################################

# Process multiple tiles (or ROIs) in parallel using separate processes.
# Each tile gets its own process and writes to its own log file (4_logfile_<tile>.log).
# True - Run tiles in parallel with ProcessPoolExecutor (recommended for multi-core servers).
# False - Run tiles sequentially (one after another).
# Other inputs besides bool will stop the pré-start.
parallel_processing = True

# Maximum number of worker processes when parallel_processing = True.
# Each tile runs as an isolated Python subprocess, so a crash in one tile
# does not affect the others.
# Use `free -h` to check available RAM and pick a value: ACOLITE may use 3-6 GB
# per worker, so a rough rule is workers ~= total_RAM_GB / 8.
# None - Use os.cpu_count().
# Integer - Use exactly that many workers.
# Other inputs besides None or positive int will stop the pré-start.
parallel_max_workers = 4

# Memory limit per worker process in GB.
# Tiles exceeding this limit are killed and auto-retried with memory_retry_workers.
# Rule: (total_RAM_GB * 0.8) / parallel_max_workers  →  251*0.8/4 ≈ 50 GB.
# Use 70 GB for headroom; watchdog checks every 5 s so spikes are caught fast.
# With 4 workers × 70 GB = 280 GB — slightly over 251 GB in the absolute worst
# case (all 4 at peak simultaneously), but heavy tiles fail fast and free RAM.
# Tiles >70 GB go to retry; typically 4-5 tiles need it (vs 3 with 80 GB).
# None - disables the watchdog (not recommended for parallel runs).
# Other inputs besides None or positive number will stop the pré-start.
memory_limit_per_worker_gb = None  # handled dynamically per batch via batch_workers

# Number of parallel workers used in the AUTO-RETRY phase.
# Retry tiles are heavy (40–90 GB each). Safe max = floor(251 GB / 90 GB) = 2.
# Other inputs besides positive int will stop the pré-start.
memory_retry_workers = 1

# Minimum free RAM (GB) required before starting each OOM retry attempt.
# The retry loop polls psutil.virtual_memory().available every 60 s and waits
# until this threshold is met (max 2 h, then proceeds anyway).
# Set to None to disable the gate (retry immediately regardless of free RAM).
min_free_ram_gb_for_retry = 150

# Tiles that require low-memory ACOLITE settings (dsf_aot_estimate='fixed').
# Use for tiles that exceed server RAM even when running alone.
# 'fixed' uses a constant AOT=0.1 — slightly less accurate but fits in RAM.
# List of tile ID strings, or empty list [] to disable.
# Other inputs besides list of strings will stop the pré-start.
low_memory_tiles = [
    "16QDG", "16QEE", "16QDD", "16QEF",
    "16QEH", "16QDF", "16QEG", "16QDE",
]

# Tiles that require split-and-mosaic classification to avoid OOM during RF predict.
# Use when classifying the full tile at once exceeds available RAM (exit code -9 at CLASSIFY stage).
# These tiles are split into 256x256 patches, each classified independently, then mosaicked.
# List of tile ID strings, or empty list [] to disable.
# Other inputs besides list of strings will stop the pré-start.
low_memory_classify_tiles = ["16QEJ"]

# Tile+date combinations to skip permanently (tile needs more RAM than server can provide).
# Format: {"YYYYMMDD": ["TILE1", "TILE2", ...], ...}
# Skipped tile-dates are logged as SKIP and never retried. Remove entries to force retry.
skip_tile_dates = {}


# Delete processed folders and files:
# Other inputs besides dictionary with correct values will stop the pré-start.
          # Delete original products:
          # Deletes original product after each processing.
delete = {"original_products": False, # True
          # Delete some intermediate after each processing - Recommended:
          # Deletes Surface_Reflectance_Bands, Top_Atmosphere_Bands, masked Patches,
          # Mosaics and single intermediate files in both sc_maps and proba_maps.
          # But DOESN'T, delete atmospheric correction stack, Masks, masked stack.
          "some_intermediate": False, # True
          # Delete all intermediate after each processing - Not Recommended:
          # Only final results available.
          # Deletes Surface_Reflectance_Bands, Top_Atmosphere_Bands, masked Patches,
          # Mosaics and single intermediate files in both sc_maps and proba_maps.
          # BUT ALSO, deletes atmospheric correction stack, Masks, masked stack.
          "all_intermediate": False
          }

# After the entire pipeline finishes, keep ONLY the classification results
# (3_Classification_Results_*). Deletes 0_S2L1C_Products_*, 1_Atmospheric_Corrected_Products_*,
# 2_Masked_Products_* and 2-1_ESA_Worldcover folders inside base_output_dir.
# Useful when disk space is limited and only the final maps matter.
# Other inputs besides bool will stop the pré-start.
keep_only_classification = True


# FOLDERS NAMES ############################################################################

# Download folder:
# Folder where the URLs file and downloaded S2L1C products will be saved.
# Other inputs besides string will stop the pré-start.
s2l1c_products_folder = os.path.join(base_output_dir, f"0_S2L1C_Products_{zone_name}_{sensing_period[0]}")

# Atmospheric correction folder:
# Folder where the S2 atmospherically corrected bands will be saved.
# Other inputs besides string will stop the pré-start.
ac_products_folder = os.path.join(base_output_dir, f"1_Atmospheric_Corrected_Products_{zone_name}_{sensing_period[0]}")

# Masking folder:
# Folder where the masked products will be saved. The water, features and cloud masks will also
# be saved in this folder.
# Other inputs besides string will stop the pré-start.
masked_products_folder = os.path.join(base_output_dir, f"2_Masked_Products_{zone_name}_{sensing_period[0]}")

# Classification folder:
# Folder where the final classification results will be saved.
# Other inputs besides string will stop the pré-start.
classification_products_folder = os.path.join(base_output_dir, f"3_Classification_Results_{zone_name}_{sensing_period[0]}")
