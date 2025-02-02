# Name: SNODASDaily_Interactive.py
# Version: 1
# Author: Emma Giles
# Organization: Open Water Foundation
#
# Purpose: This script outputs zonal statistics for historical daily SNODAS rasters given the basin boundaries of an
# input basin boundary shapefile. The zonal statistics that are calculated are as follows: SWE mean, SWE minimum, SWE
# maximum, SWE standard deviation, pixel count and percentage of snow coverage. The functions in this script are
# housed within SNODAS_utilities.py. A more detailed description of each function is documented in the
# SNODAS_utilities.py file. This script allows the user to interactively input historical dates of interest for
# processing. The user can decide between a single date and a range of dates. This script does not allow for the user
# to pick a list of non-sequential dates.

# Check to see which os is running
# Import necessary modules
import argparse
from OpenWaterFoundationScripts import SNODAS_utilities
import configparser
import logging
import os
import sys
import time

from datetime import datetime, timedelta
from logging.config import fileConfig
from pathlib import Path

from qgis.core import QgsApplication

# Read the config file to assign variables. Reference the following for code details:
# https://wiki.python.org/moin/ConfigParserExamples
if sys.platform == 'linux' or sys.platform == 'linux2' or sys.platform == 'cygwin' or sys.platform == 'darwin':
    LINUX_OS = True
else:
    LINUX_OS = False

CONFIG = configparser.ConfigParser()
# Check for a testing VM folder structure.
if Path('../test-CDSS/config/SNODAS-Tools-Config.ini').exists():
    CONFIG_FILE = '../test-CDSS/config/SNODAS-Tools-Config.ini'
# If it doesn't exist, assume a production VM folder structure.
else:
    CONFIG_FILE = '../config/SNODAS-Tools-Config.ini'
CONFIG.read(CONFIG_FILE)


def config_map(section: str) -> dict:
    """ Helper function to obtain option values of config file sections. """
    dict1 = {}
    options = CONFIG.options(section)
    for option in options:
        try:
            dict1[option] = CONFIG.get(section, option)
            if dict1[option] == -1:
                print('Skip: {}'.format(option))
        except Exception as e:
            print('Exception on {}: {}'.format(option, e))
            dict1[option] = None
    return dict1

# Assign the values from the configuration file to the python variables. See below for description of each
# variable obtained by the configuration file.
#
# SNODAS_ROOT: The SNODAS_ROOT folder is the folder housing all output files from this script.
#
# BASIN_SHP_PATH: This is the full pathname to the shapefile holding the vector input(watershed basin boundaries).
# The zonal statistics will be calculated for each feature of this layer. The shapefile must be projected into
# NAD83 Zone 13N. This script was originally developed to process Colorado Watershed Basins as the input shapefile.
#
# QGIS_HOME: The full path to QGIS installation on the local machine. This is used to initialize QGIS
# resources & utilities. More info at: http://docs.qgis.org/testing/en/docs/pyqgis_developer_cookbook/intro.html.
#
# SAVE_ALL_SNODAS_PARAMS: Each daily downloaded .tar SNODAS file contains 8 parameters as follows: (1) Snow Water
# Equivalent (2) Snow Depth (3) Snow Melt Runoff at the Base of the Snow Pack (4) Sublimation from the Snow Pack
# (5) Sublimation of Blowing Snow (6) Solid Precipitation (7) Liquid Precipitation and (8) Snow Pack Average
# Temperature. The default setting, False, will delete all parameters except 'Snow Water Equivalent'. The optional
# setting, True, will move all other parameters to a sub-folder under 1_Download called 'OtherParameters'
#
# DOWNLOAD_FOLDER: The name of the download folder. Defaulted to '1_DownloadSNODAS'. All downloaded SNODAS
# .tar files are contained here.
#
# SET_FORMAT_FOLDER: The name of the setFormat folder. Defaulted to ' 2_SetFormat'. All SNODAS masked .tif
# files are contained here.
#
# CLIP_FOLDER: The name of the clip folder. Defaulted to '3_ClipToExtnet'. All SNODAS clipped (to basin
# extent)and reprojected .tif files are contained here.
#
# CREATE_SNOW_COVER_FOLDER: The name of the snow cover folder. Defaulted to '4_CreateSnowCover'. All binary
# snow cover .tif files (clipped to basin extent) are contained here.
#
# CALCULATE_STATS_FOLDER: The name of the results folder. All zonal statistic results in csv format are
# contained here.
#
# OUTPUT_STATS_BY_DATE_FOLDER: The name of the byDate folder. All zonal statistic results in csv format organized by
# date are contained here.
#
# OUTPUT_STATS_BY_BASIN_FOLDER: The name of the byBasin folder. All zonal statistic results in csv format organized by
# basin are contained here.


# QGIS_HOME = config_map('ProgramInstall')['qgis_pathname']

SNODAS_ROOT: str = config_map('Folders')['root_dir']
# SHARED_ROOT_DIR: str = config_map('Folders')['shared_root_dir']
STATIC_FOLDER: str = config_map('Folders')['static_data_dir']
PROCESSED_FOLDER: str = config_map('Folders')['processed_data_folder']
# SHARED_PROCESSED_DATA_DIR: str = config_map('Folders')['shared_processed_data_dir']
DOWNLOAD_FOLDER: str = config_map('Folders')['download_snodas_tar_folder']
SET_FORMAT_FOLDER: str = config_map('Folders')['untar_snodas_tif_folder']
CLIP_FOLDER: str = config_map('Folders')['clip_proj_snodas_tif_folder']
CREATE_SNOW_COVER_FOLDER: str = config_map('Folders')['create_snowcover_tif_folder']
CALCULATE_STATS_FOLDER: str = config_map('Folders')['calculate_stats_folder']
OUTPUT_STATS_BY_DATE_FOLDER: str = config_map('Folders')['output_stats_by_date_folder']
OUTPUT_STATS_BY_BASIN_FOLDER: str = config_map('Folders')['output_stats_by_basin_folder']

BASIN_SHP_PATH: str = config_map('BasinBoundaryShapefile')['pathname']

OUTPUT_CRS_EPSG: str = config_map('Projections')['output_proj_epsg']

DEV_ENVIRONMENT: str = config_map('OutputLayers')['dev_environment']
SHP_ZIP: str = config_map('OutputLayers')['shp_zip']
DEL_SHP_ORIG: str = config_map('OutputLayers')['shp_delete_originals']
UPLOAD_TO_S3: str = config_map('OutputLayers')['upload_to_s3']
UPLOAD_TO_GCP: str = config_map('OutputLayers')['gcp_upload']
RUN_DAILY_TSTOOL: str = config_map('OutputLayers')['process_daily_tstool_graphs']
RUN_HIST_TSTOOL: str = config_map('OutputLayers')['process_historical_tstool_graphs']

SAVE_ALL_SNODAS_PARAMS: str = config_map('SNODASParameters')['save_all_parameters']


def arg_parse() -> None:
    """ Parse command line arguments. Currently implemented options are:\n

     -h, --help: Displays help information for the program.\n
     --version: Displays SNODAS Tools version."""

    parser = argparse.ArgumentParser(prog='SNODAS Tools', description='SNODAS Tools.')

    parser.add_argument('--version', action='version', version='%(prog)s 2.0.0')
    parser.parse_args()


if __name__ == '__main__':

    arg_parse()
    # Initialize QGIS resources: more info at
    # http://docs.qgis.org/testing/en/docs/pyqgis_developer_cookbook/intro.html. This block of code allows for the
    # script to utilize QGIS functionality.
    # QgsApplication.setPrefixPath(QGIS_HOME, True)
    QgsApplication.setPrefixPath('/usr', True)
    qgs = QgsApplication([], False)
    qgs.initQgis()

    # Define static folder and extent shapefile
    static_path: Path = Path(SNODAS_ROOT) / STATIC_FOLDER
    extent_shapefile: Path = static_path / 'studyAreaExtent_prj.shp'

    # Obtain today's date
    now = datetime.now()

    # Get user inputs as raw data
    # Ask the user to decide between downloading only one date of data or a range of multiple dates of data.
    singleOrRange = input('\n))> Are you interested in one date or a range of dates? ["one" or "range"]: ')

    # While loop that continues to prompt user for a new input if original input is invalid.
    while singleOrRange.upper() != 'ONE' and singleOrRange.upper() != 'RANGE':

        # Ask the user to re-enter downloading data type - one date or range of dates.
        singleOrRange = input('\n))> Your input is not recognized. Are you interested in one date or a range '
                              'of dates? ["one" or "range"]: ')

    # Create the SNODAS_ROOT folder and the 5 sub-folders (Defaulted to: 1_DownloadSNODAS, 2_SetFormat, 3_ClipToExtent,
    # 4_CreateSnowCover, 5_CalculateStatistics [2 sub-folders: StatisticsByBasin, StatisticsByDate]). Check for folder
    # existence. If exiting, the folder is not recreated. Refer to developer documentation for information regarding
    # the types of files contained within each folder.
    download_path = Path(SNODAS_ROOT) / PROCESSED_FOLDER / DOWNLOAD_FOLDER
    set_format_path = Path(SNODAS_ROOT) / PROCESSED_FOLDER / SET_FORMAT_FOLDER
    clip_path = Path(SNODAS_ROOT) / PROCESSED_FOLDER / CLIP_FOLDER
    snow_cover_path = Path(SNODAS_ROOT) / PROCESSED_FOLDER / CREATE_SNOW_COVER_FOLDER
    results_basin_path = Path(SNODAS_ROOT) / PROCESSED_FOLDER / (CALCULATE_STATS_FOLDER + OUTPUT_STATS_BY_BASIN_FOLDER)
    results_date_path = Path(SNODAS_ROOT) / PROCESSED_FOLDER / (CALCULATE_STATS_FOLDER + OUTPUT_STATS_BY_DATE_FOLDER)

    all_folders = [SNODAS_ROOT, download_path, set_format_path, clip_path, snow_cover_path, results_basin_path,
                   results_date_path]

    for folder in all_folders:
        if not os.path.exists(folder):
            os.makedirs(folder)

    # Create and configures logging file
    fileConfig(CONFIG_FILE)
    logger = logging.getLogger('interactive')
    logger.info('SNODASDailyDownload.py: Started \n')

    # Print version information
    print('Running SNODASDaily_Interactive.py Version 1')
    logger.info('Running SNODASDaily_Interactive.py Version 1\n')

    singleDate = None

    # After the user chooses the project folder where the data will be stored, they must then choose the dates of
    # interest. If the user is interested in only one date of data.
    if singleOrRange.upper() == 'ONE':

        # Ask the user which date of data they would like. Must be in mm/dd/yy format and be within the specified
        # range of available dates. Continues to ask the user for a date if the entered string is not a valid date with
        # correct formatting or if the date chosen is outside the range of available data.
        while True:
            try:
                userIn = input(
                    "\n))> Which date are you interested in? Date must be on or between "
                    "\n))> 30 September 2003 and today's date in the form 'mm/dd/yy': ")
                singleDate = datetime.strptime(userIn, '%m/%d/%y')
                if not (datetime(year=2003, month=9, day=29) < singleDate <= now):
                    print('\n))> You have chosen an invalid date.')
                else:
                    startDate = singleDate
                    endDate = singleDate
                    break
            except ValueError:
                print('\n))> Invalid Format!')

    # If the user is interested in a range of multiple dates.
    else:
        # Ask the user which START date they would like. Must be in mm/dd/yy format and be within the specified
        # range of available dates. Continue to ask the user for a date if the entered string is not a valid date with
        # correct formatting or if the date chosen is outside the range of available data.
        while True:
            try:
                userIn = input(
                    "\n))> What is the STARTING (earliest) date of data that you are interested in?"
                    "\n))> The date must be of or between 30 September 2003 and today's date. mm/dd/yy: ")
                startDate = datetime.strptime(userIn, '%m/%d/%y')
                if not (datetime(year=2003, month=9, day=29) < startDate <= now):
                    print('\n))> You have chosen an invalid date.')
                else:
                    break
            except ValueError:
                print('\n))> Invalid Format!')

        # Ask the user which END date they would like. Must be in mm/dd/yy format and be within the specified
        # range of available dates. Continue to ask the user for a date if the entered string is not a valid date with
        # correct formatting or if the date chosen is outside the range of available data.
        while True:
            try:
                userIn = input(
                    "\n))> What is the ENDING (most recent) date of data that you are interested in?"
                    "\n))> The date must be between {} and today's date. mm/dd/yy: ".format(startDate.date()))
                endDate = datetime.strptime(userIn, '%m/%d/%y')
                if not (startDate < endDate <= now):
                    print('\n You have chosen an invalid date.')
                else:
                    break
            except ValueError:
                print('\n))> Invalid Format!')

    # ----------------- End of user input. Begin automated script.------------------------------------------------------

    # The start time is used to calculate the elapsed time of the running script. The elapsed time will be displayed at
    # the end of the log file.
    start = time.time()

    # Create an empty list that will contain all dates that failed to download.
    failed_dates_lst = []

    # Iterate through each day of the user-specified range. Refer to:
    # http://stackoverflow.com/questions/6901436/python-expected-an-indented-block
    total_days = (endDate - startDate).days + 1

    # Define the current day depending on the user's interest in one or range of dates.
    for day_number in range(total_days):
        if singleOrRange.upper() == 'ONE':
            if singleDate is None:
                logger.warning('\n singleDate has not be defined. Using the default day 2003-09-30.')
                singleDate = datetime(year=2003, month=9, day=30)
            current = singleDate
        else:
            current = (startDate + timedelta(days=day_number)).date()

        # The start time is used to calculate the elapsed time of the running script. The elapsed time will be
        # displayed at the end of the log file.
        start_day = time.time()

        # Format date into string with format YYYYMMDD
        current_date = SNODAS_utilities.format_date_yyyymmdd(current)
        current_date_tar = 'SNODAS_' + current_date + '.tar'

        # Check to see if this date of data has already been processed in the folder
        possible_file = download_path / current_date_tar

        # If date has already been processed within the folder, the download & zonal statistics are rerun.
        if possible_file.exists():
            logger.info('This date ({}) has already been processed. The files will be reprocessed and '
                        'rewritten.\n'.format(current_date))

        # Download current date SNODAS .tar file from the FTP site at
        # ftp://sidads.colorado.edu/DATASETS/NOAA/G02158/masked/
        returnedList = SNODAS_utilities.download_snodas(download_path, current)

        failed_dates_lst.append(returnedList[2])

        # Check to see if configuration values for optional statistics 'calculate_SWE_minimum, calculate_SWE_maximum,
        # calculate_SWE_stdDev' (defined in utility function) are valid, If valid, script continues to run. If invalid,
        # error message is printed to console and the log file and script is terminated.
        valid_list = []
        for stat in returnedList[1]:
            if stat.upper() == 'TRUE' or stat.upper() == 'FALSE':
                valid_list.append(1)
            else:
                valid_list.append(0)

        if 0 in valid_list:
            logger.error(
                'See configuration file. One or more values of the "OptionalZonalStatistics" section is '
                'not valid. Please change values to either "True" or "False" and rerun the script.', exc_info=True)
            break

        else:
            # Untar current date's data.
            for file in SNODAS_utilities.list_dir(download_path, '*.tar'):
                if current_date in str(file):
                    SNODAS_utilities.untar_snodas_file(file, download_path, set_format_path)

            # Check to see if configuration file 'SAVE_ALL_SNODAS_PARAMS' value is valid. If valid, script continues
            # to run. If invalid, error message is printed to console and log file and the script is terminated.
            if SAVE_ALL_SNODAS_PARAMS.upper() == 'FALSE' or SAVE_ALL_SNODAS_PARAMS.upper() == 'TRUE':

                # Delete current date's irrelevant files (parameters other than SWE).
                if SAVE_ALL_SNODAS_PARAMS.upper() == 'FALSE':
                    for file in os.listdir(set_format_path):
                        if current_date in file:
                            SNODAS_utilities.delete_irrelevant_snodas_files(file)
                    logger.info('Finished removing files.\n')

                # Move irrelevant files (parameters other than SWE) to 'OtherSNODASParameters'.
                else:
                    parameter_path = set_format_path / 'OtherParameters'
                    if not parameter_path.exists():
                        parameter_path.mkdir()
                    for file in os.listdir(set_format_path):
                        if current_date in file:
                            SNODAS_utilities.move_irrelevant_snodas_files(file, parameter_path)

                # Extract current date's .gz files. Each SNODAS parameter files are zipped within a .gz file.
                for file in SNODAS_utilities.list_dir(set_format_path, '*.gz'):
                    if current_date in str(file):
                        SNODAS_utilities.extract_snodas_gz_file(file)

                # Convert current date's SNODAS SWE .dat file into .bil format.
                for file in SNODAS_utilities.list_dir(set_format_path, '*.dat'):
                    if current_date in str(file):
                        SNODAS_utilities.convert_snodas_dat_to_bil(file)

                # Create current date's custom .Hdr file. In order to convert today's SNODAS SWE .bil file into a usable
                # .tif file, a custom .Hdr must be created. Refer to the function in the SNODAS_utilities.py for more
                # information on the contents of the custom .HDR file.
                for file in SNODAS_utilities.list_dir(set_format_path, '*.bil'):
                    if current_date in str(file):
                        SNODAS_utilities.create_snodas_hdr_file(str(file))

                # Convert current date's .bil files to .tif files
                for file in SNODAS_utilities.list_dir(set_format_path, '*.bil'):
                    if current_date in str(file):
                        SNODAS_utilities.convert_snodas_bil_to_tif(str(file), set_format_path)

                # Delete current date's .bil and .hdr files
                for file in SNODAS_utilities.list_dir(set_format_path, ('*.bil', '*.hdr', '*.Hdr'),
                                                      multiple_types=True):
                    if current_date in str(file):
                        SNODAS_utilities.delete_snodas_files(file)

                # Create the extent shapefile if not already created.
                if not extent_shapefile.exists():
                    SNODAS_utilities.create_extent(BASIN_SHP_PATH, static_path)

                # Copy and move current date's .tif file into CLIP_FOLDER
                for file in SNODAS_utilities.list_dir(set_format_path, '*.tif'):
                    if current_date in str(file):
                        SNODAS_utilities.copy_and_move_snodas_tif_file(file, clip_path)

                # Assign datum to current date's .tif file (defaulted to WGS84)
                for file in os.listdir(clip_path):
                    if current_date in file:
                        SNODAS_utilities.assign_snodas_datum(file, clip_path)

                # Clip current date's .tif file to the extent of the basin shapefile
                for file in os.listdir(clip_path):
                    if current_date in file:
                        SNODAS_utilities.snodas_raster_clip(file, clip_path, extent_shapefile)

                # Project current date's .tif file into desired projection (defaulted to NAD83 UTM Zone 13N)
                for file in os.listdir(clip_path):
                    if current_date in file:
                        SNODAS_utilities.assign_snodas_projection(file, clip_path)

                # Create current date's snow cover binary raster
                for file in os.listdir(clip_path):
                    if current_date in file:
                        SNODAS_utilities.snow_coverage(file, clip_path, snow_cover_path)

                # Create .csv files of byBasin and byDate outputs
                for file in os.listdir(clip_path):
                    if current_date in file:
                        SNODAS_utilities.create_csv_files(file, BASIN_SHP_PATH, results_date_path, results_basin_path)

                # Delete rows from basin CSV files if the date is being reprocessed
                for file in os.listdir(clip_path):
                    if current_date in file:
                        SNODAS_utilities.delete_by_basin_csv_repeated_rows(file, BASIN_SHP_PATH, results_basin_path)

                # Calculate zonal statistics and export results
                for file in os.listdir(clip_path):
                    # print('The file: {}'.format(file))
                    if current_date in file:
                        SNODAS_utilities.z_stat_and_export(file, BASIN_SHP_PATH, results_basin_path, results_date_path,
                                                           clip_path, snow_cover_path, current,
                                                           returnedList[0], OUTPUT_CRS_EPSG)

                # If configured, zip files of output shapefile (both today's data and latestDate file)
                if SHP_ZIP.upper() == 'TRUE':
                    for file in os.listdir(results_date_path):
                        if current_date in file and file.endswith('.shp'):
                            SNODAS_utilities.zip_shapefile(file, results_date_path, DEL_SHP_ORIG)
                        if 'LatestDate' in file and file.endswith('.shp'):
                            zip_full_path = results_date_path / 'SnowpackStatisticsByDate_LatestDate.zip'
                            if zip_full_path.exists():
                                zip_full_path.unlink()

                            SNODAS_utilities.zip_shapefile(file, results_date_path, DEL_SHP_ORIG)

                # If configured, the time series will run for each processed date of data.
                if RUN_DAILY_TSTOOL.upper() == 'TRUE':
                    SNODAS_utilities.create_snodas_swe_graphs()

                # If it is the last date in the range, continue.
                if current == endDate or current == endDate.date():

                    # Remove any duplicates that occurred in the byBasin csv files (rare but could happen.)
                    SNODAS_utilities.clean_duplicates_from_by_basin_csv(results_basin_path)

                    # If configured, the time series will run for the entire historical range.
                    if RUN_HIST_TSTOOL.upper() == 'TRUE':
                        SNODAS_utilities.create_snodas_swe_graphs()

                    # Push daily statistics to the web if configuration property is set to 'True'.
                    if UPLOAD_TO_S3.upper() == 'TRUE':
                        SNODAS_utilities.push_to_aws()
                    if UPLOAD_TO_GCP.upper() == 'TRUE':
                        SNODAS_utilities.push_to_gcp()
                    if UPLOAD_TO_S3.upper() != 'TRUE' and UPLOAD_TO_GCP.upper() != 'TRUE':
                        print('Neither AWS or GCP configuration properties are True. Skipping the push to the cloud.')
                        logger.info('push_to_gcp: Output files from SNODAS_Tools are not pushed to cloud storage '
                                    'because of settings in configuration file. ')

            # If config file value SaveAllSNODASParameters is not a valid value ('True' or 'False') the remaining script
            # will not run and the following error message will be printed to the console and to the logging file.
            else:
                logger.error('See configuration file. The value of the SaveAllSNODASParameters section is not '
                             'valid. Please type in "True" or "False" and rerun the script.', exc_info=True)

        # Display elapsed time of current date's processing in log.
        end_day = time.time()
        elapsed_day = end_day - start_day
        logger.info('{}: Completed.'.format(current_date))
        logger.info('Elapsed time (date: {}): {} seconds'.format(current_date, elapsed_day))

        # TODO: jpkeahey 2021-06-22 - This called the function to mv all files onto the Host OS's shared folder.
        # if DEV_ENVIRONMENT.upper() == 'TRUE':
        #     mv_to_shared_dir([download_path, set_format_path, clip_path, snow_cover_path,
        #                       results_basin_path, results_date_path])

    # Close logging including the elapsed time of the running script in seconds.
    elapsed = time.time() - start
    elapsed_hours = int(elapsed / 3600)
    elapsed_hours_remainder = elapsed % 3600
    elapsed_minutes = int(elapsed_hours_remainder / 60)
    elapsed_seconds = int(elapsed_hours_remainder % 60)
    stringStart = str(startDate)
    stringEnd = str(endDate)
    print('SNODASHistoricalDownload.py: Completed. Dates Processed: From {} to {}.'.format(stringStart, stringEnd))
    print('Elapsed time (full script): approximately {} hours, {} minutes and {} seconds\n'
          .format(elapsed_hours, elapsed_minutes, elapsed_seconds))

    # If any dates were unsuccessfully downloaded, print those dates to the console and the logging file.
    failed_dates_lst_updated = []
    failed_dates_lst_1Week = []
    for item in failed_dates_lst:
        if item != 'None':
            item_str = str(item)
            item_plus_seven = item + timedelta(days=7)
            item_plus_seven_str = str(item_plus_seven)
            failed_dates_lst_updated.append(item_str)
            failed_dates_lst_1Week.append(item_plus_seven_str)

    if not failed_dates_lst_updated:
        print('All dates successfully downloaded!')
        logger.info('All dates successfully downloaded!')
    else:
        print('\nDates unsuccessfully downloaded: ')
        logger.info('\nDates unsuccessfully downloaded: ')
        for item in failed_dates_lst_updated:
            print(item)
            logger.info('{}'.format(item))
        print("\nDates that will be affected by assigning the 'SNODAS_SWE_Volume_1WeekChange_acft' attribute 'NULL' "
              "due to the unsuccessful downloads: ")
        logger.info("\nDates that will be affected by assigning the 'SNODAS_SWE_Volume_1WeekChange_acft' attribute "
                    "'NULL' due to the unsuccessful downloads: ")
        for item in failed_dates_lst_1Week:
            print(item)
            logger.info('{}'.format(item))

    logger.info('SNODASHistoricalDownload.py: Completed. Dates Processed: From {} to {}.'
                .format(stringStart, stringEnd))
    logger.info('Elapsed time (full script): approximately {} hours, {} minutes and {} seconds\n'
                .format(elapsed_hours, elapsed_minutes, elapsed_seconds))
