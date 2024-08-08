import logging
import os
import sys
import time
import rasterio

from datetime import datetime, timedelta
from pathlib import Path
from logging.config import fileConfig

import utilities

# Create and configures logging file
CONFIG_FILE = 'logging.conf'
fileConfig(CONFIG_FILE)
logger = logging.getLogger('interactive')
logger.info('getSNODAS.py: Started \n')

# Print version information
print('Running getSNODAS.py Version 1')
logger.info('Running getSNODAS.py Version 1\n')


def download_multiband_range(startDate, endDate, rootdir):
    """
    Function to download a range of SNODAS datasets, scale them, and combine into a multiband
    raster for analysis or display.
    Parameters
    ----------
    startDate: in the format "yyyy-mm-dd"
    endDate: in the format "yyyy-mm-dd"
    rootdir: root directory for which all raw and processed output will be saved, the function will
    create default sub-directories for organization

    Returns
    -------
    None - function completes successfully with processed files in the root directory.
    """
    download_path = Path(rootdir) / 'RAW_data'
    processed_path = Path(rootdir) / 'geotiff'

    all_folders = [download_path, processed_path]

    for folder in all_folders:
        if not os.path.exists(folder):
            os.makedirs(folder)

    # The start time is used to calculate the elapsed time of the running script. The elapsed time will be displayed at
    # the end of the log file.
    start = time.time()

    # Create an empty list that will contain all dates that failed to download.
    failed_dates_lst = []

    # Create datetime objects
    startDate = datetime.strptime(startDate, '%Y-%m-%d')
    endDate = datetime.strptime(endDate, '%Y-%m-%d')
    # Iterate through each day of the user-specified range. Refer to:
    # http://stackoverflow.com/questions/6901436/python-expected-an-indented-block
    total_days = (endDate - startDate).days + 1

    # Define the current day depending on the user's interest in one or range of dates.
    for day_number in range(total_days):
        current = (startDate + timedelta(days=day_number)).date()

        # The start time is used to calculate the elapsed time of the running script. The elapsed time will be
        # displayed at the end of the log file.
        start_day = time.time()

        # Format date into string with format YYYYMMDD
        current_date = utilities.format_date_yyyymmdd(current)
        current_date_tar = 'SNODAS_' + current_date + '.tar'

        # Check to see if this date of data has already been processed in the folder
        possible_file = download_path / current_date_tar

        # If date has already been processed within the folder, the download & zonal statistics are rerun.
        if possible_file.exists():
            logger.info('This date ({}) has already been processed. The files will be reprocessed and '
                        'rewritten.\n'.format(current_date))

        # Download current date SNODAS .tar file from the FTP site at
        # ftp://sidads.colorado.edu/DATASETS/NOAA/G02158/masked/
        returnedList = utilities.download_snodas(download_path, current)

        failed_dates_lst.append(returnedList[1])

        # Untar current date's data.
        for file in utilities.list_dir(download_path, '*.tar'):
            if current_date in str(file):
                utilities.untar_snodas_file(file, download_path, processed_path)


        # Extract current date's .gz files. Each SNODAS parameter files are zipped within a .gz file.
        for file in utilities.list_dir(processed_path, '*.gz'):
            if current_date in str(file):
                utilities.extract_snodas_gz_file(file)

        # Convert current date's SNODAS SWE .dat file into .bil format.
        for file in utilities.list_dir(processed_path, '*.dat'):
            if current_date in str(file):
                utilities.convert_snodas_dat_to_bil(file)


        # Create current date's custom .Hdr file. In order to convert today's SNODAS SWE .bil file into a usable
        # .tif file, a custom .Hdr must be created. Refer to the function in the SNODAS_utilities.py for more
        # information on the contents of the custom .HDR file.
        for file in utilities.list_dir(processed_path, '*.bil'):
            if current_date in str(file):
                if current >= datetime(2013, 10, 1).date():
                    utilities.create_snodas_hdr_file_post2013(str(file))
                else:
                    utilities.create_snodas_hdr_file_pre2013(str(file))


        # Convert current date's .bil files to .tif files
        tif_fl_lst = []
        for file in utilities.list_dir(processed_path, '*.bil'):
            if current_date in str(file):
                tif_fl_lst.append(file)
        utilities.stack_snodas_bil_to_multiband_tif(tif_fl_lst, str(processed_path) + '\\' + current_date + 'WGS84')

        # Delete current date's .bil and .hdr files
        for file in utilities.list_dir(processed_path, ('*.bil', '*.hdr', '*.Hdr', '*.prj'),
                                              multiple_types=True):
            if current_date in str(file):
                utilities.delete_snodas_files(file)

        # Move metadata files to sub-directory
        for file in utilities.list_dir(processed_path, '*.txt'):
            txt_path = processed_path / 'orig_metadata'
            if not os.path.exists(txt_path):
                os.makedirs(txt_path)
            utilities.move_snodas_txt_files(file, processed_path / 'orig_metadata')


        # Display elapsed time of current date's processing in log.
        end_day = time.time()
        elapsed_day = end_day - start_day
        logger.info('{}: Completed.'.format(current_date))
        logger.info('Elapsed time (date: {}): {} seconds'.format(current_date, elapsed_day))

    # Close logging including the elapsed time of the running script in seconds.
    elapsed = time.time() - start
    elapsed_hours = int(elapsed / 3600)
    elapsed_hours_remainder = elapsed % 3600
    elapsed_minutes = int(elapsed_hours_remainder / 60)
    elapsed_seconds = int(elapsed_hours_remainder % 60)
    stringStart = str(startDate)
    stringEnd = str(endDate)
    print('getSNODAS.py: Completed. Dates Processed: From {} to {}.'.format(stringStart, stringEnd))
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
        for item in failed_dates_lst_1Week:
            print(item)
            logger.info('{}'.format(item))

    logger.info('getSNODAS.py: Completed. Dates Processed: From {} to {}.'
                .format(stringStart, stringEnd))
    logger.info('Elapsed time (full script): approximately {} hours, {} minutes and {} seconds\n'
                .format(elapsed_hours, elapsed_minutes, elapsed_seconds))

# def download_day(date, rootdir: Path):
#     sdf

# def download_current(rootdir: Path):

if __name__ == '__main__':
    import multiprocessing as mp

    parallel = False

    startD = '2003-09-30'
    endD = '2003-12-31'
    fldr = r'D:\Spatial_Data\Statewide_Data\Raster\NSIDC\SNODAS'

    if parallel:
        # start = time.time()
        dts = utilities.split_date_range(startD, endD, mp.cpu_count())

        processes = [mp.Process(target=download_range, args=(dt[0], dt[1], fldr)) for dt in dts]
        print('Parallel Processing enabled using {0} CPUs'.format(mp.cpu_count()))
        for process in processes:
            process.start()
        for process in processes:
            process.join()


    else:
        download_range(startDate=startD, endDate=endD, rootdir=fldr)
