from pathlib import Path
from datetime import datetime, timedelta
import time
import logging
import os
from shutil import copy

from utilities import download_snodas, format_date_yyyymmdd, list_dir, untar_snodas_file, extract_snodas_gz_file, convert_snodas_dat_to_bil, create_snodas_hdr_file_post2013, create_snodas_hdr_file_pre2013
from logging.config import fileConfig

# Create and configures logging file
CONFIG_FILE = 'logging.conf'
fileConfig(CONFIG_FILE)
logger = logging.getLogger('interactive')
logger.info('main.py: Started \n')

# Print version information
print('Running main.py Version 1')
logger.info('Running main.py Version 1\n')


def test_download(dates, rootdir):
    download_path = Path(rootdir) / 'RAW_data'
    processed_path = Path(rootdir) / 'netcdf'
    temp_path = Path(rootdir) / 'temp'

    all_folders = [download_path, processed_path, temp_path]

    for folder in all_folders:
        if not os.path.exists(folder):
            os.makedirs(folder)

    # Create an empty list that will contain all dates that failed to download.
    failed_dates_lst = []

    # loop through list object of dates
    for i, date in enumerate(dates):
        # Create datetime object
        date = datetime.strptime(date, '%Y-%m-%d')

        # Define the current day depending on the user's interest in one or range of dates.
        current = date

        # Format date into string with format YYYYMMDD
        current_date = format_date_yyyymmdd(current)
        current_date_tar = 'SNODAS_' + current_date + '.tar'

        # Check to see if this date of data has already been processed in the folder
        possible_file = download_path / current_date_tar

        # If date has already been processed within the folder, the download & zonal statistics are rerun.
        if possible_file.exists():
            logger.info('This date ({}) has already been processed. The files will be reprocessed and '
                        'rewritten.\n'.format(current_date))

        # Download current date SNODAS .tar file from the FTP site at
        # ftp://sidads.colorado.edu/DATASETS/NOAA/G02158/masked/
        returnedList = download_snodas(download_path, current)
        failed_dates_lst.append(returnedList[1])


        # Untar current date's data.
        for file in list_dir(download_path, '*.tar'):
            if current_date in str(file):
                untar_snodas_file(file, download_path, processed_path)

        # Extract current date's .gz files. Each SNODAS parameter files are zipped within a .gz file.
        for file in list_dir(processed_path, '*.gz'):
            if current_date in str(file):
                extract_snodas_gz_file(file)

        # Convert current date's SNODAS SWE .dat file into .bil format.
        for file in list_dir(processed_path, '*.dat'):
            if current_date in str(file):
                convert_snodas_dat_to_bil(file)

        # Create current date's custom .Hdr file. In order to convert today's SNODAS SWE .bil file into a usable
        # .tif file, a custom .Hdr must be created. Refer to the function in the SNODAS_utilities.py for more
        # information on the contents of the custom .HDR file.
        for file in list_dir(processed_path, '*.bil'):
            if current_date in str(file):
                if current.date() >= datetime(2013, 10, 1).date():
                    create_snodas_hdr_file_post2013(str(file))
                else:
                    create_snodas_hdr_file_pre2013(str(file))

                with rasterio.open(str(file)) as src:
                    array = src.read(1)
                    height = array.shape[0]
                    width = array.shape[1]
                    cols, rows = np.meshgrid(np.arange(width), np.arange(height))
                    xs, ys = rasterio.transform.xy(src.transform, rows, cols)
                    lons = np.array(xs)
                    lats = np.array(ys)

test_download(['2022-10-01'], Path(r'D:\Python Projects\SNODAStools\testing'))

import rioxarray as rx
import xarray
import rasterio
import matplotlib.pyplot as plt
import numpy as np


# for 1st band/variable, extract lat/lon arrays, concat numpy arrays on axis=2
with rasterio.open(Path(r'D:\Python Projects\SNODAStools\testing\netcdf\us_ssmv11036tS__T0001TTNATS2022100105HP001.bil'), 'r') as src:
    array = src.read(1)
    height = array.shape[0]
    width = array.shape[1]
    cols, rows = np.meshgrid(np.arange(width), np.arange(height))
    xs, ys = rasterio.transform.xy(src.transform, rows, cols)
    lons= np.array(xs)
    lats = np.array(ys)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
