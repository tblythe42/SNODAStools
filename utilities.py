

import configparser
import csv
import ftplib
import rasterio
import gdal
import glob
import gzip
import logging
import ogr
import os
import osr
import subprocess
import sys
import tarfile
import time
import zipfile

from datetime import datetime, timedelta
from logging.config import fileConfig
from pathlib import Path
from shutil import copy, copyfile

# from PyQt5.QtCore import QVariant
# from qgis.analysis import (
#     QgsRasterCalculator,
#     QgsRasterCalculatorEntry,
#     QgsZonalStatistics
# )
# from qgis.core import (
#     QgsCoordinateReferenceSystem,
#     QgsCoordinateTransformContext,
#     QgsExpression,
#     QgsExpressionContext,
#     QgsExpressionContextScope,
#     QgsField,
#     QgsRasterLayer,
#     QgsVectorLayer,
#     QgsVectorFileWriter
# )
# sys.path.append('/usr/share/qgis/python/plugins')

CONFIG_FILE = 'logging.conf'
# Create and configures logging file
fileConfig(CONFIG_FILE)
logger = logging.getLogger('utilities')

snodas_param_info = {'1034': {'name': 'SWE',
                              'units': 'millimeters',
                              'dataSF': 1.0,
                              'na_SF': -9999},
                     '1036': {'name': 'Snow Depth',
                              'units': 'millimeters',
                              'dataSF': 1.0,
                              'na_SF': -9999},
                     '1044': {'name': 'Snow Melt Runoff at Base',
                              'units': 'millimeters',
                              'dataSF': 100.0,
                              'na_SF': -99.99},
                     '1050': {'name': 'Sublimation from Snow Pack',
                              'units': 'millimeters',
                              'dataSF': 100.0,
                              'na_SF': -99.99},
                     '1039': {'name': 'Sublimation from Blowing Snow',
                              'units': 'millimeters',
                              'dataSF': 100.0,
                              'na_SF': -99.99},
                     '1025SlL01': {'name': 'Solid Precip',
                                  'units': 'millimeters',
                                  'dataSF': 10.0,
                                  'na_SF': -999.9},
                     '1025SlL00': {'name': 'Liquid Precip',
                                  'units': 'millimeters',
                                  'dataSF': 10.0,
                                  'na_SF': -999.9},
                     '1038': {'name': 'Snow Pack Temp',
                              'units': 'kelvin',
                              'dataSF': 1.0,
                              'na_SF': -9999}
                     }


def download_snodas(download_dir: Path, single_date: datetime) -> list:
    """Access the SNODAS FTP site and download the .tar file of single_date. The .tar file saves to the specified
    download_dir folder.
    download_dir: full path name to the location where the downloaded SNODAS rasters are stored
    single_date: the date of interest in datetime format"""
    HOST = 'sidads.colorado.edu'
    USERNAME = 'anonymous'
    PASSWORD = 'None'
    SNODAS_FTP_FOLDER = '/DATASETS/NOAA/G02158/masked/'
    # log_path = download_dir / 'log'
    # if not os.path.exists(log_path):
    #     os.makedirs(log_path)
    #
    # N_filehandler = logging.handlers.TimedRotatingFileHandler(str(log_path / 'SNODAStools_utilities.log'), 'W0', 1, 5)
    # for hdlr in logger.handlers[:]:  # remove the existing file handlers
    #     if isinstance(hdlr, logging.handlers.TimedRotatingFileHandler):
    #         logger.removeHandler(hdlr)
    # logger.addHandler(N_filehandler)  # set the new handler
    #
    # logger.info('download_snodas: Starting {}'.format(single_date))

    # Code format for the following block of code in reference to:
    # http://www.informit.com/articles/article.aspx?p=686162&seqNum=7 and
    # http://stackoverflow.com/questions/5230966/python-ftp-download-all-files-in-directory
    # Connect to FTP server
    ftp = ftplib.FTP(HOST, USERNAME, PASSWORD)

    logger.info('download_snodas: Connected to FTP server. Routing to {}'.format(single_date))

    # Direct to folder within FTP site storing the SNODAS masked data.
    ftp.cwd(SNODAS_FTP_FOLDER)

    os.chdir(download_dir)

    # Move into FTP folder containing the data from single_date's year
    ftp.cwd(str(single_date.year) + '/')

    # Move into FTP folder containing the data from single_date's month
    month_folder = single_date.strftime('%m') + "_" + single_date.strftime('%b') + '/'
    ftp.cwd(month_folder)

    # Set day value to a padded digit. Ex: 2 would change to 02
    day = single_date.strftime('%d')

    # Iterate through files in FTP folder and save single_date's data as a file in download folder
    # Create empty list to track if a download is available
    no_download_available = []
    filenames = ftp.nlst()
    for file in filenames:
        if file.endswith('{}.tar'.format(day)):
            local_file = open(file, 'wb')
            ftp.retrbinary('RETR ' + file, local_file.write, 1024)

            logger.info('download_snodas: Downloaded {}'.format(single_date))
            # If SNODAS data is available for download, append a '1'
            no_download_available.append(1)
            local_file.close()
        else:
            # If SNODAS data is not available for download, append a '0'
            no_download_available.append(0)

    # Report error if download marker '1' is not in the list
    if 1 not in no_download_available:
        logger.error('download_snodas: Download unsuccessful for {}'.format(single_date), exc_info=True)
        failed_date = single_date

    # Report success if download marker '1' is in the list
    else:
        print('\nDownload complete for {}.'.format(single_date))
        logger.info('download_snodas: Download complete for {}.\n'.format(single_date))
        failed_date = 'None'

    # Retrieve a timestamp to later export to the statistical results
    timestamp = datetime.now().isoformat()

    # Add values of optional statistics to a list to be checked for validity in SNODASDaily_Automated.py and
    # SNODASDaily_Interactive.py scripts
    # Removed from return list for this use of the function
    # opt_stats = [CALCULATE_SWE_MAX, CALCULATE_SWE_MIN, CALCULATE_SWE_STD_DEV]

    return [timestamp, failed_date]

def format_date_yyyymmdd(date: datetime) -> str:
    """Convert datetime date to string date in format: YYYYMMDD.
     date: the date of interest in datetime format"""

    logger.info('format_date_yyyymmdd: Starting {} formatting.'.format(date))

    # Parse year, month and day of input date into separate entities.
    year = date.year
    month = date.strftime('%m')
    day = date.strftime('%d')

    # Concatenate strings of the year, double-digit month, and double-digit day.
    day_string = str(year) + month + day

    logger.info('format_date_yyyymmdd: Finished {} formatting.\n'.format(date))

    # Return string.
    return day_string

def untar_snodas_file(file: Path, folder_input: Path, folder_output: Path) -> None:
    """Untar downloaded SNODAS .tar file and extract the contained files to the folder_output
    file: SNODAS .tar file to untar
    folder_input: the full pathname to the folder containing 'file'
    folder_output: the full pathname to the folder containing the extracted files"""

    logger.info('untar_snodas_file: Starting {}'.format(file))

    # Set full pathname of file
    file_full = folder_input / file

    # Open .tar file
    tar = tarfile.open(file_full)

    # Change working directory to output folder
    os.chdir(folder_output)

    # Extract .tar file and save contents in output directory
    tar.extractall()

    # Close .tar file
    tar.close()

    logger.info('untar_snodas_file: {} has been untarred.\n'.format(file))

def list_dir(path: Path, ext, multiple_types=False):
    """ List all files that end with the provided extension(s).
     path: The directory to search through.
     ext: The single string or multiple tuple of extensions to search for.
     multiple_types: Whether multiple extensions need to be searched. Default is False.
     Returns: Either a generator (iterator) from path.glob(), or a list of the files. """

    if not multiple_types:
        return path.glob(ext)
    else:
        all_files = []
        for x in ext:
            all_files.extend(path.glob(x))
        return all_files


def extract_snodas_gz_file(file: Path) -> None:
    """Extract .dat and .Hdr files from SNODAS .gz file. Each daily SNODAS raster has 2 files associated with it
    (.dat and .Hdr) Both are zipped within a .gz file.
    file: .gz file to be extracted"""

    logger.info('extract_snodas_gz_file: Starting {}'.format(file))

    # This block of script was based off of the script from the following resource:
    # http://stackoverflow.com/questions/20635245/using-gzip-module-with-python
    in_file = gzip.open(str(file), 'r')
    with open(file.stem, 'wb') as out_file:
        out_file.write(in_file.read())
    in_file.close()

    # Delete .gz file
    file.unlink()

    logger.info('extract_snodas_gz_file: {} has been extracted.\n'.format(file))


def convert_snodas_dat_to_bil(file: Path) -> None:
    """Convert SNODAS .dat file into supported file format (.tif). The .dat and .Hdr files are not supported file
    formats to use with QGS processing tools. The QGS processing tools are used to calculate the daily zonal stats.
    file: .dat file to be converted to .bil format"""

    logger.info('convert_snodas_dat_to_bil: Starting {}'.format(file))

    # Change file extension from .dat to .bil
    file.rename(file.with_suffix('.bil'))

    logger.info('convert_snodas_dat_to_bil: {} has been converted into .bil format.\n'.format(file))


def create_snodas_hdr_file_pre2013(file: str) -> None:
    """Create custom .hdr file. A custom .Hdr file needs to be created to indicate the raster settings of the .bil file.
    The custom .Hdr file aids in converting the .bil file to a usable .tif file. This function is for data before
    Oct 1, 2013.
    file: .bil file that needs a custom .Hdr file"""

    logger.info('create_snodas_hdr_file_pre2013: Starting {}'.format(file))

    # Create name for the new .hdr file
    hdr_name = file.replace('.bil', '.hdr')

    # These lines of code create a custom .hdr file to give details about the .bil/raster file. The
    # specifics inside each .hdr file are the same for each daily raster. However, there must be a .hdr file
    # that matches the name of each .bil/.tif file in order for QGS to import each dataset. The text included in
    # the .Hdr file originated from page 12 of the 'National Operational Hydrologic Remote Sensing Center SNOw Data
    # Assimilation System (SNODAS) Products of NSIDC', This document can be found at the following url:
    # https://nsidc.org/pubs/documents/special/nsidc_special_report_11.pdf
    with open(hdr_name, 'w') as file2:
        file2.write('units dd\n')
        file2.write('nbands 1\n')
        file2.write('nrows 3351\n')
        file2.write('ncols 6935\n')
        file2.write('nbits 16\n')
        file2.write('pixeltype signedint')
        file2.write('byteorder M\n')
        file2.write('layout bil\n')
        file2.write('ulxmap -124.729583333333\n')
        file2.write('ulymap 52.8704166666666\n')
        file2.write('xdim 0.00833333333333333\n')
        file2.write('ydim 0.00833333333333333\n')

    logger.info('create_snodas_hdr_file_pre2013: {} now has a created a custom .hdr file.\n'.format(file))


def create_snodas_hdr_file_post2013(file: str) -> None:
    """Create custom .hdr file. A custom .Hdr file needs to be created to indicate the raster settings of the .bil file.
    The custom .Hdr file aids in converting the .bil file to a usable .tif file. This function is for data before
    Oct 1, 2003.
    file: .bil file that needs a custom .Hdr file"""

    logger.info('create_snodas_hdr_file_post2013: Starting {}'.format(file))

    # Create name for the new .hdr file
    hdr_name = file.replace('.bil', '.hdr')

    # These lines of code create a custom .hdr file to give details about the .bil/raster file. The
    # specifics inside each .hdr file are the same for each daily raster. However, there must be a .hdr file
    # that matches the name of each .bil/.tif file in order for QGS to import each dataset. The text included in
    # the .Hdr file originated from page 12 of the 'National Operational Hydrologic Remote Sensing Center SNOw Data
    # Assimilation System (SNODAS) Products of NSIDC', This document can be found at the following url:
    # https://nsidc.org/pubs/documents/special/nsidc_special_report_11.pdf
    with open(hdr_name, 'w') as file2:
        file2.write('units dd\n')
        file2.write('nbands 1\n')
        file2.write('nrows 3351\n')
        file2.write('ncols 6935\n')
        file2.write('nbits 16\n')
        file2.write('pixeltype signedint')
        file2.write('byteorder M\n')
        file2.write('layout bil\n')
        file2.write('ulxmap -124.729166666666\n')
        file2.write('ulymap 52.8708333333333\n')
        file2.write('xdim 0.00833333333333333\n')
        file2.write('ydim 0.00833333333333333\n')

    logger.info('create_snodas_hdr_file_post2013: {} now has a created a custom .hdr file.\n'.format(file))


def convert_snodas_bil_to_tif(file: str, folder_output: Path) -> None:
    """
    Convert .bil file into .tif file for processing within the QGIS environment.
    file: file to be converted into a .tif file
    folder_output: full pathname to folder where the created .tif files are contained
    """

    logger.info('convert_snodas_bil_to_tif: Starting {}'.format(file))

    # If in a Linux system
    # if LINUX_OS:
    #
    #     # Remove the original .Hdr file so that the created .hdr file will be read instead.
    #     orig_hdr_file = folder_output / file.replace('.bil', '.Hdr')
    #     if orig_hdr_file.exists():
    #         logger.info('convert_snodas_bil_to_tif: Removing the original .Hdr file so the custom .hdr can be used.')
    #         orig_hdr_file.unlink()

    # Create name with replaced .tif file extension
    tif_file = file.replace('.bil', '.tif')

    abs_output_tif = folder_output / tif_file

    # Convert file to .tif format by modifying the original file. No new file is created.
    gdal.Translate(str(abs_output_tif), file, format='GTiff')

    logger.info('convert_snodas_bil_to_tif: {} has been converted into a .tif file.\n'.format(file))


def delete_snodas_files(file: Path) -> None:
    """Delete file with .bil or .hdr extensions. The .bil and .hdr formats are no longer important to keep because the
    newly created .tif file holds the same data.
    file: file to be checked for either .hdr or .bil extension (and, ultimately deleted)"""

    logger.info('delete_snodas_files: Starting {}'.format(file))

    if file.is_file():
        # Delete file
        file.unlink()

        logger.info('delete_snodas_files: {} has been deleted.\n'.format(file))

    else:

        logger.info('delete_snodas_files: {} was not deleted. File does not exist.\n'.format(file))


def assign_snodas_datum(file: str, folder: Path) -> None:
    """Define WGS84 as datum. Defaulted in configuration file to assign SNODAS grid with WGS84 datum. The
    downloaded SNODAS raster is un-projected however the "SNODAS fields are grids of point estimates of snow cover in
    latitude/longitude coordinates with the horizontal datum WGS84." - SNODAS Data Products at NSIDC User Guide
    http://nsidc.org/data/docs/noaa/g02158_snodas_snow_cover_model/index.html
    file: the name of the .tif file that is to be assigned a projection
    folder: full pathname to the folder where both the un-projected and projected raster are stored"""

    logger.info('assign_snodas_datum: Starting {}'.format(file))

    CLIP_PROJECTION = "EPSG:4326"

    # Check for un-projected .tif files
    if file.upper().endswith('HP001.TIF'):

        # Change name from 'us_ssmv11034tS__T0001TTNATS2003093005HP001.tif' to '20030930WGS84.tif'
        new_file = file.replace('05HP001', 'WGS84').replace('us_ssmv11034tS__T0001TTNATS', '')

        # Set up for gdal.Translate tool. Set full path names for both input and output files.
        input_raster = folder / file
        output_raster = folder / new_file

        # Assign datum (Defaulted to 'EPSG:4326').
        gdal.Translate(str(output_raster), str(input_raster), outputSRS=CLIP_PROJECTION)

        # Delete un-projected file.
        input_raster.unlink()

        logger.info('assign_snodas_datum: {} has been assigned projection of {}.'.format(file, CLIP_PROJECTION))

        # Writes the projection information to the log file.
        ds = gdal.Open(str(output_raster))
        prj = ds.GetProjection()
        srs = osr.SpatialReference(wkt=prj)
        datum = srs.GetAttrValue('GEOGCS')

        if srs.IsProjected:
            proj_name = srs.GetAttrValue('AUTHORITY')
            proj_num = srs.GetAttrValue('AUTHORITY', 1)
            logger.info("assign_snodas_datum: {} has projection {}:{} and datum {}"
                        .format(output_raster.name, proj_name, proj_num, datum))
        else:
            logger.info("assign_snodas_datum: {} has projection {} and datum {}".format(output_raster.name, prj, datum))
    else:
        logger.warning("assign_snodas_datum: {} does not end in 'HP001.tif' and has not been assigned projection "
                       "of {}.\n".format(file, CLIP_PROJECTION))
        return

    logger.info('assign_snodas_datum: Successfully converted {} to {}.\n'.format(file, output_raster.name))

def stack_snodas_bil_to_multiband_tif(in_file_list, out_filenm):
    """
    Scale (to millimeters) and Stack SNODAS .bil files into multiband raster containing all parameters,
    save as geotiff

    Parameters
    ----------
    in_file_list - list of filenames for single band rasters to stack
    out_filenm - the output filename for the stacked multi-band raster

    Returns
    -------
    None - completes this operation within the file_list directory
    """
    # Read metadata of first file
    with rasterio.open(in_file_list[0], 'r+') as src0:
        src0.crs = rasterio.crs.CRS.from_epsg(4326)
        meta = src0.meta

    # Update meta to reflect the number of layers
    meta.update(count=len(in_file_list),
                driver='GTiff',
                dtype=rasterio.float32,
                nodata=-9999,
                compress='lzw')

    # Organize bands
    bnd_order = ['1034', '1036', '1044', '1050', '1039', '1025SlL01', '1025SlL00', '1038']

    # Read each layer and write it to stack
    with rasterio.open('{0}.tif'.format(out_filenm), 'w', **meta) as dst:
        for bnd, param in enumerate(bnd_order, start=1):
            for layer in in_file_list:
                if param in str(layer):
                    with rasterio.open(layer) as src1:
                        array = src1.read(1)
                        sc_array = array / snodas_param_info[param]['dataSF']
                        if snodas_param_info[param]['dataSF'] != 1.0:
                            sc_array[sc_array == snodas_param_info[param]['na_SF']] = -9999
                        dst.write_band(bnd, sc_array)
                        dst.set_band_description(bnd, 'Band {0} - {1}'.format(bnd, snodas_param_info[param]['name']))
                else:
                    continue

def move_snodas_txt_files(file: str, folder_output: Path) -> None:
    """Move the .txt file SNODAS metadata files to their own sub-directory
    file: .txt file extracted from the downloaded SNODAS .tar file
    folder_output: full pathname to folder where the other-than-SWE files are contained, OtherParameters"""

    logger.info('move_snodas_txt_files: Starting {}'.format(file))

    # Move copy of file to folder_output. Delete original file from original location.
    copy(file, folder_output)
    logger.info('move_snodas_txt_files: {} has been moved to {}.'.format(file, folder_output))
    Path(file).unlink()


    logger.info('move_snodas_txt_files: Finished {} \n'.format(file))


def split_date_range(startDate, endDate, chunks):
    """
    Split a date range into a specified number of chunks.
    Parameters
    ----------
    startDate: start date of the entire range in "yyyy-mm-dd" format
    endDate: end date of the entire range in "yyyy-mm-dd" format
    chunks: number of chunks of dates to split into

    Returns
    -------
    list of tuples representing the start and end dates of each chunk
    """
    s = datetime.strptime(startDate, '%Y-%m-%d')
    e = datetime.strptime(endDate, '%Y-%m-%d')
    ditems = e - s
    incrm = int(ditems.days / chunks) + 1

    date_chnks = []
    for c in range(chunks):
        if c == chunks - 1:
            st = s + timedelta(days=(c * incrm) + c)
            en = e
            dt_chnk = (st, en)
        else:
            st = s + timedelta(days=(c * incrm) + c)
            en = s + timedelta(days=((c+1) * incrm) + c)
            dt_chnk = (st, en)
        date_chnks.append(dt_chnk)

    return date_chnks
