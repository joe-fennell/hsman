"""Submodule for HSI ingestion pipeline
"""

import os
import shutil
import rasterio
import numpy as np
import xarray
import functools
import subprocess
import datetime
import logging
from netCDF4 import Dataset
import tempfile


def ingest_hsi_sequence(file_paths, dataset_name, dst, temporary_directory=None):
    """Ingest a list of HSI files
    """
    if not temporary_directory:
        temporary_directory = dst
    # use temporary directory context handler
    with tempfile.TemporaryDirectory(dir=temporary_directory) as temp_dir:
        tmp_master = _make_dataset_folder(os.path.join(temp_dir, dataset_name))
        tmp_data = os.path.join(tmp_master, 'DATA')

        # generate band idx and wavelengths
        band_idxs, wavelengths = _get_common_idx(file_paths)

        # For testing only, make a small version
        if "PYTEST_CURRENT_TEST" in os.environ:
            band_idxs = band_idxs[:,:3]
            wavelengths = wavelengths[:3]
            
        # get all metadata
        metadata = _get_other_metadata(file_paths)
        metadata['acquisition_start_time'] = _get_collect_time(file_paths).isoformat()

        # iterate the bands and generate band slice files one at a time
        for i in range(len(wavelengths)):
            new_band_idx = i+1
            band_idx = band_idxs[:, i]
            wavelength = wavelengths[i]

            _merge_band(file_paths,
                        tmp_data,
                        band_idx,
                        metadata,
                        wavelength,
                        new_band_idx)

        shutil.move(tmp_master, os.path.join(dst, dataset_name))
    return os.path.join(dst, dataset_name)


def _get_common_idx(file_paths):
    def _get_all_wavelengths(file_paths):
        # returns a list of lists of all wavelengths in a file for a list of files
        def _get_wavelengths(band_names):
            bn = band_names.split(',')
            try:
                bn = [float(x.split(' ')[0], 0) for x in bn]
            except ValueError:
                bn = [float(x.split(' ')[1][4:-1], 0) for x in bn]
            return np.array(bn)

        wlens = []
        for file in file_paths:
            ar = xarray.open_rasterio(file, cache=False)
            try:
                # usually the wavelength dimension is available
                wlens.append(ar.wavelength.values)
            except AttributeError:
                # sometimes this is missing so need to parse from band names
                wlens.append(_get_wavelengths(ar.attrs['band_names']))
        return wlens

    # function generates the band args to retrieve only bands common to
    # whole dataset
    # rounds all the wavelength coords to nearest integer value
    wlen_list = _get_all_wavelengths(file_paths)
    rounded = [x.round(0) for x in wlen_list]
    common = functools.reduce(np.intersect1d, rounded)
    out = []
    for wl in rounded:
        _, idxs, _ = np.intersect1d(wl, common, return_indices=True)
        # add 1 to go to raster bands
        idxs += 1
        # every other value is the -b band cmd
        out.append(idxs)
    return np.array(out), np.array(common)


def _get_collect_time(file_paths):
    # returns the earliest acquisition/collect time of a file list
    def _estimate_collect_time(filename):
        fname = os.path.basename(filename)
        date = fname.split('_')[0].split('-')[2]
        year = int(date[:4])
        month = int(date[4:6])
        day = int(date[6:])
        return datetime.datetime(year, month, day, 0, 0, 0)

    dtimes = []
    for fpath in file_paths:
        attrs = xarray.open_rasterio(fpath, cache=False).attrs
        try:
            d = attrs['acquisition_date'].split('-')
        except KeyError:
            logging.debug('acquisition_date not in metadata')
            return _estimate_collect_time(fpath)

        try:
            t = attrs['acquisition_start_time'].split(':')
        except KeyError:
            logging.debug('acquisition_start_time not in metadata')
            return _estimate_collect_time(fpath)
        d = [int(x) for x in d]
        t = [int(x) for x in t]
        dtimes.append(datetime.datetime(*d, *t))
    return min(dtimes)


def _get_other_metadata(file_paths):
    # return a metadata dictionary with flightline/georeferencing removed
    def _retrieve_file_metadata(fpath):
        with rasterio.open(fpath, 'r') as f:
            tags = f.tags(ns='ENVI')
        if len(tags) > 0:
            return tags
        raise RuntimeError('no ENVI tags found')

    remove = ['acquisition_date',
              'acquisition_start_time',
              'acquisition_time',
              'aircraft_heading',
              'coordinate_system_string',
              'lines',
              'samples',
              'x_start',
              'y_start']

    meta = {}
    for f in file_paths:
        try:
            # return only the first successful retrieval
            meta = _retrieve_file_metadata(f)
            break

        except RuntimeError:
            continue

    # remove any removal keys
    [meta.pop(x, None) for x in remove]
    return meta


# function for setting up dir structure
def _make_dataset_folder(dst):
    # creates dir structure and returns root path of new dir
    # if already exists, add a suffix
    new_dst = dst
    i = 1
    while os.path.exists(new_dst):
        new_path = dst + '_{}'.format(i)
        i += 1
    os.makedirs(os.path.join(new_dst, 'DATA'))
    os.makedirs(os.path.join(new_dst, 'METADATA'))
    return new_dst


# functions for generating new files
def _unrotate_hsi(file_paths, dst, band='all'):
    # returns a list of file paths of unrotated
    output_files = []

    # somnetimes it is necessary to specify the band number separately for each
    # file in the dataset
    if type(band) == list:
        pass
    elif type(band) == np.ndarray:
        pass
    else:
        band = [band] * len(file_paths)

    for input_file, _band in zip(file_paths, band):
        fname = f'unrotated_{os.path.basename(input_file)}'
        # Open the input file using rasterio
        with rasterio.open(input_file) as src:
            crs = f'epsg:{src.crs.to_epsg()}'
        # for now use gdal_translate to extract a single band, then
        fname_short, _ext = os.path.splitext(fname)
        # skip the next step if all bands to be included
        if _band != 'all':
            command = ['gdal_translate']

            output_file1 = os.path.join(dst, '~' + fname_short + _ext)
            # assume an integer band index (1...n)

            command += [
                    '-b', str(int(_band)),
                    input_file,
                    output_file1
                ]

            subprocess.run(command, check=True, stdout=subprocess.DEVNULL)
            input_file = output_file1

        output_file = os.path.join(dst, fname_short + _ext)

        command = [
            'gdalwarp',
            '-r', 'bilinear',
            '-t_srs', crs,
            '-overwrite',
            input_file,
            output_file
            ]

        # Run the command using subprocess
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL)

        # Cleanup intermediate file
        try:
            os.remove(output_file1)
        except UnboundLocalError:
            pass

        # Return the output file path
        output_files.append(output_file)
    return output_files


def _merge_band(file_paths, dst, band, meta, new_band_wavelength,
                new_band_index=None):
    # generates a netcdf file for a band combination
    # setup filepaths
    dst_fpath = os.path.join(dst, 'band_{}_merged.nc'.format(band))

    if os.path.exists(dst_fpath):
        raise FileExistsError(f'{dst_fpath} already exists!')

    try:
        len(band)
        if not new_band_index:
            raise ValueError('new_band_index must be set when band is array')

    except TypeError:
        new_band_index = band

    with tempfile.TemporaryDirectory() as temp_dir:
        # rotate files and write to a temp array on disk
        unrotated_file_paths = _unrotate_hsi(file_paths, temp_dir, band)
        # unrotated_file_paths = file_paths # for testing only
        command = ['gdal_merge.py',
                   '-init', '0',
                   '-o', dst_fpath,
                   '-of', 'netCDF',
                   '-n', '0',
                   ]
        command += unrotated_file_paths
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL)
    # rename the band
    # Open the NetCDF4 dataset using a context handler


    with Dataset(dst_fpath, 'r+') as dataset:
        # Rename the variable
        dataset.renameVariable('Band1', 'reflectance')

        # add a new dimension with band
        reflectance_var = dataset.variables['reflectance']

        # Add the new dimension to the dataset
        dataset.createDimension('band', 1)

        # Optionally, assign values to the new dimension using the variable's associated coordinate variable
        coordinate_var1 = dataset.createVariable('band', 'i4', ('band',))
        coordinate_var2 = dataset.createVariable('wavelength', 'f8', ('band',))
        coordinate_var1[:] = [new_band_index]
        coordinate_var2[:] = [new_band_wavelength]

        for k, v in meta.items():
            dataset.setncattr(k, v)

        # Synchronize changes to the file
        dataset.sync()

    return dst_fpath
