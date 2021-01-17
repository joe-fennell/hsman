"""
Handles data conversion and disk storage
"""
import dask
import datetime
import functools
import logging
import numpy as np
import os
import xarray
import subprocess
import shutil

from .config import DATA_PATH, logger, SCRATCH_PATH
logger()


def ingest_image(file_path, dataset_name):
    """
    Ingest a single image file (e.g. tif)
    """
    logging.info('Ingesting {}'.format(dataset_name))
    dst = _make_dataset_folder(dataset_name)
    new_fpath = os.path.join(dst,
                             'DATA',
                             os.path.basename(file_path))
    # move to directory
    shutil.copyfile(file_path, new_fpath)
    # change permissions to read only
    os.chmod(new_fpath, 0o555)
    logging.info('1 file generated for dataset {}'.format(dataset_name))


def ingest_hsi(file_list, dataset_name, target_dtype, target_file_size=2e9):
    """
    Run the ingestion script on a list of files with HSI-type data.

    Parameters
    ----------
    file_list : list-like
        list of file paths for inputfiles
    dataset_name : str
        name to use for folder and file names
    target_dtype : str
        string describing numpy dtype

    """
    os.environ['HDF5_USE_FILE_LOCKING'] = 'FALSE'
    with dask.config.set(num_workers=4):
        logging.info('Ingesting {}'.format(dataset_name))
        # make folder for storing outputs
        dst = _make_dataset_folder(dataset_name)
        dst_data = os.path.join(dst, 'DATA')
        # combine files into a VRT
        vrt_path = _make_vrt(file_list, os.path.join(dst, 'METADATA'))
        logging.debug('VRT path: {}'.format(vrt_path))
        ds = xarray.open_rasterio(vrt_path, chunks=(200, 5000, 5000))
        logging.info('Generated VRT of size {}'.format(ds.shape))
        # metadata attributes often not preserved so get these from files
        new_attrs = _merge_attrs(
            [xarray.open_rasterio(x, cache=False).attrs for x in file_list]
        )
        new_attrs['_FillValue'] = 0
        # get the capture starttime of first tile
        ast = np.min([_get_collect_time(x) for x in file_list])
        new_attrs['acquisition_start_time'] = ast.isoformat()
        # retrieve wavelength dimension and add to dataset
        ds = ds.assign_coords({'wavelength':
            ('band', _get_common_wavelengths(file_list))})

        ds = ds.astype(target_dtype)
        ds.attrs = new_attrs
        out = ds.to_dataset(name='reflectance')
        out.attrs = new_attrs

        _temp_path = os.path.join(SCRATCH_PATH,
                                  dataset_name+'.nc')

        _tile_path = os.path.join(dst_data,
                                  dataset_name+'.nc')
        out.to_netcdf(_temp_path)
        shutil.move(_temp_path, _tile_path)
        # change to read only for all users
        os.chmod(_tile_path, 0o555)
        # generate correct tile index sets
        # if target_dtype in ['bool']:
        #     tile_slices = _make_tile_slices(ds, target_file_size, 1)
        # # 8 bit formats
        # if target_dtype in ['uint8', 'int8']:
        #     tile_slices = _make_tile_slices(ds, target_file_size, 8)
        # # 16 bit formats
        # if target_dtype in ['float16', 'uint16', 'int16']:
        #     tile_slices = _make_tile_slices(ds, target_file_size, 16)
        # # 32 bit formats
        # elif target_dtype in ['float32', 'uint32', 'int32']:
        #     tile_slices = _make_tile_slices(ds, target_file_size, 32)
        # # 64 bit formats
        # elif target_dtype in ['float64', 'uint64', 'int32']:
        #     tile_slices = _make_tile_slices(ds, target_file_size, 64)
        # logging.info('Loading first layer into memory')
        # # read test layer into memory
        # test_layer = ds.isel(band=0).compute()
        # iterate tile slice indices
        # logging.info('Processing {} tiles'.format(len(tile_slices)))
        # file_number = 1
        # for idxs in tile_slices:
        #     logging.debug(idxs)
        #     tile = ds.isel(x=idxs[0], y=idxs[1])
        #     logging.info('Checking tile not empty')
        #     _data = bool(_has_data(test_layer.isel(x=idxs[0], y=idxs[1])))
        #     logging.debug(_data)
        #     if _data:
        #         logging.info('Processing tile')
        #         tile = tile.chunk((200, 2000, 2000))
        #         tile = tile.astype(target_dtype)
        #         # update attrs twice to guarantee are retained in dataarray
        #         # and dataset
        #         tile.attrs = new_attrs
        #         # add wavelength coord
        #         _tile = tile.to_dataset(name='reflectance')
        #         _tile.attrs = new_attrs
        #         _tile_path = os.path.join(dst_data,
        #                                   dataset_name+'_{}.nc'.format(
        #                                       file_number))
        #         _tile_temp = os.path.join(SCRATCH_PATH,
        #                                   dataset_name+'_{}.nc'.format(
        #                                       file_number))
        #         logging.info('Writing tile {} to scratch...'.format(
        #             file_number))
        #         _tile.to_netcdf(_tile_temp)
        #         logging.info('Moving tile {} to disk...'.format(
        #             file_number))
        #         shutil.move(_tile_temp, _tile_path)
        #         # change to read only for all users
        #         os.chmod(_tile_path, 0o555)
        #         # # update tile number and logging
        #         file_number += 1
        #     else:
        #         logging.info('Tile has no data. Skipping...')
        # logging.info('{} files generated for dataset {}'.format(file_number-1,
        #                                                         dataset_name))


# private funcs
def _get_collect_time(filename):
    """
    Parameters
    ----------
    filename : str
        path to raster
    """
    def _estimate_collect_time(filename):
        """
        Parameters
        ----------
        filename : str
            filename or path of file
        """
        fname = os.path.basename(filename)
        date = fname.split('_')[0].split('-')[2]
        year = int(date[:4])
        month = int(date[4:6])
        day = int(date[6:])
        return datetime.datetime(year, month, day, 0, 0, 0).isoformat()

    attrs = xarray.open_rasterio(filename, cache=False).attrs
    try:
        d = attrs['acquisition_date'].split('-')
    except KeyError:
        logging.debug('acquisition_date not in metadata')
        return _estimate_collect_time(filename)

    try:
        t = attrs['acquisition_start_time'].split(':')
    except KeyError:
        logging.debug('acquisition_start_time not in metadata')
        return _estimate_collect_time(filename)
    d = [int(x) for x in d]
    t = [int(x) for x in t]
    return datetime.datetime(*d, *t)


def _get_common_wavelengths(file_paths):
    # returns only wavelengths present in all
    wlens = _get_all_wavelengths(file_paths)
    return functools.reduce(np.intersect1d, wlens).round(0)


def _get_all_wavelengths(file_paths):
    """
    Returns a list of wavelength dims for all files file_paths

    Parameters
    ----------
    file_paths : list
        iterable of paths to valid rasterio-compatible files.

    Returns
    -------
    wavelengths : list
    """
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


def _has_data(dataset, nodataval=0):
    # Checks the array has more than no data vals
    # use only the first 2 bands
    return (dataset != nodataval).any().compute()


def _make_dataset_folder(name):
    path = os.path.join(DATA_PATH, name)
    # if already exists, add a suffix
    new_path = path
    i = 1
    while os.path.exists(new_path):
        new_path = path + '_{}'.format(i)
        i += 1
    os.makedirs(os.path.join(new_path, 'DATA'))
    os.makedirs(os.path.join(new_path, 'METADATA'))
    return new_path


def _make_tile_slices(dataset, target_size=1e9, value_size=16):
    # make a list of tuples of slices [(x_slice_1, y_slice_1), (x_slice_2)]
    def _calculate_tile_delta(dataset, target_size, value_size):
        # calculate tile dimension from the target file size and returns tuple
        try:
            bands = len(dataset.band)
        except AttributeError:
            raise AttributeError('dataset does not have bands dimension')
        dx = np.ceil(np.sqrt((target_size / (value_size/8)) / bands))
        return dx
    # pix in each spatial dim of tile
    _d = _calculate_tile_delta(dataset, target_size, value_size)
    # maximum indices for x and y dim
    xmax = (np.ceil(len(dataset.x)/_d) * _d) + 1
    ymax = (np.ceil(len(dataset.y)/_d) * _d) + 1
    # make manual meshgrid of slices
    xs = np.arange(0, xmax, _d, int)
    ys = np.arange(0, ymax, _d, int)
    slices = []
    for i in range(len(xs)-1):
        for j in range(len(ys)-1):
            slices.append(
                (
                    slice(xs[i], xs[i+1], 1),
                    slice(ys[j], ys[j+1], 1)
                )
            )
    return slices


def _idx_common(wlen_list):
    def _to_cmd(ar):
        out = []
        for v in ar:
            out.append('-b')
            out.append(str(v))
        return out
    # function generates the band args to retrieve only bands common to
    # whole dataset
    # rounds all the wavelength coords to nearest integer value
    rounded = [x.round(0) for x in wlen_list]
    common = functools.reduce(np.intersect1d, rounded)
    out = []
    for wl in rounded:
        _, idxs, _ = np.intersect1d(wl, common, return_indices=True)
        # add 1 to go to raster bands
        idxs += 1
        # every other value is the -b band cmd
        out.append(_to_cmd(idxs))
    return out


def _make_vrt(file_list, dst):
    # build a VRT for each file with a subset of bands contained in all
    bands = _idx_common(_get_all_wavelengths(file_list))
    sub_vrt_files = []
    for i in range(len(bands)):
        dst_path = os.path.join(dst, '_merged_{}.vrt'.format(i))
        cmd = ['gdalbuildvrt', dst_path]
        cmd += [file_list[i]]
        cmd += bands[i]
        logging.debug('building sub_vrt {}/{}'.format(i, len(bands)))
        logging.debug(cmd)
        try:
            subprocess.check_output(cmd)
        except subprocess.CalledProcessError as e:
            raise e
        sub_vrt_files.append(dst_path)

    # build a vrt from file_list and make a file
    dst_path = os.path.join(dst, 'merged.vrt')
    cmd = ['gdalbuildvrt', dst_path]
    cmd += sub_vrt_files
    # Run with subprocess
    logging.debug('building merged_vrt'.format())
    subprocess.check_call(cmd)
    return os.path.abspath(dst_path)


def _merge_attrs(attrs_list):
    # returns only values present in all dictionaries in dict_list
    new = {}
    for d in attrs_list:
        new.update(d)
    return new
