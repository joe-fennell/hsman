"""
Handles data conversion and disk storage
"""

import logging
import numpy as np
import os
import xarray
import subprocess

from .config import DATA_PATH, logger
logger()


def ingest(file_list, dataset_name, target_dtype, target_file_size=2e9):
    """
    Run the ingestion script on a list of files

    Parameters
    ----------
    file_list : list-like
        list of file paths for inputfiles
    dataset_name : str
        name to use for folder and file names
    target_dtype : str
        string describing numpy dtype

    """
    logging.info('Ingesting {}'.format(dataset_name))
    # make folder for storing outputs
    dst = _make_dataset_folder(dataset_name)
    dst_data = os.path.join(dst, 'DATA')
    # combine files into a VRT
    vrt_path = _make_vrt(file_list, dst)
    ds = xarray.open_rasterio(vrt_path)
    logging.info('Generated VRT of size {}'.format(ds.shape))
    # metadata attributes often not preserved so get these from files
    new_attrs = _merge_attrs(
        [xarray.open_rasterio(x, cache=False).attrs for x in file_list]
    )
    # generate correct tile index sets
    if target_dtype in ['bool']:
        tile_slices = _make_tile_slices(ds, target_file_size, 1)
    # 8 bit formats
    if target_dtype in ['uint8', 'int8']:
        tile_slices = _make_tile_slices(ds, target_file_size, 8)
    # 16 bit formats
    if target_dtype in ['float16', 'uint16', 'int16']:
        tile_slices = _make_tile_slices(ds, target_file_size, 16)
    # 32 bit formats
    elif target_dtype in ['float32', 'uint32', 'int32']:
        tile_slices = _make_tile_slices(ds, target_file_size, 32)
    # 64 bit formats
    elif target_dtype in ['float64', 'uint64', 'int32']:
        tile_slices = _make_tile_slices(ds, target_file_size, 64)

    logging.info('Starting processing {} tiles'.format(len(tile_slices)))
    # iterate tile slice indices
    file_number = 1
    for idxs in tile_slices:
        logging.info(idxs)
        tile = ds.isel(x=idxs[0], y=idxs[1])
        logging.info('Checking tile for data...')
        _data = bool(_has_data(tile))
        logging.info(_data)
        if _data:
            logging.info('Tile contains data. Writing to disk...')
            # # update attrs twice to guarantee are retained in dataarray and
            # # dataset
            tile.attrs = new_attrs
            tile.attrs['_FillValue'] = 0
            tile = tile.to_dataset(name='reflectance').astype(target_dtype)
            tile.attrs = new_attrs
            tile.attrs['_FillValue'] = 0
            tile.to_netcdf(os.path.join(dst_data,
                                        dataset_name+'_{}.nc'.format(
                                            file_number
                                        )))
            # # update tile number and logging
            logging.info('{} files generated'.format(file_number))
            file_number += 1
        else:
            logging.info('Tile has no data. Skipping...')
    logging.info('All files generated for dataset {}'.format(dataset_name))


def _has_data(dataset, nodataval=0):
    # Checks the array has more than no data vals
    # use only the first 2 bands
    return (dataset.isel(band=slice(0, 2)) != nodataval).any().compute()


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


def _make_vrt(file_list, dst):
    # build a vrt from file_list and make a file
    dst_path = os.path.join(dst, 'merged.vrt')
    cmd = ['gdalbuildvrt', dst_path]
    cmd += file_list
    # Run with subprocess
    subprocess.check_call(cmd)
    return os.path.abspath(dst_path)


def _merge_attrs(attrs_list):
    # returns only values present in all dictionaries in dict_list
    new = {}
    for d in attrs_list:
        new.update(d)
    return new
