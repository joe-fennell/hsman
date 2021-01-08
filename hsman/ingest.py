"""
Handles data conversion and disk storage
"""

import numpy as np
import os
import xarray
import subprocess

from .config import DATA_PATH


def ingest(file_list, dataset_name, target_size, target_dtype):
    """
    Run the ingestion script on a list of files

    Parameters
    ----------

    """
    # generate folder for storing outputs
    dst = _make_dataset_folder(dataset_name)
    dst_data = os.path.join(dst, 'DATA')
    # combine files into a VRT
    vrt_path = _generate_vrt(file_list, dst)
    ds = xarray.open_dataset(vrt_path, chunks=2000)
    # metadata attributes often not preserved so get these from files
    new_attrs = _merge_attrs(
        [xarray.open_rasterio(x, cache=False).attrs for x in file_list]
    )
    # convert to dtype
    ds = ds.astype(target_dtype)
    tile_size =


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


def _generate_vrt(file_list, dst):
    # build a vrt from file_list and generate a file
    dst_path = os.path.join(dst, 'merged.vrt')
    cmd = ['gdalbuildvrt', dst_path]
    cmd += file_list
    # Run with subprocess
    subprocess.check_call(cmd)
    return os.path.abspath(dst_path)


def _calculate_tile_dims(dataset, target_size, value_size=16):
    # calculate tile dimension from the target file size and returns tuple
    try:
        bands = len(dataset.band)
    except:
        raise AttributeError('dataset does not have bands dimension')
    dx = np.ceil(np.sqrt((target_size / (value_size/8)) / bands))
    return (bands, int(dx), int(dx))


def _merge_attrs(attrs_list):
    # returns only values present in all dictionaries in dict_list
    new = {}
    for d in attrs_list:
        new.update(d)
    return new
