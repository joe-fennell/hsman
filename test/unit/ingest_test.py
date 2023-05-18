from hsman.ingest import _get_common_idx, _make_dataset_folder, \
_get_collect_time, _unrotate_hsi, _get_other_metadata, _merge_band, \
ingest_hsi, ingest_image

from sample_data import generate_rotated_raster
import datetime
import xarray
import os
from pytest import raises


def test_get_common_idx(tmp_path):
    rpath = generate_rotated_raster(tmp_path, True)
    a, b = _get_common_idx([rpath, rpath])
    assert a.shape == (2, 182)
    assert b.shape == (182,)


def test_get_collect_time(tmp_path):
    rpath = generate_rotated_raster(tmp_path, True)
    a = _get_collect_time([rpath, rpath])
    assert type(a) is datetime.datetime


def test_get_other_metadata(tmp_path):
    rpath = generate_rotated_raster(tmp_path, True)
    a = _get_other_metadata([rpath, rpath])
    assert len(a) == 25
    assert 'solar_zenith' in a


def test_make_dataset_folder(tmp_path):
    fp = _make_dataset_folder('TEST1')
    assert os.path.exists(fp)


def test_unrotate_hsi_multi(tmp_path):
    ds1 = generate_rotated_raster(tmp_path, True)
    ds2 = generate_rotated_raster(tmp_path, True)
    unrotated = _unrotate_hsi([ds1, ds2], tmp_path)
    ds = xarray.open_rasterio(unrotated[0])
    assert len(ds.band) == 182


def test_unrotate_hsi(tmp_path):
    ds1 = generate_rotated_raster(tmp_path, True)
    ds2 = generate_rotated_raster(tmp_path, True)
    unrotated = _unrotate_hsi([ds1, ds2], tmp_path, band=10)
    ds = xarray.open_rasterio(unrotated[0])
    assert len(ds.band) == 1


def test_unrotate_hsi_multi_bands(tmp_path):
    ds1 = generate_rotated_raster(tmp_path, True)
    ds2 = generate_rotated_raster(tmp_path, True)
    unrotated = _unrotate_hsi([ds1, ds2], tmp_path, band=[10,1])
    ds = xarray.open_rasterio(unrotated[0])
    assert len(ds.band) == 1


def test_merge_band(tmp_path):
    ds1 = generate_rotated_raster(tmp_path, True)
    ds2 = generate_rotated_raster(tmp_path, False)
    merged = _merge_band([ds1, ds2], tmp_path, band=10,
                         meta={'testattr':'abc'}, new_band_wavelength=500,)
    merged = xarray.open_mfdataset(merged, combine='by_coords')
    assert merged['reflectance'].shape == (16,14)
    assert merged.attrs['testattr'] == 'abc'


def test_merge_band_error(tmp_path):
    ds1 = generate_rotated_raster(tmp_path, True)
    ds2 = generate_rotated_raster(tmp_path, False)
    with raises(ValueError):
        merged = _merge_band([ds1, ds2], tmp_path, band=[10,20],
                             meta={'testattr':'abc'},
                             new_band_wavelength=500)


def test_ingest_hsi(tmp_path):
    ds1 = generate_rotated_raster(tmp_path, True)
    ds2 = generate_rotated_raster(tmp_path, False)
    dst = ingest_hsi([ds1, ds2], 'TEST01')
    assert os.path.exists(dst)
    file_list = os.listdir(os.path.join(dst, 'DATA'))
    assert len(file_list) == 3
