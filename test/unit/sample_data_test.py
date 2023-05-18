from sample_data import generate_rotated_raster
import xarray
import math
import rasterio


def test_crs_rotations(tmp_path):
    # make sure
    rpath = generate_rotated_raster(tmp_path, True)
    assert _is_rotated(rpath)
    ar = xarray.open_rasterio(rpath, True)
    assert ar.shape == (182,10,10)
    #
    rpath2 = generate_rotated_raster(tmp_path, False)
    assert not _is_rotated(rpath2)
    ar2 = xarray.open_rasterio(rpath2)
    assert ar2.shape == (182,10,10)


def test_metadata(tmp_path):
    # make sure
    rpath = generate_rotated_raster(tmp_path, True)
    ds = xarray.open_rasterio(rpath)
    assert 'band_names' in ds.attrs



def _is_rotated(fpath):
    with rasterio.open(fpath) as src:
        # Get the affine transformation matrix
        transform = src.transform

        # Check the rotation angle
        rotation_angle = math.atan2(transform[3], transform[0]) * (180 / math.pi)
        if rotation_angle != 0:
            return True
        else:
            return False
