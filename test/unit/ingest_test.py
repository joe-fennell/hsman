# from hsman.ingest import _get_all_wavelengths, _get_collect_time, _make_vrt, _unrotate_hsi
# from sample_data import generate_rotated_raster
# import datetime
#
# def test_get_all_wavelengths(tmp_path):
#     rpath = generate_rotated_raster(tmp_path, True)
#     a = _get_all_wavelengths([rpath])
#     assert len(a[0]) == 182
#
# def test_get_collect_time(tmp_path):
#     rpath = generate_rotated_raster(tmp_path, True)
#     a = _get_collect_time(rpath)
#     assert type(a) == datetime.datetime
#
# def test_make_vrt(tmp_path):
#     ds1 = generate_rotated_raster(tmp_path, False)
#     ds2 = generate_rotated_raster(tmp_path, False)
#     rpath = _make_vrt([ds1, ds2], tmp_path)
#
# # def test_make_unrotate(tmp_path):
# #     ds1 = generate_rotated_raster(tmp_path, True)
# #     ds2 = generate_rotated_raster(tmp_path, True)
# #     unrotated = _unrotate_hsi([ds1, ds2], tmp_path)
# #     rpath = _make_vrt(unrotated, tmp_path)
