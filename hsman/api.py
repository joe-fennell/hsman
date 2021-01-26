import geopandas
import os
import pyproj
import shapely
import xarray
from .config import DATA_PATH


def get_datasets():
    """
    Get datasets from cache or by reading
    """
    def bounding_box(ds):
        """
        Retrieve bounding box in lon/lat
        """
        transformer = pyproj.Transformer.from_crs(ds.crs.split('=')[-1],
                                                  "epsg:4326")
        tl = transformer.transform(ds.x.min(), ds.y.max())[::-1]
        tr = transformer.transform(ds.x.max(), ds.y.max())[::-1]
        bl = transformer.transform(ds.x.min(), ds.y.min())[::-1]
        br = transformer.transform(ds.x.max(), ds.y.min())[::-1]

        return [tl, tr, br, bl, tl]
    # get all names that don't start with _ in data dir
    names = os.listdir(DATA_PATH)
    names = [x for x in names if not x.startswith(('_', '.'))]

    try:
        gdf = geopandas.read_file(os.path.join(DATA_PATH,
                                               '_inventory.gpkg'))
        gdf = gdf.set_index('dataset')
        # filter any names already present in gpkg
        names = [x for x in names if not (gdf.index == x).any()]
    except geopandas.io.file.fiona.errors.DriverError:
        # no file so all names need to be parsed
        gdf = None
        pass
    if len(names) < 1:
        if gdf is not None:
            return gdf
        else:
            raise IOError('No datasets found')
    # if gdf is not upto date
    bb = []
    for dsname in names:
        ds = open_dataset(dsname)
        bb.append(shapely.geometry.Polygon(bounding_box(ds)))
    new_df = geopandas.GeoDataFrame({'dataset': names},
                                    crs='epsg:4326',
                                    geometry=bb)
    new_df = geopandas.pd.concat([gdf, new_df]).reset_index(drop=True)
    # save new file
    new_df.to_file(os.path.join(DATA_PATH, '_inventory.gpkg'), driver="GPKG")
    return new_df


def open_dataset(dataset, chunks=None, mode=None):
    """
    Open a dataset
    """
    def open_hsi_dataset(dataset, chunks=None):
        def all_files(dataset):
            dpath = os.path.abspath(os.path.join(DATA_PATH, dataset, "DATA"))
            files = os.listdir(dpath)
            return [os.path.join(dpath, x) for x in files if x.endswith('.nc')]

        def add_band_dim(dataset):
            return dataset.expand_dims('band')
        # define chunks
        if chunks is None:
            chunks = {'band': 1, 'x': 1000, 'y': 1000}
        flist = all_files(dataset)
        ds = xarray.open_mfdataset(flist,
                                   preprocess=add_band_dim,
                                   chunks=10000).reflectance
        return ds.chunk(chunks)

    def open_image(dataset, chunks=None):
        if chunks is None:
            chunks = {'band': 1, 'x': 1000, 'y': 1000}
        flist = os.listdir(os.path.join(DATA_PATH, dataset, "DATA"))
        if len(flist) > 1:
            raise IOError('More than one file in image directory')
        return xarray.open_rasterio(os.path.join(DATA_PATH,
                                                 dataset,
                                                 "DATA",
                                                 flist[0]),
                                    chunks=chunks)
    if mode is None:
        try:
            return open_hsi_dataset(dataset, chunks)
        except IOError:
            return open_image(dataset, chunks)

    if mode == 'hsi':
        return open_hsi_dataset(dataset, chunks)

    if mode == 'rgb':
        return open_image(dataset, chunks)
