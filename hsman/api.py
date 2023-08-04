import geopandas
import os
import pandas as pd
import pyproj
import shapely
import xarray
import rioxarray
from .config import DATA_PATH
import warnings


def get_datasets():
    """
    Return a pandas dataframe of bounding boxes
    """
    def bounding_box(ds):
        """
        Retrieve bounding box in lon/lat
        """
        # transformer from imagery projection to lat lon
        transformer = pyproj.Transformer.from_crs(ds.rio.crs, "epsg:4326")
        tl = transformer.transform(ds.x.min(), ds.y.max())[::-1]
        tr = transformer.transform(ds.x.max(), ds.y.max())[::-1]
        bl = transformer.transform(ds.x.min(), ds.y.min())[::-1]
        br = transformer.transform(ds.x.max(), ds.y.min())[::-1]
        return [tl, tr, br, bl, tl]

    def auto_generate_fields(ds):
        ds['ID'] = ds.dataset.apply(lambda x: x.split('_')[0])
        ds['Site'] = ds.dataset.apply(lambda x: x.split('_')[0][:5])
        ds['type'] = ds.dataset.apply(lambda x: x.split('_')[1])
        ds['date'] = ds.dataset.apply(lambda x: x.split('_')[0][5:13])
        ds['date'] = pd.to_datetime(ds['date'])
        return ds.sort_values(['Site', 'type'])
    # get all names that don't start with _ in data dir
    names = os.listdir(DATA_PATH)
    names = [x for x in names if not x.startswith(('_', '.'))]

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            gdf = geopandas.read_file(os.path.join(DATA_PATH,
                                                   '.inventory.gpkg'))
        # gdf = gdf.set_index('dataset')
        # filter any names already present in gpkg
        names = [x for x in names if not (gdf.dataset == x).any()]
    except geopandas.io.file.fiona.errors.DriverError:
        # no file so all names need to be parsed
        gdf = None
        pass
    if len(names) < 1:
        if gdf is not None:
            try:
                return auto_generate_fields(gdf.reset_index())
            except:
                return gdf.reset_index()
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
    try:
        new_df.to_file(os.path.join(DATA_PATH, '.inventory.gpkg'),
                       driver="GPKG",
                       layer='dataset_bounding_boxes'
                       )
    except:
        print('New database not saved')
    try:
        return auto_generate_fields(new_df)
    except:
        return new_df


def open_dataset(dataset, chunks=None, mode=None):
    """
    Open a dataset
    """
    def get_hsi_path(dataset):
        dpath = os.path.abspath(os.path.join(DATA_PATH, dataset, "DATA"))
        files = os.listdir(dpath)
        return [os.path.join(dpath, x) for x in files if x.endswith('.nc')]

    def get_rgb_path(dataset):
        dpath = os.path.join(DATA_PATH, dataset, "DATA")
        flist = os.listdir(dpath)
        if len(flist) > 1:
            raise IOError('More than one file in image directory')
        else:
            return os.path.join(dpath, flist[0])

    def open_hsi_dataset(dataset, chunks=None):

        def set_crs(ds):
            try:
                crs_is_valid = ds.rio.crs.is_valid
            except AttributeError:
                crs_is_valid = False

            # if CRS is valid, return unchanged
            if crs_is_valid:
                return ds

            # iterate until a CRS is found
            for d in ds.data_vars:
                for name, val in ds[d].attrs.items():
                    if 'crs' in name.lower():
                        ds = ds.rio.set_crs(val)
                        return ds

            raise AttributeError('No CRS found!')

        def read_hsi_v1(flist):
            # works with original version
            def add_band_dim(dataset):
                return dataset.expand_dims('band')
            # define chunks
            chunks = {'band': 1, 'x': 10000, 'y': 10000}
            ds = xarray.open_mfdataset(flist,
                                       preprocess=add_band_dim,
                                       chunks=10000)
            # try to set crs
            return ds.chunk(chunks)

        def read_hsi_v2(flist):
            # works with the newer version
            ds = xarray.open_mfdataset(flist,
                                       chunks={'band':1, 'y':10000, 'x':10000}
                                       )
            ds = ds.assign_coords(
                {'wavelength': ('band', ds.wavelength.values)}
                )
            return ds

        flist = get_hsi_path(dataset)

        # try original version first
        try:
            ds = read_hsi_v1(flist)
        except ValueError:
            ds = read_hsi_v2(flist)

        return set_crs(ds)


    def open_image(dataset, chunks=None):
        if chunks is None:
            chunks = {'band': 1, 'x': 10000, 'y': 10000}
        fpath = get_rgb_path(dataset)
        return rioxarray.open_rasterio(fpath,
                                    chunks=chunks)
    if mode is None:
        try:
            return open_hsi_dataset(dataset, chunks)
        except IOError:
            return open_image(dataset, chunks)

    # returns path instead of dataset
    if mode == 'path':
        try:
            open_hsi_dataset(dataset, chunks)
            return get_hsi_path(dataset)
        except IOError:
            return get_rgb_path(dataset)

    if mode == 'hsi':
        return open_hsi_dataset(dataset, chunks)

    if mode == 'rgb':
        return open_image(dataset, chunks)


def view_datasets():
    """
    View available datasets on a folium map
    """
    try:
        import folium
    except ModuleNotFoundError:
        raise RuntimeError("'folium' package must be installed to call this function")

    # prepare dataset for plotting
    dsets = get_datasets().drop(labels='date', axis='columns')
    # dsets.reset_index(inplace=True)
    dsets['dataset_type'] = dsets['dataset'].apply(lambda x: x.split('_')[1])

    dsets2 = dsets.copy()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        dsets2['geometry'] = dsets2.geometry.centroid
    dsets2 = dsets2.set_geometry('geometry')

    def cstyle(x):
        cols = {
            'SWIR': '#1b9e77',
            'VNIR': '#d95f02',
            'DSM': '#7570b3',
            'DTM': '#e7298a',
            'RGB': '#66a61e'
        }
        try:
            c = cols[x['properties']['dataset_type']]
        except KeyError:
            c = '#000000'
        return {'fillColor': c, 'color': c}

    # gjson = hsman.get_datasets().reset_index().to_json()
    m = folium.Map(location=[52.7, -2],
                   zoom_start=6,
                   # tiles=maplayer,
                   height='80%',
                   attr='ESRI Aerial')

    names = ['Shortwave IR (Hyspex)',
             'Vis-NIR (Hyspex)',
             'RGB aerial',
             'Digital Surface Model',
             'Digital Terrain Model']

    for _dk, _name in zip(['SWIR', 'VNIR', 'RGB', 'DSM', 'DTM'], names):
        _ds = dsets[dsets['dataset_type'] == _dk].to_json()
        _ds2 = dsets2[dsets2['dataset_type'] == _dk].to_json()
        _layer2 = folium.GeoJson(_ds2, name=_name, style_function=cstyle,
                                 zoom_on_click=True)
        _layer = folium.GeoJson(_ds, name=_name, style_function=cstyle,
                                zoom_on_click=True)
        folium.features.GeoJsonPopup(['dataset']).add_to(_layer2)
        m.add_child(_layer2)
        m.add_child(_layer)
    folium.LayerControl(collapsed=False, ).add_to(m)
    return m
