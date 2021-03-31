import pyproj
import rasterio


def get_transform(DataArray):
    """
    Generates an affine transform for an xarray DataArray with x and y spatial
    dimensions with even grid spacing

    Parameters
    ----------
    DataArray : xarray.DataArray
        DataArray with x and y dimensions on an even grid

    Returns
    -------
    transform : rasterio.Affine
    """
    _trans = [float(DataArray.x[1] - DataArray.x[0]),
              0,
              float(DataArray.x[0]),
              0,
              float(DataArray.y[1] - DataArray.y[0]),
              float(DataArray.y[0])]
    return rasterio.Affine(*_trans)


def crop_to_valid(DataArray):
    """
    Slice a DataArray to include only the valid area

    Parameters
    ----------
    DataArray : xarray.DataArray
        DataArray with
    """
    # reduce size of mask if band present
    if 'band' in DataArray.dims:
        mask = ~DataArray.isel(band=0).isnull()
    else:
        mask = ~DataArray.isnull()
    # get data areas
    ys = DataArray.y[(mask.sum('x') > 0)]
    xs = DataArray.x[(mask.sum('y') > 0)]
    #slice and return
    return DataArray.sel(x=slice(xs[0], xs[-1]), y=slice(ys[0], ys[-1]))


def crop_to_dataframe(DataArray, GeoDataFrame):
    """
    Crop a DataArray to the limits of the geometry in a GeoDataFrame.

    Parameters
    ----------
    DataArray : xarray.DataArray
        data array with x and y spatial domains and a valid crs
    GeoDataFrame : geopandas.GeoDataFrame
        geopandas GeoDataFrame with valid geometry and crs
    """
    # project geometries to array
    GeoDataFrame = GeoDataFrame.to_crs(DataArray.crs)
    geom_bounds = GeoDataFrame.geometry.bounds
    xmin = float(geom_bounds.minx.min())
    xmax = float(geom_bounds.maxx.max())
    ymin = float(geom_bounds.miny.min())
    ymax = float(geom_bounds.maxy.max())
    return DataArray.sel(x=slice(xmin, xmax), y=slice(ymax, ymin))
