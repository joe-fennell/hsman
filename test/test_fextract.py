import xarray


def generate_test(y=10, x=20, bands=3):
    """
    Generate a test xarray
    """
    return xarray.DataArray(np.arange(x*y*bands).reshape((y,x,bands)),
                           coords=[
                               ('y', np.arange(y)),
                               ('x', np.arange(y, y+x)),
                               ('band', np.arange(bands))
                           ])
