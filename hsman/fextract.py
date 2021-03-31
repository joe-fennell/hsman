"""
Feature extraction submodule
"""
from .xarray_utils import get_transform, crop_to_dataframe

class RegionPatchExtractor(object):
    """
    Extractor object that compiles a 4D array of patches suitable for
    training testing regimes.

    For each region, patches (spatial domain) are allocated over the polygons.


    Parameters
    ----------
    patch_size : int, optional
        the length of each patch edge in pixels. Default 5 (5X5 patch)
    allow_patch_overlap : bool, optional
        If True, the regions are oversampled akin to a rolling window. If False,
        all pixels are guaranteed only present once. Default False.
        Not implemented.
    allow_edge_overlap : bool, optional
        If True, patches overlapping the edge of a region are retained
        with data from outside the region. Default is False.
        Not implemented.

    Methods
    -------
    """

    def __init__(self, patch_size=5, allow_patch_overlap=False,
                 allow_edge_overlap=False):
        # initialise parameters
        self.patch_size = patch_size
        self.allow_patch_overlap = allow_patch_overlap
        self.allow_edge_overlap = allow_edge_overlap

        # initialise attributes
        self.data = None
        self.geometry = None
        self.geometry_dataframe = None

    def extract(self, geometry_dataframe, hyperspectral_dataset):
        """
        Performs the extraction operation on a hsman hsi dataset
        """
        # check dataframe and dataset have CRS
        raise NotImplementedError
        assert hyperspectral_dataset.crs is not None
        assert geometry_dataframe.crs is not None
        # convert geometry_dataframe to same crs as hsi
        geometry_dataframe = geometry_dataframe.to_crs(
            hyperspectral_dataset.crs)
        hsi = crop_to_dataframe(hyperspectral_dataset,
                                geometry_dataframe)
        if (len(hsi.x) == 0) | (len(hsi.y) == 0):
            raise RuntimeError('The geometries did not overlap the dataset')
