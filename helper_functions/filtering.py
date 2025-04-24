import numpy as np
import geopandas as gpd
from rasterio.features import geometry_mask


def handle_nodata(data, nodata_value):
    """
    Replace nodata values with NaN.
    
    Parameters:
        data (numpy.ndarray): The input raster data.
        nodata_value (float or int): The nodata value from raster metadata.
    
    Returns:
        numpy.ndarray: Data with nodata replaced by NaN.
    """
    if nodata_value is not None:
        return np.where(data == nodata_value, np.nan, data)
    return data


def get_data_range(data):
    """
    Return min and max values of the raster data, ignoring NaNs.
    
    Parameters:
        data (numpy.ndarray): The input data array.
    
    Returns:
        tuple: (min, max)
    """
    return np.nanmin(data), np.nanmax(data)


def get_raster_extent(src):
    """
    Compute the bounding box extent of a raster.
    
    Parameters:
        src (rasterio.io.DatasetReader): An open raster file.
    
    Returns:
        list: [lon_min, lon_max, lat_min, lat_max]
    """
    transform = src.transform
    height, width = src.height, src.width
    lon_min, lat_max = transform * (0, 0)
    lon_max, lat_min = transform * (width, height)
    return [lon_min, lon_max, lat_min, lat_max]


def load_shapefile(shapefile_path):
    """
    Load a shapefile using GeoPandas.
    
    Parameters:
        shapefile_path (str): Path to the .shp file.
    
    Returns:
        GeoDataFrame: Loaded shapefile as a GeoDataFrame.
    """
    return gpd.read_file(shapefile_path)
