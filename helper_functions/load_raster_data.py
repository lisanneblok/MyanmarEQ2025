
import rasterio

def load_raster(file_path):
    """Load raster data from file."""
    src = rasterio.open(file_path)
    print("Metadata:", src.meta)
    return src

def read_band(src, band=1):
    """Read a specific band from the raster."""
    return src.read(band)

def close_raster(src):
    """Close the raster file."""
    src.close()
