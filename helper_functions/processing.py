import matplotlib.pyplot as plt
import numpy as np


def plot_raster(data, title="Raster Data", cmap="gray"):
    """
    Plot raster data using matplotlib.
    
    Parameters:
        data (numpy.ndarray): 2D array of raster data.
        title (str): Plot title.
        cmap (str): Colormap to use.
    """
    fig, ax = plt.subplots(figsize=(10, 10))
    cax = ax.imshow(data, cmap=cmap)
    ax.set_title(title)
    ax.axis("off")  # Hide axes
    plt.colorbar(cax, ax=ax, shrink=0.6)
    plt.tight_layout()
    plt.show()


def save_raster(output_path, data, profile):
    """
    Save processed raster to a file.
    
    Parameters:
        output_path (str): File path to save the raster.
        data (numpy.ndarray): The raster data to save.
        profile (dict): Metadata profile for saving.
    """
    import rasterio
    with rasterio.open(output_path, 'w', **profile) as dst:
        dst.write(data, 1)
