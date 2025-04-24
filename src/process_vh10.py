import sys
import os
import numpy as np

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
HELPER_FUNCTIONS_PATH = os.path.join(CURRENT_DIR, '..', 'helper_functions')
sys.path.append(HELPER_FUNCTIONS_PATH)

from load_raster_data import load_raster, read_band, close_raster
from filtering import handle_nodata, get_data_range
from processing import plot_raster, save_raster


class RasterProcessor:
    def __init__(self, region):
        self.region = region
        self.input_path, self.output_path = self.get_file_paths(region)
        self.src = None
        self.data = None
        self.data_cleaned = None
        self.profile = None

    def get_file_paths(self, region):
        input_path = f"data/Vh_log10_{region}.tif"
        output_path = f"results/{region}_output.tif"
        return input_path, output_path

    def load_data(self):
        self.src = load_raster(self.input_path)
        self.data = read_band(self.src, band=1)
        self.profile = self.src.profile

    def clean_data(self):
        nodata = self.src.nodata
        self.data_cleaned = handle_nodata(self.data, nodata)

    def analyze(self):
        min_val, max_val = get_data_range(self.data_cleaned)
        print(f"[{self.region}] Value range: {min_val:.2f} to {max_val:.2f}")

    def visualize(self):
        plot_raster(self.data_cleaned, title=f"{self.region.capitalize()} Raster")

    def save(self):
        self.profile.update(dtype="float32", nodata=None)
        save_raster(self.output_path, self.data_cleaned.astype("float32"), self.profile)

    def cleanup(self):
        close_raster(self.src)

    def run(self, show_plot=True):
        self.load_data()
        self.clean_data()
        self.analyze()
        if show_plot:
            self.visualize()
        self.save()
        self.cleanup()


if __name__ == "__main__":
    region = "myanmar"
    # region = "pair1"
    # region = "pair2"
    # region = "pair20"
    
    processor = RasterProcessor(region)
    processor.run()
