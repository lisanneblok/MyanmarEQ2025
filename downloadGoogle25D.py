# %% Run the following cell to initialize the API. The output will contain instructions on how to grant this notebook access to Earth Engine using your account.
# https://gorelick.medium.com/fast-er-downloads-a2abd512aa26
import ee
import geemap
import multiprocessing
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import norm, gamma, f, chi2
import pandas as pd
import IPython.display as disp
import json
import csv 
import os
import datetime
import requests
import shutil
from retry import retry
from datetime import datetime
from datetime import timedelta
import time
from osgeo import gdal
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
%matplotlib inline
ee.Authenticate()
ee.Initialize(opt_url='https://earthengine-highvolume.googleapis.com')
def ymdList(imgcol):
    def iter_func(image, newlist):
        date = ee.Number.parse(image.date().format("YYYYMMdd"));
        newlist = ee.List(newlist);
        return ee.List(newlist.add(date).sort())
    ymd = imgcol.iterate(iter_func, ee.List([]))
    return list(ee.List(ymd).reduce(ee.Reducer.frequencyHistogram()).getInfo().keys())
@retry(tries=10, delay=5, backoff=2)
def download_url(args):
    t0 = time.time()
    url = downloader(args[0],args[2])
    fn = args[1] 
    try:
        r = requests.get(url)
        with open(fn, 'wb') as f:
            f.write(r.content)
        return(url, time.time() - t0)
    except Exception as e:
        print('Exception in download_url():', e)
@retry(tries=10, delay=5, backoff=2)
def downloader(ee_object,region): 
    try:
        #download image
        if isinstance(ee_object, ee.image.Image):
            # print('Its Image')
            url = ee_object.getDownloadUrl({
                    'scale': 10, #463.831083333,
                    'crs': 'EPSG:4326',
                    'region': region,
                    'format': 'GEO_TIFF'
                })
            return url
        
        #download imagecollection
        elif isinstance(ee_object, ee.imagecollection.ImageCollection):
            print('Its ImageCollection')
            ee_object_new = ee_object.mosaic()
            url = ee_object_new.getDownloadUrl({
                    'scale': 10, #463.83108333310,
                    'crs': 'EPSG:4326',
                    'region': region,
                    'format': 'GEO_TIFF'
                })
            return url
    except:
        print("Could not download")
@retry(tries=10, delay=5, backoff=2)
def download_parallel(args):
    cpus = cpu_count()
    results = ThreadPool(cpus - 1).imap_unordered(download_url, args)
    for result in results:
        print('url:', result[0], 'time (s):', result[1])
t0 = time.time()
from datetime import datetime
from time import mktime

output_path = '/Users/joshuadimasaka/Desktop/PhD/GitHub/DeepTemporal/data/EO'

# %%
# check for city name here: https://code.earthengine.google.com/?scriptPath=Examples:Datasets/FAO/FAO_GAUL_2015_level2_FeatureView

# Pair 1
# aoiSAR = ee.Geometry.Polygon([
#     [
#         [99.432480, 13.718521],
#         [101.765335, 14.165195],
#         [102.060738, 12.657226],
#         [99.742905, 12.206713],
#         [99.432480, 13.718521]  # Closing the polygon
#     ]
# ])
# Pair 2
# aoiSAR = ee.Geometry.Polygon([
#     [101.6430, 13.5577],
#     [101.2588, 15.5157],
#     [98.9926, 15.2336],
#     [99.3970, 13.2724],
#     [101.6430, 13.5577]  # Closing the polygon
# ])
# Pair 5
aoiSAR = ee.Geometry.Polygon([
    [100.7570, 18.0904],
    [100.3772, 20.0487],
    [98.0518, 19.7729],
    [98.4593, 17.8121],
    [100.7570, 18.0904]  # Closing the polygon
])
# Pair 13
# polygon = ee.Geometry.Polygon([
#     [94.916862, 21.929922],
#     [92.477036, 22.355915],
#     [92.768730, 23.862478],
#     [95.236282, 23.439695],
#     [94.916862, 21.929922]  # Closing the polygon
# ])
# Pair 15
# polygon = ee.Geometry.Polygon([
#   [
#     [94.288246, 18.908279],
#     [91.898125, 19.340927],
#     [92.181412, 20.849600],
#     [94.594772, 20.420523],
#     [94.288246, 18.908279]
#   ]
# ]);


lsib = ee.FeatureCollection("FAO/GAUL/2015/level0")
fcollection = lsib.filterMetadata('ADM0_NAME','equals','Thailand')
aoi = ee.Geometry.MultiPolygon(fcollection.getInfo()['features'][0]['geometry']['coordinates'])

Map = geemap.Map()
Map.addLayer(aoi)
Map.addLayer(aoiSAR)
Map

# %%
result_path = '/Users/joshuadimasaka/Desktop/PhD/GitHub/MyanmarEQ2025-1/GoogleOpenBuildings/5/'
# %%
col = ee.ImageCollection('GOOGLE/Research/open-buildings-temporal/v1')

# %%
im = col.filterBounds(aoiSAR).sort('system:time_start', False).first().select('building_height').clip(aoiSAR)

nrow = 10
ncol = 10
fishnet = geemap.fishnet(aoiSAR, rows=nrow, cols=ncol)
nlist = fishnet.size().getInfo()
im_new = []
fn_new = []
rgn_new = []

for j in range(nlist):

    if not os.path.isfile(result_path+str(j+1)+"_of_"+str(nlist)+".tif") or (os.path.getsize(result_path+str(j+1)+"_of_"+str(nlist)+".tif")/(1<<10)) < 1 or not os.path.isfile(result_path+str(j+1)+"_of_"+str(nlist)+".tif") or (os.path.getsize(result_path+str(j+1)+"_of_"+str(nlist)+".tif")/(1<<10)) < 1:

        a = fishnet.toList(nlist).get(j).getInfo()
        im_new.append(im.clip(ee.Geometry.Polygon(a['geometry']['coordinates'])))
        fn_new.append(result_path+str(j+1)+"_of_"+str(nlist)+".tif")
        rgn_new.append(ee.Geometry.Polygon(a['geometry']['coordinates']))

download_parallel(zip(im_new, fn_new, rgn_new))
# %%
