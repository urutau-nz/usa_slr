'''
Download and unzip the slr data
'''

import json
import requests
import zipfile
import os
import wget
from tqdm import tqdm
import main
import geopandas as gpd
import pandas as pd
import psycopg2
import yaml
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
from geoalchemy2 import Geometry, WKTElement
import fiona
import shapely
import re

# config
with open('./config/main.yaml') as file:
    config = yaml.load(file)
# functions - logging
import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


crs = config['set_up']['projection']
db = main.init_db(config)
con = db['con']
engine = db['engine']

# f = open("data/slr_source.json") # from https://coast.noaa.gov/slrdata/js/controllers.js
# data = json.load(f)
data = next(os.walk('./data/raw/slr_extent'))[1]


# import code
# code.interact(local=locals())

def _explode(indf):
    # https://stackoverflow.com/a/64897035/5890574
    count_mp = 0
    outdf = gpd.GeoDataFrame(columns=indf.columns)
    outdf = indf[indf.geometry.type == 'Polygon']
    indf = indf[indf.geometry.type != 'Polygon']
    for idx, row in indf.iterrows():
        if type(row.geometry) == MultiPolygon:
            count_mp = count_mp + 1
            multdf = gpd.GeoDataFrame(columns=indf.columns)
            recs = len(row.geometry)
            multdf = multdf.append([row]*recs,ignore_index=True)
            for geom in range(recs):
                multdf.loc[geom,'geometry'] = row.geometry[geom]
            outdf = outdf.append(multdf,ignore_index=True)
        else:
            print(row)
    print("There were ", count_mp, "Multipolygons found and exploded")
    return outdf


for row in data[10:]:
    name = row.strip('_slr_final_dist.gdb')
    logger.info('Working on {}'.format(name))
    # url = row['slrurl']
    # print('https:'+url)
    # fnzip = wget.download('https:'+url, './data/raw/slr_extent/')
    # with zipfile.ZipFile(fnzip, "r") as zip_ref:
        # fn = zip_ref.namelist()[0]
        # zip_ref.extractall("./data/raw/slr_extent/")
    # os.remove(fnzip)
    fng = './data/raw/slr_extent/' + row
    layers = fiona.listlayers(fng)
    for layer in tqdm(layers):
        gdf = gpd.read_file(fng, driver='OpenFileGDB', layer=layer)
        gdf = gdf.to_crs(3395)
        gdf['geometry'] = gdf['geometry'].simplify(50)
        gdf['geometry'] = gdf['geometry'].unary_union
        gdf = gdf.to_crs(crs)
        layer_numbers = [s.strip('ft') for s in layer.split('_') if 'ft' in s]
        assert len(layer_numbers)==1, 'too many numbers in layer name'
        gdf['rise'] = int(layer_numbers[0])
        gdf['region'] = name
        if 'low' in layer:
            gdf['inundation'] = 'low'
        else:
            gdf['inundation'] = 'slr'
        gdf = gdf[['geometry','rise','inundation', 'region']]
        if gdf.geometry.has_z.sum()>0:
            for i in range(len(gdf)):
                gdf.geometry[i] = shapely.ops.transform(lambda *args: args[:2], gdf.geometry[i])
        gdf = _explode(gdf)
        gdf.to_postgis('slr', engine, if_exists='append', 
            dtype={'geometry': Geometry('POLYGON', srid=crs)})

print('slr written to PSQL')
db['con'].close()
