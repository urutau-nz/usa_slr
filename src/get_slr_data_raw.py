'''
Download and unzip the slr data
'''

import json
import requests
import zipfile
import os
# import wget
from tqdm import tqdm
import main
import geopandas as gpd
import pandas as pd
import psycopg2
import yaml
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry import shape, JOIN_STYLE
from geoalchemy2 import Geometry, WKTElement
import fiona
import shapely
import re
import code
import multiprocessing as mp
from joblib import Parallel, delayed

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

data = next(os.walk('./data/raw/slr_extent'))[1]
complete_regions = list(pd.read_sql('SELECT DISTINCT(region) FROM slr_raw', con)['region'].values)
data = [l for l in data if not any(x in l for x in complete_regions)]

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
    logger.info("There were {} Multipolygons found and exploded".format(count_mp))
    return outdf

# create connection
conn = db['engine'].raw_connection()
cur = conn.cursor()


for row in tqdm(data):  # reversed(data):
    name = row.strip('_slr_final_dist.gdb')
    logger.info('Working on {}'.format(name))
    fng = './data/raw/slr_extent/' + row
    layers = fiona.listlayers(fng)
    for layer in tqdm(layers):
        logger.info('Layer: {}'.format(layer))
        gdf = gpd.read_file(fng, driver='OpenFileGDB', layer=layer)
        layer_crs = gdf.crs
        print(layer_crs)
        # remove z coordinate
        if gdf.geometry.has_z.sum() > 0:
            for i in range(len(gdf)):
                gdf.geometry[i] = shapely.ops.transform(
                    lambda *args: args[:2], gdf.geometry[i])
        # explode multipolygons
        gdf = _explode(gdf)
        gdf.set_crs(layer_crs, inplace=True)
        gdf.to_crs(crs, inplace=True)
        # load into postgis
        # prepare to save
        layer_numbers = [s.strip('ft') for s in layer.split('_') if 'ft' in s]
        assert len(layer_numbers) == 1, 'too many numbers in layer name'
        rise = int(layer_numbers[0])
        gdf['rise'] = rise
        gdf['region'] = name
        gdf['layer'] = layer
        if 'low' in layer:
            inundation = 'low'
        else:
            inundation = 'slr'
        gdf['inundation'] = inundation
        gdf = gdf[['geometry','rise','inundation', 'region', 'layer']]
        # # save
        gdf.to_postgis('slr_raw', engine, if_exists='append',
            dtype={'geometry': Geometry('POLYGON', srid=crs)})
        con.commit()

print('slr written to PSQL')
db['con'].close()


