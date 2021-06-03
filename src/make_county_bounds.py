'''
Mitchell Anderson
03/06/2021

Takes counties from SQL and returns a buffered shapefile for each county into SQL
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
import numpy as np

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






def import_counties(db, crs):
    '''
    Imports city bounds from SQL
    '''
    print('IMPORTING COUNTIES FROM SQL')
    # pull buffered county bounds from SQL
    sql = 'SELECT * FROM county'
    df_counties = gpd.read_postgis(sql, con=db['con'], geom_col='geometry')
    # ensure df is correctly projected
    df_counties = df_counties.to_crs(crs)
    return(df_counties)


def make_bounding_boxes(df_counties, crs, buffer=0):
    print("CREATING BUFFERED BOUNDING BOXES")
    # reproject city to system with metres
    df_counties = df_counties.to_crs(3395)
    # make bounding boxes
    for i in np.arange(0,len(df_counties)):
        geoid = df_counties['geoid'].iloc[i]
        # get min/max co-ords
        xmin,ymin,xmax,ymax = df_counties[df_counties['geoid']==geoid].total_bounds
        # make into polygon
        county_bounds.append(shapely.geometry.box(xmin-buffer,ymin-buffer,xmax+buffer,ymax+buffer))
    # create geodataframe
    df_bounds = gpd.GeoDataFrame(county_bounds, columns=['geometry'], crs=3395)
    df_bounds = df_bounds.to_crs(crs)
    # add city ids
    df_bounds['geoid'] = list(df_counties['geoid'])
    df_bounds.to_postgis('county_buffered', engine, if_exists='replace', dtype={'geometry': Geometry('POLYGON', srid=crs)})
    return(df_bounds)




# import counties
county_bounds = []
df_counties = import_counties(db, crs)
# make bounding boxes
df_bounds = make_bounding_boxes(df_counties, crs, buffer=5000)