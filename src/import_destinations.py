'''
Mitchell Anderson
03/06/2021

Takes destination files (csv, shapefiles, etc.) and loads them into SQL
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
from shapely.geometry import Point

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
    sql = 'SELECT * FROM county_buffered'
    df_counties = gpd.read_postgis(sql, con=db['con'], geom_col='geometry')
    # ensure df is correctly projected
    df_counties = df_counties.to_crs(crs)
    return(df_counties)

# import county bounds
df_counties = import_counties(db, crs)
df_bounds = df_counties


###
# Schools
###
# import
schools_private = gpd.read_file('./data/raw/destinations/Private_Schools.shp')
schools_public = gpd.read_file('./data/raw/destinations/PublicSchools.shp')
schools_private = schools_private.to_crs(crs)
schools_public = schools_public.to_crs(crs)

# filter
schools_private['START_GRAD'] = schools_private['START_GRAD'].astype(int)
schools_private['END_GRADE'] = schools_private['END_GRADE'].astype(int)
schools_public.loc[schools_public['ST_GRADE'].str.contains('|'.join(['K','M','N','U', 'A'])),'ST_GRADE'] = '0'
schools_public['ST_GRADE'] = schools_public['ST_GRADE'].astype(int)
schools_public.loc[schools_public['END_GRADE'].str.contains('|'.join(['K','M','N','U', 'A'])),'END_GRADE'] = '0'
schools_public['END_GRADE'] = schools_public['END_GRADE'].astype(int)
# select schools that teach children aged 6-11
schools_private = schools_private[(schools_private['START_GRAD']<6) & (schools_private['END_GRADE']>1)]
schools_public = schools_public[(schools_public['ST_GRADE']<6) & (schools_public['END_GRADE']>1)]

# merge
schools = schools_private['geometry'].append(schools_public['geometry'])
schools = gpd.GeoDataFrame(geometry=schools)
# identify geoid
schools = gpd.sjoin(schools, df_bounds, how='inner', op='within')

# get the max id_dest
# sql = "SELECT MAX(id_dest) FROM destinations;"
# id_dest_max = pd.read_sql(sql, db['con']).values[0][0]
id_dest_max = -1

# export
schools['id_dest'] = np.arange(id_dest_max+1, id_dest_max+1+len(schools))
schools['dest_type'] = 'primary_school'
schools = schools[['id_dest','geoid','dest_type','geometry']]
schools.to_postgis('destinations', engine, if_exists='append', dtype={'geometry': Geometry('POINT', srid=crs)})
logger.info('Schools added SQL')

###
# Fire Stations
###
# import
fire_stations = gpd.read_file('./data/raw/destinations/Fire_Stations.shp')
fire_stations = fire_stations.to_crs(crs)

# identify geoid
fire_stations = gpd.sjoin(fire_stations, df_bounds, how='inner', op='within')

# get the max id_dest
sql = "SELECT MAX(id_dest) FROM destinations;"
id_dest_max = pd.read_sql(sql, db['con']).values[0][0]

# export
fire_stations['id_dest'] = np.arange(id_dest_max+1, id_dest_max+1+len(fire_stations))
fire_stations['dest_type'] = 'fire_station'
fire_stations = fire_stations[['id_dest', 'geoid', 'dest_type', 'geometry']]
fire_stations.to_postgis('destinations', engine, if_exists='append', dtype={'geometry': Geometry('POINT', srid=crs)})
logger.info('fire_station added SQL')

###
# Pharmacies
###
# import
pharmacies = pd.read_csv('./data/raw/destinations/facility.csv')

# filter
pharmacies = pharmacies[pharmacies['Type'] =='Pharmacy']
pharmacies[['lat','lon']] = pharmacies['CalcLocation'].str.split(',', expand=True).astype('float')
pharmacies = gpd.GeoDataFrame(pharmacies, crs=crs, geometry=[Point(xy) for xy in zip(pharmacies.lon,pharmacies.lat)])

# identify geoid
pharmacies = gpd.sjoin(pharmacies, df_bounds, how='inner', op='within')

# get the max id_dest
sql = "SELECT MAX(id_dest) FROM destinations;"
id_dest_max = pd.read_sql(sql, db['con']).values[0][0]

# export
pharmacies['id_dest'] = np.arange(id_dest_max+1, id_dest_max+1+len(pharmacies))
pharmacies['dest_type'] = 'pharmacy'
pharmacies = pharmacies[['id_dest', 'geoid', 'dest_type', 'geometry']]
pharmacies.to_postgis('destinations', engine, if_exists='append', dtype={'geometry': Geometry('POINT', srid=crs)})
logger.info('pharmacies added SQL')

###
# Emergency Medical Services
###
# import
hospitals = gpd.read_file('./data/raw/destinations/Hospitals.shp')
urgent_care = gpd.read_file('./data/raw/destinations/UrgentCareFacs.shp')
hospitals = hospitals.to_crs(crs)
urgent_care = urgent_care.to_crs(crs)

# merge
emergency_medical_services = hospitals['geometry'].append(urgent_care['geometry'])
emergency_medical_services = gpd.GeoDataFrame(geometry=emergency_medical_services)
# identify geoid
emergency_medical_services = gpd.sjoin(emergency_medical_services, df_bounds, how='inner', op='within')

# get the max id_dest
sql = "SELECT MAX(id_dest) FROM destinations;"
id_dest_max = pd.read_sql(sql, db['con']).values[0][0]

# export
emergency_medical_services['id_dest'] = np.arange(id_dest_max+1, id_dest_max+1+len(emergency_medical_services))
emergency_medical_services['dest_type'] = 'emergency_medical_service'
emergency_medical_services = emergency_medical_services[['id_dest','geoid','dest_type','geometry']]
emergency_medical_services.to_postgis('destinations', engine, if_exists='append', dtype={
                   'geometry': Geometry('POINT', srid=crs)})
logger.info('emergency_medical_services added SQL')
