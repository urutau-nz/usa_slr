'''
Create the "blocks" table in SQL
Get all of the US blocks and find the centroid for querying
'''

import code
import main
import geopandas as gpd
import pandas as pd
import psycopg2
import yaml
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
from geoalchemy2 import Geometry, WKTElement


# config
with open('./config/main.yaml') as file:
    config = yaml.load(file)
# functions - logging
import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.CRITICAL,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


crs = config['set_up']['projection']
db = main.init_db(config)
con = db['con']
engine = db['engine']
conn = db['engine'].raw_connection()
cur = conn.cursor()

# import the counties
df_county = gpd.read_file(
    './data/raw/nhgis0085_shape/US_county_2019.shp')
df_county = df_county.to_crs(crs)



df_county.rename(columns={'GEOID': 'geoid'}, inplace=True)

# add counties to temp table
logger.error('Loading counties into tmp table')
df_county.to_postgis('counties_tmp', engine, if_exists='replace', index_label='geoid', dtype={
    'geometry': Geometry('MULTIPOLYGON', srid=crs)}
)

code.interact(local=locals())

# select counties based on intersect with slr

#conn = db['engine'].raw_connection()
#cur = conn.cursor()
queries = ["SET work_mem = '500MB'", "SET max_parallel_workers = '8'", "SET max_parallel_workers_per_gather = '8'", 
        """CREATE TABLE county
        AS 
        (
            SELECT counties.geoid, counties."NAME" as name, counties.geometry 
            FROM 
                counties_tmp as counties,
                slr_raw as slr
            WHERE  
                ST_Intersects(counties.geometry, slr.geometry)        
        );
        """
            ]
logger.error('Selecting counties that intersect')
for q in queries:
    cur.execute(q)
db['con'].commit()
logger.error('Table created')

db['con'].close()
logger.error('Database connection closed')

