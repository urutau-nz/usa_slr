'''
Create the "blocks20" table in SQL
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
# functions - logging
import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.CRITICAL,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# config
with open('./config/main.yaml') as file:
    config = yaml.load(file)

crs = config['set_up']['projection']
db = main.init_db(config)
con = db['con']
engine = db['engine']

# import the counties
sql = 'SELECT geoid as geoid_county, geometry FROM county'
df_county = gpd.read_postgis(sql, con=con, geom_col='geometry')
df_county = df_county.to_crs(crs)
logger.info('Counties imported')

# import the blocks
df_blocks = gpd.read_file('/homedirs/projects/data/usa/tlgdb_2020_a_us_block.gdb',
                          driver='FileGDB', layer=0, mask=df_county)
df_blocks = df_blocks[['GEOID', 'geometry']]
df_blocks = df_blocks.to_crs(crs)
logger.info('Blocks imported')

# code.interact(local=locals())

# determine the centroid of the blocks
df_blocks['centroid'] = df_blocks.representative_point()
df_blocks.set_geometry('centroid', inplace=True)
logger.info('Centroids found')


# get the blocks within the cities
df_blocks_select = gpd.sjoin(df_blocks, df_county, how='inner', op='within')
df_county = None
df_blocks_select.set_geometry('geometry', inplace=True)
logger.info('Removed blocks not within counties of interest')

# calculate the area of the blocks
df_blocks_select = df_blocks_select.to_crs(3395)
df_blocks_select['area'] = df_blocks_select['geometry'].area / 10**6
df_blocks_select = df_blocks_select.to_crs(crs)
logger.info('Calculate block area')


# merge with block demographic data
# total population, white alone, black alone, hispanic or latino, number of housing units
variables = ['U7B001', 'U7B003', 'U7B004', 'U7C002', 'U7G001']
df_info = pd.read_csv(
    '/homedirs/projects/data/usa/nhgis0086_csv/nhgis0086_ds248_2020_block.csv', encoding="ISO-8859-1",
    usecols=['STATEA', 'COUNTYA', 'TRACTA', 'BLOCKA'] + variables,
    dtype={x: 'str' for x in ['STATEA', 'COUNTYA', 'TRACTA', 'BLOCKA']})
# create the geoid for merge
df_info['GEOID'] = df_info['STATEA'] + df_info['COUNTYA'] + df_info['TRACTA'] + df_info['BLOCKA']
df_blocks_select = df_blocks_select.merge(df_info, on='GEOID')
logger.info('Block variables found')

# calculate the density
df_blocks_select['ppl_per_km2'] = df_blocks_select['U7B001']/df_blocks_select['area']
df_blocks_select['dwellings_per_km2'] = df_blocks_select['U7G001'] / df_blocks_select['area']

# set index
df_blocks_select = df_blocks_select.rename(columns={'GEOID': 'geoid'})

df_write = df_blocks_select[['geoid', 'geoid_county', 'geometry',
                          'area', 'ppl_per_km2', 'dwellings_per_km2'] + variables]
# df_write.rename({'polygon':'geometry'}, inplace=True)

# export to sql
df_write.to_postgis('blocks20', engine, if_exists='replace', dtype={
    'geometry': Geometry('MULTIPOLYGON', srid=crs)}
)
logger.info('Blocks20 written to PSQL')

df_write = df_blocks_select[['geoid', 'geoid_county', 'centroid', 'U7B001']]
df_write.rename({'centroid': 'geometry'}, inplace=True)
df_write = df_write.set_geometry('centroid')
df_write = df_write.to_crs(crs)
df_write.to_postgis('origins20', engine, if_exists='replace', dtype={
    'centroid': Geometry('POINT', srid=crs)}
)
logger.info('Blocks centroids (origins) written to PSQL')


db['con'].close()
