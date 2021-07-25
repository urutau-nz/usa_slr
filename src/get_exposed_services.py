'''
Finds services where it intersects with the SLR scenarios
Returns a df to SQL of services that will be closed for each scenario
'''

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
import osmnx as ox
from shapely.ops import unary_union
import numpy as np

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

def main():
    '''
    main function
    '''
    # # get list of regions from SQL
    # logger.error('Obtaining region list from SQL')
    # cursor = db['con'].cursor()
    # cursor.execute("SELECT DISTINCT region FROM slr_raw")
    # regions = [list(i)[0] for i in cursor.fetchall()]
    # cursor.close()
    # # make list of slr rise
    rises = np.arange(0,11)
    # list of inundation scenarios
    inundations = ['slr', 'low']
    # for region in tqdm(regions):
    #     logger.error('Obtaining {} BBOX from SQL'.format(region))
    #     # get largest extent to use as a bbox for roads
    #     cursor = db['con'].cursor()
    #     cursor.execute("SELECT ST_Extent(geometry) as table_extent FROM slr_raw WHERE region = '{}'".format(region))
    #     bbox = [list(i)[0] for i in cursor.fetchall()]
    #     bbox = re.split(r'[(|)|\s|,]', bbox[0])
    #     cursor.close()

    for rise in tqdm(rises):
        for inundation in inundations:
            logger.error('Computing exposure for: {}-{}'.format(rise, inundation))
            #conn = db['engine'].raw_connection()
            #cur = conn.cursor()
            cursor = db['con'].cursor()
            queries = ["SET work_mem = '500MB'", "SET max_parallel_workers = '8'", "SET max_parallel_workers_per_gather = '8'",
                """CREATE TABLE IF NOT EXISTS exposed_destinations
                    (geoid VARCHAR(255),
                    id_dest BIGINT,
                    dest_type VARCHAR(255),
                    rise INT,
                    inundation VARCHAR(255));""", 
                """INSERT INTO exposed_destinations (geoid, id_dest, dest_type, rise, inundation)
                    WITH slr AS (SELECT geometry as geom FROM slr_raw WHERE rise='{rise}' AND inundation='{inundation}')
                    SELECT geoid, id_dest, dest_type, {rise}, '{inundation}'
                    FROM destinations, slr
                    WHERE ST_Intersects(destinations.geometry, slr.geom);
                    """.format(rise=rise, inundation=inundation)
            ]
            logger.error('Creating/Appending exposed services to SQL table | {}-{}'.format(rise, inundation))
            for q in queries:
                cur.execute(q)
            conn.commit()
            logger.error('Table created')

    db['con'].close()
    logger.error('Database connection closed')

main()
