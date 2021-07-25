'''
Create the "blocks" table in SQL
Get all of the US blocks and find the centroid for querying
'''

import code
import main
import geopandas as gpd
import pandas as pd
import numpy as np
import psycopg2
import yaml
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
from geoalchemy2 import Geometry, WKTElement
from tqdm import tqdm
import multiprocessing as mp
from joblib import Parallel, delayed


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


df_county['NAME'] = df_county['NAME'].str.replace(r"[\"\',]", '')

county_names = df_county['NAME'].unique()

# code.interact(local=locals())


df_county.rename(columns={'GEOID': 'geoid'}, inplace=True)
# df_county['exposed'] = 'Undefined'

# # add counties to temp table
logger.info('Loading counties into tmp table')
df_county.to_postgis('counties_tmp', engine, if_exists='replace', index_label='geoid', dtype={
    'geometry': Geometry('MULTIPOLYGON', srid=crs)}
)

# code.interact(local=locals())

# get the regions
# completed_counties = list(pd.read_sql('SELECT DISTINCT(name) FROM county', con)['name'].values)
# slr_regions = pd.read_sql(
#     'SELECT DISTINCT(region) FROM slr_raw', con)


# queries_region = """INSERT INTO county (geoid, name, geometry)
#             WITH slr AS (SELECT geometry as geom FROM slr_raw WHERE region='{region}' AND rise=10)
#             SELECT counties.geoid, counties."NAME" as name, counties.geometry 
#             FROM 
#                 counties_tmp as counties,
#                 slr
#             WHERE  
#                 ST_Intersects(counties.geometry, slr.geom)      
#         ;
#         """#.format(region=region)# for region in slr_regions['region']]
# select counties based on intersect with slr


#conn = db['engine'].raw_connection()
#cur = conn.cursor()
queries = ["SET work_mem = '500MB'", "SET max_parallel_workers = '8'", "SET max_parallel_workers_per_gather = '8'",
            """DROP TABLE IF EXISTS county""",
           """CREATE TABLE IF NOT EXISTS county
                        (geoid VARCHAR,
                        name VARCHAR,
                        geometry geometry);"""
            ]
# queries.extend(queries_region)
# queries.extend(["""DELETE FROM
#                         county a
#                         USING county b
#                     WHERE
#                         a.id < b.id
#                     AND a.geoid = b.geoid;
#     """])
logger.info('Selecting counties that intersect')
for q in queries:
    # logger.info(q)
    cur.execute(q)
conn.commit()
logger.info('Table created')

# code.interact(local=locals())
cur.close()

def find_counties(config, r):
    queries_region = """INSERT INTO county (geoid, name, geometry)
            WITH slr AS (SELECT geometry as geom FROM slr_raw WHERE rise=10)
            SELECT DISTINCT counties.geoid, counties."NAME" as name, counties.geometry 
            FROM 
                counties_tmp as counties,
                slr
            WHERE  
                ST_Intersects(counties.geometry, slr.geom)      
            AND
                counties."NAME" = '{}'
        ;
        """
    # queries_region = """WITH slr AS (SELECT geometry as geom FROM slr_raw WHERE rise=10)
    #         UPDATE counties_tmp
    #         SET counties_tmp.exposed = 'TRUE'
    #         WHERE  
    #             ST_Intersects(counties_tmp.geometry, slr.geom)      
    #         AND 
    #             counties_tmp."NAME" = '{}'
    #     ;
    #     """
    # queries_region = """WITH slr AS (SELECT geometry as geom FROM slr_raw WHERE rise=10)
    #         UPDATE counties_tmp
    #         SET counties_tmp.exposed = 'TRUE'
    #         WHERE  
    #             ST_Intersects(counties_tmp.geometry, slr.geom)      
    #         AND 
    #             counties_tmp."NAME" = '{}'
    #     ;
    #     """
    # queries_region = """INSERT INTO county (geoid, name, exposed, geometry)
    #     WITH slr AS (SELECT geometry as geom FROM slr_raw WHERE rise=10)
    #     SELECT DISTINCT counties.geoid, counties."NAME" as name, 
    #     CASE WHEN ST_Intersects(counties.geometry, slr.geom) THEN TRUE ELSE FALSE END exposed,
    #     counties.geometry 
    #     FROM 
    #         counties_tmp as counties,
    #         slr
    #     WHERE  
    #         counties."NAME" = '{}'
    # ;
    # """
    db = main.init_db(config)
    db['con'].close()
    conn = db['engine'].raw_connection()
    cur = conn.cursor()
    queries = ["SET work_mem = '1GB'", "SET max_parallel_workers = '8'", "SET max_parallel_workers_per_gather = '8'", queries_region.format(r)]
    for q in queries:
        cur.execute(q)
    conn.commit()
    cur.close()
    conn.close()

conn.close()
num_workers = np.int(mp.cpu_count() * config['par_frac'])
Parallel(n_jobs=num_workers)(delayed(find_counties)(config, r) for r in tqdm(county_names))

# for r in tqdm(slr_regions['region']):
#     queries = ["SET work_mem = '1GB'", "SET max_parallel_workers = '8'", "SET max_parallel_workers_per_gather = '8'", queries_region.format(region=r)]
#     for q in queries:
#         cur.execute(q)
#     conn.commit()

logger.info('Counties written')

# conn.close()
logger.info('Database connection closed')

# APPEND IF EXISTS. LOOP through the slr.region (python)
# Add counties that it intersects with
# drop duplicate counties
