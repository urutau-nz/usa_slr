'''
Pulls in the OSM road network where it intersects with the SLR scenarios
Returns a df to SQL of road segments ('from_osmid' and 'to_osmid') that will be closed for each scenario
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

def main_road():
    '''
    main function
    '''
    # get list of regions from SQL
    logger.error('Obtaining region list from SQL')
    cursor = db['con'].cursor()
    cursor.execute("SELECT DISTINCT region FROM slr_raw")
    completed_regions = list(pd.read_sql('SELECT DISTINCT(region) FROM exposed_roads', con)['region'].values)
    regions = [list(i)[0] for i in cursor.fetchall()]
    regions = [l for l in regions if not any(x in l for x in completed_regions)]
    cursor.close()
    # make list of slr rise
    rises = np.arange(0,11)
    # list of inundation scenarios
    inundations = ['slr', 'low']
    # regions = ['NY_Metro']
    for region in tqdm(regions):
        logger.error('Obtaining {} BBOX from SQL'.format(region))
        # get largest extent to use as a bbox for roads
        cursor = db['con'].cursor()
        cursor.execute("SELECT ST_Extent(geometry) as table_extent FROM slr_raw WHERE region = '{}'".format(region))
        bbox = [list(i)[0] for i in cursor.fetchall()]
        bbox = re.split(r'[(|)|\s|,]', bbox[0])
        cursor.close()
        # get total bounds
        xmin,ymin,xmax,ymax = float(bbox[1]), float(bbox[2]), float(bbox[3]), float(bbox[4])
        logger.error('Downloading {} region edges from OSMNX'.format(region))
        # get network 
        G = ox.graph_from_bbox(ymax, ymin, xmax, xmin, network_type='drive', simplify=False, retain_all=True, truncate_by_edge=True)
        # convert to gdf
        logger.error('Converting edges graph to GeoPandas')
        road_gdf = ox.utils_graph.graph_to_gdfs(G, nodes=False, node_geometry=False)
        # reset index to get from & to columns (v & u)
        road_gdf.reset_index(inplace=True)
        # drop rows with bridges
        if 'bridge' in road_gdf:
            road_gdf = road_gdf[(road_gdf['bridge'].isna()) | (road_gdf['bridge']=='no')]
        # drop unused columns
        road_gdf.drop(columns=['osmid', 'lanes', 'name', 'highway', 'oneway', 'length', 'ref', 'maxspeed', 'junction', 'access', 'tunnel', 'width', 'key', 'bridge'], errors='ignore', inplace=True)
        # rename columns
        road_gdf.rename(columns={"u": "to_osmid", "v": "from_osmid"}, inplace=True)
        # get length
        proj_roads = road_gdf.to_crs(3395)
        road_gdf['length'] = list(proj_roads.length.round(1))
        # send to tempory table in SQL
        logger.error('Writing roads to SQL as temp_roads')
        road_gdf.to_postgis('temp_roads', engine, if_exists='replace', dtype={'geometry': Geometry('LINESTRING', srid=crs)})
        logger.error('Roads written to SQL as temp_roads')
        for rise in tqdm(rises):
            for inundation in inundations:
                logger.error('Computing exposure for: {}-{}-{}'.format(region, rise, inundation))
                #conn = db['engine'].raw_connection()
                #cur = conn.cursor()
                cursor = db['con'].cursor()
                queries = ["SET work_mem = '500MB'", "SET max_parallel_workers = '8'", "SET max_parallel_workers_per_gather = '8'",
                    """CREATE TABLE IF NOT EXISTS exposed_roads
                        (from_osmid BIGINT,
                        to_osmid BIGINT,
                        length FLOAT,
                        rise INT,
                        inundation VARCHAR(255),
                        region VARCHAR(255));""", 
                    """INSERT INTO exposed_roads (from_osmid, to_osmid, length, rise, inundation, region)
                        WITH slr AS (SELECT geometry as geom FROM slr_raw WHERE region='{region}' AND rise='{rise}' AND inundation='{inundation}')
                        SELECT from_osmid, to_osmid, length, {rise}, '{inundation}', '{region}'
                        FROM temp_roads roads, slr
                        WHERE ST_Intersects(roads.geometry, slr.geom);
                        """.format(rise=rise, inundation=inundation, region=region)
                ]
                logger.error('Creating/Appending exposed roads to SQL table | {}-{}-{}'.format(region, rise, inundation))
                for q in queries:
                    cur.execute(q)
                conn.commit()
                logger.error('Table created')

    db['con'].close()
    logger.error('Database connection closed')




main_road()
