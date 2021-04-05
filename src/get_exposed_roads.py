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
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


crs = config['set_up']['projection']
db = main.init_db(config)
con = db['con']
engine = db['engine']

def main_road():
    '''
    main function
    '''
    # get list of regions from SQL
    cursor = db['con'].cursor()
    cursor.execute("SELECT DISTINCT region FROM slr_old")
    regions = [list(i)[0] for i in cursor.fetchall()]
    cursor.close()
    # make list of slr rise
    rises = np.arange(0,11)
    # list of inundation scenarios
    inundations = ['low', 'slr']
    for region in tqdm(regions):
        # get largest extent to use as a bbox for roads
        cursor = db['con'].cursor()
        cursor.execute("SELECT ST_Extent(geometry) as table_extent FROM slr_old WHERE region = '{}'".format(region))
        bbox = [list(i)[0] for i in cursor.fetchall()]
        bbox = re.split(r'[(|)|\s|,]', bbox[0])
        cursor.close()
        # get total bounds
        xmin,ymin,xmax,ymax = float(bbox[1]), float(bbox[2]), float(bbox[3]), float(bbox[4])
        # get network
        G = ox.graph_from_bbox(ymax, ymin, xmax, xmin, network_type='drive', simplify=False, retain_all=True, truncate_by_edge=True)
        # convert to gdf
        road_gdf = ox.utils_graph.graph_to_gdfs(G, nodes=False, node_geometry=False)
        # reset index to get from & to columns (v & u)
        road_gdf.reset_index(inplace=True)
        # drop unused columns
        road_gdf = road_gdf.drop(columns=['osmid', 'lanes', 'name', 'highway', 'oneway', 'length', 'ref', 'maxspeed', 'junction', 'access', 'tunnel', 'width', 'service', 'key'])
        for rise in tqdm(rises):
            for inundation in inundations:
                # pull in slr
                sql = "SELECT * FROM slr_old WHERE rise = '{}' AND inundation = '{}' AND region = '{}'".format(rise, inundation, region)
                extent = gpd.GeoDataFrame.from_postgis(sql, db['con'], geom_col='geometry')
                # clip with slr
                exposed_roads = gpd.overlay(road_gdf, extent, how='intersection')
                # drop rows with bridges
                exposed_roads = exposed_roads[exposed_roads['bridge'].isna()]
                # calc length of road
                exposed_roads = exposed_roads.to_crs(3395)
                exposed_roads['length'] = list(exposed_roads.length.round(1))
                # drop geom
                exposed_roads = pd.DataFrame(exposed_roads.drop(columns=['geometry', 'bridge']))
                # rename columns
                exposed_roads = exposed_roads.rename(columns={"u": "to_osmid", "v": "from_osmid"})
                # save to SQL
                exposed_roads.to_sql('exposed_roads', engine, if_exists='append')


main_road()
