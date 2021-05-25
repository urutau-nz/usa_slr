'''
Mitchell Anderson
24/05/21

Collect road network for the specified region;
Filter by road segments (ids) already determined in exposed roads table;
Save to sql as a temp_validate shapefile;
Use this to compare with slr extents in QGIS.
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


# filter inputs
region = 'HI_Mau'
rise = 10
inundation = 'slr'

# get road network
logger.error('Obtaining {} BBOX from SQL'.format(region))
# get largest extent to use as a bbox for roads
cursor = db['con'].cursor()
cursor.execute("SELECT ST_Extent(geometry) as table_extent FROM slr_raw WHERE region = '{}' AND inundation = '{}' AND rise = '{}'".format(region, inundation, rise))
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

# add column for exposed or not
# get exposed IDs from sql
sql = "SELECT * FROM exposed_roads WHERE region = '{}' AND inundation = '{}' AND rise = '{}'".format(region, inundation, rise)
df = pd.read_sql(sql, db['con'])
# add column with both ids
df['joint_id'] = [''.join(a) for a in zip(list(df['from_osmid'].astype(str)), list(df['to_osmid'].astype(str)))]
road_gdf['joint_id'] = [''.join(a) for a in zip(list(road_gdf['from_osmid'].astype(str)), list(road_gdf['to_osmid'].astype(str)))]
# add column to road_gdf where joint_ids match
road_gdf.loc[list(road_gdf[road_gdf['joint_id'].isin(df['joint_id'])].index), 'exposed'] = True

# OR, for loop through
# road_gdf['exposed'] = None
# for i in tqdm(np.arange(0, len(df))):
#     from_id = df['from_osmid'].iloc[i]
#     to_id = df['to_osmid'].iloc[i]
#     exposed_id = road_gdf.loc[(road_gdf['from_osmid'] == from_id) & (road_gdf['to_osmid'] == to_id)].index[0]
#     road_gdf.loc[exposed_id,'exposed'] = True

# get length
proj_roads = road_gdf.to_crs(3395)
road_gdf['length'] = list(proj_roads.length.round(1))
# send to tempory table in SQL
logger.error('Writing roads to SQL as temp_roads')
road_gdf.to_postgis('temp_roads_validate', engine, if_exists='replace', dtype={'geometry': Geometry('LINESTRING', srid=crs)})
logger.error('Roads written to SQL as temp_roads')


