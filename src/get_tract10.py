'''
Create the "blocks20" table in SQL
Get all of the US blocks and find the centroid for querying
'''
DC_STATEHOOD = 1
import us
from slack import post_message_to_slack
from pathlib import Path
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
    level=logging.INFO,
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
logger.error('Counties imported')

# import the blocks
# df_blocks = gpd.read_file('/homedirs/projects/data/usa/tlgdb_2020_a_us_block.gdb',
                        #   driver='FileGDB', layer=0, mask=df_county)

# code.interact(local=locals())


# folder = Path("/homedirs/projects/data/usa/nhgis0086_shape")
# shapefiles = folder.glob("*_block_2020.shp")
# shapefiles = ['/homedirs/projects/data/usa/nhgis0087_shapefile_tl2020_us_tract_2020.zip']
# print([shp for shp in shapefiles])
gdf_tracts = gpd.read_file('/media/CivilSystems/data/usa/2010/nhgis0098_shapefile_tl2010_us_tract_2010.zip', mask=df_county)

# pd.concat([
#                 gpd.read_file(shp, mask=df_county)
#                 for shp in shapefiles
#                 ]).pipe(gpd.GeoDataFrame)

# import code
# code.interact(local=locals())

# import code
# code.interact(local=locals())

gdf_tracts = gdf_tracts.to_crs(crs)
logger.error('Tracts imported')
print('Tracts imported')

gdf_tracts = gdf_tracts.rename(columns={'GEOID10': 'geoid'})

gdf_tracts['state_fips'] = gdf_tracts['STATEFP10']
state_map_abbr = us.states.mapping('fips', 'abbr')
state_map_name = us.states.mapping('fips', 'name')
gdf_tracts['state_code'] = gdf_tracts['STATEFP10']
gdf_tracts['state_name'] = gdf_tracts['STATEFP10']
gdf_tracts.replace({'state_code': state_map_abbr,
                  'state_name': state_map_name}, inplace=True)

gdf_tracts = gdf_tracts[['geoid', 'state_fips', 'state_code', 'state_name', 'GISJOIN', 'geometry']]

###
# Add Demographic Information
###
# At the tract level include:
# - JOIE001:     Median household income in the past 12 months (in 2010 inflation-adjusted dollars)
# - H7Z001:      Total population
# - H7Z003:      Not Hispanic or Latino: White alone
# - IFE001:      Total housing units
# - IFF001:      Total occupied housing units
# - IFF004:      Renter occupied housing units

### 2010
dem_tract1 = pd.read_csv('/media/CivilSystems/data/usa/2010/nhgis0098_ds176_20105_tract.csv', encoding='latin-1') 
dem_tract2 = pd.read_csv('/media/CivilSystems/data/usa/2010/nhgis0098_ds172_2010_tract.csv', encoding='latin-1')

dem_tract1.GISJOIN = dem_tract1.GISJOIN.astype('str')
dem_tract2.GISJOIN = dem_tract2.GISJOIN.astype('str')

dem_tract = dem_tract1.merge(dem_tract2, on='GISJOIN')
dem_tract = dem_tract[['GISJOIN','JOIE001','H7Z001','H7Z003','IFE001','IFF001','IFF004',]]

gdf_tracts = gdf_tracts.merge(dem_tract, on='GISJOIN')

### 2019


# determine the centroid of the blocks
# df_blocks['centroid'] = df_blocks.representative_point()
# df_blocks.set_geometry('centroid', inplace=True)
# logger.error('Centroids found')


# get the blocks within the cities
# df_blocks_select = gpd.sjoin(df_blocks, df_county, how='inner', op='within')
# df_county = None
# df_blocks_select.set_geometry('geometry', inplace=True)
# logger.error('Removed blocks not within counties of interest')

# calculate the area of the blocks
# df_blocks_select = df_blocks_select.to_crs(3395)
# df_blocks_select['area'] = df_blocks_select['geometry'].area / 10**6
# df_blocks_select = df_blocks_select.to_crs(crs)
# logger.error('Calculate block area')


# merge with block demographic data
# total population, white alone, black alone, hispanic or latino, number of housing units
# variables = ['U7B001', 'U7B003', 'U7B004', 'U7C002', 'U7G001']
# df_info = pd.read_csv(
#     '/homedirs/projects/data/usa/nhgis0086_csv/nhgis0086_ds248_2020_block.csv', encoding="ISO-8859-1",
#     usecols=['STATEA', 'COUNTYA', 'TRACTA', 'BLOCKA'] + variables,
#     dtype={x: 'str' for x in ['STATEA', 'COUNTYA', 'TRACTA', 'BLOCKA']})
# # create the geoid for merge
# df_info['geoid'] = df_info['STATEA'] + df_info['COUNTYA'] + df_info['TRACTA'] + df_info['BLOCKA']
# df_blocks_select = df_blocks_select.merge(df_info, on='geoid')
# logger.info('Block variables found')

# # calculate the density
# df_blocks_select['ppl_per_km2'] = df_blocks_select['U7B001']/df_blocks_select['area']
# df_blocks_select['dwellings_per_km2'] = df_blocks_select['U7G001'] / df_blocks_select['area']

# # set index

# df_write = df_blocks_select[['geoid', 'geoid_county', 'geometry',
#                           'area', 'ppl_per_km2', 'dwellings_per_km2'] + variables]
# # df_write.rename({'polygon':'geometry'}, inplace=True)
# df_write.drop_duplicates(inplace=True)



# export to sql
gdf_tracts.to_postgis('tract10', engine, if_exists='replace', dtype={
    'geometry': Geometry('MULTIPOLYGON', srid=crs)}
)
logger.error('Tracts10 written to PSQL')

# df_write = df_blocks_select[['geoid', 'geoid_county', 'centroid', 'U7B001']]
# df_write.rename({'centroid': 'geometry'}, inplace=True)
# df_write = df_write.set_geometry('centroid')
# df_write = df_write.to_crs(crs)
# df_write.to_postgis('origins20', engine, if_exists='replace', dtype={
#     'centroid': Geometry('POINT', srid=crs)}
# )
# logger.error('Blocks centroids (origins) written to PSQL')


db['con'].close()
post_message_to_slack("2010 Tracts written to PSQL")
