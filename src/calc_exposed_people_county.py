'''
loop through each state
- import the extent from 10ft slr+low
- import the building footprint file
- get all of the footprints that touch the extent 
- save to an SQL db

'''

from slack import post_message_to_slack
import main
import geopandas as gpd
import pandas as pd
import yaml
import numpy as np
from tqdm import tqdm
from geoalchemy2 import Geometry, WKTElement

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

# open connection to DB
crs = config['set_up']['projection']
db = main.init_db(config)
con = db['con']
# engine = db['engine']
# conn = db['engine'].raw_connection()
# cur = conn.cursor()

# demographic info
demographics = ['U7B001', 'U7C005', 'U7B004', 'U7C002']

# import code
# code.interact(local=locals())
# import code
# code.interact(local=locals())
# 
region_set = [("DC", 'DistrictofColumbia'), ("ALFL_MOB_TLH", 'Alabama'), ("ALFL_MOB_TLH", 'Florida'), ("CA_ChannelI", 'California'), ("CA_EKA", 'California'), ("CA_LOX", 'California'), ("CA_MTR", 'California'), (
               "CA_SGX", 'California'),( "CT", 'Connecticut'),( "DE", 'Delaware'),( "FL_JAX", 'Florida'),( "FL_MFL", 'Florida'),( "FL_MLB", 'Florida'),(
               "FL_TBW", 'Florida'),( "GA", 'Georgia'),( "HI_Haw", 'Hawaii'),( "HI_Kau", 'Hawaii'),( "HI_L", 'Hawaii'),( "HI_Mau", 'Hawaii'),( "HI_Molok", 'Hawaii'),( "HI_Oahu", 'Hawaii'),(
               "LA_CentralEast_slr_final_dist_poly", 'Louisiana'),( "LA_CentralNorth_slr_final_dist_poly", 'Louisiana'),( "LA_Central_slr_final_dist_poly", 'Louisiana'),(
               "LA_Delta_slr_final_dist_poly", 'Louisiana'),( "LA_LakePontchartrain_slr_final_dist_poly", 'Louisiana'),( "LA_West_slr_final_dist_poly", 'Louisiana'),(
               "MA",'Massachusetts'),("MD_E",'Maryland'),("MD_North",'Maryland'),("MD_Southe",'Maryland'),("MD_Southwe",'Maryland'),("MD_We",'Maryland'),("ME_E",'Maine'),("ME_We",'Maine'),(
               "MS", 'Mississippi'),( "NC_Middle1", 'NorthCarolina'),( "NC_Middle2", 'NorthCarolina'),( "NC_Northe", 'NorthCarolina'),( "NC_Southern1", 'NorthCarolina'),(
               "NC_Southern2", 'NorthCarolina'),( "NH", 'NewHampshire'),( "NJ_Middle", 'NewJersey'),( "NJ_Northe", 'NewJersey'),( "NJ_Southe", 'NewJersey'),( "NY_Hudso", 'NewYork'),(
               "NY_Metro", 'NewYork'),( "NY_Suffolk", 'NewYork'),( "OR_MFR", 'Oregon'),( "OR_PQR", 'Oregon'),( "PA", 'Pennsylvania'),( "RI", 'RhodeIsland'),( "SC_Ce", 'SouthCarolina'),(
               "SC_North", 'SouthCarolina'),( "SC_South", 'SouthCarolina'),( "TX_Ce", 'Texas'),( "TX_North1", 'Texas'),( "TX_North2", 'Texas'),( "TX_South1", 'Texas'),( "TX_South2", 'Texas'),(
               "VA_EasternShore", 'Virginia'),( "VA_Middle", 'Virginia'),( "VA_Northe", 'Virginia'),( "VA_Southe", 'Virginia'),( "WA_PQR", 'Washington'),( "WA_PugetNW", 'Washington'),(
               "WA_PugetSW", 'Washington'),( "WA_SEW", 'Washington')]

# region_set = [("MS", 'Mississippi')]

for region, state in tqdm(region_set):

    # import the blocks that intersect with the inundation
    # blocks2 = gpd.read_file('/homedirs/projects/data/usa/tlgdb_2020_a_us_block.gdb',
    #                           driver='FileGDB', layer=0, mask=inundation)
    # blocks = blocks.to_crs(crs)                          
    sql = """WITH slr AS (SELECT geometry as geom FROM slr_raw WHERE region='{region}' AND rise='{rise}')
            SELECT geoid, geoid_county, geometry, "U7B001", "U7B003", "U7B004", "U7C002"
            FROM blocks20, slr
            WHERE ST_Intersects(blocks20.geometry, slr.geom)
            AND blocks20."U7B001">0;
            """.format(rise=10, region=region)
    blocks = gpd.read_postgis(sql, con=db['engine'], geom_col='geometry')
    blocks.drop_duplicates(inplace=True)
    logger.info('Blocks imported')

    # import footprints in these blocks
    footprint_blocks = gpd.read_file('./data/raw/footprint/{}.geojson.zip'.format(state), mask=blocks)
    # sjoin the block geoid to the footprint
    # footprint_blocks1 = gpd.sjoin(footprint_blocks, blocks, how='left')
    footprint_blocks = gpd.sjoin(footprint_blocks, blocks, how='left', op='within')
    footprint_blocks.drop_duplicates(inplace=True)
    # calculate the area of footprint groupby block
    footprint_blocks = footprint_blocks.to_crs(3395)
    footprint_blocks['area'] = footprint_blocks.geometry.area
    block_areas = footprint_blocks[['geoid', 'area']].groupby('geoid').sum()
    logger.info('Footprints imported and area calculated')

    # calculate the population/area for that block
    blocks.set_index('geoid', inplace=True)
    blocks = blocks.join(block_areas, how='left')
    blocks.dropna(inplace=True)
    for i in demographics:
        blocks['{}m2'.format(i)] = blocks[i]/blocks['area']

    # add block information to footprint
    footprint_blocks = footprint_blocks.to_crs(crs)
    footprint_blocks = footprint_blocks[['geometry','geoid','geoid_county']+demographics]
    blocks = blocks[['U7B001m2', 'U7B003m2', 'U7B004m2', 'U7C002m2']].reset_index()
    footprint_blocks = footprint_blocks.merge(blocks, how='left', on='geoid')

    # create a tmp footprint table in SQL
    footprint_blocks.to_postgis('tmp_footprint', db['engine'], if_exists='replace', index=False,
                    dtype={
                        'geometry': Geometry('POLYGON', srid=crs)}
                    )
    # footprint_blocks.to_sql('tmp_footprint', con=db['engine'], if_exists='replace', index=False)


    # loop through the heights
    results = []
    for rise in tqdm(np.arange(0,11)):
        # import blocks intersected with inundation
        sql = """WITH slr AS (SELECT geometry as geom FROM slr_raw WHERE region='{region}' AND rise='{rise}')
            SELECT ftp.*
            FROM tmp_footprint AS ftp, slr
            WHERE ST_Intersects(ftp.geometry, slr.geom);
            """.format(rise=rise, region=region)
        footprint_exposed = gpd.read_postgis(sql, con=db['engine'], geom_col='geometry', crs=crs)
        footprint_exposed.drop_duplicates(inplace=True)
        logger.info('Footprint {}, {}, {}ft imported'.format(region,state,rise))

        # sql = "SELECT geometry FROM slr_raw WHERE region='{region}' AND rise='{rise}'".format(rise=rise, region=region)
        # inundation = gpd.read_postgis(sql, con=con, geom_col='geometry')
        # inundation = inundation.to_crs(crs)

        # # intersect footprint_blocks with inundation
        # footprint_exposed = gpd.read_file('./data/raw/footprint/{}.geojson.zip'.format(state), mask=inundation)
        # footprint_exposed = footprint_exposed.to_crs(crs)
        # join population/area for block onto the 
        # footprint_exposed = gpd.sjoin(footprint_exposed, blocks, how='left', op='within')
        footprint_exposed = footprint_exposed.to_crs(3395)
        footprint_exposed['area'] = footprint_exposed.geometry.area
        # multiply with area to determine population 
        for i in demographics:
            footprint_exposed[i] = footprint_exposed['{}m2'.format(i)]*footprint_exposed['area']
        # sum to get population in this county, region
        population_exposed = footprint_exposed[['geoid_county'] + demographics].groupby('geoid_county').sum()
        population_exposed.reset_index(inplace=True)
        # append to results
        population_exposed = population_exposed[['geoid_county','U7B001','U7B003','U7B004','U7C002']]
        population_exposed['rise'] = rise
        population_exposed['state'] = state
        results.append(population_exposed)
    
    # import code
    # code.interact(local=locals())

    # write to sql
    results = pd.concat(results)
    results.to_sql('exposed_people', con=db['engine'], if_exists='append', index=False)
    post_message_to_slack("Estimate for SLR exposed people in {}, {} is complete".format(region,state))


# results = pd.read_sql("Select * from exposed_people", db['con'])
# results = results.groupby(['rise']).sum()
# results.to_csv('./data/processed/exposed.csv')

# post_message_to_slack("Estimate for SLR exposed people for the entire USA is complete")




