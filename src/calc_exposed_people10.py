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
from geoalchemy2 import Geometry, WKTElement
# functions - parallel
import multiprocessing as mp
from joblib import Parallel, delayed
from tqdm import tqdm

# config
with open('./config/main.yaml') as file:
    config = yaml.safe_load(file)
# functions - logging
import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# open connection to DB

# con = db['con']
# engine = db['engine']
# conn = db['engine'].raw_connection()
# cur = conn.cursor()

# demographic info
demographics = ['H7X001', 'H7X002', 'H7X003', 'H7Y003'] #['U7B001', 'U7B003', 'U7B004', 'U7C002']

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

db = main.init_db(config)
regions_existing = pd.read_sql('select distinct(region,state) from exposed_people10', con=db['engine'])
db['engine'].dispose
regions_existing = [item for sublist in regions_existing.values for item in sublist]

for i in range(len(regions_existing)):
    regions_existing[i] = regions_existing[i].replace('(', '')
    regions_existing[i] = regions_existing[i].replace(')', '')
    regions_existing[i] = tuple(map(str, regions_existing[i].split(',')))

regions_left = list(set(region_set) - set(regions_existing))

# for region, state in tqdm(region_set):
def calc_exposure(region_pair, config):
    region, state = region_pair
    crs = config['set_up']['projection']
    db = main.init_db(config)
    # import the blocks that intersect with the inundation
    # blocks2 = gpd.read_file('/homedirs/projects/data/usa/tlgdb_2020_a_us_block.gdb',
    #                           driver='FileGDB', layer=0, mask=inundation)
    # blocks = blocks.to_crs(crs)  

    tmp_exists = False #pd.read_sql("select exists(select * from information_schema.tables where table_name='tmp_{}_{}')".format(state.lower(), region.lower()), db['engine']).exists[0]
    if not tmp_exists:                       
        sql = """WITH slr AS (SELECT geometry as geom FROM slr_raw WHERE region='{region}' AND rise='{rise}')
                SELECT b.geoid, b.geoid_county, b.geometry, b."H7X001", b."H7X002", b."H7X003", b."H7Y003",
                t.geoid as geoid_tract
                FROM slr, blocks b
                LEFT JOIN tract10 as t ON ST_CONTAINS(t.geometry, st_centroid(b.geometry))
                WHERE ST_Intersects(b.geometry, slr.geom)
                AND b."H7X001">0
                ;
                """.format(rise=10, region=region)
        blocks = gpd.read_postgis(sql, con=db['engine'], geom_col='geometry')
        blocks.drop_duplicates(inplace=True)
        logger.info('Blocks imported')

        # import footprints in these blocks
        footprint_blocks = gpd.read_file('./data/raw/footprint/{}.geojson.zip'.format(state), mask=blocks)
        # sjoin the block geoid to the footprint
        # footprint_blocks1 = gpd.sjoin(footprint_blocks, blocks, how='left')
        footprint_blocks = gpd.sjoin(footprint_blocks, blocks, how='left', op='within')
        footprint_blocks = footprint_blocks[~footprint_blocks.H7X001.isnull()] # footprints without pop
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
        footprint_blocks = footprint_blocks[['geometry','geoid','geoid_tract','geoid_county']+demographics]
        blocks = blocks[['H7X001m2', 'H7X002m2', 'H7X003m2', 'H7Y003m2']].reset_index()
        footprint_blocks = footprint_blocks.merge(blocks, how='left', on='geoid')
        # import code
        # code.interact(local=locals())
        # create a tmp footprint table in SQL
        footprint_blocks.to_postgis('tmp_{}_{}'.format(state.lower(), region.lower()), db['engine'], if_exists='replace', index=False,
                        dtype={
                            'geometry': Geometry('POLYGON', srid=crs)}
                        )
        # footprint_blocks.to_sql('tmp_footprint', con=db['engine'], if_exists='replace', index=False)


    # loop through the heights
    results = []
    for rise in tqdm(np.arange(0,11)):
        # import blocks intersected with inundation
        sql = """WITH slr AS (SELECT geometry as geom FROM slr_raw WHERE region='{}' AND rise='{}')
            SELECT ftp.*
            FROM tmp_{}_{} AS ftp, slr
            WHERE ST_Intersects(ftp.geometry, slr.geom);
            """.format(region, rise, state.lower(), region.lower())
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
        population_exposed = footprint_exposed[['geoid'] + demographics].groupby('geoid').sum()
        population_exposed.reset_index(inplace=True)
        population_exposed = population_exposed.merge(footprint_exposed[['geoid','geoid_tract','geoid_county',]], how='left',on='geoid')
        population_exposed.drop_duplicates(inplace=True)
        # append to results
        if len(population_exposed) > 0:
            population_exposed = population_exposed[['geoid','geoid_tract','geoid_county','H7X001','H7X002','H7X003','H7Y003']]
            population_exposed['rise'] = rise
            population_exposed['state'] = state
            population_exposed['region'] = region
            results.append(population_exposed)

    # import code
    # code.interact(local=locals())
    
    # write to sql
    results = pd.concat(results)
    results['id'] = state +'_' + results['region'] + results['rise'].astype(str)
    results.set_index('id', inplace=True)
    results.to_sql('exposed_people10', con=db['engine'], if_exists='append')
    # drop tmp table
    db['con'].cursor().execute("DROP TABLE tmp_{}_{}".format(state.lower(), region.lower()))
    db['con'].commit()
    db['con'].close()
    db['engine'].dispose()

    post_message_to_slack("Estimate for SLR 2010 exposed people in {}, {} is complete".format(region,state))


num_workers = np.int(mp.cpu_count() * config['par_frac'])
Parallel(n_jobs=num_workers)(delayed(calc_exposure)(
    region_pair, config) for region_pair in tqdm(regions_left))

# calc_exposure(("DC", 'DistrictofColumbia'),config)

db = main.init_db(config)
results = pd.read_sql("Select * from exposed_people10", db['con'])
results.drop_duplicates(inplace=True)
results = results.groupby(['rise']).sum()
results.to_csv('./data/results/exposed_block10.csv')

post_message_to_slack("Estimate for SLR 2010 exposed people for the entire USA is complete")




