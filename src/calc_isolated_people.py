'''
Estimates the number of people who are isolated from various services
as a function of SLR

We're interested in change in isolation. There may be census blocks currently in areas isolated by road, that take (e.g.) boats to get services.
Therefore, we consider the blocks that are not isolated at SLR=0 for each service.

To do this, we determine how many people are in each county and how many people HAVE access to each service


'''
# import determine_nearest
DC_STATEHOOD = 1
import us
from slack import post_message_to_slack
import itertools
import main
import yaml
import psycopg2
from sqlalchemy.types import Float, Integer
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm

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

db = main.init_db(config)

conn = db['engine'].raw_connection()
cur = conn.cursor()

###
# 2020
###

dist = pd.read_sql(
    "Select geoid, rise, dest_type, distance from nearest_block20 where inundation='slr_low'", db['con'])
dist.dropna(inplace=True)
dist.set_index('geoid', inplace=True)
sql = """ SELECT b.geoid, b."U7B001", b."U7C005", b."U7B004", b."U7C002", b."U7G001", 
                t.geoid as geoid_tract
                FROM blocks20 as b
                LEFT JOIN tract19 as t ON ST_CONTAINS(t.geometry, st_centroid(b.geometry))
                WHERE "U7B001">0;
        """
blocks = pd.read_sql(sql, db['engine'])
blocks.drop_duplicates(inplace=True)
blocks.set_index('geoid', inplace=True)
dist = dist.join(blocks, how='left')

results = dist.groupby(['geoid_tract', 'rise', 'dest_type']).sum()
results.reset_index(inplace=True)
results.set_index('geoid_tract', inplace=True)

# then to calculate the isolation
access = dist.copy()
access.reset_index(inplace=True)
access = access[['geoid','geoid_tract','rise',"U7B001","U7C005","U7B004","U7C002","U7G001"]]
access.drop_duplicates(inplace=True)

notisolated = access.groupby(['geoid_tract', 'rise']).sum()
notisolated.reset_index(inplace=True)
notisolated.set_index('geoid_tract', inplace=True)


# now merge this with the total number of each to determine the number of people isolated from
county_totals = blocks.groupby(['geoid_tract']).sum()
results = results.join(county_totals, how='left', rsuffix='total')
notisolated = notisolated.join(county_totals, how='left', rsuffix='total')

notisolated['U7B001_isolated'] = notisolated['U7B001total'] - notisolated['U7B001']
notisolated['U7C005_isolated'] = notisolated['U7C005total'] - notisolated['U7C005']
notisolated['U7B004_isolated'] = notisolated['U7B004total'] - notisolated['U7B004']
notisolated['U7C002_isolated'] = notisolated['U7C002total'] - notisolated['U7C002']
notisolated['U7G001_isolated'] = notisolated['U7G001total'] - notisolated['U7G001']

# import code
# code.interact(local=locals())

total_isolated = notisolated.groupby(['geoid_tract','rise']).sum()
total_isolated = total_isolated.reset_index()
total_isolated = total_isolated[['geoid_tract','rise','U7B001_isolated','U7C005_isolated','U7B004_isolated','U7C002_isolated','U7G001_isolated']]
total_isolated['year'] = 2020
isolated20 = total_isolated.copy()

sql = """ SELECT geoid as geoid_tract, "ALW1E001", "ALZLE001", "ALZLE003", "ALUKE001", "ALUKE003"
                FROM tract19;
        """
tract = pd.read_sql(sql, db['engine'])

sql = """ SELECT id_orig as geoid, rise
                FROM exposed_origins20;
        """
inundated = pd.read_sql(sql, db['engine'])
inundated.set_index('geoid', inplace=True)
inundated = inundated.join(blocks, how='left')
inundated = inundated.groupby(['geoid_tract', 'rise']).sum()
inundated = inundated.add_suffix('_inundated')
inundated = inundated.reset_index()

isolated20 = isolated20.merge(tract, how='left', on='geoid_tract')
isolated20 = isolated20.merge(inundated, how='left', on=['geoid_tract','rise'], suffixes=('', '_inundated'))
isolated20 = isolated20.fillna(0)


import code
code.interact(local=locals())

state_map_abbr = us.states.mapping('fips', 'abbr')
state_map_name = us.states.mapping('fips', 'name')
isolated20['state_code'] = isolated20['geoid_tract'].str[:2]
isolated20['state_name'] = isolated20['state_code']
isolated20.replace({'state_code': state_map_abbr,
                  'state_name': state_map_name}, inplace=True)
isolated20.to_csv('./data/results/isolation20_tract.csv')


###
# US wide
###

total_isolated = isolated20.groupby('rise').sum()
total_isolated = total_isolated[['U7B001_isolated', 'U7C005_isolated', 'U7B004_isolated', 'U7C002_isolated', 'U7G001_isolated',
                                'U7B001_inundated', 'U7C005_inundated', 'U7B004_inundated', 'U7C002_inundated', 'U7G001_inundated']]
total_isolated.to_csv('./data/results/isolation20_usa.csv')






results['U7B001_isolated'] = results['U7B001total'] - results['U7B001']
results['U7C005_isolated'] = results['U7C005total'] - results['U7C005']
results['U7B004_isolated'] = results['U7B004total'] - results['U7B004']
results['U7C002_isolated'] = results['U7C002total'] - results['U7C002']
results['U7G001_isolated'] = results['U7G001total'] - results['U7G001']
total_results = results.groupby(['dest_type', 'rise']).sum()
total_results.to_csv('./data/results/isolation_by_destination20.csv')

# isolation from service
service = 'fire_station'
sdist = dist[dist['dest_type']==service]
sresults = sdist.groupby(['geoid_tract', 'rise', 'dest_type']).sum()
sresults.reset_index(inplace=True)
sresults.set_index('geoid_tract', inplace=True)
saccess = sdist.copy()
saccess.reset_index(inplace=True)
saccess = saccess[['geoid','geoid_tract','rise',"U7B001","U7C005","U7B004","U7C002","U7G001"]]
saccess.drop_duplicates(inplace=True)
snotisolated = saccess.groupby(['geoid_tract', 'rise']).sum()
snotisolated.reset_index(inplace=True)
snotisolated.set_index('geoid_tract', inplace=True)

stotals = blocks.groupby(['geoid_tract']).sum()
sresults = sresults.join(stotals, how='left', rsuffix='total')
snotisolated = snotisolated.join(stotals, how='left', rsuffix='total')

snotisolated['U7B001_isolated'] = snotisolated['U7B001total'] - snotisolated['U7B001']
snotisolated['U7C005_isolated'] = snotisolated['U7C005total'] - snotisolated['U7C005']
snotisolated['U7B004_isolated'] = snotisolated['U7B004total'] - snotisolated['U7B004']
snotisolated['U7C002_isolated'] = snotisolated['U7C002total'] - snotisolated['U7C002']
snotisolated['U7G001_isolated'] = snotisolated['U7G001total'] - snotisolated['U7G001']

stotal_isolated = snotisolated.groupby(['geoid_tract','rise']).sum()
stotal_isolated = stotal_isolated.reset_index()
stotal_isolated = stotal_isolated[['geoid_tract','rise','U7B001_isolated']]
stotal_isolated['year'] = 2020
stotal_isolated.to_csv('./data/results/isolation20_tract_{}.csv'.format(service))

post_message_to_slack("Isolated people 2020 calculated - tract")

###
# 2010
###

# dist = pd.read_sql("Select geoid, rise, dest_type, distance from nearest_block where inundation='slr_low'", db['con'])
# dist.dropna(inplace=True)
# dist.set_index('geoid', inplace=True)
# blocks = pd.read_sql('SELECT geoid, geoid_tract, "H7X001","H7X002","H7X003","H7Y003","IFE001" FROM blocks WHERE "H7X001">0', db['con'])
# blocks.drop_duplicates(inplace=True)
# blocks.set_index('geoid',inplace=True)
# dist = dist.join(blocks,how='left')




# results = dist.groupby(['geoid_tract', 'rise', 'dest_type']).sum()
# results.reset_index(inplace=True)
# results.set_index('geoid_tract', inplace=True)

# # then to calculate the isolation
# access = dist.copy()
# access.reset_index(inplace=True)
# access = access[['geoid','geoid_tract','rise',"H7X001","H7X002","H7X003","H7Y003","IFE001"]]
# access.drop_duplicates(inplace=True)
# notisolated = access.groupby(['geoid_tract', 'rise']).sum()
# notisolated.reset_index(inplace=True)
# notisolated.set_index('geoid_tract', inplace=True)


# # now merge this with the total number of each to determine the number of people isolated from
# county_totals = blocks.groupby(['geoid_tract']).sum()
# results = results.join(county_totals, how='left', rsuffix='total')
# notisolated = notisolated.join(county_totals, how='left', rsuffix='total')

# notisolated['H7X001_isolated'] = notisolated['H7X001total'] - notisolated['H7X001']
# notisolated['H7X002_isolated'] = notisolated['H7X002total'] - notisolated['H7X002']
# notisolated['H7X003_isolated'] = notisolated['H7X003total'] - notisolated['H7X003']
# notisolated['H7Y003_isolated'] = notisolated['H7Y003total'] - notisolated['H7Y003']
# notisolated['IFE001_isolated'] = notisolated['IFE001total'] - notisolated['IFE001']

# # import code
# # code.interact(local=locals())

# total_isolated = notisolated.groupby(['geoid_tract','rise']).sum()
# total_isolated = total_isolated.reset_index()
# total_isolated = total_isolated[['geoid_tract','rise','H7X001_isolated']]
# total_isolated['year'] = 2010
# isolated10 = total_isolated.copy()
# total_isolated.to_csv('./data/results/isolation10_county.csv')
# total_isolated = notisolated.groupby('rise').sum()
# total_isolated.to_csv('./data/results/isolation10_usa.csv')

# results['H7X001_isolated'] = results['H7X001total'] - results['H7X001']
# results['H7X002_isolated'] = results['H7X002total'] - results['H7X002']
# results['H7X003_isolated'] = results['H7X003total'] - results['H7X003']
# results['H7Y003_isolated'] = results['H7Y003total'] - results['H7Y003']
# results['IFE001_isolated'] = results['IFE001total'] - results['IFE001']
# total_results = results.groupby(['dest_type', 'rise']).sum()
# total_results.to_csv('./data/processed/isolation_by_destination10.csv')


# # merge
# isolated20.rename(inplace=True, columns={'U7B001_isolated':'count'})
# isolated10.rename(inplace=True, columns={'H7X001_isolated':'count'})
# isolated = pd.concat([isolated20, isolated10])

# isolated.to_csv('./data/results/isolation_county.csv')

# # import code
# # code.interact(local=locals())

# post_message_to_slack("Isolated people 2010 and 2020 calculated")

db['con'].close()
db['engine'].dispose()
# conn.dispose()