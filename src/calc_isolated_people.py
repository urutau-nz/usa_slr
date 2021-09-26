'''
Estimates the number of people who are isolated from various services
as a function of SLR

We're interested in change in isolation. There may be census blocks currently in areas isolated by road, that take (e.g.) boats to get services.
Therefore, we consider the blocks that are not isolated at SLR=0 for each service.

To do this, we determine how many people are in each county and how many people HAVE access to each service


'''
import determine_nearest

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

dist = pd.read_sql("Select geoid, rise, dest_type, distance from nearest_block20 where inundation='slr_low'", db['con'])
dist.dropna(inplace=True)
dist.set_index('geoid', inplace=True)
blocks = pd.read_sql('SELECT geoid, geoid_county, "U7B001","U7B003","U7B004","U7C002","U7G001" FROM blocks WHERE "U7B001">0', db['con'])
blocks.drop_duplicates(inplace=True)
blocks.set_index('geoid',inplace=True)
dist = dist.join(blocks,how='left')

import code
code.interact(local=locals())

results = dist.groupby(['geoid_county', 'rise', 'dest_type']).sum()
results.reset_index(inplace=True)
results.set_index('geoid_county', inplace=True)

# then to calculate the isolation
access = dist.copy()
access.reset_index(inplace=True)
access = access[['geoid','geoid_county','rise',"U7B001","U7B003","U7B004","U7C002","U7G001"]]
access.drop_duplicates(inplace=True)
notisolated = access.groupby(['geoid_county', 'rise']).sum()
notisolated.reset_index(inplace=True)
notisolated.set_index('geoid_county', inplace=True)


# now merge this with the total number of each to determine the number of people isolated from
county_totals = blocks.groupby(['geoid_county']).sum()
results = results.join(county_totals, how='left', rsuffix='total')
notisolated = notisolated.join(county_totals, how='left', rsuffix='total')

notisolated['U7B001_isolated'] = notisolated['U7B001total'] - notisolated['U7B001']
notisolated['U7B003_isolated'] = notisolated['U7B003total'] - notisolated['U7B003']
notisolated['U7B004_isolated'] = notisolated['U7B004total'] - notisolated['U7B004']
notisolated['U7C002_isolated'] = notisolated['U7C002total'] - notisolated['U7C002']
notisolated['U7G001_isolated'] = notisolated['U7G001total'] - notisolated['U7G001']
total_isolated = notisolated.groupby('rise').sum()
total_isolated.to_csv('./data/processed/isolation.csv')

results['U7B001_isolated'] = results['U7B001total'] - results['U7B001']
results['U7B003_isolated'] = results['U7B003total'] - results['U7B003']
results['U7B004_isolated'] = results['U7B004total'] - results['U7B004']
results['U7C002_isolated'] = results['U7C002total'] - results['U7C002']
results['U7G001_isolated'] = results['U7G001total'] - results['U7G001']
total_results = results.groupby(['dest_type', 'rise']).sum()
total_results.to_csv('./data/processed/isolation_by_destination.csv')

post_message_to_slack("Isolated people calculated")
