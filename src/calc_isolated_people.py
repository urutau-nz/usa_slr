'''
Estimates the number of people who are isolated from various services
as a function of SLR

We're interested in change in isolation. There may be census blocks currently in areas isolated by road, that take (e.g.) boats to get services.
Therefore, we consider the blocks that are not isolated at SLR=0 for each service.

To do this, we determine how many people are in each county and how many people HAVE access to each service


'''
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

dist = pd.read_sql("Select geoid, rise, dest_type, distance from nearest_block where inundation='slr_low'", db['con'])
dist.dropna(inplace=True)
dist.set_index(['geoid', 'rise', 'dest_type'], inplace=True)
indices = set(dist.index.tolist())
logger.info('Imported: distances')
origins = pd.read_sql('SELECT geoid, geoid_county FROM origins WHERE "H7X001">0', db['con'])
logger.info('Imported: origins')
blocks = pd.read_sql('SELECT geoid, geoid_county, "H7X001","H7X002","H7X003","H7Y003","IFE001" FROM blocks WHERE "H7X001">0', db['con'])
blocks.drop_duplicates(inplace=True)
blocks.set_index('geoid',inplace=True)
logger.info('Imported: blocks')

counties = np.unique(origins.geoid_county)
demographics = ['H7X001', 'H7X002','H7X003','H7Y003','IFE001']
dest_types = np.append(dist.index.unique(level='dest_type'),'isolated')
slr = np.arange(0, 11)

# initialise results df
new_indices = itertools.product(counties, slr, dest_types, demographics)
results = pd.DataFrame(new_indices, columns = ['county','slr','dest_type','demographic'])
results['value'] = 0
results.set_index(['county', 'slr', 'dest_type', 'demographic'], inplace=True)


for county in tqdm(counties):
    # get the blocks in the county
    geoids = origins['geoid'].loc[origins['geoid_county']==county].values
    # loop through the rises
    for rise in slr:
        for geoid in geoids:
            iso = True
            for dest_type in dest_types[:-1]:
                # does geoid, slr, dest_type appear in the set of keys.
                access = (geoid, rise, dest_type) in indices
                if access:
                    iso = False  # as it's not isolated from everything
                    # add the demographics
                    for dem in demographics:
                        results.loc[(county, rise, dest_type, dem),
                                    'value'] += blocks.loc[geoid,dem]
            if iso:
                dest_type = 'isolated'
                # if isolated from all services
                for dem in demographics:
                    results.loc[(county, rise, dest_type, dem),
                                'value'] += blocks.loc[geoid, dem]


# now merge this with the total number of each to determine the number of people isolated from
county_totals = blocks.groupby(['geoid_county']).sum()
county_totals = county_totals.stack()
results.reset_index(inplace=True)
results.set_index(['county','demographic'], inplace=True)
results = results.join(county_totals, how='left')

