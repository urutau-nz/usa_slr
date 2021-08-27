'''
Create a table with the nearest distance, grouped by destination type for each of the blocks
'''
import main
import yaml 
import psycopg2
from sqlalchemy.types import Float, Integer
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
import numpy as np
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

dest_types = pd.read_sql('Select distinct(dest_type) from destinations', db['con'])['dest_type'].values
inundations = ['slr', 'slr_low']
rises = np.arange(0,11)

# get the nearest distance for each block by each destination type
queries_1 = ['DROP TABLE IF EXISTS nearest_block;',
    # """CREATE TABLE nearest_block
    # AS
    # SELECT distances.id_orig AS geoid, destinations.dest_type, MIN(distances.distance) AS distance
    # FROM distances
    # INNER JOIN destinations ON distances.id_dest = destinations.id_dest
    # INNER JOIN blocks ON  distances.id_orig = blocks.geoid
    # GROUP BY distances.id_orig, destinations.dest_type;
    # """,
    'CREATE TABLE IF NOT EXISTS nearest_block(geoid TEXT, dest_type TEXT, distance INT, rise INT, inundation TEXT)'
]
queries_2 = [''' INSERT INTO nearest_block (geoid, dest_type, distance, rise, inundation)
        SELECT distances.id_orig as geoid, destinations.dest_type, MIN(distances.distance) as distance, distances.rise, distances.inundation
        FROM distances
        INNER JOIN destinations ON distances.id_dest = destinations.id_dest
        INNER JOIN blocks20 ON  distances.id_orig = blocks20.geoid
        WHERE distances.dest_type='{}' AND distances.inundation='{}' AND distances.rise={}
        GROUP BY distances.id_orig, destinations.dest_type, distances.rise, distances.inundation;
    '''.format(dest_type, inundation, rise)
    for dest_type in dest_types
    for inundation in inundations
    for rise in rises]
queries_3 = ['CREATE INDEX nearest_geoid ON nearest_block (geoid)']

queries = queries_1 + queries_2 + queries_3

# import code
# code.interact(local=locals())
logger.error('Creating table')
for q in queries:
    cur.execute(q)
conn.commit()
logger.error('Table created')

db['con'].close()
logger.error('Database connection closed')
