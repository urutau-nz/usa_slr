import main
import yaml 
import psycopg2
from sqlalchemy.types import Float, Integer
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
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

df = pd.read_sql('Select * from nearest_block', db['con'])
blocks = pd.read_sql('SELECT geoid, "H7X001" FROM blocks', db['con'])

for rise in np.arange(0,11):
    df_iso = df[df['rise'] == rise]
    df_iso = df_iso[df_iso['dest_type']=='supermarket']
    df_iso = df_iso[df_iso['inundation']=='slr']
    df_iso = df_iso.set_index('geoid').join(blocks.set_index('geoid'))
    df_iso = df_iso[df_iso['distance'].isna()]
    pop = df_iso.H7X001.sum()
    print("Rise {}: {}".format(rise, pop))

for rise in [0,10]:
    df_test = df[df['rise'] == rise]
    df_test = df_test[df_test['dest_type']=='supermarket']
    df_test = df_test[df_test['inundation']=='slr_low']
    df_test = df_test.set_index('geoid').join(blocks.set_index('geoid'))
    df_test = df_test.dropna()
    # plot cdf
    xs, ys, ys_norm = [], [], []
    bins = 2000
    counts, bin_edges = np.histogram(np.array(df_test['distance']/1000), bins=bins, density = True, weights=df_test['H7X001'])
    dx = bin_edges[1] - bin_edges[0]
    ys = ys + list(np.cumsum(counts)*dx*100)
    xs = xs + list(bin_edges[0:-1])
    plt.plot(xs, ys, label=rise)
    plt.xlim([0,5])
    plt.legend()
plt.savefig('cdf.png')