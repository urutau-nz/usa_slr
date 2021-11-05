# custom
import main
import init_osrm
# other
import numpy as np
import pandas as pd
import geopandas as gpd
import yaml
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import multiprocessing as mp
from joblib import Parallel, delayed
from tqdm import tqdm
from geoalchemy2 import Geometry
# functions - logging
import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

config_filename = 'main'
if ('yaml' in config_filename) == True:
    config_filename = config_filename[:-5]
# import config file
with open('./config/{}.yaml'.format(config_filename)) as file:
    config = yaml.load(file)

db = main.init_db(config)

# set up the OSRM routing
init_osrm.main(config, logger, False, False)
    
# import origins
sql = '''SELECT geoid as id_orig, geoid_county, st_x(centroid) as x, st_y(centroid) as y FROM origins20 WHERE "U7B001">0;'''  # AND geoid_county='01003'
orig_df = pd.read_sql(sql, db['engine'])
orig_df.set_index('id_orig', inplace=True)

# import code
# code.interact(local=locals())

#create query string
osrm_url = config['OSRM']['host'] + ':' + config['OSRM']['port']
base_string = osrm_url + "/nearest/v1/{}/".format(config['transport_mode'])


def requests_retry_session(retries, backoff_factor, status_forcelist=(500, 502, 504), session=None):
    session = session or requests.Session()
    retry = Retry(total=retries, read=retries, connect=retries,
                  backoff_factor=backoff_factor, status_forcelist=status_forcelist)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


logger.error('Querying')
query_list = []
origin_ids = orig_df.index.unique().values

#define cpu usage
num_workers = np.int(mp.cpu_count() * config['par_frac'])


def req(id_orig, geoid_county, x, y):
    orig_string = str(x) + "," + str(y)
    query_string = base_string + orig_string
    # query for location
    response = requests_retry_session(
        retries=10, backoff_factor=0.3).get(query_string).json()
    lon, lat = response['waypoints'][0]['location']
    result = [id_orig, geoid_county, lon, lat]
    return(result)

results = Parallel(n_jobs=num_workers)(delayed(req)(
    id_orig, orig_df.loc[[id_orig], ['geoid_county']].iloc[0][0], orig_df.loc[[id_orig], ['x']].iloc[0][0], orig_df.loc[[id_orig], ['y']].iloc[0][0]) for id_orig in tqdm(origin_ids))

df = pd.DataFrame(results, columns=['id_orig', 'geoid_county', 'lon','lat'])

gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat))
gdf = gdf.set_crs(4326)

logger.error('Querying complete.')



# save
gdf.to_postgis('origins20_snap', db['engine'], if_exists='replace', dtype={
    'geometry': Geometry('POINT', srid=4326)}
)
