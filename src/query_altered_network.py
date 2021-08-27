'''
Init the database
Query origin-destination pairs using OSRM
'''
############## Imports ##############
# Packages
import os
import sys
from contextlib import contextmanager
import math
from math import radians, cos, sin, asin, sqrt
import os.path
import io
import code
import numpy as np
import pandas as pd
import itertools
from datetime import datetime
import subprocess
import time

# functions - geospatial
import osgeo.ogr
import geopandas as gpd
import shapely
from geoalchemy2 import Geometry, WKTElement
# functions - data management
import psycopg2
from sqlalchemy.types import Float, Integer
from sqlalchemy.engine import create_engine
# functions - parallel
import multiprocessing as mp
from joblib import Parallel, delayed
from tqdm import tqdm
# functions - requests
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
# functions - scripts
import init_osrm
import make_roads_csv
# functions - logging
import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

############## Main ##############


def main(config):
    '''
    gathers context and runs functions based on 'script_mode'
    '''
    # gather data and context
    db = connect_db(config)
    # get the county geoids from origins
    geoid_counties = list(pd.read_sql(
        'SELECT DISTINCT(geoid_county) FROM origins20', db['con'])['geoid_county'].values)
    # loop through the counties
    i = 0
    for rise in tqdm([10,0,6,3,9,8,7,5,4,2,1]):
        logger.error('INITIALISING DOCKER FOR RISE: {}'.format(rise))
        # reset & alter docker
        if rise == 0:
            init_osrm.main(config, logger, False, False)
        else:
            # make csv of closed road ids
            df_osmids = pd.read_sql("SELECT from_osmid, to_osmid FROM exposed_roads WHERE rise={}".format(rise), db['con'])
            make_roads_csv.main(df_osmids, config)
            init_osrm.main(config, logger, True, False)
        for geoid_county in tqdm(geoid_counties):
            if rise == 0:
                closed_ids = []
            else:
                # get list of closed services ids
                closed_ids = pd.read_sql("SELECT id_dest FROM exposed_destinations WHERE geoid = '{}' AND rise={}".format(geoid_county, rise), db['con'])
                closed_ids = list(closed_ids['id_dest'])
            # query the distances
            logger.error('QUERYING POINTS FOR {}, {}'.format(geoid_county, rise))
            origxdest = query_points(db, config, geoid_county, closed_ids)
            # format results
            origxdest['rise'] = rise
            origxdest['inundation'] = 'slr_low'
            # add df to sql
            write_to_postgres(origxdest, db, i)
            logger.error('Saved to SQL: {}, {}'.format(geoid_county, rise))
            i+=1

    # close the connection
    db['con'].close()
    logger.error('Database connection closed')


def connect_db(config):
    '''create the database and then connect to it'''
    # SQL connection
    db = config['SQL'].copy()
    db['passw'] = open('pass.txt', 'r').read().strip('\n')
    # connect to database
    db['engine'] = create_engine('postgresql+psycopg2://postgres:' + db['passw'] +
                                 '@' + db['host'] + '/' + db['database_name'] + '?port=' + db['port'])
    db['address'] = "host=" + db['host'] + " dbname=" + db['database_name'] + \
        " user=postgres password='" + db['passw'] + "' port=" + db['port']
    db['con'] = psycopg2.connect(db['address'])
    if db['replace']:
        input('Instructed to replace the table. Press Enter to continue...')
    else: 
        input('Instructed to append to the table. Press Enter to continue...')
    logger.error('Database connection established')
    return(db)


############## Query Points ##############
def query_points(db, config, geoid_county, closed_ids):
    '''
    query OSRM for distances between origins and destinations
    '''
    logger.error('Querying invoked for {} in county #{}'.format(
        config['transport_mode'], geoid_county))
    # connect to db
    cursor = db['con'].cursor()

    # get list of all origin ids that have a population > 0
    sql = '''SELECT geoid, st_x(centroid) as x, st_y(centroid) as y FROM origins20 WHERE geoid_county='{}' AND "U7B001">0;'''.format(
        geoid_county)
    orig_df = pd.read_sql(sql, db['con'])

    # drop duplicates
    orig_df.drop_duplicates(inplace=True)
    # set index (different format for different census blocks)
    orig_df['geoid'] = orig_df['geoid'].astype('category')
    orig_df = orig_df.set_index('geoid')
    orig_df.sort_index(inplace=True)
    logger.error('Data: origins20 imported')

    # get list of destination ids
    sql = '''SELECT d.id_dest, d.dest_type, st_x(d.geometry) as lon, st_y(d.geometry) as lat
            FROM destinations as d
            JOIN county_buffered as cb
            ON ST_WITHIN(d.geometry, cb.geometry)
            WHERE d.dest_type in ('{}')
            AND cb.geoid='{}';
            '''.format("','".join(config['services']), geoid_county,)
    # sql = "SELECT id_dest, dest_type, st_x(geometry) as lon, st_y(geometry) as lat FROM destinations WHERE geoid_county={} AND dest_type IN ('{}') ;".format(
    #     geoid_county, "','".join(config['services']))
    dest_df = pd.read_sql(sql, db['con'])

    # remove closed ids
    dest_df = dest_df.loc[~dest_df['id_dest'].isin(closed_ids)]

    dest_df['id_dest'] = dest_df['id_dest'].astype('int32')
    dest_df['dest_type'] = dest_df['dest_type'].astype('category')
    dest_df = dest_df.set_index('id_dest')
    logger.error('Data: destinations imported')

    origxdest_size = len(dest_df) * len(orig_df)
    logger.error(
        'Before subset there are {} orig-dest pairs'.format(origxdest_size))

    if config['query_euclidean']:
        logger.error('Subsetting based on Euclidean distance')
        if origxdest_size > 0.5e9:
            logger.error(
                'Large number of pairs, subsetting by iterating through origins')
            # db, config, geoid_county, origxdest_size)
            origxdest = euclidean_subset_large(orig_df, dest_df)
        else:
            origxdest = euclidean_subset(orig_df, dest_df)
        # query
        logger.error(
            'There are {} origin-destination pairs to query'.format(len(origxdest)))
        for metric in config['metric']:
            origxdest['{}'.format(metric)] = None
        origxdest = euclidean_query(
            origxdest, orig_df, dest_df, config)
        # subset
        # code.interact(local=locals())
        origxdest = origxdest.rename_axis('id_orig').reset_index()
        origxdest = origxdest[['id_orig', 'id_dest'] +
                              config['metric']+['dest_type']]
    else:
        # query the durations, distances
        logger.error(
            'There are {} origin-destination pairs to query'.format(len(origxdest)))
        for metric in config['metric']:
            origxdest['{}'.format(metric)] = None
        origxdest = execute_table_query(origxdest, orig_df, dest_df, config)
    origxdest[config['metric']] = origxdest[config['metric']].apply(
        pd.to_numeric, downcast='integer')
    return origxdest


def euclidean_subset(orig_df, dest_df):
    # list of origxdest pairs
    origxdest = pd.DataFrame(list(itertools.product(
        orig_df.index, dest_df.index)), columns=['id_orig', 'id_dest'])
    origxdest['dest_type'] = len(orig_df)*list(dest_df['dest_type'])
    # optimize df memory
    # integers
    ints = origxdest.select_dtypes(include=['int64']).columns.tolist()
    origxdest[ints] = origxdest[ints].apply(pd.to_numeric, downcast='integer')
    # objects
    origxdest['id_orig'] = origxdest['id_orig'].astype('category')
    origxdest['dest_type'] = origxdest['dest_type'].astype('category')
    logger.error('Data: origxdest pairs initialised')
    # calculate the euclidean distance of the pairs
    origxdest = origxdest.merge(dest_df[['lon', 'lat']].add_prefix(
        'dest_'), left_on='id_dest', right_index=True)
    origxdest = origxdest.merge(orig_df[['x', 'y']].add_prefix(
        'orig_'), left_on='id_orig', right_index=True)
    dest_df = None
    orig_df = None
    orig_lon = origxdest['orig_x'].values
    orig_lat = origxdest['orig_y'].values
    dest_lon = origxdest['dest_lon'].values
    dest_lat = origxdest['dest_lat'].values
    origxdest['euclidean'] = haversine(orig_lat, orig_lon, dest_lat, dest_lon)
    logger.error('Euclidean distances calculated')
    # subset the columns
    origxdest.drop(columns=['orig_x', 'orig_y',
                            'dest_lon', 'dest_lat'], inplace=True)
    # set the index
    if len(origxdest.dest_type.unique()) > 1:
        indices = ['id_orig', 'dest_type']
    else:
        indices = 'id_orig'
    origxdest.set_index(indices, inplace=True)

    # for each destination type
    logger.error('Subsetting origin-destinations by Euclidean distance')
    distance_threshold = 3000
    dests_number = 5

    # get all values within the distance_threshold
    pairs = origxdest[origxdest['euclidean'] < distance_threshold]
    logger.error('Subset within distance {}'.format(distance_threshold))

    # get the closest five destinations for remaining origin - dest_Type pairs
    # subset = pairs.groupby(indices)['id_dest'].count()
    # pairs = pairs[pairs.index.isin(
    #     subset[subset > dests_number].index.unique())]
    # drop_idx = pairs.index.unique()
    # origxdest = origxdest[~origxdest.index.isin(drop_idx)]
    pairs_additional = origxdest.sort_values(
        'euclidean', ascending=True).groupby(indices).head(dests_number)
    logger.error(
        'Determined closest {} destinations for the remaining origins'.format(dests_number))

    # merge
    pairs = pairs.append(pairs_additional)

    # drop duplicates
    pairs.reset_index(inplace=True)
    pairs.drop_duplicates(inplace=True, keep='first')

    return(pairs)


def euclidean_subset_large(orig_df, dest_df):
    # list of origxdest pairs
    # code.interact(local=locals())
    id_origs = orig_df.index.unique()
    distance_threshold = 2000
    dests_number = 5
    dfs = []
    for idx in tqdm(id_origs):
        # loop through ids
        origxdest = dest_df.copy()
        origxdest.reset_index(inplace=True)
        origxdest['id_orig'] = idx
        origxdest['id_orig'] = origxdest['id_orig'].astype('category')
        origxdest = origxdest.merge(orig_df[['x', 'y']].add_prefix(
            'orig_'), left_on='id_orig', right_index=True)
        origxdest['euclidean'] = haversine(
            origxdest.orig_y.values, origxdest.orig_x.values, origxdest.lat.values, origxdest.lon.values)
        # logger.error('Data: origxdest pairs initialised')
        # logger.error('Euclidean distances calculated')
        # subset the columns
        origxdest.drop(columns=['orig_x', 'orig_y',
                                'lon', 'lat'], inplace=True)
        # set the index
        if len(origxdest.dest_type.unique()) > 1:
            indices = ['id_orig', 'dest_type']
        else:
            indices = 'id_orig'
        origxdest.set_index(indices, inplace=True)
        # get all values within the distance_threshold
        pairs = origxdest[origxdest['euclidean'] < distance_threshold]
        # logger.error('Subset within distance {}'.format(distance_threshold))
        # get the closest five destinations for remaining origin - dest_Type pairs
        subset = pairs.groupby(indices)['id_dest'].count()
        pairs = pairs[pairs.index.isin(
            subset[subset > dests_number].index.unique())]
        drop_idx = pairs.index.unique()
        origxdest = origxdest[~origxdest.index.isin(drop_idx)]
        pairs_additional = origxdest.sort_values(
            'euclidean', ascending=True).groupby(indices).head(dests_number)
        # logger.error('Determined closest {} destinations for the remaining origins'.format(dests_number))
        # merge
        pairs = pairs.append(pairs_additional)
        pairs.drop_duplicates(inplace=True)
        dfs.append(pairs)
    pairs = pd.concat(dfs)
    # optimize
    pairs.reset_index(inplace=True)
    pairs['id_orig'] = pairs['id_orig'].astype('category')
    pairs['dest_type'] = pairs['dest_type'].astype('category')
    return(pairs)


############## Parallel Table Query ##############


def euclidean_query(origxdest, orig_df, dest_df, config):
    # Use the table service so as to reduce the amount of requests sent
    # https://github.com/Project-OSRM/osrm-backend/blob/master/docs/http.md#table-service

    logger.error('Setting origxdest index')
    origxdest.set_index('id_orig', inplace=True)
    logger.error('Merging dest lat,lon')
    origxdest = origxdest.merge(dest_df[['lon', 'lat']].add_prefix(
        'dest_'), left_on='id_dest', right_index=True)
    logger.error('Merging orig lat,lon')
    origxdest = origxdest.merge(orig_df[['x', 'y']].add_prefix(
        'orig_'), left_index=True, right_index=True)

    #create query string
    osrm_url = config['OSRM']['host'] + ':' + config['OSRM']['port']
    base_string = osrm_url + "/table/v1/{}/".format(config['transport_mode'])

    # options string ('?annotations=duration,distance' will give distance and duration)
    if len(config['metric']) == 2:
        options_string_base = '?annotations=duration,distance'
    else:
        options_string_base = '?annotations={}'.format(
            config['metric'][0])  # '?annotations=duration,distance'

    # create a list of queries
    logger.error('Creating query list')
    query_list = []
    origin_ids = origxdest.index.unique().values
    for id_orig in origin_ids:
        # make a string of the origin coordinates
        orig_string = str(origxdest.loc[[id_orig], ['orig_x']].iloc[0][0]) + "," + str(
            origxdest.loc[[id_orig], ['orig_y']].iloc[0][0]) + ';'
        # make a string of all the destination coordinates
        single_origxdest = origxdest.loc[[id_orig]]
        dest_string = ""
        for j in range(len(single_origxdest)):
            #now add each dest in the string
            dest_string += str(single_origxdest['dest_lon'][j]) + "," + \
                str(single_origxdest['dest_lat'][j]) + ";"
        #remove last semi colon
        dest_string = dest_string[:-1]

        # make a string of the number of the sources
        source_str = '&sources=' + \
            str(list(range(1)))[
                1:-1].replace(' ', '').replace(',', ';')
        # make the string for the destinations
        dest_idx_str = '&destinations=' + \
            str(list(range(1, 1+len(single_origxdest)))
                )[1:-1].replace(' ', '').replace(',', ';')
        # combine and create the query string
        options_string = options_string_base + source_str + dest_idx_str
        query_string = base_string + orig_string + dest_string + options_string
        # append to list of queries
        query_list.append(query_string)
    # # Table Query OSRM in parallel
    #define cpu usage
    num_workers = np.int(mp.cpu_count() * config['par_frac'])
    #gets list of tuples which contain 1list of distances and 1list
    logger.error('Querying the origin-destination pairs:')
    results = Parallel(n_jobs=num_workers)(delayed(req)(
        query_string, config) for query_string in tqdm(query_list))
    # get the results in the right format
    if len(config['metric']) == 2:
        dists = [l for orig in results for l in orig[0]]
        durs = [l for orig in results for l in orig[1]]
        origxdest['distance'] = dists
        origxdest['duration'] = durs
    else:
        formed_results = [result for query in results for result in query]
        origxdest['{}'.format(config['metric'][0])] = formed_results
    # if None in formed_results:
    #     code.interact(local=locals())
    logger.error('Querying complete.')
    return(origxdest)


def execute_table_query(origxdest, orig_df, dest_df, config):
    # Use the table service so as to reduce the amount of requests sent
    # https://github.com/Project-OSRM/osrm-backend/blob/master/docs/http.md#table-service

    batch_limit = 1000  # 10000
    dest_n = len(dest_df)
    orig_n = len(orig_df)
    batch_limit = max([batch_limit, dest_n])
    orig_per_batch = int(batch_limit/dest_n)
    batch_n = math.ceil(orig_n/orig_per_batch)

    #create query string
    osrm_url = config['OSRM']['host'] + ':' + config['OSRM']['port']
    base_string = osrm_url + "/table/v1/{}/".format(config['transport_mode'])

    # make a string of all the destination coordinates
    dest_string = ""
    dest_df.reset_index(inplace=True, drop=True)
    for j in range(dest_n):
        #now add each dest in the string
        dest_string += str(dest_df['lon'][j]) + "," + \
            str(dest_df['lat'][j]) + ";"
    #remove last semi colon
    dest_string = dest_string[:-1]

    # options string ('?annotations=duration,distance' will give distance and duration)
    if len(config['metric']) == 2:
        options_string_base = '?annotations=duration,distance'
    else:
        options_string_base = '?annotations={}'.format(
            config['metric'][0])  # '?annotations=duration,distance'
    # loop through the sets of
    orig_sets = [(i, min(i+orig_per_batch, orig_n))
                 for i in range(0, orig_n, orig_per_batch)]

    # create a list of queries
    query_list = []
    for i in orig_sets:
        # make a string of all the origin coordinates
        orig_string = ""
        orig_ids = range(i[0], i[1])
        for j in orig_ids:
            #now add each dest in the string
            orig_string += str(orig_df.x[j]) + "," + str(orig_df.y[j]) + ";"
        # make a string of the number of the sources
        source_str = '&sources=' + \
            str(list(range(len(orig_ids))))[
                1:-1].replace(' ', '').replace(',', ';')
        # make the string for the destinations
        dest_idx_str = '&destinations=' + \
            str(list(range(len(orig_ids), len(orig_ids)+len(dest_df)))
                )[1:-1].replace(' ', '').replace(',', ';')
        # combine and create the query string
        options_string = options_string_base + source_str + dest_idx_str
        query_string = base_string + orig_string + dest_string + options_string
        # append to list of queries
        query_list.append(query_string)
    # # Table Query OSRM in parallel
    #define cpu usage
    num_workers = np.int(mp.cpu_count() * config['par_frac'])
    #gets list of tuples which contain 1list of distances and 1list
    logger.error('Querying the origin-destination pairs:')
    results = Parallel(n_jobs=num_workers)(delayed(req)(
        query_string, config) for query_string in tqdm(query_list))
    # get the results in the right format
    if len(config['metric']) == 2:
        dists = [l for orig in results for l in orig[0]]
        durs = [l for orig in results for l in orig[1]]
        origxdest['distance'] = dists
        origxdest['duration'] = durs
    else:
        formed_results = [result for query in results for result in query]
        origxdest['{}'.format(config['metric'][0])] = formed_results
    logger.error('Querying complete.')
    return(origxdest)

############## Read JSON ##############


def req(query_string, config):
    time.sleep(0.05)
    response = requests_retry_session(
        retries=10, backoff_factor=0.3).get(query_string).json()
    if len(config['metric']) == 2:
        temp_dist = [item for sublist in response['distances']
                     for item in sublist]
        temp_dur = [item for sublist in response['durations']
                    for item in sublist]
        return temp_dist, temp_dur
    else:
        return [item for sublist in response['{}s'.format(config['metric'][0])] for item in sublist]


def haversine(s_lat, s_lng, e_lat, e_lng):
    # https://stackoverflow.com/a/51722117/5890574
    # approximate radius of earth in km
    R = 6373.0
    s_lat = s_lat*np.pi/180.0
    s_lng = np.deg2rad(s_lng)
    e_lat = np.deg2rad(e_lat)
    e_lng = np.deg2rad(e_lng)
    d = np.sin((e_lat - s_lat)/2)**2 + np.cos(s_lat) * \
        np.cos(e_lat) * np.sin((e_lng - s_lng)/2)**2
    # return result in meters
    return (2 * R * np.arcsin(np.sqrt(d)) * 1000).astype(int)

############## Retry Request on Failure ##############


def requests_retry_session(retries, backoff_factor, status_forcelist=(500, 502, 504), session=None):
    session = session or requests.Session()
    retry = Retry(total=retries, read=retries, connect=retries,
                  backoff_factor=backoff_factor, status_forcelist=status_forcelist)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

############## Save to SQL ##############


def write_to_postgres(df, db, i, indices=True):
    ''' quickly write to a postgres database
        from https://stackoverflow.com/a/47984180/5890574'''
    table_name = db['table_name']
    logger.error('Writing data to SQL')
    if db['replace']:
        if i == 0:
            # truncates the table
            df.head(0).to_sql(
                table_name, db['engine'], if_exists='append', index=False)
    conn = db['engine'].raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, table_name, null="")  # null values become ''
    logger.error(
        'Distances written successfully to SQL as "{}"'.format(table_name))
    # update indices
    if db['replace']:
        if i == 0:
            logger.error('Updating indices on SQL')
            if indices == True:
                if table_name == db['table_name']:
                    queries = [
                        'CREATE INDEX "{0}_idx_dest" ON {0} ("id_dest");'.format(
                            db['table_name']),
                        'CREATE INDEX "{0}_idx_orig" ON {0} ("id_orig");'.format(
                            db['table_name'])
                    ]
                for q in queries:
                    cur.execute(q)
    conn.commit()


if __name__ == '__main__':
    main()
