import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon
from shapely.geometry import Point
from geoalchemy2 import Geometry, WKTElement
import matplotlib.pyplot as plt
import shapely.geometry
import numpy as np
import requests
import json
import overpy
import main
import yaml
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from joblib import Parallel, delayed
from retry import retry
from retry.api import retry_call
import multiprocessing as mp
from difflib import SequenceMatcher
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
# init SQL connection
db = main.init_db(config)
engine = db['engine']
# init api
#api = overpy.Overpass()
overpass_url = "http://overpass-api.de/api/interpreter"
# init result lists
dest_ids, geoids, dest_types, city_bounds, lats, lons, names = [], [], [], [], [], [], []
f_geoids, f_dest_types, f_query = [], [], []
dest_id = 0
# supermarket subsets
subset_super = True
if subset_super == True:
    stores = ['WALMART', 'ALDI', 'PUBLIX', 'KROGER', 'TRADER JOE', 'SAFEWAY', 'WHOLE FOODS ', 'ALBERTSONS', 'SPROUTS FARMERS', 'FOOD LION', 'RALPHS', 'SAVE-A-LOT', 'H-E-B', 'HARRIS TEETER', 'STATER BROS.', 'GROCERY OUTLET', 
    'VONS', 'KING SOOPERS', 'STOP & SHOP', 'SMART & FINAL', 'WINN-DIXIE', 'MEIJER', 'WINCO FOODS', 'FOOD 4 LESS', 'JEWEL-OSCO', 'HY-VEE', "FRY'S", 'SHOPRITE', 'PRICE CHOPPER', 'LUCKY SUPERMARKET', 'FRED MEYER', 'FRESH MARKET', 
    '99 RANCH MARKET', 'NATURAL GROCERS', 'H MART', 'GIANT EAGLE', 'TARGET', "SMITH'S", 'ACME', 'FOOD CITY', 'GIANT', 'STAR MARKET', 'KEY FOOD', 'QFC', "RALEY'S", "BASHAS'", 'FRESH THYME', 'GORDON FOOD SERVICE', 'LIDL', 'EL SUPER', 
    'CUB FOODS', 'SCHNUCKS', 'WEGMANS', 'TOM THUMB', 'C-TOWN', 'FOODMAXX', 'MARKET BASKET', "PICK 'N SAVE", 'PATEL BROTHERS', 'LOWES FOODS', 'PIGGLY WIGGLY', "SHAW'S", 'GRISTEDES', 'BI-LO', 'HANNAFORD', 'NORTHGATE MARKET', 
    'CARDENAS', 'WINN DIXIE', "MARC'S", 'SAVE MART', 'PAVILIONS', 'NEW SEASONS MARKET', 'FOOD TOWN', 'SMART & FINAL EXTRA!', 'FAREWAY', 'FAMILY FARE', 'PRICE RITE', 'SUPER STOP & SHOP', 'FINE FARE', 'LA MICHOACANA MEAT MARKET', 
    'FIESTA MART', "BROOKSHIRE'S", 'BRAVO', 'FOODTOWN', 'LUNDS & BYERLYS', 'MARKET STREET', 'SUPERIOR', 'MORTON WILLIAMS', 'VALLARTA','SORIANA', 'EARTH FARE', 'FARM FRESH', 'WALGREENS', "MARIANO'S", 'INGLES', 
    'MITSUWA', 'NIJIYA ', 'NOB HILL', "GELSON'S", 'BRISTOL FARMS', 'DILLONS', 'WEIS', 'COMMISSARY', 'C TOWN', 'CERMAK', 'SUPER 1 FOODS', 'NEW INDIA BAZAAR', 'BIG SAVER FOODS', 'UNITED SUPERMARKETS', 'FESTIVAL FOODS', 
    "PETE'S FRESH MARKET", 'CASH & CARRY', "REASOR'S", 'SEAFOOD CITY', 'KEY FOODS', 'HEN HOUSE MARKET', 'PRICERITE', 'HONG KONGMARKET', 'TOPS', 'CALIMAX', 'COUNTY MARKET', "MACEY'S", "MOTHER'S", 'SAVE A LOT', 'BROOKLYN FARE', 
    'WESTSIDE', 'ASSOCIATED ', 'PRESIDENTE', 'FOOD BAZAAR', 'CHAVEZ', 'S-MART', 'UNION MARKET', 'PCC', 'METRO','COMPARE FOODS', 'AMAZON FRESH', 'THE FOOD EMPORIUM', 'HOMELAND', 'MI TIERRA', 'BIG Y', 'SHOP RITE', 'KINGS',
    'ROUSES', 'FOODARAMA', 'FOODS CO', "TONY'S FINER FOODS", "D'AGOSTINO", 'JEWEL', "HARMON'S", 'FOODLAND', "AJ'S FINE FOODS", "JON'S MARKETPLACE", "LUCKY'S MARKET", "DAVE'S", 'FOOD UNIVERSE MARKETPLACE', 'HARMONS', 
    'HARPS', 'IGA', 'SAVEMART', 'STRACK & VAN TIL', 'SUPER SAVER', 'FINE FAIR', 'LA AZTECA MEAT MARKET', 'NUGGET', '168 MARKET', 'THE FRESH GROCER', 'UWAJIMAYA', 'MET', 'ROSAUERS', 'SUPER MERCADO MI TIERRA', 'WESTERN BEEF', 
    "WOODMAN'S", 'LUCKY', 'SUPER KING', "WOLLASTON'S MARKET", 'RANDALLS', "DILLON'S", 'SUPERMERCADO LA CHIQUITA', "BUSCH'S", 'SUPERFRESH', 'IDEAL FOOD BASKET', 'GOOD FORTUNE SUPERMARKET', "RALPH'S", 'MARUKAI', 'SF SUPERMARKET', 
    'FOODSCO', 'MARINA FOODS', 'THANH LONG MARIA MARKET', 'LUIS', 'SUPER A FOODS', 'SUPREMO', "HEINEN'S", 'BARONS', "SEDANO'S", 'GREENLAND MARKET', "LUNARDI'S", 'RANCH MARKET', 'BUTERA', "REDNER'S", 'DOWN TO EARTH', 
    'ARTEAGAS FOOD CENTER', 'PLUM MARKET', 'MORTON WILLIAMS SUPERMARKET', 'MADRAS GROCERIES', 'JONS', "YOKE'S FRESH MARKET", "DAVE'S MARKETPLACE", 'DIERBERGS', 'CASH SAVER', "MOLLIE STONE'S", 'KING KULLEN', "BAKER'S", 
    'VALLI PRODUCE', 'TRADE FAIR', 'SUPERSAVER']
# set services of interest
services = ['supermarket']#['fast_food']['supermarket', 'doctor', 'clinic']#'hospital', 'primary_school', 'pharmacy', 'doctor', 'clinic', 'fire_station', 'police'] 
service_tags = ['shop=supermarket']#['amenity=fast_food']#['shop=supermarket', 'amenity=doctors', 'amenity=clinic']#'amenity=hospital', 'amenity=school', 'amenity=pharmacy', 'amenity=doctors', 'amenity=clinic', 'amenity=fire_station', 'amenity=police'] # consider using emergency tag for hospital? 
# can supplement with other online data
# clinics are just larger doctors offices

def main():
    '''
    Pulls buffered city bounds from SQL
    Uses OverPy (overpass api) to collect all listed services within a city
    Sorts Ways, Rels, & Nodes to point data in a geodataframe
    Sends services as point data to SQL
    '''
    # import county bounds
    df_counties = import_counties(db, crs)
    df_bounds = df_counties
    # make query strings
    make_query_strings(df_bounds)
    # query OverPy API
    print('BEGINNING OVERPASS QUERIES')
    for i in tqdm(np.arange(0,len(df_query))):
        temp_query = df_query.iloc[i]
        query_api(temp_query)
    # get co-ords from json data
    extract_coords()
    # save failed queries
    save_failed_queries()
    # add data to df
    gdf_dests = make_gdf()
    # write to SQL
    gdf_dests.to_postgis('destinations', engine, if_exists='append', dtype={'geometry': Geometry('POINT', srid=crs)})
    # save to file
    #gdf_dests.to_file(r'/homedirs/projects/500_cities_access/data/processed/destinations/destinations_{}.shp'.format('_'.join(services)))


def import_counties(db, crs):
    '''
    Imports city bounds from SQL
    '''
    print('IMPORTING COUNTIES FROM SQL')
    # pull buffered county bounds from SQL
    sql = 'SELECT * FROM county_buffered'
    df_counties = gpd.read_postgis(sql, con=db['con'], geom_col='geometry')
    # ensure df is correctly projected
    df_counties = df_counties.to_crs(crs)
    return(df_counties)



def make_query_strings(df_bounds):
    '''
    Loops through all cities and creates the query string that requests all stated services from the Overpass Turbo API
    '''
    print('MAKING QUERY STRINGS')
    global df_query
    df_query = pd.DataFrame(columns=['geoid', 'dest_type', 'query_str', 'result'])
    df_query['index'] = list(np.arange(0,len(services)*len(df_bounds)))
    df_query = df_query.set_index('index')
    index = 0
    for i in tqdm(np.arange(0, len(df_bounds))):
        # get city id
        geoid = df_bounds['geoid'].iloc[i]
        # get min/max co-ords for bounding box
        x0, y0, x1, y1 = df_bounds[df_bounds['geoid']==geoid].total_bounds
        bbox = "bbox:{},{},{},{}".format(x0,y0,x1,y1)
        coords = "{},{},{},{}".format(x0,y0,x1,y1)
        # loop through services/amenities
        for service_tag in service_tags:
            # generate query string
            query = """
                        [out:json][timeout:60];
                        node
                            [{}]
                        ({},{},{},{});
                        out center;
                        rel
                            [{}]
                        ({},{},{},{});
                        out center;
                        way
                            [{}]
                        ({},{},{},{});
                        out center;
                        """.format(service_tag,y0,x0,y1,x1,service_tag,y0,x0,y1,x1,service_tag,y0,x0,y1,x1)
            df_query.at[index, "geoid"] = geoid
            df_query.at[index, "dest_type"] = service_tag.split('=')[-1]
            df_query.at[index, "query_str"] = query
            index += 1
    return()

def query_api(temp_query):
    '''
    Takes query string and requests results from OverPy API
    Retries 25 times, saves queries that fail
    '''
    geoid = temp_query[0]
    dest_type = temp_query[1]
    query = temp_query[2]
    # if the scraper fails, try 10 times
    unsuccessful = True
    count = 0
    while unsuccessful and count < 10:
        # try to scrape the web data
        response = requests_retry_session(retries=100, backoff_factor=0.05).get(
                   overpass_url, params={'data': query})
        if response.status_code == 200:
            unsuccessful = False
            data = response.json()
        else:
            count += 1
            data = 'Failed'
    if response.status_code != 200:
        print('Query_Failed...')
    index = df_query[(df_query['geoid']==geoid) & (df_query['dest_type']==dest_type)].index[0]
    df_query.at[index, 'result'] = data
    return()

def requests_retry_session(retries,backoff_factor,status_forcelist=(500, 502, 504),session=None):
    session = session or requests.Session()
    retry = Retry(total=retries,read=retries,connect=retries,backoff_factor=backoff_factor,status_forcelist=status_forcelist)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def extract_coords():
    '''
    Loops through json data and pulls out all co-ords
    '''
    print('EXTRACTING CO_ORDINATES')
    dest_id = 0
    # get the max id_dest
    # sql = "SELECT MAX(id_dest) FROM destinations;"
    # id_dest_max = pd.read_sql(sql, db['con']).values[0][0]
    # dest_id = id_dest_max + 1
    for i in tqdm(np.arange(0,len(df_query))):
        data = df_query['result'].iloc[i]
        if data == 'Failed':
            continue
        dest_type = df_query['dest_type'].iloc[i]
        geoid = df_query['geoid'].iloc[i]
        for element in data['elements']:
            if element['type'] == 'node':
                lons.append(element['lon'])
                lats.append(element['lat'])
            elif 'center' in element:
                lons.append(element['center']['lon'])
                lats.append(element['center']['lat'])
            if subset_super == True:
                if 'name' in element['tags'].keys():
                    name = element['tags']['name'].upper()
                else:
                    name = '_'
                similarities = []
                for store in stores:
                    store = store.upper()
                    similarities.append(similar(name, store))
                max_sim = max(similarities)
                if max_sim > 0.5:
                    dest_types.append('supermarket')
                    geoids.append(geoid)
                    dest_ids.append(dest_id)
                    dest_id += 1
                    names.append(name)
                else:
                    dest_types.append('food_store')
                    geoids.append(geoid)
                    dest_ids.append(dest_id)
                    dest_id += 1
                    names.append(name)
            elif subset_super != True:
                geoids.append(geoid)
                dest_types.append(dest_type)
                dest_ids.append(dest_id)
                dest_id += 1

def save_failed_queries():
    '''
    Save all failed queries to CSV
    '''
    f_queries = df_query[df_query['result']=='Failed']
    print(len(f_queries))
    f_queries.to_csv(r'/homedirs/projects/access_usa_slr/data/processed/destinations/failed_queries.csv')
    print('{}/{} QUERIES FAILED'.format(len(f_queries), len(df_query)))
    return()

def make_gdf():
    '''
    Converts co-ods into a geodataframe
    '''
    # init results database
    df_dest = gpd.GeoDataFrame()
    # get the max id_dest
    sql = "SELECT MAX(id_dest) FROM destinations;"
    id_dest_max = pd.read_sql(sql, db['con']).values[0][0]
    # export
    df_dest['id_dest'] = np.arange(id_dest_max+1, id_dest_max+1+len(geoids))
    df_dest['dest_type'] = dest_types
    df_dest['geoid'] = geoids
    # change clinics to doctors
    df_dest["dest_type"].replace({"clinic": "doctor", "doctors": "doctor"}, inplace=True)
    #turn all point data (lats, lons) into geoms
    geometry = [Point(xy) for xy in zip(lons, lats)]
    gdf_dests = gpd.GeoDataFrame(df_dest, crs=crs, geometry=geometry)
    return(gdf_dests)

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()               

if __name__ == '__main__':
    main()
