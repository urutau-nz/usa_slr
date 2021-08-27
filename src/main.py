import init_osrm
import query
import query_altered_network
import yaml
import subprocess
# functions - data management
import psycopg2
from sqlalchemy.types import Float, Integer
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import numpy as np
# functions - geospatial
import osgeo.ogr
import geopandas as gpd
import shapely
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
from geoalchemy2 import Geometry, WKTElement
# functions - logging
import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def main(config_filename=None):
    # establish config filename
    if config_filename is None:
        # input('Insert Config Filename (filename.yaml): ')
        config_filename = 'main'
        if ('yaml' in config_filename) == True:
            config_filename = config_filename[:-5]

    # import config file
    with open('./config/{}.yaml'.format(config_filename)) as file:
        config = yaml.load(file)

    # initialize and connect to the server
    db = init_db(config)

    # add origins and destinations
    # destinations: get_services.py
    # origins: get_blocks20.py

    # query
    query_altered_network.main(config)

    # calculate nearest
    # determine_nearest.py


    # shutdown the OSRM server
    shutdown_db(config)


def init_db(config):
    '''create the database and then connect to it'''
    # SQL connection
    db = config['SQL'].copy()
    db['passw'] = open('./config/pass.txt', 'r').read().strip('\n')
    db['engine'] = create_engine('postgresql+psycopg2://postgres:' + db['passw'] +
                                 '@' + db['host'] + '/' + db['database_name'] + '?port=' + db['port'])
    db['address'] = "host=" + db['host'] + " dbname=" + db['database_name'] + \
        " user=postgres password='" + db['passw'] + "' port=" + db['port']

    # Create the database
    exists = database_exists(db['engine'].url)
    if not exists:
        create_database(db['engine'].url)

    # connect to database
    db['con'] = psycopg2.connect(db['address'])

    # enable postgis
    if not exists:
        db['con'].cursor().execute("CREATE EXTENSION postgis;")
        db['con'].commit()

    logger.info('Database connection established')
    return(db)


def shutdown_db(config):
    if config['OSRM']['shutdown']:
        shell_commands = [
            'docker stop osrm-{}'.format(config['location']['state']),
            'docker rm osrm-{}'.format(
                config['location']['state']),
        ]
        for com in shell_commands:
            com = com.split()
            subprocess.run(com)
    logger.info('OSRM server shutdown and removed')


def multi_regions():
    # establish config filenames
    states = ['il', 'md', 'fl', 'co', 'mi', 'la', 'ga', 'or', 'wa', 'tx']
    for state in states:
        config_filename = state
        # run
        main(config_filename)


if __name__ == '__main__':
    main()
