'''
Download and unzip the slr data
'''

import json
import requests
import zipfile
import os
# import wget
from tqdm import tqdm
import main
import geopandas as gpd
import pandas as pd
import psycopg2
import yaml
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry import shape, JOIN_STYLE
from geoalchemy2 import Geometry, WKTElement
import fiona
import shapely
import re
import code
import multiprocessing as mp
from joblib import Parallel, delayed

# config
with open('./config/main.yaml') as file:
    config = yaml.load(file)
# functions - logging
import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


crs = config['set_up']['projection']
db = main.init_db(config)
con = db['con']
engine = db['engine']

# f = open("data/slr_source.json") # from https://coast.noaa.gov/slrdata/js/controllers.js
# data = json.load(f)
# data = next(os.walk('./data/raw/slr_extent'))[1]
# data.remove('ALFL_MOB_TLH_slr_final_dist.gdb')
data =['NC_Southern2_slr_final_dist.gdb']
data = ['NY_Suffolk_slr_final_dist.gdb', 'FL_MFL_slr_final_dist.gdb', 'MD_Southeast_slr_final_dist.gdb', 
        'NH_slr_final_dist.gdb', 'NC_Middle1_slr_final_dist.gdb', 'TX_North1_slr_final_dist.gdb', 'ME_East_slr_final_dist.gdb', 'LA_Delta_slr_final_dist_polys.gdb', 
        'MD_East_slr_final_dist.gdb', 'NC_Northern_slr_final_dist.gdb', 'HI_Kauai_slr_final_dist.gdb', 'NC_Southern1_slr_final_dist.gdb', 'CA_LOX_slr_final_dist.gdb', 
        'LA_Central_slr_final_dist_polys.gdb', 'CA_ChannelIslands_slr_final_dist.gdb', 'NY_Hudson_slr_final_dist.gdb', 'FL_JAX_slr_final_dist.gdb', 
        'HI_Maui_slr_final_dist.gdb', 'LA_LakePontchartrain_slr_final_dist_polys.gdb', 'ME_West_slr_final_dist.gdb', 'NJ_Southern_slr_final_dist.gdb', 
        'MD_West_slr_final_dist.gdb', 'CA_MTR_slr_final_dist.gdb', 'TX_South2_slr_final_dist.gdb', 'WA_PugetNW_slr_final_dist.gdb', 'MA_slr_final_dist.gdb', 
        'RI_slr_final_dist.gdb', 'LA_West_slr_final_dist_polys.gdb', 'TX_North2_slr_final_dist.gdb', 'VA_Northern_slr_final_dist.gdb', 'CT_slr_final_dist.gdb', 
        'FL_TBW_slr_final_dist.gdb', 'MD_Southwest_slr_final_dist.gdb', 'MD_North_slr_final_dist.gdb', 'NJ_Northern_slr_final_dist.gdb', 'WA_SEW_slr_final_dist.gdb', 
        'CA_EKA_slr_final_dist.gdb', 'HI_Hawaii_slr_final_dist.gdb', 'HI_Lanai_slr_final_dist.gdb', 'TX_Central_slr_final_dist.gdb', 'WA_PQR_slr_final_dist.gdb', 
        'LA_CentralEast_slr_final_dist_polys.gdb', 'SC_North_slr_final_dist.gdb', 'VA_EasternShore_slr_final_dist.gdb', 'SC_Central_slr_final_dist.gdb', 
        'OR_PQR_slr_final_dist.gdb', 'DC_slr_final_dist.gdb', 'TX_South1_slr_final_dist.gdb', 'GA_slr_final_dist.gdb', 'HI_Molokai_slr_final_dist.gdb', 
        'MS_slr_final_dist.gdb', 'FL_MLB_slr_final_dist.gdb', 'DE_slr_final_dist.gdb', 'OR_MFR_slr_final_dist.gdb', 'VA_Southern_slr_final_dist.gdb', 
        'NC_Middle2_slr_final_dist.gdb', 'VA_Middle_slr_final_dist.gdb', 'CA_SGX_slr_final_dist.gdb', 'HI_Oahu_slr_final_dist.gdb', 'WA_PugetSW_slr_final_dist.gdb', 
        'NJ_Middle_slr_final_dist.gdb']

# import code
# code.interact(local=locals())

def _explode(indf):
    # https://stackoverflow.com/a/64897035/5890574
    count_mp = 0
    outdf = gpd.GeoDataFrame(columns=indf.columns)
    outdf = indf[indf.geometry.type == 'Polygon']
    indf = indf[indf.geometry.type != 'Polygon']
    for idx, row in indf.iterrows():
        if type(row.geometry) == MultiPolygon:
            count_mp = count_mp + 1
            multdf = gpd.GeoDataFrame(columns=indf.columns)
            recs = len(row.geometry)
            multdf = multdf.append([row]*recs,ignore_index=True)
            for geom in range(recs):
                multdf.loc[geom,'geometry'] = row.geometry[geom]
            outdf = outdf.append(multdf,ignore_index=True)
        else:
            print(row)
    logger.info("There were {} Multipolygons found and exploded".format(count_mp))
    return outdf


def slr_import(row, db):
    # create connection
    conn = db['engine'].raw_connection()
    cur = conn.cursor()
    # get information
    name = row.strip('_slr_final_dist.gdb')
    logger.info('Working on {}'.format(name))
    fng = './data/raw/slr_extent/' + row
    layers = fiona.listlayers(fng)
    for layer in tqdm(layers): 
        logger.info('Layer: {}'.format(layer))
        gdf = gpd.read_file(fng, driver='OpenFileGDB', layer=layer)
        layer_crs = gdf.crs
        # remove z coordinate
        if gdf.geometry.has_z.sum()>0:
            for i in range(len(gdf)):
                gdf.geometry[i] = shapely.ops.transform(lambda *args: args[:2], gdf.geometry[i])
        # explode multipolygons
        gdf = _explode(gdf)
        gdf.set_crs(layer_crs, inplace=True)
        gdf.to_crs(crs, inplace=True)
        # prepare to save
        layer_numbers = [s.strip('ft') for s in layer.split('_') if 'ft' in s]
        assert len(layer_numbers)==1, 'too many numbers in layer name'
        rise = int(layer_numbers[0])
        gdf['rise'] = rise
        gdf['region'] = name
        gdf['layer'] = layer
        if 'low' in layer:
            inundation = 'low'
        else:
            inundation = 'slr'
        gdf['inundation'] = inundation
        gdf = gdf[['geometry']]#,'rise','inundation', 'region', 'layer']]
        # save
        gdf.to_postgis('tmptable_{}'.format(name.lower()), engine, if_exists='replace',
            dtype={'geometry': Geometry('POLYGON', srid=crs)})
        con.commit()
        logger.info('Saved to temp')
        # dissolve overlapping
        # code.interact(local=locals())
        queries = ['CREATE TABLE IF NOT EXISTS slr (geometry geometry, rise INT, inundation TEXT, region TEXT, layer TEXT);',
                   "SET work_mem TO '500MB';",
                    '''INSERT INTO slr (geometry, rise, inundation, region, layer)
                    SELECT (ST_Dump(ST_Union(geometry))).geom as geometry, {}, '{}', '{}', '{}'
                    FROM tmptable_{};
                    '''.format(rise, inundation, name, layer, name.lower())
                        ]
        for q in queries:                        
            cur.execute(q)
        conn.commit()
        logger.info('Unioned in PSQL')
    cur.execute('DROP TABLE tmptable_{};'.format(name.lower()))
    conn.commit()


Parallel(n_jobs=10)(delayed(slr_import)(row, db) for row in tqdm(data))


# for row in data:  # reversed(data):
#     # row = 'SC_South_slr_final_dist.gdb'
#     name = row.strip('_slr_final_dist.gdb')
#     logger.info('Working on {}'.format(name))
#     # url = row['slrurl']
#     # print('https:'+url)
#     # fnzip = wget.download('https:'+url, './data/raw/slr_extent/')
#     # with zipfile.ZipFile(fnzip, "r") as zip_ref:
#     # fn = zip_ref.namelist()[0]
#     # zip_ref.extractall("./data/raw/slr_extent/")
#     # os.remove(fnzip)
#     # fng = '../../data/usa/slr/extent/' + row
#     fng = './data/raw/slr_extent/' + row
#     layers = fiona.listlayers(fng)
#     for layer in tqdm(layers):
#         logger.info('Layer: {}'.format(layer))
#         gdf = gpd.read_file(fng, driver='OpenFileGDB', layer=layer)
#         layer_crs = gdf.crs
#         print(layer_crs)
#         # remove z coordinate
#         if gdf.geometry.has_z.sum() > 0:
#             for i in range(len(gdf)):
#                 gdf.geometry[i] = shapely.ops.transform(
#                     lambda *args: args[:2], gdf.geometry[i])
#         # dissolve overlapping
#         # gdf['geometry'] = gdf['geometry'].unary_union
#         # explode multipolygons
#         gdf = _explode(gdf)
#         gdf.set_crs(layer_crs, inplace=True)
#         gdf.to_crs(crs, inplace=True)
#         # load into postgis
#         # prepare to save
#         layer_numbers = [s.strip('ft') for s in layer.split('_') if 'ft' in s]
#         assert len(layer_numbers) == 1, 'too many numbers in layer name'
#         rise = int(layer_numbers[0])
#         gdf['rise'] = rise
#         gdf['region'] = name
#         gdf['layer'] = layer
#         if 'low' in layer:
#             inundation = 'low'
#         else:
#             inundation = 'slr'
#         gdf['inundation'] = inundation
#         gdf = gdf[['geometry']]  # ,'rise','inundation', 'region', 'layer']]
#         # save
#         gdf.to_postgis('tmptable_{}'.format(name.lower()), engine, if_exists='replace',
#                        dtype={'geometry': Geometry('POLYGON', srid=crs)})
#         con.commit()
#         logger.info('Saved to temp')
#         # dissolve overlapping
#         # code.interact(local=locals())
#         queries = ['CREATE TABLE IF NOT EXISTS slr (geometry geometry, rise INT, inundation TEXT, region TEXT, layer TEXT);',
#                    "SET work_mem TO '500MB';",
#                    '''INSERT INTO slr (geometry, rise, inundation, region, layer)
#                     SELECT (ST_Dump(ST_Union(geometry))).geom as geometry, {}, '{}', '{}', '{}'
#                     FROM tmptable_{};
#                     '''.format(rise, inundation, name, layer, name.lower())
#                    ]
#         for q in queries:
#             cur.execute(q)
#         conn.commit()
#         logger.info('Unioned in PSQL')
#         # simplify
#         # gdf.to_crs(3395, inplace=True)
#         # logger.info('Reprojected to meters')
#         # gdf['geometry'] = gdf['geometry'].simplify(50)
#         # gdf['geometry'] = gdf['geometry'].buffer(1, 1, join_style=JOIN_STYLE.mitre).buffer(-1, 1, join_style=JOIN_STYLE.mitre)
#         # gdf['geometry'] = gdf['geometry'].unary_union
#         # logger.info('Simplified')
#         # gdf.to_crs(crs, inplace=True)
#         # logger.info('Reprojected to {}'.format(crs))
#         # # prepare to save
#         # layer_numbers = [s.strip('ft') for s in layer.split('_') if 'ft' in s]
#         # assert len(layer_numbers)==1, 'too many numbers in layer name'
#         # gdf['rise'] = int(layer_numbers[0])
#         # gdf['region'] = name
#         # gdf['layer'] = layer
#         # if 'low' in layer:
#         #     gdf['inundation'] = 'low'
#         # else:
#         #     gdf['inundation'] = 'slr'
#         # gdf = gdf[['geometry','rise','inundation', 'region', 'layer']]
#         # # save
#         # gdf.to_postgis('slr', engine, if_exists='append',
#         #     dtype={'geometry': Geometry('POLYGON', srid=crs)})
#         # code.interact(local=locals())
#         # con.commit()
#     cur.execute('DROP TABLE tmptable_{};'.format(name.lower()))
#     conn.commit()

print('slr written to PSQL')
db['con'].close()


# INSERT INTO slr (geometry, rise, inundation, region, layer) SELECT (ST_Dump(ST_Union(geometry))).geom as geometry, 10, 'slr', 'ALFL_MOB_TLH', 'ALFL_MOB_TLH2_slr_10ft' FROM tmptable;
