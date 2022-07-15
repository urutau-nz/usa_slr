import topojson as tp
import main
import pandas as pd
import geopandas as gpd
import us
state_map_abbr = us.states.mapping('fips', 'abbr')
state_map_name = us.states.mapping('fips', 'name')
import yaml
with open('./config/main.yaml') as file:
    config = yaml.safe_load(file)
from sqlalchemy import create_engine

passw = open('/media/CivilSystems/admin/pass.txt', 'r').read().strip('\n')
port = '5002'
db_host = 'encivmu-tml62'
db_name = 'usa_slr'
engine = create_engine('postgresql+psycopg2://postgres:' + passw + '@' + db_host + '/' + db_name + '?port=' + port)


iso = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/isolation_county.csv', dtype={'geoid_county':str})
inu = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/inundation_county.csv', dtype={'geoid_county':str})
inu = inu[inu.rise==6]
iso = iso[iso.rise==6]

df = iso.merge(inu, on='geoid_county', how='left', suffixes = ('_iso','_inu'))

# add county population



df['U7B001_inu'][df.U7B001_inu.isnull()]=0
df['difference'] = df['U7B001_iso'] - df['U7B001_inu']
df['dif_percent'] = df['difference'] / df['U7B001_inu']
df['ratio'] = df['U7B001_iso'] / df['U7B001_inu']


df.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/county_difference.csv')

gdf = gpd.read_file('/home/tml/CivilSystems/data/usa/2019/nhgis0099_shapefile_tl2019_us_county_2019.zip')

gdf = gdf.merge(df[['geoid_county','difference','dif_percent','ratio']], left_on='GEOID',right_on='geoid_county',how='right')

gdf.to_file('/home/tml/CivilSystems/projects/access_usa_slr/county_dif.shp')

###
# for the dashboard
iso = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/isolation_county.csv', dtype={'geoid_county':str})
inu = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/inundation_county.csv', dtype={'geoid_county':str})
df = iso.merge(inu, on=['geoid_county','rise'], how='left', suffixes = ('_iso','_inu'))
df = df.fillna(value=0)
df['ratio'] = df['U7B001_iso'] / df['U7B001_inu']
df.rename(columns={'U7B001_iso':'isolated','U7B001_inu':'inundated','U7B001_percentage_iso':'isolated_percentage','U7B001_percentage_inu':'inundated_percentage'},inplace=True)
df.reset_index(inplace=True)
df['state_code'] = df['geoid_county'].str[:2]
df['state_name'] = df['state_code'].copy()
df.replace({'state_code': state_map_abbr,
                  'state_name': state_map_name}, inplace=True)
df = df[['geoid_county','rise','ratio', 'isolated','inundated','isolated_percentage','inundated_percentage','state_code','state_name']]
df.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/dashboard_county.csv')


iso = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/isolation_tract.csv', dtype={'geoid_tract':str})
inu = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/inundation_tract.csv', dtype={'geoid_tract':str})
df = iso.merge(inu, on=['geoid_tract','rise'], how='left', suffixes = ('_iso','_inu'))
df = df.fillna(value=0)
df.rename(columns={'U7B001_iso':'isolated','U7B001_inu':'inundated','U7B001_percentage_iso':'isolated_percentage','U7B001_percentage_inu':'inundated_percentage'},inplace=True)
df.reset_index(inplace=True)
df['state_code'] = df['geoid_tract'].str[:2]
df['state_name'] = df['state_code'].copy()
df.replace({'state_code': state_map_abbr,
                  'state_name': state_map_name}, inplace=True)
df = df[['geoid_tract','rise', 'isolated','inundated','isolated_percentage','inundated_percentage','state_code','state_name']]
df.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/dashboard_tract.csv')

import code
code.interact(local=locals())

# import data
sql = """ SELECT t.geoid as id, t.state_fips, t.state_code, t.state_name, t.geometry
                FROM tract19 as t
                RIGHT JOIN isolated_tract19
                ON t.geoid = isolated_tract19.geoid_tract;
        """
tracts = gpd.read_postgis(sql, con=engine, geom_col='geometry')
# tracts.to_file('/home/tml/CivilSystems/projects/access_usa_slr/results/tract.shp')
# simplify
tp.Topology(tracts, prequantize=False, toposimplify=3, prevent_oversimplify=True).to_json('/home/tml/CivilSystems/projects/access_usa_slr/results/tract.json')
# topo = tp.Topology(tracts).topoquantize(0.001)

# topo.to_json('/home/tml/CivilSystems/projects/access_usa_slr/results/tract.json')

iso = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/isolation_state.csv', dtype={'state_code':str})
inu = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/inundation_state.csv', dtype={'state_code':str})
df = iso.merge(inu, on=['state_code','rise'], how='left', suffixes = ('_iso','_inu'))
df = df.fillna(value=0)
df.rename(columns={'U7B001_iso':'isolated','U7B001_inu':'inundated','U7B001_percentage_iso':'isolated_percentage','U7B001_percentage_inu':'inundated_percentage','state_name_iso':'state_name'},inplace=True)
df.reset_index(inplace=True)
df = df[['rise', 'isolated','inundated','isolated_percentage','inundated_percentage','state_code','state_name']]
df.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/dashboard_state.csv')