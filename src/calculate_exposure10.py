import main
import geopandas as gpd
import pandas as pd
import yaml
import us
state_map_abbr = us.states.mapping('fips', 'abbr')
state_map_name = us.states.mapping('fips', 'name')
with open('./config/main.yaml') as file:
    config = yaml.safe_load(file)

db = main.init_db(config)

sql = """ SELECT b.geoid, b."H7X001", b."H7X002", b."H7X003", b."H7Y003", b."IFE001", t.geoid as geoid_tract
                FROM blocks as b
                LEFT JOIN origins as o USING (geoid)
                LEFT JOIN tract10 as t ON ST_CONTAINS(t.geometry, o.centroid)
                WHERE o."H7X001">0;
        """
blocks = pd.read_sql(sql, db['engine'])
blocks.drop_duplicates(inplace=True)
blocks.set_index('geoid', inplace=True)

# block centroid
exposure_block = pd.read_sql("Select id_orig as geoid, rise from exposed_origins", db['con'])
exposure_block.drop_duplicates(inplace=True)
exposure_block.set_index('geoid', inplace=True)
exposure_block = exposure_block.join(blocks, how='left')
# results = results.add_suffix('_inundated')
exposure_block = exposure_block.reset_index()
exposure_block.drop_duplicates(inplace=True)



###
# Calculate the exposure at different spatial
exposure_block = exposure_block[exposure_block.H7X001 > 0]
exposure_block.to_sql('exposed_block10', db['engine'], if_exists='replace')



## Tract
exposure_tract = exposure_block.groupby(['rise','geoid_tract']).sum()
exposure_tract.reset_index(inplace=True)
exposure_tract['state_code'] = exposure_tract['geoid_tract'].str[:2]
exposure_tract['state_name'] = exposure_tract['state_code']
exposure_tract.replace({'state_code': state_map_abbr,
                  'state_name': state_map_name}, inplace=True)
exposure_tract.to_sql('exposed_tract10', db['engine'], if_exists='replace')
exposure_tract.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/exposure_tract10.csv')

# County
exposure_block['geoid_county'] = exposure_block['geoid_tract'].str[:5]
exposure_county = exposure_block.groupby(['rise','geoid_county']).sum()
exposure_county.reset_index(inplace=True)
exposure_county['state_code'] = exposure_county['geoid_county'].str[:2]
exposure_county['state_name'] = exposure_county['state_code']
exposure_county.replace({'state_code': state_map_abbr,
                  'state_name': state_map_name}, inplace=True)
exposure_county.to_sql('exposed_county10', db['engine'], if_exists='replace')
exposure_county.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/exposure_county10.csv')

# State
exposure_block['state_code'] = exposure_block['geoid_tract'].str[:2]
exposure_state = exposure_block.groupby(['rise','state_code']).sum()
exposure_state.reset_index(inplace=True)
exposure_state['state_name'] = exposure_state['state_code']
exposure_state.replace({'state_code': state_map_abbr,
                  'state_name': state_map_name}, inplace=True)
exposure_state.to_sql('exposed_state10', db['engine'], if_exists='replace')                 
exposure_state.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/exposure_state10.csv')


## Country
exposure_country = exposure_block.groupby(['rise']).sum()
exposure_country.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/exposure_country10.csv')

# import code
# code.interact(local=locals())