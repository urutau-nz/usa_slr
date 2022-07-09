import main
import geopandas as gpd
import pandas as pd
import yaml
import numpy as np
import us
state_map_abbr = us.states.mapping('fips', 'abbr')
state_map_name = us.states.mapping('fips', 'name')
with open('./config/main.yaml') as file:
    config = yaml.safe_load(file)

db = main.init_db(config)

sql = """ SELECT b.geoid, b."U7B001", b."U7C005", b."U7B004", b."U7C002", t.geoid as geoid_tract
                FROM blocks20 as b
                LEFT JOIN origins20 as o USING (geoid)
                LEFT JOIN tract19 as t ON ST_CONTAINS(t.geometry, o.centroid)
                WHERE o."U7B001">0;
        """
blocks = pd.read_sql(sql, db['engine'])
blocks.drop_duplicates(inplace=True)
blocks.set_index('geoid', inplace=True)

# sql = """ SELECT b.geoid, b."U7B001", b."U7C005", b."U7B004", b."U7C002", t.geoid as geoid_tract
#                 FROM exposed_origins20 as e
#                 LEFT JOIN blocks20 as b ON e.id_orig=b.geoid
#                 LEFT JOIN origins20 as o ON e.id_orig=o.geoid
#                 LEFT JOIN tract19 as t ON ST_CONTAINS(t.geometry, o.centroid)
#                 WHERE o."U7B001">0
#                 AND rise=0;
#         """
# exposure_block = pd.read_sql(sql, db['engine'])
# exposure_block.drop_duplicates(inplace=True)
# exposure_block.set_index('geoid', inplace=True)
# exp_ids = exposure_block.index

# loop through slr increments
isolation_block = []
isolation_country = []
for slr in range(11):
    dist_fire = pd.read_sql(
        "SELECT geoid FROM nearest_block20 WHERE inundation='slr_low' AND rise={} AND dest_type='fire_station' AND distance IS NOT NULL".format(slr), db['con'])
    dist_school = pd.read_sql(
        "SELECT geoid FROM nearest_block20 WHERE inundation='slr_low' AND rise={} AND dest_type='primary_school' AND distance IS NOT NULL".format(slr), db['con'])
    # dist_health = pd.read_sql(
    #     "SELECT geoid FROM nearest_block20 WHERE inundation='slr_low' AND rise={} AND dest_type='emergency_medical_service' AND distance IS NOT NULL".format(slr), db['con'])
    dist_all = set.intersection(*map(set,[dist_fire.geoid.values, dist_school.geoid.values]))#,dist_health.geoid.values
    with_access = blocks.index.isin(dist_all)
    isolated = blocks[~with_access]
    # subtract the zero SLR case from the data (but only if it is also isolated at 1ft)
    # if slr==0:
    #     iso_0 = isolated.copy()
    # if slr==1:
    #     ids_0 = set.intersection(*map(set,[iso_0.index, isolated.index]))
    #     # extra_ids = set(ids_0).difference(set(exp_ids))
    #     # ids_0 = set.union(set(ids_0),set(exp_ids))
    #     # remove_values = pd.concat([exposure_block, iso_0.loc[extra_ids]])
    # if slr>0:
    #     isolated.loc[ids_0,["U7B001", "U7C005", "U7B004", "U7C002"]] =  isolated.loc[ids_0,["U7B001", "U7C005", "U7B004", "U7C002"]] - iso_0.loc[ids_0,["U7B001", "U7C005", "U7B004", "U7C002"]]
    # these lines calculate the country's isolation
    result = pd.DataFrame(isolated.sum()).transpose()
    result['rise'] = slr
    isolation_country.append(result)
    # this saves the isolation status of the block
    isolated.reset_index(inplace=True)
    isolated['rise']=slr
    # isolated = isolated[['geoid','rise']]
    isolation_block.append(isolated)

isolation_block = pd.concat(isolation_block)
isolation_block.to_sql('isolated_block20', db['engine'], if_exists='replace')
isolation_block.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/isolation_block.csv')



###
# Calculate the isolation at other spatial resolutions

## Tract
isolation_tract = isolation_block.groupby(['rise','geoid_tract']).sum()
isolation_tract.reset_index(inplace=True)
isolation_tract['state_code'] = isolation_tract['geoid_tract'].str[:2]
isolation_tract['state_name'] = isolation_tract['state_code']
isolation_tract.replace({'state_code': state_map_abbr,
                  'state_name': state_map_name}, inplace=True)
# add the percentage of people
tract = blocks[['geoid_tract','U7B001']].groupby(['geoid_tract']).sum()
# merge isolation tract with tract to get 
isolation_tract = isolation_tract.merge(tract, left_on='geoid_tract', right_index=True, suffixes=('','_total'))
isolation_tract.drop_duplicates(inplace=True)
# calculate percentage
isolation_tract['U7B001_percentage'] = isolation_tract['U7B001']/isolation_tract['U7B001_total']*100 
isolation_tract['U7B001_percentage'] = isolation_tract['U7B001_percentage'].round(1)
# write to sql and file
isolation_tract.to_sql('isolated_tract19', db['engine'], if_exists='replace')
isolation_tract.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/isolation_tract.csv')

# County
isolation_block['geoid_county'] = isolation_block['geoid_tract'].str[:5]
isolation_county = isolation_block.groupby(['rise','geoid_county']).sum()
isolation_county.reset_index(inplace=True)
isolation_county['state_code'] = isolation_county['geoid_county'].str[:2]
isolation_county['state_name'] = isolation_county['state_code']
isolation_county.replace({'state_code': state_map_abbr,
                  'state_name': state_map_name}, inplace=True)
# add the percentage of people
county = blocks[['geoid_tract','U7B001']]
county['geoid_county'] = county['geoid_tract'].str[:5]
county = county[['geoid_county','U7B001']].groupby(['geoid_county']).sum()
# merge isolation tract with tract to get 
isolation_county = isolation_county.merge(county, left_on='geoid_county', right_index=True, suffixes=('','_total'))
isolation_county.drop_duplicates(inplace=True)
# calculate percentage
isolation_county['U7B001_percentage'] = isolation_county['U7B001']/isolation_county['U7B001_total']*100 
isolation_county['U7B001_percentage'] = isolation_county['U7B001_percentage'].round(1)
# write to sql and file  
isolation_county.to_sql('isolated_county19', db['engine'], if_exists='replace')
isolation_county.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/isolation_county.csv')
isolation_county[isolation_county.rise==6].to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/isolation_county_6.csv')

# State
isolation_block['state_code'] = isolation_block['geoid_tract'].str[:2]
isolation_state = isolation_block.groupby(['rise','state_code']).sum()
isolation_state.reset_index(inplace=True)
isolation_state['state_name'] = isolation_state['state_code']
# add the percentage of people
state_population = pd.read_csv('/media/CivilSystems/data/usa/nhgis0100_ds248_2020_state.csv', usecols=['STATEA','pop_total'])
state_population.rename(columns={'pop_total':'U7B001'}, inplace=True)
isolation_state['state_number'] =isolation_state['state_code'].astype('int')
# merge isolation tract with tract to get 
isolation_state = isolation_state.merge(state_population, left_on='state_number', right_on='STATEA', suffixes=('','_total'))
isolation_state.drop_duplicates(inplace=True)
isolation_state.drop(columns=['state_number','STATEA'], inplace=True)
# calculate percentage
isolation_state['U7B001_percentage'] = isolation_state['U7B001']/isolation_state['U7B001_total']*100 
isolation_state['U7B001_percentage'] = isolation_state['U7B001_percentage'].round(1)
# write to sql and file  
isolation_state.replace({'state_code': state_map_abbr,
                  'state_name': state_map_name}, inplace=True)
isolation_state.to_sql('isolated_state19', db['engine'], if_exists='replace')               
isolation_state.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/isolation_state.csv')   

## Country
isolation_country = isolation_block.groupby(['rise']).sum()
isolation_country.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/isolation_country.csv')

# import code
# code.interact(local=locals())