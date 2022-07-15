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

remove_zero = True

sql = """ SELECT b.geoid, b."U7B001", b."U7C005", b."U7B004", b."U7C002", b."U7G001", t.geoid as geoid_tract
                FROM blocks20 as b
                LEFT JOIN origins20 as o USING (geoid)
                LEFT JOIN tract19 as t ON ST_CONTAINS(t.geometry, o.centroid)
                WHERE o."U7B001">0;
        """
blocks = pd.read_sql(sql, db['engine'])
blocks.drop_duplicates(inplace=True)
blocks.set_index('geoid', inplace=True)

# if using building footprint area ratio
# sql = """ SELECT b.geoid, b."U7B001", b."U7C005", b."U7B004", b."U7C002", b.rise, t.geoid as geoid_tract
#                 FROM exposed_people20 as b
#                 LEFT JOIN origins20 as o USING (geoid)
#                 LEFT JOIN tract19 as t ON ST_CONTAINS(t.geometry, o.centroid)
#                 WHERE o."U7B001">0;
#         """
# if using block centroids:
sql = """ SELECT b.geoid, b."U7B001", b."U7C005", b."U7B004", b."U7C002", e.rise, t.geoid as geoid_tract
                FROM exposed_origins20 as e
                LEFT JOIN blocks20 as b ON e.id_orig=b.geoid
                LEFT JOIN origins20 as o ON e.id_orig=o.geoid
                LEFT JOIN tract19 as t ON ST_CONTAINS(t.geometry, o.centroid)
                WHERE o."U7B001">0;
        """
exposure_block = pd.read_sql(sql, db['engine'])
exposure_block.drop_duplicates(inplace=True)
exposure_block.set_index('geoid', inplace=True)

# block centroid
# exposure_block = pd.read_sql("Select id_orig as geoid, rise from exposed_origins20", db['con'])
# exposure_block.drop_duplicates(inplace=True)
# exposure_block.set_index('geoid', inplace=True)
# exposure_block = exposure_block.join(blocks, how='left')
# # results = results.add_suffix('_inundated')
# exposure_block = exposure_block.reset_index()
# exposure_block.drop_duplicates(inplace=True)

if remove_zero:
    # subtract the zero SLR case from the data (but only if it is also isolated at 1ft)
    exp_0 = exposure_block[exposure_block.rise==0].copy()
    exp_1 = exposure_block[exposure_block.rise==1].copy()

    ids_0 = set.intersection(*map(set,[exp_0.index, exp_1.index]))
    exp_list = []
    for slr in range(0,11):
        exp = exposure_block[exposure_block.rise==slr].copy()
        exp.loc[ids_0,["U7B001", "U7C005", "U7B004", "U7C002"]] = exp.loc[ids_0,["U7B001", "U7C005", "U7B004", "U7C002"]] - exp_0.loc[ids_0,["U7B001", "U7C005", "U7B004", "U7C002"]]
        exp_list.append(exp)

    exposure_block = pd.concat(exp_list)


###
# Calculate the exposure at different spatial scales
exposure_block = exposure_block[exposure_block.U7B001 > 0]
exposure_block.to_sql('exposed_block20', db['engine'], if_exists='replace')
exposure_block.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/inundation_block.csv')


## Tract
exposure_tract = exposure_block.groupby(['rise','geoid_tract']).sum()
exposure_tract.reset_index(inplace=True)
exposure_tract['state_code'] = exposure_tract['geoid_tract'].str[:2]
exposure_tract['state_name'] = exposure_tract['state_code']
exposure_tract.replace({'state_code': state_map_abbr,
                  'state_name': state_map_name}, inplace=True)
# add the percentage of people
tract = blocks[['geoid_tract','U7B001']].groupby(['geoid_tract']).sum()
# merge exposure tract with tract to get 
exposure_tract = exposure_tract.merge(tract, left_on='geoid_tract', right_index=True, suffixes=('','_total'))
exposure_tract.drop_duplicates(inplace=True)
# calculate percentage
exposure_tract['U7B001_percentage'] = exposure_tract['U7B001']/exposure_tract['U7B001_total']*100 
exposure_tract['U7B001_percentage'] = exposure_tract['U7B001_percentage'].round(1)
# write to sql and file                  
exposure_tract.to_sql('exposed_tract19', db['engine'], if_exists='replace')
exposure_tract.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/inundation_tract.csv')

# County
exposure_block['geoid_county'] = exposure_block['geoid_tract'].str[:5]
exposure_county = exposure_block.groupby(['rise','geoid_county']).sum()
exposure_county.reset_index(inplace=True)
exposure_county['state_code'] = exposure_county['geoid_county'].str[:2]
exposure_county['state_name'] = exposure_county['state_code']
exposure_county.replace({'state_code': state_map_abbr,
                  'state_name': state_map_name}, inplace=True)
# add the percentage of people
county = blocks[['geoid_tract','U7B001']]
county['geoid_county'] = county['geoid_tract'].str[:5]
county = county[['geoid_county','U7B001']].groupby(['geoid_county']).sum()
# merge exposure tract with tract to get 
exposure_county = exposure_county.merge(county, left_on='geoid_county', right_index=True, suffixes=('','_total'))
exposure_county.drop_duplicates(inplace=True)
# calculate percentage
exposure_county['U7B001_percentage'] = exposure_county['U7B001']/exposure_county['U7B001_total']*100 
exposure_county['U7B001_percentage'] = exposure_county['U7B001_percentage'].round(1)
# write to sql and file  
exposure_county.to_sql('exposed_county19', db['engine'], if_exists='replace')
exposure_county.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/inundation_county.csv')
exposure_county[exposure_county.rise==6].to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/inundation_county_6.csv')

# State
exposure_block['state_code'] = exposure_block['geoid_tract'].str[:2]
exposure_state = exposure_block.groupby(['rise','state_code']).sum()
exposure_state.reset_index(inplace=True)
exposure_state['state_name'] = exposure_state['state_code']
# add the percentage of people
state_population = pd.read_csv('/media/CivilSystems/data/usa/nhgis0100_ds248_2020_state.csv', usecols=['STATEA','pop_total'])
state_population.rename(columns={'pop_total':'U7B001'}, inplace=True)
exposure_state['state_number'] =exposure_state['state_code'].astype('int')
# merge exposure tract with tract to get 
exposure_state = exposure_state.merge(state_population, left_on='state_number', right_on='STATEA', suffixes=('','_total'))
exposure_state.drop_duplicates(inplace=True)
exposure_state.drop(columns=['state_number','STATEA'], inplace=True)
# calculate percentage
exposure_state['U7B001_percentage'] = exposure_state['U7B001']/exposure_state['U7B001_total']*100 
exposure_state['U7B001_percentage'] = exposure_state['U7B001_percentage'].round(1)
# write to sql and file 
exposure_state.replace({'state_code': state_map_abbr,
                  'state_name': state_map_name}, inplace=True)
exposure_state.to_sql('exposed_state19', db['engine'], if_exists='replace')                 
exposure_state.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/inundation_state.csv')


## Country
exposure_country = exposure_block.groupby(['rise']).sum()
exposure_country.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/inundation_country.csv')

# import code
# code.interact(local=locals())