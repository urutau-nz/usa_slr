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

sql = """ SELECT b.geoid, b."U7B001", b."U7C005", b."U7B004", b."U7C002", b."U7G001", t.geoid as geoid_tract
                FROM blocks20 as b
                LEFT JOIN origins20 as o USING (geoid)
                LEFT JOIN tract19 as t ON ST_CONTAINS(t.geometry, o.centroid)
                WHERE o."U7B001">0;
        """
blocks = pd.read_sql(sql, db['engine'])
blocks.drop_duplicates(inplace=True)
blocks.set_index('geoid', inplace=True)



# loop through slr increments
isolation_block = []
isolation_country = []
for slr in range(11):
    dist = pd.read_sql(
        "SELECT geoid, distance FROM nearest_block20 WHERE inundation='slr_low' AND rise={} AND dest_type='supermarket' AND distance IS NOT NULL".format(slr), db['con'])
    with_access = blocks.index.isin(dist.geoid)
    isolated = blocks[~with_access]
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

###
# Calculate the isolation at other spatial resolutions

## Tract
isolation_tract = isolation_block.groupby(['rise','geoid_tract']).sum()
isolation_tract.reset_index(inplace=True)
isolation_tract['state_code'] = isolation_tract['geoid_tract'].str[:2]
isolation_tract['state_name'] = isolation_tract['state_code']
isolation_tract.replace({'state_code': state_map_abbr,
                  'state_name': state_map_name}, inplace=True)
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
isolation_county.to_sql('isolated_county19', db['engine'], if_exists='replace')
isolation_county.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/isolation_county.csv')

# State
isolation_block['state_code'] = isolation_block['geoid_tract'].str[:2]
isolation_state = isolation_block.groupby(['rise','state_code']).sum()
isolation_state.reset_index(inplace=True)
isolation_state['state_name'] = isolation_state['state_code']
isolation_state.replace({'state_code': state_map_abbr,
                  'state_name': state_map_name}, inplace=True)
isolation_state.to_sql('isolated_state19', db['engine'], if_exists='replace')               
isolation_state.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/isolation_state.csv')   

## Country
isolation_country = isolation_block.groupby(['rise']).sum()
isolation_country.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/isolation_country.csv')

# import code
# code.interact(local=locals())