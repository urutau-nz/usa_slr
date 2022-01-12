import main
import geopandas as gpd
import pandas as pd
import yaml
with open('./config/main.yaml') as file:
    config = yaml.safe_load(file)

db = main.init_db(config)

sql = """ SELECT b.geoid, b."U7B001", b."U7C005", b."U7B004", b."U7C002", b."U7G001", 
                t.geoid as geoid_tract
                FROM blocks20 as b
                LEFT JOIN tract19 as t ON ST_CONTAINS(t.geometry, st_centroid(b.geometry))
                WHERE "U7B001">0;
        """
blocks = pd.read_sql(sql, db['engine'])
blocks.drop_duplicates(inplace=True)
blocks.set_index('geoid', inplace=True)


# building level information
results = pd.read_sql("Select * from exposed_people20", db['con'])
results.drop_duplicates(inplace=True)
results1 = results.groupby(['rise']).sum()
results1['meta'] = 'building'
# results.to_csv('./data/results/exposed_block20.csv')

# block centroid
results = pd.read_sql("Select id_orig as geoid, rise from exposed_origins20", db['con'])
results.drop_duplicates(inplace=True)
results.set_index('geoid', inplace=True)
results = results.join(blocks, how='left')
results = results.groupby(['geoid_tract', 'rise']).sum()
# results = results.add_suffix('_inundated')
results = results.reset_index()
results = results[['rise','U7B001','U7C005','U7B004','U7C002']]
results2 = results.groupby(['rise']).sum()
results2['meta'] = 'centroid'

# block touches slr
results = pd.read_sql("Select id_orig as geoid, rise from exposed_blocks20", db['con'])
results.drop_duplicates(inplace=True)
results.set_index('geoid', inplace=True)
results = results.join(blocks, how='left')
results = results.groupby(['geoid_tract', 'rise']).sum()
# results = results.add_suffix('_inundated')
results = results.reset_index()
results = results[['rise','U7B001','U7C005','U7B004','U7C002']]
results3 = results.groupby(['rise']).sum()
results3['meta'] = 'block'

# import code
# code.interact(local=locals())

df = pd.concat([results1, results2, results3])

df.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/exposure.csv')

