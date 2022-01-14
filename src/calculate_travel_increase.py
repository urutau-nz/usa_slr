import main
import geopandas as gpd
import pandas as pd
import yaml
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib
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
block_dist = blocks.copy()
for slr in range(11):
    dist = pd.read_sql(
        "SELECT geoid, distance as dist_{} FROM nearest_block20 WHERE inundation='slr_low' AND rise={} AND dest_type='supermarket' AND distance IS NOT NULL".format(slr,slr), db['con'])
    block_dist = pd.merge(block_dist, dist, how='left', on='geoid')
    # with_access = blocks.index.isin(dist.geoid)
    # isolated = blocks[~with_access]
    # # these lines calculate the country's isolation
    # result = pd.DataFrame(isolated.sum()).transpose()
    # result['rise'] = slr
    # isolation_country.append(result)
    # # this saves the isolation status of the block
    # isolated.reset_index(inplace=True)
    # isolated['rise']=slr
    # # isolated = isolated[['geoid','rise']]
    # isolation_block.append(isolated)

import code
code.interact(local=locals())

# subtract SLR 0 from others
dist_change = block_dist.copy()
for slr in range(1,11):
    dist_change['dist_{}'.format(slr)] = block_dist['dist_{}'.format(slr)] - block_dist['dist_0']

# sum isolated people (across the increments)
isolated = []
for slr in range(1,11):
    isolated.append(dist_change[block_dist['dist_{}'.format(slr)].isnull()]['U7B001'].sum())

# sum people non-zero change distance (across increments)
disrupted = []
for slr in range(1,11):
    disrupted.append(dist_change[dist_change['dist_{}'.format(slr)]>0]['U7B001'].sum())


# nonzero distance (across increments)
box_data = []
for slr in range(1,11):
    change = dist_change[dist_change['dist_{}'.format(slr)]>0]['dist_{}'.format(slr)]
    change = pd.DataFrame(change)
    change = change.rename(columns={'dist_{}'.format(slr):'distance'})
    change['rise'] = slr
    box_data.append(change)

box_data = pd.concat(box_data)
box_data['distance'] = box_data['distance']/1000

# plot
ax = sns.boxplot(x="rise", y="distance", data=box_data, color="white", showfliers = False)
ax = sns.stripplot(x="rise", y="distance", data=box_data, color="0.3", marker='.', alpha=0.01)
# ax.set_yscale("log")
ax2=ax.twinx()
ax2.plot(range(0,10),disrupted,'k--')
ax2.plot(range(0,10),isolated,'k--', alpha=0.1)
ax2.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
labels = [round(int(item.get_text())*0.3048, 1) for item in ax.get_xticklabels()]
ax.set_xticklabels(labels)
plt.tight_layout()
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/increase travel time.jpg')
plt.clf()