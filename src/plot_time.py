import main
import geopandas as gpd 
import pandas as pd 
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import matplotlib
import yaml
from pylab import rcParams
rcParams['figure.figsize'] = 7, 5
rcParams['pdf.fonttype'] = 42

with open('./config/main.yaml') as file:
    config = yaml.safe_load(file)

db = main.init_db(config)

# import isolation and inundation data
sql = """ SELECT geoid, rise, "U7B001" FROM isolated_block20 WHERE rise>0;"""
isolated = pd.read_sql(sql, db['engine'])
isolated.drop_duplicates(inplace=True)
sql = """ SELECT geoid, rise, "U7B001" FROM exposed_block20 WHERE rise>0;"""
inundated = pd.read_sql(sql, db['engine'])
inundated.drop_duplicates(inplace=True)

# import tidal gauges
rsl = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/data/psmsl_ft.csv')
rsl.drop(columns='Unnamed: 0', inplace=True)
# import block and gauges
sql = """ SELECT psmsl, geoid FROM block_psmsl;"""
psmsl = pd.read_sql(sql, db['engine'])
psmsl.drop_duplicates(inplace=True)
# psmsl.set_index(inplace=True, keys=['psmsl'])

# scenarios of interest
scenarios = ['1.0 - LOW', '1.0 - MED', '1.0 - HIGH', '2.0 - LOW', '2.0 - MED', '2.0 - HIGH']
years = {'RSL2020':2020, 'RSL2030':2030, 'RSL2040':2040, 'RSL2050':2050, 'RSL2060':2060, 'RSL2070':2070, 'RSL2080':2080, 'RSL2090':2090, 'RSL2100':2100, 'RSL2110':2110, 'RSL2120':2120, 'RSL2130':2130, 'RSL2140':2140, 'RSL2150':2150}
# subset
rsl = rsl[rsl.scenario.isin(scenarios)]
# stack: rsl = psmsl | scenario | year | rise
rsl.set_index(inplace=True, keys=['psmsl','scenario'])
rsl = rsl.stack()
rsl.name = 'rise'
# change rise float to int
rsl = pd.to_numeric(rsl, downcast='signed')
# if rsl>10, set to 10ft
rsl[rsl>10] = 10
# reset index
rsl = rsl.to_frame().reset_index()
# replace year string with int
rsl.rename(columns={'level_2':'year'}, inplace=True)
rsl.replace(years, inplace=True)

# join block with rsl
result = pd.merge(psmsl,rsl, on='psmsl')

# join with isolation results
result = pd.merge(result, isolated[['geoid', 'rise', 'U7B001']], on=['geoid','rise'])
# rename
result.rename(columns={'U7B001':'isolated'}, inplace=True)
# join with inundation results
result = pd.merge(result, inundated[['geoid', 'rise', 'U7B001']], on=['geoid','rise'], how='left')
# rename
result.rename(columns={'U7B001':'inundated'}, inplace=True)

# group into states

# plot for each year and scenario
country = result.groupby(by=['scenario','year'])[['isolated','inundated']].sum()


import code
code.interact(local=locals())

# plot figure
fig, ax = plt.subplots()
right_side = ax.spines["right"]
top_side = ax.spines["top"]
right_side.set_visible(False)
top_side.set_visible(False)
# ax2 = ax.twinx()
# ax.plot(extr_iso['year'], extr_iso['U7B001'], color='#0B2948', label='Isolated: Extreme', linestyle='--')
# ax.plot(extr_exp['year'], extr_exp['U7B001'], color='#0B2948', label='Inundated: Extreme')

# ax.scatter(extr_exp['year'], extr_exp['U7B001'], color='#1f386b')
ax.plot(country.loc['2.0 - MED']['isolated'], color='#217AD4', label='Isolated: High', linestyle='--')
ax.fill_between(country.loc['2.0 - MED'].index,country.loc['2.0 - LOW']['isolated'],country.loc['2.0 - HIGH']['isolated'], color='#217AD4', label='Isolated: High', alpha=0.2)
ax.plot(country.loc['2.0 - MED']['inundated'], color='#217AD4', label='Inundated: High')
ax.fill_between(country.loc['2.0 - MED'].index,country.loc['2.0 - LOW']['inundated'],country.loc['2.0 - HIGH']['inundated'], color='#217AD4', label='Inundated: High', alpha=0.2)
# ax.scatter(high_exp['year'], high_exp['U7B001'], color='#627397')
ax.plot(country.loc['1.0 - MED']['isolated'], color='#95C2EE', label='Isolated: Intermediate', linestyle='--')
ax.fill_between(country.loc['1.0 - MED'].index,country.loc['1.0 - LOW']['isolated'],country.loc['1.0 - HIGH']['isolated'], color='#95C2EE', label='Isolated: Intermediate', alpha=0.2)
ax.plot(country.loc['1.0 - MED']['inundated'], color='#95C2EE', label='Inundated: Intermediate')
ax.fill_between(country.loc['1.0 - MED'].index,country.loc['1.0 - LOW']['inundated'],country.loc['1.0 - HIGH']['inundated'], color='#95C2EE', label='Inundated: Intermediate', alpha=0.2)
# ax.scatter(inter_exp['year'], inter_exp['U7B001'], color='#a5afc3')

# ax.scatter(extr_iso['year'], extr_iso['U7B001'], color='#1f386b')
# ax.scatter(high_iso['year'], high_iso['U7B001'], color='#627397')
# ax.scatter(inter_iso['year'], inter_iso['U7B001'], color='#a5afc3')

ax.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
plt.ylabel('Number of people')
plt.xlabel('Year')
plt.legend()
plt.tight_layout()
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/time_slr.jpg')
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/time_slr.pdf')

