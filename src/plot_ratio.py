""" 
line chart plotting ratio of isolation vs inundation at the state level
"""
import main
import yaml
import geopandas as gpd 
import pandas as pd 
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from pylab import rcParams
rcParams['figure.figsize'] = 7, 5
rcParams['pdf.fonttype'] = 42
import us
state_map_abbr = us.states.mapping('fips', 'abbr')
state_map_name = us.states.mapping('fips', 'name')

# import data
exp_data = pd.read_csv('/media/CivilSystems/projects/access_usa_slr/results/inundation_state.csv')
iso_data = pd.read_csv('/media/CivilSystems/projects/access_usa_slr/results/isolation_state.csv')

##
# Plot the state ratio against SLR
df = pd.merge(iso_data,exp_data, how='left',on=['state_code','rise'], suffixes = ('_iso','_inu'))
df.rename(columns={'U7B001_iso':'isolated','U7B001_inu':'inundated','state_name_iso':'state_name'}, inplace=True)
df['ratio'] = df.isolated/df.inundated
df.rise=df.rise*0.3
df.set_index('rise',inplace=True)
df.groupby('state_code')['ratio'].plot(alpha=0.5,color='#1f386b',ylim=(0,5),xlim=(0.3,3))
plt.axhline(y=1, color='k', linestyle='--', alpha=0.2)
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/ratio_slr.jpg')
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/ratio_slr.pdf')
plt.close()


##
# Plot the State ratio against time
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
scenarios = ['1.0 - MED', '2.0 - MED']
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

result.reset_index(inplace=True)
result['state'] = result['geoid'].str[:2]
# df['state_name'] = df['state_code'].copy()
result.replace({'state': state_map_abbr}, inplace=True)
state = result.groupby(by=['scenario','state','year'])[['isolated','inundated']].sum()
state['ratio'] = state['isolated']/state['inundated']

high = state.loc['2.0 - MED']
high.reset_index(inplace=True)
high.set_index('year',inplace=True)
high.groupby('state')['ratio'].plot(alpha=0.5,color='#1f386b',xlim=(2030,2150),ylim=(0,5))
plt.axhline(y=1, color='k', linestyle='--', alpha=0.2)
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/ratio_time_high.jpg')
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/ratio_time_high.pdf')
plt.close()

inter = state.loc['1.0 - MED']
inter.reset_index(inplace=True)
inter.set_index('year',inplace=True)
inter.groupby('state')['ratio'].plot(alpha=0.5,color='#1f386b',xlim=(2030,2150),ylim=(0,5))
plt.axhline(y=1, color='k', linestyle='--', alpha=0.2)
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/ratio_time_intermediate.jpg')
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/ratio_time_intermediate.pdf')
plt.close()