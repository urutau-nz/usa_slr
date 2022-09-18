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


# import population
sql = """ SELECT geoid, "U7B001"
                FROM origins20
        """
pop = pd.read_sql(sql, con=db['engine']).set_index('geoid')

# import isolation and inundation data
sql = """ SELECT geoid, rise, "U7B001" FROM isolated_block20 WHERE rise>0;"""
isolated = pd.read_sql(sql, db['engine'])
isolated.drop_duplicates(inplace=True)
sql = """ SELECT geoid, rise, "U7B001" FROM exposed_block20 WHERE rise>0;"""
inundated = pd.read_sql(sql, db['engine'])
inundated.drop_duplicates(inplace=True)

# how many are not isolated at higher values
problem_blocks = []
for i in range(10):
    problem_blocks.append(set(isolated[isolated.rise==i].geoid)-set(isolated[isolated.rise==10].geoid))

problem_blocks = set().union(*problem_blocks)
# remove these from isolated
isolated = isolated[~isolated.geoid.isin(problem_blocks)]


# import block and gauges
sql = """ SELECT psmsl, geoid FROM block_psmsl;"""
psmsl = pd.read_sql(sql, db['engine'])
psmsl.drop_duplicates(inplace=True)
# psmsl.set_index(inplace=True, keys=['psmsl'])

# scenarios of interest
scenarios = ['0.5 - LOW', '0.5 - MED', '0.5 - HIGH', '1.0 - LOW', '1.0 - MED', '1.0 - HIGH', '2.0 - LOW', '2.0 - MED', '2.0 - HIGH']
years = {'RSL2020':2020, 'RSL2030':2030, 'RSL2040':2040, 'RSL2050':2050, 'RSL2060':2060, 'RSL2070':2070, 'RSL2080':2080, 'RSL2090':2090, 'RSL2100':2100, 'RSL2110':2110, 'RSL2120':2120, 'RSL2130':2130, 'RSL2140':2140, 'RSL2150':2150}

# import tidal gauges
rsl = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/data/psmsl_ft.csv')
rsl.drop(columns='Unnamed: 0', inplace=True)
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

# determine the cumulative increase in isolation and inundation
dfs = []
for scen in scenarios:
    result_scen = result[result.scenario==scen]
    first_isolated = result_scen.groupby('geoid').min()[['year']]
    first_inundated = result_scen.dropna(axis=0,subset=['inundated']).groupby('geoid').min()[['year']]
    first_year = first_isolated.join(first_inundated,how='left',lsuffix='_isolated',rsuffix='_inundated')
    first_year = first_year.join(pop, how='left')
    first_year['difference'] = first_year['year_inundated'] - first_year['year_isolated']
    # create df
    risk_over_time = pd.DataFrame({
        'inundated':first_year.groupby('year_inundated')['U7B001'].sum().cumsum(),
        'isolated':first_year.groupby('year_isolated')['U7B001'].sum().cumsum()
    })
    risk_over_time['scenario'] = scen
    risk_over_time.index.name='year'
    risk_over_time.reset_index(inplace=True)   
    if len(risk_over_time)<len(years):
        for yr in set(years.values())-set(risk_over_time.year):
            new_row = risk_over_time.iloc[[-1]].copy()
            new_row['year'] = yr
            risk_over_time = pd.concat([risk_over_time, new_row])
    dfs.append(risk_over_time)

country = pd.concat(dfs)
country.set_index(['scenario','year'],inplace=True)

# country = result.groupby(by=['scenario','year'])[['isolated','inundated']].sum()


projection_dict = {'Intermediate-Low':'0.5', 'Intermediate':'1.0', 'High':'2.0'}

# import code
# code.interact(local=locals())

for proj in projection_dict:
    rise = projection_dict[proj]
    # plot figure
    fig, ax = plt.subplots()
    right_side = ax.spines["right"]
    top_side = ax.spines["top"]
    right_side.set_visible(False)
    top_side.set_visible(False)
    ax.plot(country.loc[f'{rise} - MED']['isolated'], color='#0B2948', label=f'Isolated: {proj}', linestyle='-')
    ax.fill_between(country.loc[f'{rise} - MED'].index,country.loc[f'{rise} - LOW']['isolated'],country.loc[f'{rise} - HIGH']['isolated'], color='#0B2948', label=f'Isolated: {proj}', alpha=0.2)
    ax.plot(country.loc[f'{rise} - MED']['inundated'], color='#8D162A', label=f'Inundated: {proj}')
    ax.fill_between(country.loc[f'{rise} - MED'].index,country.loc[f'{rise} - LOW']['inundated'],country.loc[f'{rise} - HIGH']['inundated'], color='#8D162A', label=f'Inundated: {proj}', alpha=0.2)
    # ax.plot(country.loc['1.0 - MED']['isolated'], color='#0B2948', label='Isolated: Intermediate', linestyle='--')
    # ax.fill_between(country.loc['1.0 - MED'].index,country.loc['1.0 - LOW']['isolated'],country.loc['1.0 - HIGH']['isolated'], color='#0B2948', label='Isolated: Intermediate', alpha=0.2)
    # ax.plot(country.loc['1.0 - MED']['inundated'], color='#8D162A', label='Inundated: Intermediate')
    # ax.fill_between(country.loc['1.0 - MED'].index,country.loc['1.0 - LOW']['inundated'],country.loc['1.0 - HIGH']['inundated'], color='#8D162A', label='Inundated: Intermediate', alpha=0.2)
    ax.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
    plt.ylabel('Number of people')
    plt.xlabel('Year')
    plt.xlim(2040,2150)
    plt.ylim(0,16e6)
    plt.legend()
    plt.tight_layout()
    plt.savefig(f'/home/tml/CivilSystems/projects/access_usa_slr/fig/time_slr_{rise}.jpg')
    plt.savefig(f'/home/tml/CivilSystems/projects/access_usa_slr/fig/time_slr_{rise}.pdf')
    plt.close()


