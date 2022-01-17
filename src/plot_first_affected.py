'''
Can we determine what the difference is between being isolated and being inundated?

Create a table: isolated_block20
geoid | rise
This tells us if a block is isolated at that rise

Create a table: exposed_block20
geoid | rise
This tells us if a block is inundated at that rise

Therefore, we could look at each inundated block and determine whether it was isolated and when it was first isolated.
'''

import main
import geopandas as gpd
import numpy as np
import pandas as pd
import yaml
import seaborn as sns
import matplotlib.pyplot as plt
with open('./config/main.yaml') as file:
    config = yaml.safe_load(file)

import geopandas as gpd
import pandas as pd
import psycopg2
from scipy import stats
from shapely import wkt
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from joblib import Parallel, delayed
import shapely as shp
from tqdm import tqdm
import seaborn as sns

passw = open('/media/CivilSystems/admin/pass.txt', 'r').read().strip('\n')
port = '5002'
db_host = 'encivmu-tml62'
db_name = 'usa_slr'
engine = create_engine('postgresql+psycopg2://postgres:' + passw + '@' + db_host + '/' + db_name + '?port=' + port)

# import data
sql = """ SELECT geoid, min(rise) as first_isolated
                FROM isolated_block20
                GROUP BY geoid;
        """
isolate = pd.read_sql(sql, con=engine)

sql = """ SELECT geoid, min(rise) as first_exposed
                FROM exposed_block20
                GROUP BY geoid;
        """
expose = pd.read_sql(sql, con=engine)

time_effected = pd.merge(expose, isolate, on = 'geoid')

# rise data in m with matching dates due to three rise scenarios (extreme, high, intermediate)
exp_data = pd.read_csv('/media/CivilSystems/projects/access_usa_slr/results/exposure_state.csv')
iso_data = pd.read_csv('/media/CivilSystems/projects/access_usa_slr/results/isolation_state.csv')

year = list(range(2010,2110,10))
year.extend([2120, 2150, 2200])
extr_slr = (np.array([0.04, 0.11, 0.24, 0.41, 0.63, 0.9, 1.2, 1.6, 2, 2.5, 3.6, 5.5, 9.7])*3.281).tolist()
high_slr = (np.array([0.05, 0.11, 0.21, 0.36, 0.54, 0.77, 1, 1.3, 1.7, 2, 2.8, 4.3, 7.5])*3.281).tolist()
int_slr = (np.array([0.04, 0.1, 0.16, 0.25, 0.34, 0.45, 0.57, 0.71, 0.85, 1, 1.3, 1.8, 2.8])*3.281).tolist()

total_exp = exp_data.groupby('rise').sum()
total_exp['rise'] = total_exp.index
# total_exp['rise'] = total_exp['rise']/3.281
list_exp = total_exp['U7B001'].tolist()
total_iso = iso_data.groupby('rise').sum()
total_iso['rise'] = total_iso.index
# total_iso['rise'] = total_iso['rise']/3.281

def interp_years(scenario_slr, year_list):
    df = pd.DataFrame(np.nan, index=list(range(len(scenario_slr))), columns=['Unamed: 0', 'U7B001', 'U7C005', 'U7B004', 'U7C002', 'U7G001'])
    df['rise'] = scenario_slr
    df['year'] = year_list
    # exposure dfs
    exposed = total_exp.append(df)
    exposed = exposed.drop(columns='Unamed: 0')
    exposed = exposed.sort_values(by='rise')
    exposed = exposed.set_index('rise', drop=False)
    exposed_full = exposed.interpolate(method='index')
    exposed_full = exposed_full.drop(exposed_full[exposed_full.year%1 < 0.0001].index)
    # exposed_full = exposed_full.drop(exposed_full[exposed_full.rise > 3.5].index)
    exposed_full['type'] = ['exposed']*len(list(exposed_full['rise']))
    # isolation dfs
    isolated = total_iso.append(df)
    isolated = isolated.drop(columns='Unamed: 0')
    isolated = isolated.sort_values(by='rise')
    isolated = isolated.set_index('rise', drop=False)
    isolated_full = isolated.interpolate(method='index')
    isolated_full = isolated_full.drop(isolated_full[isolated_full.year%1 < 0.0001].index)
    # isolated_full = isolated_full.drop(isolated_full[isolated_full.year > 3.5].index)
    isolated_full['type'] = ['isolated']*len(list(isolated_full['rise']))
    # append together
    df = exposed_full.append(isolated_full)
    return df

extreme_df = interp_years(extr_slr, year)
high_df = interp_years(high_slr, year)
inter_df = interp_years(int_slr, year)

# seperate for plotting 
extr_exp = extreme_df[extreme_df['type']=='exposed']
high_exp = high_df[high_df['type']=='exposed']
inter_exp = inter_df[inter_df['type']=='exposed']
extr_iso = extreme_df[extreme_df['type']=='isolated']
high_iso = high_df[high_df['type']=='isolated']
inter_iso = inter_df[inter_df['type']=='isolated']

extr_exp = extr_exp[['rise', 'year']].fillna(2020).round({'year':0})
high_exp = high_exp[['rise', 'year']].fillna(2020).round({'year':0})
inter_exp = inter_exp[['rise', 'year']].fillna(2020).round({'year':0})
extr_iso = extr_iso[['rise', 'year']].fillna(2020).round({'year':0})
high_iso = high_iso[['rise', 'year']].fillna(2020).round({'year':0})
inter_iso = inter_iso[['rise', 'year']].fillna(2020).round({'year':0})


# distribution plots
sns.displot(data=time_effected, x="first_exposed", y="first_isolated", kind="kde")
# plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/time lag before exposure.jpg')
plt.savefig('src/figs/test.jpg')
plt.cla()

# >>>>>>>>>>> can you add into time_effected a column for year. so intermediate_exposed | intermediate_isolated | high_exposed | high_isolated
# >>> Then create the below figure so that it is showing by time instead of slr

extr_year_x = []
high_year_x = []
inter_year_x = []
extr_year_y = []
high_year_y = []
inter_year_y = []
extr_df = pd.DataFrame()
high_df = pd.DataFrame()
inter_df = pd.DataFrame()

for i in time_effected.index:
    rise_exp = time_effected['first_exposed'].iloc[i]
    extr_year_x.append(extr_exp['year'].iloc[rise_exp])
    high_year_x.append(high_exp['year'].iloc[rise_exp])
    if rise_exp == 10:
        inter_year_x.append(np.nan)
    else:
        inter_year_x.append(inter_exp['year'].iloc[rise_exp])
    rise_iso = time_effected['first_isolated'].iloc[i]
    extr_year_y.append(extr_iso['year'].iloc[rise_iso])
    high_year_y.append(high_iso['year'].iloc[rise_iso])
    if rise_exp == 10:
        inter_year_y.append(np.nan)
    else:
        inter_year_y.append(inter_iso['year'].iloc[rise_iso])

extr_df['year_isolated'] = extr_year_y
extr_df['year_exposed'] = extr_year_x
extr_df['scenario'] = ['extreme']*len(extr_year_y)
high_df['year_isolated'] = high_year_y
high_df['year_exposed'] = high_year_x
high_df['scenario'] = ['high']*len(high_year_x)
inter_df['year_isolated'] = inter_year_y
inter_df['year_exposed'] = inter_year_x
inter_df['scenario'] = ['intermediate']*len(inter_year_y)

new_df = (extr_df.append(high_df)).append(inter_df)
built_old_df = (time_effected.append(time_effected)).append(time_effected)
df = pd.concat([built_old_df, new_df], axis=1)
df = df.drop(df[df.year_isolated < 2021].index)
df = df.drop(df[df.year_exposed < 2021].index)

## boxplots
def plot_box(x_column, y_column, scenario_label):
    fig, ax = plt.subplots()
    ax = sns.boxplot(x="first_exposed", y="first_isolated", data=time_effected, color="white", notch=True)
    x = np.linspace(1,11,11) 
    plt.plot(x,x,'k--') # identity line
    plt.xlim(1,11)
    plt.ylim(1,10)
    plt.xticks(ticks=range(0,11), labels=exposure_scenario['year'], rotation=45)
    plt.yticks(ticks=range(0,11), labels=isolation_scenario['year'])
    plt.savefig('./src/figs/{}_box_plot.jpg'.format(scenario_label))
    plt.clf()

plot_box(extr_exp, extr_iso, 'extreme')

fig, ax = plt.subplots()
flierprops = dict(marker='.', markerfacecolor='grey', markersize=1)
ax = sns.boxplot(x="year_exposed", y="year_isolated", data=df, hue="scenario", notch=True, width=1, flierprops=flierprops)
# ax = sns.boxplot(x="inter_year_x", y="inter_year_y", data=time_effected, color="green", notch=True, width=0.5)
# ax = sns.boxplot(x="high_year_x", y="high_year_y", data=time_effected, color="orange", notch=True, width=0.5)
# ax = sns.boxplot(x="extr_year_x", y="extr_year_y", data=time_effected, color="red", notch=True, width=0.5)
# x = np.linspace(2020,2200,100) 
# plt.plot(x,x,'k--')
plt.xticks(rotation=45)
plt.xlabel('Year first exposed')
plt.ylabel('Year first isolated')
plt.tight_layout()
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/box_plot_time.jpg')
plt.clf()