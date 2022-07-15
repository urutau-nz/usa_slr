'''
Can we determine what the difference is between being isolated and being inundated?
'''

import main
import geopandas as gpd
import numpy as np
import pandas as pd
import yaml
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib
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
import us
state_map_abbr = us.states.mapping('fips', 'abbr')

from pylab import rcParams
rcParams['figure.figsize'] = 7, 5
rcParams['pdf.fonttype'] = 42

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
# isolate = isolate[isolate.first_isolated>0]

sql = """ SELECT geoid, min(rise) as first_inundated
                FROM exposed_block20
                GROUP BY geoid;
        """
expose = pd.read_sql(sql, con=engine)

# import population
sql = """ SELECT geoid, "U7B001"
                FROM origins20
        """
pop = pd.read_sql(sql, con=engine)



time_effected = pd.merge(isolate, expose, on = 'geoid', how='left')
time_effected = time_effected[time_effected['first_inundated']!=0]
time_effected = time_effected[time_effected['first_isolated']!=0]
time_effected = time_effected.reset_index()


# import block and gauges
sql = """ SELECT psmsl, geoid FROM block_psmsl;"""
psmsl = pd.read_sql(sql, con=engine)
psmsl.drop_duplicates(inplace=True)

# scenarios of interest
scenarios = ['1.0 - MED', '2.0 - MED']
years = {'RSL2020':2020, 'RSL2030':2030, 'RSL2040':2040, 'RSL2050':2050, 'RSL2060':2060, 'RSL2070':2070, 'RSL2080':2080, 'RSL2090':2090, 'RSL2100':2100, 'RSL2110':2110, 'RSL2120':2120, 'RSL2130':2130, 'RSL2140':2140, 'RSL2150':2150}

# import tidal gauges
rsl = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/data/psmsl_ft.csv')
rsl.drop(columns='Unnamed: 0', inplace=True)
# rsl = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/data/SLR_TF U.S. Sea Level Projections_data.csv')
# rsl.rename(columns={'PSMSL ID':'psmsl', 'Scenario':'scenario'}, inplace=True)
# years = {'RSL2020 (cm)':2020, 'RSL2030 (cm)':2030, 'RSL2040 (cm)':2040, 'RSL2050 (cm)':2050, 'RSL2060 (cm)':2060, 'RSL2070 (cm)':2070, 'RSL2080 (cm)':2080, 'RSL2090 (cm)':2090, 'RSL2100 (cm)':2100, 'RSL2110 (cm)':2110, 'RSL2120 (cm)':2120, 'RSL2130 (cm)':2130, 'RSL2140 (cm)':2140, 'RSL2150 (cm)':2150}
# rsl = rsl[['psmsl','scenario'] + list(years.keys())]

# subset
rsl = rsl[rsl.scenario.isin(scenarios)]
rsl.set_index(inplace=True, keys=['psmsl','scenario'])
rsl = rsl.stack()
rsl.name = 'rise'
rsl = pd.to_numeric(rsl, downcast='signed')

rsl = rsl.to_frame().reset_index()
# replace year string with int
rsl.rename(columns={'level_2':'year'}, inplace=True)
rsl.replace(years, inplace=True)
# rsl.rise = rsl.rise*0.0328084
# print(len(rsl))
rsl = rsl[rsl.rise<10.5]
# print(len(rsl))

# import code
# code.interact(local=locals())
# match
# time_effected = pd.merge(time_effected, psmsl, how='left', on='geoid')
# join block with rsl
result = pd.merge(time_effected,psmsl, on='geoid', how='left')
####
#  high scenario
####
rsl_high = rsl[(rsl.scenario=='2.0 - MED')]
high = result.copy()
#### isolation
high.rename(columns={'first_isolated':'rise'},inplace=True)
high = pd.merge(high,rsl_high,on=['rise','psmsl'], how='left')
high.rename(columns={'year':'year_isolated', 'rise':'first_isolated'}, inplace=True)
# which years are NaN?
nan_years = high.index[high.year_isolated.isna()].values
# loop these years and check the value isn't above 10
for i in nan_years:
    pid = high.loc[i,'psmsl']
    tmp = rsl_high[(rsl.psmsl==pid)]
    # isolation
    rise = high.loc[i,'first_isolated']
    difs = tmp['rise']-rise 
    difs[difs<0] = np.inf
    idx = difs.argsort()[:1]
    if np.isinf(difs.iloc[idx.values[0]]):
        high.loc[i,'year_isolated']=np.nan
    else:
        # dif = tmp.iloc[idx]['rise'].values[0] - rise
        # if dif < -0.5:
        #     hello
        # closest_rise = tmp.iloc[idx]['rise'].values[0]
        # if closest_rise>10:
        #     high.loc[i,'year_isolated']=np.nan
        # else:
        # else:
        high.loc[i,'year_isolated']=tmp.iloc[idx]['year'].values[0]

#### inundation
high.rename(columns={'first_inundated':'rise'},inplace=True)
high = pd.merge(high,rsl_high[['psmsl','rise','year']],on=['rise','psmsl'], how='left')
high.rename(columns={'year':'year_inundated', 'rise':'first_inundated'}, inplace=True)
# which years are NaN?
nan_years = high.index[high.year_inundated.isna()].values
# loop these years and check the value isn't above 10
for i in nan_years:
    pid = high.loc[i,'psmsl']
    tmp = rsl_high[(rsl.psmsl==pid)]
    # isolation
    rise = high.loc[i,'first_inundated']
    if np.isnan(rise):
        high.loc[i,'year_inundated']=np.nan
    else:
        difs = tmp['rise']-rise 
        difs[difs<0] = np.inf
        idx = difs.argsort()[:1]
        if np.isinf(difs.iloc[idx.values[0]]):
            high.loc[i,'year_inundated']=np.nan
        else:
            high.loc[i,'year_inundated']=tmp.iloc[idx]['year'].values[0]

high['scenario'] = '2.0 - MED'
high.dropna(axis=0, subset=['year_isolated'], inplace=True)

high = pd.merge(high, pop, how='left', on='geoid')
high[high.year_inundated.isna()]['U7B001'].sum()

import code
code.interact(local=locals())

for i in range(len(high)):
    pid = high.loc[i,'psmsl']
    tmp = rsl[(rsl.psmsl==pid)&(rsl.scenario=='2.0 - MED')]
    # isolation
    iso = high.loc[i,'first_isolated']
    idx = (tmp['rise']-iso).abs().argsort()[:1]
    dif = tmp.iloc[idx]['rise'].values[0] - iso
    if dif < -0.5:
        high.loc[i,'year_isolated']=np.nan
    else:
        high.loc[i,'year_isolated']=tmp.iloc[idx]['year'].values[0]
    # inundation
    inu = high.loc[i,'first_inundated']
    if np.isnan(inu):
        high.loc[i,'year_inundated']=np.nan
    else:
        idx = (tmp['rise']-inu).abs().argsort()[:1]
        dif = tmp.iloc[idx]['rise'].values[0] - inu
        if dif < -0.5:
            high.loc[i,'year_inundated']=np.nan
        else:
            high.loc[i,'year_inundated']=tmp.iloc[(tmp['rise']-inu).abs().argsort()[:1]]['year'].values[0]

intermediate = result.copy()
intermediate['year_isolated']=None
intermediate['year_inundated']=None
for i in range(len(intermediate)):
    pid = intermediate.loc[i,'psmsl']
    tmp = rsl[(rsl.psmsl==pid)&(rsl.scenario=='1.0 - MED')]
    # isolation
    iso = intermediate.loc[i,'first_isolated']
    idx = (tmp['rise']-iso).abs().argsort()[:1]
    dif = tmp.iloc[idx]['rise'].values[0] - iso
    if dif < -0.5:
        intermediate.loc[i,'year_isolated']=np.nan
    else:
        intermediate.loc[i,'year_isolated']=tmp.iloc[idx]['year'].values[0]
    # inundation
    inu = intermediate.loc[i,'first_inundated']
    if np.isnan(inu):
        intermediate.loc[i,'year_inundated']=np.nan
    else:
        idx = (tmp['rise']-inu).abs().argsort()[:1]
        dif = tmp.iloc[idx]['rise'].values[0] - inu
        if dif < -0.5:
            intermediate.loc[i,'year_inundated']=np.nan
        else:
            intermediate.loc[i,'year_inundated']=tmp.iloc[idx]['year'].values[0]

intermediate.dropna(axis=0, subset=['year_isolated'], inplace=True)
high['scenario'] = 'high'
intermediate['scenario'] = 'intermediate'
df = pd.concat([high, intermediate])
df.dropna(axis=0, inplace=True)


#######
# Box Plot: year of isolated vs inundated 
#######
# add population and duplicate rows per person
df = pd.merge(df, pop, how='left', on='geoid')
df_exposed = df['year_inundated'].repeat(df['U7B001'].fillna(1))
df_isolated = df['year_isolated'].repeat(df['U7B001'].fillna(1))
df_scenario = df['scenario'].repeat(df['U7B001'].fillna(1))

df_popweighted = pd.concat([df_exposed, df_isolated, df_scenario], axis=1)
df_popweighted.sort_values(by=['year_inundated'], inplace=True) 

# plot
y_labels = df_popweighted.year_inundated.unique()#[2020, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 2040, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 2060, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 2080, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 2100, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 2120, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 2140, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 2160, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 2180, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 2200]
# box plots on one graph
fig, ax = plt.subplots()
flierprops = dict(marker='.', markerfacecolor='grey', markersize=1)
ax = sns.boxplot(y="year_inundated", x="year_isolated", data=df_popweighted, hue="scenario", notch=False, flierprops=flierprops, orient='h', whis=[5, 95], showfliers = False, palette={'extreme':'#0B2948', 'high':'#0B2948', 'intermediate':'#95c2ee'})
y = list(range(len(y_labels)))
x = y_labels#list(range(len(y_labels)))
plt.plot(x,y,'k--',alpha=0.5)
yticks = ax.get_yticks()
plt.yticks(rotation=0, ticks=list(range(len(y_labels))), labels=y_labels)
plt.ylabel('Year Inundation First Occurs')
plt.xlabel('Year Isolation First Occurs')
# plt.ylim(bottom=2020, top=2200)
# plt.legend(labels = ["Intermediate", "Extreme"])
plt.tight_layout()
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/delayed_onset_box_pop.jpg')
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/delayed_onset_box_pop.pdf')
plt.close()

#######
# Histogram: Difference in years between isolation and displacement
#######
# calculate difference in years
high['difference_high'] = high['year_inundated'] - high['year_isolated']
intermediate['difference_intermediate'] = intermediate['year_inundated'] - intermediate['year_isolated']
# merge with population of the blocks
high = pd.merge(high, pop, how='left', on='geoid')
intermediate = pd.merge(intermediate, pop, how='left', on='geoid')
# how many are isolated but never inundated
intermediate.loc[intermediate.difference_intermediate.isna(), 'U7B001'].sum()
high.loc[high.difference.isna(), 'U7B001'].sum()


delayed_onset = pd.merge(high[['geoid','difference_high']], intermediate[['geoid','difference_intermediate']], on='geoid')
delayed_onset = pd.merge(delayed_onset, pop, how='left', on='geoid')
delayed_onset = delayed_onset[['difference_intermediate', 'difference_high', 'U7B001']]

# plt
colors = ['#0B2948','#95c2ee']
delayed_onset = delayed_onset.fillna(120)
bin_list = np.arange(-5,135,10)
fig, ax = plt.subplots()
plt.hist(delayed_onset[['difference_high','difference_intermediate']], bin_list, weights=delayed_onset[['U7B001','U7B001']], color=colors)
ax.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
plt.xlabel("Time lag between onset of inundation and isolation (Years)")
plt.ylabel("Number of People")
plt.legend(labels = ["High", "Intermediate"])
# labels = [item.get_text() for item in ax.get_xticklabels()]
# labels[-1] = 'NI'
# ax.set_xticklabels(labels)
plt.tight_layout()
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/delayed_onset_histogram.jpg')
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/delayed_onset_histogram.pdf')
plt.close()



##
# create these figures by state
rcParams['figure.figsize'] = 2.8, 2.4
rcParams.update({'font.size': 20})
rcParams['axes.titley'] = 1.0    # y is in axes-relative coordinates.
rcParams['axes.titlepad'] = -14
high['difference_high'] = high['year_inundated'] - high['year_isolated']
intermediate['difference_intermediate'] = intermediate['year_inundated'] - intermediate['year_isolated']
delayed_onset = pd.merge(high[['geoid','difference_high']], intermediate[['geoid','difference_intermediate']], on='geoid')
delayed_onset = pd.merge(delayed_onset, pop, how='left', on='geoid')
delayed_onset['state_code'] = delayed_onset['geoid'].str[:2]

delayed_onset.replace({'state_code': state_map_abbr}, inplace=True)


delayed_onset = delayed_onset[['difference_intermediate','difference_high', 'U7B001','state_code']]
# delayed_onset = delayed_onset.fillna(120)
# loop through states
states = delayed_onset.state_code.unique()
colors = ['#95c2ee']

full_plot_data = pd.DataFrame()
h = plt.hist(delayed_onset[['difference_intermediate']], bin_list, weights=delayed_onset[['U7B001']], color=colors)
bin_centers = h[1][:-1] + np.diff(h[1])/2
plot_data = pd.DataFrame({'x':bin_centers, 'y':h[0], 'state':'all', 'scenario':'intermediate'})
full_plot_data = pd.concat([full_plot_data, plot_data])

for state in states:
    df_plot = delayed_onset[delayed_onset.state_code==state]
    fig, ax = plt.subplots()
    right_side = ax.spines["right"]
    top_side = ax.spines["top"]
    right_side.set_visible(False)
    top_side.set_visible(False)
    h = plt.hist(df_plot[['difference_intermediate']], bin_list, weights=df_plot[['U7B001']], color=colors)
    bin_centers = h[1][:-1] + np.diff(h[1])/2
    plot_data = pd.DataFrame({'x':bin_centers, 'y':h[0], 'state':state, 'scenario':'intermediate'})
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda y, pos: '%.0f' % (y * 1e-3)))
    # ax.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
    # plt.xlabel("Time lag between onset of inundation and isolation (Years)")
    # plt.ylabel("Number of People")
    # plt.legend(labels = ["Intermediate", "Extreme"])
    plt.title(state)
    # labels = [item.get_text() for item in ax.get_xticklabels()]
    # labels[-1] = 'NI'
    # ax.set_xticklabels(labels)
    # plt.xticks(rotation = 90)
    plt.yticks(rotation = 90)
    plt.tight_layout()
    plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/delayed_onset_histogram_{}.pdf'.format(state))
    plt.close()
    full_plot_data = pd.concat([full_plot_data, plot_data])


delayed_onset.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/delayed_onset.csv')

# data for website with high
h = plt.hist(delayed_onset[['difference_high']], bin_list, weights=delayed_onset[['U7B001']], color=colors)
bin_centers = h[1][:-1] + np.diff(h[1])/2
plot_data = pd.DataFrame({'x':bin_centers, 'y':h[0], 'state':'all', 'scenario':'high'})
full_plot_data = pd.concat([full_plot_data, plot_data])


for state in states:
    df_plot = delayed_onset[delayed_onset.state_code==state]
    h = plt.hist(df_plot[['difference_high']], bin_list, weights=df_plot[['U7B001']], color=colors)
    bin_centers = h[1][:-1] + np.diff(h[1])/2
    plot_data = pd.DataFrame({'x':bin_centers, 'y':h[0], 'state':state, 'scenario':'high'})
    full_plot_data = pd.concat([full_plot_data, plot_data])


full_plot_data.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/delayed_onset_histogram_data.csv')



###
# Plot a histogram of the that never inundated blocks are first isolated
rcParams['figure.figsize'] = 7, 5
rcParams['pdf.fonttype'] = 42

time_effected = pd.merge(isolate, expose, how='left', on = 'geoid')
time_effected = time_effected[time_effected['first_inundated']!=0]
time_effected = time_effected[time_effected['first_isolated']!=0]
time_effected['isolated_high'] = None
time_effected['isolated_intermediate'] = None
time_effected = time_effected.reset_index()
never_inundated = time_effected[time_effected.first_inundated.isna()]

never_inundated = pd.merge(never_inundated,psmsl, on='geoid', how='left')

for i in range(len(never_inundated)):
    pid = never_inundated.loc[i,'psmsl']
    iso = never_inundated.loc[i,'first_isolated']
    high = rsl[(rsl.psmsl==pid)&(rsl.scenario=='2.0 - MED')]
    idx = (high['rise']-iso).abs().argsort()[:1]
    dif = high.iloc[idx]['rise'].values[0] - iso
    if dif < -0.5:
        never_inundated.loc[i,'isolated_high']=np.nan
    else:
        never_inundated.loc[i,'isolated_high']=high.iloc[idx]['year'].values[0]
    # intermediate
    inter = rsl[(rsl.psmsl==pid)&(rsl.scenario=='1.0 - MED')]
    idx = (inter['rise']-iso).abs().argsort()[:1]
    dif = inter.iloc[idx]['rise'].values[0] - iso
    if dif < -0.5:
        never_inundated.loc[i,'isolated_intermediate']=np.nan
    else:
        never_inundated.loc[i,'isolated_intermediate']=inter.iloc[idx]['year'].values[0]

never_inundated = pd.merge(never_inundated, pop, on='geoid', how='left')

never_inundated.dropna(axis=0, subset=['isolated_high'], inplace=True)

never_inundated['int_pop'] = never_inundated['U7B001'].copy()
never_inundated['int_pop'][never_inundated.isolated_intermediate.isna()]=0

# plt
colors = ['#95c2ee','#0B2948']
# delayed_onset = delayed_onset.fillna(170)
bin_list = np.arange(2015,2165,10)
fig, ax = plt.subplots()
plt.hist(never_inundated[['isolated_intermediate','isolated_high']], weights=never_inundated[['int_pop','U7B001']], color=colors, bins=bin_list)
ax.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
plt.xlabel("When are blocks that are never inundated first isolated (Year)")
plt.ylabel("Number of People")
plt.legend(labels = ["Intermediate", "High"])
# labels = [item.get_text() for item in ax.get_xticklabels()]
# labels[-1] = 'NI'
# ax.set_xticklabels(labels)
plt.tight_layout()
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/never_inundated.jpg')
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/never_inundated.pdf')
plt.close()

