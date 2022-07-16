from matplotlib.lines import _LineStyle
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
import us
state_map_abbr = us.states.mapping('fips', 'abbr')

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
psmsl.set_index('psmsl',inplace=True)
# psmsl.set_index(inplace=True, keys=['psmsl'])

# scenarios of interest
scenarios = ['1.0 - LOW', '1.0 - MED', '1.0 - HIGH', '2.0 - LOW', '2.0 - MED', '2.0 - HIGH']
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
rsl.set_index('psmsl',inplace=True)

# join block with rsl
result = psmsl.join(rsl)#, on='psmsl')
result.reset_index(inplace=True)

# join with isolation results
result = pd.merge(result,isolated[['geoid', 'rise', 'U7B001']], on=['geoid','rise'])
# rename
result.rename(columns={'U7B001':'isolated'}, inplace=True)
# join with inundation results
result = pd.merge(result, inundated[['geoid', 'rise', 'U7B001']], on=['geoid','rise'], how='left')
# rename
result.rename(columns={'U7B001':'inundated'}, inplace=True)

###
# Compare differences in onset
###
high = result[result.scenario=='2.0 - MED']
first_isolated = high.groupby('geoid').min()[['year']]
first_inundated = high.dropna(axis=0,subset=['inundated']).groupby('geoid').min()[['year']]
high_year = first_isolated.join(first_inundated,how='left',lsuffix='_isolated',rsuffix='_inundated')
high_year = high_year.join(pop, how='left')
high_year['difference'] = high_year['year_inundated'] - high_year['year_isolated']
dif_high = high_year.groupby('difference', dropna=False)['U7B001'].sum()

intermediate = result[result.scenario=='1.0 - MED']
first_isolated = intermediate.dropna(axis=0,subset=['isolated']).groupby('geoid').min()[['year']]
first_inundated = intermediate.dropna(axis=0,subset=['inundated']).groupby('geoid').min()[['year']]
intermediate_year = first_isolated.join(first_inundated,how='left',lsuffix='_isolated',rsuffix='_inundated')
intermediate_year = intermediate_year.join(pop, how='left')
intermediate_year['difference'] = intermediate_year['year_inundated'] - intermediate_year['year_isolated']
dif_inter = intermediate_year.groupby('difference', dropna=False)['U7B001'].sum()

dif_onset = pd.DataFrame({'Intermediate':dif_inter, 'High':dif_high})

# plt
ax = dif_onset.plot.bar(color={'High':'#0B2948','Intermediate':'#95c2ee'})
ax.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
plt.xlabel("Time lag between onset of inundation and isolation (Years)")
plt.ylabel("Number of People")
ax.spines.right.set_visible(False)
ax.spines.top.set_visible(False)
plt.tight_layout()
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/delayed_onset_histogram.jpg')
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/delayed_onset_histogram.pdf')
plt.close()

###
# Repeat for the States
###
rcParams['figure.figsize'] = 2.8, 2.4
rcParams.update({'font.size': 20})
rcParams['axes.titley'] = 1.0    # y is in axes-relative coordinates.
rcParams['axes.titlepad'] = -14

dif_high_all = high_year.groupby('difference', dropna=False)[['U7B001']].sum()
dif_high_all['state_code'] = 'all'
dif_high_all['scenario'] = 'high'
dif_high_all.reset_index(inplace=True)

dif_inter_all = intermediate_year.groupby('difference', dropna=False)[['U7B001']].sum()
dif_inter_all['state_code'] = 'all'
dif_inter_all['scenario'] = 'intermediate'
dif_inter_all.reset_index(inplace=True)

high_year_state = high_year.reset_index()
high_year_state['state_code'] = high_year_state['geoid'].str[:2]
high_year_state = high_year_state.groupby(['state_code','difference'], dropna=False)[['U7B001']].sum()
high_year_state.reset_index(inplace=True)
high_year_state.replace({'state_code': state_map_abbr}, inplace=True)
high_year_state['scenario'] = 'high'

intermediate_year_state = intermediate_year.reset_index()
intermediate_year_state['state_code'] = intermediate_year_state['geoid'].str[:2]
intermediate_year_state = intermediate_year_state.groupby(['state_code','difference'], dropna=False)[['U7B001']].sum()
intermediate_year_state.reset_index(inplace=True)
intermediate_year_state.replace({'state_code': state_map_abbr}, inplace=True)
intermediate_year_state['scenario'] = 'intermediate'

# for the dashboard
full_plot_data = pd.concat([dif_high_all, dif_inter_all, high_year_state, intermediate_year_state])
full_plot_data.rename(columns={'difference':'x','U7B001':'y','state_code':'state'}, inplace=True)
full_plot_data.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/delayed_onset_histogram_data.csv')
full_plot_data = full_plot_data.fillna(140)
full_plot_data.x = full_plot_data.x.astype(int)

# plots for states
for state in full_plot_data.state.unique():
    data = full_plot_data[(full_plot_data.state==state)&(full_plot_data.scenario=='intermediate')]
    x_missing = set(full_plot_data.x)-set(data.x)
    empty = pd.DataFrame([[x, 0, state, 'intermediate'] for x in x_missing],columns=['x','y','state','scenario'])
    data = pd.concat([data,empty])
    data = data.sort_values(by='x')
    ax = data.plot.bar(x='x',y='y', color='#95c2ee', legend=False, width=1)
    # ax.stem(df_plot.x, df_plot.y, linefmt='#95c2ee', markerfmt=' ', basefmt=" ")
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda y, pos: '%.0f' % (y * 1e-3)))
    ax.spines.right.set_visible(False)
    ax.spines.top.set_visible(False)
    plt.title(state)
    plt.ylabel('')
    plt.yticks(rotation = 90)
    ax.set_xticklabels(labels=[0] + [None]*9 + [100] + [None]*4,rotation=0)
    plt.tight_layout()
    plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/delayed_onset_histogram_{}.pdf'.format(state))
    plt.close()

###
# For the people never inundated, when are they first isolated
###
rcParams['figure.figsize'] = 7, 5
rcParams['pdf.fonttype'] = 42

when_isolated = pd.DataFrame(
    {'Intermediate':intermediate_year.loc[intermediate_year.year_inundated.isna(), ['year_isolated','U7B001']].groupby('year_isolated').sum()['U7B001'],
    'High':high_year.loc[high_year.year_inundated.isna(), ['year_isolated','U7B001']].groupby('year_isolated').sum()['U7B001']
    })

ax = when_isolated.plot.bar(color={'High':'#0B2948','Intermediate':'#95c2ee'})
ax.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
plt.xlabel("When are blocks that are never inundated first isolated (Year)")
plt.ylabel("Number of People")
# plt.legend(labels = {'high':"High", 'intermediate':"Intermediate"})
# labels = [item.get_text() for item in ax.get_xticklabels()]
# labels[-1] = 'NI'
# ax.set_xticklabels(labels)
ax.spines.right.set_visible(False)
ax.spines.top.set_visible(False)
plt.xticks(rotation = 0)
plt.tight_layout()
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/never_inundated.jpg')
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/never_inundated.pdf')
plt.close()




#######
# Box Plot: year of isolated vs inundated 
#######
# add population and duplicate rows per person
intermediate_year['scenario']='intermediate'
high_year['scenario']='high'
df = pd.concat([intermediate_year, high_year])
df = df[df['year_inundated']>2020]
df = df[df['year_isolated']>2020]
df.dropna(inplace=True)
df_exposed = df['year_inundated'].repeat(df['U7B001'])
df_isolated = df['year_isolated'].repeat(df['U7B001'])
df_scenario = df['scenario'].repeat(df['U7B001'])

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


