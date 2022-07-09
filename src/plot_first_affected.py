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

sql = """ SELECT geoid, min(rise) as first_exposed
                FROM exposed_block20
                GROUP BY geoid;
        """
expose = pd.read_sql(sql, con=engine)

# import population
sql = """ SELECT geoid, "U7B001"
                FROM origins20
        """
pop = pd.read_sql(sql, con=engine)

time_effected = pd.merge(isolate, expose, on = 'geoid')
time_effected = time_effected[time_effected['first_exposed']!=0]
time_effected = time_effected[time_effected['first_isolated']!=0]
time_effected = time_effected.reset_index()

# rise data in meters with matching dates due to three rise scenarios (extreme, high, intermediate)
exp_data = pd.read_csv('/media/CivilSystems/projects/access_usa_slr/results/exposure_state.csv')
iso_data = pd.read_csv('/media/CivilSystems/projects/access_usa_slr/results/isolation_state.csv')

year = list(range(2010,2110,10))
year.extend([2120, 2150, 2200])
extr_slr = (np.array([0.04, 0.11, 0.24, 0.41, 0.63, 0.9, 1.2, 1.6, 2, 2.5, 3.6, 5.5, 9.7])*3.281).tolist()
high_slr = (np.array([0.05, 0.11, 0.21, 0.36, 0.54, 0.77, 1, 1.3, 1.7, 2, 2.8, 4.3, 7.5])*3.281).tolist()
int_slr = (np.array([0.04, 0.1, 0.16, 0.25, 0.34, 0.45, 0.57, 0.71, 0.85, 1, 1.3, 1.8, 2.8, 3.048])*3.281).tolist()

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
    exposed_full['type'] = ['exposed']*len(list(exposed_full['rise']))
    # isolation dfs
    isolated = total_iso.append(df)
    isolated = isolated.drop(columns='Unamed: 0')
    isolated = isolated.sort_values(by='rise')
    isolated = isolated.set_index('rise', drop=False)
    isolated_full = isolated.interpolate(method='index')
    isolated_full = isolated_full.drop(isolated_full[isolated_full.year%1 < 0.0001].index)
    isolated_full['type'] = ['isolated']*len(list(isolated_full['rise']))
    # append together
    df = exposed_full.append(isolated_full)
    return df

extreme_df = interp_years(extr_slr, year)
high_df = interp_years(high_slr, year)
inter_df = interp_years(int_slr, year+[2210])

# seperate for plotting 
extr_exp = extreme_df[extreme_df['type']=='exposed']
high_exp = high_df[high_df['type']=='exposed']
inter_exp = inter_df[inter_df['type']=='exposed']
extr_iso = extreme_df[extreme_df['type']=='isolated']
high_iso = high_df[high_df['type']=='isolated']
inter_iso = inter_df[inter_df['type']=='isolated']

extr_exp = extr_exp[['rise', 'year']].drop(extr_exp.index[extr_exp['rise']==0]).astype({'year':int})
high_exp = high_exp[['rise', 'year']].drop(high_exp.index[high_exp['rise']==0]).astype({'year':int})
inter_exp = inter_exp[['rise', 'year']].drop(inter_exp.index[inter_exp['rise']==0]).astype({'year':int})
extr_iso = extr_iso[['rise', 'year']].drop(extr_iso.index[extr_iso['rise']==0]).astype({'year':int})
high_iso = high_iso[['rise', 'year']].drop(high_iso.index[high_iso['rise']==0]).astype({'year':int})
inter_iso = inter_iso[['rise', 'year']].drop(inter_iso.index[inter_iso['rise']==0]).astype({'year':int})

# building columns to insert into df
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
    extr_year_x.append(extr_exp['year'].iloc[rise_exp-1])
    high_year_x.append(high_exp['year'].iloc[rise_exp-1])
    if rise_exp == 10:
        inter_year_x.append(np.nan)
    else:
        inter_year_x.append(inter_exp['year'].iloc[rise_exp-1])
    rise_iso = time_effected['first_isolated'].iloc[i]
    extr_year_y.append(extr_iso['year'].iloc[rise_iso-1])
    high_year_y.append(high_iso['year'].iloc[rise_iso-1])
    if rise_exp == 10:
        inter_year_y.append(np.nan)
    else:
        inter_year_y.append(inter_iso['year'].iloc[rise_iso-1])

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
df = df.dropna()
df = df.astype({'year_isolated':int, 'year_exposed':int})
df = df.append(pd.DataFrame({'year_exposed':list(range(2020,2201))}))
y_labels = [2020, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 2040, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 2060, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 2080, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 2100, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 2120, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 2140, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 2160, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 2180, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 2200]



# add population
df = pd.merge(df, pop, how='left', on='geoid')
df_exposed = df['year_exposed'].repeat(df['U7B001'].fillna(1))
df_isolated = df['year_isolated'].repeat(df['U7B001'].fillna(1))
df_scenario = df['scenario'].repeat(df['U7B001'].fillna(1))


df_popweighted = pd.concat([df_exposed, df_isolated, df_scenario], axis=1)
# import code
# code.interact(local=locals())
df_popweighted = df_popweighted[df_popweighted.scenario!='high']




# box plots on one graph
fig, ax = plt.subplots()
flierprops = dict(marker='.', markerfacecolor='grey', markersize=1)
ax = sns.boxplot(y="year_exposed", x="year_isolated", data=df_popweighted, hue="scenario", notch=True, width=7, flierprops=flierprops, orient='h', whis=[5, 95], showfliers = False, palette={'extreme':'#0B2948', 'high':'#FCAB10', 'intermediate':'#95c2ee'})
y = list(range(0,181))
x = list(range(2020,2201))
plt.plot(x,y,'k--',alpha=0.5)
yticks = ax.get_yticks()
plt.yticks(rotation=0, ticks=list(range(0,181)), labels=y_labels)
plt.ylabel('Year Inundation First Occurs')
plt.xlabel('Year Isolation First Occurs')
# plt.ylim(bottom=2020, top=2200)
# plt.legend(labels = ["Intermediate", "Extreme"])
plt.tight_layout()
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/delayed_onset_box_pop.jpg')
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/delayed_onset_box_pop.pdf')
plt.close()


###
# Plot a histogram of the difference between isolation and displacement
extreme_df = interp_years(extr_slr, year)
inter_df = interp_years(int_slr, year+[2210])
high_df = interp_years(high_slr, year)

time_effected = pd.merge(isolate, expose, how='left', on = 'geoid')
time_effected['isolated_extreme'] = time_effected['first_isolated']
time_effected['isolated_intermediate'] = time_effected['first_isolated']
time_effected['exposed_intermediate'] = time_effected['first_exposed']
time_effected['exposed_extreme'] = time_effected['first_exposed']
time_effected['exposed_high'] = time_effected['first_exposed']
time_effected['isolated_high'] = time_effected['first_isolated']

mapper_extreme = extreme_df[['rise','year']].drop_duplicates().set_index('rise')['year'].to_dict()
mapper_intermediate = inter_df[['rise','year']].drop_duplicates().set_index('rise')['year'].to_dict()
mapper_high = high_df[['rise','year']].drop_duplicates().set_index('rise')['year'].to_dict()

time_effected = time_effected.replace({'isolated_extreme':mapper_extreme, 'exposed_extreme': mapper_extreme, 'isolated_intermediate':mapper_intermediate, 'exposed_intermediate': mapper_intermediate, 'isolated_high':mapper_high, 'exposed_high': mapper_high})

time_effected['difference_extreme'] = time_effected['exposed_extreme'] - time_effected['isolated_extreme']
time_effected['difference_intermediate'] = time_effected['exposed_intermediate'] - time_effected['isolated_intermediate']
time_effected['difference_high'] = time_effected['exposed_high'] - time_effected['isolated_high']

time_effected = pd.merge(time_effected, pop, on='geoid', how='left')

delayed_onset = time_effected[['difference_intermediate','difference_extreme', 'difference_high', 'U7B001']]

# plt
colors = ['#95c2ee','#0B2948']
delayed_onset = delayed_onset.fillna(170)
bin_list = np.arange(-5,185,10)
fig, ax = plt.subplots()
plt.hist(delayed_onset[['difference_extreme','difference_intermediate']], bin_list, weights=delayed_onset[['U7B001','U7B001']], color=colors)
ax.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
plt.xlabel("Time lag between onset of inundation and isolation (Years)")
plt.ylabel("Number of People")
plt.legend(labels = ["Intermediate", "Extreme"])
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
time_effected['state_code'] = time_effected['geoid'].str[:2]

time_effected.replace({'state_code': state_map_abbr}, inplace=True)


delayed_onset = time_effected[['difference_intermediate','difference_extreme', 'difference_high', 'U7B001','state_code']]
delayed_onset = delayed_onset.fillna(170)
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

# data for website with extreme and high
h = plt.hist(delayed_onset[['difference_high']], bin_list, weights=delayed_onset[['U7B001']], color=colors)
bin_centers = h[1][:-1] + np.diff(h[1])/2
plot_data = pd.DataFrame({'x':bin_centers, 'y':h[0], 'state':'all', 'scenario':'high'})
full_plot_data = pd.concat([full_plot_data, plot_data])

h = plt.hist(delayed_onset[['difference_extreme']], bin_list, weights=delayed_onset[['U7B001']], color=colors)
bin_centers = h[1][:-1] + np.diff(h[1])/2
plot_data = pd.DataFrame({'x':bin_centers, 'y':h[0], 'state':'all', 'scenario':'extreme'})
full_plot_data = pd.concat([full_plot_data, plot_data])

for state in states:
    df_plot = delayed_onset[delayed_onset.state_code==state]
    h = plt.hist(df_plot[['difference_extreme']], bin_list, weights=df_plot[['U7B001']], color=colors)
    bin_centers = h[1][:-1] + np.diff(h[1])/2
    plot_data = pd.DataFrame({'x':bin_centers, 'y':h[0], 'state':state, 'scenario':'extreme'})
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
extreme_df = interp_years(extr_slr, year)
inter_df = interp_years(int_slr, year+[2210])

time_effected = pd.merge(isolate, expose, how='left', on = 'geoid')
time_effected['isolated_extreme'] = time_effected['first_isolated']
time_effected['isolated_intermediate'] = time_effected['first_isolated']
time_effected['exposed_intermediate'] = time_effected['first_exposed']
time_effected['exposed_extreme'] = time_effected['first_exposed']

mapper_extreme = extreme_df[['rise','year']].drop_duplicates().set_index('rise')['year'].to_dict()
mapper_intermediate = inter_df[['rise','year']].drop_duplicates().set_index('rise')['year'].to_dict()

time_effected = time_effected.replace({'isolated_extreme':mapper_extreme, 'exposed_extreme': mapper_extreme, 'isolated_intermediate':mapper_intermediate, 'exposed_intermediate': mapper_intermediate})

# time_effected['difference_extreme'] = time_effected['exposed_extreme'] - time_effected['isolated_extreme']
# time_effected['difference_intermediate'] = time_effected['exposed_intermediate'] - time_effected['isolated_intermediate']

time_effected = pd.merge(time_effected, pop, on='geoid', how='left')

never_inundated = time_effected[time_effected.first_exposed.isna()]

# plt
colors = ['#95c2ee','#0B2948']
# delayed_onset = delayed_onset.fillna(170)
# bin_list = np.arange(-5,185,10)
fig, ax = plt.subplots()
plt.hist(never_inundated[['isolated_intermediate','isolated_extreme']], weights=never_inundated[['U7B001','U7B001']], color=colors)
ax.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
plt.xlabel("When are blocks that are never inundated first isolated (Year)")
plt.ylabel("Number of People")
plt.legend(labels = ["Intermediate", "Extreme"])
# labels = [item.get_text() for item in ax.get_xticklabels()]
# labels[-1] = 'NI'
# ax.set_xticklabels(labels)
plt.tight_layout()
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/never_inundated.jpg')
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/never_inundated.pdf')
plt.close()