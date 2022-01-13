import geopandas as gpd 
import pandas as pd 
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# renaming columns so i know whats what
exp_data = pd.read_csv('/media/CivilSystems/projects/access_usa_slr/results/exposure_state.csv')
iso_data = pd.read_csv('/media/CivilSystems/projects/access_usa_slr/results/isolation_state.csv')

year = list(range(2010,2110,10))
year.extend([2120, 2150, 2200])
extr_slr = np.array([0.04, 0.11, 0.24, 0.41, 0.63, 0.9, 1.2, 1.6, 2, 2.5, 3.6, 5.5, 9.7]).tolist()
high_slr = np.array([0.05, 0.11, 0.21, 0.36, 0.54, 0.77, 1, 1.3, 1.7, 2, 2.8, 4.3, 7.5]).tolist()
int_slr = np.array([0.04, 0.1, 0.16, 0.25, 0.34, 0.45, 0.57, 0.71, 0.85, 1, 1.3, 1.8, 2.8]).tolist()

total_exp = exp_data.groupby('rise').sum()
total_exp['rise'] = total_exp.index
total_exp['rise'] = total_exp['rise']/3.281
list_exp = total_exp['U7B001'].tolist()
total_iso = iso_data.groupby('rise').sum()
total_iso['rise'] = total_iso.index
total_iso['rise'] = total_iso['rise']/3.281

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

# plot figure
fig, ax = plt.subplots()
# ax2 = ax.twinx()
ax.plot(extr_exp['year'], extr_exp['U7B001'], color='#1f386b', label='Exposed extreme SLR')
ax.scatter(extr_exp['year'], extr_exp['U7B001'], color='#1f386b')
ax.plot(high_exp['year'], high_exp['U7B001'], color='#627397', label='Exposed high SLR')
ax.scatter(high_exp['year'], high_exp['U7B001'], color='#627397')
ax.plot(inter_exp['year'], inter_exp['U7B001'], color='#a5afc3', label='Exposed intermediate SLR')
ax.scatter(inter_exp['year'], inter_exp['U7B001'], color='#a5afc3')

ax.plot(extr_iso['year'], extr_iso['U7B001'], color='#1f386b', label='Isolated extreme SLR', linestyle='--')
ax.scatter(extr_iso['year'], extr_iso['U7B001'], color='#1f386b')
ax.plot(high_iso['year'], high_iso['U7B001'], color='#627397', label='Isolated high SLR', linestyle='--')
ax.scatter(high_iso['year'], high_iso['U7B001'], color='#627397')
ax.plot(inter_iso['year'], inter_iso['U7B001'], color='#a5afc3', label='Isolated intermediate SLR', linestyle='--')
ax.scatter(inter_iso['year'], inter_iso['U7B001'], color='#a5afc3')

plt.ylabel('Number of people')
plt.xlabel('Year')
plt.legend()
plt.savefig('src/figs/time_slr.jpg')

