""" bar chart plotting with acceess_usa_slr/results data
"""
import geopandas as gpd 
import pandas as pd 
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from pylab import rcParams
rcParams['figure.figsize'] = 7, 5
rcParams['pdf.fonttype'] = 42

# import data
exp_data = pd.read_csv('/media/CivilSystems/projects/access_usa_slr/results/exposure_state.csv')
iso_data = pd.read_csv('/media/CivilSystems/projects/access_usa_slr/results/isolation_state.csv')

def plot(exp_data, iso_data, percent):
    """ plot bar plot by population percentage if true or by number of people if false
    """
    # df of exposed pop over slr scenarios
    exp_10ft = exp_data[exp_data['rise'] == 10]
    exp_10ft.index = exp_10ft['state_name']
    # getting by population percentage of each state
    if percent == True:
        sorting = exp_10ft['state_name'].to_list()
        state_db = state_db[state_db['STATE'].isin(sorting)]
        state_db = state_db[['STATE', 'pop_total']]
        state_db.reset_index(inplace=True)
        state_db.set_index('STATE', inplace=True)
        state_db = state_db.reindex(exp_10ft.index)
        state_pop = state_db['pop_total'].to_list()
        exp_10ft['state_pop'] = state_pop
        exp_10ft['pop_percentage'] = (exp_10ft['U7B001']/exp_10ft['state_pop'])*100
        exp_10ft = exp_10ft.sort_values(by='pop_percentage', ascending=True)
        state_pop = list(exp_10ft['state_pop']) # resetting index after sorting data by %pop
    # sorting other slr scenarios
    exp_6ft = exp_data[exp_data['rise'] == 6] 
    exp_6ft.index = exp_6ft['state_name']
    if percent == True:
        exp_6ft['state_pop'] = state_pop
        exp_6ft['pop_percentage'] = (exp_6ft['U7B001']/exp_6ft['state_pop'])*100
    exp_3ft = exp_data[exp_data['rise'] == 3]
    exp_3ft.index = exp_3ft['state_name']
    if percent == True:
        exp_3ft['state_pop'] = state_pop
        exp_3ft['pop_percentage'] = (exp_3ft['U7B001']/exp_3ft['state_pop'])*100
    # df of isolated pop over slr scenarios 
    iso_10ft = iso_data[iso_data['rise'] == 10].sort_values(by='U7B001', ascending=True)
    iso_10ft.index = iso_10ft['state_name']
    if percent == True:
        iso_10ft['state_pop'] = state_pop
        iso_10ft['pop_percentage'] = (iso_10ft['U7B001']/iso_10ft['state_pop'])*100
    iso_6ft = iso_data[iso_data['rise'] == 6] 
    iso_6ft.index = iso_6ft['state_name']
    if percent == True:
        iso_6ft['state_pop'] = state_pop
        iso_6ft['pop_percentage'] = (iso_6ft['U7B001']/iso_6ft['state_pop'])*100
    iso_3ft = iso_data[iso_data['rise'] == 3]
    iso_3ft.index = iso_3ft['state_name']
    if percent == True:
        iso_3ft['state_pop'] = state_pop
        iso_3ft['pop_percentage'] = (iso_3ft['U7B001']/iso_3ft['state_pop'])*100
    # plotting data
    fig, ax = plt.subplots()
    # if percent == True:
    
    # percentage
    exp_10ft = exp_10ft.sort_values(by='U7B001', ascending=True)
    iso_10ft = iso_10ft.reindex(exp_10ft.index)
    iso_6ft = iso_6ft.reindex(iso_10ft.index)
    iso_3ft = iso_3ft.reindex(iso_10ft.index)
    exp_6ft = exp_6ft.reindex(iso_10ft.index)
    exp_3ft = exp_3ft.reindex(iso_10ft.index)

    ax.barh(iso_10ft['state_name'], iso_10ft['U7B001'], color='#a5afc3', label='Isolated at 3.0m SLR')
    ax.barh(iso_6ft['state_name'], iso_6ft['U7B001'], color='#627397', label='Isolated at 1.8m SLR')
    ax.barh(iso_3ft['state_name'], iso_3ft['U7B001'], color='#1f386b', label='Isolated at 0.9m SLR')
    ax.plot(exp_10ft['U7B001'], exp_10ft['state_name'], color='darkred', label='Inundated at 3.0m SLR')
    ax.plot(exp_6ft['U7B001'], exp_6ft['state_name'], color='firebrick', label='Inundated at 1.8m SLR')
    ax.plot(exp_3ft['U7B001'], exp_3ft['state_name'], color='lightcoral', label='Inundated at 0.9m SLR')
    ax.set_xlabel('Number of People')
    ax.legend(loc='lower right')
    ax.set_yticklabels(exp_10ft['state_name'], rotation=0)
    plt.tight_layout()
    plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/bar_population.jpg')
    plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/bar_population.pdf')
    plt.cla()

    # percentage
    exp_10ft = exp_10ft.sort_values(by='U7B001_percentage', ascending=True)
    iso_10ft = iso_10ft.reindex(exp_10ft.index)
    iso_6ft = iso_6ft.reindex(iso_10ft.index)
    iso_3ft = iso_3ft.reindex(iso_10ft.index)
    exp_6ft = exp_6ft.reindex(iso_10ft.index)
    exp_3ft = exp_3ft.reindex(iso_10ft.index)


    ax.barh(iso_10ft['state_name'], iso_10ft['U7B001_percentage'], color='#a5afc3', label='Isolated at 3.0m SLR')
    ax.barh(iso_6ft['state_name'], iso_6ft['U7B001_percentage'], color='#627397', label='Isolated at 1.8m SLR')
    ax.barh(iso_3ft['state_name'], iso_3ft['U7B001_percentage'], color='#1f386b', label='Isolated at 0.9m SLR')
    ax.plot(exp_10ft['U7B001_percentage'], exp_10ft['state_name'], color='darkred', label='Inundated at 3.0m SLR')
    ax.plot(exp_6ft['U7B001_percentage'], exp_6ft['state_name'], color='firebrick', label='Inundated at 1.8m SLR')
    ax.plot(exp_3ft['U7B001_percentage'], exp_3ft['state_name'], color='lightcoral', label='Inundated at 0.9m SLR')
    ax.set_xlabel('Percentage of State Population (%)')
    ax.legend(loc='lower right')
    ax.set_yticklabels(exp_10ft['state_name'], rotation=0)
    plt.tight_layout()
    plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/bar_population_percentage.jpg')
    plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/bar_population_percentage.pdf')


plot(exp_data, iso_data, False)
# plot(exp_data, iso_data, True)