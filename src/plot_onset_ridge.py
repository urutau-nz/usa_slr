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
from sklearn.neighbors import KernelDensity

import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.gridspec as grid_spec

from pylab import rcParams
rcParams['figure.figsize'] = 7, 5
rcParams['pdf.fonttype'] = 42

passw = open('/media/CivilSystems/admin/pass.txt', 'r').read().strip('\n')
port = '5002'
db_host = 'encivmu-tml62'
db_name = 'usa_slr'
engine = create_engine('postgresql+psycopg2://postgres:' + passw + '@' + db_host + '/' + db_name + '?port=' + port)

# import population
sql = """ SELECT geoid, "U7B001"
                FROM origins20
        """
pop = pd.read_sql(sql, con=engine)

# import isolation and inundation data
sql = """ SELECT geoid, rise FROM isolated_block20 WHERE rise>0;"""
isolated = pd.read_sql(sql, engine)
isolated.drop_duplicates(inplace=True)
sql = """ SELECT geoid, rise FROM exposed_block20 WHERE rise>0;"""
inundated = pd.read_sql(sql, engine)
inundated.drop_duplicates(inplace=True)

# how many are not isolated at higher values
problem_blocks = []
for i in range(10):
    problem_blocks.append(set(isolated[isolated.rise==i].geoid)-set(isolated[isolated.rise==10].geoid))

problem_blocks = set().union(*problem_blocks)
# remove these from isolated
isolated = isolated[~isolated.geoid.isin(problem_blocks)]

# groupby to find the minimum
isolate = isolated.groupby(by='geoid').min().rename(columns={'rise':'first_isolated'})
expose = inundated.groupby(by='geoid').min().rename(columns={'rise':'first_exposed'})

# join
time_effected = isolate.join(expose)
time_effected = time_effected.reset_index()
time_effected.dropna(inplace=True)
# import code
# code.interact(local=locals())

###
# Box plot on rise increments
box_rise = pd.merge(time_effected, pop, how='left', on='geoid')
ridge_iso = box_rise['first_isolated'].repeat(box_rise['U7B001'])
ridge_exp = box_rise['first_exposed'].repeat(box_rise['U7B001'])
data = pd.concat([ridge_iso,ridge_exp],axis=1)

data = data*0.3
data = data.round(1)


countries = [x for x in np.unique(data.first_exposed)]
# colors = ['#0000ff', '#3300cc', '#660099', '#990066', '#cc0033', '#ff0000']

gs = grid_spec.GridSpec(len(countries),1)
fig = plt.figure()

i = 0

ax_objs = []
for country in countries:
    country = countries[i]
    x = np.array(data[data.first_exposed == country].first_isolated)
    # creating new axes object
    ax_objs.append(fig.add_subplot(gs[i:i+1, 0:]))
    # plotting the distribution
    bin_list = np.arange(0.5,11.5,1)*0.3
    ax_objs[-1].hist(x, bin_list, color="#0B2948", density=True, edgecolor='white', linewidth=0.2)
    # make background transparent
    rect = ax_objs[-1].patch
    rect.set_alpha(0)
    # remove borders, axis ticks, and labels
    ax_objs[-1].set_yticklabels([])
    if i == len(countries)-1:
        ax_objs[-1].set_xlabel("SLR Increment First Isolated", fontsize=16,fontweight="bold")
    else:
        ax_objs[-1].set_xticklabels([])
    spines = ["top","right","left","bottom"]
    for s in spines:
        ax_objs[-1].spines[s].set_visible(False)
    adj_country = country
    ax_objs[-1].text(-0.02,0,adj_country,fontweight="bold",fontsize=14,ha="right")
    i += 1

gs.update(hspace=0.05)


plt.tight_layout()
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/delayed_onset_ridge.jpg')
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/delayed_onset_ridge.pdf')
plt.close()