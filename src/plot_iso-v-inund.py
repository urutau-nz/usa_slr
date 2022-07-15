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

# how many are not isolated at higher values
problem_blocks = []
for i in range(10):
    problem_blocks.append(set(isolated[isolated.rise==i].geoid)-set(isolated[isolated.rise==10].geoid))

problem_blocks = set().union(*problem_blocks)
# remove these from isolated
isolated = isolated[~isolated.geoid.isin(problem_blocks)]

# plot for each year and scenario
iso = isolated.groupby(by='rise').sum().reset_index()
inu = inundated.groupby(by='rise').sum().reset_index()


#Plotting
fig, ax = plt.subplots()
right_side = ax.spines["right"]
top_side = ax.spines["top"]
right_side.set_visible(False)
top_side.set_visible(False)
plt.xlabel("Sea Level Rise (m)")
plt.ylabel("Number of People")
# ax.set_yticks(ticks=(np.arange(0, 12000000, step = 1000000)))
# ax.set_xticks(ticks=(np.arange(0, 11, step = 1)))
ax.plot(iso.rise*0.3, iso.U7B001, color = '#0B2948', linewidth=2, linestyle='--')
ax.plot(inu.rise*0.3, inu.U7B001, color = '#8D162A', linewidth=2)
ax.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
ax.legend(labels = ["Isolated", "Inundated"])
plt.ylim(0,16e6)
plt.xlim(0.3,3)
plt.tight_layout()
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/iso-v-inu.jpg')
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/iso-v-inu.pdf')
# plt.show()

