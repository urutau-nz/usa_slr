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

db = main.init_db(config)

# import data
sql = """ SELECT geoid, min(rise) as first_isolated
                FROM isolated_block20
                GROUP BY geoid;
        """
isolate = pd.read_sql(sql, db['engine'])

sql = """ SELECT geoid, min(rise) as first_exposed
                FROM exposed_block20
                GROUP BY geoid;
        """
expose = pd.read_sql(sql, db['engine'])


time_effected = pd.merge(expose, isolate, on = 'geoid')


# distribution plots
# sns.displot(data=time_effected, x="first_exposed", y="first_isolated") #, kind="kde"
# plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/time lag before exposure.jpg')
# plt.savefig('src/figs/test.jpg')
# plt.cla()

# >>>>>>>>>>> can you add into time_effected a column for year. so intermediate_exposed | intermediate_isolated | high_exposed | high_isolated
# >>> Then create the below figure so that it is showing by time instead of slr


## boxplots
fig, ax = plt.subplots()
ax = sns.boxplot(x="first_exposed", y="first_isolated", data=time_effected, color="white", notch=True)
ax = sns.swarmplot(x="first_exposed", y="first_isolated", data=time_effected, color="grey")
x = np.linspace(1,11,11) 
plt.plot(x,x,'k--') # identity line
plt.xlim(1,11)
plt.ylim(1,10)
# plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/time lag before exposure box.jpg')
plt.savefig('src/figs/test3.jpg')
plt.cla()



hi
