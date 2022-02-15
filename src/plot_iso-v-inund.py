from matplotlib import colors
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import matplotlib

#Read data
iso = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/isolation_country.csv')
inu = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/exposure_country.csv')

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
ax.plot(iso.rise[1:11]*0.3, iso.U7B001[1:11], color = '#0B2948', linewidth=2, linestyle='--')
ax.plot(inu.rise*0.3, inu.U7B001, color = '#0B2948', linewidth=2)
ax.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
ax.legend(labels = ["Isolated", "Displaced"])
plt.tight_layout()
plt.savefig('/home/tml/CivilSystems/projects/access_usa_slr/fig/iso-v-inu.jpg')
# plt.show()