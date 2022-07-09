import main
import geopandas as gpd
import pandas as pd
from scipy.spatial.distance import cdist
import yaml
import us
state_map_abbr = us.states.mapping('fips', 'abbr')
state_map_name = us.states.mapping('fips', 'name')
with open('./config/main.yaml') as file:
    config = yaml.safe_load(file)

db = main.init_db(config)

# import Sweet RSL projections
rsl = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/data/SLR_TF U.S. Sea Level Projections_data.csv')


sql = """ SELECT all_blocks.geoid as geoid, st_x(o.centroid), st_y(o.centroid) 
            FROM (SELECT geoid FROM isolated_block20 
                UNION ALL
                SELECT geoid FROM exposed_block20) as all_blocks
            LEFT JOIN origins20 as o USING (geoid);
        """
blocks = pd.read_sql(sql, db['engine'])
blocks.drop_duplicates(inplace=True)
blocks.set_index('geoid', inplace=True)

# dataframe columns: geoid | scenario | year | rsl | risk | ft | demographic | value


def closest_point(point, points):
    """ Find closest point from a list of points. """
    return points[cdist([point], points).argmin()]

def match_value(df, col1, x, col2):
    """ Match value x from col1 row to value in col2. """
    return df[df[col1] == x][col2].values[0]

blocks['point'] = [(x, y) for x,y in zip(blocks['st_y'], blocks['st_x'])]
rsl['point'] = [(x, y) for x,y in zip(rsl['Lat'], rsl['Long'])]
rsl.dropna(axis=0, subset=['Lat','Long'], inplace=True)

# return the PSMSL ID of the closest gauge
blocks['closest'] = [closest_point(x, list(rsl['point'])) for x in blocks['point']]
blocks['psmsl'] = [match_value(rsl, 'point', x, 'PSMSL ID') for x in blocks['closest']]

blocks.reset_index('geoid', inplace=True)
blocks = blocks[['geoid','psmsl']]
blocks.to_sql('block_psmsl', db['engine'], if_exists='replace')

years = ['RSL2020 (cm)', 'RSL2030 (cm)', 'RSL2040 (cm)', 'RSL2050 (cm)', 'RSL2060 (cm)', 'RSL2070 (cm)', 'RSL2080 (cm)', 'RSL2090 (cm)', 'RSL2100 (cm)', 'RSL2110 (cm)', 'RSL2120 (cm)', 'RSL2130 (cm)', 'RSL2140 (cm)', 'RSL2150 (cm)']
scenarios = ['1.0 - LOW', '1.0 - MED', '1.0 - HIGH', '2.0 - LOW', '2.0 - MED', '2.0 - HIGH']
val = 2020
new_years = []
for year in years:
    new_column = f'RSL{val}'
    new_years.append(new_column)
    val += 10
    rsl[new_column] = round(rsl[year]*0.0328084)

rsl['psmsl'] = rsl['PSMSL ID']
rsl['scenario'] = rsl['Scenario']
rsl = rsl[['psmsl','scenario'] + new_years]

rsl.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/data/psmsl_ft.csv')
