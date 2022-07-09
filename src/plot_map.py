import pandas as pd
import geopandas as gpd

iso = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/isolation_county.csv', dtype={'geoid_county':str})
inu = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/inundation_county.csv', dtype={'geoid_county':str})
inu = inu[inu.rise==6]
iso = iso[iso.rise==6]

df = iso.merge(inu, on='geoid_county', how='left', suffixes = ('_iso','_inu'))

# add county population



df['U7B001_inu'][df.U7B001_inu.isnull()]=0
df['difference'] = df['U7B001_iso'] - df['U7B001_inu']
df['dif_percent'] = df['difference'] / df['U7B001_inu']
df['ratio'] = df['U7B001_iso'] / df['U7B001_inu']


df.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/county_difference.csv')

gdf = gpd.read_file('/home/tml/CivilSystems/data/usa/2019/nhgis0099_shapefile_tl2019_us_county_2019.zip')

gdf = gdf.merge(df[['geoid_county','difference','dif_percent']], left_on='GEOID',right_on='geoid_county',how='right')

gdf.to_file('/home/tml/CivilSystems/projects/access_usa_slr/results/county_dif.shp')

###
# for the dashboard
iso = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/isolation_county.csv', dtype={'geoid_county':str})
inu = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/inundation_county.csv', dtype={'geoid_county':str})
df = iso.merge(inu, on=['geoid_county','rise'], how='left', suffixes = ('_iso','_inu'))
df = df.fillna(value=0)
df['ratio'] = df['U7B001_iso'] / df['U7B001_inu']
df.rename(columns={'U7B001_iso':'isolated','U7B001_inu':'inundated','U7B001_percentage_iso':'isolated_percentage','U7B001_percentage_inu':'inundated_percentage'})
df.reset_index(inplace=True)
df = df[['geoid_county','rise','ratio', 'isolated','inundated','isolated_percentage','inundated_percentage','state_code','state_name']]
df.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/dashboard_county.csv')

iso = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/isolation_tract.csv', dtype={'geoid_tract':str})
inu = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/inundation_tract.csv', dtype={'geoid_tract':str})
df = iso.merge(inu, on=['geoid_tract','rise'], how='left', suffixes = ('_iso','_inu'))
df = df.fillna(value=0)
df.rename(columns={'U7B001_iso':'isolated','U7B001_inu':'inundated','U7B001_percentage_iso':'isolated_percentage','U7B001_percentage_inu':'inundated_percentage'})
df.reset_index(inplace=True)
df = df[['geoid_tract','rise', 'isolated','inundated','isolated_percentage','inundated_percentage','state_code','state_name']]
df.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/dashboard_tract.csv')

iso = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/isolation_state.csv', dtype={'state_code':str})
inu = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/inundation_state.csv', dtype={'state_code':str})
df = iso.merge(inu, on=['state_code','rise'], how='left', suffixes = ('_iso','_inu'))
df = df.fillna(value=0)
df.rename(columns={'U7B001_iso':'isolated','U7B001_inu':'inundated','U7B001_percentage_iso':'isolated_percentage','U7B001_percentage_inu':'inundated_percentage'})
df.reset_index(inplace=True)
df = df[['rise', 'isolated','inundated','isolated_percentage','inundated_percentage','state_code','state_name']]
df.to_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/dashboard_state.csv')