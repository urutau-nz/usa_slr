import pandas as pd
import geopandas as gpd

iso = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/isolation_county.csv', dtype={'geoid_county':str})
inu = pd.read_csv('/home/tml/CivilSystems/projects/access_usa_slr/results/exposure_county.csv', dtype={'geoid_county':str})
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