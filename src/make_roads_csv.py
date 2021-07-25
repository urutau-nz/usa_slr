'''
Mitchell Anderson
16/06/21

Takes a selection of from and to osmids, formats, and saves to csv
'''
import pandas as pd


def main(df_osmids, config):
    # reverse way
    df_inv = pd.DataFrame()
    df_inv['from_osmid'] = df_osmids['to_osmid']
    df_inv['to_osmid'] = df_osmids['from_osmid']

    df_osmids = df_osmids.append(df_inv)
    df_osmids = df_osmids.astype(int)

    # set edge speeds
    df_osmids['edge_speed'] = 0

    df_osmids.to_csv(r'{}/updates/update.csv'.format(config['OSM']['data_directory']), header=False, index=False)