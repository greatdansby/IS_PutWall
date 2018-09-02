import pandas as pd
from utilities import print_timer
import time


def load_from_db(debug, engine, sql, data_name, load_from_db=True, data_filename='data.h5', index=None):
    start = time.time()
    data_store = pd.HDFStore(data_filename)
    if load_from_db:
        data = pd.read_sql_query(sql, engine)
        if index:
            data = data.groupby(index).sum()
        data_store[data_name] = data
        data_store.close()
    else:
        data_store = pd.HDFStore(data_filename)
        data = data_store[data_name]
    print_timer(debug, start, 'load_from_db {}'.format(data_name))
    return data


def split_inv_to_tote(inventory_df, sku_list):
    tote_count = 0
    totes_df = inventory_df.loc[sku_list].copy().reset_index()
    totes_df['unit_count'] = totes_df.units - totes_df.unitspercase
    while True:
        new_totes = totes_df[(totes_df['unit_count'] > 0) & (totes_df.index >= tote_count)]
        if len(new_totes) == 0:
            break
        tote_count = len(totes_df)
        totes_df = totes_df.append(new_totes, ignore_index=True)
        totes_df['unit_count'] = totes_df.unit_count - totes_df.unitspercase
    totes_df['units'] = totes_df.unitspercase
    totes_df['active'] = True
    totes_df['allocated'] = False
    totes_df['alloc_qty'] = 0
    return totes_df.loc[:, totes_df.columns.isin(['units', 'sku', 'active', 'allocated', 'alloc_qty'])]