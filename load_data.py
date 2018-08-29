import pandas as pd
from utilities import print_timer
import time

def load_from_db(debug, engine, sql, data_name, load_from_db=True, data_filename='data.h5', index=None):
    start = time.time()
    data_store = pd.HDFStore(data_filename)
    if load_from_db:
        data = pd.read_sql_query(sql, engine)
        if index:
            data.set_index(index)
        data_store[data_name] = data
        data_store.close()
    else:
        data_store = pd.HDFStore(data_filename)
        data = data_store[data_name]
    print_timer(debug, start, 'load_from_db {}'.format(data_name))
    return data
