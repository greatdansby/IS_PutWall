import numpy as np

def assign_store(**kwargs):
    random_order = np.random.choice(list(kwargs['orders'].keys()))
    return random_order, kwargs['orders'][random_order].lines

def add_tote_to_queue(**kwargs):

    return np.random.choice(list(kwargs['totes'].values()))