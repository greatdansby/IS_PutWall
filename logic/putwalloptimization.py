import numpy as np
import pandas as pd
import time

def print_timer(start, label=''):
    buffer = '-'*max(30-len(label), 0)
    print('{}{} Elapsed Time {:.4} seconds'.format(label, buffer, time.time()-start))
    return time.time()

def assign_store(**kwargs):
    '''

    Optimization function for returning the best candidate store
    to be slotted into the active putwall (kwargs['pw'])

    :param kwargs['pw']: The active put-wall object being slotted to
    :param kwargs['orders']: A dictionary of all orders
    :param kwargs['totes']: A dictionary of all inventory totes
    :param kwargs['item_master']: A dictionary of all SKUs
    :param kwargs['put_walls']: A dictionary of all put-walls
    :return: Store Recommendation, Lines to be Allocated to Slot
    '''

    # Get a list of stores with the best affinity to the current put-wall
    top_stores = get_store_affinity(kwargs['pw'], kwargs['orders'], kwargs['order_data'])

    if top_stores:
        return top_stores[0], kwargs['orders'][top_stores[0]].lines

    return None, None


def assign_carton(**kwargs):
    '''

    Return the tote with the lowest demand, able to satisfy the most volume within the current put-wall.

    :param kwargs:
    :return:
    '''

    pw_demand = kwargs['pw'].get_allocation()
    cartons_not_in_queue = [carton for carton in kwargs['cartons'].values()
                          if (carton not in kwargs['pw'].queue and
                              carton.active and
                              carton.sku in pw_demand and
                              carton.allocated == False)]

    for carton in cartons_not_in_queue:
        if pw_demand[carton.sku]/carton.quantity >= 1:
            return carton
    if cartons_not_in_queue:
        return sorted([(carton, pw_demand[carton.sku]) for carton in cartons_not_in_queue], key=lambda k: -k[1])[0][0]
    return None


def get_top_stores(orders, sort='Lines', num=999999):
    '''

    Return a list of stores based on their number of Lines or Units

    :param orders: Dictionary of all orders
    :param sort: Type of sort "Lines" or "Units"
    :param num: Number of stores to return
    :return: List of stores
    '''

    if sort == 'Lines':
        return [order.id for order in sorted(orders.values(), key=lambda k: -len(k.lines))
                if order.allocated == False][:num]

    if sort == 'Units':
        return [order.id for order in sorted(orders.values(), key=lambda k: -sum([l.quantity for l in k.lines]))
                if order.allocated == False][:num]

    return None


def get_store_affinity(pw, orders, order_data):
    '''

    Find all stores active in the current put-wall
    Find all stores available to be allocated
    Find all SKUs allocated to current put-wall
    Return a list of stores sorted by those that have the
    highest unit qty for SKUs already allocated to the put-wall

    :param pw: Current PutWall object
    :param orders: Dictionary of all orders
    :return: List of stores in order of recommendation
    '''

    stores_in_pw = [s.order for s in pw.slots.values()]
    stores_avail_for_alloc = [order.id for order in orders.values() if order.allocated == False]
    skus_alloc_to_pw = list(set([l.sku for o in orders.values() for l in o.lines
                            if o.id in stores_in_pw and l.quantity > 0]))
    mask = order_data[['store', 'sku']].isin({'store': stores_avail_for_alloc,
                                             'sku': skus_alloc_to_pw}).all(axis=1)
    order_array = order_data[mask]
    order_demand = order_array.groupby('store').sum()
    top_stores = list(order_demand.sort_values('units', ascending=False).index)

    return top_stores