import numpy as np
import pandas as pd
import time
from totes.totes import Tote

def print_timer(debug, start, label=''):
    buffer = '-'*max(30-len(label), 0)
    if debug:
        print('{}{} Elapsed Time {:.4} seconds'.format(label, buffer, time.time()-start))
    return time.time()

def assign_stores(debug, pw, orders_df):
    '''

    Optimization function for returning the best candidate store
    to be slotted into the active putwall.
    
    '''

    # Get a list of stores with the best affinity to the current put-wall
    top_stores = get_store_affinity(debug=debug, pw=pw, orders_df=orders_df)

    if len(top_stores) > 0:
        for n, slot in enumerate(pw.empty_slots()):
            slot.order = top_stores[n]
            slot.active = True
            slot.capacity = np.random.randint(25, 35)
    else:
        order_list = orders_df.groupby('store').sum()
        for n, slot in enumerate(pw.empty_slots()):
            slot.order = top_stores[n]


def pick_clean(alloc, row):
    return alloc[row['sku']]/alloc['quantity'] >= 1


def assign_totes(debug, totes_df, pw, num_to_assign, orders_df):
    '''

    Return the tote with the lowest demand, able to satisfy the most volume within the current put-wall.

    :param kwargs:
    :return:
    '''
    start = time.time()
    totes_assigned = []

    for i in range(min(num_to_assign, pw.queue_length-len(pw.queue))):
        stores_in_pw = [s.order for s in pw.slots.values()]
        start = print_timer(debug, start, 'Store in PW')

        skus_alloc_to_pw = pw.get_allocation()
        start = print_timer(debug, start, 'Get Allocation')

        if skus_alloc_to_pw.empty:
            idx = totes_df.loc[(totes_df['active'] == True) & (totes_df['allocated'] == False)].first_valid_index()
            pw.add_to_queue(Tote(id=idx, totes_df=totes_df, allocated=True))
            totes_assigned.append(idx)
            continue

        totes_not_in_queue = totes_df.loc[(totes_df['active'] == True) &
                                          (totes_df['allocated'] == False) &
                                          (totes_df['sku'].isin(skus_alloc_to_pw.index))]
        start = print_timer(debug, start, 'Get cartons')

        avail_cartons = totes_not_in_queue.loc[totes_not_in_queue['units'].isin(skus_alloc_to_pw.units)]
        start = print_timer(debug, start, 'Avail cartons')

        if not avail_cartons.empty:
            idx = avail_cartons.first_valid_index()
            start = print_timer(debug, start, 'Get index')
            pw.add_to_queue(Tote(id=idx, totes_df=totes_df, allocated=True))
            totes_assigned.append(idx)
        elif not totes_not_in_queue.empty:
            idx = totes_not_in_queue.sort_values(by='quantity', ascending=False).first_valid_index()
            start = print_timer(debug, start, 'Get index no clean picks')
            pw.add_to_queue(Tote(id=idx, totes_df=totes_df, allocated=True))
            totes_assigned.append(idx)
    if not totes_assigned:
        print('No cartons found for  assignment')
    return totes_assigned


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


def get_store_affinity(debug, pw, orders_df):
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
    start = time.time()

    stores_in_pw = [s.order for s in pw.slots.values()]
    start = print_timer(debug, start, 'Store in PW')

    stores_avail_for_alloc = ~orders_df.index.get_level_values('store').isin(stores_in_pw)
    start = print_timer(debug, start, 'Store avail')

    skus_alloc_to_pw = orders_df.loc[stores_in_pw].groupby('sku').sum()
    start = print_timer(debug, start, 'SKU alloc')

    skus = orders_df.index.get_level_values('sku').isin(skus_alloc_to_pw)
    start = print_timer(debug, start, 'Mask')

    order_array = orders_df[skus & stores_avail_for_alloc]
    start = print_timer(debug, start, 'Masking')

    order_demand = order_array.groupby('store').sum()
    start = print_timer(debug, start, 'Order demand')

    top_stores = list(order_demand.sort_values('units', ascending=False).index)
    start = print_timer(debug, start, 'Store sort')

    if not top_stores: return orders_df[stores_avail_for_alloc].index.get_level_values(0).unique()
    return top_stores

def pass_to_pw(debug, tote, put_walls, orders_df, pw_id):
    pw_list = sorted([(k, min(abs(pw_id-k), len(put_walls.keys())-abs(pw_id-k-1)))
                      for k in put_walls.keys() if k != pw_id], key=lambda k: k[1])
    for pw, _ in pw_list[:18]:
        alloc = put_walls[pw].get_allocation()
        if tote.sku in alloc:
            if alloc[tote.sku] >= tote.quantity:
                put_walls[pw].add_to_queue(tote)
                return True
    return False