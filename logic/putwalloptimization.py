import numpy as np
import pandas as pd
import time
from totes.totes import Tote

def print_timer(debug, start, label=''):
    buffer = '-'*max(30-len(label), 0)
    if debug:
        print('{}{} Elapsed Time {:.4} seconds'.format(label, buffer, time.time()-start))
    return time.time()

def assign_stores(debug, pw, orders_df, totes_df, stores_to_fill=1):
    '''

    Optimization function for returning the best candidate store
    to be slotted into the active putwall.
    
    '''
    start = time.time()
# Break if there is nothing to fill
    if len(pw.empty_slots()) == 0: return False

# Get a list of stores with the best affinity to the current put-wall
    tote_ids = [t.id for t in pw.queue]
    totes_in_queue = totes_df.iloc[tote_ids]
    order_ids = [s.order for s in pw.slots.values()]
    orders_not_in_pw = orders_df.loc[~orders_df.index.get_level_values(0).isin(order_ids)].reset_index(level=0)
    combined_df = totes_in_queue.join(orders_not_in_pw, on='sku', rsuffix='_order')
    combined_df['tote_close'] = 1*((combined_df['units_order']-combined_df['alloc_qty_order']) == (combined_df['units']-combined_df['alloc_qty']))
    combined_df['fulfillment'] = (combined_df['units_order'] - combined_df['alloc_qty_order'])
    combined_df = combined_df.groupby('store').sum()
    combined_df['score'] = combined_df['fulfillment'] + combined_df['tote_close']*10
    combined_df = combined_df.sort_values(by=['score'], ascending=False)

    for n, slot in enumerate(pw.empty_slots()[:stores_to_fill]):
        if len(combined_df) > n:
            slot.order = combined_df.index[n]
            slot.alloc_lines = {}
            slot.active = True
            slot.capacity = np.random.randint(25, 35)
            allocate_order_to_pw(totes_df, pw, orders_df, slot.order)
        else:
            top_stores = get_store_affinity(debug=debug, pw=pw, orders_df=orders_df)
            if len(top_stores) >= n:
                slot.order = top_stores[n]
                slot.alloc_lines = {}
                slot.active = True
                slot.capacity = np.random.randint(25, 35)
                allocate_order_to_pw(totes_df, pw, orders_df, slot.order)
            else:
                order_list = orders_df.loc[orders_df['alloc_qty'] == 0].groupby('store').sum()
                if len(order_list) >= n:
                    slot.order = order_list.sort_values(by='units').index[n]
                    slot.alloc_lines = {}
                    slot.active = True
                    slot.capacity = np.random.randint(25, 35)
                    allocate_order_to_pw(totes_df, pw, orders_df, slot.order)
    start = print_timer(debug, start, 'assign_stores')


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
    totes_needed = min(num_to_assign, pw.queue_length-len(pw.queue))
    for i in range(totes_needed):

        stores_in_pw = {s.order: s.capacity-s.alloc_qty-s.quantity for s in pw.slots.values()
                        if s.order is not None if s.capacity-s.alloc_qty-s.quantity > 0}

        if not any(stores_in_pw):
            # sku_demand = orders_df.groupby('sku').sum()
            # sku_demand = sku_demand.merge(totes_df.loc[(totes_df['active'] == True) &
            #                                           (totes_df['allocated'] == False)].reset_index(), on='sku', how='inner')
            # sku_demand['score'] = 1 - abs(sku_demand['units_x'] - sku_demand['units_y']) / sku_demand.units_y
            # sku_demand = sku_demand.sort_values(by='score', ascending=False)
            # idx = sku_demand['index'][0]
            idx = np.random.choice(totes_df.loc[(totes_df['active']==True) & (totes_df['allocated']==False)].index, 1)[0]
            tote = Tote(id=idx, totes_df=totes_df, alloc_qty=0, allocated=True)
            pw.add_to_queue(tote)
            allocate_tote_to_pw(totes_df, pw, orders_df, tote)
            totes_assigned.append(idx)
            continue

        sku_demand = orders_df.loc[orders_df.index.get_level_values(0).isin(stores_in_pw)].copy()
        #start = print_timer(debug, start, 'Copy orders')
        sku_demand['remaining_capacity'] = [stores_in_pw[s] for s in sku_demand.index.get_level_values(0)]
        sku_demand['store_close'] = 1*((sku_demand['units'] - sku_demand['alloc_qty']) >= sku_demand['remaining_capacity'])
        sku_demand = sku_demand.groupby('sku').agg({'units': ['count', 'sum'], 'store_close':['sum'], 'alloc_qty':['sum']})
        #start = print_timer(debug, start, 'Sum demand')
        totes_not_in_queue = totes_df.loc[(totes_df['active'] == True) &
                                          (totes_df['allocated'] == False)]
        combined_df = totes_not_in_queue.reset_index().merge(sku_demand, on='sku').dropna(thresh=5, axis=0)
        #start = print_timer(debug, start, 'Carton join')

        pw_open_demand = (combined_df[('units','sum')] - combined_df[('alloc_qty','sum')])
        combined_df['score'] = pw_open_demand/combined_df['units']*combined_df[('units','count')] + 1*(combined_df[('store_close','sum')])
        combined_df = combined_df.sort_values(by=['score', ('units', 'sum')], ascending=False)
        if not combined_df.empty and combined_df['units'].max() > 0:
            idx = combined_df.at[combined_df.first_valid_index(), 'index']
            tote = Tote(id=idx, totes_df=totes_df, allocated=True)
            pw.add_to_queue(tote)
            allocate_tote_to_pw(totes_df, pw, orders_df, tote)
            totes_assigned.append(idx)
            if tote.id == 1998 and pw.id == 32:
                print('Debug')
        #start = print_timer(debug, start, 'Score SKUs')

    if not totes_assigned and totes_needed > 0:
        print('No cartons found for  assignment')
        return totes_assigned, True
    start = print_timer(debug, start, 'assign_totes')
    return totes_assigned, False


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

def pass_to_pw(debug, tote, put_walls, orders_df, pw_id, totes_df):
    pw_list = sorted([(k, min(abs(pw_id-k), len(put_walls.keys())-abs(pw_id-k-1)))
                      for k in put_walls.keys() if k != pw_id], key=lambda k: k[1])
    for pw, _ in pw_list[:18]:
        alloc = put_walls[pw].get_allocation()
        if tote.sku in alloc.index:
            if (alloc.at[tote.sku, 'units']-alloc.at[tote.sku, 'alloc_qty'])/(tote.quantity - tote.alloc_qty) > .25:
                if pw == 32 and tote.id == 1998:
                    print('debug')
                if allocate_tote_to_pw(totes_df, put_walls[pw], orders_df, tote):
                    tote.source = 'pass'
                    put_walls[pw].add_to_queue(tote)
                    return True
    del tote
    return False

def allocate_tote_to_pw(totes_df, pw, orders_df, tote):
    order_ids = pw.get_orders()
    for order_id in order_ids:
        if tote.sku == 'LB16459564' and order_id == 'ST0880':
            print('debug')
        if tote.quantity - tote.alloc_qty == 0:
            break
        if (order_id, tote.sku) in orders_df.index:
            order_alloc = orders_df.at[(order_id, tote.sku), 'alloc_qty']
            order_units = orders_df.at[(order_id, tote.sku), 'units']
            if order_units - order_alloc == 0:
                continue
            slot_capacity = pw.slots[order_ids[order_id]].capacity - pw.slots[order_ids[order_id]].quantity - pw.slots[order_ids[order_id]].alloc_qty
            alloc = max(0, min(tote.quantity - tote.alloc_qty,
                        order_units - order_alloc,
                        slot_capacity))
            # if (slot_capacity > 0 and
            #         (order_units - order_alloc) - slot_capacity <= 3 and
            #         (tote.quantity - tote.alloc_qty) >= (order_units - order_alloc)):
            #     alloc = (order_units - order_alloc)
            #if alloc == slot_capacity: alloc -= int(np.random.randint(100)/100)
            tote.alloc_qty += alloc
            pw.slots[order_ids[order_id]].alloc_qty += alloc
            pw.slots[order_ids[order_id]].allocate(tote.sku, alloc)
            order_alloc += alloc
            orders_df.at[(order_id, tote.sku), 'alloc_qty'] += alloc
    if tote.alloc_qty <= 0:
        return False
    totes_df.at[tote.id, 'alloc_qty'] = tote.alloc_qty
    totes_df.at[tote.id, 'allocated'] = True
    return True

def allocate_order_to_pw(totes_df, pw, orders_df, order_id):
    order_ids = pw.get_orders()
    for tote in pw.queue:
        if tote.quantity - tote.alloc_qty == 0:
            continue
        if (order_id, tote.sku) in orders_df.index:
            order_alloc = orders_df.at[(order_id, tote.sku), 'alloc_qty']
            order_units = orders_df.at[(order_id, tote.sku), 'units']
            alloc = min(tote.quantity - tote.alloc_qty,
                        order_units - order_alloc,
                        pw.slots[order_ids[order_id]].capacity - pw.slots[order_ids[order_id]].quantity - pw.slots[order_ids[order_id]].alloc_qty
                        )
            tote.alloc_qty += alloc
            pw.slots[order_ids[order_id]].alloc_qty += alloc
            pw.slots[order_ids[order_id]].allocate(tote.sku, alloc)
            order_alloc += alloc
            orders_df.at[(order_id, tote.sku), 'alloc_qty'] += alloc
        totes_df.at[tote.id, 'alloc_qty'] = tote.alloc_qty