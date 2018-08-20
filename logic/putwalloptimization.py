import numpy as np
import pandas as pd

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
    top_stores = get_store_affinity(kwargs['pw'], kwargs['orders'])

    if top_stores:
        return top_stores[0]['store'], kwargs['orders'][top_stores[0]['store']].lines

    return None, None


def assign_carton(**kwargs):
    '''

    Return the tote with the lowest demand, able to satisfy the most volume within the current put-wall.

    :param kwargs:
    :return:
    '''
    pw_demand = kwargs['pw'].get_allocation()

    totes_not_in_queue = [carton for carton in kwargs['cartons'].values()
                          if (carton not in kwargs['pw'].queue and
                              carton.active and
                              carton.sku in pw_demand)]

    order_array = pd.DataFrame([[l.sku, l.quantity] for o in kwargs['orders'].values() for l in o.lines
                              if l.sku in pw_demand])
    sku_demand = order_array.groupby(0).sum()
    sku_demand = {sku: qty for sku, qty in zip(order_array.index, sku_demand)}

    ordered_cartons = sorted(totes_not_in_queue, key=lambda k: sku_demand[k.sku])

    for carton in ordered_cartons:
        if pw_demand[carton.sku]/carton.quantity >= 1:
            return carton
    if totes_not_in_queue:
        return np.random.choice(totes_not_in_queue)

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


def get_store_affinity(pw, orders):
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
    skus_alloc_to_pw = set([l.sku for o in orders.values() for l in o.lines if o.id in stores_in_pw])
    ##TODO Fix with matrix
    top_stores = sorted([{'store': order.id, 'qty': sum([l.quantity for o in orders.values() for l in o.lines
                                                      if l.sku in skus_alloc_to_pw])}
                             for order in orders if order in stores_avail_for_alloc], key=lambda k: -k['qty'])
    return top_stores
