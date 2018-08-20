import numpy as np


def assign_store(**kwargs):

    top_stores = get_store_affinity(kwargs['pw'], kwargs['orders'])

    if top_stores:
        return top_stores[0]['store'], kwargs['orders'][top_stores[0]['store']].lines

    return None, None


def add_tote_to_queue(**kwargs):
    totes_not_in_queue = {k: v for k, v in kwargs['totes'].items() if v not in kwargs['pw'].queue and v.active}
    pw_demand = kwargs['pw'].get_allocation()
    order_demand = sorted([{'sku': l.sku, 'qty': sum([l.quantity for o in kwargs['orders'].values()
                                                      for l in o.lines if l.sku in pw_demand])}
                           for order in kwargs['orders'].values()], key=lambda k: k['qty'])
    for order in order_demand:
        if order['sku'] in totes_not_in_queue:
            return totes_not_in_queue[order['sku']]
    return np.random.choice(list(totes_not_in_queue))


def get_top_stores(orders, sort='Lines', num=999999):

    if sort == 'Lines':
        return [order.id for order in sorted(orders.values(), key=lambda k: -len(k.lines))
                if order.allocated == False][:num]

    if sort == 'units':
        return [order.id for order in sorted(orders.values(), key=lambda k: -sum([l.quantity for l in k.lines]))
                if order.allocated == False][:num]

    return None


def get_store_affinity(pw, orders):
    stores_in_pw = [s.order for s in pw.slots.values()]
    stores_avail_for_alloc = [order.id for order in orders.values() if order.allocated == False]
    skus_alloc_to_pw = set([l.sku for o in orders.values() for l in o.lines if o.id in stores_in_pw])
    top_stores = sorted([{'store': order.id, 'qty': sum([l.quantity for o in orders.values() for l in o.lines
                                                      if l.sku in skus_alloc_to_pw])}
                             for order in orders if order in stores_avail_for_alloc], key=lambda k: -k['qty'])
    return top_stores
