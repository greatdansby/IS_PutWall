from putwall.putwall import PutWall, PutSlot
from cartons.cartons import Carton
from skus.skus import SKU
from orders.orders import Order, Line
from logic.putwalloptimization import assign_store, assign_carton, get_top_stores
import sqlalchemy as sa
import pandas as pd
import numpy as np
import time

# create engine for quering using sqlalchemy
engine = sa.create_engine('mssql+pyodbc://sa:FT123!@#@lab-sqlserver3.invata.com\SQL2014/Burlington?driver=SQL+Server+Native+Client+11.0')
receiving_table = 'tblReceiving'
ItemMaster_table = 'dbo.tblItemMaster'


def print_timer(start, label=''):
    buffer = '-'*max(30-len(label), 0)
    print('{}{} Elapsed Time {:.4} seconds'.format(label, buffer, time.time()-start))
    return time.time()


def run_model(num_putwalls=65, num_slot_per_wall=6, inventory_file=None, order_table='dbo.Burlington0501to0511',
              date='5/11/2017'):
    start = time.time()

    ## Initialize orders
    orders = {}
    sql_query = '''select ShipTo as store,
                    sku,
                    UnitQty_Future as units,
                    0 as fulfilled
                    From {}
                    Where ShipDate = '{}'
                   Order by ShipTo, SKU'''.format(order_table, date)
    order_data = pd.read_sql_query(sql_query, engine)
    order_data = (order_data.groupby(['store', 'sku'])['units'].sum()).reset_index()
    for line in order_data.to_dict('records'):
        if line['store'] not in orders:
            print('Added store order: {}'.format(line['store']))
            orders[line['store']] = Order(id=line['store'])
        orders[line['store']].add_line(Line(sku=line['sku'], quantity=line['units']))
    print('{} Open Lines Initialized'.format(sum([o.line_status() for o in orders.values()])))

    start = print_timer(start, 'Initialized orders')

    ## Initialize all put walls

    put_walls = {}
    top_stores = get_top_stores(orders, sort='Lines')
    for pw in range(num_putwalls):
        put_walls[pw] = PutWall(id=pw, num_slots=num_slot_per_wall)
        for ps in range(num_slot_per_wall):
            if top_stores:
                order_id = top_stores.pop(0)
                orders[order_id].allocated = True
                put_walls[pw].add_slot(PutSlot(id=ps,
                                               capacity=np.random.randint(25, 35),
                                               order=order_id,
                                               active=True,
                                               alloc_lines=orders[order_id].lines))
            else:
                put_walls[pw].add_slot(PutSlot(id=ps, capacity=np.random.randint(25, 35)))

    start = print_timer(start, 'Initialized put-walls')

    ## Initialize item_master
    item_master = {}

    sql_query = '''select sku,
    		Sum(UnitQty_Future) AS Units,
    		count(sku) AS Lines,
    		ISNULL(COALESCE(OB.MaxUnitsPerCase, OA.MaxUnitsPerCase),21) AS unitspercase
    From {} OO
    OUTER APPLY (
    select AVG(RO.UOMQty) AVGUnitsPerCase,
    		MAX(RO.UOMQty) MaxUnitsPerCase
    from tblReceiving_Old RO
    where OO.SKU = RO.SKU) OA
    OUTER APPLY (
    select AVG(R.UOMQty) AVGUnitsPerCase,
    		MAX(R.UOMQty) MaxUnitsPerCase
    from tblReceiving R
    where OO.SKU = R.SKU) OB
    Where ShipDate = '{}'
    group by sku,
    		OA.MaxUnitsPerCase,
    		OB.MaxUnitsPerCase
    order by lines desc'''.format(order_table, date)
    item_master_data = pd.read_sql_query(sql_query, engine)
    item_master_data.set_index('sku', inplace=True)
    item_master = {i: SKU(id=i) for i in item_master_data.index}
    sku_list = list(item_master.keys())
    np.random.seed(32)
    active_skus = np.random.choice(sku_list, size=int(len(sku_list)*.6))
    for sku in active_skus:
        item_master[sku].active = False

    start = print_timer(start, 'Initialized item master')

    ## Intialize inventory
    cartons = {}
    inventory_table = pd.DataFrame(index=None,
                             columns=['tote_id', 'sku_id', 'tote_qty', 'inv_qty', 'active', 'loop_activated'],
                             dtype=int)
    units = item_master_data
    ttl_units = np.sum(units.loc[:, 'Units'])
    carton_id = 0

    for sku in sku_list:
        while units.loc[sku, 'Units'] > 0:
            units_per_case = units.loc[sku, 'unitspercase']
            cartons[carton_id] = Carton(carton_id, active=item_master[sku].active, sku=sku, quantity=units_per_case)
            units.loc[sku, 'Units'] -= min(units_per_case, units.loc[sku, 'Units'])
            carton_id += 1
    print('{} Cartons created.'.format(len(cartons)))

    start = print_timer(start, 'Initialized Totes')

    count_carton_pulls = 0

    for i in range(100):
        print('Loop: {}\nCarton Pulls: {}'.format(i, count_carton_pulls))

        for pw in put_walls.values():
            pw.fill_from_queue(1)

            empty_slots = []
            for slot in pw.slots.values():
                if slot.is_clear():
                    print('Carton for {} shipped from Put-Wall {}'.format(slot.order, pw.id))
                    if slot.order in orders:
                        orders[slot.order].lines = slot.alloc_lines
                        if sum([l.quantity for l in orders[slot.order].lines]) == 0:
                            del orders[slot.order]
                            print('Order closed: {}'.format(slot.order))
                        orders[slot.order].allocated = False
                    slot.clear()
                    empty_slots.append(slot)

            for slot in empty_slots:
                store, lines = assign_store(pw=pw,
                                            orders=orders,
                                            cartons=cartons)
                if store is None:
                    break
                slot.assign(order=store, alloc_lines=lines)
                orders[store].allocated = True
                print('Store {} assigned to Put-Wall {}'.format(store, pw.id))

            carton = assign_carton(pw=pw, orders=orders, cartons=cartons)
            if carton:
                pw.add_to_queue(carton)
                carton.allocated = True
                start = print_timer(start, 'Tote assignment')
                print('Carton added to queue for Put-Wall {}'.format(pw.id))
                count_carton_pulls += 1

            # Release more SKUs
            active_units = sum([c.quantity for c in cartons.values() if c.active == True])
            if active_units < 50000:
                inactive_skus = [k for k, v in item_master.items() if v.active == False]
                activate_skus = np.random.choice(inactive_skus, size=100)
                for sku in active_skus:
                    item_master[sku].active = True
                for carton in cartons:
                    carton.active = item_master[carton.sku].active

    count_carton_returns = count_tote_pulls = len([t for t in cartons.values() if t.active == False])
    print('Carton Pulls: {}'.format(count_carton_pulls))
    print('Carton Returns: {}'.format(count_carton_returns))
    print('Carton Tote Moves: {}'.format(count_carton_pulls+count_carton_returns))

if __name__ == '__main__':
    run_model()