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
#engine = sa.create_engine('mssql+pyodbc://sa:FT123!@#@lab-sqlserver3.invata.com\SQL2014/Burlington?driver=SQL+Server+Native+Client+11.0')
receiving_table = 'tblReceiving'
ItemMaster_table = 'dbo.tblItemMaster'


def print_timer(debug, start, label=''):
    buffer = '-'*max(30-len(label), 0)
    if debug:
        print('{}{} Elapsed Time {:.4} seconds'.format(label, buffer, time.time()-start))
        return time.time()
    return start


def run_model(num_putwalls=65, num_slot_per_wall=6, inventory_file=None, order_table='dbo.Burlington0501to0511',
              date='5/11/2017'):
    debug = True
    start = time.time()
    df_dict = {'units': {}}

    ## Initialize orders
    sql_query = '''select ShipTo as id,
                    sku,
                    UnitQty_Future as units
                    From {}
                    Where ShipDate = '{}'
                   Order by ShipTo, SKU'''.format(order_table, date)
    #order_data = pd.read_sql_query(sql_query, engine)
    order_data = pd.DataFrame().from_csv('order_data.csv')
    order_data = (order_data.groupby(['id', 'sku'])['units'].sum()).reset_index()
    order_data = order_data.set_index(['id', 'sku'])
    stores = pd.unique(order_data.index.get_level_values('id'))
    skus = pd.unique(order_data.index.get_level_values('sku'))

    start = print_timer(debug, start, 'Initialized stores')

    ## Initialize all put walls
    put_walls = {'PW{}'.format(pw): ['PS{}'.format(ps) for ps in range(num_slot_per_wall)]
                 for pw in range(num_putwalls)}
    df_dict['units'].update({('{}-{}'.format(pw, ps), sku): 0 for pw in put_walls
                             for ps in put_walls[pw]
                             for sku in skus})

    start = print_timer(debug, start, 'Initialized put-walls')

    ## Initialize item_master

    sql_query = '''select sku,
    		Sum(UnitQty_Future) AS Units,
    		count(sku) AS Lines,
    		COALESCE(OB.MaxUnitsPerCase, OA.MaxUnitsPerCase, 21) AS unitspercase
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
    #item_master_data = pd.read_sql_query(sql_query, engine)
    item_master_data = pd.DataFrame().from_csv('item_master.csv')
    item_master_data.set_index('sku', inplace=True)
    item_master_data['cases'] = (np.ceil(item_master_data.Units / item_master_data.unitspercase)).astype(int)
    start = print_timer(debug, start, 'Initialized item master')

    ## Intialize inventory
    df_dict['units'].update({('{}-{}'.format(sku, i), sku): row.unitspercase
               for sku, row in item_master_data.iterrows()
               for i in range(row.cases)})
    start = print_timer(debug, start, 'Create cartons')

    df = pd.DataFrame(df_dict)
    start = print_timer(debug, start, 'Load DataFrame')

    count_carton_pulls = 0
    units_shipped = 0
    loop = 0
    while len(stores) > 0:
        print('Loop: {}\nCarton Pulls: {}'.format(loop, count_carton_pulls))
        start = print_timer(True, start, 'Loop Start')
        order_totals = df.loc[stores].groupby(level=[0]).sum()
        start = print_timer(True, start, 'Order totals')
        print('Open Orders: {}'.format(len(df.loc[stores].sum() < 0)))
        print('Open Lines: {}'.format(len([l for o in orders.values() for l in o.lines if l.quantity > 0])))
        print('Active Units: {}'.format(sum([c.quantity for c in cartons.values() if c.active == True])))
        start = print_timer(True, start, 'Loop Start')
        loop += 1
        for pw in put_walls.values():
            log = pw.fill_from_queue(1)
            if debug: print(log)
            start = print_timer(debug, start, 'Fill from Q')
            empty_slots = []
            for slot in [s for s in pw.slots.values() if s.is_clear()]:
                if debug: print('Carton for {} shipped from Put-Wall {}'.format(slot.order, pw.id))
                orders[slot.order].lines = slot.alloc_lines
                order_data.update(pd.DataFrame([{'store': slot.order, 'sku': l.sku, 'units': l.quantity}
                                   for l in slot.alloc_lines]).set_index(['store', 'sku']))
                if sum([l.quantity for l in orders[slot.order].lines]) == 0:
                    del orders[slot.order]
                    print('Order closed: {}'.format(slot.order))
                orders[slot.order].allocated = False
                slot.clear()
                slot.capacity = np.random.randint(25, 35)
                empty_slots.append(slot)
            start = print_timer(debug, start, 'Empty Slots')
            for slot in empty_slots:
                store, lines = assign_store(pw=pw,
                                            orders=orders,
                                            order_data=order_data,
                                            cartons=cartons)
                if store is None:
                    break
                slot.assign(order=store, alloc_lines=lines)
                orders[store].allocated = True
                if debug: print('Store {} assigned to Put-Wall {}'.format(store, pw.id))
            start = print_timer(debug, start, 'Store allocation')
            carton = assign_carton(pw=pw, orders=orders, cartons=cartons)
            if carton:
                pw.add_to_queue(carton)
                carton.allocated = True
                if debug: print('Carton added to queue for Put-Wall {}'.format(pw.id))
                count_carton_pulls += 1
            start = print_timer(debug, start, 'Carton allocation')

            # Release more SKUs
            active_units = sum([c.quantity for c in cartons.values() if c.active == True])
            if active_units < 500000:
                print('Releasing more SKUs...')
                inactive_skus = [k for k, v in item_master.items() if v.active == False]
                if inactive_skus:
                    activate_skus = np.random.choice(inactive_skus, size=100)
                    for sku in active_skus:
                        item_master[sku].active = True
                    for carton in cartons.values():
                        carton.active = item_master[carton.sku].active

    count_carton_returns = count_carton_pulls - len([t for t in cartons.values() if t.active == False])
    print('Carton Pulls: {}'.format(count_carton_pulls))
    print('Carton Returns: {}'.format(count_carton_returns))
    print('Carton Tote Moves: {}'.format(count_carton_pulls+count_carton_returns))

if __name__ == '__main__':
    run_model()