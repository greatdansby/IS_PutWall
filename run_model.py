from putwall.putwall import PutWall, PutSlot
from totes.totes import Tote, Compartment
from skus.skus import SKU
from orders.orders import Order, Line
from logic.putwalloptimization import assign_store, add_tote_to_queue, get_top_stores
import sqlalchemy as sa
import pandas as pd
import numpy as np
import time

# create engine for quering using sqlalchemy
engine = sa.create_engine('mssql+pyodbc://sa:FT123!@#@lab-sqlserver3.invata.com\SQL2014/Burlington?driver=SQL+Server+Native+Client+11.0')
receiving_table = 'tblReceiving'
ItemMaster_table = 'dbo.tblItemMaster'


def print_timer(start, label=''):
    time_ttl = round(time.time()-start, 2)
    label_len = len(label)
    buffer = '-'*max(85-label_len, 0)
    print('{0}{1} Elapsed Time {2} seconds'.format(label, buffer, time_ttl))
    return time.time()


def run_model(num_putwalls=65, num_slot_per_wall=6, inventory_file=None, order_table='dbo.Burlington0501to0511',
              date='5/11/2017'):
    start = time.time()

    ## Initialize orders
    orders = {}
    sql_query = '''select TOP 10000 ShipTo as store,
                    sku,
                    UnitQty_Future as units
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
                put_walls[pw].add_slot(PutSlot(id=ps, capacity=np.random.randint(25, 35), order=order_id, active=True))
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

    start = print_timer(start, 'Initialized Item Master')

    ## Intialize inventory
    totes = {}
    inventory = pd.DataFrame(index=None,
                             columns=['Tote_id', 'Sku_id', 'Tote_Qty', 'Inv_Qty', 'Active', 'loop_activated'],
                             dtype=int)
    units = item_master_data
    ttl_units = np.sum(units.loc[:, 'Units'])
    tote_id = 0

    start = print_timer(start, 'Initialized inventory')

    for sku in sku_list:
        while units.loc[sku, 'Units'] > 0:
            units_per_case = units.loc[sku, 'unitspercase']
            totes[tote_id] = Tote(tote_id, active=item_master[sku].active)
            totes[tote_id].add_compartment(Compartment(id=1,
                                                      sku=sku,
                                                      quantity=units_per_case,
                                                      active=True))
            units.loc[sku, 'Units'] -= min(units_per_case, units.loc[sku, 'Units'])
            tote_id += 1
    print('{} Totes created.'.format(len(totes)))

    start = print_timer(start, 'Initialized Totes')

    for i in range(100):
        count_tote_pulls = 0
        for pw in put_walls.values():
            print(pw.id)
            print(pw.fill_from_queue(1))

            #start = print_timer(start, 'Fill from queue')

            empty_slots = []
            for id, slot in pw.slots.items():
                if slot.is_clear():
                    if slot.order in orders:
                        orders[slot.order] = slot.allocation ##TODO ensure order slotted to single slot
                    slot.clear()
                    empty_slots.append(slot)

            #start = print_timer(start, 'Clear slots')

            for slot in empty_slots:
                store, lines = assign_store(pw=pw, orders=orders, item_master=item_master,
                                         totes=totes, put_walls=put_walls)

                if store is None:
                    break

                #start = print_timer(start, 'Assign store')

                slot.assign(order=store, alloc_lines=lines)

                #start = print_timer(start, 'Assign slot')

            if pw.add_to_queue(add_tote_to_queue(pw=pw, orders=orders, item_master=item_master,
                                                 totes=totes, put_walls=put_walls)):
                count_tote_pulls += 1

            #start = print_timer(start, 'Assign SKU')

            ##TODO add sku release
    count_tote_returns = count_tote_pulls = len([t for t in totes.values() if t.active == False])
    print('Tote Pulls: {}'.format(count_tote_pulls))
    print('Tote Returns: {}'.format(count_tote_returns))
    print('Total Tote Moves: {}'.format(count_tote_pulls+count_tote_returns))



if __name__ == '__main__':
    run_model()