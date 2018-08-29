from putwall.putwall import PutWall, PutSlot
from cartons.cartons import Carton
from skus.skus import SKU
from orders.orders import Order, Line
from logic.putwalloptimization import assign_store, assign_carton, get_top_stores, pass_to_pw
import sqlalchemy as sa
import pandas as pd
import numpy as np
import time, csv, argparse
from utilities import print_timer
from load_data import load_from_db

# create engine for quering using sqlalchemy
engine = sa.create_engine('mssql+pyodbc://sa:FT123!@#@lab-sqlserver3.invata.com\SQL2014/Burlington?driver=SQL+Server+Native+Client+11.0')
receiving_table = 'tblReceiving'
ItemMaster_table = 'dbo.tblItemMaster'


def run_model(num_putwalls=65, num_slot_per_wall=6, order_table='dbo.Burlington0501to0511',
              date='5/11/2017', output_file='output.csv'):
# Setup
    debug = False
    initialize = False
    np.random.seed(32)
    start = time.time()

# Initialize orders
    sql = '''select ShipTo as store,
                            sku,
                            UnitQty_Future as units,
                            0 as fulfilled
                            From {}
                            Where ShipDate = '{}'
                           Order by ShipTo, SKU'''.format(order_table, date)

    orders_df = load_from_db(debug=debug,
                             engine=engine,
                             sql=sql,
                             data_name='orders_df',
                             load_from_db=initialize,
                             data_filename='data.h5',
                             index=['store,sku'])

# Initialize put walls
#TODO init put-wall df

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

    start = print_timer(debug, start, 'Initialized put-walls')

# Initialize item_master

    sql = '''select sku,
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

    inventory_df = load_from_db( debug=debug,
                                 engine=engine,
                                 sql=sql,
                                 data_name='inventory_df',
                                 load_from_db=initialize,
                                 data_filename='data.h5',
                                 index=['sku'])

    item_master = {i: SKU(id=i) for i in inventory_df.index}
    # Order sku_list by demand ascending
    order_array = pd.DataFrame([[l.sku, l.quantity] for o in orders.values() for l in o.lines])
    sku_demand = order_array.groupby(0).sum()
    sku_list = sku_demand.index
    active_skus = np.random.choice(sku_list, size=int(len(sku_list)*.6))
    for sku in active_skus:
        item_master[sku].active = False

    start = print_timer(debug, start, 'Initialized item master')

# Intialize cartons
#TODO init carton df
    cartons = {}
    units = inventory_df
    carton_id = 0
    carton_data = []
    for sku in sku_list:
        while units.loc[sku, 'Units'] > 0:
            units_per_case = units.loc[sku, 'unitspercase']
            if units_per_case == 0:
                units_per_case = units.loc[sku, 'Units']
            cartons[carton_id] = Carton(carton_id, active=item_master[sku].active, sku=sku, quantity=units_per_case)
            carton_data.append({'id':carton_id,
                                'active': item_master[sku].active,
                                'sku': sku,
                                'quantity': units_per_case,
                                'allocated': False})
            units.loc[sku, 'Units'] -= min(units_per_case, units.loc[sku, 'Units'])
            carton_id += 1
    carton_data = pd.DataFrame(carton_data)
    carton_data = carton_data.set_index('id')
    print('{} Cartons created.'.format(len(cartons)))

    start = print_timer(debug, start, 'Initialized Totes')
    loop_time = time.time()

    count_carton_pulls = 0
    units_shipped = 0
    loop = 0
    output = []
    open_lines = []
    passes = 0
    while order_data['units'].sum() > 0:
        if sum(order_data[order_data['units'] > 0].units) == sum(open_lines):
            print('Nothing left')
        print('Loop: {}\nCarton Pulls: {}'.format(loop, count_carton_pulls))
        open_lines = list(order_data[order_data['units'] > 0].units)
        print('Open Lines: {}'.format(len(open_lines)))
        print('Open Units: {}'.format(sum(open_lines)))
        print('Active Units: {}'.format(carton_data[carton_data['active'] == True]['quantity'].sum()))

        loop_time = print_timer(True, loop_time, 'Loop Start')
        loop += 1
        for pw in put_walls.values():
            log = pw.fill_from_queue(1, loop)
            if log:
                carton_data.at[log[0]['carton_id'], 'quantity'] -= sum([m['quantity'] for m in log])
                carton_data.at[log[0]['carton_id'], 'allocated'] = False
                if carton_data.at[log[0]['carton_id'], 'quantity'].sum() == 0:
                    carton_data.at[log[0]['carton_id'], 'active'] = False
                    del cartons[log[0]['carton_id']]
                else:
                    # Pass non-empty totes to another put-wall if available
                    if pass_to_pw(cartons[log[0]['carton_id']], put_walls, pw.id):
                        passes += 1
                        cartons[log[0]['carton_id']].allocated = True
                        carton_data.at[log[0]['carton_id'], 'allocated'] = True

                order_data.loc[[(r['order'], r['sku']) for r in log], 'units'] -= [r['quantity'] for r in log]
                closed_orders = [k for k, v in (order_data.groupby('store').units.sum()== 0).to_dict().items() if v]
                for order_id in closed_orders:
                    if order_id in orders: del orders[order_id]
                for slot in pw.slots.values():
                    if slot.order in closed_orders:
                        slot.clear()
                output.extend(log)
            if debug: print(log)
            start = print_timer(debug, start, 'Fill from Q')

            empty_slots = []
            for slot in [s for s in pw.slots.values() if s.is_clear()]:
                if slot.order is not None:
                    if debug: print('Carton for {} shipped from Put-Wall {}'.format(slot.order, pw.id))
                    orders[slot.order].lines = slot.alloc_lines
                    orders[slot.order].allocated = False
                    if sum([l.quantity for l in orders[slot.order].lines]) == 0:
                        del orders[slot.order]
                        print('Order closed: {}'.format(slot.order))
                slot.clear()
                slot.capacity = np.random.randint(25, 35)
                empty_slots.append(slot)
            start = print_timer(debug, start, 'Empty Slots')

            for slot in empty_slots:
                store, lines = assign_store(pw=pw,
                                            orders=orders,
                                            order_data=order_data,
                                            )
                if store is None:
                    break
                slot.assign(order=store, alloc_lines=lines)
                orders[store].allocated = True
                if debug: print('Store {} assigned to Put-Wall {}'.format(store, pw.id))
            start = print_timer(debug, start, 'Store allocation')

            carton_id = assign_carton(pw=pw, carton_data=carton_data, cartons=cartons)
            if carton_id is not None:
                pw.add_to_queue(cartons[carton_id])
                cartons[carton_id].allocated = True
                carton_data.at[carton_id, 'allocated'] = True
                if debug: print('Carton added to queue for Put-Wall {}'.format(pw.id))
                count_carton_pulls += 1
            elif loop > 1:
                # Release more SKUs
                print('Releasing more SKUs...')
                inactive_skus = [k for k, v in item_master.items() if v.active == False]
                if inactive_skus:
                    active_skus = np.random.choice(inactive_skus, size=1000)
                    for sku in active_skus:
                        item_master[sku].active = True
                    update_carton_ids = [carton.id for carton in cartons.values()
                                         if item_master[carton.sku].active and carton.active == False]
                    for carton_id in update_carton_ids:
                        cartons[carton_id].active = True
                    carton_data.active.iloc[update_carton_ids] = True
                start = print_timer(debug, start, 'Release more SKUs')

    file = open(output_file, 'w')
    writer = csv.DictWriter(file, fieldnames=output[0].keys())
    writer.writeheader()
    writer.writerows(output)
    file.close()

    count_carton_returns = count_carton_pulls - len([t for t in cartons.values() if t.active == False])
    print('Carton Pulls: {}'.format(count_carton_pulls))
    print('Carton Returns: {}'.format(count_carton_returns))
    print('Carton Tote Moves: {}'.format(count_carton_pulls+count_carton_returns))
    print('Carton Passes: {}'.format(passes))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_file', '-o')
    parser.add_argument('--num_putwalls', '-n', type=int)
    parser.add_argument('--num_slot_per_wall', '-s', type=int)
    args = parser.parse_args()
    run_model(**vars(args))