from putwall.putwall import PutWall, PutSlot
from totes.totes import Tote
from orders.orders import Order_Handler
from logic.putwalloptimization import assign_stores, assign_totes, pass_to_pw
import sqlalchemy as sa
import pandas as pd
import numpy as np
import time, csv, argparse
from utilities import print_timer
from load_data import load_from_db, split_inv_to_tote

# create engine for quering using sqlalchemy
engine = sa.create_engine('mssql+pyodbc://sa:FT123!@#@lab-sqlserver3.invata.com\SQL2014/Burlington?driver=SQL+Server+Native+Client+11.0')
receiving_table = 'tblReceiving'
ItemMaster_table = 'dbo.tblItemMaster'


def run_model(num_putwalls=65, num_slot_per_wall=6, order_table='dbo.Outbound_New',
              date='01/23/2017', output_file='low_vol_results.csv'):
    #Max day 5/11/2017 @ dbo.Burlington0501to0511
# Setup
    debug = False
    initialize = False
    np.random.seed(32)
    start = time.time()

# Initialize orders
    sql = '''select ShipTo as store,
                            sku,
                            UnitQty_Future as units,
                            0 as alloc_qty
                            From {}
                            Where ShipDate = '{}'
                           Order by ShipTo, SKU'''.format(order_table, date)
    sql = '''select ShipTo as store,
                    Sum(UnitQty) as units,
                    0 as alloc_qty,
                    sku
                    From {}
                    Where ShipDate = '{}'
                    Group by ShipTo, sku'''.format(order_table, date)

    orders_df = load_from_db(debug=debug,
                             engine=engine,
                             sql=sql,
                             data_name='orders_df',
                             load_from_db=initialize,
                             data_filename='data.h5',
                             index=['store', 'sku'])

# Initialize item_master & inventory
    sql = '''select sku,
            Sum(UnitQty) AS units,
            count(sku) AS lines,
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

    item_master_df = inventory_df.loc[:, ~inventory_df.columns.isin(['lines', 'units'])].copy()
    sku_list = np.random.choice(item_master_df.index, size=int(len(item_master_df)*.6))
    item_master_df['active'] = item_master_df.index.isin(sku_list)
    inventory_df['active'] = item_master_df.index.isin(sku_list)

    #Fragile SKUs to own orders
    # sku_list = np.random.choice(item_master_df.index, size=int(len(item_master_df) * .05))
    # orders_df.reset_index(inplace=True)
    # orders_df.loc[orders_df['sku'].isin(sku_list), 'store'] = orders_df.loc[orders_df['sku'].isin(sku_list), 'store'] + '_f'
    # orders_df = orders_df.groupby(['store', 'sku']).sum()
    order_handler = Order_Handler(orders_df)
    start = print_timer(debug, start, 'Initialized item master & inventory')

# Split inventory into full totes
    totes_df = split_inv_to_tote(inventory_df, sku_list)

# Initialize put walls
    put_walls = {}
    for pw in range(num_putwalls):
        put_walls[pw] = PutWall(id=pw, num_slots=num_slot_per_wall, debug=debug, queue_length=5,
                                orders_df=orders_df, totes_df=totes_df)
        for ps in range(num_slot_per_wall):
            put_walls[pw].add_slot(PutSlot(id=ps))

    start = print_timer(debug, start, 'Initialized put-walls')

# Track stats
    loop_time = time.time()
    tote_pulls = 0
    tote_returns = 0
    tote_passes = 0
    loop = 0
    output = csv.DictWriter(open(output_file, 'w'), fieldnames=['quantity', 'sku', 'carton_id',
                                'order', 'putwall', 'loop'])
    output.writeheader()
    output = csv.DictWriter(open(output_file, 'a'), fieldnames=['quantity', 'sku', 'carton_id',
                                                                  'order', 'putwall', 'loop'])
    initial_units = orders_df['units'].sum()

    while orders_df['units'].sum() > 0:
        current_units = orders_df['units'].sum()
        print('Open Units: {}'.format(current_units))
        print('Picks per Tote: {}'.format((initial_units-current_units)/(tote_pulls-65*5)))
        print('Tote Pulls: {}'.format(tote_pulls))
        print('Tote Passes: {}'.format(tote_passes))
        print('Tote Returns: {}'.format(tote_returns))
        loop_time = print_timer(True, loop_time, 'Loop Start: {}'.format(loop))
        loop += 1

# Process each put_wall in order one at a time
        for pw in put_walls.values():

# Process totes
            if loop > 1 and loop < 6:
                assign_stores(debug=debug, pw=pw, orders_df=orders_df, totes_df=totes_df, stores_to_fill=1)
            if loop >= 6:
                assign_stores(debug=debug, pw=pw, orders_df=orders_df, totes_df=totes_df, stores_to_fill=6)


            if loop > 5:
                tote, log = pw.fill_from_queue(num_to_process=1, loop=loop, order_handler=order_handler)
                output.writerows(log)

                if tote: #If carton didn't pick clean, pass it or return it.
                    tote.alloc_qty = 0
                    tote.alloc_lines = {}
                    tote.allocated = False
                    totes_df.at[tote.id, 'allocated'] = False
                    if pass_to_pw(debug=debug, tote=tote, put_walls=put_walls, orders_df=orders_df,
                                  pw_id=pw.id, totes_df=totes_df):
                        tote_passes += 1
                    else:
                        tote_returns += 1

            carton_ids, need_skus = assign_totes(debug=debug, pw=pw, totes_df=totes_df,
                                      num_to_assign=1, orders_df=orders_df)
            tote_pulls += len(carton_ids)

            if need_skus and loop > 1:
# Release more SKUs
                inactive_skus = item_master_df.loc[item_master_df.active == False].index
                if len(inactive_skus) > 0:
                    sku_list = np.random.choice(inactive_skus, size=100) #TODO remove hardcoding
                    item_master_df.loc[sku_list, 'active'] = True
                    totes_df = totes_df.append(split_inv_to_tote(inventory_df, sku_list), ignore_index=True)

                start = print_timer(True, start, 'Release more SKUs')

# Save output to file
    file = open(output_file, 'w')
    writer = csv.DictWriter(file, fieldnames=output[0].keys())
    writer.writeheader()
    writer.writerows(output)
    file.close()

# Print stats
    print('Tote Pulls: {}'.format(tote_pulls))
    print('Tote Returns: {}'.format(tote_returns))
    print('Total Tote Moves: {}'.format(tote_pulls+tote_returns))
    print('Tote Passes: {}'.format(tote_passes))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_file', '-o')
    parser.add_argument('--num_putwalls', '-n', type=int)
    parser.add_argument('--num_slot_per_wall', '-s', type=int)
    args = parser.parse_args()
    run_model(**{k: v for k,v in args.__dict__.items() if v is not None})