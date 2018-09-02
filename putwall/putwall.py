from totes.totes import Tote
from utilities import print_timer
import pandas as pd
import time

#TODO Add putwall_manager (future)

class PutWall:
    def __init__(self, num_slots, id, queue_length=5, debug=False, orders_df=None, totes_df=None):
        self.num_slots = num_slots
        self.id = id
        self.slots = {}
        self.queue = []
        self.queue_length = queue_length
        self.debug = debug
        self.orders_df = orders_df
        self.totes_df = totes_df

    def add_slot(self, putslot):
        self.slots[putslot.id] = putslot

    def add_to_queue(self, tote):
        self.queue.append(tote)

    def allocate_queue_to_orders(self):
        start = time.time()

        order_ids = self.get_orders()
        if order_ids:
            self.orders_df.loc[self.orders_df.index.get_level_values(0).isin(order_ids), 'alloc_qty'] = 0
            for tote in self.queue:
                tote.alloc_qty = 0
                for order_id in order_ids:
                    if tote.quantity - tote.alloc_qty == 0:
                        break
                    if (order_id, tote.sku) in self.orders_df.index:
                        order_alloc = self.orders_df.at[(order_id, tote.sku), 'alloc_qty']
                        order_units = self.orders_df.at[(order_id, tote.sku), 'units']
                        if order_units - order_alloc == 0:
                            break
                        alloc = min(tote.quantity - tote.alloc_qty, order_units - order_alloc)
                        tote.alloc_qty += alloc
                        order_alloc += alloc
                        self.orders_df.at[(order_id, tote.sku), 'alloc_qty'] += alloc
                self.totes_df.at[tote.id, 'alloc_qty'] = tote.alloc_qty

        print_timer(self.debug, start, 'allocate_queue_to_orders')

    def get_orders(self):
        return [s.order for s in self.slots.values() if s.order is not None]

    def get_totes(self):
        return [t.id for t in self.queue]

    def fill_from_queue(self, num_to_process, loop, order_handler):

        start = time.time()
        # TODO generalize fill fucntion and allow override
        log = []
        tote = None

        for n in range(min(num_to_process, len(self.queue))):

            obj = self.queue.pop(0)
            if type(obj) == Tote:
                tote = obj
                for slot in self.find_slots(sku=tote.sku):
                    qty_allocated = self.orders_df.at[(slot.order, tote.sku), 'units']
                    qty_remaining = tote.quantity
                    qty_available = slot.capacity - slot.quantity
                    qty_moved = min(qty_allocated, qty_remaining, qty_available)

                    if qty_moved == 0:
                        print('Warning (fill_from_queue): 0 quantity moved.')
                        break
                    log.append({'quantity': qty_moved,
                                'sku': tote.sku,
                                'carton_id': tote.id,
                                'order': slot.order,
                                'putwall': self.id,
                                'loop': loop})

                    slot.update_quantity(qty=qty_moved)
                    order_closed = order_handler.deplete_inv(order=slot.order, sku=tote.sku, quantity=qty_moved) #TODO not crazy about this
                    tote = tote.update_quantity(-qty_moved)
                    if slot.capacity - slot.quantity == 0 or order_closed:
                        print('Clearing slot: {}-{}'.format(self.id, slot.id))
                        slot.clear()
                    if tote.quantity == 0:
                        print('Tote picked clean')
                        break

        if log == []:
            print('No fulfillment')

        print_timer(self.debug, start, 'fill_from_queue')

        return tote, log

    def empty_slots(self):
        return [slot for slot in self.slots.values() if slot.is_clear()]

    def find_slots(self, sku=None):
        order_list = [(slot.order, sku) for slot in self.slots.values() if slot.order is not None]
        if order_list:
            filtered_order_list = self.orders_df.loc[self.orders_df.index.isin(order_list) &
                                                     (self.orders_df['units'] > 0)].index.get_level_values(0)
            return [slot for slot in self.slots.values() if slot.order in filtered_order_list]
        return pd.DataFrame()

    def get_allocation(self):
        order_list = [slot.order for slot in self.slots.values() if slot.order is not None]
        if order_list:
            return self.orders_df.loc[order_list].groupby('sku').sum()
        return pd.DataFrame()


class PutSlot:
    def __init__(self, id, capacity=0, quantity=0, active=False, order=None):
        self.id = id
        self.active = active
        self.capacity = capacity
        self.quantity = quantity
        self.order = order

    def is_clear(self, clear_func=None):
        return not self.active and self.order is None

    def clear(self):
        #TODO refactor
        self.order = None
        self.active = False
        self.quantity = 0

    def get_allocation(self, sku):
        #TODO refactor
        if self.alloc_lines:
            return sum([l.quantity for l in self.alloc_lines if l.sku == sku])
        return 0

    def update_quantity(self, qty):
        #TODO refactor
        if self.quantity + qty <= self.capacity:
            self.quantity += qty
        else:
            print('Put slot {} is full, cannot add to quantity'.format(self.id))
            raise Exception

    def update_allocation(self, sku, qty):
        #TODO refactor
        for line in [l for l in self.alloc_lines if l.sku == sku]:
            if line.quantity + qty >= 0:
                line.quantity += qty
                line.status = 'Updated'
            else:
                print('Cannot change allocation to a negative')
                raise Exception

    def assign(self, order, alloc_lines):
        #TODO remove
        if order and alloc_lines:
            self.order = order
            self.active = True
