from cartons.cartons import Carton
from utilities import print_timer
import time

#TODO Add putwall_manager (future)

class PutWall:
    def __init__(self, num_slots, id, queue_length=1, facings=1, debug=False):
        self.num_slots = num_slots
        self.id = id
        self.slots = {}
        self.queue = []
        self.debug = debug

    def add_slot(self, putslot):
        self.slots[putslot.id] = putslot

    def add_to_queue(self, tote):
        if tote:
            self.queue.append(tote)
            return True
        print('Warning: No tote provided')
        return False

    def fill_from_queue(self, num_to_process, loop, order_handler):

        start = time.time()
        # TODO generalize fill fucntion and allow override
        log = []

        for n in range(min(num_to_process, len(self.queue))):

            obj = self.queue.pop(0)
            if type(obj) == Tote:
                tote = obj
                for slot in self.find_slots(sku=tote.sku):
                    qty_allocated = slot.get_allocation(sku=tote.sku)
                    qty_remaining = tote.quantity
                    qty_available = slot.capacity - slot.quantity
                    qty_moved = min(qty_allocated, qty_remaining, qty_available)

                    if qty_moved == 0: print('Warning (fill_from_queue): 0 quantity moved.')
                    log.append({'quantity': qty_moved,
                                'sku': tote.sku,
                                'carton_id': tote.id,
                                'order': slot.order,
                                'putwall': self.id,
                                'loop': loop})

                    slot.update_quantity(qty=qty_moved)
                    order_closed = order_handler.deplete_inv(order=slot.order, sku=tote.sku, quantity=qty_moved) #TODO not crazy about this
                    tote_id = tote.update_quantity(-qty_moved)
                    if slot.capacity - slot.quantity == 0 or order_closed: slot.clear()

        if log == []:
            print('No fulfillment')

        print_timer(self.debug, start, 'fill_from_queue')

        return tote_id, log

    def clear_empty_slots(self):
        empty_slots = []
        for id, slot in self.slots.items():
            if slot.is_clear():
                slot.clear()
                empty_slots.append(id)
        return empty_slots

    def find_slots(self, sku=None):
        return [slot for slot in self.slots.values() if slot.get_allocation(sku=sku) > 0]

    def get_allocation(self):
        #TODO refactor
        allocation = {}
        for slot in [s for s in self.slots.values() if s.active]:
            for line in slot.alloc_lines:
                if line.sku in allocation:
                    allocation[line.sku] += line.quantity
                elif line.quantity > 0:
                    allocation[line.sku] = line.quantity
        return allocation


class PutSlot:
    def __init__(self, id, alloc_lines=None, capacity=0, quantity=0, active=False, order=None):
        self.id = id
        self.active = active
        self.capacity = capacity
        self.quantity = quantity
        self.alloc_lines = alloc_lines
        self.order = order

    def is_clear(self, clear_func=None):
        if clear_func:
            if clear_func(self):
                return True
            return False
        elif not self.active or self.quantity >= self.capacity:
            return True
        return False

    def clear(self):
        #TODO refactor
        self.order = None
        self.alloc_lines = None
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
            self.alloc_lines = alloc_lines
            self.active = True
