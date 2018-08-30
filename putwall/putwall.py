from cartons.cartons import Carton
from utilities import print_timer
import time

#TODO Add putwall_manager (future)

class PutWall:
    def __init__(self, num_slots, id, queue_length=5, debug=False):
        self.num_slots = num_slots
        self.id = id
        self.slots = {}
        self.queue = []
        self.queue_length = queue_length
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
        tote = None

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
                    tote = tote.update_quantity(-qty_moved)
                    if slot.capacity - slot.quantity == 0 or order_closed: slot.clear()

        if log == []:
            print('No fulfillment')

        print_timer(self.debug, start, 'fill_from_queue')

        return tote, log

    def empty_slots(self):
        return [slot for slot in self.slots.values() if slot.is_clear()]

    def find_slots(self, sku=None):
        return [slot for slot in self.slots.values() if slot.get_allocation(sku=sku) > 0]


class PutSlot:
    def __init__(self, id, capacity=0, quantity=0, active=False, order=None):
        self.id = id
        self.active = active
        self.capacity = capacity
        self.quantity = quantity
        self.order = order

    def is_clear(self, clear_func=None):
        return self.active and self.order is None

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
