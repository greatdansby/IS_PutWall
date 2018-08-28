from cartons.cartons import Carton
from totes.totes import Tote

class PutWall:
    def __init__(self, num_slots, id, queue_length=1, facings=1):
        self.num_slots = num_slots
        self.id = id
        self.slots = {}
        self.queue = []

    def add_slot(self, putslot):
        self.slots[putslot.id] = putslot

    def add_to_queue(self, tote):
        if tote:
            self.queue.append(tote)
            return True
        print('Warning: No tote provided')
        return False

    def fill_from_queue(self, num_obj, loop):
        debug = False
        log = []
        for n in range(min(num_obj, len(self.queue))):
            obj = self.queue.pop(0)
            if type(obj) == Tote:
                tote = obj
                for sku in tote.get_contents():
                    for slot in self.find_slots(sku=sku):
                        qty_allocated = slot.get_allocation(sku=sku)
                        qty_remaining = tote.get_qty(sku=sku)
                        qty_available = slot.capacity - slot.quantity
                        qty_moved = min(qty_allocated, qty_remaining, qty_available)
                        slot.update_quantity(qty=qty_moved)
                        slot.update_allocation(sku=sku, qty=-qty_moved)
                        tote.update_quantity(sku=sku, qty=-qty_moved)
                        if tote.is_empty():
                            tote.active = False
                        tote.allocated = False
                        log.append('{} of {} moved from tote {} to order {}'.format(qty_moved, sku, tote.id, slot.order))
            if type(obj) == Carton:
                carton = obj
                for slot in self.find_slots(sku=carton.sku):
                    qty_allocated = slot.get_allocation(sku=carton.sku)
                    qty_remaining = carton.quantity
                    qty_available = slot.capacity - slot.quantity
                    qty_moved = min(qty_allocated, qty_remaining, qty_available)
                    if qty_moved == 0:
                        if debug: print('No fulfillment due to empty Carton')
                    slot.update_quantity(qty=qty_moved)
                    slot.update_allocation(sku=carton.sku, qty=-qty_moved)
                    carton.quantity -= qty_moved
                    if carton.quantity == 0:
                        carton.active = False
                    carton.allocated = False
                    log.append({'quantity': qty_moved,
                                'sku': carton.sku,
                                'carton_id': carton.id,
                                'order': slot.order,
                                'putwall': self.id,
                                'loop': loop})
        if log == []:
            print('No fulfillment')
        return log

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
        self.order = None
        self.alloc_lines = None
        self.active = False
        self.quantity = 0

    def get_allocation(self, sku):
        if self.alloc_lines:
            return sum([l.quantity for l in self.alloc_lines if l.sku == sku])
        return 0

    def update_quantity(self, qty):
        if self.quantity + qty <= self.capacity:
            self.quantity += qty
        else:
            print('Put slot {} is full, cannot add to quantity'.format(self.id))
            raise Exception

    def update_allocation(self, sku, qty):
        for line in [l for l in self.alloc_lines if l.sku == sku]:
            if line.quantity + qty >= 0:
                line.quantity += qty
                line.status = 'Updated'
            else:
                print('Cannot change allocation to a negative')
                raise Exception

    def assign(self, order, alloc_lines):
        if order and alloc_lines:
            self.order = order
            self.alloc_lines = alloc_lines
            self.active = True
