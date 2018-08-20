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

    def fill_from_queue(self, num_totes):
        for tote in self.queue[:num_totes]:
            for sku in tote.get_contents():
                for slot in self.find_slots(sku=sku):
                    qty_allocated = slot.get_allocation(sku=sku)
                    qty_remaining = tote.get_qty(sku=sku)
                    qty_available = slot.capacity - slot.quantity
                    qty_moved = min(qty_allocated, qty_remaining, qty_available)
                    slot.update_quantity(qty=qty_moved)
                    slot.update_allocation(sku=sku, qty=-qty_moved)
                    tote.update_quantity(sku=sku, qty=-qty_moved)

    def clear_empty_slots(self):
        empty_slots = []
        for id, slot in self.slots.items():
            if slot.is_clear():
                slot.clear()
                empty_slots.append(id)
        return empty_slots

    def find_slots(self, sku=None):
        found_slots = []
        for slot in self.slots.values():
            if slot.get_allocation(sku=sku) > 0:
                found_slots.append(slot)
        return found_slots


class PutSlot:
    def __init__(self, id, alloc_lines=None, capacity=0, quantity=0):
        self.id = id
        self.active = False
        self.capacity = capacity
        self.quantity = quantity
        self.alloc_lines = alloc_lines
        self.order = None

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
            else:
                print('Cannot change allocation to a negative')
                raise Exception

    def assign(self, order, alloc_lines):
        self.order = order
        self.alloc_lines = alloc_lines
        self.active = True