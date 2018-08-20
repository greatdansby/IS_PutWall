class Tote:
    def __init__(self, id, num_compartments=1, active=True):
        self.num_compartments = num_compartments
        self.compartments = {}
        self.active = active
        self.id = id

    def add_compartment(self, compartment):
        if len(self.compartments) >= self.num_compartments:
            print('Trying to add too many compartments in tote: {}'.format(self.id))
            raise Exception
        self.compartments[compartment.id] = compartment

    def get_contents(self, active_only=True):
        skus = set([c.sku for c in self.compartments.values() if active_only and  c.active == active_only])
        return {sku: self.get_qty(sku=sku, active_only=active_only) for sku in skus}

    def update_quantity(self, sku, qty):
        for c in self.compartments.values():
            if c.sku == sku:
                adj_qty = min(qty, c.quantity)
                if c.quantity + adj_qty >= 0:
                    qty = adj_qty
                    c.quantity += adj_qty
                else:
                    print('Compartment cannot have negative quantity')
                    raise Exception
                if qty == 0:
                    break
        if self.is_empty():
            self.active = False

    def is_empty(self):
        if self.get_contents(active_only=False):
            return False
        return True

    def get_qty(self, sku, active_only=True):
        return sum([c.quantity for c in self.compartments.values() if c.sku == sku and c.active == active_only])

class Compartment:
    def __init__(self, id, sku=None, quantity=0, UOM=None, active=False):
        self.id = id
        self.sku = sku
        self.active = active
        self.quantity = quantity
        self.UOM = UOM