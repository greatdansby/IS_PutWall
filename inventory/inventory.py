class Inventory:
    def __init__(self):
        self.locations = {}

class Location:
    def __init__(self, id, type=None, quantity=0, UOM=None, sku=None, active=False):
        self.id = id
        self.type = type
        self.quantity = quantity
        self.UOM = UOM
        self.sku = sku
        self.active = active

    def update_qty(self, adjustment):
        new_qty = self.quantity + adjustment
        if new_qty < 0:
            print('Negative inventory after adjustment')
            raise Exception
        if new_qty == 0:
            #TODO: Do something if depleated?
            pass
        self.quantity = new_qty