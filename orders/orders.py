#TODO add order manager (future)
class Order:
    def __init__(self, id):
        self.id = id
        self.lines = []
        self.allocated = False

    def add_line(self, line):
        self.lines.append(line)

    def line_status(self, status='Open'):
        #TODO remove
        return len([l for l in self.lines if l.status == status])

class Line:
    def __init__(self, sku, quantity, UOM=None, status='Open'):
        self.sku = sku
        self.quantity = quantity
        self.UOM = UOM
        self.status = status

class Order_Handler:
    def __init__(self, orders_df):
        self.orders_df = orders_df

    def deplete_inv(self, order, sku, quantity):
        current_inv = self.orders_df.at[(order, sku), 'units']
        if current_inv > quantity:
            self.orders_df.at[(order, sku), 'units'] -= quantity
            self.orders_df.at[(order, sku), 'alloc_qty'] -= quantity
        elif current_inv == quantity:
            return self.close_line(order=order, sku=sku)
        else:
            print('Error: Cannot create negative inventory: {}-{}'.format(order, sku))
            raise Exception

    def close_line(self, order, sku):
        self.orders_df.at[(order, sku), 'units'] = 0
        self.orders_df.at[(order, sku), 'alloc_qty'] = 0
        self.orders_df.at[(order, sku), 'active'] = False
        if self.orders_df.loc[order].units.sum() == 0:
            print('Closing order {}'.format(order))
            return True
        return False # Return False if order still open, True if closed