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