class Carton:
     def __init__(self, id, sku=None, quantity=None, active=True, allocated=False):
         self.id = id
         self.sku = sku
         self.quantity = quantity
         self.active = active
         self.allocated = allocated

