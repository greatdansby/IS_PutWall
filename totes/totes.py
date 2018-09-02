class Tote:
    def __init__(self, id, totes_df, alloc_qty=0, allocated=True, active=True):
        self.id = id
        self.sku = totes_df.at[id, 'sku']
        self.quantity = totes_df.at[id, 'units']
        self.active = active
        self.alloc_qty = alloc_qty
        self.allocated = allocated
        self.totes_df = totes_df
        self.totes_df.at[self.id, ['allocated', 'active', 'alloc_qty']] = [allocated, active, alloc_qty]

    def update_quantity(self, quantity):
        self.totes_df.at[self.id, 'units'] += quantity
        self.quantity += quantity
        self.totes_df.at[self.id, 'alloc_qty'] += quantity
        self.alloc_qty += quantity
        if self.quantity == 0:
            self.close()
        return self

    def close(self):
        self.totes_df.at[self.id, 'active'] = False
        self.active = False
        self.totes_df.at[self.id, 'allocated'] = 0
        self.allocated = False
        self.alloc_qty = 0