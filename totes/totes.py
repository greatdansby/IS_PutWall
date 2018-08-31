class Tote:
    def __init__(self, id, totes_df, allocated=True, active=True):
        self.id = id
        self.sku = totes_df.at[id, 'sku']
        self.quantity = totes_df.at[id, 'units']
        self.active = active
        self.allocated = allocated
        self.totes_df = totes_df
        self.totes_df.at[self.id, ['allocated', 'active']] = [allocated, active]

    def update_quantity(self, quantity):
        self.totes_df.at[self.id, 'units'] += quantity
        self.quantity += quantity
        if self.quantity == 0:
            self.close()
        return self

    def close(self):
        self.totes_df.at[self.id, 'active'] = False
        self.active = False
        self.totes_df.at[self.id, 'allocated'] = False
        self.allocated = False