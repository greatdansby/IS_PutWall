class Tote:
    def __init__(self, id, totes_df, allocated=True):
        self.id = id
        self.sku = totes_df[id].sku
        self.quantity = totes_df[id].units
        self.active = True
        self.allocated = allocated

    def update_quantity(self):
        #TODO this
        return self