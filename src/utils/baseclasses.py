class Table:
    def __init__(self, dimapath):
        self._dimapath = dimapath
        self.raw_table = arcno.MakeTableView()
