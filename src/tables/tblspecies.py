from src.utils.database_functions import arcno

import os
import pandas as pd
import logging


class Species:
    _table_name = "tblSpecies"

    def __init__(self, dimapath):
        self._dimapath = dimapath

        self.raw_table = arcno.MakeTableView(self._table_name, dimapath)
        self.final_df = self.tbl_fixes(self.raw_table).drop_duplicates()

    def tbl_fixes(self, df):
        return df
