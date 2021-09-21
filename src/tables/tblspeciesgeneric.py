from src.utils.database_functions import arcno

import os
import pandas as pd
import logging


class SpeciesGeneric:
    _table_name = "tblSpeciesGeneric"

    def __init__(self, dimapath):
        self._dimapath = dimapath

        self.raw_table = arcno.MakeTableView(self._table_name, dimapath)
        self.final_df = self.tbl_fixes(self.raw_table)

    def tbl_fixes(self, df):
        return df
