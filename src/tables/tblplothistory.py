from src.utils.database_functions import arcno

import os
import pandas as pd
import logging


class PlotHistory:
    _table_name = "tblPlotHistory"

    def __init__(self, dimapath):
        self._dimapath = dimapath

        self.raw_table = arcno.MakeTableView(self._table_name, dimapath)
        self.final_df = self.tbl_fixes(self.raw_table).drop_duplicates()

    def tbl_fixes(self, df):
        df = df.loc[:,~df.columns.duplicated()]
        return df
