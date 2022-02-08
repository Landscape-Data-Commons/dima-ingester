from src.utils.database_functions import arcno

import os
import pandas as pd
import logging


class SpeciesGeneric:
    _table_name = "tblSpeciesGeneric"

    def __init__(self, dimapath):
        self._dimapath = dimapath

        self.raw_table = arcno.MakeTableView(self._table_name, dimapath)
        # self.final_df = self.tbl_fixes(self.raw_table).drop_duplicates()

    def tbl_fixes(self, df):
        df = df.loc[:,~df.columns.duplicated()]

        # df = df.fillna(0)
        for i in df.columns:
            checklist = ["petalsNumber", "sepalsNumber", "stamensNumber"]
            if i in [i for i in df.columns if i in checklist]:
                df[i] = df[i].fillna(0)
                df[i] = df[i].astype(int)

        return df
