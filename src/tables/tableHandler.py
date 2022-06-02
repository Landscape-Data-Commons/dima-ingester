from src.utils.database_functions import arcno
from src.primarykeys.primary_key_functions import (
    pk_appender,
    get_plotkeys,
    pk_appender_bsne,
    form_date_check,
    pk_appender_soil)

import json

import os
import pandas as pd
import logging

#### generic table handler
config = r"C:\Users\kbonefont\OneDrive - USDA\Documents\GitHub\ingester_v3\src\utils\configs.json"
[i for i in data['bsne']]


dir = r"C:\Users\kbonefont\OneDrive - USDA\Documents\GitHub\ingester_v3\dimas"


class TableHandler:

    def __init__(self, dimapath, pk_formdate_range, tablename):
        self.bsne = json.load(open(file=config))["bsne"]
        self._join_key = json.load(open(file=config))["joinkeys"][tablename]
        self._dimapath = dimapath
        self._table_name = tablename
        self._bsne_check = any([
            i for i in form_date_check(self._dimapath).keys() if "tblBSNE" in i
            ])

        self.raw_table = arcno.MakeTableView(
            self._table_name,
            self._dimapath)

        self.table_pk = self.get_pk(pk_formdate_range)

    def get_pk(self, custom_daterange):

        if self._table_name in self.bsne and self._bsne_check==True:
            print("bsne")
            self.pk_source = pk_appender_bsne(self._dimapath,
                custom_daterange).drop_duplicates(ignore_index=True)
        elif self._table_name in self.bsne and self._bsne_check==False:
            print("not bsne")
            self.pk_source = pk_appender(self._dimapath,
                custom_daterange).drop_duplicates(ignore_index=True)

        cols = [i for i in self.raw_table.columns if '_x' not in i and '_y' not in i]
        cols.append('PrimaryKey')

        if self.pk_source is not None:
            return pd.merge(
                self.raw_table,
                self.pk_source.filter([self._join_key,
                                       'PrimaryKey'
                                       ]).drop_duplicates(ignore_index=True),
                how="inner", on=self._join_key).loc[:,cols]
        else:
            return pd.DataFrame(columns=[i for i in self.raw_table.columns])

        # self.table_pk = self.get_pk(pk_formdate_range)
        # self.final_df = self.tbl_fixes(
            # self.table_pk).drop_duplicates()
