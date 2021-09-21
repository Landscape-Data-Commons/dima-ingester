from src.utils.database_functions import arcno
from src.primarykeys.primary_key_functions import pk_appender
import os
import pandas as pd
import logging


class Plots:
    _table_name = "tblPlots"
    _join_key = "PlotKey"

    def __init__(self, dimapath):
        self._dimapath = dimapath
        logging.info(f"Extracting the {self._table_name} from the dimafile..")
        self.raw_table = arcno.MakeTableView(self._table_name, dimapath)
        logging.info(f"Appending primary key to the {self._table_name}..")
        self.table_pk = self.get_pk(3)
        self.final_df = self.tbl_fixes(self.table_pk)

    def get_pk(self, custom_daterange):
        # join to pk source with
        self.pk_source = pk_appender(self._dimapath, custom_daterange)
        return pd.merge(self.raw_table, self.pk_source.loc[:,[self._join_key,'PrimaryKey']], how="inner", on=self._join_key)

    def tbl_fixes(self, df):
        return df
